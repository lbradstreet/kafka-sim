use super::*;

#[test]
fn cross_thread_host_capabilities_are_send_and_sync() {
    fn assert_send_sync<T: Send + Sync>() {}

    assert_send_sync::<HostSendHandle>();
    assert_send_sync::<HostSendJoinHandle<usize>>();
    assert_send_sync::<HostControl>();
}

#[test]
fn typed_send_spawn_before_owner_drive_joins_with_its_value() {
    let mut runtime = HostRuntime::default();
    let join = runtime
        .send_handle()
        .spawn(async { String::from("portable") })
        .expect("portable task is admitted before driving");
    assert!(join.id().is_some());

    assert_eq!(
        runtime.block_on(join).expect("runtime drives join"),
        Ok(String::from("portable"))
    );
}

#[test]
fn typed_send_spawn_from_foreign_thread_joins_with_its_value() {
    let mut runtime = HostRuntime::default();
    let send = runtime.send_handle();
    let join = thread::spawn(move || {
        send.spawn(async { 6 * 7 })
            .expect("foreign portable task is admitted")
    })
    .join()
    .expect("foreign spawn thread exits");

    assert_eq!(runtime.block_on(join).expect("runtime drives join"), Ok(42));
}

#[test]
fn send_join_can_be_aborted_from_a_foreign_thread() {
    let mut runtime = HostRuntime::default();
    let join = runtime
        .send_handle()
        .spawn(pending::<()>())
        .expect("portable pending task is admitted");
    let join = thread::spawn(move || {
        join.abort();
        join
    })
    .join()
    .expect("abort thread exits");

    assert_eq!(
        runtime.block_on(join).expect("runtime applies send abort"),
        Err(JoinError::Cancelled)
    );
}

const STORM_THREADS: usize = 8;
const WAKES_PER_THREAD: usize = 10_000;

struct ForeignWakeStorm {
    started: bool,
    ready: Arc<AtomicBool>,
    workers: Vec<thread::JoinHandle<()>>,
}

impl Future for ForeignWakeStorm {
    type Output = ();

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        if this.ready.load(Ordering::Acquire) {
            for worker in this.workers.drain(..) {
                worker.join().expect("wake worker exits");
            }
            return Poll::Ready(());
        }

        if !this.started {
            this.started = true;
            let barrier = Arc::new(Barrier::new(STORM_THREADS + 1));
            let remaining = Arc::new(AtomicUsize::new(STORM_THREADS));
            for _ in 0..STORM_THREADS {
                let worker_barrier = Arc::clone(&barrier);
                let worker_remaining = Arc::clone(&remaining);
                let worker_ready = Arc::clone(&this.ready);
                let waker = context.waker().clone();
                this.workers.push(thread::spawn(move || {
                    worker_barrier.wait();
                    for _ in 0..WAKES_PER_THREAD {
                        waker.wake_by_ref();
                    }
                    if worker_remaining.fetch_sub(1, Ordering::AcqRel) == 1 {
                        worker_ready.store(true, Ordering::Release);
                        waker.wake();
                    }
                }));
            }
            barrier.wait();
        }

        Poll::Pending
    }
}

#[test]
fn foreign_wake_storm_coalesces_and_unparks_the_owner() {
    let mut runtime = HostRuntime::new(HostConfig {
        max_ingress: 1,
        max_ingress_per_turn: 1,
        ..HostConfig::default()
    })
    .expect("config is valid");
    runtime
        .block_on(ForeignWakeStorm {
            started: false,
            ready: Arc::new(AtomicBool::new(false)),
            workers: Vec::new(),
        })
        .expect("foreign wakes resume the root without overflow");

    assert_eq!(runtime.control().status(), HostStatus::Running);
}

#[test]
fn infallible_ingress_overflow_is_retained_as_a_fatal_error() {
    let mut runtime = HostRuntime::new(HostConfig {
        max_ingress: 1,
        max_ingress_per_turn: 1,
        ..HostConfig::default()
    })
    .expect("config is valid");
    let mut join = runtime
        .send_handle()
        .spawn(pending::<()>())
        .expect("the send-spawn occupies the only ingress slot");

    join.abort();

    let error = runtime
        .block_on(async {})
        .expect_err("an infallible abort cannot be silently discarded");
    assert!(matches!(
        &error.kind,
        HostRunErrorKind::ResourceExhausted {
            resource: "cross-thread ingress",
            limit: 1,
        }
    ));
    assert_eq!(runtime.control().status(), HostStatus::Failed);
    assert_eq!(
        poll_once(&mut join),
        Poll::Ready(Err(JoinError::RuntimeStopped))
    );
    assert_eq!(
        runtime
            .shutdown()
            .expect_err("fatal teardown remains latched")
            .kind,
        error.kind
    );
}

#[test]
fn fallible_send_spawn_reports_a_full_ingress_synchronously() {
    let mut runtime = HostRuntime::new(HostConfig {
        max_ingress: 1,
        max_ingress_per_turn: 1,
        ..HostConfig::default()
    })
    .expect("config is valid");
    let first = runtime
        .send_handle()
        .spawn(pending::<()>())
        .expect("the first send-spawn is admitted");

    assert!(matches!(
        runtime.send_handle().spawn(async {}),
        Err(SpawnError::ResourceExhausted {
            resource: "cross-thread ingress",
            limit: 1,
        })
    ));
    assert_eq!(runtime.control().status(), HostStatus::Running);

    runtime
        .shutdown()
        .expect("ordinary teardown remains healthy");
    assert!(first.is_finished());
}

struct StopAfterFirstPoll {
    barrier: Arc<Barrier>,
    polled: bool,
}

impl Future for StopAfterFirstPoll {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, _context: &mut Context<'_>) -> Poll<Self::Output> {
        if !self.polled {
            self.polled = true;
            self.barrier.wait();
        }
        Poll::Pending
    }
}

#[test]
fn request_stop_from_foreign_thread_interrupts_the_owner() {
    let mut runtime = HostRuntime::default();
    let control = runtime.control();
    let barrier = Arc::new(Barrier::new(2));
    let worker_barrier = Arc::clone(&barrier);
    let worker = thread::spawn(move || {
        worker_barrier.wait();
        control.request_stop();
    });

    let error = runtime
        .block_on(StopAfterFirstPoll {
            barrier,
            polled: false,
        })
        .expect_err("stop interrupts pending root");
    worker.join().expect("stop worker exits");

    assert_eq!(error.kind, HostRunErrorKind::StopRequested);
    assert_eq!(runtime.control().status(), HostStatus::Stopped);
    runtime.shutdown().expect("repeated shutdown is idempotent");
}
