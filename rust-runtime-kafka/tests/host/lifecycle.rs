use super::*;

#[test]
fn invalid_ingress_bounds_are_rejected() {
    assert!(matches!(
        HostRuntime::new(HostConfig {
            max_ingress: 0,
            ..HostConfig::default()
        }),
        Err(HostConfigError::ZeroIngressCapacity)
    ));
    assert!(matches!(
        HostRuntime::new(HostConfig {
            max_ingress_per_turn: 0,
            ..HostConfig::default()
        }),
        Err(HostConfigError::ZeroIngressPerTurn)
    ));
}

#[test]
fn local_spawn_accepts_non_send_future_output_and_typed_join() {
    let mut runtime = HostRuntime::default();
    let value = Rc::new(Cell::new(0));
    let task_value = Rc::clone(&value);
    let join = runtime
        .handle()
        .spawn(async move {
            task_value.set(41);
            task_value
        })
        .expect("local non-Send task is admitted");

    let returned = runtime
        .block_on(join)
        .expect("runtime drives local task")
        .expect("local task completes");

    assert!(Rc::ptr_eq(&returned, &value));
    assert_eq!(value.get(), 41);
}

#[test]
fn send_spawn_after_shutdown_resolves_runtime_stopped() {
    let mut runtime = HostRuntime::default();
    let send = runtime.send_handle();
    runtime.shutdown().expect("runtime shuts down");

    let mut join = send
        .spawn(async { 42 })
        .expect("stopped admission returns a terminal join");
    assert_eq!(join.id(), None);
    assert_eq!(
        poll_once(&mut join),
        Poll::Ready(Err(JoinError::RuntimeStopped))
    );
}

#[test]
fn abort_resolves_join_as_cancelled_and_marks_it_finished() {
    let mut runtime = HostRuntime::default();
    let mut join = runtime
        .handle()
        .spawn(pending::<()>())
        .expect("pending task is admitted");
    join.abort();

    let (result, finished) = runtime
        .block_on(async move {
            let result = (&mut join).await;
            (result, join.is_finished())
        })
        .expect("runtime applies abort");

    assert_eq!(result, Err(JoinError::Cancelled));
    assert!(finished);
}

#[test]
fn abort_after_completion_is_a_no_op() {
    let mut runtime = HostRuntime::default();
    let mut join = runtime
        .handle()
        .spawn(async { 42 })
        .expect("task is admitted");
    let abort = join.abort_handle();
    assert!(!abort.is_abort_requested());

    assert_eq!(
        runtime.block_on(&mut join).expect("runtime drives join"),
        Ok(42)
    );
    assert!(join.is_finished());
    join.abort();
    assert!(abort.is_abort_requested());
    assert!(join.is_finished());
    runtime
        .block_on(async {})
        .expect("late abort does not poison the runtime");
}

struct SelfAbort {
    abort: Rc<RefCell<Option<AbortHandle>>>,
    events: Rc<RefCell<Vec<&'static str>>>,
}

impl Future for SelfAbort {
    type Output = ();

    fn poll(self: Pin<&mut Self>, _context: &mut Context<'_>) -> Poll<Self::Output> {
        self.events.borrow_mut().push("poll-before-abort");
        self.abort
            .borrow_mut()
            .take()
            .expect("self abort handle was installed")
            .abort();
        self.events.borrow_mut().push("poll-after-abort");
        Poll::Pending
    }
}

impl Drop for SelfAbort {
    fn drop(&mut self) {
        self.events.borrow_mut().push("drop");
    }
}

#[test]
fn self_abort_is_deferred_until_the_current_poll_returns() {
    let mut runtime = HostRuntime::default();
    let abort = Rc::new(RefCell::new(None));
    let events = Rc::new(RefCell::new(Vec::new()));
    let join = runtime
        .handle()
        .spawn(SelfAbort {
            abort: Rc::clone(&abort),
            events: Rc::clone(&events),
        })
        .expect("self-aborting task is admitted");
    *abort.borrow_mut() = Some(join.abort_handle());

    assert_eq!(
        runtime.block_on(join).expect("runtime applies self-abort"),
        Err(JoinError::Cancelled)
    );
    assert_eq!(
        &*events.borrow(),
        &["poll-before-abort", "poll-after-abort", "drop"]
    );
}

#[test]
fn healthy_shutdown_is_idempotent() {
    let mut runtime = HostRuntime::default();
    let mut join = runtime
        .handle()
        .spawn(pending::<()>())
        .expect("pending task is admitted");

    runtime.shutdown().expect("first shutdown succeeds");
    runtime.shutdown().expect("second shutdown succeeds");

    assert_eq!(runtime.control().status(), HostStatus::Stopped);
    assert_eq!(
        poll_once(&mut join),
        Poll::Ready(Err(JoinError::RuntimeStopped))
    );
}

#[test]
fn control_status_transitions_without_driving() {
    let mut runtime = HostRuntime::default();
    let control = runtime.control();
    assert_eq!(control.status(), HostStatus::Running);

    control.request_stop();
    assert_eq!(control.status(), HostStatus::StopRequested);

    runtime
        .shutdown()
        .expect("requested stop tears down cleanly");
    assert_eq!(control.status(), HostStatus::Stopped);
}

#[test]
fn spawned_task_panic_resolves_join_and_sibling_keeps_running() {
    let mut runtime = HostRuntime::default();
    let handle = runtime.handle();
    let panic_join = handle
        .spawn(async { panic!("spawned task failed") })
        .expect("panicking task is admitted");
    let sibling_join = handle
        .spawn(async { 42 })
        .expect("sibling task is admitted");

    let (panic_result, sibling_result) = runtime
        .block_on(async move { (panic_join.await, sibling_join.await) })
        .expect("spawned panic is contained");

    assert!(matches!(
        panic_result,
        Err(JoinError::Panicked(ref panic)) if panic.message == "spawned task failed"
    ));
    assert_eq!(sibling_result, Ok(42));
    assert_eq!(runtime.control().status(), HostStatus::Running);
    assert_eq!(
        runtime
            .block_on(async { 7 })
            .expect("runtime remains reusable"),
        7
    );
}

#[test]
fn root_panic_is_structured_and_runtime_is_reusable() {
    let mut runtime = HostRuntime::default();
    let error = runtime
        .block_on(async {
            panic!("root task failed");
        })
        .expect_err("root panic leaves block_on as an error");

    assert!(matches!(
        error.kind,
        HostRunErrorKind::RootPanicked { ref panic }
            if panic.message == "root task failed"
    ));
    assert_eq!(runtime.control().status(), HostStatus::Running);
    assert_eq!(
        runtime
            .block_on(async { 42 })
            .expect("runtime remains reusable after root panic"),
        42
    );
}

struct PanicOnDrop;

impl Future for PanicOnDrop {
    type Output = ();

    fn poll(self: Pin<&mut Self>, _context: &mut Context<'_>) -> Poll<Self::Output> {
        Poll::Pending
    }
}

struct PanicPayloadWithPanickingDrop;

impl Drop for PanicPayloadWithPanickingDrop {
    fn drop(&mut self) {
        panic!("panic payload destructor failed");
    }
}

struct NestedPanicOnDrop;

impl Future for NestedPanicOnDrop {
    type Output = ();

    fn poll(self: Pin<&mut Self>, _context: &mut Context<'_>) -> Poll<Self::Output> {
        Poll::Pending
    }
}

impl Drop for NestedPanicOnDrop {
    fn drop(&mut self) {
        panic_any(PanicPayloadWithPanickingDrop);
    }
}

impl Drop for PanicOnDrop {
    fn drop(&mut self) {
        panic!("task destructor failed");
    }
}

#[test]
fn task_destructor_panic_is_a_checked_fatal_error() {
    let mut runtime = HostRuntime::default();
    let mut join = runtime
        .handle()
        .spawn(PanicOnDrop)
        .expect("panicking destructor task is admitted");
    let task = join.id();

    let error = runtime
        .shutdown()
        .expect_err("checked teardown reports the destructor panic");

    assert!(matches!(
        error.kind,
        HostRunErrorKind::Task(TaskFailure::DropPanicked {
            task: failed_task,
            ref panic,
        }) if failed_task == task && panic.message == "task destructor failed"
    ));
    assert_eq!(runtime.control().status(), HostStatus::Failed);
    assert_eq!(
        poll_once(&mut join),
        Poll::Ready(Err(JoinError::RuntimeStopped))
    );
}

#[test]
fn secondary_queued_destructor_and_payload_panics_cannot_escape_teardown() {
    let mut runtime = HostRuntime::default();
    let _local = runtime
        .handle()
        .spawn(PanicOnDrop)
        .expect("the first failing destructor task is admitted");
    let _queued = runtime
        .send_handle()
        .spawn(NestedPanicOnDrop)
        .expect("the portable destructor remains queued");

    let shutdown = catch_unwind(AssertUnwindSafe(|| runtime.shutdown()));
    let error = shutdown
        .expect("secondary panic payload destructor remains contained")
        .expect_err("the first destructor failure remains visible");

    assert!(matches!(
        error.kind,
        HostRunErrorKind::Task(TaskFailure::DropPanicked { .. })
    ));
}

#[test]
fn join_observer_waker_panic_is_a_checked_fatal_error() {
    let mut runtime = HostRuntime::default();
    let mut join = runtime
        .handle()
        .spawn(async { 42 })
        .expect("observed task is admitted");
    let task = join.id();
    let waker = Waker::from(Arc::new(common::PanicWake("join observer wake failed")));
    let mut context = Context::from_waker(&waker);
    assert_eq!(Pin::new(&mut join).poll(&mut context), Poll::Pending);

    let error = runtime
        .block_on(async {})
        .expect_err("the panicking observer waker is contained");

    assert!(matches!(
        error.kind,
        HostRunErrorKind::Task(TaskFailure::WakerPanicked {
            task: failed_task,
            ref panic,
        }) if failed_task == task && panic.message == "join observer wake failed"
    ));
    assert_eq!(runtime.control().status(), HostStatus::Failed);
}

struct VerifyJoinsResolvedThenPanicOnDrop {
    local: Option<JoinHandle<()>>,
    queued_send: Option<HostSendJoinHandle<()>>,
    observed: Rc<Cell<bool>>,
}

impl Future for VerifyJoinsResolvedThenPanicOnDrop {
    type Output = ();

    fn poll(self: Pin<&mut Self>, _context: &mut Context<'_>) -> Poll<Self::Output> {
        Poll::Pending
    }
}

impl Drop for VerifyJoinsResolvedThenPanicOnDrop {
    fn drop(&mut self) {
        let mut local = self
            .local
            .take()
            .expect("local join is available during teardown");
        assert_eq!(
            poll_once(&mut local),
            Poll::Ready(Err(JoinError::RuntimeStopped))
        );
        let mut queued_send = self
            .queued_send
            .take()
            .expect("queued send join is available during teardown");
        assert_eq!(
            poll_once(&mut queued_send),
            Poll::Ready(Err(JoinError::RuntimeStopped))
        );
        self.observed.set(true);
        panic!("observer destructor failed after joins resolved");
    }
}

#[test]
fn teardown_resolves_local_and_queued_joins_before_a_destructor_panics() {
    let mut runtime = HostRuntime::default();
    let handle = runtime.handle();
    let local = handle
        .spawn(pending::<()>())
        .expect("local task is admitted");
    let queued_send = runtime
        .send_handle()
        .spawn(pending::<()>())
        .expect("send task remains queued until the owner drives");
    let observed = Rc::new(Cell::new(false));
    let observer = handle
        .spawn(VerifyJoinsResolvedThenPanicOnDrop {
            local: Some(local),
            queued_send: Some(queued_send),
            observed: Rc::clone(&observed),
        })
        .expect("observer task is admitted");
    let observer_id = observer.id();

    let error = runtime
        .shutdown()
        .expect_err("checked teardown reports the observer destructor panic");

    assert!(observed.get());
    assert!(observer.is_finished());
    assert!(matches!(
        error.kind,
        HostRunErrorKind::Task(TaskFailure::DropPanicked {
            task,
            ref panic,
        }) if task == observer_id
            && panic.message == "observer destructor failed after joins resolved"
    ));
    assert_eq!(runtime.control().status(), HostStatus::Failed);
}

fn assert_fatal_error_is_retained(runtime: &mut HostRuntime, error: &kr_runtime::HostRunError) {
    assert_eq!(error.disposition(), RunErrorDisposition::Fatal);
    assert_eq!(runtime.control().status(), HostStatus::Failed);
    assert_eq!(runtime.shutdown().unwrap_err(), *error);
    assert_eq!(runtime.block_on(async {}).unwrap_err(), *error);
    assert_eq!(runtime.shutdown().unwrap_err(), *error);
}

#[test]
fn requested_stop_keeps_root_cleanup_failure_as_secondary_context() {
    let mut runtime = HostRuntime::default();
    let control = runtime.control();
    let error = runtime
        .block_on(async move {
            let _drop_guard = PanicOnDrop;
            control.request_stop();
            pending::<()>().await;
        })
        .expect_err("stop preserves the failing root destructor as context");

    assert_eq!(error.kind, HostRunErrorKind::StopRequested);
    assert!(matches!(
        error.cleanup_failure.as_deref().map(|failure| &failure.kind),
        Some(HostRunErrorKind::Task(TaskFailure::DropPanicked { panic, .. }))
            if panic.message == "task destructor failed"
    ));
    assert_fatal_error_is_retained(&mut runtime, &error);
}

#[test]
fn requested_stop_keeps_sibling_cleanup_failure_as_secondary_context() {
    let mut runtime = HostRuntime::default();
    let mut sibling = runtime
        .handle()
        .spawn(PanicOnDrop)
        .expect("sibling with a failing destructor is admitted");
    let sibling_id = sibling.id();
    let control = runtime.control();
    let error = runtime
        .block_on(async move {
            control.request_stop();
            pending::<()>().await;
        })
        .expect_err("stop remains primary after sibling teardown fails");

    assert_eq!(error.kind, HostRunErrorKind::StopRequested);
    assert!(matches!(
        error.cleanup_failure.as_deref().map(|failure| &failure.kind),
        Some(HostRunErrorKind::Task(TaskFailure::DropPanicked { task, panic }))
            if *task == sibling_id && panic.message == "task destructor failed"
    ));
    assert_eq!(
        poll_once(&mut sibling),
        Poll::Ready(Err(JoinError::RuntimeStopped))
    );
    assert_fatal_error_is_retained(&mut runtime, &error);
}

#[test]
fn stopped_runtime_keeps_rejected_root_drop_failure_in_its_error_chain() {
    use std::error::Error;

    let mut runtime = HostRuntime::default();
    runtime.shutdown().expect("runtime stops without a failure");
    let error = runtime
        .block_on(PanicOnDrop)
        .expect_err("a rejected root destructor cannot replace the stopped error");

    assert_eq!(error.kind, HostRunErrorKind::RuntimeStopped);
    let cleanup = error
        .cleanup_failure
        .as_deref()
        .expect("rejected root drop failure is retained");
    assert!(matches!(
        &cleanup.kind,
        HostRunErrorKind::RejectedRootDropPanicked { panic }
            if panic.message == "task destructor failed"
    ));
    assert_eq!(
        error
            .source()
            .unwrap()
            .downcast_ref::<kr_runtime::HostRunError>(),
        Some(cleanup)
    );
    assert!(cleanup.source().is_none());
    assert_eq!(
        error.to_string(),
        "host runtime is stopped; cleanup also failed: rejected root destructor panicked: task destructor failed"
    );
    assert_fatal_error_is_retained(&mut runtime, &error);
}

struct PanicOnPollAndDrop {
    drops: Rc<Cell<usize>>,
    stop: Option<HostControl>,
}

impl Future for PanicOnPollAndDrop {
    type Output = ();

    fn poll(self: Pin<&mut Self>, _context: &mut Context<'_>) -> Poll<Self::Output> {
        if let Some(control) = &self.stop {
            control.request_stop();
        }
        panic!("root poll failed");
    }
}

impl Drop for PanicOnPollAndDrop {
    fn drop(&mut self) {
        self.drops.set(self.drops.get() + 1);
        panic!("root drop failed");
    }
}

#[test]
fn root_poll_panic_remains_primary_when_its_destructor_also_panics() {
    let mut runtime = HostRuntime::default();
    let drops = Rc::new(Cell::new(0));
    let error = runtime
        .block_on(PanicOnPollAndDrop {
            drops: Rc::clone(&drops),
            stop: None,
        })
        .expect_err("both root failures are caught");

    assert!(matches!(
        &error.kind,
        HostRunErrorKind::RootPanicked { panic } if panic.message == "root poll failed"
    ));
    assert!(matches!(
        error.cleanup_failure.as_deref().map(|failure| &failure.kind),
        Some(HostRunErrorKind::Task(TaskFailure::DropPanicked { panic, .. }))
            if panic.message == "root drop failed"
    ));
    assert_fatal_error_is_retained(&mut runtime, &error);
    assert_eq!(drops.get(), 1);
}

#[test]
fn requested_stop_remains_terminal_when_the_same_root_poll_panics() {
    let mut runtime = HostRuntime::default();
    let control = runtime.control();
    let error = runtime
        .block_on(async move {
            control.request_stop();
            panic!("root panicked after requesting stop");
        })
        .expect_err("requested stop remains visible after the poll panics");

    assert_eq!(error.kind, HostRunErrorKind::StopRequested);
    assert!(matches!(
        error.cleanup_failure.as_deref().map(|failure| &failure.kind),
        Some(HostRunErrorKind::RootPanicked { panic })
            if panic.message == "root panicked after requesting stop"
    ));
    assert_eq!(error.disposition(), RunErrorDisposition::Terminal);
    assert_eq!(runtime.control().status(), HostStatus::Stopped);
    runtime.shutdown().expect("the stopped runtime is healthy");
    assert_eq!(
        runtime.block_on(async {}).unwrap_err().kind,
        HostRunErrorKind::RuntimeStopped
    );
}

#[test]
fn requested_stop_retains_same_poll_panic_and_fatal_root_drop_context() {
    let mut runtime = HostRuntime::default();
    let control = runtime.control();
    let drops = Rc::new(Cell::new(0));
    let error = runtime
        .block_on(PanicOnPollAndDrop {
            drops: Rc::clone(&drops),
            stop: Some(control),
        })
        .expect_err("the initiating stop and both root failures are retained");

    assert_eq!(error.kind, HostRunErrorKind::StopRequested);
    let poll_failure = error
        .cleanup_failure
        .as_deref()
        .expect("the poll failure is retained");
    assert!(matches!(
        &poll_failure.kind,
        HostRunErrorKind::RootPanicked { panic } if panic.message == "root poll failed"
    ));
    assert!(matches!(
        poll_failure.cleanup_failure.as_deref().map(|failure| &failure.kind),
        Some(HostRunErrorKind::Task(TaskFailure::DropPanicked { panic, .. }))
            if panic.message == "root drop failed"
    ));
    assert_fatal_error_is_retained(&mut runtime, &error);
    assert_eq!(drops.get(), 1);
}

#[test]
fn fatal_waker_failure_keeps_root_cleanup_failure_as_secondary_context() {
    let mut runtime = HostRuntime::default();
    let mut join = runtime
        .handle()
        .spawn(async { 42 })
        .expect("observed task is admitted before the root");
    let task = join.id();
    let waker = Waker::from(Arc::new(common::PanicWake("primary wake failed")));
    let mut context = Context::from_waker(&waker);
    assert_eq!(Pin::new(&mut join).poll(&mut context), Poll::Pending);

    let error = runtime
        .block_on(PanicOnDrop)
        .expect_err("fatal wake and root cleanup failures are both retained");

    assert!(matches!(
        &error.kind,
        HostRunErrorKind::Task(TaskFailure::WakerPanicked { task: failed_task, panic })
            if *failed_task == task && panic.message == "primary wake failed"
    ));
    assert!(matches!(
        error.cleanup_failure.as_deref().map(|failure| &failure.kind),
        Some(HostRunErrorKind::Task(TaskFailure::DropPanicked { task: root, panic }))
            if *root != task && panic.message == "task destructor failed"
    ));
    assert_eq!(poll_once(&mut join), Poll::Ready(Ok(42)));
    assert_fatal_error_is_retained(&mut runtime, &error);
}

#[test]
fn fatal_waker_failure_keeps_sibling_cleanup_failure_as_secondary_context() {
    let mut runtime = HostRuntime::default();
    let handle = runtime.handle();
    let mut join = handle
        .spawn(async { 42 })
        .expect("observed task is admitted first");
    let task = join.id();
    let mut sibling = handle
        .spawn(PanicOnDrop)
        .expect("sibling with a failing destructor is admitted");
    let sibling_id = sibling.id();
    let waker = Waker::from(Arc::new(common::PanicWake("primary wake failed")));
    let mut context = Context::from_waker(&waker);
    assert_eq!(Pin::new(&mut join).poll(&mut context), Poll::Pending);

    let error = runtime
        .block_on(async {})
        .expect_err("fatal wake and sibling cleanup failures are both retained");

    assert!(matches!(
        &error.kind,
        HostRunErrorKind::Task(TaskFailure::WakerPanicked { task: failed_task, panic })
            if *failed_task == task && panic.message == "primary wake failed"
    ));
    assert!(matches!(
        error.cleanup_failure.as_deref().map(|failure| &failure.kind),
        Some(HostRunErrorKind::Task(TaskFailure::DropPanicked { task, panic }))
            if *task == sibling_id && panic.message == "task destructor failed"
    ));
    assert_eq!(
        poll_once(&mut sibling),
        Poll::Ready(Err(JoinError::RuntimeStopped))
    );
    assert_fatal_error_is_retained(&mut runtime, &error);
}

struct OverflowIngressOnDrop {
    send: HostSendHandle,
    waker: Option<Waker>,
    queued_join: Rc<RefCell<Option<HostSendJoinHandle<()>>>>,
}

impl Future for OverflowIngressOnDrop {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        self.waker = Some(context.waker().clone());
        panic!("root poll failed before ingress overflow");
    }
}

impl Drop for OverflowIngressOnDrop {
    fn drop(&mut self) {
        let join = self
            .send
            .spawn(async {})
            .expect("send-spawn fills the one available ingress slot");
        *self.queued_join.borrow_mut() = Some(join);
        self.waker
            .take()
            .expect("root poll captured its waker")
            .wake();
    }
}

#[test]
fn root_drop_ingress_overflow_makes_the_primary_poll_error_fatal_immediately() {
    let mut runtime = HostRuntime::new(HostConfig {
        max_ingress: 1,
        ..HostConfig::default()
    })
    .expect("one ingress slot is valid");
    let send = runtime.send_handle();
    let queued_join = Rc::new(RefCell::new(None));
    let error = runtime
        .block_on(OverflowIngressOnDrop {
            send,
            waker: None,
            queued_join: Rc::clone(&queued_join),
        })
        .expect_err("cleanup ingress overflow is reported by the current drive");

    assert!(matches!(
        &error.kind,
        HostRunErrorKind::RootPanicked { panic }
            if panic.message == "root poll failed before ingress overflow"
    ));
    assert!(matches!(
        error
            .cleanup_failure
            .as_deref()
            .map(|failure| &failure.kind),
        Some(HostRunErrorKind::ResourceExhausted {
            resource: "cross-thread ingress",
            limit: 1,
        })
    ));
    let mut join = queued_join
        .borrow_mut()
        .take()
        .expect("root cleanup queued a task before overflowing ingress");
    assert_eq!(
        poll_once(&mut join),
        Poll::Ready(Err(JoinError::RuntimeStopped))
    );
    assert_fatal_error_is_retained(&mut runtime, &error);
}
