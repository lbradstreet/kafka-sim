use kr_runtime::rng::{RandomError, RandomStream};
use kr_runtime::trace::{EventKind, RecordingTrace};
use kr_runtime::{
    Handle, JoinError, PollResult, RunErrorDisposition, RunErrorKind, RunOutcome, RuntimeConfig,
    SimDuration, SimInstant, SimRuntime, Sleep, SpawnError, Step, TimeError, current_task_id,
    yield_now,
};
use std::cell::{Cell, RefCell};
use std::future::{Future, pending};
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::pin::Pin;
use std::rc::Rc;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::task::{Context, Poll, Waker};

mod common;
use common::PanicWake;

fn lock_unpoisoned<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

/// Stores each observed waker for cross-thread wake tests and stays pending.
struct StoreWaker(Arc<Mutex<Option<Waker>>>);

impl Future for StoreWaker {
    type Output = ();

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        *lock_unpoisoned(&self.0) = Some(context.waker().clone());
        Poll::Pending
    }
}

/// Stays pending forever and panics with its message when dropped.
struct PanicOnDrop(&'static str);

impl Future for PanicOnDrop {
    type Output = ();

    fn poll(self: Pin<&mut Self>, _context: &mut Context<'_>) -> Poll<Self::Output> {
        Poll::Pending
    }
}

impl Drop for PanicOnDrop {
    fn drop(&mut self) {
        std::panic::panic_any(self.0);
    }
}

/// Completes on its first poll and panics with its message when dropped.
struct ReadyThenPanicOnDrop(&'static str);

impl Future for ReadyThenPanicOnDrop {
    type Output = ();

    fn poll(self: Pin<&mut Self>, _context: &mut Context<'_>) -> Poll<Self::Output> {
        Poll::Ready(())
    }
}

impl Drop for ReadyThenPanicOnDrop {
    fn drop(&mut self) {
        std::panic::panic_any(self.0);
    }
}

/// Captures its waker on the first poll and completes on the second.
struct CaptureThenComplete {
    captured: Rc<RefCell<Option<Waker>>>,
    polled: bool,
}

impl Future for CaptureThenComplete {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        if self.polled {
            Poll::Ready(())
        } else {
            self.polled = true;
            self.captured.borrow_mut().replace(context.waker().clone());
            Poll::Pending
        }
    }
}

/// Polls its inner sleep with a waker that panics when the timer fires.
struct PollSleepWithPanickingWaker {
    sleep: Sleep,
}

impl Future for PollSleepWithPanickingWaker {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, _context: &mut Context<'_>) -> Poll<Self::Output> {
        let waker = Waker::from(Arc::new(PanicWake("waiter wake failure")));
        let mut context = Context::from_waker(&waker);
        Pin::new(&mut self.sleep).poll(&mut context).map(|result| {
            result.unwrap();
        })
    }
}

#[test]
fn tasks_run_in_fifo_spawn_order() {
    let mut runtime = SimRuntime::default();
    let handle = runtime.handle();
    let log = Rc::new(RefCell::new(Vec::new()));

    for label in ["first", "second", "third", "fourth"] {
        let log = Rc::clone(&log);
        handle
            .spawn(async move { log.borrow_mut().push(label) })
            .unwrap();
    }

    assert!(matches!(
        runtime.run_until_stalled().unwrap(),
        RunOutcome::Idle(_)
    ));
    assert_eq!(&*log.borrow(), &["first", "second", "third", "fourth"]);
}

#[test]
fn yield_requeues_at_the_back_without_advancing_time() {
    let mut runtime = SimRuntime::default();
    let handle = runtime.handle();
    let log = Rc::new(RefCell::new(Vec::new()));

    let first_log = Rc::clone(&log);
    handle
        .spawn(async move {
            first_log.borrow_mut().push("a1");
            yield_now().await;
            first_log.borrow_mut().push("a2");
        })
        .unwrap();
    let second_log = Rc::clone(&log);
    handle
        .spawn(async move { second_log.borrow_mut().push("b") })
        .unwrap();

    runtime.run_until_stalled().unwrap();
    assert_eq!(&*log.borrow(), &["a1", "b", "a2"]);
    assert_eq!(runtime.snapshot().now, SimInstant::ZERO);
}

#[test]
fn timers_jump_time_and_promote_equal_deadlines_in_registration_order() {
    let mut runtime = SimRuntime::default();
    let handle = runtime.handle();
    let log = Rc::new(RefCell::new(Vec::new()));
    let deadline = SimInstant::from_nanos(10);

    let first_handle = handle.clone();
    let first_log = Rc::clone(&log);
    handle
        .spawn(async move {
            first_handle.sleep_until(deadline).await.unwrap();
            first_log.borrow_mut().push("first");
        })
        .unwrap();
    let second_handle = handle.clone();
    let second_log = Rc::clone(&log);
    handle
        .spawn(async move {
            second_handle.sleep_until(deadline).await.unwrap();
            second_log.borrow_mut().push("second");
        })
        .unwrap();

    assert!(matches!(runtime.step().unwrap(), Step::TaskPolled { .. }));
    assert!(matches!(runtime.step().unwrap(), Step::TaskPolled { .. }));
    let Step::TimeAdvanced { from, to, timers } = runtime.step().unwrap() else {
        panic!("expected a virtual-time jump");
    };
    assert_eq!(from, SimInstant::ZERO);
    assert_eq!(to, deadline);
    assert_eq!(timers.len(), 2);

    runtime.run_until_stalled().unwrap();
    assert_eq!(&*log.borrow(), &["first", "second"]);
    assert_eq!(runtime.snapshot().now, deadline);
}

#[test]
fn ready_work_always_runs_before_future_time() {
    let mut runtime = SimRuntime::default();
    let handle = runtime.handle();
    let log = Rc::new(RefCell::new(Vec::new()));

    let sleeper_handle = handle.clone();
    let sleeper_log = Rc::clone(&log);
    handle
        .spawn(async move {
            sleeper_handle
                .sleep(SimDuration::from_nanos(5))
                .await
                .unwrap();
            sleeper_log.borrow_mut().push("timer");
        })
        .unwrap();
    let ready_log = Rc::clone(&log);
    handle
        .spawn(async move {
            ready_log.borrow_mut().push("ready-1");
            yield_now().await;
            ready_log.borrow_mut().push("ready-2");
        })
        .unwrap();

    runtime.run_until_stalled().unwrap();
    assert_eq!(&*log.borrow(), &["ready-1", "ready-2", "timer"]);
}

#[test]
fn duplicate_and_self_wakes_coalesce() {
    struct WakeMany {
        polls: Rc<Cell<usize>>,
    }

    impl Future for WakeMany {
        type Output = ();

        fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
            let polls = self.polls.get() + 1;
            self.polls.set(polls);
            if polls == 1 {
                for _ in 0..10 {
                    context.waker().wake_by_ref();
                }
                Poll::Pending
            } else {
                Poll::Ready(())
            }
        }
    }

    let mut runtime = SimRuntime::default();
    let polls = Rc::new(Cell::new(0));
    runtime
        .handle()
        .spawn(WakeMany {
            polls: Rc::clone(&polls),
        })
        .unwrap();
    runtime.run_until_stalled().unwrap();
    assert_eq!(polls.get(), 2);
}

#[test]
fn duplicate_wake_preserves_a_tasks_fifo_position() {
    let mut runtime = SimRuntime::default();
    let handle = runtime.handle();
    let captured = Rc::new(RefCell::new(None));
    let first = handle
        .spawn(CaptureThenComplete {
            captured: Rc::clone(&captured),
            polled: false,
        })
        .expect("first task spawns");
    assert!(matches!(
        runtime.step().expect("first task reaches Pending"),
        Step::TaskPolled {
            task,
            result: PollResult::Pending,
        } if task == first.id()
    ));

    captured
        .borrow()
        .as_ref()
        .expect("first task captured its waker")
        .wake_by_ref();
    let second = handle.spawn(async {}).expect("second task spawns");
    captured
        .borrow()
        .as_ref()
        .expect("first task retained its pending wake")
        .wake_by_ref();

    assert_eq!(
        runtime.step().expect("original FIFO leader runs"),
        Step::TaskPolled {
            task: first.id(),
            result: PollResult::Ready,
        }
    );
    assert_eq!(
        runtime.step().expect("FIFO follower runs second"),
        Step::TaskPolled {
            task: second.id(),
            result: PollResult::Ready,
        }
    );
}

#[test]
fn captured_wakers_are_admitted_in_wake_order() {
    let mut runtime = SimRuntime::default();
    let handle = runtime.handle();
    let first_waker = Rc::new(RefCell::new(None));
    let second_waker = Rc::new(RefCell::new(None));
    let first = handle
        .spawn(CaptureThenComplete {
            captured: Rc::clone(&first_waker),
            polled: false,
        })
        .unwrap();
    let second = handle
        .spawn(CaptureThenComplete {
            captured: Rc::clone(&second_waker),
            polled: false,
        })
        .unwrap();
    assert!(matches!(runtime.step().unwrap(), Step::TaskPolled { .. }));
    assert!(matches!(runtime.step().unwrap(), Step::TaskPolled { .. }));

    second_waker.borrow().as_ref().unwrap().wake_by_ref();
    first_waker.borrow().as_ref().unwrap().wake_by_ref();

    assert_eq!(
        runtime.step().unwrap(),
        Step::TaskPolled {
            task: second.id(),
            result: PollResult::Ready,
        }
    );
    assert_eq!(
        runtime.step().unwrap(),
        Step::TaskPolled {
            task: first.id(),
            result: PollResult::Ready,
        }
    );
}

#[test]
fn stale_waker_cannot_wake_a_reused_task_slot() {
    struct CaptureWaker {
        destination: Rc<RefCell<Option<Waker>>>,
    }

    impl Future for CaptureWaker {
        type Output = ();

        fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
            *self.destination.borrow_mut() = Some(context.waker().clone());
            Poll::Ready(())
        }
    }

    let mut runtime = SimRuntime::default();
    let captured = Rc::new(RefCell::new(None));
    let first = runtime
        .handle()
        .spawn(CaptureWaker {
            destination: Rc::clone(&captured),
        })
        .unwrap();
    let first_id = first.id();
    runtime.run_until_stalled().unwrap();

    let polls = Rc::new(Cell::new(0));
    let task_polls = Rc::clone(&polls);
    let second = runtime
        .handle()
        .spawn(async move { task_polls.set(task_polls.get() + 1) })
        .unwrap();
    assert_eq!(first_id.slot(), second.id().slot());
    assert_ne!(first_id.generation(), second.id().generation());

    captured.borrow().as_ref().unwrap().wake_by_ref();
    runtime.run_until_stalled().unwrap();
    assert_eq!(polls.get(), 1);
}

#[test]
fn abort_drops_once_on_the_runtime_thread_and_wakes_joiner() {
    struct DropCount(Rc<Cell<usize>>);
    impl Drop for DropCount {
        fn drop(&mut self) {
            self.0.set(self.0.get() + 1);
            assert!(current_task_id().is_some());
        }
    }

    let mut runtime = SimRuntime::default();
    let drops = Rc::new(Cell::new(0));
    let guard = DropCount(Rc::clone(&drops));
    let task = runtime
        .handle()
        .spawn(async move {
            let _guard = guard;
            pending::<()>().await;
        })
        .unwrap();
    task.abort();
    task.abort();

    let joined = runtime.block_on(task).unwrap();
    assert_eq!(joined, Err(JoinError::Cancelled));
    assert_eq!(drops.get(), 1);
    assert!(runtime.snapshot().tasks.is_empty());
}

#[test]
fn completion_wins_when_the_running_task_aborts_itself() {
    let mut runtime = SimRuntime::default();
    let abort = Rc::new(RefCell::new(None::<kr_runtime::AbortHandle>));
    let task_abort = Rc::clone(&abort);
    let task = runtime
        .handle()
        .spawn(async move {
            task_abort
                .borrow()
                .as_ref()
                .expect("abort handle is installed before polling")
                .abort();
            42
        })
        .expect("task spawns");
    abort.borrow_mut().replace(task.abort_handle());

    assert_eq!(runtime.block_on(task).expect("runtime completes"), Ok(42));
    assert!(runtime.snapshot().tasks.is_empty());
}

#[test]
fn aborting_a_sleeper_removes_its_timer_without_a_ghost_jump() {
    let mut runtime = SimRuntime::default();
    let handle = runtime.handle();
    let sleep_handle = handle.clone();
    let task = handle
        .spawn(async move {
            sleep_handle
                .sleep(SimDuration::from_nanos(1_000))
                .await
                .unwrap();
        })
        .unwrap();

    assert_eq!(
        runtime.step().unwrap(),
        Step::TaskPolled {
            task: task.id(),
            result: PollResult::Pending,
        }
    );
    assert_eq!(runtime.snapshot().live_timers, 1);
    task.abort();
    assert!(matches!(
        runtime.run_until_stalled().unwrap(),
        RunOutcome::Idle(_)
    ));
    assert_eq!(runtime.snapshot().live_timers, 0);
    assert_eq!(runtime.snapshot().now, SimInstant::ZERO);
}

#[test]
fn bulk_ready_task_cancellation_is_bounded_by_scheduler_steps() {
    const TASKS: usize = 4_096;

    let mut runtime = SimRuntime::default();
    let handle = runtime.handle();
    let mut joins = Vec::with_capacity(TASKS);
    for _ in 0..TASKS {
        joins.push(handle.spawn(pending::<()>()).unwrap());
    }

    for join in &joins {
        join.abort();
    }
    assert!(matches!(
        runtime.run_until_stalled().unwrap(),
        RunOutcome::Idle(_)
    ));
    let snapshot = runtime.snapshot();
    assert_eq!(snapshot.ready_tasks, 0);
    assert_eq!(snapshot.total_steps, TASKS as u64);
    assert!(snapshot.tasks.is_empty());

    let mut task_context = Context::from_waker(Waker::noop());
    for mut join in joins {
        assert_eq!(
            Pin::new(&mut join).poll(&mut task_context),
            Poll::Ready(Err(JoinError::Cancelled))
        );
    }
}

#[test]
fn bulk_sleep_cancellation_reclaims_timer_capacity_without_advancing_time() {
    const TASKS: usize = 2_048;

    let mut runtime = SimRuntime::new(RuntimeConfig {
        max_timers: TASKS,
        ..RuntimeConfig::default()
    });
    let handle = runtime.handle();
    let mut joins = Vec::with_capacity(TASKS);
    for _ in 0..TASKS {
        let timer_handle = handle.clone();
        joins.push(
            handle
                .spawn(async move {
                    timer_handle
                        .sleep(SimDuration::from_nanos(1_000_000))
                        .await
                        .unwrap();
                })
                .unwrap(),
        );
    }
    for _ in 0..TASKS {
        assert!(matches!(
            runtime.step().unwrap(),
            Step::TaskPolled {
                result: PollResult::Pending,
                ..
            }
        ));
    }
    assert_eq!(runtime.snapshot().live_timers, TASKS);

    for join in &joins {
        join.abort();
    }
    assert!(matches!(
        runtime.run_until_stalled().unwrap(),
        RunOutcome::Idle(_)
    ));
    assert_eq!(runtime.snapshot().live_timers, 0);
    assert_eq!(runtime.snapshot().now, SimInstant::ZERO);

    let replacement_handle = handle.clone();
    let replacement = handle
        .spawn(async move {
            replacement_handle
                .sleep(SimDuration::from_nanos(1_000_000))
                .await
                .unwrap();
        })
        .unwrap();
    assert!(matches!(
        runtime.step().unwrap(),
        Step::TaskPolled {
            result: PollResult::Pending,
            ..
        }
    ));
    assert_eq!(runtime.snapshot().live_timers, 1);
    replacement.abort();
    assert_eq!(
        runtime.step().unwrap(),
        Step::TaskCancelled {
            task: replacement.id()
        }
    );
    assert_eq!(runtime.step().unwrap(), Step::Idle);

    let mut task_context = Context::from_waker(Waker::noop());
    for mut join in joins {
        assert_eq!(
            Pin::new(&mut join).poll(&mut task_context),
            Poll::Ready(Err(JoinError::Cancelled))
        );
    }
}

#[test]
fn canceled_timer_keys_do_not_disturb_survivor_deadlines_or_order() {
    let mut runtime = SimRuntime::default();
    let handle = runtime.handle();
    let completed = Rc::new(RefCell::new(Vec::new()));

    let early_handle = handle.clone();
    let early = handle
        .spawn(async move {
            early_handle
                .sleep(SimDuration::from_nanos(5))
                .await
                .unwrap();
        })
        .unwrap();
    let first_handle = handle.clone();
    let first_completed = Rc::clone(&completed);
    let _first = handle
        .spawn(async move {
            first_handle
                .sleep(SimDuration::from_nanos(10))
                .await
                .unwrap();
            first_completed.borrow_mut().push("first");
        })
        .unwrap();
    let late_handle = handle.clone();
    let late = handle
        .spawn(async move {
            late_handle
                .sleep(SimDuration::from_nanos(20))
                .await
                .unwrap();
        })
        .unwrap();
    let second_handle = handle.clone();
    let second_completed = Rc::clone(&completed);
    let _second = handle
        .spawn(async move {
            second_handle
                .sleep(SimDuration::from_nanos(10))
                .await
                .unwrap();
            second_completed.borrow_mut().push("second");
        })
        .unwrap();

    for _ in 0..4 {
        assert!(matches!(runtime.step().unwrap(), Step::TaskPolled { .. }));
    }
    late.abort();
    early.abort();

    assert_eq!(
        runtime.step().unwrap(),
        Step::TaskCancelled { task: late.id() }
    );
    assert_eq!(
        runtime.step().unwrap(),
        Step::TaskCancelled { task: early.id() }
    );

    let Step::TimeAdvanced { to, timers, .. } = runtime.step().unwrap() else {
        panic!("expected surviving timers to advance time");
    };
    assert_eq!(to, SimInstant::from_nanos(10));
    assert_eq!(
        timers.iter().map(|timer| timer.get()).collect::<Vec<_>>(),
        vec![1, 3]
    );

    assert!(matches!(
        runtime.run_until_stalled().unwrap(),
        RunOutcome::Idle(_)
    ));
    assert_eq!(&*completed.borrow(), &["first", "second"]);
    assert_eq!(runtime.snapshot().now, SimInstant::from_nanos(10));
}

#[test]
fn pending_without_an_event_is_a_diagnosable_stall() {
    let mut runtime = SimRuntime::default();
    let error = runtime.block_on(pending::<()>()).unwrap_err();
    assert_eq!(error.kind, RunErrorKind::Stalled);
    assert_eq!(error.snapshot.tasks.len(), 1);
    assert_eq!(
        error.snapshot.tasks[0].state,
        kr_runtime::TaskState::Waiting
    );
    assert!(runtime.snapshot().tasks.is_empty());
}

#[test]
fn an_always_ready_task_hits_the_step_budget() {
    let mut runtime = SimRuntime::new(RuntimeConfig {
        max_steps_per_run: 8,
        ..RuntimeConfig::default()
    });
    let error = runtime
        .block_on(async {
            loop {
                yield_now().await;
            }
        })
        .unwrap_err();
    assert_eq!(error.kind, RunErrorKind::StepBudgetExceeded { limit: 8 });
    assert_eq!(error.snapshot.now, SimInstant::ZERO);
    assert!(runtime.snapshot().tasks.is_empty());
    assert_eq!(runtime.snapshot().live_timers, 0);
}

#[test]
fn run_outcomes_at_the_step_budget_boundary_are_successful() {
    let mut idle = SimRuntime::new(RuntimeConfig {
        max_steps_per_run: 1,
        ..RuntimeConfig::default()
    });
    idle.handle().spawn(async {}).unwrap();
    assert!(matches!(
        idle.run_until_stalled().unwrap(),
        RunOutcome::Idle(_)
    ));

    let mut self_cancelled = SimRuntime::new(RuntimeConfig {
        max_steps_per_run: 1,
        ..RuntimeConfig::default()
    });
    let abort = Rc::new(RefCell::new(None::<kr_runtime::AbortHandle>));
    let task_abort = Rc::clone(&abort);
    let task = self_cancelled
        .handle()
        .spawn(async move {
            task_abort.borrow().as_ref().unwrap().abort();
        })
        .unwrap();
    abort.borrow_mut().replace(task.abort_handle());
    assert!(matches!(
        self_cancelled.run_until_stalled().unwrap(),
        RunOutcome::Idle(_)
    ));

    let mut stalled = SimRuntime::new(RuntimeConfig {
        max_steps_per_run: 1,
        ..RuntimeConfig::default()
    });
    let task = stalled.handle().spawn(pending::<()>()).unwrap();
    assert!(matches!(
        stalled.run_until_stalled().unwrap(),
        RunOutcome::Stalled(_)
    ));
    task.abort();
    assert_eq!(
        stalled.step().unwrap(),
        Step::TaskCancelled { task: task.id() }
    );
    assert_eq!(stalled.step().unwrap(), Step::Idle);
}

#[test]
fn block_on_scopes_borrowed_roots_outputs_and_wakes() {
    fn through_runtime<'a>(
        runtime: &mut SimRuntime,
        value: &'a str,
    ) -> Result<&'a str, kr_runtime::RunError> {
        runtime.block_on(async move { value })
    }

    let mut runtime = SimRuntime::default();
    let text = String::from("borrowed output");
    assert_eq!(through_runtime(&mut runtime, text.as_str()).unwrap(), text);

    let mut observed = 0;
    runtime
        .block_on(async {
            yield_now().await;
            observed = 7;
        })
        .expect("borrowed pending root wakes and completes");
    assert_eq!(observed, 7);
    assert!(runtime.snapshot().tasks.is_empty());
    assert_eq!(runtime.step().unwrap(), Step::Idle);
}

#[test]
fn panic_is_a_structured_fatal_outcome() {
    let mut runtime = SimRuntime::default();
    let error = runtime
        .block_on(async { panic!("simulated failure") })
        .unwrap_err();
    let RunErrorKind::TaskPanicked { panic, .. } = &error.kind else {
        panic!("unexpected error: {error:?}");
    };
    assert_eq!(panic.message, "simulated failure");
    assert!(error.snapshot.tasks.is_empty());
    assert!(
        !error.snapshot.stopped,
        "snapshot captures the failure point"
    );
    assert!(
        runtime.snapshot().stopped,
        "fatal failure tears down runtime"
    );

    assert_eq!(runtime.run_until_stalled().unwrap_err(), error);
    assert_eq!(runtime.finish().unwrap_err(), error);
}

#[test]
fn every_driver_returns_a_latched_fatal_with_a_zero_step_budget() {
    let mut runtime = SimRuntime::new(RuntimeConfig {
        max_steps_per_run: 0,
        ..RuntimeConfig::default()
    });
    runtime
        .handle()
        .spawn(PanicOnDrop("fatal teardown failure"))
        .unwrap();

    let error = runtime.shutdown().unwrap_err();
    assert!(matches!(
        &error.kind,
        RunErrorKind::TaskDropPanicked { panic, .. }
            if panic.message == "fatal teardown failure"
    ));

    assert_eq!(runtime.step().unwrap_err(), error);
    assert_eq!(runtime.run_until_stalled().unwrap_err(), error);
    assert_eq!(runtime.block_on(async {}).unwrap_err(), error);
    assert_eq!(runtime.shutdown().unwrap_err(), error);
    assert_eq!(runtime.finish().unwrap_err(), error);
}

#[test]
fn fatal_error_from_a_cancellation_step_is_latched() {
    let mut runtime = SimRuntime::new(RuntimeConfig {
        max_steps_per_run: 1,
        ..RuntimeConfig::default()
    });
    let task = runtime
        .handle()
        .spawn(PanicOnDrop("aborted task destructor failure"))
        .unwrap();
    task.abort();

    let error = runtime.run_until_stalled().unwrap_err();
    assert!(matches!(
        &error.kind,
        RunErrorKind::TaskDropPanicked { panic, .. }
            if panic.message == "aborted task destructor failure"
    ));
    assert!(
        !error.snapshot.stopped,
        "snapshot stays at the failure point"
    );
    assert!(
        runtime.snapshot().stopped,
        "fatal failure tears down runtime"
    );
    assert_eq!(runtime.step().unwrap_err(), error);
}

#[test]
fn task_panic_payload_destructor_cannot_escape_block_on() {
    struct PanicPayload;

    impl Drop for PanicPayload {
        fn drop(&mut self) {
            panic!("panic payload destructor escaped");
        }
    }

    let mut runtime = SimRuntime::default();
    let polled = Cell::new(false);
    let boundary = catch_unwind(AssertUnwindSafe(|| {
        runtime.block_on(async {
            polled.set(true);
            std::panic::panic_any(PanicPayload)
        })
    }));
    let error = boundary
        .expect("block_on contains destruction of the caught panic payload")
        .unwrap_err();

    assert!(matches!(
        &error.kind,
        RunErrorKind::TaskPanicked { panic, .. }
            if panic.message == "non-string panic payload"
    ));
    assert!(polled.get());
    assert!(runtime.snapshot().tasks.is_empty());
    assert_eq!(runtime.finish().unwrap_err(), error);
}

#[test]
fn panicking_root_preserves_a_secondary_future_destructor_failure() {
    struct PanicInPollAndDrop<'a> {
        drops: &'a Cell<usize>,
    }

    impl Future for PanicInPollAndDrop<'_> {
        type Output = ();

        fn poll(self: Pin<&mut Self>, _context: &mut Context<'_>) -> Poll<Self::Output> {
            panic!("root poll failure");
        }
    }

    impl Drop for PanicInPollAndDrop<'_> {
        fn drop(&mut self) {
            self.drops.set(self.drops.get() + 1);
            panic!("root destructor failure");
        }
    }

    let drops = Cell::new(0);
    let mut runtime = SimRuntime::default();
    let error = runtime
        .block_on(PanicInPollAndDrop { drops: &drops })
        .unwrap_err();

    assert!(matches!(
        &error.kind,
        RunErrorKind::TaskPanicked { panic, .. } if panic.message == "root poll failure"
    ));
    assert!(error.snapshot.tasks.is_empty());
    assert!(
        !error.snapshot.stopped,
        "primary snapshot precedes teardown"
    );
    let cleanup = error
        .cleanup_failure
        .as_deref()
        .expect("root destructor failure remains attached");
    assert!(matches!(
        &cleanup.kind,
        RunErrorKind::TaskDropPanicked { panic, .. }
            if panic.message == "root destructor failure"
    ));
    assert_eq!(drops.get(), 1);
    assert_eq!(error.disposition(), RunErrorDisposition::Fatal);
    assert_eq!(runtime.finish().unwrap_err(), error);
}

#[test]
fn runtime_error_dispositions_are_explicit() {
    assert_eq!(
        RunErrorKind::Stalled.disposition(),
        RunErrorDisposition::Resumable
    );
    assert_eq!(
        RunErrorKind::StepBudgetExceeded { limit: 1 }.disposition(),
        RunErrorDisposition::Resumable
    );
    assert_eq!(
        RunErrorKind::RuntimeStopped.disposition(),
        RunErrorDisposition::Terminal
    );
    assert_eq!(
        RunErrorKind::RootSpawnFailed(SpawnError::ResourceExhausted {
            resource: "live tasks",
            limit: 0,
        })
        .disposition(),
        RunErrorDisposition::Resumable
    );
    assert_eq!(
        RunErrorKind::RootSpawnFailed(SpawnError::RuntimeStopped).disposition(),
        RunErrorDisposition::Terminal
    );
    assert_eq!(
        RunErrorKind::RootSpawnFailed(SpawnError::IdentifierExhausted).disposition(),
        RunErrorDisposition::Fatal
    );
}

#[test]
fn root_task_limit_failure_is_resumable() {
    let mut runtime = SimRuntime::new(RuntimeConfig {
        max_tasks: 1,
        ..RuntimeConfig::default()
    });
    let task = runtime.handle().spawn(pending::<()>()).unwrap();

    let error = runtime.block_on(async {}).unwrap_err();
    assert_eq!(
        error.kind,
        RunErrorKind::RootSpawnFailed(SpawnError::ResourceExhausted {
            resource: "live tasks",
            limit: 1,
        })
    );
    assert_eq!(error.disposition(), RunErrorDisposition::Resumable);
    assert!(!runtime.snapshot().stopped);

    task.abort();
    assert!(matches!(
        runtime.run_until_stalled().unwrap(),
        RunOutcome::Idle(_)
    ));
    runtime
        .block_on(async {})
        .expect("runtime accepts a later root");
}

#[test]
fn cross_thread_wake_is_rejected_as_nondeterministic_input() {
    let mut runtime = SimRuntime::default();
    let stored = Arc::new(Mutex::new(None));
    let task = runtime
        .handle()
        .spawn(StoreWaker(Arc::clone(&stored)))
        .unwrap();
    runtime.step().unwrap();
    let waker = lock_unpoisoned(&stored).take().unwrap();
    std::thread::spawn(move || waker.wake()).join().unwrap();

    let error = runtime.step().unwrap_err();
    assert_eq!(
        error.kind,
        RunErrorKind::NondeterministicExternalWake { task: task.id() }
    );
}

#[test]
fn zero_sleep_is_immediately_ready_and_spawn_overflow_is_typed() {
    let mut runtime = SimRuntime::default();
    let handle = runtime.handle();
    let zero_handle = handle.clone();
    runtime
        .block_on(async move { zero_handle.sleep(SimDuration::ZERO).await })
        .unwrap()
        .unwrap();
    assert_eq!(runtime.snapshot().now, SimInstant::ZERO);

    let mut runtime = SimRuntime::default();
    let handle = runtime.handle();
    let max_handle = handle.clone();
    let result = runtime
        .block_on(async move {
            max_handle.sleep_until(SimInstant::MAX).await?;
            max_handle.sleep(SimDuration::from_nanos(1)).await
        })
        .unwrap();
    assert_eq!(result, Err(kr_runtime::TimeError::DeadlineOverflow));
}

#[test]
fn sleep_terminal_state_wins_over_later_runtime_shutdown() {
    let mut runtime = SimRuntime::default();
    let handle = runtime.handle();
    let mut elapsed = Box::pin(handle.sleep(SimDuration::ZERO));
    let mut pending = Box::pin(handle.sleep(SimDuration::from_nanos(1)));
    runtime.shutdown().expect("shutdown succeeds");
    let mut context = Context::from_waker(Waker::noop());

    assert_eq!(elapsed.as_mut().poll(&mut context), Poll::Ready(Ok(())));
    assert_eq!(
        pending.as_mut().poll(&mut context),
        Poll::Ready(Err(TimeError::RuntimeStopped))
    );
}

#[test]
fn shutdown_is_distinct_from_ordinary_idle_quiescence() {
    let mut runtime = SimRuntime::default();

    assert_eq!(
        runtime.step().expect("empty runtime is observable"),
        Step::Idle
    );
    let RunOutcome::Idle(idle) = runtime.run_until_stalled().expect("empty runtime quiesces")
    else {
        panic!("ordinary quiescence must remain idle");
    };
    assert!(!idle.stopped);

    runtime.shutdown().expect("shutdown succeeds");
    assert_eq!(
        runtime.step().expect("stopped runtime is observable"),
        Step::Stopped
    );
    let RunOutcome::Stopped(stopped) = runtime
        .run_until_stalled()
        .expect("stopped runtime has a terminal outcome")
    else {
        panic!("shutdown must not be reported as idle");
    };
    assert!(stopped.stopped);
    assert!(stopped.tasks.is_empty());
    assert_eq!(stopped.live_timers, 0);

    let mut zero_budget = SimRuntime::new(RuntimeConfig {
        max_steps_per_run: 0,
        ..RuntimeConfig::default()
    });
    zero_budget
        .shutdown()
        .expect("zero-budget shutdown succeeds");
    assert!(matches!(
        zero_budget
            .run_until_stalled()
            .expect("terminal observation does not require a scheduler step"),
        RunOutcome::Stopped(snapshot) if snapshot.stopped
    ));
}

#[test]
fn shutdown_seals_random_sources_without_mutating_terminal_artifacts() {
    let trace = Rc::new(RecordingTrace::new(32));
    let mut runtime = SimRuntime::with_trace(RuntimeConfig::default(), trace.clone());
    let workload = runtime.handle();
    let faults = runtime.random_source(RandomStream::Fault);
    workload.random_u64().expect("runtime is active");
    faults.random_u64().expect("runtime is active");
    runtime.shutdown().expect("clean shutdown succeeds");

    let checkpoint = runtime.snapshot().determinism_checkpoint();
    let event_count = trace.len();
    let fingerprint = trace.fingerprint();

    assert_eq!(workload.random_u64(), Err(RandomError::RuntimeStopped));
    assert_eq!(workload.random_below(1), Err(RandomError::RuntimeStopped));
    assert_eq!(
        faults.random_bool_ratio(1, 2),
        Err(RandomError::RuntimeStopped)
    );
    assert_eq!(
        workload.random_below(0),
        Err(RandomError::ZeroUpperBound),
        "argument validation takes precedence"
    );
    assert_eq!(
        faults.random_bool_ratio(1, 0),
        Err(RandomError::InvalidRatio {
            numerator: 1,
            denominator: 0,
        }),
        "argument validation takes precedence"
    );
    assert_eq!(runtime.snapshot().determinism_checkpoint(), checkpoint);
    assert_eq!(trace.len(), event_count);
    assert_eq!(trace.fingerprint(), fingerprint);
}

#[test]
fn rejected_spawn_drops_the_future_without_borrowing_runtime_state() {
    struct InspectRuntimeOnDrop(Handle);

    impl Future for InspectRuntimeOnDrop {
        type Output = ();

        fn poll(self: Pin<&mut Self>, _context: &mut Context<'_>) -> Poll<Self::Output> {
            Poll::Pending
        }
    }

    impl Drop for InspectRuntimeOnDrop {
        fn drop(&mut self) {
            let _ = self.0.snapshot();
        }
    }

    let mut runtime = SimRuntime::new(RuntimeConfig {
        max_tasks: 1,
        ..RuntimeConfig::default()
    });
    let handle = runtime.handle();
    let first = handle.spawn(pending::<()>()).unwrap();
    let rejected = handle.spawn(InspectRuntimeOnDrop(handle.clone()));

    assert!(matches!(
        rejected,
        Err(SpawnError::ResourceExhausted {
            resource: "live tasks",
            limit: 1,
        })
    ));
    first.abort();
    assert!(matches!(
        runtime.run_until_stalled().unwrap(),
        RunOutcome::Idle(_)
    ));
}

#[test]
fn wake_coalescing_handles_large_wake_storms() {
    struct WakeStorm(bool);

    impl Future for WakeStorm {
        type Output = ();

        fn poll(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
            if self.0 {
                Poll::Ready(())
            } else {
                self.0 = true;
                for _ in 0..10_000 {
                    context.waker().wake_by_ref();
                }
                Poll::Pending
            }
        }
    }

    let mut runtime = SimRuntime::default();
    runtime.handle().spawn(WakeStorm(false)).unwrap();
    assert!(matches!(
        runtime.run_until_stalled().unwrap(),
        RunOutcome::Idle(_)
    ));
}

#[test]
fn duplicate_abort_requests_share_the_owner_local_latch() {
    let mut runtime = SimRuntime::default();
    let handle = runtime.handle();
    let mut join = handle.spawn(pending::<()>()).unwrap();
    let abort = join.abort_handle();

    assert!(!abort.is_abort_requested());
    assert!(matches!(
        runtime.step().unwrap(),
        Step::TaskPolled {
            result: PollResult::Pending,
            ..
        }
    ));

    abort.abort();
    assert!(abort.is_abort_requested());

    // Duplicate requests observe the same per-task latch and do not add steps.
    abort.abort();

    assert_eq!(
        runtime.step().unwrap(),
        Step::TaskCancelled { task: join.id() }
    );
    assert_eq!(runtime.step().unwrap(), Step::Idle);
    let mut task_context = Context::from_waker(Waker::noop());
    assert_eq!(
        Pin::new(&mut join).poll(&mut task_context),
        Poll::Ready(Err(JoinError::Cancelled))
    );
}

#[test]
fn abort_request_latch_records_intent_after_task_completion() {
    let mut runtime = SimRuntime::default();
    let join = runtime.handle().spawn(async { 42 }).unwrap();
    let abort = join.abort_handle();

    assert_eq!(runtime.block_on(join).unwrap(), Ok(42));
    assert!(!abort.is_abort_requested());

    abort.abort();

    assert!(abort.is_abort_requested());
    assert_eq!(runtime.step().unwrap(), Step::Idle);
}

#[test]
fn cancellation_steps_bound_destructors_that_replenish_the_queue() {
    struct RespawnOnDrop {
        handle: Handle,
        remaining: Rc<Cell<usize>>,
    }

    impl Future for RespawnOnDrop {
        type Output = ();

        fn poll(self: Pin<&mut Self>, _context: &mut Context<'_>) -> Poll<Self::Output> {
            Poll::Pending
        }
    }

    impl Drop for RespawnOnDrop {
        fn drop(&mut self) {
            let remaining = self.remaining.get();
            if remaining == 0 {
                return;
            }
            self.remaining.set(remaining - 1);
            if let Ok(successor) = self.handle.spawn(Self {
                handle: self.handle.clone(),
                remaining: Rc::clone(&self.remaining),
            }) {
                successor.abort();
            }
        }
    }

    let mut runtime = SimRuntime::new(RuntimeConfig {
        max_steps_per_run: 4,
        ..RuntimeConfig::default()
    });
    let remaining = Rc::new(Cell::new(12));
    let task = runtime
        .handle()
        .spawn(RespawnOnDrop {
            handle: runtime.handle(),
            remaining: Rc::clone(&remaining),
        })
        .unwrap();
    task.abort();

    let error = runtime.run_until_stalled().unwrap_err();

    assert_eq!(error.kind, RunErrorKind::StepBudgetExceeded { limit: 4 });
    assert_eq!(remaining.get(), 8);

    remaining.set(0);
    assert!(matches!(
        runtime.run_until_stalled().unwrap(),
        RunOutcome::Idle(_)
    ));
    runtime.handle().spawn(async {}).unwrap();
    assert!(matches!(
        runtime.run_until_stalled().unwrap(),
        RunOutcome::Idle(_)
    ));
}

#[test]
fn block_on_reports_a_foreign_wake_from_the_final_root_poll() {
    struct ForeignWakeThenReady;

    impl Future for ForeignWakeThenReady {
        type Output = ();

        fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
            let waker = context.waker().clone();
            std::thread::spawn(move || waker.wake()).join().unwrap();
            Poll::Ready(())
        }
    }

    let mut runtime = SimRuntime::default();

    let error = runtime.block_on(ForeignWakeThenReady).unwrap_err();

    assert!(matches!(
        error.kind,
        RunErrorKind::NondeterministicExternalWake { .. }
    ));
    assert!(runtime.snapshot().tasks.is_empty());
}

#[test]
fn time_limit_failure_does_not_orphan_the_block_on_root() {
    let mut runtime = SimRuntime::new(RuntimeConfig {
        max_time: Some(SimInstant::from_nanos(5)),
        ..RuntimeConfig::default()
    });
    let handle = runtime.handle();
    let root_handle = handle.clone();
    let error = runtime
        .block_on(async move {
            root_handle
                .sleep(SimDuration::from_nanos(10))
                .await
                .unwrap();
        })
        .unwrap_err();

    assert!(matches!(error.kind, RunErrorKind::TimeLimitExceeded { .. }));
    assert!(runtime.snapshot().tasks.is_empty());
    assert_eq!(runtime.snapshot().live_timers, 0);
}

#[test]
fn max_time_boundary_is_inclusive() {
    let limit = SimInstant::from_nanos(5);
    let mut runtime = SimRuntime::new(RuntimeConfig {
        max_time: Some(limit),
        ..RuntimeConfig::default()
    });
    let handle = runtime.handle();
    let sleeper = handle.clone();

    runtime
        .block_on(async move { sleeper.sleep_until(limit).await })
        .expect("runtime reaches the inclusive time limit")
        .expect("sleep at the limit succeeds");
    assert_eq!(handle.now(), limit);
}

#[test]
fn destructor_panic_is_a_structured_runtime_failure() {
    let trace = Rc::new(RecordingTrace::new(32));
    let mut runtime = SimRuntime::with_trace(RuntimeConfig::default(), trace.clone());
    let error = runtime
        .block_on(ReadyThenPanicOnDrop("destructor failure"))
        .unwrap_err();
    let RunErrorKind::TaskDropPanicked { task, panic } = error.kind else {
        panic!("unexpected error: {error:?}");
    };
    assert_eq!(panic.message, "destructor failure");
    assert!(runtime.snapshot().tasks.is_empty());
    assert_eq!(current_task_id(), None);

    let events = trace.events();
    let completed = events
        .iter()
        .position(|event| {
            matches!(&event.kind, EventKind::TaskCompleted { task: completed } if *completed == task)
        })
        .expect("ready task completion is traced");
    let drop_panicked = events
        .iter()
        .position(|event| {
            matches!(&event.kind, EventKind::TaskDropPanicked { task: dropped, .. } if *dropped == task)
        })
        .expect("destructor panic is traced");
    assert!(completed < drop_panicked);
}

#[test]
fn failed_root_cleanup_preserves_the_primary_failure_and_snapshot() {
    struct PendingThenPanicOnDrop<'a> {
        drops: &'a Cell<usize>,
    }

    impl Future for PendingThenPanicOnDrop<'_> {
        type Output = ();

        fn poll(self: Pin<&mut Self>, _context: &mut Context<'_>) -> Poll<Self::Output> {
            Poll::Pending
        }
    }

    impl Drop for PendingThenPanicOnDrop<'_> {
        fn drop(&mut self) {
            self.drops.set(self.drops.get() + 1);
            panic!("failed root cleanup");
        }
    }

    let mut runtime = SimRuntime::default();
    let drops = Cell::new(0);
    let error = runtime
        .block_on(PendingThenPanicOnDrop { drops: &drops })
        .unwrap_err();

    assert_eq!(error.kind, RunErrorKind::Stalled);
    assert_eq!(error.snapshot.tasks.len(), 1);
    let cleanup = error
        .cleanup_failure
        .as_deref()
        .expect("cleanup failure is retained separately");
    assert!(matches!(
        &cleanup.kind,
        RunErrorKind::TaskDropPanicked { panic, .. }
            if panic.message == "failed root cleanup"
    ));
    assert!(cleanup.snapshot.tasks.is_empty());
    assert_eq!(error.disposition(), RunErrorDisposition::Fatal);
    assert_eq!(drops.get(), 1);
    assert!(runtime.snapshot().stopped);
    assert_eq!(runtime.run_until_stalled().unwrap_err(), error);
}

#[test]
fn completion_is_traced_before_drop_panic_when_notifying_the_joiner_panics() {
    let trace = Rc::new(RecordingTrace::new(32));
    let mut runtime = SimRuntime::with_trace(RuntimeConfig::default(), trace.clone());
    let mut join = runtime
        .handle()
        .spawn(ReadyThenPanicOnDrop("secondary destructor failure"))
        .expect("task spawns");
    let task = join.id();
    let panic_waker = Waker::from(Arc::new(PanicWake("waiter wake failure")));
    let mut context = Context::from_waker(&panic_waker);
    assert!(Pin::new(&mut join).poll(&mut context).is_pending());

    let error = runtime.step().expect_err("destructor panic fails the step");
    assert!(matches!(
        error.kind,
        RunErrorKind::TaskDropPanicked { task: failed, .. } if failed == task
    ));
    let events = trace.events();
    let completed = events
        .iter()
        .position(|event| {
            matches!(&event.kind, EventKind::TaskCompleted { task: completed } if *completed == task)
        })
        .expect("ready task completion is traced");
    let drop_panicked = events
        .iter()
        .position(|event| {
            matches!(&event.kind, EventKind::TaskDropPanicked { task: dropped, .. } if *dropped == task)
        })
        .expect("destructor panic is traced");
    assert!(completed < drop_panicked);
}

#[test]
fn shutdown_finishes_every_join_even_when_one_destructor_panics() {
    struct CountedDrop {
        drops: Rc<Cell<usize>>,
        panic: bool,
    }

    impl Future for CountedDrop {
        type Output = ();

        fn poll(self: Pin<&mut Self>, _context: &mut Context<'_>) -> Poll<Self::Output> {
            Poll::Pending
        }
    }

    impl Drop for CountedDrop {
        fn drop(&mut self) {
            self.drops.set(self.drops.get() + 1);
            if self.panic {
                panic!("shutdown destructor failure");
            }
        }
    }

    let mut runtime = SimRuntime::default();
    let drops = Rc::new(Cell::new(0));
    let first = runtime
        .handle()
        .spawn(CountedDrop {
            drops: Rc::clone(&drops),
            panic: true,
        })
        .unwrap();
    let second = runtime
        .handle()
        .spawn(CountedDrop {
            drops: Rc::clone(&drops),
            panic: false,
        })
        .unwrap();

    let error = runtime.shutdown().unwrap_err();
    assert!(matches!(error.kind, RunErrorKind::TaskDropPanicked { .. }));
    assert_eq!(drops.get(), 2);
    assert!(first.is_finished());
    assert!(second.is_finished());
    assert!(runtime.snapshot().tasks.is_empty());
}

#[test]
fn consuming_finish_surfaces_teardown_failure() {
    fn finish(runtime: SimRuntime) -> Result<(), kr_runtime::RunError> {
        runtime.finish()
    }

    let runtime = SimRuntime::default();
    runtime
        .handle()
        .spawn(PanicOnDrop("checked teardown failure"))
        .unwrap();

    let error = finish(runtime).expect_err("consuming finish checks task teardown");
    assert!(matches!(
        error.kind,
        RunErrorKind::TaskDropPanicked { ref panic, .. }
            if panic.message == "checked teardown failure"
    ));
    assert!(error.snapshot.stopped);
    assert!(error.snapshot.tasks.is_empty());
}

#[test]
fn dropping_another_runtime_inside_a_task_preserves_the_outer_task() {
    let inner = SimRuntime::default();
    inner.handle().spawn(pending::<()>()).unwrap();

    let mut outer = SimRuntime::default();
    let observed = Rc::new(Cell::new(None));
    let task_observed = Rc::clone(&observed);
    let task = outer
        .handle()
        .spawn(async move {
            drop(inner);
            task_observed.set(current_task_id());
        })
        .unwrap();
    let task_id = task.id();

    outer.run_until_stalled().unwrap();
    assert_eq!(observed.get(), Some(task_id));
    assert_eq!(current_task_id(), None);
}

#[test]
#[should_panic(expected = "JoinHandle polled after completion")]
fn join_handle_panics_when_polled_after_ready() {
    let mut runtime = SimRuntime::default();
    let mut task = runtime.handle().spawn(async { 42 }).unwrap();
    runtime.run_until_stalled().unwrap();
    let mut context = Context::from_waker(Waker::noop());

    assert_eq!(Pin::new(&mut task).poll(&mut context), Poll::Ready(Ok(42)));
    let _ = Pin::new(&mut task).poll(&mut context);
}

#[test]
fn panicking_join_waiter_does_not_contradict_successful_completion() {
    let mut runtime = SimRuntime::default();
    let mut task = runtime.handle().spawn(async { 42 }).unwrap();
    let panic_waker = Waker::from(Arc::new(PanicWake("waiter wake failure")));
    let mut panic_context = Context::from_waker(&panic_waker);
    assert!(Pin::new(&mut task).poll(&mut panic_context).is_pending());

    let error = runtime.step().unwrap_err();
    assert!(matches!(error.kind, RunErrorKind::WakerPanicked { .. }));

    let mut noop_context = Context::from_waker(Waker::noop());
    assert_eq!(
        Pin::new(&mut task).poll(&mut noop_context),
        Poll::Ready(Ok(42))
    );
    assert!(runtime.snapshot().tasks.is_empty());
}

#[test]
fn panicking_join_waiter_is_contained_during_cancellation() {
    let mut runtime = SimRuntime::default();
    let mut task = runtime.handle().spawn(pending::<()>()).unwrap();
    runtime.step().unwrap();

    let panic_waker = Waker::from(Arc::new(PanicWake("waiter wake failure")));
    let mut panic_context = Context::from_waker(&panic_waker);
    assert!(Pin::new(&mut task).poll(&mut panic_context).is_pending());
    task.abort();

    let error = runtime.step().unwrap_err();
    assert!(matches!(error.kind, RunErrorKind::WakerPanicked { .. }));

    let mut noop_context = Context::from_waker(Waker::noop());
    assert_eq!(
        Pin::new(&mut task).poll(&mut noop_context),
        Poll::Ready(Err(JoinError::Cancelled))
    );
}

#[test]
fn foreign_wake_is_rejected_even_when_a_wake_is_already_queued() {
    let mut runtime = SimRuntime::default();
    let captured = Arc::new(Mutex::new(None));
    runtime
        .handle()
        .spawn(StoreWaker(Arc::clone(&captured)))
        .unwrap();
    runtime.step().unwrap();
    let waker = lock_unpoisoned(&captured).as_ref().unwrap().clone();
    waker.wake_by_ref();
    std::thread::spawn(move || waker.wake()).join().unwrap();

    let error = runtime.step().unwrap_err();
    assert!(matches!(
        error.kind,
        RunErrorKind::NondeterministicExternalWake { .. }
    ));
    assert!(runtime.snapshot().tasks.is_empty());
}

#[test]
fn panicking_timer_waker_is_terminal_instead_of_stranding_its_task() {
    let mut runtime = SimRuntime::default();
    let sleep = runtime.handle().sleep(SimDuration::from_nanos(1));
    runtime
        .handle()
        .spawn(PollSleepWithPanickingWaker { sleep })
        .unwrap();
    runtime.step().unwrap();

    let error = runtime.step().unwrap_err();
    assert!(matches!(error.kind, RunErrorKind::WakerPanicked { .. }));
    assert!(runtime.snapshot().tasks.is_empty());
    assert!(matches!(
        runtime.handle().spawn(async {}),
        Err(SpawnError::RuntimeStopped)
    ));
}

#[test]
fn timer_waker_failure_is_latched_before_secondary_shutdown_failure() {
    let mut runtime = SimRuntime::default();
    let sleep = runtime.handle().sleep(SimDuration::from_nanos(1));
    runtime
        .handle()
        .spawn(PollSleepWithPanickingWaker { sleep })
        .unwrap();
    runtime
        .handle()
        .spawn(PanicOnDrop("secondary shutdown failure"))
        .unwrap();
    runtime.step().unwrap();
    runtime.step().unwrap();

    let error = runtime.step().expect_err("timer waker panics");
    assert!(matches!(error.kind, RunErrorKind::WakerPanicked { .. }));
    assert!(!error.snapshot.stopped);
    assert_eq!(runtime.finish().unwrap_err(), error);
}

#[test]
fn runtime_starts_at_its_configured_start_time_and_times_relative_to_it() {
    let start = RuntimeConfig::derived_start_time(17);
    assert_eq!(start, SimInstant::from_nanos(1_234_623_339_628_287_327));

    let mut runtime = SimRuntime::new(RuntimeConfig {
        seed: 17,
        start_time: start,
        ..RuntimeConfig::default()
    });
    let handle = runtime.handle();
    assert_eq!(handle.now(), start);

    let sleeper = handle.clone();
    handle
        .spawn(async move {
            sleeper.sleep(SimDuration::from_nanos(5)).await.unwrap();
        })
        .unwrap();

    assert!(matches!(runtime.step().unwrap(), Step::TaskPolled { .. }));
    let Step::TimeAdvanced { from, to, timers } = runtime.step().unwrap() else {
        panic!("expected a virtual-time jump");
    };
    assert_eq!(from, start);
    assert_eq!(to, start.checked_add(SimDuration::from_nanos(5)).unwrap());
    assert_eq!(timers.len(), 1);

    runtime.run_until_stalled().unwrap();
    let snapshot = runtime.snapshot();
    assert_eq!(
        snapshot.now,
        start.checked_add(SimDuration::from_nanos(5)).unwrap()
    );
    assert_eq!(snapshot.reproduction.config.start_time, start);
}

#[test]
#[should_panic(expected = "exceeds max_time")]
fn a_runtime_that_starts_beyond_max_time_panics_on_construction() {
    let _ = SimRuntime::new(RuntimeConfig {
        max_time: Some(SimInstant::from_nanos(10)),
        start_time: SimInstant::from_nanos(11),
        ..RuntimeConfig::default()
    });
}

#[test]
fn randomized_timer_workloads_replay_identically_across_a_seed_sweep() {
    kr_runtime::seed_sweep!(16, |seed| {
        let run = || {
            let mut runtime = SimRuntime::new(RuntimeConfig {
                seed,
                start_time: RuntimeConfig::derived_start_time(seed),
                ..RuntimeConfig::default()
            });
            let handle = runtime.handle();
            let worker = handle.clone();
            handle
                .spawn(async move {
                    let delay = worker.random_below(50).expect("runtime is active") + 1;
                    worker
                        .sleep(SimDuration::from_nanos(delay))
                        .await
                        .expect("sleep completes");
                })
                .expect("workload task spawns");
            runtime.run_until_stalled().expect("workload completes");
            runtime.snapshot().determinism_checkpoint()
        };
        assert_eq!(run(), run(), "seed {seed} did not replay identically");
    });
}

#[test]
fn simulation_identity_is_passive_distinct_and_safe_after_runtime_teardown() {
    fn send_sync<T: Send + Sync>(_: &T) {}
    let runtime = SimRuntime::new(RuntimeConfig::default());
    let other = SimRuntime::new(RuntimeConfig::default());
    let before = runtime.snapshot().determinism_checkpoint();
    let identity = runtime.handle().identity();
    send_sync(&identity);
    assert!(identity.belongs_to(&runtime.handle()));
    assert!(identity.clone().belongs_to(&runtime.handle().clone()));
    assert!(!identity.belongs_to(&other.handle()));
    assert_eq!(before, runtime.snapshot().determinism_checkpoint());
    drop(runtime);
    let replacement = SimRuntime::new(RuntimeConfig::default());
    assert!(!identity.belongs_to(&replacement.handle()));
    std::thread::spawn(move || drop(identity)).join().unwrap();
}
