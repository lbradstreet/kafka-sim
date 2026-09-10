use kr_runtime::trace::{EventKind, RecordingTrace};
use kr_runtime::{RuntimeConfig, SimDuration, SimInstant, SimRuntime, Sleep, TaskId};
use std::cell::RefCell;
use std::future::{Future, poll_fn};
use std::pin::Pin;
use std::rc::Rc;
use std::sync::Arc;
use std::task::{Context, Poll, Waker};

mod common;
use common::PanicWake;

fn register_and_transfer(
    runtime: &SimRuntime,
    initial_waker: Option<Waker>,
) -> (TaskId, Rc<RefCell<Option<Sleep>>>) {
    let mailbox = Rc::new(RefCell::new(None));
    let destination = Rc::clone(&mailbox);
    let handle = runtime.handle();
    let mut sleep = Some(handle.sleep(SimDuration::from_nanos(5)));
    let initial = handle
        .spawn(poll_fn(move |context| {
            let mut sleep = sleep.take().expect("first task is polled once");
            let result = if let Some(waker) = &initial_waker {
                Pin::new(&mut sleep).poll(&mut Context::from_waker(waker))
            } else {
                Pin::new(&mut sleep).poll(context)
            };
            assert_eq!(result, Poll::Pending);
            *destination.borrow_mut() = Some(sleep);
            Poll::Ready(())
        }))
        .expect("initial task is admitted");
    (initial.id(), mailbox)
}

#[test]
fn moved_sleep_fires_for_the_latest_waiter_and_keeps_its_initial_scheduling_event() {
    let trace = Rc::new(RecordingTrace::new(64));
    let mut runtime = SimRuntime::with_trace(RuntimeConfig::default(), trace.clone());
    let (initial, mailbox) = register_and_transfer(&runtime, None);
    let destination = runtime
        .handle()
        .spawn(async move {
            let sleep = mailbox
                .borrow_mut()
                .take()
                .expect("first task transferred sleep");
            sleep.await.expect("moved sleep fires");
        })
        .expect("destination task is admitted")
        .id();

    runtime.run_until_stalled().expect("both tasks complete");

    let timers: Vec<_> = trace
        .events()
        .into_iter()
        .filter_map(|event| match event.kind {
            EventKind::TimerScheduled { id, task, .. } => Some(("scheduled", id, task)),
            EventKind::TimerFired { id, task } => Some(("fired", id, task)),
            EventKind::TimerCancelled { id, task } => Some(("cancelled", id, task)),
            _ => None,
        })
        .collect();
    assert_eq!(timers.len(), 2);
    assert_eq!(timers[0], ("scheduled", timers[0].1, initial));
    assert_eq!(timers[1], ("fired", timers[0].1, destination));
    assert_eq!(runtime.handle().now(), SimInstant::from_nanos(5));
    assert!(runtime.snapshot().tasks.is_empty());
}

#[test]
fn moved_sleep_cancellation_identifies_the_latest_waiter() {
    let trace = Rc::new(RecordingTrace::new(64));
    let mut runtime = SimRuntime::with_trace(RuntimeConfig::default(), trace.clone());
    let (initial, mailbox) = register_and_transfer(&runtime, None);
    let destination = runtime
        .handle()
        .spawn(poll_fn(move |context| {
            let mut sleep = mailbox
                .borrow_mut()
                .take()
                .expect("first task transferred sleep");
            assert_eq!(Pin::new(&mut sleep).poll(context), Poll::Pending);
            drop(sleep);
            Poll::Ready(())
        }))
        .expect("destination task is admitted")
        .id();

    runtime.run_until_stalled().expect("both tasks complete");

    let events = trace.events();
    let scheduled = events
        .iter()
        .find_map(|event| match event.kind {
            EventKind::TimerScheduled { id, task, .. } if task == initial => Some(id),
            _ => None,
        })
        .expect("scheduling keeps the initial task");
    let cancelled: Vec<_> = events
        .iter()
        .filter_map(|event| match event.kind {
            EventKind::TimerCancelled { id, task } => Some((id, task)),
            _ => None,
        })
        .collect();
    assert_eq!(cancelled, vec![(scheduled, destination)]);
    assert!(
        !events
            .iter()
            .any(|event| matches!(event.kind, EventKind::TimerFired { .. }))
    );
    assert_eq!(runtime.handle().now(), SimInstant::ZERO);
}

#[test]
fn moved_timer_waker_failures_identify_the_latest_waiter_on_fire_and_shutdown() {
    for stop in [false, true] {
        let trace = Rc::new(RecordingTrace::new(64));
        let mut runtime = SimRuntime::with_trace(RuntimeConfig::default(), trace.clone());
        let waker = Waker::from(Arc::new(PanicWake("moved timer wake failed")));
        let (_initial, mailbox) = register_and_transfer(&runtime, Some(waker.clone()));
        let destination = runtime
            .handle()
            .spawn(async move {
                let mut sleep = mailbox
                    .borrow_mut()
                    .take()
                    .expect("first task transferred sleep");
                // Both tasks use the same waker identity. Attribution must
                // still move when no replacement clone is necessary.
                poll_fn(|_| Pin::new(&mut sleep).poll(&mut Context::from_waker(&waker)))
                    .await
                    .expect("test fails the timer wake before the sleep completes");
            })
            .expect("destination task is admitted")
            .id();
        runtime
            .step()
            .expect("first task registers and transfers sleep");
        runtime.step().expect("destination installs its waiter");

        let error = if stop {
            runtime
                .shutdown()
                .expect_err("stopping wakes the moved timer")
        } else {
            runtime.step().expect_err("firing wakes the moved timer")
        };

        assert!(
            matches!(
                error.kind,
                kr_runtime::RunErrorKind::WakerPanicked { task, ref panic }
                    if task == destination && panic.message == "moved timer wake failed"
            ),
            "stop={stop}: {error:?}"
        );
        assert!(
            trace.events().iter().any(|event| matches!(
                event.kind,
                EventKind::WakerPanicked { task, .. } if task == destination
            )),
            "stop={stop}"
        );
    }
}
