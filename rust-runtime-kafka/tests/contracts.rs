use kr_runtime::trace::{
    EventKind, RecordingTrace, SamplingTrace, TaskCancellationReason, TraceEvent,
};
use kr_runtime::{
    Handle, JoinError, MAX_PANIC_MESSAGE_BYTES, RunErrorKind, RuntimeConfig, RuntimeDuration,
    RuntimeInstant, SimDuration, SimInstant, SimRuntime, SpawnError, TimeError, yield_now,
};
use std::future::{Future, pending};
use std::num::NonZeroU64;
use std::pin::Pin;
use std::rc::Rc;
use std::sync::Arc;
use std::task::{Context, Poll, Waker};

mod common;
use common::PanicWake;

async fn reference_actor(handle: Handle) -> u64 {
    let choice = handle.random_below(1_000).unwrap();
    yield_now().await;
    handle.sleep(SimDuration::from_nanos(7)).await.unwrap();
    handle
        .spawn(async move { choice + 1 })
        .unwrap()
        .await
        .unwrap()
}

fn drive_reference(runtime: &mut SimRuntime) -> Result<u64, kr_runtime::RunError> {
    let handle = runtime.handle();
    runtime.block_on(reference_actor(handle))
}

#[test]
fn reference_runtime_exposes_the_concrete_simulation_contract() {
    let mut runtime = SimRuntime::new(RuntimeConfig {
        seed: 42,
        ..RuntimeConfig::default()
    });

    let result = drive_reference(&mut runtime).unwrap();

    assert!((1..=1_000).contains(&result));
    assert_eq!(runtime.snapshot().now.as_nanos(), 7);
}

#[test]
fn simulation_time_names_are_exact_aliases_for_runtime_time() {
    let runtime_duration: RuntimeDuration = SimDuration::from_nanos(7);
    let sim_duration: SimDuration = RuntimeDuration::from_nanos(7);
    let runtime_instant: RuntimeInstant = SimInstant::from_nanos(11);
    let sim_instant: SimInstant = RuntimeInstant::from_nanos(11);

    assert_eq!(runtime_duration, sim_duration);
    assert_eq!(runtime_instant, sim_instant);
}

#[test]
fn trace_sink_is_behaviorally_passive() {
    let trace = Rc::new(RecordingTrace::new(1_024));
    let mut traced = SimRuntime::with_trace(
        RuntimeConfig {
            seed: 17,
            ..RuntimeConfig::default()
        },
        trace,
    );
    let traced_outcome = drive_reference(&mut traced).unwrap();

    let mut untraced = SimRuntime::new(RuntimeConfig {
        seed: 17,
        ..RuntimeConfig::default()
    });
    let untraced_outcome = drive_reference(&mut untraced).unwrap();

    assert_eq!(traced_outcome, untraced_outcome);
    assert_eq!(traced.snapshot(), untraced.snapshot());

    let sampled_events = Rc::new(RecordingTrace::new(1_024));
    let sampling = Rc::new(SamplingTrace::new(
        sampled_events.clone(),
        NonZeroU64::new(4).expect("period is nonzero"),
    ));
    let mut sampled = SimRuntime::with_trace(
        RuntimeConfig {
            seed: 17,
            ..RuntimeConfig::default()
        },
        sampling,
    );
    let sampled_outcome = drive_reference(&mut sampled).unwrap();

    assert_eq!(sampled_outcome, untraced_outcome);
    assert_eq!(sampled.snapshot(), untraced.snapshot());
    assert!(
        sampled_events
            .events()
            .iter()
            .all(|event| event.sequence % 4 == 0)
    );
}

#[test]
fn timers_cannot_be_moved_between_runtime_domains() {
    let first = SimRuntime::default();
    let timer = first.handle().sleep(SimDuration::from_nanos(1));
    let mut second = SimRuntime::default();

    let result = second.block_on(timer).unwrap();

    assert_eq!(result, Err(TimeError::WrongRuntime));
}

#[test]
fn failed_block_on_drops_its_root_after_capturing_diagnostics() {
    let mut runtime = SimRuntime::default();

    let error = runtime.block_on(pending::<()>()).unwrap_err();

    assert_eq!(error.snapshot.tasks.len(), 1);
    assert!(runtime.snapshot().tasks.is_empty());
}

#[test]
fn resource_limits_are_typed_and_enforced_at_the_boundary() {
    let mut runtime = SimRuntime::new(RuntimeConfig {
        max_tasks: 1,
        ..RuntimeConfig::default()
    });
    let handle = runtime.handle();
    let spawn_handle = handle.clone();
    let spawn_error = runtime
        .block_on(async move {
            match spawn_handle.spawn(async {}) {
                Ok(_) => panic!("spawn unexpectedly exceeded its configured capacity"),
                Err(error) => error,
            }
        })
        .unwrap();
    assert_eq!(
        spawn_error,
        SpawnError::ResourceExhausted {
            resource: "live tasks",
            limit: 1,
        }
    );

    let mut runtime = SimRuntime::new(RuntimeConfig {
        max_timers: 0,
        ..RuntimeConfig::default()
    });
    let handle = runtime.handle();
    let timer_handle = handle.clone();
    let timer_error = runtime
        .block_on(async move { timer_handle.sleep(SimDuration::from_nanos(1)).await })
        .unwrap();
    assert_eq!(
        timer_error,
        Err(TimeError::ResourceExhausted {
            resource: "live timers",
            limit: 0,
        })
    );
}

fn runtime_for_passivity_check(traced: bool) -> SimRuntime {
    if traced {
        SimRuntime::with_trace(RuntimeConfig::default(), Rc::new(RecordingTrace::new(128)))
    } else {
        SimRuntime::default()
    }
}

fn task_panic_artifact(traced: bool) -> (RunErrorKind, kr_runtime::DeterminismCheckpoint) {
    let mut runtime = runtime_for_passivity_check(traced);
    let error = runtime
        .block_on(async { panic!("passivity task panic") })
        .expect_err("task panic fails the run");
    (error.kind, runtime.snapshot().determinism_checkpoint())
}

fn waker_panic_artifact(traced: bool) -> (RunErrorKind, kr_runtime::DeterminismCheckpoint) {
    let mut runtime = runtime_for_passivity_check(traced);
    let mut task = runtime.handle().spawn(async { 42 }).unwrap();
    let panic_waiter = Waker::from(Arc::new(PanicWake("join notification failed")));
    let mut context = Context::from_waker(&panic_waiter);
    assert!(Pin::new(&mut task).poll(&mut context).is_pending());

    let error = runtime.step().expect_err("join notification panics");

    let mut context = Context::from_waker(Waker::noop());
    assert_eq!(Pin::new(&mut task).poll(&mut context), Poll::Ready(Ok(42)));
    (error.kind, runtime.snapshot().determinism_checkpoint())
}

fn completion_trace(panicking_waiter: bool) -> (Vec<TraceEvent>, u64, Option<RunErrorKind>) {
    let trace = Rc::new(RecordingTrace::new(64));
    let mut runtime = SimRuntime::with_trace(RuntimeConfig::default(), trace.clone());
    let mut task = runtime.handle().spawn(async { 42 }).unwrap();
    let panic_waiter =
        panicking_waiter.then(|| Waker::from(Arc::new(PanicWake("join notification failed"))));
    let waiter = panic_waiter.as_ref().unwrap_or(Waker::noop());
    let mut context = Context::from_waker(waiter);
    assert!(Pin::new(&mut task).poll(&mut context).is_pending());

    let error = runtime.step().err().map(|error| error.kind);

    let mut context = Context::from_waker(Waker::noop());
    assert_eq!(Pin::new(&mut task).poll(&mut context), Poll::Ready(Ok(42)));
    (trace.events(), trace.fingerprint(), error)
}

#[test]
fn waker_failure_changes_the_trace_and_fingerprint() {
    let normal = completion_trace(false);
    let failed = completion_trace(true);

    assert_eq!(failed.0[..normal.0.len()], normal.0);
    assert!(
        failed
            .0
            .iter()
            .any(|event| matches!(event.kind, EventKind::WakerPanicked { .. }))
    );
    assert!(matches!(
        failed.0.last().map(|event| &event.kind),
        Some(EventKind::RuntimeStopped)
    ));
    assert_ne!(failed.1, normal.1);
    assert!(matches!(failed.2, Some(RunErrorKind::WakerPanicked { .. })));
    assert_eq!(normal.2, None);
}

#[test]
fn abort_and_shutdown_have_distinct_lifecycle_artifacts() {
    let abort_trace = Rc::new(RecordingTrace::new(64));
    let mut aborted = SimRuntime::with_trace(RuntimeConfig::default(), abort_trace.clone());
    let mut aborted_task = aborted.handle().spawn(pending::<()>()).unwrap();
    aborted.step().unwrap();
    aborted_task.abort();
    aborted.step().unwrap();
    let aborted_snapshot = aborted.snapshot();

    let shutdown_trace = Rc::new(RecordingTrace::new(64));
    let mut stopped = SimRuntime::with_trace(RuntimeConfig::default(), shutdown_trace.clone());
    let mut stopped_task = stopped.handle().spawn(pending::<()>()).unwrap();
    stopped.step().unwrap();
    stopped.shutdown().unwrap();
    let stopped_snapshot = stopped.snapshot();

    assert!(!aborted_snapshot.stopped);
    assert!(stopped_snapshot.stopped);
    assert_ne!(abort_trace.fingerprint(), shutdown_trace.fingerprint());
    assert!(abort_trace.events().iter().any(|event| matches!(
        event.kind,
        EventKind::TaskCancelled {
            reason: TaskCancellationReason::ExplicitAbort,
            ..
        }
    )));
    assert!(shutdown_trace.events().iter().any(|event| matches!(
        event.kind,
        EventKind::TaskCancelled {
            reason: TaskCancellationReason::RuntimeStopped,
            ..
        }
    )));
    assert!(
        shutdown_trace
            .events()
            .iter()
            .any(|event| matches!(event.kind, EventKind::RuntimeStopped))
    );
    let mut context = Context::from_waker(Waker::noop());
    assert_eq!(
        Pin::new(&mut aborted_task).poll(&mut context),
        Poll::Ready(Err(JoinError::Cancelled))
    );
    assert_eq!(
        Pin::new(&mut stopped_task).poll(&mut context),
        Poll::Ready(Err(JoinError::RuntimeStopped))
    );

    let event_count = shutdown_trace.len();
    let fingerprint = shutdown_trace.fingerprint();
    stopped.shutdown().unwrap();
    assert_eq!(shutdown_trace.len(), event_count);
    assert_eq!(shutdown_trace.fingerprint(), fingerprint);
}

#[test]
fn failed_block_on_records_internal_root_cleanup() {
    let trace = Rc::new(RecordingTrace::new(64));
    let mut runtime = SimRuntime::with_trace(RuntimeConfig::default(), trace.clone());

    let _ = runtime.block_on(pending::<()>()).unwrap_err();

    assert!(trace.events().iter().any(|event| matches!(
        event.kind,
        EventKind::TaskCancelled {
            reason: TaskCancellationReason::BlockOnFailure,
            ..
        }
    )));
}

#[test]
fn panic_trace_and_error_share_the_same_bounded_record() {
    let trace = Rc::new(RecordingTrace::new(64));
    let mut runtime = SimRuntime::with_trace(RuntimeConfig::default(), trace.clone());
    let oversized = "x".repeat(MAX_PANIC_MESSAGE_BYTES + 17);

    let error = runtime
        .block_on(async move { std::panic::panic_any(oversized) })
        .unwrap_err();
    let RunErrorKind::TaskPanicked { panic, .. } = error.kind else {
        panic!("unexpected error: {error:?}");
    };
    let traced = trace
        .events()
        .into_iter()
        .find_map(|event| match event.kind {
            EventKind::TaskPanicked { panic, .. } => Some(panic),
            _ => None,
        })
        .unwrap();

    assert_eq!(panic, traced);
    assert_eq!(panic.message.len(), MAX_PANIC_MESSAGE_BYTES);
    assert!(panic.message_truncated);
}

struct PendingDropPanic {
    panic_on_drop: bool,
}

impl Future for PendingDropPanic {
    type Output = ();

    fn poll(self: Pin<&mut Self>, _context: &mut Context<'_>) -> Poll<Self::Output> {
        Poll::Pending
    }
}

impl Drop for PendingDropPanic {
    fn drop(&mut self) {
        if self.panic_on_drop {
            panic!("shutdown drop failed");
        }
    }
}

fn shutdown_drop_trace(panic_on_drop: bool) -> (Vec<TraceEvent>, u64, Option<RunErrorKind>) {
    let trace = Rc::new(RecordingTrace::new(64));
    let mut runtime = SimRuntime::with_trace(RuntimeConfig::default(), trace.clone());
    runtime
        .handle()
        .spawn(PendingDropPanic { panic_on_drop })
        .unwrap();
    runtime.step().unwrap();

    let error = runtime.shutdown().err().map(|error| error.kind);

    (trace.events(), trace.fingerprint(), error)
}

fn destructor_panic_artifact(traced: bool) -> (RunErrorKind, kr_runtime::DeterminismCheckpoint) {
    let mut runtime = runtime_for_passivity_check(traced);
    runtime
        .handle()
        .spawn(PendingDropPanic {
            panic_on_drop: true,
        })
        .unwrap();
    runtime.step().unwrap();

    let error = runtime.shutdown().expect_err("task destructor panics");

    (error.kind, runtime.snapshot().determinism_checkpoint())
}

#[test]
fn tracing_is_passive_on_structured_failure_paths() {
    assert_eq!(task_panic_artifact(true), task_panic_artifact(false));
    assert_eq!(waker_panic_artifact(true), waker_panic_artifact(false));
    assert_eq!(
        destructor_panic_artifact(true),
        destructor_panic_artifact(false)
    );
}

#[test]
fn destructor_failure_changes_the_shutdown_trace_and_fingerprint() {
    let clean = shutdown_drop_trace(false);
    let failed = shutdown_drop_trace(true);

    assert_eq!(failed.0[..clean.0.len()], clean.0);
    assert!(matches!(
        failed.0.last().map(|event| &event.kind),
        Some(EventKind::TaskDropPanicked { panic, .. })
            if panic.message == "shutdown drop failed"
    ));
    assert_ne!(failed.1, clean.1);
    assert!(matches!(
        failed.2,
        Some(RunErrorKind::TaskDropPanicked { .. })
    ));
    assert_eq!(clean.2, None);
}

#[test]
fn shutdown_records_terminal_cancellation_before_secondary_waker_failure() {
    let trace = Rc::new(RecordingTrace::new(64));
    let mut runtime = SimRuntime::with_trace(RuntimeConfig::default(), trace.clone());
    let mut task = runtime.handle().spawn(pending::<()>()).unwrap();
    runtime.step().unwrap();
    let panic_waker = Waker::from(Arc::new(PanicWake("join notification failed")));
    let mut context = Context::from_waker(&panic_waker);
    assert!(Pin::new(&mut task).poll(&mut context).is_pending());

    let error = runtime.shutdown().unwrap_err();
    assert!(matches!(error.kind, RunErrorKind::WakerPanicked { .. }));
    let events = trace.events();
    let cancelled = events
        .iter()
        .position(|event| matches!(event.kind, EventKind::TaskCancelled { .. }))
        .unwrap();
    let waker_failed = events
        .iter()
        .position(|event| matches!(event.kind, EventKind::WakerPanicked { .. }))
        .unwrap();

    assert!(cancelled < waker_failed);
}
