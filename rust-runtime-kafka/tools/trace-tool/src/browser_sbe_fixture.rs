//! Shared deterministic fixture for the browser SBE golden artifacts.
//!
//! This module is compiled by crate tests and included by the explicit golden
//! generator example. It intentionally depends only on public runtime APIs.
//! `TaskId` and `TimerId` constructors are crate-private, so the fixture derives
//! them through the runtime and asserts their expected stable coordinates.

use std::future;
use std::num::NonZeroU64;
use std::rc::Rc;

use kr_runtime::rng::{RandomStream, RngCheckpoint};
use kr_runtime::trace::sbe::{SbeRecordingTrace, SbeTraceRetention};
use kr_runtime::trace::{
    EventKind, EventKindTag, RandomChoiceKind, RecordingTrace, SamplingTrace,
    TaskCancellationReason, TraceEvent, TraceRetention, TraceSink,
};
use kr_runtime::{
    PanicRecord, RuntimeConfig, RuntimeSnapshot, SimDuration, SimInstant, SimRuntime, TaskSnapshot,
    TaskState,
};

pub(crate) const DRIVER: &str = "\u{feff}browser-sbe-golden/雪";
pub(crate) const OUTCOME: &str = "\u{feff}completed/精确";

const MAX_SAFE_INTEGER_PLUS_ONE: u64 = 9_007_199_254_740_992;
const PREFIX_CAPACITY_BYTES: usize = 256;
const TAIL_CAPACITY_BYTES: usize = 4 * 1_024;

pub(crate) struct BrowserSbeFixture {
    pub(crate) events: Vec<TraceEvent>,
    pub(crate) snapshot: RuntimeSnapshot,
}

impl BrowserSbeFixture {
    pub(crate) fn typed_trace(&self) -> (Rc<RecordingTrace>, SamplingTrace) {
        let prefix_capacity = 5;
        let trace = Rc::new(RecordingTrace::with_retention(
            TraceRetention::PrefixAndTail {
                prefix_capacity,
                tail_capacity: self.events.len() - prefix_capacity,
            },
        ));
        let sampling = SamplingTrace::new(
            trace.clone(),
            NonZeroU64::new(1).expect("fixture sampling period is nonzero"),
        );
        record_fixture(&sampling, &self.events);
        (trace, sampling)
    }

    pub(crate) fn buffered_trace(&self) -> (Rc<SbeRecordingTrace>, SamplingTrace) {
        let trace = Rc::new(SbeRecordingTrace::with_retention(
            SbeTraceRetention::PrefixAndTail {
                prefix_capacity_bytes: PREFIX_CAPACITY_BYTES,
                tail_capacity_bytes: TAIL_CAPACITY_BYTES,
            },
        ));
        let sampling = SamplingTrace::new(
            trace.clone(),
            NonZeroU64::new(1).expect("fixture sampling period is nonzero"),
        );
        record_fixture(&sampling, &self.events);
        assert_eq!(trace.len(), self.events.len(), "fixture byte capacity");
        assert_eq!(trace.encoding_failures(), 0, "fixture events encode");
        (trace, sampling)
    }
}

fn record_fixture(sampling: &SamplingTrace, events: &[TraceEvent]) {
    for event in events {
        assert!(sampling.should_record(event.sequence, event.kind.tag()));
        sampling.record(event.clone());
    }

    let duplicate = events.last().expect("fixture contains events").clone();
    assert!(sampling.should_record(duplicate.sequence, duplicate.kind.tag()));
    sampling.record(duplicate);
}

pub(crate) fn browser_sbe_fixture() -> BrowserSbeFixture {
    let fixture_trace = Rc::new(RecordingTrace::new(128));
    let config = RuntimeConfig {
        seed: u64::MAX,
        max_tasks: 8,
        max_timers: 4,
        max_steps_per_run: u64::MAX,
        max_time: Some(SimInstant::from_nanos(u64::MAX)),
        start_time: SimInstant::ZERO,
    };
    let mut runtime = SimRuntime::with_trace(config, fixture_trace.clone());
    let handle = runtime.handle();
    let task_ids = runtime
        .block_on(async move {
            let first = handle
                .spawn(future::pending::<()>())
                .expect("first fixture task spawns");
            let second = handle
                .spawn(future::pending::<()>())
                .expect("second fixture task spawns");
            let third = handle
                .spawn(future::pending::<()>())
                .expect("third fixture task spawns");
            let ids = [first.id(), second.id(), third.id()];
            handle
                .sleep(SimDuration::from_nanos(1))
                .await
                .expect("fixture timer completes");
            ids
        })
        .expect("fixture runtime completes its root");
    assert_eq!(
        task_ids.map(|id| (id.slot(), id.generation())),
        [(1, 0), (2, 0), (3, 0)],
        "browser golden intentionally pins the runtime's fixture task IDs"
    );
    let timer = fixture_trace
        .events()
        .iter()
        .find_map(|event| match event.kind {
            EventKind::TimerScheduled { id, .. } => Some(id),
            _ => None,
        })
        .expect("fixture runtime emits a timer ID");
    assert_eq!(
        timer.get(),
        0,
        "browser golden intentionally pins the runtime's fixture timer ID"
    );

    let mut snapshot = runtime.snapshot();
    snapshot.now = SimInstant::from_nanos(u64::MAX);
    snapshot.total_steps = u64::MAX;
    snapshot.next_enqueue_sequence = u64::MAX;
    snapshot.next_timer_sequence = u64::MAX;
    snapshot.next_timer_id = u64::MAX;
    snapshot.ready_tasks = 1;
    snapshot.live_timers = 0;
    snapshot.stopped = false;
    for (index, random) in snapshot.random.iter_mut().enumerate() {
        let index = u64::try_from(index).expect("five fixture streams fit u64");
        random.checkpoint =
            RngCheckpoint::from_raw_parts(u64::MAX - index, MAX_SAFE_INTEGER_PLUS_ONE + index);
    }
    snapshot.tasks = vec![
        TaskSnapshot {
            id: task_ids[0],
            state: TaskState::Waiting,
        },
        TaskSnapshot {
            id: task_ids[1],
            state: TaskState::Ready,
        },
        TaskSnapshot {
            id: task_ids[2],
            state: TaskState::Running,
        },
    ];

    let at = SimInstant::from_nanos(u64::MAX);
    let events = vec![
        EventKind::RuntimeStarted { seed: u64::MAX },
        EventKind::TaskSpawned {
            task: task_ids[0],
            parent: Some(task_ids[1]),
        },
        EventKind::TaskSpawned {
            task: task_ids[1],
            parent: None,
        },
        EventKind::TaskEnqueued {
            task: task_ids[1],
            sequence: u64::MAX,
        },
        EventKind::TaskPollStarted { task: task_ids[0] },
        EventKind::TaskPending { task: task_ids[0] },
        EventKind::TaskCompleted { task: task_ids[2] },
        EventKind::TaskCancelled {
            task: task_ids[0],
            reason: TaskCancellationReason::ExplicitAbort,
        },
        EventKind::TaskCancelled {
            task: task_ids[1],
            reason: TaskCancellationReason::BlockOnFailure,
        },
        EventKind::TaskCancelled {
            task: task_ids[2],
            reason: TaskCancellationReason::RuntimeStopped,
        },
        EventKind::TaskPanicked {
            task: task_ids[0],
            panic: PanicRecord {
                message: "\u{feff}panic \"雪\"".to_owned(),
                message_truncated: false,
            },
        },
        EventKind::TaskDropPanicked {
            task: task_ids[1],
            panic: PanicRecord {
                message: "drop\nline".to_owned(),
                message_truncated: false,
            },
        },
        EventKind::WakerPanicked {
            task: task_ids[2],
            panic: PanicRecord {
                message: "wake/精确".to_owned(),
                message_truncated: true,
            },
        },
        EventKind::TimerScheduled {
            id: timer,
            task: task_ids[0],
            deadline: SimInstant::from_nanos(u64::MAX),
        },
        EventKind::TimerFired {
            id: timer,
            task: task_ids[0],
        },
        EventKind::TimerCancelled {
            id: timer,
            task: task_ids[0],
        },
        EventKind::TimeAdvanced {
            from: SimInstant::from_nanos(u64::MAX - 1),
            to: SimInstant::from_nanos(u64::MAX),
        },
        EventKind::RuntimeStalled {
            live_tasks: u64::MAX,
        },
        EventKind::BudgetExhausted { steps: u64::MAX },
        EventKind::RuntimeStopped,
        EventKind::RandomChoice {
            stream: RandomStream::Workload,
            choice: RandomChoiceKind::U64,
            draws_before: MAX_SAFE_INTEGER_PLUS_ONE,
            draws_after: MAX_SAFE_INTEGER_PLUS_ONE + 1,
            value: u64::MAX,
        },
        EventKind::RandomChoice {
            stream: RandomStream::Fault,
            choice: RandomChoiceKind::Below {
                upper_exclusive: u64::MAX,
            },
            draws_before: u64::MAX - 3,
            draws_after: u64::MAX - 2,
            value: u64::MAX - 1,
        },
        EventKind::RandomChoice {
            stream: RandomStream::Scenario,
            choice: RandomChoiceKind::BoolRatio {
                numerator: u64::MAX - 1,
                denominator: u64::MAX,
            },
            draws_before: u64::MAX - 1,
            draws_after: u64::MAX,
            value: 1,
        },
    ];
    assert_eq!(
        events
            .iter()
            .map(EventKind::tag)
            .collect::<std::collections::BTreeSet<_>>(),
        EventKindTag::ALL.iter().copied().collect(),
        "browser fixture covers every stable event tag"
    );
    let first_sequence =
        u64::MAX - (u64::try_from(events.len()).expect("fixture event count fits u64") - 1);
    let events = events
        .into_iter()
        .enumerate()
        .map(|(index, kind)| {
            TraceEvent::new(
                first_sequence + u64::try_from(index).expect("fixture index fits u64"),
                at,
                kind,
            )
        })
        .collect();

    BrowserSbeFixture { events, snapshot }
}
