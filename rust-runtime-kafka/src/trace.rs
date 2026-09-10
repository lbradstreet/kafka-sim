//! Structured events emitted by the runtime.
//!
//! Tracing is designed as a passive boundary: the runtime decides when an
//! event happened and assigns its global sequence number, while a conforming
//! [`TraceSink`] only consumes the resulting value. A sink must not call back
//! into the runtime or panic.

use std::cell::RefCell;
use std::collections::VecDeque;
use std::num::NonZeroU64;
use std::rc::Rc;

use crate::rng::RandomStream;
use crate::task::{PanicRecord, TaskId};
use crate::time::SimInstant;
use crate::timer::TimerId;

pub mod sbe;

/// Version of the structured diagnostic event schema and trace fingerprint encoding.
pub const TRACE_SCHEMA_VERSION: u32 = 5;

/// Version of [`SamplingTrace`]'s periodic sequence-selection algorithm.
pub const PERIODIC_SAMPLING_ALGORITHM_VERSION: u32 = 1;

const TRACE_FINGERPRINT_OFFSET: u64 = 0xcbf2_9ce4_8422_2325;
const FINGERPRINT_PRIME: u64 = 0x0000_0100_0000_01b3;

/// A single, totally ordered runtime observation.
///
/// `sequence` is assigned by the runtime, not by a trace sink.  Runtime events
/// must be submitted to a sink in strictly increasing sequence order.  Gaps are
/// permitted so that a caller may filter events without renumbering them.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct TraceEvent {
    /// The event's position in the runtime's global diagnostic trace order.
    pub sequence: u64,
    /// Virtual time at which the event was emitted.
    pub at: SimInstant,
    /// Structured event-specific data.
    pub kind: EventKind,
}

impl TraceEvent {
    /// Creates an event whose sequence has already been assigned by the
    /// runtime.
    pub fn new(sequence: u64, at: SimInstant, kind: EventKind) -> Self {
        Self { sequence, at, kind }
    }

    /// Returns the task directly associated with this event, if any.
    pub fn task_id(&self) -> Option<TaskId> {
        self.kind.task_id()
    }
}

/// Folds one event into a platform-independent diagnostic trace digest.
///
/// This deliberately does not use Rust's [`Hash`](std::hash::Hash) encoding,
/// which is not a cross-version or cross-architecture serialization contract.
fn fold_trace_fingerprint(current: u64, event: &TraceEvent) -> u64 {
    let mut fingerprint = StableFingerprint(current);
    fingerprint.u64(event.sequence);
    fingerprint.u64(event.at.as_nanos());
    fingerprint.event_kind(&event.kind);
    fingerprint.0
}

/// Structured state transitions that are useful when diagnosing a rerun.
///
/// The variants intentionally contain IDs and scalar values rather than
/// references to runtime state.  A recorded event is therefore a self-contained
/// snapshot and cannot observe later mutations.
#[non_exhaustive]
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub enum EventKind {
    /// Runtime initialization.  This must be emitted before seed-dependent work.
    RuntimeStarted { seed: u64 },

    /// A task was allocated and became known to the runtime.
    TaskSpawned {
        task: TaskId,
        parent: Option<TaskId>,
    },
    /// A task was put in the ready queue.
    ///
    /// `sequence` is the ready queue's FIFO tie-breaker, distinct from
    /// [`TraceEvent::sequence`].
    TaskEnqueued { task: TaskId, sequence: u64 },
    /// The runtime is about to poll a task.
    TaskPollStarted { task: TaskId },
    /// Polling returned `Pending`.
    TaskPending { task: TaskId },
    /// Polling returned `Ready` and the task completed normally.
    TaskCompleted { task: TaskId },
    /// A live task was cancelled without completing normally.
    TaskCancelled {
        task: TaskId,
        reason: TaskCancellationReason,
    },
    /// Polling unwound with a panic.
    TaskPanicked { task: TaskId, panic: PanicRecord },
    /// Dropping a task future unwound with a panic.
    TaskDropPanicked { task: TaskId, panic: PanicRecord },
    /// Notifying a future's registered waker panicked.
    WakerPanicked { task: TaskId, panic: PanicRecord },

    /// A timer was first registered by `task`.
    TimerScheduled {
        id: TimerId,
        task: TaskId,
        deadline: SimInstant,
    },
    /// A registered timer reached its deadline; `task` is its latest installed waiter.
    TimerFired { id: TimerId, task: TaskId },
    /// A registered timer was removed before firing; `task` is its latest installed waiter.
    TimerCancelled { id: TimerId, task: TaskId },

    /// The scheduler jumped the virtual clock because no task was ready.
    TimeAdvanced { from: SimInstant, to: SimInstant },
    /// No event can currently make progress while tasks remain live.
    RuntimeStalled { live_tasks: u64 },
    /// The caller-supplied execution step budget was exhausted.
    BudgetExhausted { steps: u64 },
    /// The runtime entered its stopped state.
    RuntimeStopped,
    /// A behavioral random stream produced a choice.
    RandomChoice {
        stream: RandomStream,
        choice: RandomChoiceKind,
        draws_before: u64,
        draws_after: u64,
        value: u64,
    },
}

macro_rules! define_event_kind_tags {
    ($($(#[$meta:meta])* $variant:ident => $name:literal),+ $(,)?) => {
        /// Stable registry of the event variants in the current trace schema.
        ///
        /// [`Self::ALL`] lets downstream schema consumers prove that their
        /// fixtures cover every known [`EventKind`] variant while retaining a
        /// forward-compatible fallback for future schema versions.
        #[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
        #[non_exhaustive]
        pub enum EventKindTag {
            $($(#[$meta])* $variant),+
        }

        impl EventKindTag {
            /// Every event tag in the current trace schema.
            pub const ALL: &'static [Self] = &[$(Self::$variant),+];

            /// Stable structured-event type name.
            #[must_use]
            pub const fn name(self) -> &'static str {
                match self {
                    $(Self::$variant => $name),+
                }
            }
        }
    };
}

define_event_kind_tags! {
    /// Runtime initialization.
    RuntimeStarted => "runtime_started",
    /// Task allocation.
    TaskSpawned => "task_spawned",
    /// Ready-queue insertion.
    TaskEnqueued => "task_enqueued",
    /// Beginning a task poll.
    TaskPollStarted => "task_poll_started",
    /// A pending task poll.
    TaskPending => "task_pending",
    /// Normal task completion.
    TaskCompleted => "task_completed",
    /// Task cancellation.
    TaskCancelled => "task_cancelled",
    /// Panic while polling a task.
    TaskPanicked => "task_panicked",
    /// Panic while dropping a task future.
    TaskDropPanicked => "task_drop_panicked",
    /// Panic while notifying a waker.
    WakerPanicked => "waker_panicked",
    /// Timer registration.
    TimerScheduled => "timer_scheduled",
    /// Timer firing.
    TimerFired => "timer_fired",
    /// Timer cancellation.
    TimerCancelled => "timer_cancelled",
    /// Virtual-time advancement.
    TimeAdvanced => "time_advanced",
    /// Runtime stall detection.
    RuntimeStalled => "runtime_stalled",
    /// Execution-budget exhaustion.
    BudgetExhausted => "budget_exhausted",
    /// Runtime stop.
    RuntimeStopped => "runtime_stopped",
    /// Behavioral random choice.
    RandomChoice => "random_choice",
}

/// Why a live task was cancelled.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
#[non_exhaustive]
pub enum TaskCancellationReason {
    /// A task abort was explicitly requested.
    ExplicitAbort,
    /// The root was cleaned up after `block_on` failed.
    BlockOnFailure,
    /// The owning runtime stopped.
    RuntimeStopped,
}

/// The random API contract associated with a traced choice.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum RandomChoiceKind {
    /// One unconstrained 64-bit draw.
    U64,
    /// A uniform integer in `0..upper_exclusive`.
    Below { upper_exclusive: u64 },
    /// An exact rational boolean decision.
    BoolRatio { numerator: u64, denominator: u64 },
}

impl EventKind {
    /// Returns this variant's stable current-schema tag.
    #[must_use]
    pub const fn tag(&self) -> EventKindTag {
        match self {
            Self::RuntimeStarted { .. } => EventKindTag::RuntimeStarted,
            Self::TaskSpawned { .. } => EventKindTag::TaskSpawned,
            Self::TaskEnqueued { .. } => EventKindTag::TaskEnqueued,
            Self::TaskPollStarted { .. } => EventKindTag::TaskPollStarted,
            Self::TaskPending { .. } => EventKindTag::TaskPending,
            Self::TaskCompleted { .. } => EventKindTag::TaskCompleted,
            Self::TaskCancelled { .. } => EventKindTag::TaskCancelled,
            Self::TaskPanicked { .. } => EventKindTag::TaskPanicked,
            Self::TaskDropPanicked { .. } => EventKindTag::TaskDropPanicked,
            Self::WakerPanicked { .. } => EventKindTag::WakerPanicked,
            Self::TimerScheduled { .. } => EventKindTag::TimerScheduled,
            Self::TimerFired { .. } => EventKindTag::TimerFired,
            Self::TimerCancelled { .. } => EventKindTag::TimerCancelled,
            Self::TimeAdvanced { .. } => EventKindTag::TimeAdvanced,
            Self::RuntimeStalled { .. } => EventKindTag::RuntimeStalled,
            Self::BudgetExhausted { .. } => EventKindTag::BudgetExhausted,
            Self::RuntimeStopped => EventKindTag::RuntimeStopped,
            Self::RandomChoice { .. } => EventKindTag::RandomChoice,
        }
    }

    /// Returns the task directly associated with this event, if any.
    pub fn task_id(&self) -> Option<TaskId> {
        match self {
            Self::TaskSpawned { task, .. }
            | Self::TaskEnqueued { task, .. }
            | Self::TaskPollStarted { task }
            | Self::TaskPending { task }
            | Self::TaskCompleted { task }
            | Self::TaskCancelled { task, .. }
            | Self::TaskPanicked { task, .. }
            | Self::TaskDropPanicked { task, .. }
            | Self::WakerPanicked { task, .. }
            | Self::TimerScheduled { task, .. }
            | Self::TimerFired { task, .. }
            | Self::TimerCancelled { task, .. } => Some(*task),
            Self::RuntimeStarted { .. }
            | Self::TimeAdvanced { .. }
            | Self::RuntimeStalled { .. }
            | Self::BudgetExhausted { .. }
            | Self::RuntimeStopped
            | Self::RandomChoice { .. } => None,
        }
    }
}

struct StableFingerprint(u64);

impl StableFingerprint {
    fn bytes(&mut self, bytes: &[u8]) {
        for byte in bytes {
            self.0 ^= u64::from(*byte);
            self.0 = self.0.wrapping_mul(FINGERPRINT_PRIME);
        }
    }

    fn u8(&mut self, value: u8) {
        self.bytes(&[value]);
    }

    fn u32(&mut self, value: u32) {
        self.bytes(&value.to_le_bytes());
    }

    fn u64(&mut self, value: u64) {
        self.bytes(&value.to_le_bytes());
    }

    fn task(&mut self, task: TaskId) {
        self.u32(task.slot());
        self.u32(task.generation());
    }

    fn timer(&mut self, timer: TimerId) {
        self.u64(timer.get());
    }

    fn panic(&mut self, panic: &PanicRecord) {
        self.u64(panic.message.len() as u64);
        self.bytes(panic.message.as_bytes());
        self.u8(u8::from(panic.message_truncated));
    }

    fn event_kind(&mut self, kind: &EventKind) {
        match kind {
            EventKind::RuntimeStarted { seed } => {
                self.u8(0);
                self.u64(*seed);
            }
            EventKind::TaskSpawned { task, parent } => {
                self.u8(1);
                self.task(*task);
                match parent {
                    Some(parent) => {
                        self.u8(1);
                        self.task(*parent);
                    }
                    None => self.u8(0),
                }
            }
            EventKind::TaskEnqueued { task, sequence } => {
                self.u8(2);
                self.task(*task);
                self.u64(*sequence);
            }
            EventKind::TaskPollStarted { task } => {
                self.u8(3);
                self.task(*task);
            }
            EventKind::TaskPending { task } => {
                self.u8(4);
                self.task(*task);
            }
            EventKind::TaskCompleted { task } => {
                self.u8(5);
                self.task(*task);
            }
            EventKind::TaskCancelled { task, reason } => {
                self.u8(6);
                self.task(*task);
                self.u8(match reason {
                    TaskCancellationReason::ExplicitAbort => 0,
                    TaskCancellationReason::BlockOnFailure => 1,
                    TaskCancellationReason::RuntimeStopped => 2,
                });
            }
            EventKind::TaskPanicked { task, panic } => {
                self.u8(7);
                self.task(*task);
                self.panic(panic);
            }
            EventKind::TimerScheduled { id, task, deadline } => {
                self.u8(8);
                self.timer(*id);
                self.task(*task);
                self.u64(deadline.as_nanos());
            }
            EventKind::TimerFired { id, task } => {
                self.u8(9);
                self.timer(*id);
                self.task(*task);
            }
            EventKind::TimerCancelled { id, task } => {
                self.u8(10);
                self.timer(*id);
                self.task(*task);
            }
            EventKind::TimeAdvanced { from, to } => {
                self.u8(11);
                self.u64(from.as_nanos());
                self.u64(to.as_nanos());
            }
            EventKind::RuntimeStalled { live_tasks } => {
                self.u8(12);
                self.u64(*live_tasks);
            }
            EventKind::BudgetExhausted { steps } => {
                self.u8(13);
                self.u64(*steps);
            }
            EventKind::RandomChoice {
                stream,
                choice,
                draws_before,
                draws_after,
                value,
            } => {
                self.u8(14);
                self.u64(*stream as u64);
                match choice {
                    RandomChoiceKind::U64 => self.u8(0),
                    RandomChoiceKind::Below { upper_exclusive } => {
                        self.u8(1);
                        self.u64(*upper_exclusive);
                    }
                    RandomChoiceKind::BoolRatio {
                        numerator,
                        denominator,
                    } => {
                        self.u8(2);
                        self.u64(*numerator);
                        self.u64(*denominator);
                    }
                }
                self.u64(*draws_before);
                self.u64(*draws_after);
                self.u64(*value);
            }
            EventKind::WakerPanicked { task, panic } => {
                self.u8(15);
                self.task(*task);
                self.panic(panic);
            }
            EventKind::RuntimeStopped => self.u8(16),
            EventKind::TaskDropPanicked { task, panic } => {
                self.u8(17);
                self.task(*task);
                self.panic(panic);
            }
        }
    }
}

/// A synchronous consumer of runtime trace events.
///
/// The deterministic runtime calls this trait from its event-loop thread.  An
/// implementation must not call back into the runtime or use a wall clock or
/// entropy source to affect runtime behavior.  Thread-safe production sinks can
/// implement this same trait; thread safety is not required by the trait itself.
pub trait TraceSink {
    /// Returns whether the runtime should construct and submit events.
    ///
    /// The runtime reads this once during construction. Sinks must therefore
    /// return a stable value for their lifetime. The default keeps existing
    /// sinks enabled; a disabled sink avoids event construction entirely.
    fn enabled(&self) -> bool {
        true
    }

    /// Returns whether one sequence position should be constructed and submitted.
    ///
    /// The runtime calls this after assigning and advancing the global trace
    /// sequence but before reading virtual time or evaluating the event builder.
    /// Returning `false` therefore leaves a visible sequence gap at very low
    /// cost. Implementations must make this decision deterministically without
    /// calling back into the runtime, reading a wall clock, or using entropy.
    fn should_record(&self, _sequence: u64, _tag: EventKindTag) -> bool {
        true
    }

    /// Consumes one event admitted by [`Self::should_record`].
    ///
    /// The runtime invokes the admission hook exactly once per candidate and
    /// calls this method only when it returns `true`.
    fn record(&self, event: TraceEvent);
}

/// Construction failure for a bounded [`TraceReplayChecker`].
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum TraceReplayCheckerError {
    /// The recorded trace uses a schema this runtime cannot compare exactly.
    UnsupportedSchema { expected: u32, supported: u32 },
    /// The expected trace exceeds the caller-provided comparison bound.
    ExpectedTraceTooLong { events: usize, limit: usize },
}

impl std::fmt::Display for TraceReplayCheckerError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::UnsupportedSchema {
                expected,
                supported,
            } => write!(
                formatter,
                "trace schema {expected} cannot be compared by schema {supported}"
            ),
            Self::ExpectedTraceTooLong { events, limit } => write!(
                formatter,
                "expected trace has {events} events, exceeding comparison limit {limit}"
            ),
        }
    }
}

impl std::error::Error for TraceReplayCheckerError {}

/// The first exact divergence between a recorded trace and its rerun.
#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum TraceReplayDivergence {
    /// Both streams contain an event at `index`, but the events differ.
    EventMismatch {
        index: usize,
        expected: Box<TraceEvent>,
        actual: Box<TraceEvent>,
    },
    /// The rerun ended before all expected events were observed.
    ActualEndedEarly {
        index: usize,
        expected_events: usize,
        next_expected: Box<TraceEvent>,
    },
    /// The rerun emitted an event after the expected trace ended.
    UnexpectedEvent {
        index: usize,
        expected_events: usize,
        actual: Box<TraceEvent>,
    },
}

impl std::fmt::Display for TraceReplayDivergence {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::EventMismatch {
                index,
                expected,
                actual,
            } => write!(
                formatter,
                "trace event {index} differs: expected {expected:?}, actual {actual:?}"
            ),
            Self::ActualEndedEarly {
                index,
                expected_events,
                next_expected,
            } => write!(
                formatter,
                "rerun ended after {index} events; expected {expected_events}, next event {next_expected:?}"
            ),
            Self::UnexpectedEvent {
                index,
                expected_events,
                actual,
            } => write!(
                formatter,
                "rerun emitted unexpected event {index} after {expected_events} expected events: {actual:?}"
            ),
        }
    }
}

impl std::error::Error for TraceReplayDivergence {}

#[derive(Debug, Default)]
struct TraceReplayState {
    matched_events: usize,
    divergence: Option<TraceReplayDivergence>,
    finished: bool,
}

/// A bounded sink that stops at the first exact replay-trace divergence.
///
/// The checker consumes an expected trace whose length must fit
/// `max_expected_events`. Rerun events are compared in order without retaining
/// a second trace. At most one actual event is retained, as part of the first
/// divergence. Call [`Self::finish`] after the rerun ends so a truncated actual
/// stream becomes an explicit length divergence.
///
/// Exact structural comparison is valid only within [`TRACE_SCHEMA_VERSION`].
/// A recorded artifact with any other schema is rejected at construction.
#[derive(Debug)]
pub struct TraceReplayChecker {
    expected: Vec<TraceEvent>,
    max_expected_events: usize,
    state: RefCell<TraceReplayState>,
}

impl TraceReplayChecker {
    /// Creates a current-schema checker and takes ownership of the expected trace.
    pub fn new(
        expected_schema: u32,
        expected: Vec<TraceEvent>,
        max_expected_events: usize,
    ) -> Result<Self, TraceReplayCheckerError> {
        if expected_schema != TRACE_SCHEMA_VERSION {
            return Err(TraceReplayCheckerError::UnsupportedSchema {
                expected: expected_schema,
                supported: TRACE_SCHEMA_VERSION,
            });
        }
        if expected.len() > max_expected_events {
            return Err(TraceReplayCheckerError::ExpectedTraceTooLong {
                events: expected.len(),
                limit: max_expected_events,
            });
        }
        Ok(Self {
            expected,
            max_expected_events,
            state: RefCell::new(TraceReplayState::default()),
        })
    }

    /// Trace schema understood by this checker.
    pub const fn schema_version(&self) -> u32 {
        TRACE_SCHEMA_VERSION
    }

    /// Caller-provided bound on the expected trace length.
    pub const fn max_expected_events(&self) -> usize {
        self.max_expected_events
    }

    /// Number of leading events matched before comparison stopped.
    pub fn matched_events(&self) -> usize {
        self.state.borrow().matched_events
    }

    /// Returns the first observed divergence without finalizing the stream.
    pub fn divergence(&self) -> Option<TraceReplayDivergence> {
        self.state.borrow().divergence.clone()
    }

    /// Finalizes comparison and reports an exact match or the first divergence.
    ///
    /// If the rerun ended early, this call records that length divergence.
    /// Repeated calls return the same result.
    pub fn finish(&self) -> Result<(), TraceReplayDivergence> {
        let mut state = self.state.borrow_mut();
        if !state.finished {
            if state.divergence.is_none() && state.matched_events < self.expected.len() {
                let index = state.matched_events;
                state.divergence = Some(TraceReplayDivergence::ActualEndedEarly {
                    index,
                    expected_events: self.expected.len(),
                    next_expected: Box::new(self.expected[index].clone()),
                });
            }
            state.finished = true;
        }
        match &state.divergence {
            Some(divergence) => Err(divergence.clone()),
            None => Ok(()),
        }
    }
}

impl TraceSink for TraceReplayChecker {
    fn record(&self, event: TraceEvent) {
        let mut state = self.state.borrow_mut();
        if state.finished || state.divergence.is_some() {
            return;
        }
        let index = state.matched_events;
        let Some(expected) = self.expected.get(index) else {
            state.divergence = Some(TraceReplayDivergence::UnexpectedEvent {
                index,
                expected_events: self.expected.len(),
                actual: Box::new(event),
            });
            return;
        };
        if expected != &event {
            state.divergence = Some(TraceReplayDivergence::EventMismatch {
                index,
                expected: Box::new(expected.clone()),
                actual: Box::new(event),
            });
            return;
        }
        state.matched_events += 1;
    }
}

/// A sink that discards every event.
#[derive(Clone, Copy, Debug, Default)]
pub struct NoopTrace;

impl TraceSink for NoopTrace {
    fn enabled(&self) -> bool {
        false
    }

    fn record(&self, _event: TraceEvent) {}
}

/// A deterministic every-N sampling wrapper for another [`TraceSink`].
///
/// Sampling happens through [`TraceSink::should_record`], before the runtime
/// reads virtual time or constructs an [`EventKind`] or [`TraceEvent`]. The
/// default phase is zero, so sequence zero (`RuntimeStarted`) is retained.
/// Unsampled sequence positions remain visible as gaps in the sampled stream.
/// Power-of-two periods use a mask-only fast path; other periods use modulo.
/// This is systematic periodic sampling, so a period can alias regular event
/// patterns and is not a statistically representative sample.
///
/// The wrapped sink observes only sampled events. Its fingerprint, drop count,
/// and retention metadata therefore describe the sampled subsequence, not the
/// full candidate event stream. Persist `period` and `phase` in the surrounding
/// harness artifact when exporting a sampled trace.
/// When wrappers are nested, the inner admission hook sees only candidates
/// selected by this outer sampler; decorator order is therefore significant.
/// Exact comparison against a sampled artifact must wrap the
/// [`TraceReplayChecker`] in the identical sampling policy; an unwrapped
/// checker expects the omitted candidates and will report a divergence.
#[derive(Clone)]
pub struct SamplingTrace {
    inner: Rc<dyn TraceSink>,
    period: NonZeroU64,
    phase: u64,
    power_of_two_mask: Option<u64>,
}

impl SamplingTrace {
    /// Samples sequence zero and every `period` positions thereafter.
    #[must_use]
    pub fn new(inner: Rc<dyn TraceSink>, period: NonZeroU64) -> Self {
        Self {
            inner,
            period,
            phase: 0,
            power_of_two_mask: period.get().is_power_of_two().then_some(period.get() - 1),
        }
    }

    /// Selects the congruence class sampled within each period using modulo.
    ///
    /// Values greater than or equal to the period are reduced modulo it.
    #[must_use]
    pub fn with_phase_modulo(mut self, phase: u64) -> Self {
        self.phase = phase % self.period.get();
        self
    }

    /// Distance between sampled sequence positions.
    #[must_use]
    pub const fn period(&self) -> NonZeroU64 {
        self.period
    }

    /// Sampled congruence class in `0..period`.
    #[must_use]
    pub const fn phase(&self) -> u64 {
        self.phase
    }

    /// Returns whether this wrapper samples `sequence`.
    #[must_use]
    pub const fn samples(&self, sequence: u64) -> bool {
        match self.power_of_two_mask {
            Some(mask) => sequence.wrapping_sub(self.phase) & mask == 0,
            None => sequence % self.period.get() == self.phase,
        }
    }

    /// Returns whether this wrapper directly owns `inner` as its sink.
    #[must_use]
    pub fn wraps<T>(&self, inner: &Rc<T>) -> bool
    where
        T: TraceSink + 'static,
    {
        let erased: Rc<dyn TraceSink> = inner.clone();
        Rc::ptr_eq(&self.inner, &erased)
    }
}

impl TraceSink for SamplingTrace {
    fn enabled(&self) -> bool {
        self.inner.enabled()
    }

    fn should_record(&self, sequence: u64, tag: EventKindTag) -> bool {
        self.samples(sequence) && self.inner.should_record(sequence, tag)
    }

    fn record(&self, event: TraceEvent) {
        self.inner.record(event);
    }
}

/// The first event rejected because its sequence did not advance.
///
/// Recording the condition instead of panicking keeps trace collection safe on
/// runtime shutdown and other destructor-reachable paths.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TraceOrderingViolation {
    /// Last sequence accepted before the invalid event.
    pub previous_sequence: u64,
    /// Non-increasing sequence carried by the rejected event.
    pub rejected_sequence: u64,
}

#[derive(Debug)]
struct RecordingState {
    prefix: Vec<TraceEvent>,
    tail: VecDeque<TraceEvent>,
    dropped: u64,
    last_sequence: Option<u64>,
    ordering_violation: Option<TraceOrderingViolation>,
    fingerprint: u64,
}

/// Which accepted events a bounded [`RecordingTrace`] retains.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum TraceRetention {
    /// Retain the first `capacity` events.
    Prefix { capacity: usize },
    /// Retain the most recent `capacity` events.
    Tail { capacity: usize },
    /// Retain the first events and the most recent events, omitting the middle.
    PrefixAndTail {
        /// Maximum number of initial events to retain.
        prefix_capacity: usize,
        /// Maximum number of most-recent events to retain.
        tail_capacity: usize,
    },
}

impl TraceRetention {
    /// Maximum total number of retained events.
    #[must_use]
    pub const fn capacity(self) -> usize {
        match self {
            Self::Prefix { capacity } | Self::Tail { capacity } => capacity,
            Self::PrefixAndTail {
                prefix_capacity,
                tail_capacity,
            } => match prefix_capacity.checked_add(tail_capacity) {
                Some(capacity) => capacity,
                None => panic!("combined trace retention capacity overflowed"),
            },
        }
    }

    /// Maximum number of initial events retained by this policy.
    #[must_use]
    pub const fn prefix_capacity(self) -> usize {
        match self {
            Self::Prefix { capacity } => capacity,
            Self::Tail { .. } => 0,
            Self::PrefixAndTail {
                prefix_capacity, ..
            } => prefix_capacity,
        }
    }

    /// Maximum number of most-recent events retained by this policy.
    #[must_use]
    pub const fn tail_capacity(self) -> usize {
        match self {
            Self::Prefix { .. } => 0,
            Self::Tail { capacity } => capacity,
            Self::PrefixAndTail { tail_capacity, .. } => tail_capacity,
        }
    }

    /// Stable artifact name for this policy.
    #[must_use]
    pub const fn mode_name(self) -> &'static str {
        match self {
            Self::Prefix { .. } => "prefix",
            Self::Tail { .. } => "tail",
            Self::PrefixAndTail { .. } => "prefix_and_tail",
        }
    }
}

/// An in-memory, fixed-capacity trace sink for deterministic tests.
///
/// [`RecordingTrace::new`] retains a prefix because the earliest divergence
/// generally explains a traced-rerun mismatch better than the final tail.
/// [`RecordingTrace::with_retention`] also supports production-oriented tail
/// capture and a bounded prefix plus tail.
/// Sequence validation continues after the recorder fills. A non-increasing
/// event is rejected and the first bounded ordering violation remains available
/// through [`RecordingTrace::ordering_violation`].
///
/// Use one recorder per runtime. Runtime sequence spaces are independent, so
/// sharing a recorder can reject interleaved events as ordering violations and
/// produces a fingerprint that does not describe either run.
///
/// Interior mutability keeps [`TraceSink::record`] usable through a shared
/// reference.  This reference implementation is intended for the simulator's
/// single event-loop thread and is therefore not `Sync`.
#[derive(Debug)]
pub struct RecordingTrace {
    retention: TraceRetention,
    state: RefCell<RecordingState>,
}

impl RecordingTrace {
    /// Creates a recorder that retains at most `capacity` events.
    ///
    /// A zero-capacity recorder validates ordering and counts every event as
    /// dropped without retaining an event.
    pub fn new(capacity: usize) -> Self {
        Self::with_retention(TraceRetention::Prefix { capacity })
    }

    /// Creates a recorder with an explicit bounded retention policy.
    ///
    /// # Panics
    ///
    /// Panics if the combined prefix-plus-tail capacity overflows `usize` or
    /// if the requested buffers cannot be allocated.
    pub fn with_retention(retention: TraceRetention) -> Self {
        let _ = retention.capacity();
        Self {
            retention,
            state: RefCell::new(RecordingState {
                prefix: Vec::with_capacity(retention.prefix_capacity()),
                tail: VecDeque::with_capacity(retention.tail_capacity()),
                dropped: 0,
                last_sequence: None,
                ordering_violation: None,
                fingerprint: TRACE_FINGERPRINT_OFFSET,
            }),
        }
    }

    /// Maximum number of events retained by this recorder.
    pub fn capacity(&self) -> usize {
        self.retention.capacity()
    }

    /// Retention policy used by this recorder.
    pub const fn retention(&self) -> TraceRetention {
        self.retention
    }

    /// Number of events currently retained.
    pub fn len(&self) -> usize {
        let state = self.state.borrow();
        state.prefix.len() + state.tail.len()
    }

    /// Returns whether no events are currently retained.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Number of accepted events omitted from the current retained snapshot.
    pub fn dropped(&self) -> u64 {
        self.state.borrow().dropped
    }

    /// Sequence of the most recently accepted event, including a dropped one.
    pub fn last_sequence(&self) -> Option<u64> {
        self.state.borrow().last_sequence
    }

    /// First rejected non-increasing sequence pair, if one was observed.
    pub fn ordering_violation(&self) -> Option<TraceOrderingViolation> {
        self.state.borrow().ordering_violation
    }

    /// Canonical digest of every accepted event, including events not retained
    /// after the recorder reaches capacity. Ordering violations are excluded.
    pub fn fingerprint(&self) -> u64 {
        self.state.borrow().fingerprint
    }

    /// Returns a stable snapshot of the retained events in sequence order.
    pub fn events(&self) -> Vec<TraceEvent> {
        let state = self.state.borrow();
        state
            .prefix
            .iter()
            .chain(state.tail.iter())
            .cloned()
            .collect()
    }

    /// Consumes the recorder and returns its retained events without cloning.
    pub fn into_events(self) -> Vec<TraceEvent> {
        let state = self.state.into_inner();
        let mut events = state.prefix;
        events.extend(state.tail);
        events
    }
}

impl TraceSink for RecordingTrace {
    fn record(&self, event: TraceEvent) {
        let mut state = self.state.borrow_mut();

        if let Some(last) = state.last_sequence
            && event.sequence <= last
        {
            if state.ordering_violation.is_none() {
                state.ordering_violation = Some(TraceOrderingViolation {
                    previous_sequence: last,
                    rejected_sequence: event.sequence,
                });
            }
            return;
        }
        state.last_sequence = Some(event.sequence);
        state.fingerprint = fold_trace_fingerprint(state.fingerprint, &event);

        if state.prefix.len() < self.retention.prefix_capacity() {
            state.prefix.push(event);
            return;
        }

        let tail_capacity = self.retention.tail_capacity();
        if tail_capacity == 0 {
            state.dropped = state
                .dropped
                .checked_add(1)
                .expect("trace dropped-event counter exhausted");
            return;
        }
        if state.tail.len() == tail_capacity {
            let evicted = state.tail.pop_front();
            debug_assert!(evicted.is_some(), "a full trace tail is nonempty");
            state.dropped = state
                .dropped
                .checked_add(1)
                .expect("trace dropped-event counter exhausted");
        }
        state.tail.push_back(event);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn event(sequence: u64, nanos: u64) -> TraceEvent {
        TraceEvent::new(
            sequence,
            SimInstant::from_nanos(nanos),
            EventKind::RuntimeStarted { seed: sequence },
        )
    }

    #[test]
    fn noop_disables_runtime_emission_but_still_accepts_direct_calls() {
        assert!(!NoopTrace.enabled());
        NoopTrace.record(event(0, 0));
    }

    #[test]
    fn sampling_trace_selects_one_deterministic_sequence_class() {
        let inner = Rc::new(RecordingTrace::new(8));
        let sampling = SamplingTrace::new(
            inner.clone(),
            NonZeroU64::new(3).expect("period is nonzero"),
        )
        .with_phase_modulo(4);
        let candidates = [
            event(0, 0),
            event(1, 10),
            event(2, 20),
            event(3, 30),
            event(4, 40),
            event(5, 50),
            event(6, 60),
            event(7, 70),
        ];

        for event in candidates {
            if sampling.should_record(event.sequence, event.kind.tag()) {
                sampling.record(event);
            }
        }

        assert_eq!(sampling.period().get(), 3);
        assert_eq!(sampling.phase(), 1, "phase is normalized by the period");
        assert_eq!(
            inner.events(),
            vec![event(1, 10), event(4, 40), event(7, 70)]
        );
        assert_eq!(inner.dropped(), 0);
        assert_eq!(inner.last_sequence(), Some(7));
        assert_eq!(
            inner.fingerprint(),
            [event(1, 10), event(4, 40), event(7, 70)]
                .iter()
                .fold(TRACE_FINGERPRINT_OFFSET, fold_trace_fingerprint)
        );
    }

    #[test]
    fn sampling_trace_inherits_disabled_inner_sink() {
        let sampling = SamplingTrace::new(
            Rc::new(NoopTrace),
            NonZeroU64::new(2).expect("period is nonzero"),
        );

        assert!(!sampling.enabled());
    }

    #[test]
    fn sampled_out_candidates_do_not_count_as_retention_drops() {
        let inner = Rc::new(RecordingTrace::new(1));
        let sampling = SamplingTrace::new(
            inner.clone(),
            NonZeroU64::new(2).expect("period is nonzero"),
        );

        for sequence in 0..5 {
            let event = event(sequence, sequence * 10);
            if sampling.should_record(event.sequence, event.kind.tag()) {
                sampling.record(event);
            }
        }

        assert_eq!(inner.events(), vec![event(0, 0)]);
        assert_eq!(
            inner.dropped(),
            2,
            "only selected sequences two and four exceeded retention"
        );
        assert_eq!(inner.last_sequence(), Some(4));
        assert_eq!(
            inner.fingerprint(),
            [event(0, 0), event(2, 20), event(4, 40)]
                .iter()
                .fold(TRACE_FINGERPRINT_OFFSET, fold_trace_fingerprint)
        );
    }

    #[test]
    fn recording_trace_preserves_events_and_allows_sequence_gaps() {
        let trace = RecordingTrace::new(3);

        trace.record(event(2, 10));
        trace.record(event(7, 20));

        assert_eq!(trace.capacity(), 3);
        assert_eq!(trace.len(), 2);
        assert!(!trace.is_empty());
        assert_eq!(trace.dropped(), 0);
        assert_eq!(trace.last_sequence(), Some(7));
        assert_eq!(trace.events(), vec![event(2, 10), event(7, 20)]);
    }

    #[test]
    fn full_recording_trace_keeps_the_earliest_events() {
        let trace = RecordingTrace::new(2);
        let all = [event(4, 10), event(5, 20), event(6, 30), event(9, 40)];

        for event in all.clone() {
            trace.record(event);
        }

        assert_eq!(trace.events(), vec![event(4, 10), event(5, 20)]);
        assert_eq!(trace.len(), 2);
        assert_eq!(trace.dropped(), 2);
        assert_eq!(trace.last_sequence(), Some(9));
        assert_eq!(
            trace.fingerprint(),
            all.iter()
                .fold(TRACE_FINGERPRINT_OFFSET, fold_trace_fingerprint)
        );
    }

    #[test]
    fn tail_recording_trace_keeps_the_most_recent_events() {
        let trace = RecordingTrace::with_retention(TraceRetention::Tail { capacity: 2 });
        let all = [event(4, 10), event(5, 20), event(6, 30), event(9, 40)];

        for event in all.clone() {
            trace.record(event);
        }

        assert_eq!(trace.retention(), TraceRetention::Tail { capacity: 2 });
        assert_eq!(trace.capacity(), 2);
        assert_eq!(trace.events(), vec![event(6, 30), event(9, 40)]);
        assert_eq!(trace.dropped(), 2);
        assert_eq!(
            trace.fingerprint(),
            all.iter()
                .fold(TRACE_FINGERPRINT_OFFSET, fold_trace_fingerprint)
        );
    }

    #[test]
    fn prefix_and_tail_trace_omits_only_the_middle() {
        let trace = RecordingTrace::with_retention(TraceRetention::PrefixAndTail {
            prefix_capacity: 2,
            tail_capacity: 2,
        });
        let all = [
            event(0, 0),
            event(1, 10),
            event(2, 20),
            event(3, 30),
            event(4, 40),
            event(5, 50),
        ];

        for event in all {
            trace.record(event);
        }

        assert_eq!(trace.capacity(), 4);
        assert_eq!(
            trace.events(),
            vec![event(0, 0), event(1, 10), event(4, 40), event(5, 50)]
        );
        assert_eq!(trace.dropped(), 2);
    }

    #[test]
    fn prefix_and_tail_trace_has_no_overlap_before_filling() {
        let trace = RecordingTrace::with_retention(TraceRetention::PrefixAndTail {
            prefix_capacity: 2,
            tail_capacity: 2,
        });

        trace.record(event(0, 0));
        trace.record(event(1, 10));
        trace.record(event(2, 20));

        assert_eq!(
            trace.events(),
            vec![event(0, 0), event(1, 10), event(2, 20)]
        );
        assert_eq!(trace.dropped(), 0);
    }

    #[test]
    fn zero_capacity_records_only_drop_and_order_metadata() {
        let trace = RecordingTrace::with_retention(TraceRetention::Tail { capacity: 0 });

        trace.record(event(11, 10));
        trace.record(event(12, 20));

        assert!(trace.is_empty());
        assert_eq!(trace.events(), Vec::new());
        assert_eq!(trace.dropped(), 2);
        assert_eq!(trace.last_sequence(), Some(12));
    }

    #[test]
    fn ordering_violation_does_not_disturb_wrapped_tail() {
        let trace = RecordingTrace::with_retention(TraceRetention::Tail { capacity: 2 });

        trace.record(event(3, 10));
        trace.record(event(4, 20));
        trace.record(event(3, 30));
        trace.record(event(5, 40));

        assert_eq!(
            trace.ordering_violation(),
            Some(TraceOrderingViolation {
                previous_sequence: 4,
                rejected_sequence: 3,
            })
        );
        assert_eq!(trace.events(), vec![event(4, 20), event(5, 40)]);
        assert_eq!(trace.dropped(), 1);
        assert_eq!(
            trace.fingerprint(),
            [event(3, 10), event(4, 20), event(5, 40)]
                .iter()
                .fold(TRACE_FINGERPRINT_OFFSET, fold_trace_fingerprint)
        );
    }

    #[test]
    fn ordering_violation_is_bounded_and_non_panicking_during_drop() {
        struct RecordOnDrop<'a> {
            trace: &'a RecordingTrace,
            event: Option<TraceEvent>,
        }

        impl Drop for RecordOnDrop<'_> {
            fn drop(&mut self) {
                self.trace
                    .record(self.event.take().expect("drop event is present"));
            }
        }

        let trace = RecordingTrace::new(1);

        trace.record(event(3, 10));
        trace.record(event(4, 20));
        drop(RecordOnDrop {
            trace: &trace,
            event: Some(event(4, 30)),
        });
        trace.record(event(2, 40));
        trace.record(event(5, 50));

        assert_eq!(
            trace.ordering_violation(),
            Some(TraceOrderingViolation {
                previous_sequence: 4,
                rejected_sequence: 4,
            })
        );
        assert_eq!(trace.last_sequence(), Some(5));
        assert_eq!(trace.events(), vec![event(3, 10)]);
        assert_eq!(trace.dropped(), 2);
        assert_eq!(
            trace.fingerprint(),
            [event(3, 10), event(4, 20), event(5, 50)]
                .iter()
                .fold(TRACE_FINGERPRINT_OFFSET, fold_trace_fingerprint)
        );
    }

    #[test]
    fn into_events_returns_the_retained_prefix() {
        let trace = RecordingTrace::new(2);
        trace.record(event(8, 80));
        trace.record(event(9, 90));

        assert_eq!(trace.into_events(), vec![event(8, 80), event(9, 90)]);
    }

    #[test]
    fn canonical_fingerprint_has_a_golden_value() {
        let events = [
            event(0, 0),
            TraceEvent::new(
                1,
                SimInstant::from_nanos(5),
                EventKind::TimeAdvanced {
                    from: SimInstant::ZERO,
                    to: SimInstant::from_nanos(5),
                },
            ),
        ];
        let fingerprint = events
            .iter()
            .fold(TRACE_FINGERPRINT_OFFSET, fold_trace_fingerprint);

        assert_eq!(fingerprint, 0x58a6_ed01_6cf6_6955);
    }

    #[test]
    fn every_event_encoding_has_one_canonical_golden_digest() {
        assert_eq!(TRACE_SCHEMA_VERSION, 5);
        let task = TaskId::from_parts(3, 4);
        let parent = TaskId::from_parts(1, 2);
        let timer = TimerId::from_u64(9);
        let at = SimInstant::from_nanos(11);
        let kinds = vec![
            EventKind::RuntimeStarted { seed: 17 },
            EventKind::TaskSpawned {
                task,
                parent: Some(parent),
            },
            EventKind::TaskEnqueued { task, sequence: 5 },
            EventKind::TaskPollStarted { task },
            EventKind::TaskPending { task },
            EventKind::TaskCompleted { task },
            EventKind::TaskCancelled {
                task,
                reason: TaskCancellationReason::ExplicitAbort,
            },
            EventKind::TaskCancelled {
                task,
                reason: TaskCancellationReason::BlockOnFailure,
            },
            EventKind::TaskCancelled {
                task,
                reason: TaskCancellationReason::RuntimeStopped,
            },
            EventKind::TaskPanicked {
                task,
                panic: PanicRecord {
                    message: "boom".to_owned(),
                    message_truncated: false,
                },
            },
            EventKind::TaskDropPanicked {
                task,
                panic: PanicRecord {
                    message: "drop".to_owned(),
                    message_truncated: false,
                },
            },
            EventKind::WakerPanicked {
                task,
                panic: PanicRecord {
                    message: "wake".to_owned(),
                    message_truncated: true,
                },
            },
            EventKind::TimerScheduled {
                id: timer,
                task,
                deadline: SimInstant::from_nanos(23),
            },
            EventKind::TimerFired { id: timer, task },
            EventKind::TimerCancelled { id: timer, task },
            EventKind::TimeAdvanced {
                from: SimInstant::from_nanos(11),
                to: SimInstant::from_nanos(23),
            },
            EventKind::RuntimeStalled { live_tasks: 2 },
            EventKind::BudgetExhausted { steps: 99 },
            EventKind::RuntimeStopped,
            EventKind::RandomChoice {
                stream: RandomStream::Workload,
                choice: RandomChoiceKind::U64,
                draws_before: 0,
                draws_after: 1,
                value: 31,
            },
            EventKind::RandomChoice {
                stream: RandomStream::Fault,
                choice: RandomChoiceKind::Below {
                    upper_exclusive: 10,
                },
                draws_before: 2,
                draws_after: 3,
                value: 7,
            },
            EventKind::RandomChoice {
                stream: RandomStream::Scenario,
                choice: RandomChoiceKind::BoolRatio {
                    numerator: 1,
                    denominator: 4,
                },
                draws_before: 4,
                draws_after: 5,
                value: 1,
            },
        ];
        let fingerprint = kinds.into_iter().enumerate().fold(
            TRACE_FINGERPRINT_OFFSET,
            |fingerprint, (sequence, kind)| {
                fold_trace_fingerprint(fingerprint, &TraceEvent::new(sequence as u64, at, kind))
            },
        );

        assert_eq!(fingerprint, 0x2efa_79c2_77d4_bda5);
    }

    #[test]
    fn panic_truncation_state_changes_the_fingerprint() {
        let task = TaskId::from_parts(1, 0);
        let event = |message_truncated| {
            TraceEvent::new(
                0,
                SimInstant::ZERO,
                EventKind::TaskPanicked {
                    task,
                    panic: PanicRecord {
                        message: "same-prefix".to_owned(),
                        message_truncated,
                    },
                },
            )
        };

        assert_ne!(
            fold_trace_fingerprint(TRACE_FINGERPRINT_OFFSET, &event(false)),
            fold_trace_fingerprint(TRACE_FINGERPRINT_OFFSET, &event(true))
        );
    }
}
