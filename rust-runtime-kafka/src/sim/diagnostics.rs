use super::RuntimeConfig;
use crate::rng::{RandomStream, RngCheckpoint};
use crate::task::{PanicRecord, RunErrorDisposition, SpawnError, TaskFailure, TaskId, TaskState};
use crate::time::SimInstant;
use std::fmt;

/// Version of the kernel-owned reproduction-input fragment.
///
/// A complete reproduction manifest is owned by the simulation harness and
/// must additionally pin its driver, workload, fault plan, budgets, and input
/// state. This version covers only [`RuntimeConfig`] and the deterministic RNG
/// contract exposed by [`RuntimeReproduction`]. Version 3 added the
/// configured virtual start time to [`RuntimeConfig`].
pub const RUNTIME_REPRODUCTION_SCHEMA_VERSION: u32 = 3;

/// Version of the terminal determinism-checkpoint contract.
pub const DETERMINISM_CHECKPOINT_SCHEMA_VERSION: u32 = 3;

/// A bounded diagnostic description of one live task.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TaskSnapshot {
    /// Stable task identifier.
    pub id: TaskId,
    /// Current lifecycle state.
    pub state: TaskState,
}

/// A deterministic snapshot used to diagnose stalls and budget exhaustion.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RuntimeSnapshot {
    /// Kernel-owned fragment of the inputs required to reproduce this run.
    pub reproduction: RuntimeReproduction,
    /// Current virtual time.
    pub now: SimInstant,
    /// Total scheduler actions performed during the runtime lifetime.
    pub total_steps: u64,
    /// Next ready-queue sequence, equal to the total number of successful task enqueues.
    pub next_enqueue_sequence: u64,
    /// Next timer ordering sequence, equal to the total number of successful registrations.
    pub next_timer_sequence: u64,
    /// Next timer identifier, after all successful timer registrations.
    pub next_timer_id: u64,
    /// Number of ready entries.
    pub ready_tasks: usize,
    /// Number of live timers.
    pub live_timers: usize,
    /// Stable positions of all domain-separated random streams.
    pub random: Vec<RandomStreamSnapshot>,
    /// All live tasks, ordered by task identifier.
    pub tasks: Vec<TaskSnapshot>,
    /// Whether the runtime has entered its terminal stopped state.
    pub stopped: bool,
}

impl RuntimeSnapshot {
    /// Builds a cheap terminal canary for comparing deterministic reruns.
    ///
    /// This excludes the `Debug` random stream. It is not a replay manifest or
    /// correctness proof: callers must also compare the harness outcome and
    /// application/model-checker result.
    #[must_use]
    pub fn determinism_checkpoint(&self) -> DeterminismCheckpoint {
        DeterminismCheckpoint {
            schema_version: DETERMINISM_CHECKPOINT_SCHEMA_VERSION,
            reproduction: self.reproduction.clone(),
            now: self.now,
            total_steps: self.total_steps,
            next_enqueue_sequence: self.next_enqueue_sequence,
            next_timer_sequence: self.next_timer_sequence,
            next_timer_id: self.next_timer_id,
            ready_tasks: self.ready_tasks,
            live_timers: self.live_timers,
            live_tasks: self.tasks.len(),
            stopped: self.stopped,
            random: self
                .random
                .iter()
                .copied()
                .filter(|entry| entry.stream != RandomStream::Debug)
                .collect(),
        }
    }
}

/// Kernel-owned reproduction inputs.
///
/// Harnesses should embed this value in a broader, independently versioned
/// manifest that also identifies the driver and scenario-specific inputs.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RuntimeReproduction {
    /// Schema of this kernel-owned reproduction fragment.
    pub schema_version: u32,
    /// Version of the generator, stream derivation, and choice mappings.
    pub rng_version: u32,
    /// Exact runtime configuration supplied at construction.
    pub config: RuntimeConfig,
}

/// Cheap terminal canary for verifying a deterministic rerun.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DeterminismCheckpoint {
    /// Schema of this checkpoint representation.
    pub schema_version: u32,
    /// Kernel-owned reproduction inputs used by the run.
    pub reproduction: RuntimeReproduction,
    /// Terminal virtual time.
    pub now: SimInstant,
    /// Terminal scheduler-action count.
    pub total_steps: u64,
    /// Total successful task enqueues during the run.
    pub next_enqueue_sequence: u64,
    /// Total successful timer registrations used for timer ordering.
    pub next_timer_sequence: u64,
    /// Next timer identifier after all successful registrations.
    pub next_timer_id: u64,
    /// Terminal ready-entry count.
    pub ready_tasks: usize,
    /// Terminal live-timer count.
    pub live_timers: usize,
    /// Terminal live-task count.
    pub live_tasks: usize,
    /// Whether the runtime reached its stopped state.
    pub stopped: bool,
    /// Behavioral random-stream positions, excluding the diagnostic stream.
    pub random: Vec<RandomStreamSnapshot>,
}

/// One deterministic random stream's replay position.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RandomStreamSnapshot {
    /// Choice domain.
    pub stream: RandomStream,
    /// Resumable generator state and primitive draw count.
    pub checkpoint: RngCheckpoint,
}

/// A deterministic runtime failure category.
#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum RunErrorKind {
    /// The root future cannot make progress because no event can wake it.
    Stalled,
    /// A driving call exhausted its scheduler-action budget.
    StepBudgetExceeded { limit: u64 },
    /// The next event lies beyond the configured virtual-time limit.
    TimeLimitExceeded {
        limit: SimInstant,
        next_event: SimInstant,
    },
    /// A task panicked; simulation panics are fatal by default.
    TaskPanicked { task: TaskId, panic: PanicRecord },
    /// Dropping a task future panicked at a runtime lifecycle boundary.
    TaskDropPanicked { task: TaskId, panic: PanicRecord },
    /// A future's registered waker panicked while being notified.
    WakerPanicked { task: TaskId, panic: PanicRecord },
    /// A task waker was invoked from another OS thread.
    NondeterministicExternalWake { task: TaskId },
    /// Driving a runtime from inside another runtime poll is unsupported.
    ReentrantDrive,
    /// The root task could not be spawned.
    RootSpawnFailed(SpawnError),
    /// The runtime stopped before the root task completed.
    RuntimeStopped,
    /// The root task was explicitly cancelled before completing.
    RootCancelled,
    /// A scoped root marker escaped its owning `block_on` drive.
    ScopedRootUnavailable { task: TaskId },
    /// An internal monotonic sequence or identifier was exhausted.
    SequenceExhausted,
}

impl RunErrorKind {
    /// Classifies whether deterministic driving may continue after this error.
    #[must_use]
    pub const fn disposition(&self) -> RunErrorDisposition {
        match self {
            Self::Stalled
            | Self::StepBudgetExceeded { .. }
            | Self::TimeLimitExceeded { .. }
            | Self::ReentrantDrive
            | Self::RootCancelled => RunErrorDisposition::Resumable,
            Self::RootSpawnFailed(error) => match error {
                SpawnError::ResourceExhausted { .. } => RunErrorDisposition::Resumable,
                SpawnError::RuntimeStopped => RunErrorDisposition::Terminal,
                SpawnError::IdentifierExhausted => RunErrorDisposition::Fatal,
            },
            Self::RuntimeStopped => RunErrorDisposition::Terminal,
            Self::TaskPanicked { .. }
            | Self::TaskDropPanicked { .. }
            | Self::WakerPanicked { .. }
            | Self::NondeterministicExternalWake { .. }
            | Self::ScopedRootUnavailable { .. }
            | Self::SequenceExhausted => RunErrorDisposition::Fatal,
        }
    }
}

pub(super) fn run_error_kind(failure: TaskFailure) -> RunErrorKind {
    match failure {
        TaskFailure::Panicked { task, panic } => RunErrorKind::TaskPanicked { task, panic },
        TaskFailure::DropPanicked { task, panic } => RunErrorKind::TaskDropPanicked { task, panic },
        TaskFailure::WakerPanicked { task, panic } => RunErrorKind::WakerPanicked { task, panic },
        TaskFailure::SequenceExhausted => RunErrorKind::SequenceExhausted,
    }
}

/// A runtime error with the state at the point of failure.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RunError {
    /// Machine-readable failure category.
    pub kind: RunErrorKind,
    /// Deterministic runtime state at failure.
    pub snapshot: Box<RuntimeSnapshot>,
    /// A secondary failure encountered while cancelling a failed `block_on`
    /// root, without replacing the initiating failure or its snapshot.
    pub cleanup_failure: Option<Box<RunError>>,
}

impl RunError {
    pub(super) fn new(kind: RunErrorKind, snapshot: RuntimeSnapshot) -> Self {
        Self {
            kind,
            snapshot: Box::new(snapshot),
            cleanup_failure: None,
        }
    }

    /// Classifies the whole error, including a secondary root-cleanup failure.
    #[must_use]
    pub fn disposition(&self) -> RunErrorDisposition {
        if self
            .cleanup_failure
            .as_deref()
            .is_some_and(|cleanup| cleanup.disposition() == RunErrorDisposition::Fatal)
        {
            RunErrorDisposition::Fatal
        } else {
            self.kind.disposition()
        }
    }
}

impl fmt::Display for RunError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{:?} at {}", self.kind, self.snapshot.now)?;
        if let Some(cleanup) = &self.cleanup_failure {
            write!(formatter, "; root cleanup also failed: {cleanup}")?;
        }
        Ok(())
    }
}

impl std::error::Error for RunError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.cleanup_failure
            .as_deref()
            .map(|cleanup| cleanup as &(dyn std::error::Error + 'static))
    }
}

/// The result of driving all available deterministic work.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RunOutcome {
    /// No live tasks remain.
    Idle(RuntimeSnapshot),
    /// Live tasks remain, but no ready task or timer can make progress.
    Stalled(RuntimeSnapshot),
    /// The runtime has entered its terminal stopped state.
    Stopped(RuntimeSnapshot),
}
