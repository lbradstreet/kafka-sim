//! Concrete single-threaded async runtimes for deterministic simulation and
//! production host I/O.
//!
//! [`SimRuntime`] owns deterministic task ordering and virtual time.
//! [`HostRuntime`] uses host monotonic time and accepts cross-thread wakes while
//! still polling every task on one owner thread.

#![forbid(unsafe_code)]

mod completion;
mod handle;
mod host;
mod panic;
mod sim;
mod task;
mod time;
mod timer;

pub mod rng;
#[cfg(any(test, feature = "test-support"))]
pub mod test_support;
pub mod trace;

pub use completion::{CompletionCertainty, CompletionError, CompletionResult};
pub use handle::RuntimeHandle;
pub use host::{
    HostBlocking, HostBlockingError, HostConfig, HostConfigError, HostControl, HostHandle,
    HostRunError, HostRunErrorKind, HostRuntime, HostRuntimeDiagnostics, HostRuntimeObserver,
    HostSendHandle, HostSendJoinHandle, HostStatus,
};
pub use panic::contain_panic;
pub use sim::{
    DETERMINISM_CHECKPOINT_SCHEMA_VERSION, DeterminismCheckpoint, Handle, PollResult,
    RUNTIME_REPRODUCTION_SCHEMA_VERSION, RandomHandle, RandomStreamSnapshot, RunError,
    RunErrorKind, RunOutcome, RuntimeConfig, RuntimeReproduction, RuntimeSnapshot, SimRuntime,
    SimRuntimeIdentity, Step, TaskSnapshot,
};
pub use task::{
    AbortHandle, JoinError, JoinHandle, MAX_PANIC_MESSAGE_BYTES, PanicRecord, RunErrorDisposition,
    SpawnError, TaskFailure, TaskId, TaskState, YieldNow, current_task_id, yield_now,
};
pub use time::{RuntimeDuration, RuntimeInstant, SimDuration, SimInstant, TimeError};
pub use timer::{Sleep, TimerId};
