//! The simulated tokio surface, active under `--cfg kr_runtime_sim`.

pub mod net;
pub mod task;
pub mod time;

// Executor-independent surfaces are real tokio even under simulation: they
// are plain futures and macros that wake through whatever waker the polling
// runtime installed, so kr-runtime's single-owner executors drive them unchanged
// and deterministically.
pub use tokio::{io, join, pin, select, sync, try_join};

pub use task::spawn;
