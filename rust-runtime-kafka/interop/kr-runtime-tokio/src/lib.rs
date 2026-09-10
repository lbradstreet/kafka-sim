//! A tokio-shaped facade over the kr-runtime runtimes.
//!
//! Without `--cfg kr_runtime_sim` this crate is a transparent re-export of tokio,
//! so code written against it behaves exactly like code written against
//! tokio itself. With `--cfg kr_runtime_sim` (supplied through `RUSTFLAGS` by
//! simulation builds) the scheduling-sensitive surfaces — `time` and `task` —
//! are reimplemented over the ambient kr-runtime runtime discovered through
//! [`kr_runtime::RuntimeHandle::current`], while executor-independent surfaces
//! (`sync`, the `io` traits, and the combinator macros) still come from
//! tokio because they are plain futures that any executor can poll.
//!
//! No tokio machinery runs under `kr_runtime_sim`: no reactor, no timer wheel, no
//! worker threads. Every sleep is a kr-runtime timer on the owning runtime's
//! deadline map, and every spawn is an owner-local kr-runtime task, so a component
//! written against this crate participates in deterministic simulation with
//! virtual time and seed-stable scheduling.
//!
//! # Divergences under `kr_runtime_sim`
//!
//! tokio's API has no error channel on `spawn` or the time primitives, so
//! conditions kr-runtime reports as typed errors surface here as panics with the
//! underlying error in the message: spawning on a stopped runtime or past its
//! task limit, and timer registration failures. Both executors' tasks are
//! owner-local, so `spawn` deliberately does not require `Send` futures;
//! tokio-compatible callers, whose futures are `Send`, are unaffected.

#[cfg(not(kr_runtime_sim))]
pub use tokio::*;

#[cfg(kr_runtime_sim)]
mod sim;
#[cfg(kr_runtime_sim)]
pub use sim::*;
