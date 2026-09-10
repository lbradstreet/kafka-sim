//! tokio::task-shaped spawning over the ambient kr-runtime runtime.
//!
//! Tasks are owner-local on both kr-runtime executors, so `spawn` deliberately
//! omits tokio's `Send` bound: every tokio-compatible caller, whose futures
//! are `Send`, still compiles, and kr-runtime-native `!Send` futures are accepted
//! as a superset. tokio's `spawn` has no error channel, so conditions kr-runtime
//! reports as a typed [`kr_runtime::SpawnError`] surface here as panics.

use kr_runtime::RuntimeHandle;
use std::any::Any;
use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

/// Spawns a task on the ambient kr-runtime runtime.
///
/// # Panics
///
/// Panics when called outside a kr-runtime task, and when the runtime rejects the
/// spawn (stopped, live-task limit reached, or task identifier space
/// exhausted) — raise `max_tasks` in the runtime configuration rather than
/// treating that panic as recoverable.
pub fn spawn<F>(future: F) -> JoinHandle<F::Output>
where
    F: Future + 'static,
    F::Output: 'static,
{
    let handle = RuntimeHandle::current().unwrap_or_else(|| {
        panic!("task::spawn requires an ambient kr-runtime runtime: call it from inside a kr-runtime task")
    });
    match handle.spawn(future) {
        Ok(join) => JoinHandle { inner: join },
        Err(error) => panic!("kr-runtime-tokio spawn failed: {error}"),
    }
}

/// Spawns an owner-local task; identical to [`spawn`] on kr-runtime runtimes.
///
/// # Panics
///
/// Panics under the same conditions as [`spawn`].
pub fn spawn_local<F>(future: F) -> JoinHandle<F::Output>
where
    F: Future + 'static,
    F::Output: 'static,
{
    spawn(future)
}

/// Runs `function` to completion inside an owner-local task.
///
/// Simulation has no blocking thread pool: real blocking would stall the
/// deterministic scheduler invisibly, so the closure runs on the owner
/// thread as one atomic, zero-virtual-time poll. Model genuinely slow work
/// with virtual delays instead of host-blocking calls.
///
/// # Panics
///
/// Panics under the same conditions as [`spawn`].
pub fn spawn_blocking<F, R>(function: F) -> JoinHandle<R>
where
    F: FnOnce() -> R + 'static,
    R: 'static,
{
    spawn(async move { function() })
}

/// Yields once to the back of the owning runtime's ready queue.
pub async fn yield_now() {
    kr_runtime::yield_now().await;
}

/// An owned permission to join a spawned task.
///
/// Dropping a join handle detaches the task; [`JoinHandle::abort`] cancels
/// it explicitly, matching both kr-runtime and tokio.
pub struct JoinHandle<T> {
    inner: kr_runtime::JoinHandle<T>,
}

impl<T> JoinHandle<T> {
    /// Requests cancellation at the next scheduler boundary.
    pub fn abort(&self) {
        self.inner.abort();
    }

    /// Returns a cancellation capability for this task.
    #[must_use]
    pub fn abort_handle(&self) -> AbortHandle {
        AbortHandle {
            inner: self.inner.abort_handle(),
        }
    }

    /// Returns whether the task has produced a terminal join result.
    #[must_use]
    pub fn is_finished(&self) -> bool {
        self.inner.is_finished()
    }
}

impl<T> Future for JoinHandle<T> {
    type Output = Result<T, JoinError>;

    fn poll(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        Pin::new(&mut self.inner)
            .poll(context)
            .map_err(|inner| JoinError { inner })
    }
}

/// A cloneable cancellation capability detached from the join result.
#[derive(Clone)]
pub struct AbortHandle {
    inner: kr_runtime::AbortHandle,
}

impl AbortHandle {
    /// Requests cancellation at the next scheduler boundary.
    pub fn abort(&self) {
        self.inner.abort();
    }
}

/// The reason a joined task did not produce its output.
///
/// kr-runtime's `RuntimeStopped` join outcome reports as cancelled: from the
/// joiner's perspective the runtime cancelled the task during shutdown.
#[derive(Debug)]
pub struct JoinError {
    inner: kr_runtime::JoinError,
}

impl JoinError {
    /// Returns whether the task was cancelled rather than panicking.
    #[must_use]
    pub fn is_cancelled(&self) -> bool {
        matches!(
            self.inner,
            kr_runtime::JoinError::Cancelled | kr_runtime::JoinError::RuntimeStopped
        )
    }

    /// Returns whether the task panicked.
    #[must_use]
    pub fn is_panic(&self) -> bool {
        matches!(self.inner, kr_runtime::JoinError::Panicked(_))
    }

    /// Returns the panic message as a payload, if the task panicked.
    ///
    /// kr-runtime retains a bounded textual [`kr_runtime::PanicRecord`] rather than the
    /// original payload, so the payload downcasts to `String`.
    #[must_use]
    pub fn try_into_panic(self) -> Option<Box<dyn Any + Send + 'static>> {
        match self.inner {
            kr_runtime::JoinError::Panicked(record) => {
                Some(Box::new(record.message) as Box<dyn Any + Send + 'static>)
            }
            // `kr_runtime::JoinError` is non-exhaustive; every non-panic outcome
            // has no payload to surface.
            _ => None,
        }
    }

    /// Returns the panic payload of a panicked task.
    ///
    /// # Panics
    ///
    /// Panics if the task did not panic; check [`JoinError::is_panic`] first.
    #[must_use]
    pub fn into_panic(self) -> Box<dyn Any + Send + 'static> {
        self.try_into_panic()
            .expect("into_panic called on a join error that is not a panic")
    }
}

impl fmt::Display for JoinError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.inner.fmt(formatter)
    }
}

impl std::error::Error for JoinError {}
