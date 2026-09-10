//! The runtime-portable task capability shared by both executors.

use crate::host::HostHandle;
use crate::rng::{RandomError, RngCheckpoint};
use crate::sim::Handle;
use crate::task::{JoinHandle, SpawnError, TaskId, current_task_for_host, current_task_for_sim};
use crate::time::{RuntimeDuration, RuntimeInstant, TimeError};
use crate::timer::{Sleep, TimerRegistration};
use std::future::Future;
use std::rc::Rc;
use std::task::Waker;

/// A cloneable task capability that is valid on either concrete executor.
///
/// [`Handle`] and [`HostHandle`] expose the same task-facing operations over
/// the same shared types ([`JoinHandle`], [`Sleep`], [`crate::YieldNow`],
/// [`RuntimeInstant`], and [`RuntimeDuration`]). `RuntimeHandle` is the
/// portable sum of those two concrete capabilities, so one application actor
/// can be written once and started on a deterministic simulation or a host
/// runtime by its wiring layer.
///
/// This is deliberately not an executor trait: runtime construction, driving,
/// and cross-thread admission remain concrete and executor-specific. The
/// variants are public so wiring code can still reach executor-only surfaces
/// (for example simulation snapshots) after matching.
#[derive(Clone)]
pub enum RuntimeHandle {
    /// A deterministic simulation capability.
    Sim(Handle),
    /// An owner-local host capability.
    Host(HostHandle),
}

impl RuntimeHandle {
    /// Returns the handle for the task currently being polled, if any.
    #[must_use]
    pub fn current() -> Option<Self> {
        if let Some(handle) = Handle::current() {
            return Some(Self::Sim(handle));
        }
        HostHandle::current().map(Self::Host)
    }

    /// Returns the current instant on the owning runtime's timeline.
    ///
    /// Simulation reports virtual time; host execution reports monotonic
    /// elapsed time since runtime construction.
    #[must_use]
    pub fn now(&self) -> RuntimeInstant {
        match self {
            Self::Sim(handle) => handle.now(),
            Self::Host(handle) => handle.now(),
        }
    }

    /// Spawns an owner-local task. The future and output may be `!Send`.
    ///
    /// # Errors
    ///
    /// Returns [`SpawnError`] if the runtime is stopped, its live-task limit is
    /// reached, or its task identifier space is exhausted.
    pub fn spawn<F>(&self, future: F) -> Result<JoinHandle<F::Output>, SpawnError>
    where
        F: Future + 'static,
        F::Output: 'static,
    {
        match self {
            Self::Sim(handle) => handle.spawn(future),
            Self::Host(handle) => handle.spawn(future),
        }
    }

    /// Creates a timer relative to the current instant.
    #[must_use]
    pub fn sleep(&self, duration: RuntimeDuration) -> Sleep {
        match self {
            Self::Sim(handle) => handle.sleep(duration),
            Self::Host(handle) => handle.sleep(duration),
        }
    }

    /// Creates a timer for an absolute runtime-relative instant.
    #[must_use]
    pub fn sleep_until(&self, deadline: RuntimeInstant) -> Sleep {
        match self {
            Self::Sim(handle) => handle.sleep_until(deadline),
            Self::Host(handle) => handle.sleep_until(deadline),
        }
    }

    /// Returns one seeded workload value.
    ///
    /// # Errors
    ///
    /// Returns [`RandomError::RuntimeStopped`] without consuming a draw after
    /// the owning runtime enters its terminal state.
    pub fn random_u64(&self) -> Result<u64, RandomError> {
        match self {
            Self::Sim(handle) => handle.random_u64(),
            Self::Host(handle) => handle.random_u64(),
        }
    }

    /// Uniformly chooses from `0..upper_exclusive`.
    ///
    /// # Errors
    ///
    /// Returns [`RandomError::ZeroUpperBound`] when `upper_exclusive` is zero,
    /// or [`RandomError::RuntimeStopped`] after terminal shutdown. Neither
    /// failure consumes a draw. Argument validation takes precedence over the
    /// stopped-state check.
    pub fn random_below(&self, upper_exclusive: u64) -> Result<u64, RandomError> {
        match self {
            Self::Sim(handle) => handle.random_below(upper_exclusive),
            Self::Host(handle) => handle.random_below(upper_exclusive),
        }
    }

    /// Makes an exact rational boolean choice.
    ///
    /// # Errors
    ///
    /// Returns [`RandomError::InvalidRatio`] when the ratio is invalid, or
    /// [`RandomError::RuntimeStopped`] after terminal shutdown. Neither failure
    /// consumes a draw. Argument validation takes precedence over the
    /// stopped-state check.
    pub fn random_bool_ratio(&self, numerator: u64, denominator: u64) -> Result<bool, RandomError> {
        match self {
            Self::Sim(handle) => handle.random_bool_ratio(numerator, denominator),
            Self::Host(handle) => handle.random_bool_ratio(numerator, denominator),
        }
    }

    /// Returns the workload stream's diagnostic position without consuming a
    /// choice.
    #[must_use]
    pub fn random_position(&self) -> RngCheckpoint {
        match self {
            Self::Sim(handle) => handle.random_position(),
            Self::Host(handle) => handle.random_position(),
        }
    }
}

/// Crate-internal timer routing used by [`Sleep`].
///
/// A sleep retains the handle that created it so it can identify its owning
/// runtime, re-read that runtime's clock, and register or cancel its timer.
impl RuntimeHandle {
    pub(crate) fn is_stopped(&self) -> bool {
        match self {
            Self::Sim(handle) => handle.shared.is_stopped(),
            Self::Host(handle) => handle.shared.is_stopped(),
        }
    }

    pub(crate) fn current_task(&self) -> Option<TaskId> {
        match self {
            Self::Sim(handle) => current_task_for_sim(&handle.shared),
            Self::Host(handle) => current_task_for_host(&handle.shared),
        }
    }

    pub(crate) fn register_timer(
        &self,
        task: TaskId,
        deadline: RuntimeInstant,
        waker: &Waker,
    ) -> Result<Rc<TimerRegistration>, TimeError> {
        match self {
            Self::Sim(handle) => handle.shared.register_timer(task, deadline, waker),
            Self::Host(handle) => handle.shared.register_timer(task, deadline, waker),
        }
    }

    pub(crate) fn cancel_timer(&self, registration: &TimerRegistration) {
        match self {
            Self::Sim(handle) => handle.shared.cancel_timer(registration),
            Self::Host(handle) => handle.shared.cancel_timer(registration),
        }
    }
}

impl From<Handle> for RuntimeHandle {
    fn from(handle: Handle) -> Self {
        Self::Sim(handle)
    }
}

impl From<HostHandle> for RuntimeHandle {
    fn from(handle: HostHandle) -> Self {
        Self::Host(handle)
    }
}
