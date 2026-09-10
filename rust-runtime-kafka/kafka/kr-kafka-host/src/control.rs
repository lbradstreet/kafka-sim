//! Admission wrapper for the runtime's blocking capability. Use a separately
//! provisioned control capability if data jobs share a host process: this
//! adapter's credits are disjoint, but it cannot reorder a worker's FIFO queue.

use std::sync::{Arc, Mutex, MutexGuard};

use kr_runtime::HostBlocking;
use kr_runtime_io::completion::{SyncOperation, SyncResponder};

use crate::{
    SecurityError,
    sasl::{ScramProof, ScramWork},
};

#[derive(Default)]
struct Usage {
    jobs: usize,
    bytes: usize,
}

struct State {
    blocking: HostBlocking,
    max_jobs: usize,
    max_bytes: usize,
    usage: Mutex<Usage>,
}

/// Explicitly budgeted control jobs. Dropping a response abandons observation,
/// and retains both credits until the actual job is terminal. An unconsumed
/// terminal result also retains credits through the shared completion guard.
#[derive(Clone)]
pub struct ControlJobs {
    state: Arc<State>,
}

impl ControlJobs {
    /// # Errors
    /// Rejects zero job/byte capacities.
    pub fn new(
        blocking: HostBlocking,
        max_jobs: usize,
        max_bytes: usize,
    ) -> Result<Self, SecurityError> {
        if max_jobs == 0 {
            return Err(SecurityError::InvalidConfig {
                field: "control_jobs",
            });
        }
        if max_bytes == 0 {
            return Err(SecurityError::InvalidConfig {
                field: "control_bytes",
            });
        }
        Ok(Self {
            state: Arc::new(State {
                blocking,
                max_jobs,
                max_bytes,
                usage: Mutex::new(Usage::default()),
            }),
        })
    }

    #[must_use]
    pub fn usage(&self) -> (usize, usize) {
        let usage = lock(&self.state.usage);
        (usage.jobs, usage.bytes)
    }

    /// Submits owned proof work after acquiring job and retained-byte credits.
    ///
    /// # Errors
    /// Returns exhaustion before submitting any work to the host fleet.
    pub fn scram(
        &self,
        work: ScramWork,
    ) -> Result<SyncOperation<Result<ScramProof, SecurityError>>, SecurityError> {
        let bytes = work.retained_bytes()?;
        self.submit(bytes, move || work.compute())
    }

    /// Submit a bounded control operation (for example a DNS or auth job).
    /// `retained_bytes` must include input allocations, worst-case output and
    /// scratch; the closure must not retain additional unaccounted work.
    ///
    /// # Errors
    /// Rejects exhausted admission before submission. A worker panic is a typed
    /// terminal response and cannot strand credits or a waiter.
    pub fn submit<T: Send + 'static>(
        &self,
        retained_bytes: usize,
        work: impl FnOnce() -> Result<T, SecurityError> + Send + 'static,
    ) -> Result<SyncOperation<Result<T, SecurityError>>, SecurityError> {
        self.submit_guarded(retained_bytes, (), work)
    }

    /// Retains a caller's passive shared budget guard through actual worker
    /// completion and consumption or abandonment of its terminal output.
    pub fn submit_guarded<T: Send + 'static, G: Send + 'static>(
        &self,
        retained_bytes: usize,
        guard: G,
        work: impl FnOnce() -> Result<T, SecurityError> + Send + 'static,
    ) -> Result<SyncOperation<Result<T, SecurityError>>, SecurityError> {
        {
            let mut usage = lock(&self.state.usage);
            let jobs = usage
                .jobs
                .checked_add(1)
                .filter(|n| *n <= self.state.max_jobs)
                .ok_or(SecurityError::ResourceExhausted {
                    resource: "control jobs",
                    limit: self.state.max_jobs,
                })?;
            let bytes = usage
                .bytes
                .checked_add(retained_bytes)
                .filter(|n| *n <= self.state.max_bytes)
                .ok_or(SecurityError::ResourceExhausted {
                    resource: "control bytes",
                    limit: self.state.max_bytes,
                })?;
            usage.jobs = jobs;
            usage.bytes = bytes;
        }
        let (response, sender) = SyncOperation::channel_with_guard((
            Permit {
                state: self.state.clone(),
                bytes: retained_bytes,
            },
            guard,
        ));
        let terminal = Terminal(Some(sender));
        self.state.blocking.submit(move || {
            let mut output = Err(SecurityError::WorkerPanicked);
            kr_runtime::contain_panic(|| output = work());
            terminal.complete(output);
        });
        Ok(response)
    }
}

struct Permit {
    state: Arc<State>,
    bytes: usize,
}

impl Drop for Permit {
    fn drop(&mut self) {
        let mut usage = lock(&self.state.usage);
        usage.jobs -= 1;
        usage.bytes -= self.bytes;
    }
}

struct Terminal<T>(Option<SyncResponder<Result<T, SecurityError>>>);

impl<T> Terminal<T> {
    fn complete(mut self, output: Result<T, SecurityError>) {
        if let Some(sender) = self.0.take() {
            sender.complete(output);
        }
    }
}

impl<T> Drop for Terminal<T> {
    fn drop(&mut self) {
        if let Some(sender) = self.0.take() {
            sender.complete(Err(SecurityError::WorkerPanicked));
        }
    }
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}
