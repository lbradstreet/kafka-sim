//! Shared host execution environment for io_uring providers.
//!
//! A cloneable handle, not a trait, per the workspace's capability
//! convention: providers take the environment by value and share the
//! execution resources behind it, the way rings share an attached kernel
//! io-wq. Today it carries the blocking-syscall pool — the userspace
//! analog of that io-wq — and it is the designed seam where reactor and
//! coordinator placement land as later fields, without changing provider
//! signatures again. Placement stays host-only wiring: nothing here
//! appears in the portable `kr-runtime-io` contracts.
//!
//! Jobs are closures that own their completion path: each captures its
//! provider's ingress and pushes its own terminal outcome, so the
//! environment stays generic across providers without a job vocabulary.
//! The worker contains per-job panics — a shared pool must not shrink
//! because one tenant's job unwound — which makes guard-protected
//! terminal reporting part of the job-author contract: a contained panic
//! must still terminalize the command it was running.
//!
//! The queue is deliberately unbounded. Backpressure belongs to per-handle
//! admission in each provider; a shared bound here would let one chatty
//! handle consume every tenant's backpressure, the exact coupling the
//! design forbids. Teardown is drain-then-stop: when the last clone drops,
//! stop sentinels queue behind every admitted job, so each job runs before
//! its worker exits.
//!
//! The workers behind the environment are one of two concrete backings —
//! an enum, not a trait, exactly as [`kr_runtime::RuntimeHandle`] sums concrete
//! executors: the environment's own fleet ([`UringEnv::new`]), or workers
//! the host runtime provisioned ([`UringEnv::on_runtime`] over
//! [`kr_runtime::HostBlocking`]). Providers cannot observe the difference, and a
//! trait here would invite foreign implementations to run provider jobs —
//! foreign code under provider state, which this crate forbids.

use std::collections::VecDeque;
use std::fmt;
use std::sync::{Arc, Condvar, Mutex};
use std::thread::{self, JoinHandle};

use kr_runtime::{HostBlocking, contain_panic};

use crate::support::{join_if_other_thread, lock_unpoisoned};

/// One blocking job: runs on an environment worker, reports its own
/// terminal outcome through state it captured, and must do so through a
/// guard that survives a contained panic.
pub(crate) type BlockingJob = Box<dyn FnOnce() + Send + 'static>;

/// Fixed limits for one shared provider environment.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct UringEnvConfig {
    /// Threads serving blocking lifecycle syscalls for every provider
    /// sharing this environment.
    pub blocking_threads: usize,
}

impl Default for UringEnvConfig {
    fn default() -> Self {
        Self {
            blocking_threads: 2,
        }
    }
}

/// Failure to construct a provider environment.
#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum UringEnvOpenError {
    /// A configuration field is outside its supported range.
    InvalidConfig { field: &'static str, reason: String },
    /// An operating-system interface failed.
    Io {
        action: &'static str,
        raw_os_error: Option<i32>,
        message: String,
    },
}

impl fmt::Display for UringEnvOpenError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidConfig { field, reason } => {
                write!(formatter, "invalid environment config {field}: {reason}")
            }
            Self::Io {
                action,
                raw_os_error,
                message,
            } => {
                write!(formatter, "could not {action}")?;
                if let Some(code) = raw_os_error {
                    write!(formatter, " (OS error {code})")?;
                }
                write!(formatter, ": {message}")
            }
        }
    }
}

impl std::error::Error for UringEnvOpenError {}

enum WorkerMessage {
    Job(BlockingJob),
    Stop,
}

struct BlockingQueue {
    messages: Mutex<VecDeque<WorkerMessage>>,
    available: Condvar,
}

impl BlockingQueue {
    fn push(&self, message: WorkerMessage) {
        lock_unpoisoned(&self.messages).push_back(message);
        self.available.notify_one();
    }
}

fn run_blocking_worker(queue: &BlockingQueue) {
    loop {
        let message = {
            let mut messages = lock_unpoisoned(&queue.messages);
            loop {
                if let Some(message) = messages.pop_front() {
                    break message;
                }
                messages = queue
                    .available
                    .wait(messages)
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
            }
        };
        match message {
            WorkerMessage::Stop => return,
            WorkerMessage::Job(job) => {
                // One tenant's unwinding job must not shrink the shared
                // pool; the job's own guards terminalize its command.
                contain_panic(job);
            }
        }
    }
}

/// The workers and their queue; stopped and joined when the last
/// [`UringEnv`] clone drops.
struct BlockingHost {
    queue: Arc<BlockingQueue>,
    workers: Mutex<Vec<JoinHandle<()>>>,
}

impl Drop for BlockingHost {
    fn drop(&mut self) {
        // Sentinels queue behind every admitted job, so each job runs
        // before its worker stops.
        let workers = std::mem::take(&mut *lock_unpoisoned(&self.workers));
        for _ in 0..workers.len() {
            self.queue.push(WorkerMessage::Stop);
        }
        for worker in workers {
            join_if_other_thread(Some(worker));
        }
    }
}

/// Which concrete worker fleet serves the environment's blocking jobs.
#[derive(Clone)]
enum BlockingBackend {
    /// Environment-owned workers.
    Owned(Arc<BlockingHost>),
    /// Workers the host runtime provisioned; their lifetime follows the
    /// capability clone held here, so tenants keep their drain guarantees
    /// even past runtime drop.
    Runtime(HostBlocking),
}

/// Shared host execution environment for io_uring providers.
///
/// Clones share the environment; providers hold a clone for as long as
/// they may submit work, so the workers structurally outlive every tenant.
#[derive(Clone)]
pub struct UringEnv {
    backend: BlockingBackend,
}

impl UringEnv {
    /// Starts the environment's blocking workers.
    ///
    /// # Errors
    ///
    /// Returns [`UringEnvOpenError`] when the configuration is invalid or
    /// a worker thread cannot be spawned.
    pub fn new(config: UringEnvConfig) -> Result<Self, UringEnvOpenError> {
        if config.blocking_threads == 0 {
            return Err(UringEnvOpenError::InvalidConfig {
                field: "blocking_threads",
                reason: "must be nonzero".to_owned(),
            });
        }
        let queue = Arc::new(BlockingQueue {
            messages: Mutex::new(VecDeque::new()),
            available: Condvar::new(),
        });
        let mut workers = Vec::with_capacity(config.blocking_threads);
        for _ in 0..config.blocking_threads {
            let worker_queue = Arc::clone(&queue);
            let join = thread::Builder::new()
                .name("kr-runtime-io-uring-env-blocking".to_owned())
                .spawn(move || run_blocking_worker(&worker_queue));
            match join {
                Ok(join) => workers.push(join),
                Err(error) => {
                    // A partial environment must not leak parked workers.
                    for _ in 0..workers.len() {
                        queue.push(WorkerMessage::Stop);
                    }
                    for worker in workers {
                        let _ = worker.join();
                    }
                    return Err(UringEnvOpenError::Io {
                        action: "spawn environment blocking worker",
                        raw_os_error: error.raw_os_error(),
                        message: error.to_string(),
                    });
                }
            }
        }
        Ok(Self {
            backend: BlockingBackend::Owned(Arc::new(BlockingHost {
                queue,
                workers: Mutex::new(workers),
            })),
        })
    }

    /// Builds an environment whose jobs run on workers the host runtime
    /// provisioned, from [`kr_runtime::HostRuntime::blocking`].
    ///
    /// Infallible: the capability's workers already exist and live as long
    /// as its clones, so there is no partial state to construct or fail.
    #[must_use]
    pub fn on_runtime(blocking: HostBlocking) -> Self {
        Self {
            backend: BlockingBackend::Runtime(blocking),
        }
    }

    /// Enqueues one blocking job, FIFO per environment.
    pub(crate) fn submit_blocking(&self, job: BlockingJob) {
        match &self.backend {
            BlockingBackend::Owned(host) => host.queue.push(WorkerMessage::Job(job)),
            BlockingBackend::Runtime(blocking) => blocking.submit(job),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::mpsc;

    #[test]
    fn config_rejects_zero_blocking_threads() {
        assert!(matches!(
            UringEnv::new(UringEnvConfig {
                blocking_threads: 0
            }),
            Err(UringEnvOpenError::InvalidConfig {
                field: "blocking_threads",
                ..
            })
        ));
    }

    #[test]
    fn queued_jobs_drain_before_workers_stop() {
        let env = UringEnv::new(UringEnvConfig {
            blocking_threads: 2,
        })
        .expect("create environment");
        let completed = Arc::new(AtomicUsize::new(0));
        for _ in 0..16 {
            let counter = Arc::clone(&completed);
            env.submit_blocking(Box::new(move || {
                counter.fetch_add(1, Ordering::AcqRel);
            }));
        }
        // Dropping the last clone queues the stop sentinels behind every
        // admitted job and joins the workers.
        drop(env);
        assert_eq!(completed.load(Ordering::Acquire), 16);
    }

    #[test]
    fn a_panicking_job_is_contained_and_the_workers_keep_serving() {
        let env = UringEnv::new(UringEnvConfig {
            blocking_threads: 1,
        })
        .expect("create environment");
        // With one worker, every later job completing proves the panicking
        // job did not take a worker down with it.
        let (entered, observe) = mpsc::channel();
        env.submit_blocking(Box::new(move || {
            entered.send(()).expect("report the panicking job started");
            panic!("injected blocking job panic");
        }));
        observe.recv().expect("panicking job ran");
        let completed = Arc::new(AtomicUsize::new(0));
        for _ in 0..4 {
            let counter = Arc::clone(&completed);
            env.submit_blocking(Box::new(move || {
                counter.fetch_add(1, Ordering::AcqRel);
            }));
        }
        drop(env);
        assert_eq!(completed.load(Ordering::Acquire), 4);
    }
}
