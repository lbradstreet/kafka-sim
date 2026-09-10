//! Deterministic scheduling, virtual-time, tracing, and failure policy.

mod diagnostics;

pub use diagnostics::{
    DETERMINISM_CHECKPOINT_SCHEMA_VERSION, DeterminismCheckpoint,
    RUNTIME_REPRODUCTION_SCHEMA_VERSION, RandomStreamSnapshot, RunError, RunErrorKind, RunOutcome,
    RuntimeReproduction, RuntimeSnapshot, TaskSnapshot,
};

use diagnostics::run_error_kind;

use crate::handle::RuntimeHandle;
use crate::panic::{contain_panic, panic_record_from_payload};
use crate::rng::{
    DETERMINISTIC_RNG_VERSION, DeterministicRng, RandomError, RandomStream, RngCheckpoint,
};
use crate::task::{
    AbortHandle, BoxTaskFuture, CurrentGuard, ErasedJoinState, JoinError, JoinHandle, JoinState,
    PanicRecord, ReadyTask, RunErrorDisposition, SpawnError, Task, TaskFailure, TaskHarness,
    TaskId, TaskJoin, TaskSlab, TaskSlabError, TaskState, current_sim_shared, current_task_for_sim,
    current_task_id, drop_value_caught, drop_value_result,
};
use crate::time::{SimDuration, SimInstant, TimeError};
use crate::timer::{Sleep, TimerId, TimerRegistration, TimerStore};
use crate::trace::{
    EventKind, EventKindTag, RandomChoiceKind, TaskCancellationReason, TraceEvent, TraceSink,
};
use std::cell::{Cell, RefCell};
use std::collections::{BTreeMap, VecDeque};
use std::future::Future;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::pin::Pin;
use std::rc::Rc;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering as AtomicOrdering};
use std::task::{Context, Poll, Wake, Waker};
use std::thread::{self, ThreadId};

const NO_PENDING_WAKE: u64 = u64::MAX;

type OwnerWakeQueue = RefCell<VecDeque<TaskId>>;

thread_local! {
    // Waker targets must be Send + Sync, so their owner-local queues cannot
    // live in WakeBridge. The owner alone resolves a bridge's stable address;
    // foreign threads are rejected before accessing this registry. Weak
    // entries cannot retain a runtime, and shutdown removes them before any
    // user cleanup runs. No address can be reused while its bridge is alive.
    static OWNER_WAKE_QUEUES: RefCell<BTreeMap<usize, std::rc::Weak<OwnerWakeQueue>>> =
        const { RefCell::new(BTreeMap::new()) };
}

/// Resource and driving limits for a runtime.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RuntimeConfig {
    /// Root seed from which all behavioral streams are derived.
    pub seed: u64,
    /// Maximum number of task slots live at one time.
    pub max_tasks: usize,
    /// Maximum number of live timers.
    pub max_timers: usize,
    /// Maximum scheduler actions performed by one driving call.
    pub max_steps_per_run: u64,
    /// Optional maximum virtual instant that the scheduler may enter.
    pub max_time: Option<SimInstant>,
    /// Initial virtual instant, observed by every task before any advance.
    ///
    /// Defaults to [`SimInstant::ZERO`]. Harnesses that want to flush out
    /// absolute-time assumptions should start each run at
    /// [`RuntimeConfig::derived_start_time`] for its seed instead of zero.
    pub start_time: SimInstant,
}

impl RuntimeConfig {
    /// Derives a nonzero per-seed virtual start time.
    ///
    /// The mapping consumes no random-stream draws and is pinned by
    /// [`START_TIME_DERIVATION_VERSION`](crate::rng::START_TIME_DERIVATION_VERSION);
    /// the exact instant a run used is recorded in its
    /// [`RuntimeReproduction`] configuration.
    #[must_use]
    pub const fn derived_start_time(root_seed: u64) -> SimInstant {
        SimInstant::from_nanos(crate::rng::derive_start_time_nanos(root_seed))
    }
}

impl Default for RuntimeConfig {
    fn default() -> Self {
        Self {
            seed: 0,
            max_tasks: 100_000,
            max_timers: 100_000,
            max_steps_per_run: 1_000_000,
            max_time: None,
            start_time: SimInstant::ZERO,
        }
    }
}

/// The result of one task poll.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PollResult {
    /// The future returned `Pending`.
    Pending,
    /// The future returned `Ready`.
    Ready,
}

/// One inspectable scheduler action.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Step {
    /// Exactly one task was polled.
    TaskPolled { task: TaskId, result: PollResult },
    /// Exactly one queued cancellation was applied.
    TaskCancelled { task: TaskId },
    /// Time jumped to the earliest event and all timers there fired in order.
    TimeAdvanced {
        from: SimInstant,
        to: SimInstant,
        timers: Vec<TimerId>,
    },
    /// No live task remains.
    Idle,
    /// Tasks remain, but there is no ready work or future timer.
    Stalled,
    /// The runtime has entered its terminal stopped state.
    Stopped,
}

/// A cloneable capability for spawning tasks and using virtual time.
#[derive(Clone)]
pub struct Handle {
    pub(crate) shared: Rc<Shared>,
}

/// Opaque identity of a simulation runtime, safe to retain in a shared ingress
/// handle. It provides no scheduling, clock, wake, or random capability and does
/// not keep the runtime alive. Identity comparisons admit no work or draws.
#[derive(Clone)]
pub struct SimRuntimeIdentity {
    bridge: std::sync::Weak<WakeBridge>,
}

impl SimRuntimeIdentity {
    /// Tests whether a handle belongs to this exact runtime instance. A new
    /// runtime with the same seed and initial instant is a different instance.
    #[must_use]
    pub fn belongs_to(&self, handle: &Handle) -> bool {
        self.bridge.ptr_eq(&Arc::downgrade(&handle.shared.wakes))
    }
}

impl std::fmt::Debug for SimRuntimeIdentity {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("SimRuntimeIdentity")
    }
}

impl Handle {
    /// Returns a passive identity token without retaining owner-local state.
    #[must_use]
    pub fn identity(&self) -> SimRuntimeIdentity {
        SimRuntimeIdentity {
            bridge: Arc::downgrade(&self.shared.wakes),
        }
    }

    /// Returns the handle for the task currently being polled, if any.
    #[must_use]
    pub fn current() -> Option<Self> {
        current_sim_shared().map(|shared| Self { shared })
    }

    /// Returns the current virtual instant.
    #[must_use]
    pub fn now(&self) -> SimInstant {
        self.shared.state.borrow().now
    }

    /// Spawns a task on the runtime's FIFO scheduler.
    ///
    /// # Errors
    ///
    /// Returns [`SpawnError`] if the runtime is stopped, its live-task limit is
    /// reached, or the generational task identifier space is exhausted.
    pub fn spawn<F>(&self, future: F) -> Result<JoinHandle<F::Output>, SpawnError>
    where
        F: Future + 'static,
        F::Output: 'static,
    {
        self.shared.spawn(future)
    }

    /// Creates a timer relative to the current virtual instant.
    #[must_use]
    pub fn sleep(&self, duration: SimDuration) -> Sleep {
        let deadline = self
            .now()
            .checked_add(duration)
            .ok_or(TimeError::DeadlineOverflow);
        Sleep::new(RuntimeHandle::Sim(self.clone()), deadline)
    }

    /// Creates a timer for an absolute virtual instant.
    #[must_use]
    pub fn sleep_until(&self, deadline: SimInstant) -> Sleep {
        Sleep::new(RuntimeHandle::Sim(self.clone()), Ok(deadline))
    }

    /// Returns one deterministic workload value.
    ///
    /// # Errors
    ///
    /// Returns [`RandomError::RuntimeStopped`] without consuming a draw after
    /// the owning runtime enters its terminal state.
    pub fn random_u64(&self) -> Result<u64, RandomError> {
        self.shared.random_u64(RandomStream::Workload)
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
        self.shared
            .random_below(RandomStream::Workload, upper_exclusive)
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
        self.shared
            .random_bool_ratio(RandomStream::Workload, numerator, denominator)
    }

    /// Returns the workload stream's diagnostic position without consuming a
    /// choice.
    #[must_use]
    pub fn random_position(&self) -> RngCheckpoint {
        self.shared.random_checkpoint(RandomStream::Workload)
    }

    /// Returns a deterministic snapshot of the runtime.
    #[must_use]
    pub fn snapshot(&self) -> RuntimeSnapshot {
        self.shared.snapshot()
    }
}

/// A cloneable deterministic source bound to one privileged choice domain.
///
/// Ordinary task [`Handle`]s are bound to [`RandomStream::Workload`]. The
/// runtime controller can hand a `RandomHandle` to scenario or fault layers
/// without allowing application actors to consume those streams accidentally.
#[derive(Clone)]
pub struct RandomHandle {
    shared: Rc<Shared>,
    stream: RandomStream,
}

impl RandomHandle {
    /// Returns the source's fixed choice domain.
    #[must_use]
    pub const fn stream(&self) -> RandomStream {
        self.stream
    }

    /// Returns one deterministic 64-bit value.
    ///
    /// # Errors
    ///
    /// Returns [`RandomError::RuntimeStopped`] without consuming a draw after
    /// the owning runtime enters its terminal state.
    pub fn random_u64(&self) -> Result<u64, RandomError> {
        self.shared.random_u64(self.stream)
    }

    /// Uniformly chooses from `0..upper_exclusive`.
    ///
    /// # Errors
    ///
    /// Returns a validation error for a zero bound, or
    /// [`RandomError::RuntimeStopped`] after terminal shutdown, without
    /// consuming a draw. Argument validation takes precedence over the
    /// stopped-state check.
    pub fn random_below(&self, upper_exclusive: u64) -> Result<u64, RandomError> {
        self.shared.random_below(self.stream, upper_exclusive)
    }

    /// Makes an exact rational boolean choice.
    ///
    /// # Errors
    ///
    /// Returns a validation error for an invalid ratio, or
    /// [`RandomError::RuntimeStopped`] after terminal shutdown, without
    /// consuming a draw. Argument validation takes precedence over the
    /// stopped-state check.
    pub fn random_bool_ratio(&self, numerator: u64, denominator: u64) -> Result<bool, RandomError> {
        self.shared
            .random_bool_ratio(self.stream, numerator, denominator)
    }

    /// Returns the stream's diagnostic position without consuming a choice.
    #[must_use]
    pub fn random_position(&self) -> RngCheckpoint {
        self.shared.random_checkpoint(self.stream)
    }
}

/// The single-owner deterministic executor controller.
///
/// Wakes are coalesced in per-task atomics and cancellations use an owner-local
/// queue. Foreign-thread waker ingress surfaces from later runtime driving.
pub struct SimRuntime {
    shared: Rc<Shared>,
}

impl Default for SimRuntime {
    fn default() -> Self {
        Self::new(RuntimeConfig::default())
    }
}

impl SimRuntime {
    /// Creates a runtime at its configured start time (virtual time zero by
    /// default).
    ///
    /// Diagnostic tracing is disabled. The runtime does not construct trace
    /// events, assign trace sequence numbers, or compute a trace fingerprint.
    ///
    /// # Panics
    ///
    /// Panics if `config.start_time` exceeds `config.max_time`: such a
    /// runtime would begin beyond the last instant it may enter.
    #[must_use]
    pub fn new(config: RuntimeConfig) -> Self {
        Self::build(config, None)
    }

    /// Creates a runtime with a passive structured trace sink.
    ///
    /// The sink observes structured runtime events but cannot influence event
    /// ordering. It may reject a candidate through [`TraceSink::should_record`]
    /// before event construction, and must not call back into this runtime from
    /// either trace-sink method.
    ///
    /// # Panics
    ///
    /// Panics if `config.start_time` exceeds `config.max_time`, as for
    /// [`SimRuntime::new`].
    #[must_use]
    pub fn with_trace(config: RuntimeConfig, trace: Rc<dyn TraceSink>) -> Self {
        let trace = trace.enabled().then_some(trace);
        Self::build(config, trace)
    }

    fn build(config: RuntimeConfig, trace: Option<Rc<dyn TraceSink>>) -> Self {
        let owner = thread::current().id();
        let wakes = Arc::new(WakeBridge::new(owner));
        let runtime = Self {
            shared: Rc::new(Shared {
                wakes,
                pending_wakes: Rc::new(RefCell::new(VecDeque::new())),
                pending_aborts: RefCell::new(VecDeque::new()),
                state: RefCell::new(State::new(config)),
                fatal_error: RefCell::new(None),
                trace: trace.map(|sink| TraceDriver {
                    sink,
                    next_sequence: Cell::new(0),
                    last_stall: Cell::new(None),
                }),
            }),
        };
        OWNER_WAKE_QUEUES.with(|queues| {
            queues.borrow_mut().insert(
                Arc::as_ptr(&runtime.shared.wakes) as usize,
                Rc::downgrade(&runtime.shared.pending_wakes),
            );
        });
        runtime
            .shared
            .emit(EventKindTag::RuntimeStarted, || EventKind::RuntimeStarted {
                seed: runtime.shared.state.borrow().config.seed,
            });
        runtime
    }

    /// Returns a cloneable runtime capability.
    #[must_use]
    pub fn handle(&self) -> Handle {
        Handle {
            shared: Rc::clone(&self.shared),
        }
    }

    /// Returns a deterministic source scoped to a subsystem choice domain.
    ///
    /// Keep schedule/scenario/fault sources in their owning harness layers.
    /// A diagnostic source is isolated from other streams, but its values must
    /// still never drive simulated behavior.
    #[must_use]
    pub fn random_source(&self, stream: RandomStream) -> RandomHandle {
        RandomHandle {
            shared: Rc::clone(&self.shared),
            stream,
        }
    }

    /// Returns a deterministic snapshot without driving the runtime.
    #[must_use]
    pub fn snapshot(&self) -> RuntimeSnapshot {
        self.shared.snapshot()
    }

    /// Stops the runtime and deterministically tears down all remaining tasks.
    ///
    /// Every join is resolved before any future is dropped. Each future is then
    /// dropped while publishing its task identity and inside a panic boundary.
    ///
    /// # Errors
    ///
    /// Returns a structured error when a task future's destructor or a
    /// registered waker panics. All other tasks are still torn down before the
    /// error is returned. Repeated calls return any retained fatal error or
    /// otherwise succeed without repeating teardown.
    pub fn shutdown(&mut self) -> Result<(), RunError> {
        if let Some(error) = self.shared.shutdown() {
            return Err(error);
        }
        Ok(())
    }

    /// Consumes this runtime and checks deterministic teardown.
    ///
    /// Prefer this operation when relinquishing the runtime. Dropping a
    /// runtime also tears it down, but [`Drop`] cannot surface destructor or
    /// registered-waker failures.
    ///
    /// # Errors
    ///
    /// Returns any retained fatal error, or the first teardown failure after all
    /// remaining tasks have a terminal join result and their futures have been
    /// dropped.
    pub fn finish(mut self) -> Result<(), RunError> {
        self.shutdown()
    }

    /// Performs one scheduler action.
    ///
    /// A task poll and a time jump are distinct actions. Ready tasks always run
    /// before virtual time advances.
    ///
    /// # Errors
    ///
    /// Returns [`RunError`] when the scheduler action cannot complete. Fatal
    /// errors are retained for later fallible runtime operations.
    pub fn step(&mut self) -> Result<Step, RunError> {
        self.step_with_scoped_root(None)
    }

    fn step_with_scoped_root(
        &mut self,
        scoped_root: Option<&mut dyn ScopedRootDriver>,
    ) -> Result<Step, RunError> {
        if let Some(error) = self.shared.fatal_error() {
            return Err(error);
        }
        let result = self.step_unlatched(scoped_root);
        result.map_err(|error| self.finalize_error(error))
    }

    fn step_unlatched(
        &mut self,
        scoped_root: Option<&mut dyn ScopedRootDriver>,
    ) -> Result<Step, RunError> {
        if current_task_id().is_some() {
            return Err(self.error(RunErrorKind::ReentrantDrive));
        }
        if self.shared.is_stopped() {
            return Ok(Step::Stopped);
        }

        self.shared.check_wake_failure()?;

        if let Some(task) = self.shared.cancel_next_task()? {
            self.shared.check_wake_failure()?;
            return Ok(Step::TaskCancelled { task });
        }

        self.shared.admit_wakes()?;

        if let Some(task) = self.shared.take_ready_task()? {
            return match task {
                ReadyTask::Owned { id, future, signal } => self.poll_task(id, future, signal),
                ReadyTask::ScopedRoot { id, signal } => {
                    let Some(root) = scoped_root else {
                        return Err(self.error(RunErrorKind::ScopedRootUnavailable { task: id }));
                    };
                    if root.id() != id {
                        return Err(self.error(RunErrorKind::ScopedRootUnavailable { task: id }));
                    }
                    root.poll_ready(&self.shared, signal)
                }
            };
        }

        if let Some(step) = self.shared.advance_to_next_timer()? {
            return Ok(step);
        }

        let live_tasks = self.shared.state.borrow().tasks.live_len();
        if live_tasks == 0 {
            Ok(Step::Idle)
        } else {
            self.shared.emit_stalled(live_tasks);
            Ok(Step::Stalled)
        }
    }

    /// Drives until no more deterministic work can be performed.
    ///
    /// # Errors
    ///
    /// Returns [`RunError`] for any [`Self::step`] failure or when the driving
    /// call exhausts `RuntimeConfig::max_steps_per_run` while another scheduler
    /// action remains. An outcome already visible at the boundary is returned.
    pub fn run_until_stalled(&mut self) -> Result<RunOutcome, RunError> {
        if let Some(error) = self.shared.fatal_error() {
            return Err(error);
        }
        let limit = self.shared.state.borrow().config.max_steps_per_run;
        for _ in 0..limit {
            match self.step()? {
                Step::Idle => return Ok(RunOutcome::Idle(self.snapshot())),
                Step::Stalled => return Ok(RunOutcome::Stalled(self.snapshot())),
                Step::Stopped => return Ok(RunOutcome::Stopped(self.snapshot())),
                Step::TaskPolled { .. }
                | Step::TaskCancelled { .. }
                | Step::TimeAdvanced { .. } => {}
            }
        }
        if let Err(error) = self.shared.check_wake_failure() {
            return Err(self.finalize_error(error));
        }
        self.shared
            .admit_wakes()
            .map_err(|error| self.finalize_error(error))?;
        if let Some(outcome) = self.shared.terminal_outcome() {
            return Ok(outcome);
        }
        self.shared.emit(EventKindTag::BudgetExhausted, || {
            EventKind::BudgetExhausted { steps: limit }
        });
        Err(self.error(RunErrorKind::StepBudgetExceeded { limit }))
    }

    /// Runs a root future until it completes.
    ///
    /// The root and its output may borrow caller-owned stack data. The root is
    /// scheduler-visible while this call is active, but is never detached or
    /// stored beyond the call; its future is dropped under panic containment
    /// before an output or error is returned.
    ///
    /// Other tasks are left in place. Call [`Self::run_until_stalled`] when a
    /// harness requires the whole runtime to drain.
    ///
    /// # Errors
    ///
    /// Returns [`RunError`] if the root cannot be spawned, panics, stalls, the
    /// runtime encounters another scheduler failure, or the driving budget is
    /// exhausted. If cancelling the failed root also fails, the initiating
    /// error and snapshot remain primary and the cleanup failure is attached
    /// through [`RunError::cleanup_failure`].
    pub fn block_on<F>(&mut self, future: F) -> Result<F::Output, RunError>
    where
        F: Future,
    {
        if let Some(error) = self.shared.fatal_error() {
            return Err(error);
        }
        if current_task_id().is_some() {
            return Err(self.error(RunErrorKind::ReentrantDrive));
        }
        let registration = self.shared.spawn_scoped_root().map_err(|error| {
            self.finalize_error(self.error(RunErrorKind::RootSpawnFailed(error)))
        })?;
        let mut root = ScopedRoot::new(Rc::clone(&self.shared), registration, future);
        let limit = self.shared.state.borrow().config.max_steps_per_run;

        for _ in 0..limit {
            let step = match self.step_with_scoped_root(Some(&mut root)) {
                Ok(step) => step,
                Err(error) => return Err(self.cancel_failed_scoped_root(&mut root, error)),
            };
            if let Some(output) = root.take_output() {
                if let Err(error) = self.shared.check_wake_failure() {
                    drop_value_caught(output);
                    return Err(self.finalize_error(error));
                }
                return Ok(output);
            }
            if let Some(error) = root.take_join_error() {
                let error = self.root_join_error(root.id, error);
                return Err(self.cancel_failed_scoped_root(&mut root, error));
            }
            match step {
                Step::Idle | Step::Stalled => {
                    let error = self.error(RunErrorKind::Stalled);
                    return Err(self.cancel_failed_scoped_root(&mut root, error));
                }
                Step::Stopped => {
                    let error = self.error(RunErrorKind::RuntimeStopped);
                    return Err(self.cancel_failed_scoped_root(&mut root, error));
                }
                Step::TaskPolled { .. }
                | Step::TaskCancelled { .. }
                | Step::TimeAdvanced { .. } => {}
            }
        }

        if let Some(output) = root.take_output() {
            if let Err(error) = self.shared.check_wake_failure() {
                drop_value_caught(output);
                return Err(self.finalize_error(error));
            }
            return Ok(output);
        }
        if let Some(error) = root.take_join_error() {
            let error = self.root_join_error(root.id, error);
            return Err(self.cancel_failed_scoped_root(&mut root, error));
        }
        if let Err(error) = self.shared.check_wake_failure() {
            return Err(self.cancel_failed_scoped_root(&mut root, error));
        }
        if let Err(error) = self.shared.admit_wakes() {
            return Err(self.cancel_failed_scoped_root(&mut root, error));
        }
        if let Some(error) = root.take_join_error() {
            let error = self.root_join_error(root.id, error);
            return Err(self.cancel_failed_scoped_root(&mut root, error));
        }
        if self.shared.is_stopped() {
            let error = self.error(RunErrorKind::RuntimeStopped);
            return Err(self.cancel_failed_scoped_root(&mut root, error));
        }
        if self.shared.is_stalled() {
            let error = self.error(RunErrorKind::Stalled);
            return Err(self.cancel_failed_scoped_root(&mut root, error));
        }

        self.shared.emit(EventKindTag::BudgetExhausted, || {
            EventKind::BudgetExhausted { steps: limit }
        });
        let error = self.error(RunErrorKind::StepBudgetExceeded { limit });
        Err(self.cancel_failed_scoped_root(&mut root, error))
    }

    fn poll_task(
        &self,
        task_id: TaskId,
        mut future: BoxTaskFuture,
        signal: Arc<TaskSignal>,
    ) -> Result<Step, RunError> {
        self.shared.emit(EventKindTag::TaskPollStarted, || {
            EventKind::TaskPollStarted { task: task_id }
        });
        let waker = Waker::from(signal);
        let mut context = Context::from_waker(&waker);
        let _guard = CurrentGuard::enter_sim(&self.shared, task_id);
        let poll = catch_unwind(AssertUnwindSafe(|| future.as_mut().poll(&mut context)));

        match poll {
            Ok(Poll::Ready(Ok(()))) => {
                self.shared.complete_task(task_id)?;
                self.shared
                    .emit(EventKindTag::TaskCompleted, || EventKind::TaskCompleted {
                        task: task_id,
                    });
                if let Some(panic) = self.shared.drop_future_traced(task_id, future) {
                    return Err(self.error(run_error_kind(TaskFailure::DropPanicked {
                        task: task_id,
                        panic,
                    })));
                }
                Ok(Step::TaskPolled {
                    task: task_id,
                    result: PollResult::Ready,
                })
            }
            Ok(Poll::Ready(Err(panic))) => {
                self.shared.complete_task(task_id)?;
                self.shared
                    .emit(EventKindTag::TaskCompleted, || EventKind::TaskCompleted {
                        task: task_id,
                    });
                if let Some(drop_panic) = self.shared.drop_future_traced(task_id, future) {
                    self.shared
                        .emit(EventKindTag::WakerPanicked, || EventKind::WakerPanicked {
                            task: task_id,
                            panic: panic.clone(),
                        });
                    return Err(self.error(run_error_kind(TaskFailure::DropPanicked {
                        task: task_id,
                        panic: drop_panic,
                    })));
                }
                self.shared
                    .emit(EventKindTag::WakerPanicked, || EventKind::WakerPanicked {
                        task: task_id,
                        panic: panic.clone(),
                    });
                Err(self.error(run_error_kind(TaskFailure::WakerPanicked {
                    task: task_id,
                    panic,
                })))
            }
            Ok(Poll::Pending) => {
                self.shared.put_task_future(task_id, future)?;
                self.shared
                    .emit(EventKindTag::TaskPending, || EventKind::TaskPending {
                        task: task_id,
                    });
                Ok(Step::TaskPolled {
                    task: task_id,
                    result: PollResult::Pending,
                })
            }
            Err(payload) => {
                let panic = panic_record_from_payload(payload);
                let waker_panic = self.shared.panic_task(task_id, panic.clone())?;
                self.shared
                    .emit(EventKindTag::TaskPanicked, || EventKind::TaskPanicked {
                        task: task_id,
                        panic: panic.clone(),
                    });
                let _ = self.shared.drop_future_traced(task_id, future);
                if let Some(waker_panic) = waker_panic {
                    self.shared
                        .emit(EventKindTag::WakerPanicked, || EventKind::WakerPanicked {
                            task: task_id,
                            panic: waker_panic,
                        });
                }
                Err(self.error(run_error_kind(TaskFailure::Panicked {
                    task: task_id,
                    panic,
                })))
            }
        }
    }

    fn error(&self, kind: RunErrorKind) -> RunError {
        RunError::new(kind, self.snapshot())
    }

    fn finalize_error(&self, error: RunError) -> RunError {
        if error.disposition() == RunErrorDisposition::Fatal {
            self.shared.latch_fatal(error)
        } else {
            error
        }
    }

    fn root_join_error(&self, task: TaskId, error: JoinError) -> RunError {
        match error {
            JoinError::Panicked(panic) => {
                self.error(run_error_kind(TaskFailure::Panicked { task, panic }))
            }
            JoinError::Cancelled => self.error(RunErrorKind::RootCancelled),
            JoinError::RuntimeStopped => self.error(RunErrorKind::RuntimeStopped),
        }
    }

    fn cancel_failed_scoped_root<F: Future>(
        &self,
        root: &mut ScopedRoot<F>,
        mut original: RunError,
    ) -> RunError {
        let primary_already_latched = self
            .shared
            .fatal_error()
            .as_ref()
            .is_some_and(|latched| latched == &original);
        let mut cleanup = self
            .shared
            .cancel_task(root.id, TaskCancellationReason::BlockOnFailure)
            .err();
        if let Some(drop_failure) = root.drop_future()
            && cleanup.is_none()
        {
            cleanup = Some(drop_failure);
        }
        if primary_already_latched {
            return original;
        }
        if let Some(cleanup) = cleanup {
            original.cleanup_failure = Some(Box::new(cleanup));
        }
        self.finalize_error(original)
    }
}

impl Drop for SimRuntime {
    fn drop(&mut self) {
        let _ = self.shared.shutdown();
    }
}

pub(crate) struct Shared {
    wakes: Arc<WakeBridge>,
    pending_wakes: Rc<OwnerWakeQueue>,
    pending_aborts: RefCell<VecDeque<TaskId>>,
    state: RefCell<State>,
    fatal_error: RefCell<Option<RunError>>,
    trace: Option<TraceDriver>,
}

struct TraceDriver {
    sink: Rc<dyn TraceSink>,
    next_sequence: Cell<u64>,
    last_stall: Cell<Option<(u64, usize)>>,
}

impl Shared {
    #[inline]
    fn reserve_trace(&self, tag: EventKindTag) -> Option<u64> {
        let Some(trace) = &self.trace else {
            return None;
        };
        let sequence = trace.next_sequence.get();
        trace
            .next_sequence
            .set(sequence.checked_add(1).expect("trace sequence exhausted"));
        if !trace.sink.should_record(sequence, tag) {
            return None;
        }
        Some(sequence)
    }

    #[inline]
    fn record_trace(
        &self,
        sequence: u64,
        expected_tag: EventKindTag,
        make_kind: impl FnOnce() -> EventKind,
    ) {
        let Some(trace) = &self.trace else {
            return;
        };
        let at = self.state.borrow().now;
        let kind = make_kind();
        debug_assert_eq!(kind.tag(), expected_tag);
        let event = TraceEvent::new(sequence, at, kind);
        trace.sink.record(event);
    }

    #[inline]
    fn emit(&self, tag: EventKindTag, make_kind: impl FnOnce() -> EventKind) {
        let Some(sequence) = self.reserve_trace(tag) else {
            return;
        };
        self.record_trace(sequence, tag, make_kind);
    }

    fn emit_stalled(&self, live_tasks: usize) {
        let Some(trace) = &self.trace else {
            return;
        };
        // Repeated inspection of one stall is not new runtime activity. Keep
        // this marker in the trace driver so tracing remains passive and the
        // untraced scheduler and determinism checkpoint need no extra state.
        let stall = (self.state.borrow().total_steps, live_tasks);
        if trace.last_stall.replace(Some(stall)) == Some(stall) {
            return;
        }
        self.emit(EventKindTag::RuntimeStalled, || EventKind::RuntimeStalled {
            live_tasks: u64::try_from(live_tasks).unwrap_or(u64::MAX),
        });
    }

    fn emit_random_choice(
        &self,
        sequence: u64,
        stream: RandomStream,
        choice: RandomChoiceKind,
        draws_before: u64,
        draws_after: u64,
        value: u64,
    ) {
        if stream == RandomStream::Debug {
            return;
        }
        self.record_trace(sequence, EventKindTag::RandomChoice, || {
            EventKind::RandomChoice {
                stream,
                choice,
                draws_before,
                draws_after,
                value,
            }
        });
    }

    /// Emits the `TaskDropPanicked` event for a contained destructor panic.
    fn emit_task_drop_panicked(&self, task: TaskId, panic: &PanicRecord) {
        self.emit(EventKindTag::TaskDropPanicked, || {
            EventKind::TaskDropPanicked {
                task,
                panic: panic.clone(),
            }
        });
    }

    /// Drops a task's future behind a panic boundary and traces a destructor
    /// panic.
    ///
    /// The record is returned instead of an error so every caller keeps its
    /// own primary-versus-secondary failure ordering explicit.
    fn drop_future_traced<T>(&self, task: TaskId, future: T) -> Option<PanicRecord> {
        let panic = drop_value_result(future).err()?;
        self.emit_task_drop_panicked(task, &panic);
        Some(panic)
    }

    fn reserve_random_trace(&self, stream: RandomStream) -> Option<u64> {
        if stream == RandomStream::Debug {
            None
        } else {
            self.reserve_trace(EventKindTag::RandomChoice)
        }
    }

    /// Performs one already-validated deterministic draw, recording it as a
    /// `RandomChoice` event when this draw's reserved trace position is
    /// sampled.
    ///
    /// The untraced path consumes exactly the same draws as the traced path,
    /// so tracing stays behaviorally passive. Callers validate arguments
    /// before entering, so a draw failure cannot strand the reserved
    /// position.
    fn traced_draw<T: Copy + Into<u64>>(
        &self,
        stream: RandomStream,
        choice: RandomChoiceKind,
        draw: impl FnOnce(&mut DeterministicRng) -> Result<T, RandomError>,
    ) -> Result<T, RandomError> {
        self.ensure_random_active()?;
        let Some(sequence) = self.reserve_random_trace(stream) else {
            return draw(self.state.borrow_mut().random.get_mut(stream));
        };
        let (before, after, value) = {
            let mut state = self.state.borrow_mut();
            let rng = state.random.get_mut(stream);
            let before = rng.draws();
            draw(rng).map(|value| (before, rng.draws(), value))
        }?;
        self.emit_random_choice(sequence, stream, choice, before, after, value.into());
        Ok(value)
    }

    fn random_u64(&self, stream: RandomStream) -> Result<u64, RandomError> {
        self.traced_draw(stream, RandomChoiceKind::U64, |rng| Ok(rng.next_u64()))
    }

    fn random_below(&self, stream: RandomStream, upper_exclusive: u64) -> Result<u64, RandomError> {
        crate::rng::validate_upper_bound(upper_exclusive)?;
        self.traced_draw(stream, RandomChoiceKind::Below { upper_exclusive }, |rng| {
            rng.u64_below(upper_exclusive)
        })
    }

    fn random_bool_ratio(
        &self,
        stream: RandomStream,
        numerator: u64,
        denominator: u64,
    ) -> Result<bool, RandomError> {
        crate::rng::validate_ratio(numerator, denominator)?;
        self.traced_draw(
            stream,
            RandomChoiceKind::BoolRatio {
                numerator,
                denominator,
            },
            |rng| rng.bool_ratio(numerator, denominator),
        )
    }

    fn ensure_random_active(&self) -> Result<(), RandomError> {
        if self.state.borrow().stopped {
            Err(RandomError::RuntimeStopped)
        } else {
            Ok(())
        }
    }

    fn random_checkpoint(&self, stream: RandomStream) -> RngCheckpoint {
        self.state.borrow().random.get(stream).checkpoint()
    }

    fn spawn<F>(self: &Rc<Self>, future: F) -> Result<JoinHandle<F::Output>, SpawnError>
    where
        F: Future + 'static,
        F::Output: 'static,
    {
        // Preflight before wrapping the opaque user future. On rejection, its
        // destructor therefore runs without a live `State` RefCell borrow.
        let preflight = { self.state.borrow().can_insert_task() };
        if let Err(error) = preflight {
            drop(future);
            return Err(error);
        }
        let join = Rc::new(JoinState::new());
        let erased_join: Rc<dyn ErasedJoinState> = join.clone();
        let task_future = TaskHarness {
            future: Box::pin(future),
            join: Rc::clone(&join),
        };
        let requested = Rc::new(Cell::new(false));
        let id = self.state.borrow_mut().insert_task_with(|id| Task {
            future: Some(Box::pin(task_future)),
            scoped_root: false,
            join: TaskJoin::Local(erased_join),
            state: TaskState::Waiting,
            signal: Arc::new(TaskSignal {
                task: id,
                bridge: Arc::clone(&self.wakes),
                pending_order: AtomicU64::new(NO_PENDING_WAKE),
                retired: AtomicBool::new(false),
            }),
        })?;
        self.emit(EventKindTag::TaskSpawned, || {
            let parent = current_task_for_sim(self);
            EventKind::TaskSpawned { task: id, parent }
        });
        self.state
            .borrow()
            .tasks
            .task(id)
            .expect("newly inserted task exists")
            .signal
            .notify();
        Ok(JoinHandle::new(
            id,
            join,
            AbortHandle::new_sim(id, Rc::downgrade(self), requested),
        ))
    }

    fn spawn_scoped_root(self: &Rc<Self>) -> Result<ScopedRootRegistration, SpawnError> {
        self.state.borrow().can_insert_task()?;
        let join = Rc::new(JoinState::new());
        let erased_join: Rc<dyn ErasedJoinState> = join.clone();
        let id = self.state.borrow_mut().insert_task_with(|id| Task {
            future: None,
            scoped_root: true,
            join: TaskJoin::Local(erased_join),
            state: TaskState::Waiting,
            signal: Arc::new(TaskSignal {
                task: id,
                bridge: Arc::clone(&self.wakes),
                pending_order: AtomicU64::new(NO_PENDING_WAKE),
                retired: AtomicBool::new(false),
            }),
        })?;
        self.emit(EventKindTag::TaskSpawned, || EventKind::TaskSpawned {
            task: id,
            parent: None,
        });
        self.state
            .borrow()
            .tasks
            .task(id)
            .expect("newly inserted scoped root exists")
            .signal
            .notify();
        Ok(ScopedRootRegistration { id, join })
    }

    fn snapshot(&self) -> RuntimeSnapshot {
        self.state.borrow().snapshot()
    }

    pub(crate) fn is_stopped(&self) -> bool {
        self.state.borrow().stopped
    }

    fn fatal_error(&self) -> Option<RunError> {
        self.fatal_error.borrow().clone()
    }

    fn record_fatal(&self, error: RunError) -> RunError {
        debug_assert_eq!(error.disposition(), RunErrorDisposition::Fatal);
        let mut fatal_error = self.fatal_error.borrow_mut();
        if fatal_error.is_none() {
            *fatal_error = Some(error);
        }
        fatal_error
            .as_ref()
            .expect("fatal error was just recorded")
            .clone()
    }

    fn latch_fatal(self: &Rc<Self>, error: RunError) -> RunError {
        let error = self.record_fatal(error);
        // Fatal failures invalidate scheduler progress. Teardown is still
        // deterministic, but a secondary teardown failure never replaces the
        // initiating error or its failure-point snapshot.
        let _ = self.shutdown();
        error
    }

    fn terminal_outcome(&self) -> Option<RunOutcome> {
        if self.has_pending_abort() {
            return None;
        }
        let state = self.state.borrow();
        if state.stopped {
            Some(RunOutcome::Stopped(state.snapshot()))
        } else if state.tasks.live_len() == 0 {
            Some(RunOutcome::Idle(state.snapshot()))
        } else if state.tasks.ready_is_empty() && state.timers.is_empty() {
            Some(RunOutcome::Stalled(state.snapshot()))
        } else {
            None
        }
    }

    fn is_stalled(&self) -> bool {
        if self.has_pending_abort() {
            return false;
        }
        let state = self.state.borrow();
        state.tasks.live_len() != 0 && state.tasks.ready_is_empty() && state.timers.is_empty()
    }

    fn has_pending_abort(&self) -> bool {
        let state = self.state.borrow();
        self.pending_aborts
            .borrow()
            .iter()
            .any(|id| state.tasks.task(*id).is_some())
    }

    pub(crate) fn request_abort(&self, id: TaskId) {
        let should_queue = {
            let state = self.state.borrow();
            if state.tasks.task(id).is_none() {
                return;
            }
            !state.stopped
        };
        if should_queue {
            self.pending_aborts.borrow_mut().push_back(id);
        }
    }

    fn cancel_next_task(self: &Rc<Self>) -> Result<Option<TaskId>, RunError> {
        loop {
            let Some(id) = self.pending_aborts.borrow_mut().pop_front() else {
                return Ok(None);
            };
            let mut state = self.state.borrow_mut();
            if state.tasks.task(id).is_none() {
                continue;
            }
            state.total_steps = state.total_steps.checked_add(1).ok_or_else(|| {
                RunError::new(
                    run_error_kind(TaskFailure::SequenceExhausted),
                    state.snapshot(),
                )
            })?;
            drop(state);
            self.cancel_task(id, TaskCancellationReason::ExplicitAbort)?;
            return Ok(Some(id));
        }
    }

    fn check_wake_failure(&self) -> Result<(), RunError> {
        if let Some(task) = self.wakes.foreign_task() {
            return Err(RunError::new(
                RunErrorKind::NondeterministicExternalWake { task },
                self.snapshot(),
            ));
        }
        if self.wakes.order_exhausted() {
            return Err(RunError::new(
                run_error_kind(TaskFailure::SequenceExhausted),
                self.snapshot(),
            ));
        }
        Ok(())
    }

    fn admit_wakes(&self) -> Result<(), RunError> {
        let admissions = {
            let mut state = self.state.borrow_mut();
            let mut pending = self.pending_wakes.borrow_mut();
            let mut admissions = Vec::with_capacity(pending.len());
            for id in pending.drain(..) {
                if let Some(sequence) = state
                    .enqueue_task(id)
                    .map_err(|kind| RunError::new(kind, state.snapshot()))?
                {
                    admissions.push((id, sequence));
                }
            }
            admissions
        };
        for (task, sequence) in admissions {
            self.emit(EventKindTag::TaskEnqueued, || EventKind::TaskEnqueued {
                task,
                sequence,
            });
        }
        Ok(())
    }

    fn take_ready_task(&self) -> Result<Option<ReadyTask<Arc<TaskSignal>>>, RunError> {
        let result = self.state.borrow_mut().take_ready_task();
        result.map_err(|kind| RunError::new(kind, self.snapshot()))
    }

    fn put_task_future(&self, id: TaskId, future: BoxTaskFuture) -> Result<(), RunError> {
        let mut state = self.state.borrow_mut();
        let Some(task) = state.tasks.task_mut(id) else {
            return Err(RunError::new(
                RunErrorKind::RuntimeStopped,
                state.snapshot(),
            ));
        };
        task.future = Some(future);
        task.state = TaskState::Waiting;
        Ok(())
    }

    fn put_scoped_root(&self, id: TaskId) -> Result<(), RunError> {
        let mut state = self.state.borrow_mut();
        let Some(task) = state.tasks.task_mut(id) else {
            return Err(RunError::new(
                RunErrorKind::RuntimeStopped,
                state.snapshot(),
            ));
        };
        debug_assert!(task.scoped_root);
        debug_assert!(task.future.is_none());
        task.state = TaskState::Waiting;
        Ok(())
    }

    fn remove_task(&self, id: TaskId) -> Option<Task<Arc<TaskSignal>>> {
        let task = self.state.borrow_mut().tasks.remove_task(id)?;
        // Retire before invoking a join observer or future destructor. A stale
        // owner wake must neither admit work nor occupy pending queue space.
        task.signal.retired.store(true, AtomicOrdering::Release);
        if task.signal.pending_order.load(AtomicOrdering::Acquire) != NO_PENDING_WAKE {
            self.pending_wakes
                .borrow_mut()
                .retain(|pending| *pending != id);
        }
        Some(task)
    }

    fn complete_task(&self, id: TaskId) -> Result<(), RunError> {
        self.remove_task(id)
            .ok_or_else(|| RunError::new(RunErrorKind::RuntimeStopped, self.snapshot()))?;
        Ok(())
    }

    fn panic_task(&self, id: TaskId, panic: PanicRecord) -> Result<Option<PanicRecord>, RunError> {
        let task = self
            .remove_task(id)
            .ok_or_else(|| RunError::new(RunErrorKind::RuntimeStopped, self.snapshot()))?;
        // Preserve the task panic as the primary failure if notifying its
        // observer also panics. The observer panic is contained here.
        Ok(task.join.finish(Err(JoinError::Panicked(panic))).err())
    }

    fn cancel_task(
        self: &Rc<Self>,
        id: TaskId,
        reason: TaskCancellationReason,
    ) -> Result<(), RunError> {
        let task = self.remove_task(id);
        let Some(mut task) = task else {
            return Ok(());
        };
        let _guard = CurrentGuard::enter_sim(self, id);
        let join_error = match reason {
            TaskCancellationReason::ExplicitAbort | TaskCancellationReason::BlockOnFailure => {
                JoinError::Cancelled
            }
            TaskCancellationReason::RuntimeStopped => JoinError::RuntimeStopped,
        };
        let waker_panic = task.join.finish(Err(join_error)).err();
        let drop_panic = drop_value_result(task.future.take()).err();
        self.emit(EventKindTag::TaskCancelled, || EventKind::TaskCancelled {
            task: id,
            reason,
        });
        if let Some(panic) = &drop_panic {
            self.emit_task_drop_panicked(id, panic);
        }
        if let Some(panic) = &waker_panic {
            self.emit(EventKindTag::WakerPanicked, || EventKind::WakerPanicked {
                task: id,
                panic: panic.clone(),
            });
        }
        if let Some(panic) = drop_panic {
            return Err(RunError::new(
                run_error_kind(TaskFailure::DropPanicked { task: id, panic }),
                self.snapshot(),
            ));
        }
        if let Some(panic) = waker_panic {
            return Err(RunError::new(
                run_error_kind(TaskFailure::WakerPanicked { task: id, panic }),
                self.snapshot(),
            ));
        }
        Ok(())
    }

    pub(crate) fn register_timer(
        &self,
        task: TaskId,
        deadline: SimInstant,
        waker: &Waker,
    ) -> Result<Rc<TimerRegistration>, TimeError> {
        let registration = self
            .state
            .borrow_mut()
            .register_timer(task, deadline, waker)?;
        self.emit(EventKindTag::TimerScheduled, || EventKind::TimerScheduled {
            id: registration.id(),
            task,
            deadline,
        });
        Ok(registration)
    }

    pub(crate) fn cancel_timer(&self, registration: &TimerRegistration) {
        let removed = { self.state.borrow_mut().cancel_timer(registration) };
        if removed {
            self.emit(EventKindTag::TimerCancelled, || EventKind::TimerCancelled {
                id: registration.id(),
                task: registration.task(),
            });
        }
    }

    fn advance_to_next_timer(self: &Rc<Self>) -> Result<Option<Step>, RunError> {
        let (from, to, fired) = {
            let mut state = self.state.borrow_mut();
            let Some(deadline) = state.timers.next_deadline() else {
                return Ok(None);
            };
            if let Some(limit) = state.config.max_time
                && deadline > limit
            {
                return Err(RunError::new(
                    RunErrorKind::TimeLimitExceeded {
                        limit,
                        next_event: deadline,
                    },
                    state.snapshot(),
                ));
            }
            let next_total_steps = state.total_steps.checked_add(1).ok_or_else(|| {
                RunError::new(
                    run_error_kind(TaskFailure::SequenceExhausted),
                    state.snapshot(),
                )
            })?;

            let from = state.now;
            state.now = deadline;
            let mut fired = Vec::new();
            let mut wake = Vec::new();
            state.timers.fire_deadline(deadline, |id, task, waker| {
                fired.push((id, task));
                wake.push((task, waker));
            });
            state.total_steps = next_total_steps;
            (from, deadline, (fired, wake))
        };
        self.emit(EventKindTag::TimeAdvanced, || EventKind::TimeAdvanced {
            from,
            to,
        });
        for (id, task) in &fired.0 {
            self.emit(EventKindTag::TimerFired, || EventKind::TimerFired {
                id: *id,
                task: *task,
            });
        }
        let mut waker_panic = None;
        for (task, waker) in fired.1 {
            if let Err(payload) = catch_unwind(AssertUnwindSafe(|| waker.wake())) {
                let panic = panic_record_from_payload(payload);
                self.emit(EventKindTag::WakerPanicked, || EventKind::WakerPanicked {
                    task,
                    panic: panic.clone(),
                });
                if waker_panic.is_none() {
                    waker_panic = Some((task, panic));
                }
            }
        }
        if let Some((task, panic)) = waker_panic {
            let error = RunError::new(
                run_error_kind(TaskFailure::WakerPanicked { task, panic }),
                self.snapshot(),
            );
            // The public step boundary latches this initiating error before
            // teardown, so a secondary destructor failure cannot replace it.
            return Err(error);
        }
        Ok(Some(Step::TimeAdvanced {
            from,
            to,
            timers: fired.0.into_iter().map(|(id, _)| id).collect(),
        }))
    }

    fn shutdown(self: &Rc<Self>) -> Option<RunError> {
        self.wakes.stop();
        let _ = OWNER_WAKE_QUEUES.try_with(|queues| {
            queues
                .borrow_mut()
                .remove(&(Arc::as_ptr(&self.wakes) as usize));
        });
        self.pending_wakes.borrow_mut().clear();
        self.pending_aborts.borrow_mut().clear();
        let (tasks, timer_wakers) = {
            let mut state = self.state.borrow_mut();
            if state.stopped {
                return self.fatal_error();
            }
            state.stopped = true;
            let timer_wakers = state.timers.stop_all();
            (state.tasks.take_all_tasks(), timer_wakers)
        };
        self.emit(EventKindTag::RuntimeStopped, || EventKind::RuntimeStopped);

        // Resolve every join before running arbitrary user destructors.
        let mut first_error = None;
        let mut waker_panics = Vec::new();
        for (id, task) in &tasks {
            if let Err(panic) = task.join.finish(Err(JoinError::RuntimeStopped)) {
                if first_error.is_none() {
                    first_error = Some(run_error_kind(TaskFailure::WakerPanicked {
                        task: *id,
                        panic: panic.clone(),
                    }));
                }
                waker_panics.push((*id, panic));
            }
        }
        for (task, waker) in timer_wakers {
            if let Err(payload) = catch_unwind(AssertUnwindSafe(|| waker.wake())) {
                let panic = panic_record_from_payload(payload);
                if first_error.is_none() {
                    first_error = Some(run_error_kind(TaskFailure::WakerPanicked {
                        task,
                        panic: panic.clone(),
                    }));
                }
                waker_panics.push((task, panic));
            }
        }

        for (id, mut task) in tasks {
            let _guard = CurrentGuard::enter_sim(self, id);
            let drop_panic = drop_value_result(task.future.take()).err();
            self.emit(EventKindTag::TaskCancelled, || EventKind::TaskCancelled {
                task: id,
                reason: TaskCancellationReason::RuntimeStopped,
            });
            if let Some(panic) = drop_panic {
                self.emit_task_drop_panicked(id, &panic);
                if first_error.is_none() {
                    first_error = Some(run_error_kind(TaskFailure::DropPanicked {
                        task: id,
                        panic,
                    }));
                }
            }
        }
        for (task, panic) in waker_panics {
            self.emit(EventKindTag::WakerPanicked, || EventKind::WakerPanicked {
                task,
                panic,
            });
        }
        first_error.map(|kind| self.record_fatal(RunError::new(kind, self.snapshot())))
    }
}

fn task_slab_error(error: TaskSlabError) -> RunErrorKind {
    match error {
        TaskSlabError::SequenceExhausted => run_error_kind(TaskFailure::SequenceExhausted),
        TaskSlabError::RuntimeStopped => RunErrorKind::RuntimeStopped,
    }
}

struct State {
    config: RuntimeConfig,
    random: RandomStreams,
    now: SimInstant,
    total_steps: u64,
    tasks: TaskSlab<Arc<TaskSignal>>,
    timers: TimerStore,
    stopped: bool,
}

impl State {
    fn new(config: RuntimeConfig) -> Self {
        if let Some(limit) = config.max_time {
            assert!(
                config.start_time <= limit,
                "runtime start_time {} exceeds max_time {limit}",
                config.start_time
            );
        }
        let random = RandomStreams::new(config.seed);
        let timers = TimerStore::new(config.max_timers);
        Self {
            now: config.start_time,
            config,
            random,
            total_steps: 0,
            tasks: TaskSlab::new(),
            timers,
            stopped: false,
        }
    }

    fn can_insert_task(&self) -> Result<(), SpawnError> {
        self.tasks
            .can_insert_task(self.stopped, self.config.max_tasks)
    }

    fn insert_task_with(
        &mut self,
        make_task: impl FnOnce(TaskId) -> Task<Arc<TaskSignal>>,
    ) -> Result<TaskId, SpawnError> {
        self.tasks
            .insert_task_with(self.stopped, self.config.max_tasks, make_task)
    }

    fn enqueue_task(&mut self, id: TaskId) -> Result<Option<u64>, RunErrorKind> {
        self.tasks.enqueue_task(id).map_err(task_slab_error)
    }

    fn take_ready_task(&mut self) -> Result<Option<ReadyTask<Arc<TaskSignal>>>, RunErrorKind> {
        let Some(id) = self.tasks.pop_ready_task() else {
            return Ok(None);
        };
        let next_total_steps = self
            .total_steps
            .checked_add(1)
            .ok_or_else(|| run_error_kind(TaskFailure::SequenceExhausted))?;
        let ready = self.tasks.start_ready_task(id, |signal| {
            signal
                .pending_order
                .store(NO_PENDING_WAKE, AtomicOrdering::Release);
        });
        self.total_steps = next_total_steps;
        ready.map(Some).map_err(task_slab_error)
    }

    fn register_timer(
        &mut self,
        task: TaskId,
        deadline: SimInstant,
        waker: &Waker,
    ) -> Result<Rc<TimerRegistration>, TimeError> {
        if self.stopped {
            return Err(TimeError::RuntimeStopped);
        }
        self.timers.register(task, deadline, waker)
    }

    fn cancel_timer(&mut self, registration: &TimerRegistration) -> bool {
        if self.stopped {
            return false;
        }
        self.timers.cancel(registration)
    }

    fn snapshot(&self) -> RuntimeSnapshot {
        let tasks = self
            .tasks
            .iter()
            .map(|(id, task)| TaskSnapshot {
                id,
                state: task.state,
            })
            .collect();
        RuntimeSnapshot {
            reproduction: RuntimeReproduction {
                schema_version: RUNTIME_REPRODUCTION_SCHEMA_VERSION,
                rng_version: DETERMINISTIC_RNG_VERSION,
                config: self.config.clone(),
            },
            now: self.now,
            total_steps: self.total_steps,
            next_enqueue_sequence: self.tasks.next_enqueue_sequence(),
            next_timer_sequence: self.timers.next_sequence(),
            next_timer_id: self.timers.next_id(),
            ready_tasks: self.tasks.ready_len(),
            live_timers: self.timers.len(),
            random: self.random.snapshots(),
            tasks,
            stopped: self.stopped,
        }
    }
}

struct RandomStreams([DeterministicRng; 5]);

impl RandomStreams {
    /// Stable stream storage and snapshot order; `index` must match it.
    const ORDER: [RandomStream; 5] = [
        RandomStream::Schedule,
        RandomStream::Scenario,
        RandomStream::Workload,
        RandomStream::Fault,
        RandomStream::Debug,
    ];

    const fn index(stream: RandomStream) -> usize {
        match stream {
            RandomStream::Schedule => 0,
            RandomStream::Scenario => 1,
            RandomStream::Workload => 2,
            RandomStream::Fault => 3,
            RandomStream::Debug => 4,
        }
    }

    fn new(seed: u64) -> Self {
        Self(Self::ORDER.map(|stream| DeterministicRng::from_root_seed(seed, stream)))
    }

    fn get(&self, stream: RandomStream) -> &DeterministicRng {
        &self.0[Self::index(stream)]
    }

    fn get_mut(&mut self, stream: RandomStream) -> &mut DeterministicRng {
        &mut self.0[Self::index(stream)]
    }

    fn snapshots(&self) -> Vec<RandomStreamSnapshot> {
        Self::ORDER
            .into_iter()
            .map(|stream| RandomStreamSnapshot {
                stream,
                checkpoint: self.get(stream).checkpoint(),
            })
            .collect()
    }
}

struct ScopedRootRegistration {
    id: TaskId,
    join: Rc<JoinState<()>>,
}

struct ScopedRoot<F: Future> {
    shared: Rc<Shared>,
    id: TaskId,
    join: Rc<JoinState<()>>,
    future: Option<Pin<Box<F>>>,
    output: Option<F::Output>,
}

impl<F: Future> ScopedRoot<F> {
    fn new(shared: Rc<Shared>, registration: ScopedRootRegistration, future: F) -> Self {
        Self {
            shared,
            id: registration.id,
            join: registration.join,
            future: Some(Box::pin(future)),
            output: None,
        }
    }

    fn take_output(&mut self) -> Option<F::Output> {
        self.output.take()
    }

    fn take_join_error(&self) -> Option<JoinError> {
        self.join.try_take()?.err()
    }

    fn drop_future(&mut self) -> Option<RunError> {
        let future = self.future.take()?;
        let _guard = CurrentGuard::enter_sim(&self.shared, self.id);
        let panic = self.shared.drop_future_traced(self.id, future)?;
        Some(RunError::new(
            run_error_kind(TaskFailure::DropPanicked {
                task: self.id,
                panic,
            }),
            self.shared.snapshot(),
        ))
    }
}

impl<F: Future> Drop for ScopedRoot<F> {
    fn drop(&mut self) {
        contain_panic(|| {
            let _ = self
                .shared
                .cancel_task(self.id, TaskCancellationReason::BlockOnFailure);
        });
        contain_panic(|| {
            let _ = self.drop_future();
        });
    }
}

trait ScopedRootDriver {
    fn id(&self) -> TaskId;

    fn poll_ready(
        &mut self,
        shared: &Rc<Shared>,
        signal: Arc<TaskSignal>,
    ) -> Result<Step, RunError>;
}

impl<F: Future> ScopedRootDriver for ScopedRoot<F> {
    fn id(&self) -> TaskId {
        self.id
    }

    fn poll_ready(
        &mut self,
        shared: &Rc<Shared>,
        signal: Arc<TaskSignal>,
    ) -> Result<Step, RunError> {
        shared.emit(EventKindTag::TaskPollStarted, || {
            EventKind::TaskPollStarted { task: self.id }
        });
        let waker = Waker::from(signal);
        let mut context = Context::from_waker(&waker);
        let _guard = CurrentGuard::enter_sim(shared, self.id);
        let future = self
            .future
            .as_mut()
            .expect("a ready scoped root retains its future");
        let poll = catch_unwind(AssertUnwindSafe(|| future.as_mut().poll(&mut context)));

        match poll {
            Ok(Poll::Ready(output)) => {
                shared.complete_task(self.id)?;
                shared.emit(EventKindTag::TaskCompleted, || EventKind::TaskCompleted {
                    task: self.id,
                });
                let future = self
                    .future
                    .take()
                    .expect("completed scoped root retains its future");
                if let Some(panic) = shared.drop_future_traced(self.id, future) {
                    drop_value_caught(output);
                    return Err(RunError::new(
                        run_error_kind(TaskFailure::DropPanicked {
                            task: self.id,
                            panic,
                        }),
                        shared.snapshot(),
                    ));
                }
                self.output = Some(output);
                Ok(Step::TaskPolled {
                    task: self.id,
                    result: PollResult::Ready,
                })
            }
            Ok(Poll::Pending) => {
                shared.put_scoped_root(self.id)?;
                shared.emit(EventKindTag::TaskPending, || EventKind::TaskPending {
                    task: self.id,
                });
                Ok(Step::TaskPolled {
                    task: self.id,
                    result: PollResult::Pending,
                })
            }
            Err(payload) => {
                let panic = panic_record_from_payload(payload);
                let waker_panic = shared.panic_task(self.id, panic.clone())?;
                shared.emit(EventKindTag::TaskPanicked, || EventKind::TaskPanicked {
                    task: self.id,
                    panic: panic.clone(),
                });
                // Capture the initiating panic before running the root's
                // arbitrary destructor. A second destructor panic is cleanup
                // context; it must not replace either the primary kind or its
                // failure-point snapshot.
                let mut error = RunError::new(
                    run_error_kind(TaskFailure::Panicked {
                        task: self.id,
                        panic,
                    }),
                    shared.snapshot(),
                );
                if let Some(cleanup) = self.drop_future() {
                    error.cleanup_failure = Some(Box::new(cleanup));
                }
                if let Some(waker_panic) = waker_panic {
                    shared.emit(EventKindTag::WakerPanicked, || EventKind::WakerPanicked {
                        task: self.id,
                        panic: waker_panic,
                    });
                }
                Err(error)
            }
        }
    }
}

struct TaskSignal {
    task: TaskId,
    bridge: Arc<WakeBridge>,
    pending_order: AtomicU64,
    retired: AtomicBool,
}

impl TaskSignal {
    fn notify(&self) {
        if self.bridge.stopped.load(AtomicOrdering::Acquire) {
            return;
        }
        if thread::current().id() != self.bridge.owner {
            self.bridge.record_foreign(self.task);
            return;
        }
        if self.retired.load(AtomicOrdering::Acquire)
            || self.pending_order.load(AtomicOrdering::Acquire) != NO_PENDING_WAKE
        {
            return;
        }
        // Another TLS destructor can hold a live runtime and wake its task
        // after the owner registry has been destroyed. Such a wake cannot be
        // admitted; leave both notification and ordering state untouched.
        let _ = OWNER_WAKE_QUEUES.try_with(|queues| {
            let queue = queues
                .borrow()
                .get(&(Arc::as_ptr(&self.bridge) as usize))
                .and_then(std::rc::Weak::upgrade)
                .expect("active owner wake bridge has a registered queue");
            if let Some(order) = self.bridge.allocate_order() {
                self.pending_order.store(order, AtomicOrdering::Release);
                queue.borrow_mut().push_back(self.task);
            }
        });
    }
}

impl Wake for TaskSignal {
    fn wake(self: Arc<Self>) {
        self.notify();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.notify();
    }
}

struct WakeBridge {
    owner: ThreadId,
    stopped: AtomicBool,
    next_order: AtomicU64,
    order_exhausted: AtomicBool,
    foreign_seen: AtomicBool,
    foreign_task: AtomicU64,
}

impl WakeBridge {
    fn new(owner: ThreadId) -> Self {
        Self {
            owner,
            stopped: AtomicBool::new(false),
            next_order: AtomicU64::new(0),
            order_exhausted: AtomicBool::new(false),
            foreign_seen: AtomicBool::new(false),
            foreign_task: AtomicU64::new(0),
        }
    }

    fn allocate_order(&self) -> Option<u64> {
        match self.next_order.fetch_update(
            AtomicOrdering::Relaxed,
            AtomicOrdering::Relaxed,
            |current| (current != NO_PENDING_WAKE).then_some(current.wrapping_add(1)),
        ) {
            Ok(order) => Some(order),
            Err(_) => {
                self.order_exhausted.store(true, AtomicOrdering::Release);
                None
            }
        }
    }

    fn record_foreign(&self, task: TaskId) {
        self.foreign_task.store(
            (u64::from(task.slot()) << 32) | u64::from(task.generation()),
            AtomicOrdering::Relaxed,
        );
        self.foreign_seen.store(true, AtomicOrdering::Release);
    }

    fn foreign_task(&self) -> Option<TaskId> {
        if !self.foreign_seen.load(AtomicOrdering::Acquire) {
            return None;
        }
        let packed = self.foreign_task.load(AtomicOrdering::Acquire);
        Some(TaskId::from_parts((packed >> 32) as u32, packed as u32))
    }

    fn order_exhausted(&self) -> bool {
        self.order_exhausted.load(AtomicOrdering::Acquire)
    }

    fn stop(&self) {
        self.stopped.store(true, AtomicOrdering::Release);
    }
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroU64;

    use crate::trace::{NoopTrace, RecordingTrace, SamplingTrace};

    use super::*;

    fn pending_task_for_test(runtime: &SimRuntime) -> (JoinHandle<()>, Arc<TaskSignal>) {
        let task = runtime.handle().spawn(std::future::pending()).unwrap();
        let signal = runtime
            .shared
            .state
            .borrow()
            .tasks
            .task(task.id())
            .unwrap()
            .signal
            .clone();
        (task, signal)
    }

    #[test]
    fn owner_wake_after_queue_tls_teardown_does_not_abort_or_reserve_notification() {
        // A panic escaping a TLS destructor aborts the process, so exercise the
        // real thread-exit path in a subprocess rather than the test runner.
        let output = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--ignored",
                "--exact",
                "sim::tests::owner_wake_during_tls_teardown_helper",
                "--nocapture",
            ])
            .env("KR_RUNTIME_SIM_TLS_TEARDOWN_HELPER", "1")
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "TLS teardown helper failed: {:?}\nstdout:\n{}\nstderr:\n{}",
            output.status,
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr),
        );
    }

    #[test]
    #[ignore = "subprocess helper for owner wakes during thread-local teardown"]
    fn owner_wake_during_tls_teardown_helper() {
        if std::env::var_os("KR_RUNTIME_SIM_TLS_TEARDOWN_HELPER").is_none() {
            return;
        }

        struct WakeDuringDrop(Arc<TaskSignal>);

        impl Drop for WakeDuringDrop {
            fn drop(&mut self) {
                assert!(!self.0.bridge.stopped.load(AtomicOrdering::Acquire));
                assert!(OWNER_WAKE_QUEUES.try_with(|_| ()).is_err());
                let next_order = self.0.bridge.next_order.load(AtomicOrdering::Relaxed);
                Waker::from(self.0.clone()).wake();
                assert_eq!(
                    self.0.pending_order.load(AtomicOrdering::Relaxed),
                    NO_PENDING_WAKE
                );
                assert_eq!(
                    self.0.bridge.next_order.load(AtomicOrdering::Relaxed),
                    next_order
                );
            }
        }

        std::thread::spawn(|| {
            thread_local! {
                static RUNTIME: RefCell<Option<SimRuntime>> = const { RefCell::new(None) };
                static WAKE: RefCell<Option<WakeDuringDrop>> = const { RefCell::new(None) };
            }

            // TLS destructors run in reverse initialization order. Keep task
            // identity and runtime storage alive after the wake-on-drop object;
            // constructing the runtime last makes its queue registry die first.
            assert_eq!(current_task_id(), None);
            RUNTIME.with(|runtime| assert!(runtime.borrow().is_none()));
            WAKE.with(|wake| assert!(wake.borrow().is_none()));
            let mut runtime = SimRuntime::default();
            let (_, signal) = pending_task_for_test(&runtime);
            assert!(matches!(runtime.step().unwrap(), Step::TaskPolled { .. }));
            WAKE.with(|wake| *wake.borrow_mut() = Some(WakeDuringDrop(signal)));
            RUNTIME.with(|stored| *stored.borrow_mut() = Some(runtime));
        })
        .join()
        .unwrap();
    }

    #[test]
    fn owner_wake_queues_are_isolated_and_unregister_on_shutdown() {
        let mut first = SimRuntime::default();
        let mut second = SimRuntime::default();
        let (first_task, first_signal) = pending_task_for_test(&first);
        let (second_task, second_signal) = pending_task_for_test(&second);
        assert_eq!(first_task.id(), second_task.id());
        first.step().unwrap();
        second.step().unwrap();

        second_signal.notify();
        assert_eq!(first.step().unwrap(), Step::Stalled);
        assert_eq!(
            second.step().unwrap(),
            Step::TaskPolled {
                task: second_task.id(),
                result: PollResult::Pending,
            }
        );

        let key = Arc::as_ptr(&first.shared.wakes) as usize;
        first.shutdown().unwrap();
        OWNER_WAKE_QUEUES.with(|queues| assert!(!queues.borrow().contains_key(&key)));
        first_signal.notify();
        assert!(first.shared.pending_wakes.borrow().is_empty());
        assert_eq!(second.step().unwrap(), Step::Stalled);
    }

    #[test]
    fn completing_self_wakes_are_retired_before_destructor_wakes() {
        struct WakeOnDrop(Option<Waker>);

        impl Future for WakeOnDrop {
            type Output = ();

            fn poll(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<()> {
                self.0 = Some(context.waker().clone());
                context.waker().wake_by_ref();
                Poll::Ready(())
            }
        }

        impl Drop for WakeOnDrop {
            fn drop(&mut self) {
                if let Some(waker) = self.0.take() {
                    waker.wake();
                }
            }
        }

        let mut runtime = SimRuntime::default();
        let task = runtime.handle().spawn(WakeOnDrop(None)).unwrap();
        assert_eq!(
            runtime.step().unwrap(),
            Step::TaskPolled {
                task: task.id(),
                result: PollResult::Ready,
            }
        );
        assert!(runtime.shared.pending_wakes.borrow().is_empty());
        assert_eq!(runtime.step().unwrap(), Step::Idle);
    }

    #[test]
    fn cancellation_removes_pending_wakes_before_slot_reuse() {
        let mut runtime = SimRuntime::new(RuntimeConfig {
            max_tasks: 1,
            ..RuntimeConfig::default()
        });
        let mut stale_signals = Vec::new();
        for generation in 0..128 {
            let (task, signal) = pending_task_for_test(&runtime);
            assert_eq!(task.id(), TaskId::from_parts(0, generation));
            assert_eq!(runtime.shared.pending_wakes.borrow().len(), 1);
            task.abort();
            assert_eq!(
                runtime.step().unwrap(),
                Step::TaskCancelled { task: task.id() }
            );
            stale_signals.push(signal);
            for stale in &stale_signals {
                stale.notify();
            }
            assert!(runtime.shared.pending_wakes.borrow().is_empty());
        }
        assert_eq!(runtime.step().unwrap(), Step::Idle);
    }

    #[test]
    fn queued_wakes_match_fifo_model_across_cancellation_and_slot_reuse() {
        crate::seed_sweep!(16, |seed| {
            let mut runtime = SimRuntime::new(RuntimeConfig {
                seed,
                start_time: RuntimeConfig::derived_start_time(seed),
                max_tasks: 16,
                ..RuntimeConfig::default()
            });
            let mut random = DeterministicRng::new(seed);
            let mut tasks: Vec<_> = (0..16).map(|_| pending_task_for_test(&runtime)).collect();
            let mut ready: VecDeque<_> = tasks.iter().map(|(task, _)| task.id()).collect();
            let mut stale = Vec::new();
            let mut operations = Vec::new();
            let mut wake_count = 0;
            let mut cancel_count = 0;
            let mut poll_count = 0;
            for step in 0..256 {
                let index = random.u64_below(tasks.len() as u64).unwrap() as usize;
                let action = random.u64_below(4).unwrap();
                let id = tasks[index].0.id();
                operations.push((action, id));
                match action {
                    0 | 1 => {
                        wake_count += 1;
                        tasks[index].1.notify();
                        tasks[index].1.notify();
                        if !ready.contains(&id) {
                            ready.push_back(id);
                        }
                    }
                    2 => {
                        poll_count += 1;
                        let expected =
                            ready
                                .pop_front()
                                .map_or(Step::Stalled, |task| Step::TaskPolled {
                                    task,
                                    result: PollResult::Pending,
                                });
                        assert_eq!(
                            runtime.step().unwrap(),
                            expected,
                            "seed={seed} step={step} operations={operations:?}"
                        );
                    }
                    3 => {
                        cancel_count += 1;
                        tasks[index].0.abort();
                        assert_eq!(
                            runtime.step().unwrap(),
                            Step::TaskCancelled { task: id },
                            "seed={seed} step={step} operations={operations:?}"
                        );
                        ready.retain(|task| *task != id);
                        let replacement = pending_task_for_test(&runtime);
                        ready.push_back(replacement.0.id());
                        stale.push(std::mem::replace(&mut tasks[index], replacement).1);
                    }
                    _ => unreachable!(),
                }
                for signal in &stale {
                    signal.notify();
                }
                assert!(
                    runtime.shared.pending_wakes.borrow().len() <= tasks.len(),
                    "seed={seed} step={step} operations={operations:?}"
                );
            }
            assert!(wake_count > 0 && cancel_count > 0 && poll_count > 0);
            for task in ready {
                assert_eq!(
                    runtime.step().unwrap(),
                    Step::TaskPolled {
                        task,
                        result: PollResult::Pending,
                    },
                    "seed={seed} operations={operations:?}"
                );
            }
            assert_eq!(runtime.step().unwrap(), Step::Stalled);
        });
    }

    #[test]
    fn exhausted_wake_order_admits_nothing_and_latches_failure() {
        let mut runtime = SimRuntime::default();
        let (_, signal) = pending_task_for_test(&runtime);
        runtime.step().unwrap();
        runtime
            .shared
            .wakes
            .next_order
            .store(NO_PENDING_WAKE, AtomicOrdering::Relaxed);
        signal.notify();
        assert!(runtime.shared.pending_wakes.borrow().is_empty());
        assert_eq!(
            signal.pending_order.load(AtomicOrdering::Relaxed),
            NO_PENDING_WAKE
        );
        let error = runtime.step().unwrap_err();
        assert_eq!(error.kind, RunErrorKind::SequenceExhausted);
        assert_eq!(runtime.step().unwrap_err(), error);
    }

    #[test]
    fn repeated_stall_inspection_emits_again_only_after_progress() {
        let recording = Rc::new(RecordingTrace::new(64));
        let mut traced = SimRuntime::with_trace(RuntimeConfig::default(), recording.clone());
        let mut baseline = SimRuntime::default();
        let (_, traced_signal) = pending_task_for_test(&traced);
        let (_, baseline_signal) = pending_task_for_test(&baseline);
        assert_eq!(traced.step().unwrap(), baseline.step().unwrap());
        assert_eq!(traced.step().unwrap(), Step::Stalled);
        assert_eq!(baseline.step().unwrap(), Step::Stalled);
        let events = recording.events();
        let fingerprint = recording.fingerprint();
        let checkpoint = traced.snapshot().determinism_checkpoint();

        for _ in 0..8 {
            assert_eq!(traced.step().unwrap(), Step::Stalled);
            assert_eq!(recording.events(), events);
            assert_eq!(recording.fingerprint(), fingerprint);
            assert_eq!(traced.snapshot().determinism_checkpoint(), checkpoint);
        }

        traced_signal.notify();
        baseline_signal.notify();
        assert_eq!(traced.step().unwrap(), baseline.step().unwrap());
        assert_eq!(traced.step().unwrap(), Step::Stalled);
        assert_eq!(baseline.step().unwrap(), Step::Stalled);
        assert_eq!(
            traced.snapshot().determinism_checkpoint(),
            baseline.snapshot().determinism_checkpoint()
        );
        assert_eq!(
            recording
                .events()
                .iter()
                .filter(|event| matches!(event.kind, EventKind::RuntimeStalled { .. }))
                .count(),
            2
        );
        assert_eq!(
            recording.events().len(),
            events.len() + 4,
            "wake admission, poll start, pending, and a new stall"
        );
    }

    #[test]
    fn disabled_trace_does_not_evaluate_event_builder() {
        let runtime = SimRuntime::default();
        let built = Cell::new(0);

        runtime.shared.emit(EventKindTag::RuntimeStopped, || {
            built.set(built.get() + 1);
            EventKind::RuntimeStopped
        });
        assert_eq!(built.get(), 0);

        let runtime = SimRuntime::with_trace(RuntimeConfig::default(), Rc::new(NoopTrace));
        runtime.shared.emit(EventKindTag::RuntimeStopped, || {
            built.set(built.get() + 1);
            EventKind::RuntimeStopped
        });
        assert_eq!(built.get(), 0);

        let trace = Rc::new(RecordingTrace::new(4));
        let runtime = SimRuntime::with_trace(RuntimeConfig::default(), trace.clone());
        runtime.shared.emit(EventKindTag::RuntimeStopped, || {
            built.set(built.get() + 1);
            EventKind::RuntimeStopped
        });
        assert_eq!(built.get(), 1);
        assert_eq!(trace.len(), 2, "runtime start plus the explicit event");
    }

    #[test]
    fn enabled_trace_releases_state_borrow_before_building_event() {
        let trace = Rc::new(RecordingTrace::new(4));
        let runtime = SimRuntime::with_trace(RuntimeConfig::default(), trace.clone());
        let shared = &runtime.shared;

        shared.emit(EventKindTag::RuntimeStopped, || {
            shared.state.borrow_mut().now = SimInstant::from_nanos(7);
            EventKind::RuntimeStopped
        });

        let events = trace.events();
        assert_eq!(events.len(), 2, "runtime start plus the explicit event");
        assert_eq!(events[1].at, SimInstant::ZERO);
        assert_eq!(runtime.snapshot().now, SimInstant::from_nanos(7));
    }

    #[test]
    fn rejected_trace_sample_does_not_evaluate_event_builder() {
        let recording = Rc::new(RecordingTrace::new(4));
        let sampling = Rc::new(SamplingTrace::new(
            recording.clone(),
            NonZeroU64::new(2).expect("period is nonzero"),
        ));
        let runtime = SimRuntime::with_trace(RuntimeConfig::default(), sampling);
        let built = Cell::new(0);

        runtime.shared.emit(EventKindTag::RuntimeStopped, || {
            built.set(built.get() + 1);
            EventKind::RuntimeStopped
        });
        assert_eq!(built.get(), 0, "sequence one is not constructed");

        runtime.shared.emit(EventKindTag::RuntimeStopped, || {
            built.set(built.get() + 1);
            EventKind::RuntimeStopped
        });
        assert_eq!(built.get(), 1, "sequence two is constructed");
        assert_eq!(
            recording
                .events()
                .iter()
                .map(|event| event.sequence)
                .collect::<Vec<_>>(),
            vec![0, 2]
        );
    }

    #[test]
    fn rejected_random_sample_uses_direct_rng_path_without_changing_behavior() {
        let config = RuntimeConfig {
            seed: 17,
            ..RuntimeConfig::default()
        };
        let baseline = SimRuntime::new(config.clone());
        let expected = baseline.handle().random_u64().expect("runtime is active");

        let recording = Rc::new(RecordingTrace::new(4));
        let sampling = Rc::new(SamplingTrace::new(
            recording.clone(),
            NonZeroU64::new(2).expect("period is nonzero"),
        ));
        let sampled = SimRuntime::with_trace(config, sampling);
        let actual = sampled.handle().random_u64().expect("runtime is active");

        assert_eq!(actual, expected);
        assert_eq!(sampled.snapshot(), baseline.snapshot());
        let next_sequence = sampled
            .shared
            .trace
            .as_ref()
            .expect("sampling keeps tracing enabled")
            .next_sequence
            .get();
        assert_eq!(
            next_sequence, 2,
            "the rejected random event reserves one position"
        );
        assert_eq!(
            sampled.handle().random_below(0),
            Err(RandomError::ZeroUpperBound)
        );
        assert_eq!(
            sampled
                .shared
                .trace
                .as_ref()
                .expect("sampling keeps tracing enabled")
                .next_sequence
                .get(),
            next_sequence,
            "an invalid request does not reserve a trace position"
        );

        sampled
            .shared
            .emit(EventKindTag::RuntimeStopped, || EventKind::RuntimeStopped);
        assert_eq!(
            recording
                .events()
                .iter()
                .map(|event| event.sequence)
                .collect::<Vec<_>>(),
            vec![0, 2]
        );
    }

    #[test]
    fn random_choice_emitter_excludes_the_debug_stream() {
        let trace = Rc::new(RecordingTrace::new(4));
        let runtime = SimRuntime::with_trace(RuntimeConfig::default(), trace.clone());
        let fingerprint = trace.fingerprint();

        runtime
            .shared
            .emit_random_choice(1, RandomStream::Debug, RandomChoiceKind::U64, 0, 1, 42);

        assert_eq!(trace.len(), 1, "only the runtime-start event is retained");
        assert_eq!(trace.fingerprint(), fingerprint);
    }

    #[test]
    fn determinism_checkpoint_excludes_debug_randomness() {
        let runtime = SimRuntime::new(RuntimeConfig {
            seed: 17,
            ..RuntimeConfig::default()
        });
        let debug = runtime.random_source(RandomStream::Debug);
        let baseline = runtime.snapshot().determinism_checkpoint();

        let _ = debug.random_u64().expect("runtime is active");
        let instrumented = runtime.snapshot().determinism_checkpoint();

        assert_eq!(baseline, instrumented);
        assert_eq!(
            instrumented.schema_version,
            DETERMINISM_CHECKPOINT_SCHEMA_VERSION
        );
        assert_eq!(
            instrumented.reproduction.schema_version,
            RUNTIME_REPRODUCTION_SCHEMA_VERSION
        );
        assert_eq!(instrumented.reproduction.config.seed, 17);
        assert_eq!(instrumented.random.len(), 4);
    }

    #[test]
    fn determinism_checkpoint_includes_behavioral_volume_counters() {
        let mut baseline = SimRuntime::default();
        baseline.block_on(async {}).expect("empty root completes");
        let baseline = baseline.snapshot().determinism_checkpoint();

        let mut registered_timer = SimRuntime::default();
        let clock = registered_timer.handle();
        registered_timer
            .block_on(async move {
                let mut sleep = Box::pin(clock.sleep(SimDuration::from_nanos(1)));
                std::future::poll_fn(|context| {
                    assert!(sleep.as_mut().poll(context).is_pending());
                    Poll::Ready(())
                })
                .await;
            })
            .expect("root drops its registered timer");
        let registered_timer = registered_timer.snapshot().determinism_checkpoint();

        assert_eq!(registered_timer.now, baseline.now);
        assert_eq!(registered_timer.total_steps, baseline.total_steps);
        assert_eq!(registered_timer.ready_tasks, baseline.ready_tasks);
        assert_eq!(registered_timer.live_timers, baseline.live_timers);
        assert_eq!(registered_timer.live_tasks, baseline.live_tasks);
        assert_eq!(registered_timer.random, baseline.random);
        assert_eq!(registered_timer.next_enqueue_sequence, 1);
        assert_eq!(registered_timer.next_timer_sequence, 1);
        assert_eq!(registered_timer.next_timer_id, 1);
        assert_ne!(registered_timer, baseline);

        let untouched = SimRuntime::default().snapshot().determinism_checkpoint();
        let mut aborted = SimRuntime::default();
        let task = aborted
            .handle()
            .spawn(std::future::pending::<()>())
            .expect("pending task spawns");
        task.abort();
        assert!(matches!(
            aborted.run_until_stalled().expect("abort is processed"),
            RunOutcome::Idle(_)
        ));
        let aborted = aborted.snapshot().determinism_checkpoint();

        assert_eq!(aborted.now, untouched.now);
        assert_eq!(aborted.total_steps, untouched.total_steps + 1);
        assert_eq!(aborted.ready_tasks, untouched.ready_tasks);
        assert_eq!(aborted.live_timers, untouched.live_timers);
        assert_eq!(aborted.live_tasks, untouched.live_tasks);
        assert_eq!(aborted.random, untouched.random);
        assert_eq!(aborted.next_enqueue_sequence, 0);
        assert_ne!(aborted, untouched);
    }

    #[test]
    fn generation_exhausted_task_slot_is_retired_after_its_final_use() {
        let mut runtime = SimRuntime::default();
        let handle = runtime.handle();
        let first = handle
            .spawn(std::future::pending::<()>())
            .expect("first task spawns");
        assert_eq!(first.id(), TaskId::from_parts(0, 0));
        first.abort();
        assert!(matches!(
            runtime.run_until_stalled().expect("abort completes"),
            RunOutcome::Idle(_)
        ));

        {
            let mut state = runtime.shared.state.borrow_mut();
            assert!(state.tasks.slot_is_vacant_for_test(0));
            state
                .tasks
                .set_vacant_slot_generation_for_test(0, u32::MAX - 1);
            assert_eq!(state.tasks.free_slots_for_test(), &[0]);
        }

        let final_generation = handle.spawn(async {}).expect("final generation spawns");
        assert_eq!(final_generation.id(), TaskId::from_parts(0, u32::MAX));
        assert!(matches!(
            runtime
                .run_until_stalled()
                .expect("final generation completes"),
            RunOutcome::Idle(_)
        ));

        let replacement = handle
            .spawn(std::future::pending::<()>())
            .expect("a new slot replaces the retired slot");
        assert_eq!(replacement.id(), TaskId::from_parts(1, 0));
        {
            let state = runtime.shared.state.borrow();
            assert_eq!(state.tasks.slot_generation_for_test(0), u32::MAX);
            assert!(state.tasks.slot_is_vacant_for_test(0));
            assert!(!state.tasks.free_slots_for_test().contains(&0));
        }
        replacement.abort();
        assert!(matches!(
            runtime.run_until_stalled().expect("replacement aborts"),
            RunOutcome::Idle(_)
        ));
    }
}
