//! Single-owner production scheduling driven by host monotonic time.

use crate::handle::RuntimeHandle;
use crate::panic::panic_record_from_payload;
use crate::rng::{DeterministicRng, RandomError, RandomStream, RngCheckpoint};
use crate::task::{
    AbortHandle, BoxTaskFuture, CurrentGuard, ErasedJoinState, JoinCore, JoinError, JoinHandle,
    JoinState, PanicRecord, ReadyTask, RunErrorDisposition, SpawnError, Task, TaskFailure,
    TaskHarness, TaskId, TaskJoin, TaskSlab, TaskSlabError, TaskState, current_host_shared,
    current_task_id, drop_value_caught, drop_value_result,
};
use crate::time::{RuntimeDuration, RuntimeInstant, TimeError};
use crate::timer::{Sleep, TimerRegistration, TimerStore};
use std::cell::{Cell, RefCell};
use std::collections::VecDeque;
use std::fmt;
use std::future::Future;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::pin::Pin;
use std::rc::Rc;
use std::sync::atomic::{AtomicBool, AtomicU8, Ordering};
use std::sync::{Arc, Condvar, Mutex, MutexGuard};
use std::task::{Context, Poll, Wake, Waker};
use std::thread::{self, Thread};
use std::time::{Duration, Instant};

type SendTaskFuture = Pin<Box<dyn Future<Output = Result<(), PanicRecord>> + Send + 'static>>;

struct HostSendJoinState<T> {
    inner: Mutex<JoinCore<T>>,
}

impl<T> HostSendJoinState<T> {
    fn new() -> Self {
        Self {
            inner: Mutex::new(JoinCore::new()),
        }
    }

    fn finish_typed(&self, result: Result<T, JoinError>) -> Result<(), PanicRecord> {
        let (waiter, discarded) = lock_unpoisoned(&self.inner).finish(result);
        drop_value_caught(discarded);
        if let Some(waiter) = waiter {
            catch_unwind(AssertUnwindSafe(|| waiter.wake())).map_err(panic_record_from_payload)?;
        }
        Ok(())
    }

    fn poll(&self, context: &mut Context<'_>) -> Poll<Result<T, JoinError>> {
        // The candidate is cloned before taking the lock and stays owned by
        // this frame, so no foreign waker code runs under the lock; the core
        // takes it out of the slot only when it is actually stored.
        let mut candidate = Some(context.waker().clone());
        let (poll, stale) = lock_unpoisoned(&self.inner).poll(
            context.waker(),
            || {
                candidate
                    .take()
                    .expect("candidate waker is consumed at most once per poll")
            },
            "HostSendJoinHandle polled after completion",
        );
        drop_value_caught(stale);
        poll
    }

    fn is_finished(&self) -> bool {
        lock_unpoisoned(&self.inner).is_finished()
    }
}

impl<T: Send + 'static> ErasedJoinState for HostSendJoinState<T> {
    fn finish(&self, result: Result<(), JoinError>) -> Result<(), PanicRecord> {
        if let Err(error) = result {
            self.finish_typed(Err(error))
        } else {
            Ok(())
        }
    }
}

struct HostSendTaskHarness<F: Future> {
    future: Pin<Box<F>>,
    join: Arc<HostSendJoinState<F::Output>>,
}

impl<F: Future> Future for HostSendTaskHarness<F> {
    type Output = Result<(), PanicRecord>;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        match this.future.as_mut().poll(context) {
            Poll::Ready(output) => Poll::Ready(this.join.finish_typed(Ok(output))),
            Poll::Pending => Poll::Pending,
        }
    }
}

const STATE_RUNNING: u8 = 0;
const STATE_STOP_REQUESTED: u8 = 1;
const STATE_STOPPED: u8 = 2;
const STATE_FAILED: u8 = 3;

/// Resource limits and the behavioral seed for a [`HostRuntime`].
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HostConfig {
    /// Root seed for the owner-local workload random stream.
    pub seed: u64,
    /// Maximum admitted spawned tasks. The borrowed `block_on` root is excluded.
    pub max_tasks: usize,
    /// Maximum owner-local timer registrations.
    pub max_timers: usize,
    /// Maximum queued cross-thread wakes, aborts, and send-spawns.
    pub max_ingress: usize,
    /// Maximum ingress items drained during one scheduler turn.
    pub max_ingress_per_turn: usize,
    /// Workers provisioned for the blocking capability on first
    /// [`HostRuntime::blocking`] request. Provisioning is lazy, so a
    /// runtime that never punts blocking work spawns none of them.
    pub blocking_workers: usize,
}

impl Default for HostConfig {
    fn default() -> Self {
        Self {
            seed: 0,
            max_tasks: 100_000,
            max_timers: 100_000,
            max_ingress: 200_000,
            max_ingress_per_turn: 200_000,
            blocking_workers: 2,
        }
    }
}

/// Invalid host-runtime construction input.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum HostConfigError {
    /// Cross-thread ingress needs at least one slot.
    ZeroIngressCapacity,
    /// Every scheduler turn must be able to drain at least one ingress item.
    ZeroIngressPerTurn,
    /// The blocking capability needs at least one worker to provision.
    ZeroBlockingWorkers,
}

impl fmt::Display for HostConfigError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ZeroIngressCapacity => {
                formatter.write_str("host runtime ingress capacity must be non-zero")
            }
            Self::ZeroIngressPerTurn => {
                formatter.write_str("host runtime ingress per turn must be non-zero")
            }
            Self::ZeroBlockingWorkers => {
                formatter.write_str("host runtime blocking workers must be non-zero")
            }
        }
    }
}

impl std::error::Error for HostConfigError {}

/// The externally visible host-runtime lifecycle.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum HostStatus {
    /// Tasks and ingress are accepted.
    Running,
    /// A controller requested graceful stop and teardown is pending.
    StopRequested,
    /// Checked teardown completed without a retained failure.
    Stopped,
    /// A fatal runtime boundary failed.
    Failed,
}

/// A host-runtime failure category.
#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum HostRunErrorKind {
    /// A controller interrupted the borrowed root.
    StopRequested,
    /// The runtime is already terminal.
    RuntimeStopped,
    /// A runtime was driven recursively from a task poll or teardown.
    ReentrantDrive,
    /// A ready scoped root did not match the root currently being driven.
    ScopedRootUnavailable { task: TaskId },
    /// The borrowed root could not reserve a task identifier.
    RootSpawnFailed(SpawnError),
    /// The borrowed root panicked while being polled.
    RootPanicked { panic: PanicRecord },
    /// A rejected root panicked on drop before it acquired a task identifier.
    RejectedRootDropPanicked { panic: PanicRecord },
    /// A task-level fatal boundary failed.
    Task(TaskFailure),
    /// Infallible wake or abort ingress exceeded its configured bound.
    ResourceExhausted {
        /// The exhausted resource; always cross-thread ingress for this error.
        resource: &'static str,
        /// The configured bound that was exceeded.
        limit: usize,
    },
}

impl HostRunErrorKind {
    /// Classifies whether later runtime use is meaningful.
    #[must_use]
    pub const fn disposition(&self) -> RunErrorDisposition {
        match self {
            Self::StopRequested | Self::RuntimeStopped => RunErrorDisposition::Terminal,
            Self::ReentrantDrive | Self::RootPanicked { .. } => RunErrorDisposition::Resumable,
            Self::RootSpawnFailed(error) => match error {
                SpawnError::ResourceExhausted { .. } => RunErrorDisposition::Resumable,
                SpawnError::RuntimeStopped => RunErrorDisposition::Terminal,
                SpawnError::IdentifierExhausted => RunErrorDisposition::Fatal,
            },
            Self::ScopedRootUnavailable { .. }
            | Self::RejectedRootDropPanicked { .. }
            | Self::Task(_)
            | Self::ResourceExhausted { .. } => RunErrorDisposition::Fatal,
        }
    }
}

impl From<TaskFailure> for HostRunErrorKind {
    fn from(failure: TaskFailure) -> Self {
        Self::Task(failure)
    }
}

impl fmt::Display for HostRunErrorKind {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::StopRequested => formatter.write_str("host runtime stop was requested"),
            Self::RuntimeStopped => formatter.write_str("host runtime is stopped"),
            Self::ReentrantDrive => {
                formatter.write_str("host runtime was driven from inside a task poll or teardown")
            }
            Self::ScopedRootUnavailable { task } => {
                write!(
                    formatter,
                    "ready scoped root {task} does not match the root being driven"
                )
            }
            Self::RootSpawnFailed(error) => {
                write!(formatter, "borrowed root could not be spawned: {error}")
            }
            Self::RootPanicked { panic } => {
                write!(formatter, "borrowed root panicked: {}", panic.message)
            }
            Self::RejectedRootDropPanicked { panic } => {
                write!(
                    formatter,
                    "rejected root destructor panicked: {}",
                    panic.message
                )
            }
            Self::Task(failure) => fmt::Display::fmt(failure, formatter),
            Self::ResourceExhausted { resource, limit } => {
                write!(formatter, "{resource} limit of {limit} is exhausted")
            }
        }
    }
}

/// A host-runtime failure without simulation snapshot state.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HostRunError {
    /// Machine-readable failure category.
    pub kind: HostRunErrorKind,
    /// The first secondary root or runtime teardown failure, without
    /// replacing the initiating failure.
    pub cleanup_failure: Option<Box<HostRunError>>,
}

impl HostRunError {
    fn new(kind: HostRunErrorKind) -> Self {
        Self {
            kind,
            cleanup_failure: None,
        }
    }

    fn attach_cleanup(&mut self, cleanup: Self) {
        match &mut self.cleanup_failure {
            None => self.cleanup_failure = Some(Box::new(cleanup)),
            Some(first) if first.disposition() != RunErrorDisposition::Fatal => {
                // A secondary root panic can be resumable. Keep the first
                // fatal teardown failure too, so disposition remains honest.
                if cleanup.disposition() == RunErrorDisposition::Fatal {
                    first.attach_cleanup(cleanup);
                }
            }
            Some(_) => {}
        }
    }

    /// Classifies the whole error, including any secondary cleanup failure.
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

impl fmt::Display for HostRunError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self.kind, formatter)?;
        if let Some(cleanup) = &self.cleanup_failure {
            write!(formatter, "; cleanup also failed: {cleanup}")?;
        }
        Ok(())
    }
}

impl std::error::Error for HostRunError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.cleanup_failure
            .as_deref()
            .map(|cleanup| cleanup as &(dyn std::error::Error + 'static))
    }
}

#[derive(Clone, Copy)]
struct IdSlot {
    generation: u32,
    allocated: bool,
}

#[derive(Default)]
struct IdPool {
    slots: Vec<IdSlot>,
    free: Vec<u32>,
    counted: usize,
}

impl IdPool {
    fn allocate(&mut self, counted: bool, max_tasks: usize) -> Result<TaskId, SpawnError> {
        if counted && self.counted >= max_tasks {
            return Err(SpawnError::ResourceExhausted {
                resource: "live tasks",
                limit: max_tasks,
            });
        }
        let id = loop {
            let Some(slot_index) = self.free.pop() else {
                let slot =
                    u32::try_from(self.slots.len()).map_err(|_| SpawnError::IdentifierExhausted)?;
                self.slots.push(IdSlot {
                    generation: 0,
                    allocated: true,
                });
                break TaskId::from_parts(slot, 0);
            };
            let slot = &mut self.slots[slot_index as usize];
            debug_assert!(!slot.allocated);
            let Some(generation) = slot.generation.checked_add(1) else {
                continue;
            };
            slot.generation = generation;
            slot.allocated = true;
            break TaskId::from_parts(slot_index, generation);
        };
        if counted {
            self.counted += 1;
        }
        Ok(id)
    }

    fn release(&mut self, id: TaskId, counted: bool) {
        let Some(slot) = self.slots.get_mut(id.slot() as usize) else {
            return;
        };
        if !slot.allocated || slot.generation != id.generation() {
            return;
        }
        slot.allocated = false;
        if slot.generation < u32::MAX {
            self.free.push(id.slot());
        }
        if counted {
            self.counted -= 1;
        }
    }
}

enum IngressItem {
    Wake(TaskId),
    Abort(TaskId),
    Spawn(SendSpawnCommand),
}

struct SendSpawnCommand {
    id: Option<TaskId>,
    future: SendTaskFuture,
    join: Arc<dyn ErasedJoinState + Send + Sync>,
}

struct HostCrossThreadCoreInner {
    queue: VecDeque<IngressItem>,
    ids: IdPool,
}

/// Thread-safe host state shared by signals, send handles, and controls.
pub(crate) struct HostCrossThreadCore {
    owner: Thread,
    epoch: Instant,
    max_tasks: usize,
    max_ingress: usize,
    blocking_workers: usize,
    /// The lazily provisioned blocking capability; one fleet per runtime,
    /// cached so repeated requests share it.
    blocking: Mutex<Option<HostBlocking>>,
    lifecycle: AtomicU8,
    overflow: AtomicBool,
    inner: Mutex<HostCrossThreadCoreInner>,
}

impl HostCrossThreadCore {
    fn new(config: &HostConfig) -> Self {
        Self {
            owner: thread::current(),
            epoch: Instant::now(),
            max_tasks: config.max_tasks,
            max_ingress: config.max_ingress,
            blocking_workers: config.blocking_workers,
            blocking: Mutex::new(None),
            lifecycle: AtomicU8::new(STATE_RUNNING),
            overflow: AtomicBool::new(false),
            inner: Mutex::new(HostCrossThreadCoreInner {
                queue: VecDeque::new(),
                ids: IdPool::default(),
            }),
        }
    }

    /// Provisions the blocking fleet once and shares it thereafter.
    fn blocking_capability(&self) -> Result<HostBlocking, HostBlockingError> {
        let mut cached = lock_unpoisoned(&self.blocking);
        if let Some(capability) = &*cached {
            return Ok(capability.clone());
        }
        let capability = HostBlocking::provision(self.blocking_workers)?;
        *cached = Some(capability.clone());
        Ok(capability)
    }

    fn state(&self) -> HostStatus {
        match self.lifecycle.load(Ordering::Acquire) {
            STATE_RUNNING => HostStatus::Running,
            STATE_STOP_REQUESTED => HostStatus::StopRequested,
            STATE_STOPPED => HostStatus::Stopped,
            STATE_FAILED => HostStatus::Failed,
            _ => unreachable!("host runtime lifecycle is valid"),
        }
    }

    fn is_running(&self) -> bool {
        self.lifecycle.load(Ordering::Acquire) == STATE_RUNNING
    }

    fn now_raw(&self) -> RuntimeInstant {
        let nanos = u64::try_from(self.epoch.elapsed().as_nanos())
            .expect("host runtime exceeded 584 years");
        RuntimeInstant::from_nanos(nanos)
    }

    fn request_stop(&self) {
        let _ = self.lifecycle.compare_exchange(
            STATE_RUNNING,
            STATE_STOP_REQUESTED,
            Ordering::Release,
            Ordering::Acquire,
        );
        self.owner.unpark();
    }

    fn mark_failed(&self) {
        self.lifecycle.store(STATE_FAILED, Ordering::Release);
        self.owner.unpark();
    }

    fn reserve_id(&self, counted: bool) -> Result<TaskId, SpawnError> {
        if !self.is_running() {
            return Err(SpawnError::RuntimeStopped);
        }
        let mut inner = lock_unpoisoned(&self.inner);
        if !self.is_running() {
            return Err(SpawnError::RuntimeStopped);
        }
        let id = inner.ids.allocate(counted, self.max_tasks)?;
        Ok(id)
    }

    fn release_id(&self, id: TaskId, counted: bool) {
        lock_unpoisoned(&self.inner).ids.release(id, counted);
    }

    fn enqueue_spawn(
        &self,
        mut command: SendSpawnCommand,
    ) -> Result<TaskId, (SpawnError, SendSpawnCommand)> {
        if !self.is_running() {
            return Err((SpawnError::RuntimeStopped, command));
        }
        let mut inner = lock_unpoisoned(&self.inner);
        if !self.is_running() {
            return Err((SpawnError::RuntimeStopped, command));
        }
        if inner.queue.len() >= self.max_ingress {
            return Err((
                SpawnError::ResourceExhausted {
                    resource: "cross-thread ingress",
                    limit: self.max_ingress,
                },
                command,
            ));
        }
        let id = match inner.ids.allocate(true, self.max_tasks) {
            Ok(id) => id,
            Err(error) => return Err((error, command)),
        };
        command.id = Some(id);
        inner.queue.push_back(IngressItem::Spawn(command));
        drop(inner);
        self.owner.unpark();
        Ok(id)
    }

    fn push_item(&self, item: IngressItem) -> bool {
        if !self.is_running() {
            return false;
        }
        let accepted = {
            let mut inner = lock_unpoisoned(&self.inner);
            if !self.is_running() {
                return false;
            }
            if inner.queue.len() < self.max_ingress {
                inner.queue.push_back(item);
            } else {
                // Publish while holding the same lock used by `close`, so a
                // concurrent teardown cannot miss an already-observed
                // infallible ingress failure.
                self.overflow.store(true, Ordering::Release);
            }
            true
        };
        self.owner.unpark();
        accepted
    }

    fn push_wake(&self, task: TaskId) -> bool {
        self.push_item(IngressItem::Wake(task))
    }

    pub(crate) fn push_abort(&self, task: TaskId) {
        let _ = self.push_item(IngressItem::Abort(task));
    }

    fn drain(&self, limit: usize) -> VecDeque<IngressItem> {
        let mut inner = lock_unpoisoned(&self.inner);
        if inner.queue.is_empty() {
            return VecDeque::new();
        }
        let count = limit.min(inner.queue.len());
        inner.queue.drain(..count).collect()
    }

    fn has_pending(&self) -> bool {
        !lock_unpoisoned(&self.inner).queue.is_empty()
    }

    fn close(&self) -> Vec<IngressItem> {
        let mut inner = lock_unpoisoned(&self.inner);
        if self.lifecycle.load(Ordering::Acquire) != STATE_FAILED {
            self.lifecycle.store(STATE_STOPPED, Ordering::Release);
        }
        let items = inner.queue.drain(..).collect();
        drop(inner);
        self.owner.unpark();
        items
    }
}

fn lock_unpoisoned<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

struct HostSignal {
    task: TaskId,
    core: Arc<HostCrossThreadCore>,
    pending: AtomicBool,
}

impl HostSignal {
    fn notify(&self) {
        if self.pending.swap(true, Ordering::AcqRel) {
            return;
        }
        if !self.core.push_wake(self.task) {
            // A rejected wake did not enter ingress, so do not leave the
            // coalescing latch claiming that one is pending.
            self.pending.store(false, Ordering::Release);
        }
    }
}

impl Wake for HostSignal {
    fn wake(self: Arc<Self>) {
        self.notify();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.notify();
    }
}

struct State {
    tasks: TaskSlab<Arc<HostSignal>>,
    timers: TimerStore,
    stopped: bool,
}

impl State {
    fn new(max_timers: usize) -> Self {
        Self {
            tasks: TaskSlab::new(),
            timers: TimerStore::new(max_timers),
            stopped: false,
        }
    }

    fn insert(
        &mut self,
        id: TaskId,
        task: Task<Arc<HostSignal>>,
    ) -> Result<(), (TaskSlabError, Task<Arc<HostSignal>>)> {
        self.tasks.insert_reserved_task(id, task, self.stopped)
    }

    fn enqueue(&mut self, id: TaskId) -> Result<(), TaskSlabError> {
        self.tasks.enqueue_task(id).map(|_| ())
    }

    fn take_ready(&mut self) -> Result<Option<ReadyTask<Arc<HostSignal>>>, TaskSlabError> {
        let Some(id) = self.tasks.pop_ready_task() else {
            return Ok(None);
        };
        self.tasks
            .start_ready_task(id, |signal| {
                // A foreign wake can coalesce into an already-pending wake
                // without taking the ingress lock. Acquire its published
                // writes before polling, including the release sequence of
                // any intervening coalesced wakes.
                signal.pending.swap(false, Ordering::AcqRel);
            })
            .map(Some)
    }
}

pub(crate) struct Shared {
    pub(crate) core: Arc<HostCrossThreadCore>,
    config: HostConfig,
    state: RefCell<State>,
    pending_aborts: RefCell<VecDeque<TaskId>>,
    failed_ingress_remainder: RefCell<VecDeque<IngressItem>>,
    random: RefCell<DeterministicRng>,
    fatal_error: RefCell<Option<HostRunError>>,
}

struct AdmissionFailure {
    spawn_error: SpawnError,
    fatal_error: Option<HostRunError>,
}

impl Shared {
    fn error(&self, kind: HostRunErrorKind) -> HostRunError {
        HostRunError::new(kind)
    }

    fn sync_ingress_failure(&self) -> Option<HostRunError> {
        if let Some(error) = self.fatal_error.borrow().clone() {
            return Some(error);
        }
        if !self.core.overflow.load(Ordering::Acquire) {
            return None;
        }
        let kind = HostRunErrorKind::ResourceExhausted {
            resource: "cross-thread ingress",
            limit: self.config.max_ingress,
        };
        let error = self.error(kind);
        self.core.mark_failed();
        *self.fatal_error.borrow_mut() = Some(error.clone());
        Some(error)
    }

    fn retain_fatal(&self, error: HostRunError) -> HostRunError {
        debug_assert_eq!(error.disposition(), RunErrorDisposition::Fatal);
        let mut retained = self.fatal_error.borrow_mut();
        if retained.is_none() {
            self.core.mark_failed();
            *retained = Some(error);
        }
        retained.as_ref().expect("fatal error was retained").clone()
    }

    fn admit_reserved_task(
        self: &Rc<Self>,
        id: TaskId,
        task: Task<Arc<HostSignal>>,
        rejected_join: JoinError,
    ) -> Result<(), AdmissionFailure> {
        let rejected = {
            let mut state = self.state.borrow_mut();
            match state.insert(id, task) {
                Ok(()) => match state.enqueue(id) {
                    Ok(()) => None,
                    Err(error) => Some((
                        error,
                        state
                            .tasks
                            .remove_reserved_task(id)
                            .expect("new task remains present after enqueue failure"),
                    )),
                },
                Err(rejected) => Some(rejected),
            }
        };
        let Some((cause, mut task)) = rejected else {
            return Ok(());
        };

        self.core.release_id(id, !task.scoped_root);
        let _guard = CurrentGuard::enter_host(self, id);
        let waker_panic = task.join.finish(Err(rejected_join)).err();
        let drop_panic = drop_value_result(task.future.take()).err();
        // The admission failure happened before rollback. Preserve it as the
        // primary failure; cleanup panics become primary only after a clean
        // stopped-state rejection.
        let fatal_error = match cause {
            TaskSlabError::SequenceExhausted => Some(TaskFailure::SequenceExhausted),
            TaskSlabError::RuntimeStopped => None,
        }
        .or_else(|| drop_panic.map(|panic| TaskFailure::DropPanicked { task: id, panic }))
        .or_else(|| waker_panic.map(|panic| TaskFailure::WakerPanicked { task: id, panic }))
        .map(|failure| self.error(failure.into()));
        let spawn_error = match cause {
            TaskSlabError::RuntimeStopped => SpawnError::RuntimeStopped,
            TaskSlabError::SequenceExhausted => SpawnError::IdentifierExhausted,
        };
        Err(AdmissionFailure {
            spawn_error,
            fatal_error,
        })
    }

    fn spawn_local<F>(self: &Rc<Self>, future: F) -> Result<JoinHandle<F::Output>, SpawnError>
    where
        F: Future + 'static,
        F::Output: 'static,
    {
        let id = match self.core.reserve_id(true) {
            Ok(id) => id,
            Err(error) => {
                drop_value_caught(future);
                return Err(error);
            }
        };
        let join = Rc::new(JoinState::new());
        let erased: Rc<dyn ErasedJoinState> = join.clone();
        let requested = Rc::new(Cell::new(false));
        let task_future = TaskHarness {
            future: Box::pin(future),
            join: Rc::clone(&join),
        };
        let task = Task {
            future: Some(Box::pin(task_future)),
            scoped_root: false,
            join: TaskJoin::Local(erased),
            state: TaskState::Waiting,
            signal: Arc::new(HostSignal {
                task: id,
                core: Arc::clone(&self.core),
                pending: AtomicBool::new(false),
            }),
        };
        if let Err(failure) = self.admit_reserved_task(id, task, JoinError::RuntimeStopped) {
            if let Some(error) = failure.fatal_error {
                self.retain_fatal(error);
            }
            return Err(failure.spawn_error);
        }
        Ok(JoinHandle::new(
            id,
            join,
            AbortHandle::new_host(id, Rc::downgrade(self), requested),
        ))
    }

    fn accept_send_spawn(self: &Rc<Self>, command: SendSpawnCommand) -> Result<(), HostRunError> {
        let SendSpawnCommand { id, future, join } = command;
        let id = id.expect("queued send-spawn has an identifier");
        let task = Task {
            future: Some(future),
            scoped_root: false,
            join: TaskJoin::Send(join),
            state: TaskState::Waiting,
            signal: Arc::new(HostSignal {
                task: id,
                core: Arc::clone(&self.core),
                pending: AtomicBool::new(false),
            }),
        };
        if let Err(failure) = self.admit_reserved_task(id, task, JoinError::RuntimeStopped) {
            return failure.fatal_error.map_or(Ok(()), Err);
        }
        Ok(())
    }

    fn drain_ingress(self: &Rc<Self>) -> Result<(), HostRunError> {
        if let Some(error) = self.sync_ingress_failure() {
            return Err(error);
        }
        let mut items = self.core.drain(self.config.max_ingress_per_turn);
        while let Some(item) = items.pop_front() {
            let result = match item {
                IngressItem::Wake(id) => self.admit_wake(id),
                IngressItem::Abort(id) => {
                    self.pending_aborts.borrow_mut().push_back(id);
                    Ok(())
                }
                IngressItem::Spawn(command) => self.accept_send_spawn(command),
            };
            if let Err(error) = result {
                // These items were already admitted before any concurrent
                // arrivals that refilled the bounded ingress queue. Preserve
                // them separately, in front of that queue's contents, for
                // checked teardown without exceeding `max_ingress`.
                self.failed_ingress_remainder
                    .borrow_mut()
                    .append(&mut items);
                return Err(error);
            }
        }
        if let Some(error) = self.sync_ingress_failure() {
            return Err(error);
        }
        Ok(())
    }

    fn admit_wake(&self, id: TaskId) -> Result<(), HostRunError> {
        let mut state = self.state.borrow_mut();
        if state.tasks.task(id).is_none() {
            return Ok(());
        }
        state
            .enqueue(id)
            .map_err(|error| self.error(host_slab_error(error)))
    }

    pub(crate) fn request_abort(&self, id: TaskId) {
        let should_queue = {
            let state = self.state.borrow();
            !state.stopped && state.tasks.task(id).is_some()
        };
        if should_queue {
            self.pending_aborts.borrow_mut().push_back(id);
        }
    }

    fn cancel_next_task(self: &Rc<Self>) -> Result<bool, HostRunError> {
        loop {
            let Some(id) = self.pending_aborts.borrow_mut().pop_front() else {
                return Ok(false);
            };
            if self.state.borrow().tasks.task(id).is_none() {
                continue;
            }
            self.cancel_task(id)?;
            return Ok(true);
        }
    }

    fn has_pending_abort(&self) -> bool {
        let state = self.state.borrow();
        self.pending_aborts
            .borrow()
            .iter()
            .any(|id| state.tasks.task(*id).is_some())
    }

    fn take_ready(&self) -> Result<Option<ReadyTask<Arc<HostSignal>>>, HostRunError> {
        self.state
            .borrow_mut()
            .take_ready()
            .map_err(|error| self.error(host_slab_error(error)))
    }

    fn put_task(&self, id: TaskId, future: BoxTaskFuture) -> Result<(), HostRunError> {
        let mut state = self.state.borrow_mut();
        let Some(task) = state.tasks.task_mut(id) else {
            return Err(self.error(HostRunErrorKind::RuntimeStopped));
        };
        task.future = Some(future);
        task.state = TaskState::Waiting;
        Ok(())
    }

    fn put_root(&self, id: TaskId) -> Result<(), HostRunError> {
        let mut state = self.state.borrow_mut();
        let Some(task) = state.tasks.task_mut(id) else {
            return Err(self.error(HostRunErrorKind::RuntimeStopped));
        };
        task.state = TaskState::Waiting;
        Ok(())
    }

    fn remove_task(&self, id: TaskId) -> Option<Task<Arc<HostSignal>>> {
        let task = self.state.borrow_mut().tasks.remove_reserved_task(id)?;
        self.core.release_id(id, !task.scoped_root);
        Some(task)
    }

    fn cancel_task(self: &Rc<Self>, id: TaskId) -> Result<(), HostRunError> {
        let Some(mut task) = self.remove_task(id) else {
            return Ok(());
        };
        let _guard = CurrentGuard::enter_host(self, id);
        let waker_panic = task.join.finish(Err(JoinError::Cancelled)).err();
        let drop_panic = drop_value_result(task.future.take()).err();
        if let Some(panic) = drop_panic {
            return Err(self.error(TaskFailure::DropPanicked { task: id, panic }.into()));
        }
        if let Some(panic) = waker_panic {
            return Err(self.error(TaskFailure::WakerPanicked { task: id, panic }.into()));
        }
        Ok(())
    }

    fn poll_task(
        self: &Rc<Self>,
        id: TaskId,
        mut future: BoxTaskFuture,
        signal: Arc<HostSignal>,
    ) -> Result<(), HostRunError> {
        let waker = Waker::from(signal);
        let mut context = Context::from_waker(&waker);
        let _guard = CurrentGuard::enter_host(self, id);
        let poll = catch_unwind(AssertUnwindSafe(|| future.as_mut().poll(&mut context)));
        match poll {
            Ok(Poll::Pending) => self.put_task(id, future),
            Ok(Poll::Ready(Ok(()))) => {
                self.remove_task(id)
                    .ok_or_else(|| self.error(HostRunErrorKind::RuntimeStopped))?;
                drop_value_result(future).map_err(|panic| {
                    self.error(TaskFailure::DropPanicked { task: id, panic }.into())
                })
            }
            Ok(Poll::Ready(Err(panic))) => {
                self.remove_task(id)
                    .ok_or_else(|| self.error(HostRunErrorKind::RuntimeStopped))?;
                let drop_panic = drop_value_result(future).err();
                if let Some(drop_panic) = drop_panic {
                    return Err(self.error(
                        TaskFailure::DropPanicked {
                            task: id,
                            panic: drop_panic,
                        }
                        .into(),
                    ));
                }
                Err(self.error(TaskFailure::WakerPanicked { task: id, panic }.into()))
            }
            Err(payload) => {
                let panic = panic_record_from_payload(payload);
                let task = self
                    .remove_task(id)
                    .ok_or_else(|| self.error(HostRunErrorKind::RuntimeStopped))?;
                let waker_panic = task
                    .join
                    .finish(Err(JoinError::Panicked(panic.clone())))
                    .err();
                let drop_panic = drop_value_result(future).err();
                if let Some(drop_panic) = drop_panic {
                    return Err(self.error(
                        TaskFailure::DropPanicked {
                            task: id,
                            panic: drop_panic,
                        }
                        .into(),
                    ));
                }
                if let Some(waker_panic) = waker_panic {
                    return Err(self.error(
                        TaskFailure::WakerPanicked {
                            task: id,
                            panic: waker_panic,
                        }
                        .into(),
                    ));
                }
                Ok(())
            }
        }
    }

    fn fire_due_timers(self: &Rc<Self>) -> Result<bool, HostRunError> {
        if self.next_deadline().is_none() {
            return Ok(false);
        }
        let now = self.core.now_raw();
        let mut any_fired = false;
        loop {
            let fired = {
                let mut state = self.state.borrow_mut();
                let Some(deadline) = state.timers.next_deadline() else {
                    return Ok(any_fired);
                };
                if deadline > now {
                    return Ok(any_fired);
                }
                any_fired = true;
                let mut fired = Vec::new();
                state.timers.fire_deadline(deadline, |_, task, waker| {
                    fired.push((task, waker));
                });
                fired
            };
            for (task, waker) in fired {
                if let Err(payload) = catch_unwind(AssertUnwindSafe(|| waker.wake())) {
                    return Err(self.error(
                        TaskFailure::WakerPanicked {
                            task,
                            panic: panic_record_from_payload(payload),
                        }
                        .into(),
                    ));
                }
            }
        }
    }

    pub(crate) fn is_stopped(&self) -> bool {
        !self.core.is_running() || self.state.borrow().stopped
    }

    pub(crate) fn timer_now(&self) -> RuntimeInstant {
        self.core.now_raw()
    }

    pub(crate) fn register_timer(
        &self,
        task: TaskId,
        deadline: RuntimeInstant,
        waker: &Waker,
    ) -> Result<Rc<TimerRegistration>, TimeError> {
        if !self.core.is_running() {
            return Err(TimeError::RuntimeStopped);
        }
        let registration = self
            .state
            .borrow_mut()
            .timers
            .register(task, deadline, waker)?;
        Ok(registration)
    }

    pub(crate) fn cancel_timer(&self, registration: &TimerRegistration) {
        if self.state.borrow().stopped {
            return;
        }
        let _ = self.state.borrow_mut().timers.cancel(registration);
    }

    fn next_deadline(&self) -> Option<RuntimeInstant> {
        self.state.borrow_mut().timers.next_deadline()
    }

    fn random_u64(&self) -> Result<u64, RandomError> {
        if !self.core.is_running() {
            return Err(RandomError::RuntimeStopped);
        }
        Ok(self.random.borrow_mut().next_u64())
    }

    fn random_below(&self, upper_exclusive: u64) -> Result<u64, RandomError> {
        crate::rng::validate_upper_bound(upper_exclusive)?;
        if !self.core.is_running() {
            return Err(RandomError::RuntimeStopped);
        }
        self.random.borrow_mut().u64_below(upper_exclusive)
    }

    fn random_bool_ratio(&self, numerator: u64, denominator: u64) -> Result<bool, RandomError> {
        crate::rng::validate_ratio(numerator, denominator)?;
        if !self.core.is_running() {
            return Err(RandomError::RuntimeStopped);
        }
        self.random.borrow_mut().bool_ratio(numerator, denominator)
    }

    fn random_position(&self) -> RngCheckpoint {
        self.random.borrow().checkpoint()
    }

    fn install_root(self: &Rc<Self>) -> Result<TaskId, SpawnError> {
        let id = self.core.reserve_id(false)?;
        let join = Rc::new(JoinState::<()>::new());
        let erased: Rc<dyn ErasedJoinState> = join;
        let task = Task {
            future: None,
            scoped_root: true,
            join: TaskJoin::Local(erased),
            state: TaskState::Waiting,
            signal: Arc::new(HostSignal {
                task: id,
                core: Arc::clone(&self.core),
                pending: AtomicBool::new(false),
            }),
        };
        if let Err(failure) = self.admit_reserved_task(id, task, JoinError::RuntimeStopped) {
            if let Some(error) = failure.fatal_error {
                self.retain_fatal(error);
            }
            return Err(failure.spawn_error);
        }
        Ok(id)
    }

    fn shutdown(self: &Rc<Self>) -> Option<HostRunError> {
        if self.state.borrow().stopped {
            return self.sync_ingress_failure();
        }
        let ingress_queued = self.core.close();
        let mut queued = std::mem::take(&mut *self.failed_ingress_remainder.borrow_mut());
        queued.extend(ingress_queued);
        self.pending_aborts.borrow_mut().clear();
        let (tasks, timer_wakers) = {
            let mut state = self.state.borrow_mut();
            state.stopped = true;
            let timer_wakers = state.timers.stop_all();
            let tasks = state.tasks.take_all_tasks();
            (tasks, timer_wakers)
        };

        let mut queued_spawns = Vec::new();
        for item in queued {
            if let IngressItem::Spawn(command) = item {
                queued_spawns.push(command);
            }
        }

        // Collect teardown failures separately from any retained initiating
        // error so cleanup never hides that error or loses its own context.
        let mut first_failure = None;

        // Resolve every observer before running any future destructor.
        for (id, task) in &tasks {
            if let Err(panic) = task.join.finish(Err(JoinError::RuntimeStopped))
                && first_failure.is_none()
            {
                first_failure = Some(TaskFailure::WakerPanicked { task: *id, panic }.into());
            }
        }
        for command in &queued_spawns {
            let id = command.id.expect("queued send-spawn has an identifier");
            if let Err(panic) = command.join.finish(Err(JoinError::RuntimeStopped))
                && first_failure.is_none()
            {
                first_failure = Some(TaskFailure::WakerPanicked { task: id, panic }.into());
            }
        }
        for (task, waker) in timer_wakers {
            if let Err(payload) = catch_unwind(AssertUnwindSafe(|| waker.wake())) {
                let panic = panic_record_from_payload(payload);
                if first_failure.is_none() {
                    first_failure = Some(TaskFailure::WakerPanicked { task, panic }.into());
                }
            }
        }

        for (id, mut task) in tasks {
            self.core.release_id(id, !task.scoped_root);
            let _guard = CurrentGuard::enter_host(self, id);
            if let Err(panic) = drop_value_result(task.future.take())
                && first_failure.is_none()
            {
                first_failure = Some(TaskFailure::DropPanicked { task: id, panic }.into());
            }
        }
        for command in queued_spawns {
            let id = command.id.expect("queued send-spawn has an identifier");
            self.core.release_id(id, true);
            let _guard = CurrentGuard::enter_host(self, id);
            if let Err(panic) = drop_value_result(command)
                && first_failure.is_none()
            {
                first_failure = Some(TaskFailure::DropPanicked { task: id, panic }.into());
            }
        }

        let mut error = self.sync_ingress_failure();
        if let Some(kind) = first_failure {
            let cleanup = self.error(kind);
            if let Some(primary) = &mut error {
                primary.attach_cleanup(cleanup);
            } else {
                error = Some(cleanup);
            }
        }
        if error.is_some() {
            self.core.mark_failed();
            *self.fatal_error.borrow_mut() = error.clone();
        }
        error
    }
}

fn host_slab_error(error: TaskSlabError) -> HostRunErrorKind {
    match error {
        TaskSlabError::SequenceExhausted => TaskFailure::SequenceExhausted.into(),
        TaskSlabError::RuntimeStopped => HostRunErrorKind::RuntimeStopped,
    }
}

struct RootDriver<F: Future> {
    shared: Rc<Shared>,
    id: TaskId,
    future: Option<Pin<Box<F>>>,
    output: Option<F::Output>,
}

impl<F: Future> RootDriver<F> {
    fn new(shared: Rc<Shared>, id: TaskId, future: F) -> Self {
        Self {
            shared,
            id,
            future: Some(Box::pin(future)),
            output: None,
        }
    }

    fn poll_ready(&mut self, signal: Arc<HostSignal>) -> Result<(), HostRunError> {
        let waker = Waker::from(signal);
        let mut context = Context::from_waker(&waker);
        let _guard = CurrentGuard::enter_host(&self.shared, self.id);
        let future = self
            .future
            .as_mut()
            .expect("active root retains its future while pending");
        let poll = catch_unwind(AssertUnwindSafe(|| future.as_mut().poll(&mut context)));
        match poll {
            Ok(Poll::Pending) => self.shared.put_root(self.id),
            Ok(Poll::Ready(output)) => {
                self.shared
                    .remove_task(self.id)
                    .ok_or_else(|| self.shared.error(HostRunErrorKind::RuntimeStopped))?;
                let future = self.future.take().expect("completed root retains future");
                if let Err(panic) = drop_value_result(future) {
                    let _ = drop_value_result(output);
                    return Err(self.shared.error(
                        TaskFailure::DropPanicked {
                            task: self.id,
                            panic,
                        }
                        .into(),
                    ));
                }
                self.output = Some(output);
                Ok(())
            }
            Err(payload) => {
                let panic = panic_record_from_payload(payload);
                self.shared
                    .remove_task(self.id)
                    .ok_or_else(|| self.shared.error(HostRunErrorKind::RuntimeStopped))?;
                let future = self.future.take().expect("panicked root retains future");
                let mut error = self.shared.error(HostRunErrorKind::RootPanicked { panic });
                if let Err(drop_panic) = drop_value_result(future) {
                    error.attach_cleanup(
                        self.shared.error(
                            TaskFailure::DropPanicked {
                                task: self.id,
                                panic: drop_panic,
                            }
                            .into(),
                        ),
                    );
                }
                Err(error)
            }
        }
    }

    fn take_output(&mut self) -> Option<F::Output> {
        self.output.take()
    }

    fn cancel(&mut self) -> Option<HostRunError> {
        self.future.as_ref()?;
        self.shared.remove_task(self.id);
        let future = self.future.take()?;
        let _guard = CurrentGuard::enter_host(&self.shared, self.id);
        drop_value_result(future).err().map(|panic| {
            self.shared.error(
                TaskFailure::DropPanicked {
                    task: self.id,
                    panic,
                }
                .into(),
            )
        })
    }
}

impl<F: Future> Drop for RootDriver<F> {
    fn drop(&mut self) {
        if let Some(error) = self.cancel()
            && error.disposition() == RunErrorDisposition::Fatal
        {
            self.shared.retain_fatal(error);
        }
    }
}

/// Cloneable owner-thread capability for local host-runtime tasks.
///
/// This handle intentionally cannot cross threads:
///
/// ```compile_fail
/// fn require_send<T: Send>(_: T) {}
/// let runtime = kr_runtime::HostRuntime::default();
/// require_send(runtime.handle());
/// ```
#[derive(Clone)]
pub struct HostHandle {
    pub(crate) shared: Rc<Shared>,
}

impl HostHandle {
    /// Returns the host handle for the task currently being polled, if any.
    #[must_use]
    pub fn current() -> Option<Self> {
        current_host_shared().map(|shared| Self { shared })
    }

    /// Provisions (once) and returns the runtime's blocking capability.
    ///
    /// See [`HostRuntime::blocking`].
    ///
    /// # Errors
    ///
    /// Returns [`HostBlockingError`] when a worker thread cannot be
    /// spawned; a failed provisioning attempt may be retried.
    pub fn blocking(&self) -> Result<HostBlocking, HostBlockingError> {
        self.shared.core.blocking_capability()
    }

    /// Returns host monotonic time relative to runtime construction.
    #[must_use]
    pub fn now(&self) -> RuntimeInstant {
        self.shared.timer_now()
    }

    /// Spawns an owner-local task. The future and output may be `!Send`.
    ///
    /// # Errors
    ///
    /// Returns [`SpawnError`] if the runtime is stopped, its live-task limit
    /// is reached, or the generational task identifier space is exhausted.
    pub fn spawn<F>(&self, future: F) -> Result<JoinHandle<F::Output>, SpawnError>
    where
        F: Future + 'static,
        F::Output: 'static,
    {
        self.shared.spawn_local(future)
    }

    /// Creates a timer relative to host monotonic runtime time.
    #[must_use]
    pub fn sleep(&self, duration: RuntimeDuration) -> Sleep {
        let deadline = self
            .now()
            .checked_add(duration)
            .ok_or(TimeError::DeadlineOverflow);
        Sleep::new(RuntimeHandle::Host(self.clone()), deadline)
    }

    /// Creates a timer for an absolute runtime-relative instant.
    #[must_use]
    pub fn sleep_until(&self, deadline: RuntimeInstant) -> Sleep {
        Sleep::new(RuntimeHandle::Host(self.clone()), Ok(deadline))
    }

    /// Returns one seeded workload value.
    ///
    /// # Errors
    ///
    /// Returns [`RandomError::RuntimeStopped`] without consuming a draw after
    /// the runtime leaves its running state.
    pub fn random_u64(&self) -> Result<u64, RandomError> {
        self.shared.random_u64()
    }

    /// Uniformly chooses from `0..upper_exclusive`.
    ///
    /// # Errors
    ///
    /// Returns [`RandomError::ZeroUpperBound`] when `upper_exclusive` is zero,
    /// or [`RandomError::RuntimeStopped`] after the runtime leaves its running
    /// state. Neither failure consumes a draw. Argument validation takes
    /// precedence over the stopped-state check.
    pub fn random_below(&self, upper_exclusive: u64) -> Result<u64, RandomError> {
        self.shared.random_below(upper_exclusive)
    }

    /// Makes an exact rational boolean choice.
    ///
    /// # Errors
    ///
    /// Returns [`RandomError::InvalidRatio`] when the ratio is invalid, or
    /// [`RandomError::RuntimeStopped`] after the runtime leaves its running
    /// state. Neither failure consumes a draw. Argument validation takes
    /// precedence over the stopped-state check.
    pub fn random_bool_ratio(&self, numerator: u64, denominator: u64) -> Result<bool, RandomError> {
        self.shared.random_bool_ratio(numerator, denominator)
    }

    /// Returns the workload stream position without consuming a draw.
    #[must_use]
    pub fn random_position(&self) -> RngCheckpoint {
        self.shared.random_position()
    }
}

/// Thread-safe join future for a portable host-runtime task.
pub struct HostSendJoinHandle<T> {
    task: Option<TaskId>,
    state: Arc<HostSendJoinState<T>>,
    core: Arc<HostCrossThreadCore>,
    requested: Arc<AtomicBool>,
}

impl<T> HostSendJoinHandle<T> {
    /// Returns the admitted task identifier.
    ///
    /// This is `None` only for the already-resolved handle returned when a
    /// send-spawn races with, or occurs after, runtime shutdown.
    #[must_use]
    pub const fn id(&self) -> Option<TaskId> {
        self.task
    }

    /// Requests cancellation from any thread.
    pub fn abort(&self) {
        if !self.requested.swap(true, Ordering::AcqRel)
            && let Some(task) = self.task
        {
            self.core.push_abort(task);
        }
    }

    /// Returns whether the task has produced a terminal join result.
    #[must_use]
    pub fn is_finished(&self) -> bool {
        self.state.is_finished()
    }
}

impl<T> Future for HostSendJoinHandle<T> {
    type Output = Result<T, JoinError>;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        self.state.poll(context)
    }
}

/// Cloneable thread-safe capability for admitting portable tasks.
#[derive(Clone)]
pub struct HostSendHandle {
    core: Arc<HostCrossThreadCore>,
}

impl HostSendHandle {
    /// Admits a `Send` task through bounded cross-thread ingress.
    ///
    /// # Errors
    ///
    /// Returns [`SpawnError::ResourceExhausted`] when the ingress queue or the
    /// live-task limit is full, or [`SpawnError::IdentifierExhausted`] when no
    /// task identifier can be reserved. A spawn that races with or follows
    /// runtime shutdown is not an error: it returns an already-resolved
    /// [`HostSendJoinHandle`] whose join yields [`JoinError::RuntimeStopped`].
    pub fn spawn<F>(&self, future: F) -> Result<HostSendJoinHandle<F::Output>, SpawnError>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        let join = Arc::new(HostSendJoinState::new());
        let erased: Arc<dyn ErasedJoinState + Send + Sync> = join.clone();
        let requested = Arc::new(AtomicBool::new(false));
        let command = SendSpawnCommand {
            id: None,
            future: Box::pin(HostSendTaskHarness {
                future: Box::pin(future),
                join: Arc::clone(&join),
            }),
            join: erased,
        };
        match self.core.enqueue_spawn(command) {
            Ok(id) => Ok(HostSendJoinHandle {
                task: Some(id),
                state: join,
                core: Arc::clone(&self.core),
                requested,
            }),
            Err((SpawnError::RuntimeStopped, command)) => {
                let _ = join.finish_typed(Err(JoinError::RuntimeStopped));
                drop_value_caught(command);
                Ok(HostSendJoinHandle {
                    task: None,
                    state: join,
                    core: Arc::clone(&self.core),
                    requested,
                })
            }
            Err((error, command)) => {
                drop_value_caught(command);
                Err(error)
            }
        }
    }
}

/// Failure to provision the host runtime's blocking workers.
#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum HostBlockingError {
    /// A worker thread could not be spawned.
    WorkerSpawn {
        /// The spawning failure's operating-system error code, when known.
        raw_os_error: Option<i32>,
        /// The spawning failure rendered for diagnostics.
        message: String,
    },
}

impl fmt::Display for HostBlockingError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::WorkerSpawn {
                raw_os_error,
                message,
            } => {
                formatter.write_str("could not spawn host blocking worker")?;
                if let Some(code) = raw_os_error {
                    write!(formatter, " (OS error {code})")?;
                }
                write!(formatter, ": {message}")
            }
        }
    }
}

impl std::error::Error for HostBlockingError {}

enum BlockingWork {
    Job(Box<dyn FnOnce() + Send + 'static>),
    Stop,
}

struct BlockingJobQueue {
    work: Mutex<VecDeque<BlockingWork>>,
    available: Condvar,
}

impl BlockingJobQueue {
    fn push(&self, work: BlockingWork) {
        lock_unpoisoned(&self.work).push_back(work);
        self.available.notify_one();
    }
}

fn run_blocking_worker(queue: &BlockingJobQueue) {
    loop {
        let work = {
            let mut work = lock_unpoisoned(&queue.work);
            loop {
                if let Some(work) = work.pop_front() {
                    break work;
                }
                work = queue
                    .available
                    .wait(work)
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
            }
        };
        match work {
            BlockingWork::Stop => return,
            BlockingWork::Job(job) => {
                // One tenant's unwinding job must not shrink the shared
                // fleet; the job's own guards terminalize whatever command
                // it was running.
                crate::contain_panic(job);
            }
        }
    }
}

/// The blocking fleet: stopped and joined when the last capability clone —
/// including the runtime core's own cache — drops.
struct BlockingWorkers {
    queue: Arc<BlockingJobQueue>,
    workers: Mutex<Vec<thread::JoinHandle<()>>>,
}

impl Drop for BlockingWorkers {
    fn drop(&mut self) {
        // Stop sentinels queue behind every admitted job, so each job runs
        // before its worker exits. A worker cannot be the one joining
        // itself: a job holding the last capability clone skips its own
        // thread and detaches it, mirroring the owner-thread join guard.
        let workers = std::mem::take(&mut *lock_unpoisoned(&self.workers));
        for _ in 0..workers.len() {
            self.queue.push(BlockingWork::Stop);
        }
        for worker in workers {
            if worker.thread().id() != thread::current().id() {
                let _ = worker.join();
            }
        }
    }
}

/// Cloneable capability for punting blocking closures onto workers
/// provisioned by the host runtime.
///
/// Host-only, deliberately: simulation providers complete in virtual time
/// and have no blocking work to punt, so no sim analog exists and this
/// type never appears on the portable surface. The workers live as long as
/// any clone of this capability — the runtime provisions them, but their
/// lifetime follows the holders, so submission is infallible and a
/// provider draining its own teardown never races runtime shutdown.
#[derive(Clone)]
pub struct HostBlocking {
    host: Arc<BlockingWorkers>,
}

impl HostBlocking {
    fn provision(workers: usize) -> Result<Self, HostBlockingError> {
        let queue = Arc::new(BlockingJobQueue {
            work: Mutex::new(VecDeque::new()),
            available: Condvar::new(),
        });
        let mut joins = Vec::with_capacity(workers);
        for _ in 0..workers {
            let worker_queue = Arc::clone(&queue);
            let join = thread::Builder::new()
                .name("kr-runtime-host-blocking".to_owned())
                .spawn(move || run_blocking_worker(&worker_queue));
            match join {
                Ok(join) => joins.push(join),
                Err(error) => {
                    // A partial fleet must not leak parked workers.
                    for _ in 0..joins.len() {
                        queue.push(BlockingWork::Stop);
                    }
                    for join in joins {
                        let _ = join.join();
                    }
                    return Err(HostBlockingError::WorkerSpawn {
                        raw_os_error: error.raw_os_error(),
                        message: error.to_string(),
                    });
                }
            }
        }
        Ok(Self {
            host: Arc::new(BlockingWorkers {
                queue,
                workers: Mutex::new(joins),
            }),
        })
    }

    /// Enqueues one blocking job, FIFO across the runtime's workers.
    ///
    /// Infallible by design: the workers live as long as any clone of this
    /// capability, so there is no stopped-executor error path to surface.
    /// The job owns its completion path and must guard terminal reporting;
    /// the worker contains per-job panics.
    pub fn submit(&self, job: impl FnOnce() + Send + 'static) {
        self.host.queue.push(BlockingWork::Job(Box::new(job)));
    }
}

/// Cloneable cross-thread stop and status capability.
#[derive(Clone)]
pub struct HostControl {
    core: Arc<HostCrossThreadCore>,
}

/// A passive ingress snapshot; sampling admits no work and reads no clocks.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct HostRuntimeDiagnostics {
    /// Ingress entries waiting for owner admission at the time of sampling.
    pub queued_ingress: usize,
    /// Maximum queued cross-thread wakes, aborts, and send-spawns.
    pub ingress_limit: usize,
    /// Whether infallible ingress has exceeded the configured capacity.
    pub overflowed: bool,
}

/// A weak, cross-thread observer that does not retain the host runtime core.
#[derive(Clone)]
pub struct HostRuntimeObserver {
    core: std::sync::Weak<HostCrossThreadCore>,
}

impl HostRuntimeObserver {
    /// Samples ingress without admitting work or reading time.
    /// Returns `None` once the runtime core and its strong capabilities drop.
    #[must_use]
    pub fn snapshot(&self) -> Option<HostRuntimeDiagnostics> {
        self.core
            .upgrade()
            .map(|core| HostControl { core }.diagnostics())
    }
}

impl HostControl {
    /// Tests runtime identity without reading time or admitting any work.
    #[must_use]
    pub fn belongs_to(&self, handle: &HostHandle) -> bool {
        Arc::ptr_eq(&self.core, &handle.shared.core)
    }

    /// A passive weak observer cannot postpone runtime destruction.
    #[must_use]
    pub fn observer(&self) -> HostRuntimeObserver {
        HostRuntimeObserver {
            core: Arc::downgrade(&self.core),
        }
    }

    /// Samples bounded ingress state without admitting work or reading time.
    #[must_use]
    pub fn diagnostics(&self) -> HostRuntimeDiagnostics {
        HostRuntimeDiagnostics {
            queued_ingress: lock_unpoisoned(&self.core.inner).queue.len(),
            ingress_limit: self.core.max_ingress,
            overflowed: self.core.overflow.load(Ordering::Acquire),
        }
    }

    /// Reads the same monotonic timeline used by owner-task timers, from any
    /// thread. This admits no work and does not wake or advance the runtime.
    #[must_use]
    pub fn now(&self) -> RuntimeInstant {
        self.core.now_raw()
    }

    /// Requests graceful stop. Repeated requests are idempotent.
    pub fn request_stop(&self) {
        self.core.request_stop();
    }

    /// Returns the current host-runtime lifecycle.
    #[must_use]
    pub fn status(&self) -> HostStatus {
        self.core.state()
    }
}

/// Single-owner production executor with cross-thread wake ingress.
///
/// The controller remains bound to its construction thread:
///
/// ```compile_fail
/// fn require_send<T: Send>(_: T) {}
/// require_send(kr_runtime::HostRuntime::default());
/// ```
pub struct HostRuntime {
    shared: Rc<Shared>,
}

impl Default for HostRuntime {
    fn default() -> Self {
        Self::new(HostConfig::default()).expect("default host runtime config is valid")
    }
}

impl HostRuntime {
    /// Creates a host runtime bound to the current OS thread.
    ///
    /// # Errors
    ///
    /// Returns [`HostConfigError`] when `max_ingress`, `max_ingress_per_turn`,
    /// or `blocking_workers` is zero.
    pub fn new(config: HostConfig) -> Result<Self, HostConfigError> {
        if config.max_ingress == 0 {
            return Err(HostConfigError::ZeroIngressCapacity);
        }
        if config.max_ingress_per_turn == 0 {
            return Err(HostConfigError::ZeroIngressPerTurn);
        }
        if config.blocking_workers == 0 {
            return Err(HostConfigError::ZeroBlockingWorkers);
        }
        let core = Arc::new(HostCrossThreadCore::new(&config));
        Ok(Self {
            shared: Rc::new(Shared {
                core,
                state: RefCell::new(State::new(config.max_timers)),
                pending_aborts: RefCell::new(VecDeque::new()),
                failed_ingress_remainder: RefCell::new(VecDeque::new()),
                random: RefCell::new(DeterministicRng::from_root_seed(
                    config.seed,
                    RandomStream::Workload,
                )),
                fatal_error: RefCell::new(None),
                config,
            }),
        })
    }

    /// Returns an owner-thread handle for local `!Send` tasks.
    #[must_use]
    pub fn handle(&self) -> HostHandle {
        HostHandle {
            shared: Rc::clone(&self.shared),
        }
    }

    /// Returns a thread-safe handle for admitting `Send` tasks.
    #[must_use]
    pub fn send_handle(&self) -> HostSendHandle {
        HostSendHandle {
            core: Arc::clone(&self.shared.core),
        }
    }

    /// Returns a cross-thread stop and status capability.
    #[must_use]
    pub fn control(&self) -> HostControl {
        HostControl {
            core: Arc::clone(&self.shared.core),
        }
    }

    /// Provisions (once) and returns the runtime's blocking capability.
    ///
    /// The workers live as long as any clone of the returned capability,
    /// so holders may outlive this runtime; a runtime that never calls
    /// this spawns no blocking workers.
    ///
    /// # Errors
    ///
    /// Returns [`HostBlockingError`] when a worker thread cannot be
    /// spawned; a failed provisioning attempt may be retried.
    pub fn blocking(&self) -> Result<HostBlocking, HostBlockingError> {
        self.shared.core.blocking_capability()
    }

    /// Drives a borrowed root until completion, stop, or a runtime failure.
    ///
    /// # Errors
    ///
    /// Returns [`HostRunError`] when the root cannot be admitted or driven to
    /// completion: a controller stop request, an already-terminal runtime, a
    /// reentrant drive, a rejected root spawn, a root panic, a fatal task
    /// boundary failure, or exceeded ingress capacity.
    /// [`HostRunError::disposition`] classifies whether later driving is
    /// meaningful; a fatal failure is retained and returned by every later
    /// fallible operation.
    /// Cleanup failures are attached through [`HostRunError::cleanup_failure`]
    /// and never replace the initiating failure. Fatal cleanup makes the
    /// combined error fatal even if its primary category is terminal or
    /// resumable.
    pub fn block_on<F>(&mut self, future: F) -> Result<F::Output, HostRunError>
    where
        F: Future,
    {
        if current_task_id().is_some() {
            let error = self.shared.error(HostRunErrorKind::ReentrantDrive);
            return Err(self.reject_root(future, error));
        }
        if let Some(error) = self.shared.sync_ingress_failure() {
            return Err(self.reject_root(future, error));
        }
        match self.shared.core.state() {
            HostStatus::Running => {}
            HostStatus::StopRequested => {
                let error = self.shared.error(HostRunErrorKind::StopRequested);
                return Err(self.reject_root(future, error));
            }
            HostStatus::Stopped | HostStatus::Failed => {
                let error = self
                    .shared
                    .sync_ingress_failure()
                    .unwrap_or_else(|| self.shared.error(HostRunErrorKind::RuntimeStopped));
                return Err(self.reject_root(future, error));
            }
        }

        let id = match self.shared.install_root() {
            Ok(id) => id,
            Err(SpawnError::RuntimeStopped) => {
                let error = self.shared.sync_ingress_failure().unwrap_or_else(|| {
                    if self.shared.core.state() == HostStatus::StopRequested {
                        self.shared.error(HostRunErrorKind::StopRequested)
                    } else {
                        self.shared.error(HostRunErrorKind::RuntimeStopped)
                    }
                });
                return Err(self.reject_root(future, error));
            }
            Err(error) => {
                let run_error = self.shared.error(HostRunErrorKind::RootSpawnFailed(error));
                return Err(self.reject_root(future, run_error));
            }
        };
        let mut root = RootDriver::new(Rc::clone(&self.shared), id, future);

        loop {
            if let Some(error) = self.shared.sync_ingress_failure() {
                return Err(self.fail_root(&mut root, error));
            }
            if self.shared.core.state() == HostStatus::StopRequested {
                return Err(self.stop_root(&mut root));
            }

            if let Err(error) = self.shared.drain_ingress() {
                return Err(self.fail_root(&mut root, error));
            }
            if self.shared.core.state() == HostStatus::StopRequested {
                return Err(self.stop_root(&mut root));
            }

            // Ready tasks may wake themselves indefinitely. Service timers
            // every turn after freeing ingress capacity. If timers wake any
            // tasks, admit those wakes on the next turn before polling more
            // work that could refill the bounded ingress queue.
            match self.shared.fire_due_timers() {
                Ok(true) => continue,
                Ok(false) => {}
                Err(error) => return Err(self.fail_root(&mut root, error)),
            }

            match self.shared.cancel_next_task() {
                Ok(true) => continue,
                Ok(false) => {}
                Err(error) => return Err(self.fail_root(&mut root, error)),
            }

            let ready = match self.shared.take_ready() {
                Ok(ready) => ready,
                Err(error) => return Err(self.fail_root(&mut root, error)),
            };
            if let Some(ready) = ready {
                match ready {
                    ReadyTask::Owned { id, future, signal } => {
                        if let Err(error) = self.shared.poll_task(id, future, signal) {
                            return Err(self.fail_root(&mut root, error));
                        }
                        continue;
                    }
                    ReadyTask::ScopedRoot { id, signal } if id == root.id => {
                        if let Err(error) = root.poll_ready(signal) {
                            if self.shared.core.state() == HostStatus::StopRequested
                                && matches!(error.kind, HostRunErrorKind::RootPanicked { .. })
                            {
                                let mut stop = self.shared.error(HostRunErrorKind::StopRequested);
                                stop.attach_cleanup(error);
                                return Err(self.fail_root(&mut root, stop));
                            }
                            return Err(self.fail_root(&mut root, error));
                        }
                        if let Some(output) = root.take_output() {
                            return Ok(output);
                        }
                        continue;
                    }
                    ReadyTask::ScopedRoot { id, .. } => {
                        let error = self
                            .shared
                            .error(HostRunErrorKind::ScopedRootUnavailable { task: id });
                        return Err(self.fail_root(&mut root, error));
                    }
                }
            }

            if self.shared.core.has_pending()
                || self.shared.has_pending_abort()
                || !self.shared.state.borrow().tasks.ready_is_empty()
            {
                continue;
            }
            match self.shared.core.state() {
                HostStatus::Running => {}
                HostStatus::StopRequested => return Err(self.stop_root(&mut root)),
                HostStatus::Stopped | HostStatus::Failed => {
                    let error = self
                        .shared
                        .sync_ingress_failure()
                        .unwrap_or_else(|| self.shared.error(HostRunErrorKind::RuntimeStopped));
                    return Err(self.fail_root(&mut root, error));
                }
            }
            if let Some(deadline) = self.shared.next_deadline() {
                let now = self.shared.core.now_raw();
                if let Some(remaining) = deadline.checked_duration_since(now) {
                    thread::park_timeout(Duration::from_nanos(remaining.as_nanos()));
                }
            } else {
                thread::park();
            }
        }
    }

    fn reject_root<F: Future>(&mut self, future: F, mut error: HostRunError) -> HostRunError {
        if error.disposition() == RunErrorDisposition::Fatal {
            error = self.shared.retain_fatal(error);
        }
        if let Err(panic) = drop_value_result(future) {
            error.attach_cleanup(
                self.shared
                    .error(HostRunErrorKind::RejectedRootDropPanicked { panic }),
            );
        }
        self.finish_failure(error)
    }

    fn fail_root<F: Future>(
        &mut self,
        root: &mut RootDriver<F>,
        error: HostRunError,
    ) -> HostRunError {
        let mut primary = if error.disposition() == RunErrorDisposition::Fatal {
            self.shared.retain_fatal(error)
        } else {
            error
        };
        if let Some(cleanup) = root.cancel() {
            primary.attach_cleanup(cleanup);
        }
        self.finish_failure(primary)
    }

    fn stop_root<F: Future>(&mut self, root: &mut RootDriver<F>) -> HostRunError {
        let stop = self.shared.error(HostRunErrorKind::StopRequested);
        self.fail_root(root, stop)
    }

    fn finish_failure(&self, mut primary: HostRunError) -> HostRunError {
        // Root destructors may trigger infallible ingress failure without
        // panicking. Observe that boundary before classifying the result.
        if let Some(failure) = self.shared.sync_ingress_failure()
            && failure != primary
        {
            primary.attach_cleanup(failure);
        }
        if primary.disposition() == RunErrorDisposition::Fatal {
            // The caller selected the initiating error before running user
            // cleanup. Publish it with its secondary context before teardown.
            self.shared.core.mark_failed();
            *self.shared.fatal_error.borrow_mut() = Some(primary.clone());
        }
        if current_task_id().is_some() {
            // A rejected recursive drive must let the active poll return
            // before the outer driver performs teardown.
            return primary;
        }
        if (primary.disposition() != RunErrorDisposition::Resumable
            || self.shared.core.state() == HostStatus::StopRequested)
            && let Some(cleanup) = self.shared.shutdown()
        {
            if primary.disposition() == RunErrorDisposition::Fatal {
                return cleanup;
            }
            primary.attach_cleanup(cleanup);
            *self.shared.fatal_error.borrow_mut() = Some(primary.clone());
        }
        primary
    }

    /// Performs idempotent checked teardown of every admitted task and timer.
    ///
    /// # Errors
    ///
    /// Returns [`HostRunErrorKind::ReentrantDrive`] when called from inside a
    /// task poll, or the first retained teardown failure: a destructor or
    /// registered-waker panic, or ingress overflow that could strand work.
    /// Repeated calls return the retained fatal error or otherwise succeed
    /// without repeating teardown.
    pub fn shutdown(&mut self) -> Result<(), HostRunError> {
        if current_task_id().is_some() {
            return Err(self.shared.error(HostRunErrorKind::ReentrantDrive));
        }
        if let Some(error) = self.shared.shutdown() {
            Err(error)
        } else {
            Ok(())
        }
    }

    /// Consumes the runtime and reports checked teardown failures.
    ///
    /// Prefer this operation when relinquishing the runtime: [`Drop`] also
    /// tears it down but cannot surface destructor or waker failures.
    ///
    /// # Errors
    ///
    /// Identical to [`Self::shutdown`].
    pub fn finish(mut self) -> Result<(), HostRunError> {
        self.shutdown()
    }
}

impl Drop for HostRuntime {
    fn drop(&mut self) {
        let _ = self.shared.shutdown();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::future::{Future, pending};
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[test]
    fn due_timers_fire_while_the_root_remains_continuously_ready() {
        struct CountWake(AtomicUsize);

        impl Wake for CountWake {
            fn wake(self: Arc<Self>) {
                self.0.fetch_add(1, Ordering::Relaxed);
            }
        }

        let mut runtime = HostRuntime::default();
        let shared = Rc::clone(&runtime.shared);
        let fired = Arc::new(CountWake(AtomicUsize::new(0)));
        let waker = Waker::from(Arc::clone(&fired));
        let mut registration = None;
        let mut polls = 0;
        let observed = runtime
            .block_on(std::future::poll_fn(|context| {
                polls += 1;
                if registration.is_none() {
                    // Register an already-due timer inside the first poll.
                    // No clock delay or external wake can mask starvation.
                    registration = Some(
                        shared
                            .register_timer(
                                current_task_id().expect("root is current"),
                                RuntimeInstant::ZERO,
                                &waker,
                            )
                            .expect("timer is admitted"),
                    );
                }
                if fired.0.load(Ordering::Relaxed) != 0 || polls == 128 {
                    return Poll::Ready(fired.0.load(Ordering::Relaxed));
                }
                context.waker().wake_by_ref();
                Poll::Pending
            }))
            .expect("root completes within its poll bound");
        assert_eq!(observed, 1, "ready work must not starve a due timer");
        assert_eq!(polls, 2, "the next scheduler turn services the timer");
    }

    #[test]
    fn timer_wakes_share_one_ingress_slot_with_a_continuously_ready_root() {
        let mut runtime = HostRuntime::new(HostConfig {
            max_ingress: 1,
            max_ingress_per_turn: 1,
            ..HostConfig::default()
        })
        .expect("one ingress slot is valid");
        let completed = Rc::new(Cell::new(false));
        let sibling_completed = Rc::clone(&completed);
        let mut sibling_polls = 0;
        let sibling = runtime
            .handle()
            .spawn(std::future::poll_fn(move |_| {
                sibling_polls += 1;
                if sibling_polls == 1 {
                    Poll::Pending
                } else {
                    sibling_completed.set(true);
                    Poll::Ready(())
                }
            }))
            .expect("sibling is admitted");
        let signal = runtime
            .shared
            .state
            .borrow()
            .tasks
            .task(sibling.id())
            .expect("sibling is live")
            .signal
            .clone();
        let waker = Waker::from(signal);
        let shared = Rc::clone(&runtime.shared);
        let mut registration = None;
        let mut root_polls = 0;
        let observed = runtime
            .block_on(std::future::poll_fn(|context| {
                root_polls += 1;
                if registration.is_none() {
                    registration = Some(
                        shared
                            .register_timer(sibling.id(), RuntimeInstant::ZERO, &waker)
                            .expect("sibling timer is admitted"),
                    );
                }
                if completed.get() || root_polls == 128 {
                    return Poll::Ready(completed.get());
                }
                context.waker().wake_by_ref();
                Poll::Pending
            }))
            .expect("timer and root wakes do not overflow their shared ingress slot");
        assert!(
            observed,
            "the timer wakes the sibling while the root remains ready"
        );
    }

    #[test]
    fn the_blocking_fleet_is_provisioned_once_and_shared() {
        let runtime = HostRuntime::new(HostConfig::default()).expect("create runtime");
        let first = runtime.blocking().expect("provision blocking workers");
        let second = runtime.blocking().expect("reuse blocking workers");
        assert!(
            Arc::ptr_eq(&first.host, &second.host),
            "repeated requests share one fleet"
        );
        let through_handle = runtime
            .handle()
            .blocking()
            .expect("handle shares the fleet");
        assert!(
            Arc::ptr_eq(&first.host, &through_handle.host),
            "the owner handle shares the runtime's fleet"
        );
    }

    fn poll_once<F: Future + Unpin>(future: &mut F) -> Poll<F::Output> {
        let mut context = Context::from_waker(Waker::noop());
        Pin::new(future).poll(&mut context)
    }

    fn assert_live_reservation(shared: &Shared, id: TaskId, counted: usize) {
        let state = shared.state.borrow();
        assert!(
            state.tasks.task(id).is_some(),
            "task {id} is live in the slab"
        );
        drop(state);

        let inner = lock_unpoisoned(&shared.core.inner);
        let slot = &inner.ids.slots[id.slot() as usize];
        assert!(slot.allocated, "task {id} owns its IdPool slot");
        assert_eq!(slot.generation, id.generation());
        assert_eq!(inner.ids.counted, counted);
    }

    fn assert_no_reserved_ids(shared: &Shared) {
        let state = shared.state.borrow();
        assert_eq!(state.tasks.live_len(), 0);
        drop(state);

        let inner = lock_unpoisoned(&shared.core.inner);
        assert_eq!(inner.ids.counted, 0);
        assert!(inner.ids.slots.iter().all(|slot| !slot.allocated));
        assert!(
            inner
                .queue
                .iter()
                .all(|item| !matches!(item, IngressItem::Spawn(_)))
        );
        assert!(shared.failed_ingress_remainder.borrow().is_empty());
    }

    fn poll_next_owned_task(shared: &Rc<Shared>) -> TaskId {
        let ready = shared
            .take_ready()
            .expect("ready-task lookup succeeds")
            .expect("one owned task is ready");
        let ReadyTask::Owned { id, future, signal } = ready else {
            panic!("expected an owned task");
        };
        shared
            .poll_task(id, future, signal)
            .expect("owned task poll succeeds");
        id
    }

    #[test]
    fn reserved_task_id_generation_stays_in_sync_across_slot_reuse() {
        let runtime = HostRuntime::new(HostConfig {
            max_tasks: 1,
            ..HostConfig::default()
        })
        .expect("config is valid");
        let shared = Rc::clone(&runtime.shared);

        let root = shared.install_root().expect("root reserves the fresh slot");
        assert_eq!(root, TaskId::from_parts(0, 0));
        assert_live_reservation(&shared, root, 0);
        let removed = shared.remove_task(root).expect("root remains live");
        assert!(removed.scoped_root);
        assert_no_reserved_ids(&shared);

        let local = shared
            .spawn_local(pending::<()>())
            .expect("local task reuses the root slot");
        assert_eq!(local.id(), TaskId::from_parts(0, 1));
        assert_live_reservation(&shared, local.id(), 1);
        shared
            .cancel_task(local.id())
            .expect("local task cancellation succeeds");
        assert_no_reserved_ids(&shared);

        let mut send = runtime
            .send_handle()
            .spawn(pending::<()>())
            .expect("send task reserves the released slot");
        let send_id = send.id().expect("running-runtime spawn has an id");
        assert_eq!(send_id, TaskId::from_parts(0, 2));
        shared
            .drain_ingress()
            .expect("send task admission succeeds");
        assert_live_reservation(&shared, send_id, 1);
        shared
            .cancel_task(send_id)
            .expect("send task cancellation succeeds");
        assert_eq!(poll_once(&mut send), Poll::Ready(Err(JoinError::Cancelled)));
        assert_no_reserved_ids(&shared);

        let replacement = shared
            .spawn_local(pending::<()>())
            .expect("local task reuses the send slot");
        assert_eq!(replacement.id(), TaskId::from_parts(0, 3));
        assert_live_reservation(&shared, replacement.id(), 1);
        shared
            .cancel_task(replacement.id())
            .expect("replacement cancellation succeeds");
        assert_no_reserved_ids(&shared);
        assert!(shared.shutdown().is_none());
    }

    #[test]
    fn rejected_local_reserved_id_is_released() {
        let runtime = HostRuntime::default();
        let shared = Rc::clone(&runtime.shared);
        let seed = shared
            .spawn_local(pending::<()>())
            .expect("seed task reserves the fresh slot");
        let slot = seed.id().slot();
        shared
            .cancel_task(seed.id())
            .expect("seed task cancellation succeeds");

        shared
            .state
            .borrow_mut()
            .tasks
            .set_vacant_slot_generation_for_test(slot, 7);
        assert!(matches!(
            shared.spawn_local(pending::<()>()),
            Err(SpawnError::IdentifierExhausted)
        ));

        assert_no_reserved_ids(&shared);
        assert!(matches!(
            shared.shutdown(),
            Some(HostRunError {
                kind: HostRunErrorKind::Task(TaskFailure::SequenceExhausted),
                ..
            })
        ));
    }

    #[test]
    fn local_enqueue_rejection_removes_task_and_releases_id() {
        let runtime = HostRuntime::default();
        let shared = Rc::clone(&runtime.shared);
        shared
            .state
            .borrow_mut()
            .tasks
            .set_next_enqueue_sequence_for_test(u64::MAX);

        assert!(matches!(
            shared.spawn_local(pending::<()>()),
            Err(SpawnError::IdentifierExhausted)
        ));

        assert_no_reserved_ids(&shared);
        assert!(matches!(
            shared.shutdown(),
            Some(HostRunError {
                kind: HostRunErrorKind::Task(TaskFailure::SequenceExhausted),
                ..
            })
        ));
    }

    struct PanicOnAdmissionDrop;

    impl Future for PanicOnAdmissionDrop {
        type Output = ();

        fn poll(self: Pin<&mut Self>, _context: &mut Context<'_>) -> Poll<Self::Output> {
            Poll::Pending
        }
    }

    impl Drop for PanicOnAdmissionDrop {
        fn drop(&mut self) {
            panic!("admission future destructor failed");
        }
    }

    #[test]
    fn admission_sequence_failure_precedes_destructor_panic() {
        let runtime = HostRuntime::default();
        let shared = Rc::clone(&runtime.shared);
        shared
            .state
            .borrow_mut()
            .tasks
            .set_next_enqueue_sequence_for_test(u64::MAX);

        assert!(matches!(
            shared.spawn_local(PanicOnAdmissionDrop),
            Err(SpawnError::IdentifierExhausted)
        ));

        assert_no_reserved_ids(&shared);
        assert!(matches!(
            shared.shutdown(),
            Some(HostRunError {
                kind: HostRunErrorKind::Task(TaskFailure::SequenceExhausted),
                ..
            })
        ));
    }

    struct ReenterAdmissionOnDrop {
        shared: Rc<Shared>,
        replacement: Rc<RefCell<Option<JoinHandle<()>>>>,
    }

    impl Future for ReenterAdmissionOnDrop {
        type Output = ();

        fn poll(self: Pin<&mut Self>, _context: &mut Context<'_>) -> Poll<Self::Output> {
            Poll::Pending
        }
    }

    impl Drop for ReenterAdmissionOnDrop {
        fn drop(&mut self) {
            self.shared
                .state
                .borrow_mut()
                .tasks
                .set_next_enqueue_sequence_for_test(0);
            let handle = HostHandle::current().expect("admission cleanup installs task context");
            let replacement = handle
                .spawn(pending::<()>())
                .expect("admission cleanup released scheduler state and task capacity");
            *self.replacement.borrow_mut() = Some(replacement);
        }
    }

    #[test]
    fn admission_rollback_releases_state_and_id_before_future_drop() {
        let runtime = HostRuntime::new(HostConfig {
            max_tasks: 1,
            ..HostConfig::default()
        })
        .expect("config is valid");
        let shared = Rc::clone(&runtime.shared);
        shared
            .state
            .borrow_mut()
            .tasks
            .set_next_enqueue_sequence_for_test(u64::MAX);
        let replacement = Rc::new(RefCell::new(None));

        assert!(matches!(
            shared.spawn_local(ReenterAdmissionOnDrop {
                shared: Rc::clone(&shared),
                replacement: Rc::clone(&replacement),
            }),
            Err(SpawnError::IdentifierExhausted)
        ));

        let mut replacement = replacement
            .borrow_mut()
            .take()
            .expect("future destructor admitted its replacement");
        assert_live_reservation(&shared, replacement.id(), 1);
        assert!(matches!(
            shared.shutdown(),
            Some(HostRunError {
                kind: HostRunErrorKind::Task(TaskFailure::SequenceExhausted),
                ..
            })
        ));
        assert_eq!(
            poll_once(&mut replacement),
            Poll::Ready(Err(JoinError::RuntimeStopped))
        );
        assert_no_reserved_ids(&shared);
    }

    #[test]
    fn send_enqueue_rejection_removes_task_and_releases_id() {
        let runtime = HostRuntime::default();
        let shared = Rc::clone(&runtime.shared);
        shared
            .state
            .borrow_mut()
            .tasks
            .set_next_enqueue_sequence_for_test(u64::MAX);
        let mut join = runtime
            .send_handle()
            .spawn(pending::<()>())
            .expect("send task reserves an id");

        let error = shared
            .drain_ingress()
            .expect_err("send task enqueue sequence is exhausted");

        assert_eq!(
            error.kind,
            HostRunErrorKind::Task(TaskFailure::SequenceExhausted)
        );
        assert_eq!(
            poll_once(&mut join),
            Poll::Ready(Err(JoinError::RuntimeStopped))
        );
        assert_no_reserved_ids(&shared);
        let retained = shared.retain_fatal(error);
        assert_eq!(shared.shutdown(), Some(retained));
    }

    #[test]
    fn send_spawn_rejected_during_owner_admission_resolves_runtime_stopped() {
        let runtime = HostRuntime::default();
        let mut join = runtime
            .send_handle()
            .spawn(pending::<()>())
            .expect("send task reserves an id before stop");
        let mut queued = runtime.shared.core.drain(1);
        runtime.shared.shutdown();
        let IngressItem::Spawn(command) = queued.pop_front().expect("spawn was queued") else {
            panic!("expected send-spawn");
        };
        runtime
            .shared
            .accept_send_spawn(command)
            .expect("stopped-state rejection has no cleanup failure");
        assert_eq!(
            poll_once(&mut join),
            Poll::Ready(Err(JoinError::RuntimeStopped))
        );
        assert_no_reserved_ids(&runtime.shared);
    }

    #[test]
    fn root_enqueue_rejection_removes_task_and_releases_id() {
        let runtime = HostRuntime::default();
        let shared = Rc::clone(&runtime.shared);
        shared
            .state
            .borrow_mut()
            .tasks
            .set_next_enqueue_sequence_for_test(u64::MAX);

        assert!(matches!(
            shared.install_root(),
            Err(SpawnError::IdentifierExhausted)
        ));

        assert_no_reserved_ids(&shared);
        assert!(matches!(
            shared.shutdown(),
            Some(HostRunError {
                kind: HostRunErrorKind::Task(TaskFailure::SequenceExhausted),
                ..
            })
        ));
    }

    #[test]
    fn root_insert_rejection_releases_id_and_retains_failure() {
        let runtime = HostRuntime::default();
        let shared = Rc::clone(&runtime.shared);
        let seed = shared
            .spawn_local(pending::<()>())
            .expect("seed task reserves the fresh slot");
        let slot = seed.id().slot();
        shared
            .cancel_task(seed.id())
            .expect("seed task cancellation succeeds");
        shared
            .state
            .borrow_mut()
            .tasks
            .set_vacant_slot_generation_for_test(slot, 7);

        assert!(matches!(
            shared.install_root(),
            Err(SpawnError::IdentifierExhausted)
        ));

        assert_no_reserved_ids(&shared);
        assert!(matches!(
            shared.shutdown(),
            Some(HostRunError {
                kind: HostRunErrorKind::Task(TaskFailure::SequenceExhausted),
                ..
            })
        ));
    }

    struct CaptureReadyWaker {
        captured: Arc<Mutex<Option<Waker>>>,
    }

    impl Future for CaptureReadyWaker {
        type Output = ();

        fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
            *lock_unpoisoned(&self.captured) = Some(context.waker().clone());
            Poll::Ready(())
        }
    }

    struct CountPendingPolls {
        polls: Arc<AtomicUsize>,
    }

    impl Future for CountPendingPolls {
        type Output = ();

        fn poll(self: Pin<&mut Self>, _context: &mut Context<'_>) -> Poll<Self::Output> {
            self.polls.fetch_add(1, Ordering::Relaxed);
            Poll::Pending
        }
    }

    #[test]
    fn stale_send_abort_and_wake_ignore_a_reused_slot() {
        let runtime = HostRuntime::default();
        let shared = Rc::clone(&runtime.shared);
        let captured = Arc::new(Mutex::new(None));
        let mut stale_join = runtime
            .send_handle()
            .spawn(CaptureReadyWaker {
                captured: Arc::clone(&captured),
            })
            .expect("send task is queued");
        let stale_id = stale_join.id().expect("queued task has an id");
        shared
            .drain_ingress()
            .expect("send task admission succeeds");
        assert_eq!(poll_next_owned_task(&shared), stale_id);
        assert_no_reserved_ids(&shared);

        let polls = Arc::new(AtomicUsize::new(0));
        let replacement = shared
            .spawn_local(CountPendingPolls {
                polls: Arc::clone(&polls),
            })
            .expect("replacement task is admitted");
        assert_eq!(replacement.id().slot(), stale_id.slot());
        assert_eq!(replacement.id().generation(), stale_id.generation() + 1);
        assert_eq!(poll_next_owned_task(&shared), replacement.id());
        assert_eq!(polls.load(Ordering::Relaxed), 1);
        assert_eq!(
            shared
                .state
                .borrow()
                .tasks
                .task(replacement.id())
                .expect("replacement remains live")
                .state,
            TaskState::Waiting
        );

        stale_join.abort();
        lock_unpoisoned(&captured)
            .take()
            .expect("completed task captured its waker")
            .wake();
        shared
            .drain_ingress()
            .expect("stale ingress is admitted and ignored");
        assert!(
            !shared
                .cancel_next_task()
                .expect("stale abort processing succeeds")
        );

        assert_live_reservation(&shared, replacement.id(), 1);
        assert_eq!(
            shared
                .state
                .borrow()
                .tasks
                .task(replacement.id())
                .expect("stale handles did not remove the replacement")
                .state,
            TaskState::Waiting
        );
        assert!(
            shared
                .take_ready()
                .expect("ready-task lookup succeeds")
                .is_none()
        );
        assert_eq!(polls.load(Ordering::Relaxed), 1);
        assert_eq!(poll_once(&mut stale_join), Poll::Ready(Ok(())));

        shared
            .cancel_task(replacement.id())
            .expect("replacement cancellation succeeds");
        assert_no_reserved_ids(&shared);
        assert!(shared.shutdown().is_none());
    }

    struct RefillIngressOnDrop {
        send: HostSendHandle,
        count: usize,
    }

    impl Future for RefillIngressOnDrop {
        type Output = ();

        fn poll(self: Pin<&mut Self>, _context: &mut Context<'_>) -> Poll<Self::Output> {
            Poll::Pending
        }
    }

    impl Drop for RefillIngressOnDrop {
        fn drop(&mut self) {
            for _ in 0..self.count {
                let _detached = self
                    .send
                    .spawn(pending::<()>())
                    .expect("failure cleanup can refill freed ingress slots");
            }
        }
    }

    #[test]
    fn fatal_send_rejection_preserves_remainder_with_bounded_ingress() {
        let runtime = HostRuntime::new(HostConfig {
            max_ingress: 2,
            max_ingress_per_turn: 2,
            ..HostConfig::default()
        })
        .expect("config is valid");
        let seed = runtime
            .shared
            .spawn_local(pending::<()>())
            .expect("seed task reserves the fresh slot");
        let slot = seed.id().slot();
        runtime
            .shared
            .cancel_task(seed.id())
            .expect("seed task cancellation succeeds");
        runtime
            .shared
            .state
            .borrow_mut()
            .tasks
            .set_vacant_slot_generation_for_test(slot, 7);

        let send = runtime.send_handle();
        let mut rejected = runtime
            .send_handle()
            .spawn(RefillIngressOnDrop { send, count: 2 })
            .expect("first send task is queued");
        let mut later = runtime
            .send_handle()
            .spawn(pending::<()>())
            .expect("later send task is queued in the same batch");

        let error = runtime
            .shared
            .drain_ingress()
            .expect_err("the first send task violates reserved-id generation sync");
        assert_eq!(
            error.kind,
            HostRunErrorKind::Task(TaskFailure::SequenceExhausted)
        );
        {
            let inner = lock_unpoisoned(&runtime.shared.core.inner);
            assert_eq!(inner.queue.len(), runtime.shared.config.max_ingress);
        }
        assert_eq!(runtime.shared.failed_ingress_remainder.borrow().len(), 1);
        assert_eq!(
            poll_once(&mut rejected),
            Poll::Ready(Err(JoinError::RuntimeStopped))
        );
        assert_eq!(poll_once(&mut later), Poll::Pending);

        let retained = runtime.shared.retain_fatal(error);
        assert_eq!(runtime.shared.shutdown(), Some(retained));
        assert_eq!(
            poll_once(&mut later),
            Poll::Ready(Err(JoinError::RuntimeStopped))
        );
        assert_no_reserved_ids(&runtime.shared);
        assert_eq!(runtime.control().status(), HostStatus::Failed);
    }

    #[test]
    fn mismatched_scoped_root_has_an_explicit_fatal_error() {
        let mut runtime = HostRuntime::default();
        let unexpected = runtime
            .shared
            .install_root()
            .expect("test-only competing root is installed");

        let error = runtime
            .block_on(async {})
            .expect_err("a ready root must match the root being driven");

        assert_eq!(
            error.kind,
            HostRunErrorKind::ScopedRootUnavailable { task: unexpected }
        );
        assert_eq!(error.disposition(), RunErrorDisposition::Fatal);
        assert_no_reserved_ids(&runtime.shared);
        assert_eq!(runtime.control().status(), HostStatus::Failed);
    }

    #[test]
    fn stop_rejected_wake_does_not_latch_the_pending_flag() {
        let core = Arc::new(HostCrossThreadCore::new(&HostConfig::default()));
        let signal = HostSignal {
            task: TaskId::from_parts(0, 0),
            core: Arc::clone(&core),
            pending: AtomicBool::new(false),
        };
        core.request_stop();

        signal.notify();

        assert!(!signal.pending.load(Ordering::Acquire));
        assert!(!core.has_pending());
        let _ = core.close();
    }
}

#[cfg(test)]
mod diagnostics_tests {
    use super::*;
    #[test]
    fn identity_and_weak_diagnostics_do_not_retain_runtime() {
        let first = HostRuntime::default();
        let second = HostRuntime::default();
        let control = first.control();
        assert!(control.belongs_to(&first.handle()));
        assert!(control.clone().belongs_to(&first.handle().clone()));
        assert!(!control.belongs_to(&second.handle()));
        let observer = control.observer();
        assert_eq!(observer.snapshot().unwrap().queued_ingress, 0);
        assert!(!control.diagnostics().overflowed);
        drop(control);
        drop(first);
        assert!(observer.snapshot().is_none());
    }
}
