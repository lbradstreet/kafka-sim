//! Task storage, polling harnesses, joins, and cancellation handles.

use crate::panic::panic_record_from_payload;
use std::cell::{Cell, RefCell};
use std::fmt;
use std::future::Future;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::pin::Pin;
use std::rc::{Rc, Weak as RcWeak};
use std::sync::Arc;
use std::task::{Context, Poll, Waker};

pub(crate) type BoxTaskFuture = Pin<Box<dyn Future<Output = Result<(), PanicRecord>> + 'static>>;

/// Maximum UTF-8 bytes retained from a panic payload.
pub const MAX_PANIC_MESSAGE_BYTES: usize = 4 * 1024;

/// A stable, generation-tagged task identifier scoped to one runtime instance.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct TaskId {
    slot: u32,
    generation: u32,
}

impl TaskId {
    pub(crate) const fn from_parts(slot: u32, generation: u32) -> Self {
        Self { slot, generation }
    }

    /// Returns the slab slot component.
    #[must_use]
    pub const fn slot(self) -> u32 {
        self.slot
    }

    /// Returns the generation component.
    #[must_use]
    pub const fn generation(self) -> u32 {
        self.generation
    }
}

impl fmt::Display for TaskId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}:{}", self.slot, self.generation)
    }
}

/// Why a task was not successfully joined.
#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum JoinError {
    /// The task was explicitly aborted.
    Cancelled,
    /// The task panicked while being polled.
    Panicked(PanicRecord),
    /// The owning runtime stopped before the task completed.
    RuntimeStopped,
}

impl fmt::Display for JoinError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Cancelled => formatter.write_str("task was cancelled"),
            Self::Panicked(record) => write!(formatter, "task panicked: {}", record.message),
            Self::RuntimeStopped => formatter.write_str("runtime stopped before task completed"),
        }
    }
}

impl std::error::Error for JoinError {}

/// Owned panic information suitable for a diagnostic artifact.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct PanicRecord {
    /// A stable text representation of the panic payload.
    pub message: String,
    /// Whether the original text exceeded [`MAX_PANIC_MESSAGE_BYTES`].
    pub message_truncated: bool,
}

/// A task-level failure category shared by all executors.
#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum TaskFailure {
    /// The task's future panicked while being polled.
    Panicked { task: TaskId, panic: PanicRecord },
    /// Dropping the task's future panicked.
    DropPanicked { task: TaskId, panic: PanicRecord },
    /// Notifying a registered waker panicked.
    WakerPanicked { task: TaskId, panic: PanicRecord },
    /// A checked scheduler sequence or identifier was exhausted.
    SequenceExhausted,
}

impl fmt::Display for TaskFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Panicked { task, panic } => {
                write!(formatter, "task {task} panicked: {}", panic.message)
            }
            Self::DropPanicked { task, panic } => {
                write!(
                    formatter,
                    "dropping task {task}'s future panicked: {}",
                    panic.message
                )
            }
            Self::WakerPanicked { task, panic } => {
                write!(
                    formatter,
                    "a waker registered by task {task} panicked: {}",
                    panic.message
                )
            }
            Self::SequenceExhausted => {
                formatter.write_str("scheduler sequence or identifier space exhausted")
            }
        }
    }
}

/// Whether a runtime error permits later driving.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RunErrorDisposition {
    /// The failed operation did not invalidate the runtime.
    Resumable,
    /// The runtime was already in its clean terminal stopped state.
    Terminal,
    /// The runtime is invalid and permanently retains the first such failure.
    Fatal,
}

/// An error returned while spawning a task.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum SpawnError {
    /// A bounded spawn resource is at its configured limit.
    ResourceExhausted {
        /// The exhausted resource, such as live tasks or cross-thread ingress.
        resource: &'static str,
        /// The configured bound that was reached.
        limit: usize,
    },
    /// The runtime has already stopped.
    RuntimeStopped,
    /// A task identifier generation was exhausted.
    IdentifierExhausted,
}

impl fmt::Display for SpawnError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ResourceExhausted { resource, limit } => {
                write!(formatter, "{resource} limit of {limit} is exhausted")
            }
            Self::RuntimeStopped => formatter.write_str("runtime is stopped"),
            Self::IdentifierExhausted => formatter.write_str("task identifier space exhausted"),
        }
    }
}

impl std::error::Error for SpawnError {}

/// The lifecycle state exposed in a runtime snapshot.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TaskState {
    /// The task is waiting for a wake-up.
    Waiting,
    /// The task has one ready-queue entry.
    Ready,
    /// The task is currently being polled.
    Running,
}

/// A future that resolves when its spawned task terminates.
///
/// # Panics
///
/// Polling a join handle again after it has returned [`Poll::Ready`] panics.
///
/// A local join handle is owner-thread-only even when its output is `Send`:
///
/// ```compile_fail
/// fn require_send<T: Send>(_: T) {}
/// let runtime = kr_runtime::HostRuntime::default();
/// let join = runtime.handle().spawn(async { 42 }).unwrap();
/// require_send(join);
/// ```
pub struct JoinHandle<T> {
    id: TaskId,
    state: Rc<JoinState<T>>,
    abort: AbortHandle,
}

impl<T> JoinHandle<T> {
    pub(crate) fn new(id: TaskId, state: Rc<JoinState<T>>, abort: AbortHandle) -> Self {
        Self { id, state, abort }
    }

    /// Returns the task identifier.
    #[must_use]
    pub const fn id(&self) -> TaskId {
        self.id
    }

    /// Requests cancellation at the next scheduler boundary.
    pub fn abort(&self) {
        self.abort.abort();
    }

    /// Returns an owner-thread cancellation capability.
    #[must_use]
    pub fn abort_handle(&self) -> AbortHandle {
        self.abort.clone()
    }

    /// Returns whether the task has produced a terminal join result.
    #[must_use]
    pub fn is_finished(&self) -> bool {
        self.state.is_finished()
    }
}

impl<T> Future for JoinHandle<T> {
    type Output = Result<T, JoinError>;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        self.state.poll(context)
    }
}

/// A cloneable owner-thread cancellation capability that never drops a future
/// inline.
#[derive(Clone)]
pub struct AbortHandle {
    task: TaskId,
    route: ExecutorRef,
    requested: Rc<Cell<bool>>,
}

impl AbortHandle {
    pub(crate) fn new_sim(
        task: TaskId,
        shared: RcWeak<crate::sim::Shared>,
        requested: Rc<Cell<bool>>,
    ) -> Self {
        Self {
            task,
            route: ExecutorRef::Sim(shared),
            requested,
        }
    }

    pub(crate) fn new_host(
        task: TaskId,
        shared: RcWeak<crate::host::Shared>,
        requested: Rc<Cell<bool>>,
    ) -> Self {
        Self {
            task,
            route: ExecutorRef::Host(shared),
            requested,
        }
    }

    /// Returns the target task identifier.
    #[must_use]
    pub const fn id(&self) -> TaskId {
        self.task
    }

    /// Records a cancellation request once and routes it to the owning runtime
    /// when the task is still live.
    ///
    /// The request is recorded even when the task has already completed or its
    /// runtime has stopped. Repeated calls through any clone are no-ops.
    pub fn abort(&self) {
        if self.requested.replace(true) {
            return;
        }
        self.route.request_abort(self.task);
    }

    /// Returns whether [`Self::abort`] was called through this handle or one of
    /// its clones.
    ///
    /// This is a caller-side intent latch: `true` does not imply that the
    /// request was delivered or that cancellation won a race with completion.
    #[must_use]
    pub fn is_abort_requested(&self) -> bool {
        self.requested.get()
    }
}

/// Returns a future that yields exactly once to the back of the runnable queue.
#[must_use]
pub const fn yield_now() -> YieldNow {
    YieldNow { yielded: false }
}

/// The future returned by [`yield_now`].
pub struct YieldNow {
    yielded: bool,
}

thread_local! {
    static CURRENT: RefCell<Option<CurrentTask>> = const { RefCell::new(None) };
}

/// A weak reference to either concrete executor's owner-local shared state.
#[derive(Clone)]
enum ExecutorRef {
    Sim(RcWeak<crate::sim::Shared>),
    Host(RcWeak<crate::host::Shared>),
}

impl ExecutorRef {
    /// Routes a cancellation request to the owning runtime if it is still
    /// alive.
    fn request_abort(&self, task: TaskId) {
        match self {
            Self::Sim(shared) => {
                if let Some(shared) = shared.upgrade() {
                    shared.request_abort(task);
                }
            }
            Self::Host(shared) => {
                if let Some(shared) = shared.upgrade() {
                    shared.request_abort(task);
                }
            }
        }
    }
}

struct CurrentTask {
    executor: ExecutorRef,
    task: TaskId,
}

pub(crate) struct CurrentGuard {
    previous: Option<CurrentTask>,
}

impl CurrentGuard {
    pub(crate) fn enter_sim(shared: &Rc<crate::sim::Shared>, task: TaskId) -> Self {
        Self::enter(ExecutorRef::Sim(Rc::downgrade(shared)), task)
    }

    pub(crate) fn enter_host(shared: &Rc<crate::host::Shared>, task: TaskId) -> Self {
        Self::enter(ExecutorRef::Host(Rc::downgrade(shared)), task)
    }

    fn enter(executor: ExecutorRef, task: TaskId) -> Self {
        let previous =
            CURRENT.with(|current| current.borrow_mut().replace(CurrentTask { executor, task }));
        Self { previous }
    }
}

impl Drop for CurrentGuard {
    fn drop(&mut self) {
        CURRENT.with(|current| {
            *current.borrow_mut() = self.previous.take();
        });
    }
}

/// Returns the runtime task currently being polled or torn down.
#[must_use]
pub fn current_task_id() -> Option<TaskId> {
    CURRENT.with(|current| current.borrow().as_ref().map(|entry| entry.task))
}

pub(crate) fn current_sim_shared() -> Option<Rc<crate::sim::Shared>> {
    CURRENT.with(|current| {
        let current = current.borrow();
        let ExecutorRef::Sim(shared) = &current.as_ref()?.executor else {
            return None;
        };
        shared.upgrade()
    })
}

pub(crate) fn current_host_shared() -> Option<Rc<crate::host::Shared>> {
    CURRENT.with(|current| {
        let current = current.borrow();
        let ExecutorRef::Host(shared) = &current.as_ref()?.executor else {
            return None;
        };
        shared.upgrade()
    })
}

pub(crate) fn current_task_for_sim(shared: &Rc<crate::sim::Shared>) -> Option<TaskId> {
    CURRENT.with(|current| {
        let current = current.borrow();
        let entry = current.as_ref()?;
        let ExecutorRef::Sim(candidate) = &entry.executor else {
            return None;
        };
        candidate
            .upgrade()
            .is_some_and(|candidate| Rc::ptr_eq(&candidate, shared))
            .then_some(entry.task)
    })
}

pub(crate) fn current_task_for_host(shared: &Rc<crate::host::Shared>) -> Option<TaskId> {
    CURRENT.with(|current| {
        let current = current.borrow();
        let entry = current.as_ref()?;
        let ExecutorRef::Host(candidate) = &entry.executor else {
            return None;
        };
        candidate
            .upgrade()
            .is_some_and(|candidate| Rc::ptr_eq(&candidate, shared))
            .then_some(entry.task)
    })
}

impl Future for YieldNow {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        if self.yielded {
            Poll::Ready(())
        } else {
            self.yielded = true;
            context.waker().wake_by_ref();
            Poll::Pending
        }
    }
}

pub(crate) struct TaskHarness<F: Future> {
    pub(crate) future: Pin<Box<F>>,
    pub(crate) join: Rc<JoinState<F::Output>>,
}

impl<F: Future> Future for TaskHarness<F> {
    type Output = Result<(), PanicRecord>;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        let poll = this.future.as_mut().poll(context);
        if let Poll::Ready(output) = poll {
            Poll::Ready(this.join.finish(Ok(output)))
        } else {
            Poll::Pending
        }
    }
}

/// Shared result/waiter/consumed storage behind every join handle.
///
/// The core is storage-agnostic: [`JoinState`] guards it with an owner-thread
/// `RefCell` and the host executor's send join guards it with a `Mutex`. Core
/// methods only mutate state; any waker to wake, stale waker to drop, or
/// duplicate result to discard is returned so each wrapper runs that foreign
/// code under its own cell or lock discipline.
pub(crate) struct JoinCore<T> {
    result: Option<Result<T, JoinError>>,
    waiter: Option<Waker>,
    consumed: bool,
}

impl<T> JoinCore<T> {
    pub(crate) const fn new() -> Self {
        Self {
            result: None,
            waiter: None,
            consumed: false,
        }
    }

    /// Records the terminal result once.
    ///
    /// Returns the waiter to wake and any duplicate result to discard; the
    /// caller handles both after releasing its guard.
    pub(crate) fn finish(
        &mut self,
        result: Result<T, JoinError>,
    ) -> (Option<Waker>, Option<Result<T, JoinError>>) {
        if self.result.is_some() || self.consumed {
            (None, Some(result))
        } else {
            self.result = Some(result);
            (self.waiter.take(), None)
        }
    }

    /// Advances the consumed/`will_wake` poll state machine.
    ///
    /// `clone_waker` runs at most once and only when the candidate is
    /// actually stored, so each wrapper decides whether the foreign clone
    /// happens inside or outside its guard. Any replaced stale waker is
    /// returned for the caller to drop under its own discipline.
    ///
    /// # Panics
    ///
    /// Panics with `poll_after_completion` when polled again after the
    /// result was consumed.
    pub(crate) fn poll(
        &mut self,
        waker: &Waker,
        clone_waker: impl FnOnce() -> Waker,
        poll_after_completion: &'static str,
    ) -> (Poll<Result<T, JoinError>>, Option<Waker>) {
        assert!(!self.consumed, "{poll_after_completion}");
        if let Some(result) = self.result.take() {
            self.consumed = true;
            (Poll::Ready(result), None)
        } else if self
            .waiter
            .as_ref()
            .is_some_and(|waiter| waiter.will_wake(waker))
        {
            (Poll::Pending, None)
        } else {
            (Poll::Pending, self.waiter.replace(clone_waker()))
        }
    }

    pub(crate) fn try_take(&mut self) -> Option<Result<T, JoinError>> {
        if self.consumed {
            return None;
        }
        let result = self.result.take()?;
        self.consumed = true;
        Some(result)
    }

    pub(crate) fn is_finished(&self) -> bool {
        self.result.is_some() || self.consumed
    }
}

pub(crate) struct JoinState<T> {
    inner: RefCell<JoinCore<T>>,
}

impl<T> JoinState<T> {
    pub(crate) fn new() -> Self {
        Self {
            inner: RefCell::new(JoinCore::new()),
        }
    }

    pub(crate) fn finish(&self, result: Result<T, JoinError>) -> Result<(), PanicRecord> {
        let (waiter, discarded) = self.inner.borrow_mut().finish(result);
        drop(discarded);
        if let Some(waiter) = waiter {
            catch_unwind(AssertUnwindSafe(|| waiter.wake())).map_err(panic_record_from_payload)?;
        }
        Ok(())
    }

    fn poll(&self, context: &mut Context<'_>) -> Poll<Result<T, JoinError>> {
        let (poll, stale) = self.inner.borrow_mut().poll(
            context.waker(),
            || context.waker().clone(),
            "JoinHandle polled after completion",
        );
        drop(stale);
        poll
    }

    pub(crate) fn try_take(&self) -> Option<Result<T, JoinError>> {
        self.inner.borrow_mut().try_take()
    }

    fn is_finished(&self) -> bool {
        self.inner.borrow().is_finished()
    }
}

pub(crate) trait ErasedJoinState {
    fn finish(&self, result: Result<(), JoinError>) -> Result<(), PanicRecord>;
}

impl<T> ErasedJoinState for JoinState<T> {
    fn finish(&self, result: Result<(), JoinError>) -> Result<(), PanicRecord> {
        if let Err(error) = result {
            self.finish(Err(error))
        } else {
            Ok(())
        }
    }
}

pub(crate) enum TaskJoin {
    Local(Rc<dyn ErasedJoinState>),
    Send(Arc<dyn ErasedJoinState + Send + Sync>),
}

impl TaskJoin {
    pub(crate) fn finish(&self, result: Result<(), JoinError>) -> Result<(), PanicRecord> {
        match self {
            Self::Local(join) => join.finish(result),
            Self::Send(join) => join.finish(result),
        }
    }
}

pub(crate) struct Task<S> {
    pub(crate) future: Option<BoxTaskFuture>,
    pub(crate) scoped_root: bool,
    pub(crate) join: TaskJoin,
    pub(crate) state: TaskState,
    pub(crate) signal: S,
}

struct TaskSlot<S> {
    generation: u32,
    ever_used: bool,
    task: Option<Task<S>>,
}

pub(crate) enum ReadyTask<S> {
    Owned {
        id: TaskId,
        future: BoxTaskFuture,
        signal: S,
    },
    ScopedRoot {
        id: TaskId,
        signal: S,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum TaskSlabError {
    SequenceExhausted,
    RuntimeStopped,
}

pub(crate) struct TaskSlab<S> {
    slots: Vec<TaskSlot<S>>,
    free_slots: Vec<u32>,
    live_tasks: usize,
    next_enqueue_sequence: u64,
    ready: std::collections::VecDeque<TaskId>,
}

impl<S> TaskSlab<S> {
    pub(crate) fn new() -> Self {
        Self {
            slots: Vec::new(),
            free_slots: Vec::new(),
            live_tasks: 0,
            next_enqueue_sequence: 0,
            ready: std::collections::VecDeque::new(),
        }
    }

    pub(crate) fn can_insert_task(
        &self,
        stopped: bool,
        max_tasks: usize,
    ) -> Result<(), SpawnError> {
        if stopped {
            return Err(SpawnError::RuntimeStopped);
        }
        if self.live_tasks >= max_tasks {
            return Err(SpawnError::ResourceExhausted {
                resource: "live tasks",
                limit: max_tasks,
            });
        }
        let reusable = self
            .free_slots
            .iter()
            .any(|slot_index| self.slots[*slot_index as usize].generation < u32::MAX);
        if !reusable && u32::try_from(self.slots.len()).is_err() {
            return Err(SpawnError::IdentifierExhausted);
        }
        Ok(())
    }

    pub(crate) fn insert_task_with(
        &mut self,
        stopped: bool,
        max_tasks: usize,
        make_task: impl FnOnce(TaskId) -> Task<S>,
    ) -> Result<TaskId, SpawnError> {
        self.can_insert_task(stopped, max_tasks)?;

        let reusable_slot = loop {
            let Some(slot_index) = self.free_slots.pop() else {
                break None;
            };
            if self.slots[slot_index as usize].generation < u32::MAX {
                break Some(slot_index);
            }
            // A generation-exhausted slot is retired permanently so a stale
            // waker can never alias a future task.
        };
        let id = if let Some(slot_index) = reusable_slot {
            let generation = self.slots[slot_index as usize]
                .generation
                .checked_add(1)
                .expect("generation-exhausted slots were retired");
            let id = TaskId::from_parts(slot_index, generation);
            let task = make_task(id);
            let slot = &mut self.slots[slot_index as usize];
            debug_assert!(slot.task.is_none());
            slot.generation = generation;
            slot.task = Some(task);
            id
        } else {
            let slot =
                u32::try_from(self.slots.len()).map_err(|_| SpawnError::IdentifierExhausted)?;
            let id = TaskId::from_parts(slot, 0);
            self.slots.push(TaskSlot {
                generation: 0,
                ever_used: true,
                task: Some(make_task(id)),
            });
            id
        };
        self.live_tasks += 1;
        Ok(id)
    }

    /// Installs an identifier reserved by the host's external pool, which owns
    /// generation allocation. Reservations released before insertion may leave
    /// gaps, but a used slot must advance strictly so stale IDs never alias.
    pub(crate) fn insert_reserved_task(
        &mut self,
        id: TaskId,
        task: Task<S>,
        stopped: bool,
    ) -> Result<(), (TaskSlabError, Task<S>)> {
        if stopped {
            return Err((TaskSlabError::RuntimeStopped, task));
        }
        let index = id.slot as usize;
        if index >= self.slots.len() {
            self.slots.resize_with(index + 1, || TaskSlot {
                generation: 0,
                ever_used: false,
                task: None,
            });
        }
        let slot = &mut self.slots[index];
        if slot.task.is_some() || (slot.ever_used && id.generation <= slot.generation) {
            return Err((TaskSlabError::SequenceExhausted, task));
        }
        slot.generation = id.generation;
        slot.ever_used = true;
        slot.task = Some(task);
        self.live_tasks += 1;
        Ok(())
    }

    pub(crate) fn enqueue_task(&mut self, id: TaskId) -> Result<Option<u64>, TaskSlabError> {
        let sequence = self.next_enqueue_sequence;
        let Some(task) = self.task(id) else {
            return Ok(None);
        };
        if task.state == TaskState::Ready {
            return Ok(None);
        }
        debug_assert_ne!(
            task.state,
            TaskState::Running,
            "wakes are admitted only at scheduler boundaries, after a polled task leaves Running"
        );
        if task.state == TaskState::Running {
            // Preserve release behavior if the scheduler boundary invariant is
            // violated in a non-debug build.
            return Ok(None);
        }
        let next_sequence = sequence
            .checked_add(1)
            .ok_or(TaskSlabError::SequenceExhausted)?;
        let task = self.task_mut(id).expect("task was just observed");
        task.state = TaskState::Ready;
        self.next_enqueue_sequence = next_sequence;
        self.ready.push_back(id);
        Ok(Some(sequence))
    }

    pub(crate) fn pop_ready_task(&mut self) -> Option<TaskId> {
        loop {
            let id = self.ready.pop_front()?;
            let Some(task) = self.task(id) else {
                continue;
            };
            if task.state == TaskState::Ready {
                return Some(id);
            }
        }
    }

    pub(crate) fn start_ready_task(
        &mut self,
        id: TaskId,
        clear_pending: impl FnOnce(&S),
    ) -> Result<ReadyTask<S>, TaskSlabError>
    where
        S: Clone,
    {
        let task = self.task_mut(id).expect("ready task was just observed");
        debug_assert_eq!(task.state, TaskState::Ready);
        task.state = TaskState::Running;
        let future = task.future.take();
        let scoped_root = task.scoped_root;
        let signal = task.signal.clone();
        clear_pending(&signal);
        match (future, scoped_root) {
            (Some(future), false) => Ok(ReadyTask::Owned { id, future, signal }),
            (None, true) => Ok(ReadyTask::ScopedRoot { id, signal }),
            _ => Err(TaskSlabError::RuntimeStopped),
        }
    }

    pub(crate) fn task(&self, id: TaskId) -> Option<&Task<S>> {
        let slot = self.slots.get(id.slot as usize)?;
        (slot.generation == id.generation)
            .then_some(slot.task.as_ref())
            .flatten()
    }

    pub(crate) fn task_mut(&mut self, id: TaskId) -> Option<&mut Task<S>> {
        let slot = self.slots.get_mut(id.slot as usize)?;
        (slot.generation == id.generation)
            .then_some(slot.task.as_mut())
            .flatten()
    }

    pub(crate) fn remove_task(&mut self, id: TaskId) -> Option<Task<S>> {
        self.remove_task_inner(id, true)
    }

    pub(crate) fn remove_reserved_task(&mut self, id: TaskId) -> Option<Task<S>> {
        self.remove_task_inner(id, false)
    }

    fn remove_task_inner(&mut self, id: TaskId, recycle: bool) -> Option<Task<S>> {
        let task = self.task(id)?;
        if task.state == TaskState::Ready {
            self.ready.retain(|queued| *queued != id);
        }
        let slot = &mut self.slots[id.slot as usize];
        let task = slot.task.take()?;
        if recycle {
            self.free_slots.push(id.slot);
        }
        self.live_tasks -= 1;
        Some(task)
    }

    pub(crate) fn take_all_tasks(&mut self) -> Vec<(TaskId, Task<S>)> {
        self.ready.clear();
        let mut tasks = Vec::with_capacity(self.live_tasks);
        for (index, slot) in self.slots.iter_mut().enumerate() {
            if let Some(task) = slot.task.take() {
                tasks.push((
                    TaskId::from_parts(
                        u32::try_from(index).expect("task slot already fit in u32"),
                        slot.generation,
                    ),
                    task,
                ));
            }
        }
        self.live_tasks = 0;
        self.free_slots.clear();
        tasks
    }

    pub(crate) fn iter(&self) -> impl Iterator<Item = (TaskId, &Task<S>)> {
        self.slots.iter().enumerate().filter_map(|(index, slot)| {
            slot.task.as_ref().map(|task| {
                (
                    TaskId::from_parts(
                        u32::try_from(index).expect("task slot already fit in u32"),
                        slot.generation,
                    ),
                    task,
                )
            })
        })
    }

    pub(crate) const fn live_len(&self) -> usize {
        self.live_tasks
    }

    pub(crate) fn ready_len(&self) -> usize {
        self.ready.len()
    }

    pub(crate) const fn next_enqueue_sequence(&self) -> u64 {
        self.next_enqueue_sequence
    }

    pub(crate) fn ready_is_empty(&self) -> bool {
        self.ready.is_empty()
    }

    #[cfg(test)]
    pub(crate) fn set_next_enqueue_sequence_for_test(&mut self, sequence: u64) {
        self.next_enqueue_sequence = sequence;
    }

    #[cfg(test)]
    pub(crate) fn set_vacant_slot_generation_for_test(&mut self, slot: u32, generation: u32) {
        let slot = &mut self.slots[slot as usize];
        assert!(slot.task.is_none());
        slot.generation = generation;
    }

    #[cfg(test)]
    pub(crate) fn slot_generation_for_test(&self, slot: u32) -> u32 {
        self.slots[slot as usize].generation
    }

    #[cfg(test)]
    pub(crate) fn slot_is_vacant_for_test(&self, slot: u32) -> bool {
        self.slots[slot as usize].task.is_none()
    }

    #[cfg(test)]
    pub(crate) fn free_slots_for_test(&self) -> &[u32] {
        &self.free_slots
    }
}

pub(crate) fn drop_value_caught<T>(value: T) {
    if let Err(payload) = catch_unwind(AssertUnwindSafe(|| drop(value))) {
        crate::panic::discard_panic_payload(payload);
    }
}

pub(crate) fn drop_value_result<T>(value: T) -> Result<(), PanicRecord> {
    catch_unwind(AssertUnwindSafe(|| drop(value))).map_err(panic_record_from_payload)
}

#[cfg(test)]
mod tests {
    use super::{JoinState, Task, TaskId, TaskJoin, TaskSlab, TaskSlabError, TaskState};
    use std::rc::Rc;

    fn waiting_task(signal: u32) -> Task<u32> {
        Task {
            future: Some(Box::pin(std::future::pending())),
            scoped_root: false,
            join: TaskJoin::Local(Rc::new(JoinState::<()>::new())),
            state: TaskState::Waiting,
            signal,
        }
    }

    #[test]
    fn reserved_ids_can_skip_generations_before_the_first_insertion() {
        let mut tasks = TaskSlab::new();
        // An earlier reservation for each slot was released without insertion;
        // later reservations may also reach the owner in a different slot order.
        for id in [TaskId::from_parts(1, 3), TaskId::from_parts(0, 7)] {
            assert!(
                tasks
                    .insert_reserved_task(id, waiting_task(0), false)
                    .is_ok()
            );
            assert!(tasks.task(id).is_some());
            assert!(tasks.task(TaskId::from_parts(id.slot(), 0)).is_none());
        }
        assert_eq!(tasks.live_len(), 2);
    }

    #[test]
    fn skipped_reserved_generations_cannot_revive_stale_task_ids() {
        let mut tasks = TaskSlab::new();
        let mut prior_ids = Vec::new();
        // Gaps represent reservations released without ever entering the slab.
        for generation in [0, 1, 4, 17, u32::MAX] {
            let id = TaskId::from_parts(0, generation);
            assert!(
                tasks
                    .insert_reserved_task(id, waiting_task(generation), false)
                    .is_ok()
            );
            for stale in &prior_ids {
                assert!(tasks.task(*stale).is_none());
                assert!(tasks.task_mut(*stale).is_none());
                assert_eq!(tasks.enqueue_task(*stale), Ok(None));
                assert!(tasks.remove_reserved_task(*stale).is_none());
            }
            assert_eq!(tasks.live_len(), 1);
            assert_eq!(tasks.enqueue_task(id), Ok(Some(prior_ids.len() as u64)));
            assert_eq!(tasks.ready_len(), 1);
            assert_eq!(
                tasks.remove_reserved_task(id).map(|task| task.signal),
                Some(generation)
            );
            assert_eq!(tasks.live_len(), 0);
            assert!(tasks.ready_is_empty());
            prior_ids.push(id);
        }
    }

    #[test]
    fn rejecting_an_occupied_reserved_slot_preserves_the_live_task() {
        let mut tasks = TaskSlab::new();
        let id = TaskId::from_parts(0, 3);
        assert!(
            tasks
                .insert_reserved_task(id, waiting_task(7), false)
                .is_ok()
        );
        assert_eq!(tasks.enqueue_task(id), Ok(Some(0)));

        for generation in [0, 3, 4, u32::MAX] {
            let Err((error, rejected)) = tasks.insert_reserved_task(
                TaskId::from_parts(0, generation),
                waiting_task(9),
                false,
            ) else {
                panic!("occupied slot accepted generation {generation}");
            };
            assert_eq!(error, TaskSlabError::SequenceExhausted);
            assert_eq!(rejected.signal, 9);
            assert_eq!(tasks.task(id).map(|task| task.signal), Some(7));
            assert_eq!(tasks.slot_generation_for_test(0), 3);
            assert_eq!(tasks.live_len(), 1);
            assert_eq!(tasks.ready_len(), 1);
            assert_eq!(tasks.next_enqueue_sequence(), 1);
        }
        assert_eq!(tasks.pop_ready_task(), Some(id));
    }

    #[test]
    fn stale_and_exhausted_reserved_generations_leave_vacant_slots_untouched() {
        let mut tasks = TaskSlab::new();
        for last_generation in [7, u32::MAX] {
            let id = TaskId::from_parts(0, last_generation);
            assert!(
                tasks
                    .insert_reserved_task(id, waiting_task(0), false)
                    .is_ok()
            );
            assert!(tasks.remove_reserved_task(id).is_some());

            for generation in [0, last_generation - 1, last_generation] {
                let Err((error, rejected)) = tasks.insert_reserved_task(
                    TaskId::from_parts(0, generation),
                    waiting_task(9),
                    false,
                ) else {
                    panic!("stale generation {generation} followed {last_generation}");
                };
                assert_eq!(error, TaskSlabError::SequenceExhausted);
                assert_eq!(rejected.signal, 9);
                assert_eq!(tasks.slot_generation_for_test(0), last_generation);
                assert!(tasks.slot_is_vacant_for_test(0));
                assert_eq!(tasks.live_len(), 0);
                assert!(tasks.ready_is_empty());
            }
        }
        // The external pool retires exhausted slots and supplies a fresh slot.
        let fresh = TaskId::from_parts(1, 0);
        assert!(
            tasks
                .insert_reserved_task(fresh, waiting_task(1), false)
                .is_ok()
        );
        assert!(tasks.task(fresh).is_some());
        assert_eq!(tasks.slot_generation_for_test(0), u32::MAX);
    }

    #[test]
    fn stopped_reserved_insertion_does_not_consume_the_identifier() {
        let mut tasks = TaskSlab::new();
        let id = TaskId::from_parts(0, 5);
        let Err((error, task)) = tasks.insert_reserved_task(id, waiting_task(9), true) else {
            panic!("stopped slab accepted a reservation");
        };
        assert_eq!(error, TaskSlabError::RuntimeStopped);
        assert_eq!(task.signal, 9);
        assert!(tasks.slots.is_empty());
        assert_eq!(tasks.live_len(), 0);
        assert!(tasks.insert_reserved_task(id, task, false).is_ok());
        assert_eq!(tasks.task(id).map(|task| task.signal), Some(9));
    }
}
