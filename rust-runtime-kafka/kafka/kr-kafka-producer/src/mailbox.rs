//! Preallocated shared ingress and its application-facing event-ring twin.
//!
//! A mailbox has one consumer waker, bounded FIFO data and control lanes, and
//! one independent emergency close slot. All operations are synchronous except
//! `poll_pop`, which registers at most one waker and creates no pending future
//! queue. The caller supplies the single logical consumer. Clones share state;
//! dropping a clone neither cancels accepted items nor closes other clones.
//!
//! Control and close items take priority over data. The owner must therefore
//! carry its own admission watermarks for flush/close semantics. An event ring
//! must publish its terminal `Closed` event only when that watermark permits it.
//! Queue admission alone does not reserve producer byte or completion credits.

use std::{
    collections::VecDeque,
    fmt,
    sync::{Arc, Mutex, MutexGuard},
    task::{Context, Poll, Waker},
    thread::{self, ThreadId},
};

use kr_runtime::contain_panic;

/// Simulation prohibits both foreign submissions and foreign consumption, so
/// no real host thread can inject a wake into an owner-local simulated runtime.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SubmissionPolicy {
    AnyThread,
    OwnerThreadOnly,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct MailboxConfig {
    pub data_capacity: usize,
    pub control_capacity: usize,
    pub submission_policy: SubmissionPolicy,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum MailboxError {
    InvalidCapacity,
    AllocationFailed,
    Full { capacity: usize },
    Closed,
    ForeignThread,
    WakerPanicked,
}

impl fmt::Display for MailboxError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidCapacity => {
                f.write_str("mailbox capacities must fit usize and data must be nonzero")
            }
            Self::AllocationFailed => f.write_str("could not preallocate mailbox capacity"),
            Self::Full { capacity } => write!(f, "mailbox lane is full (capacity {capacity})"),
            Self::Closed => f.write_str("mailbox admission is closed"),
            Self::ForeignThread => {
                f.write_str("simulation mailbox accessed outside its owner thread")
            }
            Self::WakerPanicked => f.write_str("mailbox consumer waker panicked while cloning"),
        }
    }
}
impl std::error::Error for MailboxError {}

/// Rejected input remains caller-owned, including after close or capacity failure.
#[derive(Debug, Eq, PartialEq)]
pub struct Rejected<T> {
    pub error: MailboxError,
    pub value: T,
}

#[derive(Debug, Eq, PartialEq)]
pub enum QueueItem<T> {
    Data(T),
    Control(T),
    Close(T),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct MailboxStatus {
    pub data_len: usize,
    pub control_len: usize,
    pub close_pending: bool,
    pub closed: bool,
    pub data_capacity: usize,
    pub control_capacity: usize,
}
impl MailboxStatus {
    #[must_use]
    pub const fn is_empty(self) -> bool {
        self.data_len == 0 && self.control_len == 0 && !self.close_pending
    }
}

pub struct BoundedMailbox<T> {
    inner: Arc<Inner<T>>,
}

/// The same check/register/recheck protocol in the opposite direction: the
/// actor publishes and the application is the single logical consumer.
pub type EventRing<T> = BoundedMailbox<T>;

/// The outer admission transaction releases its own mutex before invoking this
/// notification. Queue publication and waker extraction remain atomic.
#[must_use]
pub(crate) struct DeferredWake(Option<ContainedWaker>);
impl DeferredWake {
    pub(crate) fn wake(mut self) {
        wake(self.0.take());
    }
}

impl<T> Clone for BoundedMailbox<T> {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}
impl<T> fmt::Debug for BoundedMailbox<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BoundedMailbox")
            .field("status", &self.status())
            .finish_non_exhaustive()
    }
}

struct Inner<T> {
    config: MailboxConfig,
    owner: ThreadId,
    state: Mutex<State<T>>,
}
struct State<T> {
    data: VecDeque<T>,
    control: VecDeque<T>,
    close: Option<T>,
    closed: bool,
    waker: Option<ContainedWaker>,
}
impl<T> State<T> {
    fn pop(&mut self) -> Option<QueueItem<T>> {
        self.close
            .take()
            .map(QueueItem::Close)
            .or_else(|| self.control.pop_front().map(QueueItem::Control))
            .or_else(|| self.data.pop_front().map(QueueItem::Data))
    }
}

// Last-handle teardown has exclusive access, not a held mutex. Containing each
// destructor separately still releases the remaining queued ownership if one
// application-supplied destructor panics.
impl<T> Drop for Inner<T> {
    fn drop(&mut self) {
        let state = self.state.get_mut().unwrap_or_else(|p| p.into_inner());
        while let Some(item) = state.pop() {
            contain_panic(|| drop(item));
        }
        drop(state.waker.take());
    }
}

impl<T> BoundedMailbox<T> {
    /// Exact retained lane element storage after successful construction.
    /// Shared state, referenced owners and construction transients are separate.
    pub(crate) fn configured_storage_bytes(data: usize, control: usize) -> Option<usize> {
        data.checked_add(control)?.checked_mul(size_of::<T>())
    }

    #[cfg(test)]
    pub(crate) fn storage_capacity_bytes(&self) -> usize {
        let state = self.lock();
        state.data.capacity() * size_of::<T>() + state.control.capacity() * size_of::<T>()
    }

    /// Captures the current thread as the simulation owner and preallocates both
    /// lanes. Accepted pushes never grow their backing allocations.
    ///
    /// # Errors
    /// Returns invalid capacity or allocation failure before publishing a handle.
    /// An allocator-returned capacity above the requested limit is also rejected.
    pub fn new(config: MailboxConfig) -> Result<Self, MailboxError> {
        if config.data_capacity == 0
            || config
                .data_capacity
                .checked_add(config.control_capacity)
                .and_then(|n| n.checked_add(1))
                .is_none()
        {
            return Err(MailboxError::InvalidCapacity);
        }
        let data = crate::fixed::try_deque(config.data_capacity)
            .map_err(|_| MailboxError::AllocationFailed)?;
        let control = crate::fixed::try_deque(config.control_capacity)
            .map_err(|_| MailboxError::AllocationFailed)?;
        Ok(Self {
            inner: Arc::new(Inner {
                config,
                owner: thread::current().id(),
                state: Mutex::new(State {
                    data,
                    control,
                    close: None,
                    closed: false,
                    waker: None,
                }),
            }),
        })
    }

    fn check_thread(&self) -> Result<(), MailboxError> {
        if self.inner.config.submission_policy == SubmissionPolicy::OwnerThreadOnly
            && thread::current().id() != self.inner.owner
        {
            Err(MailboxError::ForeignThread)
        } else {
            Ok(())
        }
    }
    fn lock(&self) -> MutexGuard<'_, State<T>> {
        self.inner.state.lock().unwrap_or_else(|p| p.into_inner())
    }

    /// A point-in-time snapshot; it does not reserve any capacity. Read-only
    /// inspection is permitted from any thread, including simulation observers.
    #[must_use]
    pub fn status(&self) -> MailboxStatus {
        let state = self.lock();
        MailboxStatus {
            data_len: state.data.len(),
            control_len: state.control.len(),
            close_pending: state.close.is_some(),
            closed: state.closed,
            data_capacity: self.inner.config.data_capacity,
            control_capacity: self.inner.config.control_capacity,
        }
    }

    /// Wakes the registered owner after an out-of-band resource release without
    /// needing another bounded mailbox slot.
    pub(crate) fn notify(&self) -> Result<(), MailboxError> {
        self.check_thread()?;
        let waker = self.lock().waker.take();
        wake(waker);
        Ok(())
    }

    /// # Errors
    /// Returns the original item on full/closed/foreign-thread rejection.
    pub fn try_push(&self, value: T) -> Result<(), Rejected<T>> {
        self.push(value, false)
    }

    /// Uses the independently bounded control reserve.
    ///
    /// # Errors
    /// Returns the original item on full/closed/foreign-thread rejection.
    pub fn try_push_control(&self, value: T) -> Result<(), Rejected<T>> {
        self.push(value, true)
    }

    fn push(&self, value: T, control: bool) -> Result<(), Rejected<T>> {
        self.push_deferred(value, control)?.wake();
        Ok(())
    }

    pub(crate) fn push_deferred(
        &self,
        value: T,
        control: bool,
    ) -> Result<DeferredWake, Rejected<T>> {
        if let Err(error) = self.check_thread() {
            return Err(Rejected { error, value });
        }
        let waker = {
            let mut state = self.lock();
            if state.closed {
                return Err(Rejected {
                    error: MailboxError::Closed,
                    value,
                });
            }
            let capacity = if control {
                self.inner.config.control_capacity
            } else {
                self.inner.config.data_capacity
            };
            let lane = if control {
                &mut state.control
            } else {
                &mut state.data
            };
            if lane.len() == capacity {
                return Err(Rejected {
                    error: MailboxError::Full { capacity },
                    value,
                });
            }
            lane.push_back(value);
            state.waker.take()
        };
        Ok(DeferredWake(waker))
    }

    /// Moves a FIFO prefix in one critical section. The suffix stays in `input`.
    /// Full capacity returns zero; no input is removed on an error. The prefix
    /// is bounded by `maximum`, available data capacity, and the input length.
    ///
    /// # Errors
    /// Rejects closed admission or a foreign simulation thread.
    pub fn push_prefix(
        &self,
        input: &mut VecDeque<T>,
        maximum: usize,
    ) -> Result<usize, MailboxError> {
        self.check_thread()?;
        let (count, waker) = {
            let mut state = self.lock();
            if state.closed {
                return Err(MailboxError::Closed);
            }
            let count = input
                .len()
                .min(maximum)
                .min(self.inner.config.data_capacity - state.data.len());
            for _ in 0..count {
                if let Some(value) = input.pop_front() {
                    state.data.push_back(value);
                }
            }
            (count, if count == 0 { None } else { state.waker.take() })
        };
        wake(waker);
        Ok(count)
    }

    /// Closes ordinary admission and publishes one priority close item using an
    /// emergency slot that is independent of both lane capacities. Existing
    /// queued items remain owned until consumed or final-handle destruction.
    ///
    /// # Errors
    /// Repeated close and foreign-thread calls return the original item.
    pub fn request_close(&self, value: T) -> Result<(), Rejected<T>> {
        self.close_deferred(value)?.wake();
        Ok(())
    }

    pub(crate) fn close_deferred(&self, value: T) -> Result<DeferredWake, Rejected<T>> {
        if let Err(error) = self.check_thread() {
            return Err(Rejected { error, value });
        }
        let waker = {
            let mut state = self.lock();
            if state.closed {
                return Err(Rejected {
                    error: MailboxError::Closed,
                    value,
                });
            }
            state.closed = true;
            state.close = Some(value);
            state.waker.take()
        };
        Ok(DeferredWake(waker))
    }

    /// Idempotently closes admission without an item. Returns whether this call
    /// changed the state. The consumer drains accepted items before end-of-stream.
    ///
    /// # Errors
    /// Rejects a foreign simulation thread without closing or waking.
    pub fn close(&self) -> Result<bool, MailboxError> {
        self.check_thread()?;
        let (changed, waker) = {
            let mut state = self.lock();
            let changed = !state.closed;
            state.closed = true;
            (changed, state.waker.take())
        };
        wake(waker);
        Ok(changed)
    }

    /// Pops emergency close, then control, then data, FIFO within each lane.
    /// An empty result may represent either open or closed admission; use
    /// `poll_pop` when the consumer needs a wake or terminal end-of-stream.
    ///
    /// # Errors
    /// Rejects a foreign simulation thread without removing an item.
    pub fn try_pop(&self) -> Result<Option<QueueItem<T>>, MailboxError> {
        self.check_thread()?;
        Ok(self.lock().pop())
    }

    /// Checks, clones the caller waker outside the lock, then atomically
    /// registers/rechecks under the publisher's lock. Only an empty open queue
    /// returns `Pending`. Later calls replace the sole consumer registration.
    ///
    /// # Errors
    /// Rejects a foreign simulation thread or a panicking waker clone. Clone,
    /// wake, and destruction callbacks never run while holding the queue lock.
    pub fn poll_pop(
        &self,
        cx: &mut Context<'_>,
    ) -> Poll<Result<Option<QueueItem<T>>, MailboxError>> {
        self.poll_pop_after_check(cx, || {})
    }

    fn poll_pop_after_check(
        &self,
        cx: &mut Context<'_>,
        after_check: impl FnOnce(),
    ) -> Poll<Result<Option<QueueItem<T>>, MailboxError>> {
        if let Err(error) = self.check_thread() {
            return Poll::Ready(Err(error));
        }
        {
            let mut state = self.lock();
            if let Some(value) = state.pop() {
                return Poll::Ready(Ok(Some(value)));
            }
            if state.closed {
                return Poll::Ready(Ok(None));
            }
        }
        after_check();
        let Some(waker) = ContainedWaker::clone_from(cx.waker()) else {
            return Poll::Ready(Err(MailboxError::WakerPanicked));
        };
        let (result, previous) = {
            let mut state = self.lock();
            if let Some(value) = state.pop() {
                (Poll::Ready(Ok(Some(value))), Some(waker))
            } else if state.closed {
                (Poll::Ready(Ok(None)), Some(waker))
            } else {
                (Poll::Pending, state.waker.replace(waker))
            }
        };
        drop(previous);
        result
    }
}

struct ContainedWaker(Option<Waker>);
impl ContainedWaker {
    fn clone_from(waker: &Waker) -> Option<Self> {
        let mut cloned = None;
        contain_panic(|| cloned = Some(Self(Some(waker.clone()))));
        cloned
    }
    fn wake(mut self) {
        if let Some(waker) = self.0.take() {
            contain_panic(|| waker.wake());
        }
    }
}
impl Drop for ContainedWaker {
    fn drop(&mut self) {
        if let Some(waker) = self.0.take() {
            contain_panic(|| drop(waker));
        }
    }
}
fn wake(waker: Option<ContainedWaker>) {
    if let Some(waker) = waker {
        waker.wake();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn zero_sized_lanes_preserve_logical_limits_without_element_backing() {
        let mailbox = queue::<()>(3, 0);
        assert_eq!(mailbox.storage_capacity_bytes(), 0);
        assert_eq!(
            BoundedMailbox::<()>::configured_storage_bytes(3, 0),
            Some(0)
        );
        for _ in 0..100 {
            for _ in 0..3 {
                mailbox.try_push(()).unwrap();
            }
            assert_eq!(
                mailbox.try_push(()).unwrap_err().error,
                MailboxError::Full { capacity: 3 }
            );
            assert_eq!(
                mailbox.try_push_control(()).unwrap_err().error,
                MailboxError::Full { capacity: 0 }
            );
            for _ in 0..3 {
                assert_eq!(mailbox.try_pop().unwrap(), Some(QueueItem::Data(())));
            }
            assert_eq!(mailbox.try_pop().unwrap(), None);
        }
    }

    #[test]
    fn fixed_lane_storage_survives_wraparound_and_exhaustion_without_growth() {
        assert!(BoundedMailbox::<u64>::configured_storage_bytes(usize::MAX, 1).is_none());
        assert!(BoundedMailbox::<u64>::configured_storage_bytes(usize::MAX, 0).is_none());
        let mailbox = queue::<u64>(7, 3);
        let storage = mailbox.storage_capacity_bytes();
        assert_eq!(
            storage,
            BoundedMailbox::<u64>::configured_storage_bytes(7, 3).unwrap()
        );
        for _ in 0..100 {
            for value in 0..7 {
                mailbox.try_push(value).unwrap();
            }
            for value in 0..3 {
                mailbox.try_push_control(value).unwrap();
            }
            assert!(mailbox.try_push(7).is_err());
            assert!(mailbox.try_push_control(3).is_err());
            for _ in 0..10 {
                assert!(mailbox.try_pop().unwrap().is_some());
            }
            assert_eq!(mailbox.storage_capacity_bytes(), storage);
        }
    }
    use std::sync::{
        Weak,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    };
    use std::task::Wake;

    fn queue<T>(data_capacity: usize, control_capacity: usize) -> BoundedMailbox<T> {
        BoundedMailbox::new(MailboxConfig {
            data_capacity,
            control_capacity,
            submission_policy: SubmissionPolicy::AnyThread,
        })
        .unwrap()
    }

    #[derive(Default)]
    struct CountWake(AtomicUsize);
    impl Wake for CountWake {
        fn wake(self: Arc<Self>) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
        fn wake_by_ref(self: &Arc<Self>) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }

    #[test]
    fn bulk_prefix_preserves_suffix_and_lane_order_without_growing_storage() {
        let queue = queue(3, 2);
        let capacities = {
            let state = queue.lock();
            (state.data.capacity(), state.control.capacity())
        };
        let mut input = VecDeque::from([0, 1, 2, 3, 4]);
        assert_eq!(queue.push_prefix(&mut input, 2), Ok(2));
        assert_eq!(input, [2, 3, 4]);
        assert_eq!(queue.push_prefix(&mut input, usize::MAX), Ok(1));
        assert_eq!(input, [3, 4]);
        assert_eq!(queue.push_prefix(&mut input, usize::MAX), Ok(0));
        queue.try_push_control(10).unwrap();
        queue.try_push_control(11).unwrap();
        assert_eq!(
            queue.try_push_control(12),
            Err(Rejected {
                error: MailboxError::Full { capacity: 2 },
                value: 12,
            })
        );
        for expected in [
            QueueItem::Control(10),
            QueueItem::Control(11),
            QueueItem::Data(0),
            QueueItem::Data(1),
            QueueItem::Data(2),
        ] {
            assert_eq!(queue.try_pop(), Ok(Some(expected)));
        }
        for value in 0..1000 {
            queue.try_push(value).unwrap();
            assert_eq!(queue.try_pop(), Ok(Some(QueueItem::Data(value))));
        }
        let state = queue.lock();
        assert_eq!(
            capacities,
            (state.data.capacity(), state.control.capacity())
        );
    }

    #[test]
    fn emergency_close_survives_full_data_and_control_and_preserves_queued_ownership() {
        let queue = queue(1, 1);
        queue.try_push(1).unwrap();
        queue.try_push_control(2).unwrap();
        queue.request_close(3).unwrap();
        assert_eq!(
            queue.request_close(4),
            Err(Rejected {
                error: MailboxError::Closed,
                value: 4
            })
        );
        assert_eq!(
            queue.try_push(5),
            Err(Rejected {
                error: MailboxError::Closed,
                value: 5
            })
        );
        let mut suffix = VecDeque::from([6, 7]);
        assert_eq!(queue.push_prefix(&mut suffix, 2), Err(MailboxError::Closed));
        assert_eq!(suffix, [6, 7]);
        assert_eq!(queue.close(), Ok(false));
        let waker = Waker::noop();
        let mut cx = Context::from_waker(waker);
        for expected in [
            QueueItem::Close(3),
            QueueItem::Control(2),
            QueueItem::Data(1),
        ] {
            assert_eq!(queue.poll_pop(&mut cx), Poll::Ready(Ok(Some(expected))));
        }
        assert_eq!(queue.poll_pop(&mut cx), Poll::Ready(Ok(None)));
        assert!(queue.status().is_empty());
    }

    #[test]
    fn check_register_recheck_covers_each_publication_and_close_boundary() {
        for boundary in 0..3 {
            let queue = queue(1, 0);
            let count = Arc::new(CountWake::default());
            let waker = Waker::from(Arc::clone(&count));
            let mut cx = Context::from_waker(&waker);
            match boundary {
                0 => {
                    queue.try_push(7).unwrap();
                    assert_eq!(
                        queue.poll_pop(&mut cx),
                        Poll::Ready(Ok(Some(QueueItem::Data(7))))
                    );
                }
                1 => assert_eq!(
                    queue.poll_pop_after_check(&mut cx, || queue.try_push(7).unwrap()),
                    Poll::Ready(Ok(Some(QueueItem::Data(7))))
                ),
                _ => {
                    assert_eq!(queue.poll_pop(&mut cx), Poll::Pending);
                    queue.try_push(7).unwrap();
                    assert_eq!(count.0.load(Ordering::SeqCst), 1);
                    assert_eq!(
                        queue.poll_pop(&mut cx),
                        Poll::Ready(Ok(Some(QueueItem::Data(7))))
                    );
                }
            }
            assert_eq!(
                queue.poll_pop_after_check(&mut cx, || {
                    queue.close().unwrap();
                }),
                Poll::Ready(Ok(None))
            );
        }
        let queue = queue::<()>(1, 0);
        let count = Arc::new(CountWake::default());
        let waker = Waker::from(Arc::clone(&count));
        let mut cx = Context::from_waker(&waker);
        assert_eq!(queue.poll_pop(&mut cx), Poll::Pending);
        assert_eq!(queue.close(), Ok(true));
        assert_eq!(count.0.load(Ordering::SeqCst), 1);
        assert_eq!(queue.poll_pop(&mut cx), Poll::Ready(Ok(None)));
    }

    struct ReentrantWake {
        queue: Weak<Inner<u8>>,
        observed_unlocked: Arc<AtomicBool>,
        check_on_drop: bool,
    }
    impl ReentrantWake {
        fn check(&self) {
            if let Some(inner) = self.queue.upgrade() {
                self.observed_unlocked
                    .store(inner.state.try_lock().is_ok(), Ordering::SeqCst);
            }
        }
    }
    impl Wake for ReentrantWake {
        fn wake(self: Arc<Self>) {
            self.check();
        }
    }
    impl Drop for ReentrantWake {
        fn drop(&mut self) {
            if self.check_on_drop {
                self.check();
            }
        }
    }

    #[test]
    fn foreign_host_publication_wakes_outside_lock_and_replaces_only_one_registration() {
        let queue = queue(2, 0);
        let first = Arc::new(CountWake::default());
        let first_waker = Waker::from(Arc::clone(&first));
        assert_eq!(
            queue.poll_pop(&mut Context::from_waker(&first_waker)),
            Poll::Pending
        );
        let observed = Arc::new(AtomicBool::new(false));
        let second_waker = Waker::from(Arc::new(ReentrantWake {
            queue: Arc::downgrade(&queue.inner),
            observed_unlocked: Arc::clone(&observed),
            check_on_drop: false,
        }));
        assert_eq!(
            queue.poll_pop(&mut Context::from_waker(&second_waker)),
            Poll::Pending
        );
        let submitter = queue.clone();
        thread::spawn(move || submitter.try_push(9).unwrap())
            .join()
            .unwrap();
        assert!(observed.load(Ordering::SeqCst));
        assert_eq!(first.0.load(Ordering::SeqCst), 0);
        assert_eq!(queue.try_pop(), Ok(Some(QueueItem::Data(9))));
    }

    #[test]
    fn replaced_waker_destructor_runs_outside_lock_and_wake_panics_are_contained() {
        let queue = queue(1, 0);
        let observed = Arc::new(AtomicBool::new(false));
        let waker = Waker::from(Arc::new(ReentrantWake {
            queue: Arc::downgrade(&queue.inner),
            observed_unlocked: Arc::clone(&observed),
            check_on_drop: true,
        }));
        assert_eq!(
            queue.poll_pop(&mut Context::from_waker(&waker)),
            Poll::Pending
        );
        drop(waker);
        assert_eq!(
            queue.poll_pop(&mut Context::from_waker(Waker::noop())),
            Poll::Pending
        );
        assert!(observed.load(Ordering::SeqCst));

        struct PanicWake;
        impl Wake for PanicWake {
            fn wake(self: Arc<Self>) {
                panic!("application notification failed");
            }
        }
        let waker = Waker::from(Arc::new(PanicWake));
        assert_eq!(
            queue.poll_pop(&mut Context::from_waker(&waker)),
            Poll::Pending
        );
        queue.try_push(1).unwrap();
        assert_eq!(queue.try_pop(), Ok(Some(QueueItem::Data(1))));
    }

    #[test]
    fn simulation_rejects_foreign_operations_without_state_change_or_real_wake() {
        let queue = BoundedMailbox::new(MailboxConfig {
            data_capacity: 2,
            control_capacity: 1,
            submission_policy: SubmissionPolicy::OwnerThreadOnly,
        })
        .unwrap();
        let count = Arc::new(CountWake::default());
        let waker = Waker::from(Arc::clone(&count));
        assert_eq!(
            queue.poll_pop(&mut Context::from_waker(&waker)),
            Poll::Pending
        );
        let foreign = queue.clone();
        thread::spawn(move || {
            for result in [
                foreign.try_push(1),
                foreign.try_push_control(2),
                foreign.request_close(3),
            ] {
                assert_eq!(result.unwrap_err().error, MailboxError::ForeignThread);
            }
            let mut input = VecDeque::from([4]);
            assert_eq!(
                foreign.push_prefix(&mut input, 1),
                Err(MailboxError::ForeignThread)
            );
            assert_eq!(input, [4]);
            assert_eq!(foreign.close(), Err(MailboxError::ForeignThread));
            assert_eq!(foreign.try_pop(), Err(MailboxError::ForeignThread));
            assert_eq!(
                foreign.poll_pop(&mut Context::from_waker(Waker::noop())),
                Poll::Ready(Err(MailboxError::ForeignThread))
            );
        })
        .join()
        .unwrap();
        assert!(queue.status().is_empty());
        assert!(!queue.status().closed);
        assert_eq!(count.0.load(Ordering::SeqCst), 0);
        queue.try_push(5).unwrap();
        assert_eq!(count.0.load(Ordering::SeqCst), 1);
    }

    #[derive(Debug)]
    struct Owned {
        dropped: Arc<AtomicUsize>,
        panic_on_drop: bool,
    }
    impl Drop for Owned {
        fn drop(&mut self) {
            self.dropped.fetch_add(1, Ordering::SeqCst);
            assert!(!self.panic_on_drop, "payload cleanup panic");
        }
    }

    #[test]
    fn stopped_application_is_bounded_and_last_handle_releases_every_queued_owner() {
        let events: EventRing<Owned> = queue(2, 1);
        let dropped = Arc::new(AtomicUsize::new(0));
        let owned = |panic_on_drop| Owned {
            dropped: Arc::clone(&dropped),
            panic_on_drop,
        };
        events.try_push(owned(false)).unwrap();
        events.try_push(owned(true)).unwrap();
        events.try_push_control(owned(false)).unwrap();
        for _ in 0..100 {
            let rejected = events.try_push(owned(false)).unwrap_err();
            assert_eq!(rejected.error, MailboxError::Full { capacity: 2 });
            drop(rejected.value);
        }
        events.request_close(owned(false)).unwrap();
        let clone = events.clone();
        drop(events);
        assert_eq!(dropped.load(Ordering::SeqCst), 100);
        assert_eq!(clone.status().data_len, 2);
        assert_eq!(clone.status().control_len, 1);
        assert!(clone.status().close_pending);
        drop(clone);
        assert_eq!(dropped.load(Ordering::SeqCst), 104);
    }

    #[test]
    fn bounded_seeded_queue_model_preserves_admission_and_ownership_order() {
        for seed in 1_u64..129 {
            let queue = queue(5, 2);
            let mut model: Vec<(u8, u64)> = Vec::new();
            let mut rng = seed;
            for step in 0..256_u64 {
                rng ^= rng << 13;
                rng ^= rng >> 7;
                rng ^= rng << 17;
                match rng % 4 {
                    0 | 1 => {
                        let lane = u8::from(rng % 4 == 1);
                        let capacity = if lane == 0 { 5 } else { 2 };
                        let full =
                            model.iter().filter(|(class, _)| *class == lane).count() == capacity;
                        let actual = if lane == 0 {
                            queue.try_push(step)
                        } else {
                            queue.try_push_control(step)
                        };
                        assert_eq!(actual.is_err(), full, "seed={seed} step={step}");
                        if !full {
                            model.push((lane, step));
                        }
                    }
                    2 => {
                        let mut input = VecDeque::from([step, step + 1000, step + 2000]);
                        let count =
                            (5 - model.iter().filter(|(class, _)| *class == 0).count()).min(3);
                        assert_eq!(
                            queue.push_prefix(&mut input, 3),
                            Ok(count),
                            "seed={seed} step={step}"
                        );
                        for value in [step, step + 1000, step + 2000].into_iter().take(count) {
                            model.push((0, value));
                        }
                        assert_eq!(input.len(), 3 - count);
                    }
                    _ => {
                        let index = model
                            .iter()
                            .position(|(class, _)| *class == 1)
                            .or(if model.is_empty() { None } else { Some(0) });
                        let expected = index.map(|index| {
                            let (class, value) = model.remove(index);
                            if class == 1 {
                                QueueItem::Control(value)
                            } else {
                                QueueItem::Data(value)
                            }
                        });
                        assert_eq!(queue.try_pop(), Ok(expected), "seed={seed} step={step}");
                    }
                }
                let status = queue.status();
                assert_eq!(
                    status.data_len + status.control_len,
                    model.len(),
                    "seed={seed} step={step}"
                );
            }
        }
    }

    #[test]
    fn impossible_capacities_fail_before_allocation() {
        for (data_capacity, control_capacity) in [(0, 1), (usize::MAX, 1), (usize::MAX, 0)] {
            assert_eq!(
                BoundedMailbox::<u8>::new(MailboxConfig {
                    data_capacity,
                    control_capacity,
                    submission_policy: SubmissionPolicy::AnyThread,
                })
                .unwrap_err(),
                MailboxError::InvalidCapacity
            );
        }
        assert_eq!(
            BoundedMailbox::<u64>::new(MailboxConfig {
                data_capacity: usize::MAX / 2,
                control_capacity: 0,
                submission_policy: SubmissionPolicy::AnyThread,
            })
            .unwrap_err(),
            MailboxError::AllocationFailed
        );
    }
}
