//! Local, bounded asynchronous communication primitives.
//!
//! These primitives are deliberately single-threaded. They use [`Rc`] rather
//! than synchronization, making them a small fit for Quarry actors running on
//! the deterministic runtime. The mailbox has one consumer, any number of
//! cloned producers, FIFO delivery, and immediate backpressure.

use std::cell::RefCell;
use std::collections::VecDeque;
use std::error::Error;
use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::rc::{Rc, Weak};
use std::task::{Context, Poll, Waker};

use kr_runtime::contain_panic;

fn wake_safely(waker: Waker) {
    // Delivery is already committed before a wake. A caller-provided waker is
    // arbitrary code, so its panic must not unwind the producer or actor.
    contain_panic(|| waker.wake());
}

/// Creates a bounded, single-consumer mailbox.
///
/// A capacity of zero is valid, but every send fails with
/// [`TrySendError::Full`] while the receiver remains open.
pub(crate) fn channel<T>(capacity: usize) -> (Sender<T>, Receiver<T>) {
    let shared = Rc::new(RefCell::new(MailboxState {
        // `capacity` is a logical backpressure bound, not an allocation
        // request. Reserving it eagerly lets a caller-controlled bound panic
        // on capacity overflow or exhaust memory before the first message.
        queue: VecDeque::new(),
        capacity,
        sender_count: 1,
        receiver_open: true,
        receiver_waker: None,
    }));

    (
        Sender {
            shared: Rc::clone(&shared),
        },
        Receiver { shared },
    )
}

struct MailboxState<T> {
    queue: VecDeque<T>,
    capacity: usize,
    sender_count: usize,
    receiver_open: bool,
    receiver_waker: Option<Waker>,
}

/// The sending side of a bounded local mailbox.
///
/// Cloning a sender adds another producer. The receiver observes end-of-stream
/// only after every sender has been dropped and all queued values are drained.
pub(crate) struct Sender<T> {
    shared: Rc<RefCell<MailboxState<T>>>,
}

impl<T> Sender<T> {
    /// Attempts to append `value` to the mailbox without waiting.
    ///
    /// Ownership of `value` is returned when the mailbox is full or closed.
    /// A closed mailbox takes precedence over a full one.
    pub(crate) fn try_send(&self, value: T) -> Result<(), TrySendError<T>> {
        try_send(&self.shared, value)
    }

    /// Creates a producer that can send only while at least one ordinary
    /// sender remains alive.
    ///
    /// Weak senders are intended for work spawned internally by the mailbox
    /// consumer. They do not keep the receive stream open after every external
    /// producer has gone away.
    pub(crate) fn downgrade(&self) -> WeakSender<T> {
        WeakSender {
            shared: Rc::downgrade(&self.shared),
        }
    }

    /// Returns the number of values currently queued.
    #[must_use]
    pub(crate) fn len(&self) -> usize {
        self.shared.borrow().queue.len()
    }

    /// Returns whether the mailbox currently contains no values.
    #[must_use]
    #[cfg(test)]
    pub(crate) fn is_empty(&self) -> bool {
        self.shared.borrow().queue.is_empty()
    }

    /// Returns the maximum number of queued values.
    #[must_use]
    pub(crate) fn capacity(&self) -> usize {
        self.shared.borrow().capacity
    }

    /// Returns whether the receiver has been dropped.
    #[must_use]
    #[cfg(test)]
    pub(crate) fn is_closed(&self) -> bool {
        !self.shared.borrow().receiver_open
    }
}

/// An internal producer that does not keep a mailbox receive stream alive.
pub(crate) struct WeakSender<T> {
    shared: Weak<RefCell<MailboxState<T>>>,
}

impl<T> WeakSender<T> {
    /// Attempts to append `value` while an ordinary sender and the receiver
    /// both remain alive.
    pub(crate) fn try_send(&self, value: T) -> Result<(), TrySendError<T>> {
        let Some(shared) = self.shared.upgrade() else {
            return Err(TrySendError::Closed(value));
        };
        if shared.borrow().sender_count == 0 {
            return Err(TrySendError::Closed(value));
        }
        try_send(&shared, value)
    }
}

impl<T> Clone for WeakSender<T> {
    fn clone(&self) -> Self {
        Self {
            shared: self.shared.clone(),
        }
    }
}

fn try_send<T>(shared: &Rc<RefCell<MailboxState<T>>>, value: T) -> Result<(), TrySendError<T>> {
    let waker = {
        let mut state = shared.borrow_mut();
        if !state.receiver_open {
            return Err(TrySendError::Closed(value));
        }
        if state.queue.len() == state.capacity {
            return Err(TrySendError::Full(value));
        }

        state.queue.push_back(value);
        state.receiver_waker.take()
    };

    if let Some(waker) = waker {
        wake_safely(waker);
    }
    Ok(())
}

impl<T> Clone for Sender<T> {
    fn clone(&self) -> Self {
        let mut state = self.shared.borrow_mut();
        state.sender_count = state
            .sender_count
            .checked_add(1)
            .expect("mailbox sender count exhausted");
        drop(state);

        Self {
            shared: Rc::clone(&self.shared),
        }
    }
}

impl<T> Drop for Sender<T> {
    fn drop(&mut self) {
        let waker = {
            let mut state = self.shared.borrow_mut();
            debug_assert!(state.sender_count > 0);
            state.sender_count -= 1;
            (state.sender_count == 0)
                .then(|| state.receiver_waker.take())
                .flatten()
        };

        if let Some(waker) = waker {
            wake_safely(waker);
        }
    }
}

/// The single receiving side of a bounded local mailbox.
pub(crate) struct Receiver<T> {
    shared: Rc<RefCell<MailboxState<T>>>,
}

impl<T> Receiver<T> {
    /// Waits for the next queued value.
    ///
    /// Values are returned in send order. Once every sender is dropped, queued
    /// values are drained before this future returns `None`.
    pub(crate) fn recv(&mut self) -> Recv<'_, T> {
        Recv { receiver: self }
    }

    /// Returns the number of values currently queued.
    #[must_use]
    #[cfg(test)]
    pub(crate) fn len(&self) -> usize {
        self.shared.borrow().queue.len()
    }

    /// Returns whether the mailbox currently contains no values.
    #[must_use]
    #[cfg(test)]
    pub(crate) fn is_empty(&self) -> bool {
        self.shared.borrow().queue.is_empty()
    }

    /// Returns the maximum number of queued values.
    #[must_use]
    #[cfg(test)]
    pub(crate) fn capacity(&self) -> usize {
        self.shared.borrow().capacity
    }

    /// Returns whether every sender has been dropped.
    ///
    /// A closed receiver may still have queued values available to drain.
    #[must_use]
    #[cfg(test)]
    pub(crate) fn is_closed(&self) -> bool {
        self.shared.borrow().sender_count == 0
    }
}

impl<T> Drop for Receiver<T> {
    fn drop(&mut self) {
        let (queued, stale_waker) = {
            let mut state = self.shared.borrow_mut();
            state.receiver_open = false;
            (
                std::mem::take(&mut state.queue),
                state.receiver_waker.take(),
            )
        };

        // User values and wakers can run arbitrary destructors. Drop them only
        // after releasing the shared RefCell borrow.
        drop(queued);
        drop(stale_waker);
    }
}

/// Future returned by [`Receiver::recv`].
pub(crate) struct Recv<'a, T> {
    receiver: &'a mut Receiver<T>,
}

impl<T> Future for Recv<'_, T> {
    type Output = Option<T>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let (poll, stale_waker) = {
            let mut state = self.receiver.shared.borrow_mut();
            if let Some(value) = state.queue.pop_front() {
                (Poll::Ready(Some(value)), state.receiver_waker.take())
            } else if state.sender_count == 0 {
                (Poll::Ready(None), state.receiver_waker.take())
            } else {
                let replace = state
                    .receiver_waker
                    .as_ref()
                    .is_none_or(|registered| !registered.will_wake(cx.waker()));
                let stale_waker = replace
                    .then(|| state.receiver_waker.replace(cx.waker().clone()))
                    .flatten();
                (Poll::Pending, stale_waker)
            }
        };

        drop(stale_waker);
        poll
    }
}

impl<T> Drop for Recv<'_, T> {
    fn drop(&mut self) {
        let stale_waker = self.receiver.shared.borrow_mut().receiver_waker.take();
        drop(stale_waker);
    }
}

/// Failure returned by [`Sender::try_send`].
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum TrySendError<T> {
    /// The mailbox is at its configured capacity.
    Full(T),
    /// The receiver has been dropped.
    Closed(T),
}

/// Creates a local, single-value channel.
pub(crate) fn oneshot<T>() -> (OneshotSender<T>, OneshotReceiver<T>) {
    let shared = Rc::new(RefCell::new(OneshotState {
        value: None,
        sender_alive: true,
        receiver_open: true,
        receiver_waker: None,
    }));

    (
        OneshotSender {
            shared: Some(Rc::clone(&shared)),
        },
        OneshotReceiver {
            shared,
            completed: false,
        },
    )
}

struct OneshotState<T> {
    value: Option<T>,
    sender_alive: bool,
    receiver_open: bool,
    receiver_waker: Option<Waker>,
}

/// The sending side of a local single-value channel.
pub(crate) struct OneshotSender<T> {
    shared: Option<Rc<RefCell<OneshotState<T>>>>,
}

impl<T> OneshotSender<T> {
    /// Delivers the value, consuming the sender.
    ///
    /// If the receiver has already been dropped, ownership of `value` is
    /// returned to the caller.
    pub(crate) fn send(mut self, value: T) -> Result<(), T> {
        let shared = self
            .shared
            .take()
            .expect("oneshot sender used after completion");
        let (result, waker) = {
            let mut state = shared.borrow_mut();
            state.sender_alive = false;
            if state.receiver_open {
                state.value = Some(value);
                (Ok(()), state.receiver_waker.take())
            } else {
                (Err(value), None)
            }
        };

        if let Some(waker) = waker {
            wake_safely(waker);
        }
        result
    }
}

impl<T> Drop for OneshotSender<T> {
    fn drop(&mut self) {
        let Some(shared) = self.shared.take() else {
            return;
        };

        let waker = {
            let mut state = shared.borrow_mut();
            state.sender_alive = false;
            state.receiver_waker.take()
        };
        if let Some(waker) = waker {
            wake_safely(waker);
        }
    }
}

/// The receiving future of a local single-value channel.
///
/// Dropping this future closes the channel. Polling it after it has completed
/// is a programming error and panics.
pub(crate) struct OneshotReceiver<T> {
    shared: Rc<RefCell<OneshotState<T>>>,
    completed: bool,
}

impl<T> Future for OneshotReceiver<T> {
    type Output = Result<T, OneshotCanceled>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        assert!(!this.completed, "oneshot receiver polled after completion");

        let (poll, stale_waker) = {
            let mut state = this.shared.borrow_mut();
            if let Some(value) = state.value.take() {
                state.receiver_open = false;
                (Poll::Ready(Ok(value)), state.receiver_waker.take())
            } else if !state.sender_alive {
                state.receiver_open = false;
                (
                    Poll::Ready(Err(OneshotCanceled)),
                    state.receiver_waker.take(),
                )
            } else {
                let replace = state
                    .receiver_waker
                    .as_ref()
                    .is_none_or(|registered| !registered.will_wake(cx.waker()));
                let stale_waker = replace
                    .then(|| state.receiver_waker.replace(cx.waker().clone()))
                    .flatten();
                (Poll::Pending, stale_waker)
            }
        };

        drop(stale_waker);
        if poll.is_ready() {
            this.completed = true;
        }
        poll
    }
}

impl<T> Drop for OneshotReceiver<T> {
    fn drop(&mut self) {
        let (value, stale_waker) = {
            let mut state = self.shared.borrow_mut();
            state.receiver_open = false;
            (state.value.take(), state.receiver_waker.take())
        };

        drop(value);
        drop(stale_waker);
    }
}

/// The oneshot sender was dropped without delivering a value.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct OneshotCanceled;

impl fmt::Display for OneshotCanceled {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("oneshot sender dropped without sending")
    }
}

impl Error for OneshotCanceled {}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::task::{Wake, Waker};

    #[derive(Default)]
    struct WakeCounter {
        count: AtomicUsize,
    }

    impl Wake for WakeCounter {
        fn wake(self: Arc<Self>) {
            self.count.fetch_add(1, Ordering::SeqCst);
        }

        fn wake_by_ref(self: &Arc<Self>) {
            self.count.fetch_add(1, Ordering::SeqCst);
        }
    }

    impl WakeCounter {
        fn get(&self) -> usize {
            self.count.load(Ordering::SeqCst)
        }
    }

    fn counting_waker() -> (Arc<WakeCounter>, Waker) {
        let counter = Arc::new(WakeCounter::default());
        let waker = Waker::from(Arc::clone(&counter));
        (counter, waker)
    }

    fn poll<F: Future>(future: Pin<&mut F>, waker: &Waker) -> Poll<F::Output> {
        future.poll(&mut Context::from_waker(waker))
    }

    #[test]
    fn mailbox_is_fifo_and_reports_capacity() {
        let (sender, mut receiver) = channel(2);
        assert_eq!(sender.capacity(), 2);
        assert_eq!(receiver.capacity(), 2);
        assert!(sender.is_empty());
        assert!(receiver.is_empty());

        assert_eq!(sender.try_send(10), Ok(()));
        assert_eq!(sender.try_send(20), Ok(()));
        assert_eq!(sender.try_send(30), Err(TrySendError::Full(30)));
        assert_eq!(sender.len(), 2);
        assert_eq!(receiver.len(), 2);

        let (_, waker) = counting_waker();
        let mut first = receiver.recv();
        assert_eq!(poll(Pin::new(&mut first), &waker), Poll::Ready(Some(10)));
        drop(first);
        assert_eq!(sender.try_send(30), Ok(()));

        let mut second = receiver.recv();
        assert_eq!(poll(Pin::new(&mut second), &waker), Poll::Ready(Some(20)));
        drop(second);
        let mut third = receiver.recv();
        assert_eq!(poll(Pin::new(&mut third), &waker), Poll::Ready(Some(30)));
    }

    #[test]
    fn zero_capacity_mailbox_applies_backpressure() {
        let (sender, _receiver) = channel(0);
        assert_eq!(sender.capacity(), 0);
        assert_eq!(sender.try_send("value"), Err(TrySendError::Full("value")));
    }

    #[test]
    fn maximum_logical_capacity_does_not_reserve_eagerly() {
        let (sender, mut receiver) = channel(usize::MAX);
        assert_eq!(sender.capacity(), usize::MAX);
        assert_eq!(sender.try_send(7), Ok(()));

        let (_, waker) = counting_waker();
        let mut receive = receiver.recv();
        assert_eq!(poll(Pin::new(&mut receive), &waker), Poll::Ready(Some(7)));
    }

    #[test]
    fn send_wakes_a_pending_receiver() {
        let (sender, mut receiver) = channel(1);
        let (counter, waker) = counting_waker();
        let mut receive = receiver.recv();

        assert_eq!(poll(Pin::new(&mut receive), &waker), Poll::Pending);
        assert_eq!(counter.get(), 0);
        assert_eq!(sender.try_send(7), Ok(()));
        assert_eq!(counter.get(), 1);
        assert_eq!(poll(Pin::new(&mut receive), &waker), Poll::Ready(Some(7)));
    }

    #[test]
    fn receiver_replaces_an_obsolete_waker() {
        let (sender, mut receiver) = channel(1);
        let (first_counter, first_waker) = counting_waker();
        let (second_counter, second_waker) = counting_waker();
        let mut receive = receiver.recv();

        assert_eq!(poll(Pin::new(&mut receive), &first_waker), Poll::Pending);
        assert_eq!(poll(Pin::new(&mut receive), &second_waker), Poll::Pending);
        assert_eq!(sender.try_send(1), Ok(()));
        assert_eq!(first_counter.get(), 0);
        assert_eq!(second_counter.get(), 1);
    }

    #[test]
    fn final_sender_drop_wakes_receiver_and_ends_stream() {
        let (sender, mut receiver) = channel::<u8>(1);
        let other_sender = sender.clone();
        let (counter, waker) = counting_waker();
        let mut receive = receiver.recv();

        assert_eq!(poll(Pin::new(&mut receive), &waker), Poll::Pending);
        drop(sender);
        assert_eq!(counter.get(), 0);
        drop(other_sender);
        assert_eq!(counter.get(), 1);
        assert_eq!(poll(Pin::new(&mut receive), &waker), Poll::Ready(None));
    }

    #[test]
    fn weak_sender_does_not_keep_stream_open_or_send_after_last_sender_drops() {
        let (sender, mut receiver) = channel::<u8>(1);
        let weak = sender.downgrade();
        let (counter, waker) = counting_waker();
        let mut receive = receiver.recv();

        assert_eq!(poll(Pin::new(&mut receive), &waker), Poll::Pending);
        assert_eq!(weak.try_send(7), Ok(()));
        assert_eq!(counter.get(), 1);
        assert_eq!(poll(Pin::new(&mut receive), &waker), Poll::Ready(Some(7)));
        drop(receive);

        let mut end = receiver.recv();
        assert_eq!(poll(Pin::new(&mut end), &waker), Poll::Pending);
        drop(sender);
        assert_eq!(poll(Pin::new(&mut end), &waker), Poll::Ready(None));
        drop(end);
        assert_eq!(weak.try_send(9), Err(TrySendError::Closed(9)));
    }

    #[test]
    fn queued_values_are_drained_after_senders_close() {
        let (sender, mut receiver) = channel(1);
        assert_eq!(sender.try_send(42), Ok(()));
        drop(sender);
        assert!(receiver.is_closed());

        let (_, waker) = counting_waker();
        let mut receive = receiver.recv();
        assert_eq!(poll(Pin::new(&mut receive), &waker), Poll::Ready(Some(42)));
        drop(receive);
        let mut end = receiver.recv();
        assert_eq!(poll(Pin::new(&mut end), &waker), Poll::Ready(None));
    }

    #[test]
    fn receiver_drop_closes_mailbox() {
        let (sender, receiver) = channel(2);
        assert_eq!(sender.try_send(String::from("queued")), Ok(()));
        drop(receiver);

        assert!(sender.is_closed());
        let value = String::from("returned");
        assert_eq!(
            sender.try_send(value),
            Err(TrySendError::Closed(String::from("returned")))
        );
        assert_eq!(sender.len(), 0);
    }

    #[test]
    fn dropping_pending_receive_removes_its_waker() {
        let (sender, mut receiver) = channel(1);
        let (counter, waker) = counting_waker();
        let mut receive = receiver.recv();
        assert_eq!(poll(Pin::new(&mut receive), &waker), Poll::Pending);
        drop(receive);

        assert_eq!(sender.try_send(1), Ok(()));
        assert_eq!(counter.get(), 0);
    }

    #[test]
    fn oneshot_send_wakes_receiver_and_delivers_value() {
        let (sender, mut receiver) = oneshot();
        let (counter, waker) = counting_waker();

        assert_eq!(poll(Pin::new(&mut receiver), &waker), Poll::Pending);
        assert_eq!(sender.send(99), Ok(()));
        assert_eq!(counter.get(), 1);
        assert_eq!(poll(Pin::new(&mut receiver), &waker), Poll::Ready(Ok(99)));
    }

    #[test]
    fn oneshot_can_send_before_receiver_is_polled() {
        let (sender, mut receiver) = oneshot();
        assert_eq!(sender.send("ready"), Ok(()));

        let (_, waker) = counting_waker();
        assert_eq!(
            poll(Pin::new(&mut receiver), &waker),
            Poll::Ready(Ok("ready"))
        );
    }

    #[test]
    fn oneshot_sender_drop_wakes_and_cancels_receiver() {
        let (sender, mut receiver) = oneshot::<u8>();
        let (counter, waker) = counting_waker();

        assert_eq!(poll(Pin::new(&mut receiver), &waker), Poll::Pending);
        drop(sender);
        assert_eq!(counter.get(), 1);
        assert_eq!(
            poll(Pin::new(&mut receiver), &waker),
            Poll::Ready(Err(OneshotCanceled))
        );
    }

    #[test]
    fn oneshot_sender_drop_before_poll_is_cancellation() {
        let (sender, mut receiver) = oneshot::<u8>();
        drop(sender);

        let (_, waker) = counting_waker();
        assert_eq!(
            poll(Pin::new(&mut receiver), &waker),
            Poll::Ready(Err(OneshotCanceled))
        );
    }

    #[test]
    fn oneshot_receiver_drop_returns_unsent_value() {
        let (sender, receiver) = oneshot();
        drop(receiver);
        assert_eq!(
            sender.send(String::from("value")),
            Err(String::from("value"))
        );
    }

    #[test]
    fn oneshot_uses_only_the_latest_waker() {
        let (sender, mut receiver) = oneshot();
        let (first_counter, first_waker) = counting_waker();
        let (second_counter, second_waker) = counting_waker();

        assert_eq!(poll(Pin::new(&mut receiver), &first_waker), Poll::Pending);
        assert_eq!(poll(Pin::new(&mut receiver), &second_waker), Poll::Pending);
        assert_eq!(sender.send(1), Ok(()));
        assert_eq!(first_counter.get(), 0);
        assert_eq!(second_counter.get(), 1);
    }
}
