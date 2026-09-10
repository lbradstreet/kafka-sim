//! Shared single-response completion futures for I/O providers.
//!
//! Every provider in this crate resolves an admitted operation through one of
//! two primitives: an owner-thread [`LocalOperation`] backed by an
//! `Rc<RefCell<..>>` cell, or a thread-safe [`SyncOperation`] backed by an
//! `Arc<Mutex<..>>` cell. Both deliver exactly one output, coalesce wakers,
//! contain waker panics so a provider task or actor cannot be unwound by a
//! caller-supplied waker, and release any held admission permit when the
//! output is consumed.
//!
//! Dropping either future abandons only response delivery: the provider
//! retains the admitted operation until its terminal completion, and the
//! abandoned cell's waker is cleared so completion cannot wake a stale task.

use std::cell::{Cell, RefCell};
use std::future::Future;
use std::pin::Pin;
use std::rc::Rc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::task::{Context, Poll, Waker};

use kr_runtime::contain_panic;

mod metrics;
pub use metrics::{CompletionMetrics, CompletionSnapshot};

/// Adds a caller-owned lifetime obligation to the provider's actual completion
/// cell, rather than to the observing future alone.
///
/// Attach immediately after warm submission and before handing the response to
/// another owner. Abandoning the response does not release the guard while the
/// provider retains the admitted operation. It is released outside the cell's
/// lock/borrow after terminal output consumption or destruction. An already
/// consumed response releases a newly attached guard immediately.
///
/// Attachments compose without replacing prior obligations. Each additional
/// attachment after the first allocates one small pair; adapters must bound and
/// account for their own attachment count. Guards must not retain the provider,
/// operation response, or an object owning that provider: doing so forms a cycle.
/// Their destructors must obey the provider's documented non-reentrancy rules.
pub trait CompletionGuard {
    fn attach_completion_guard(&mut self, guard: Arc<dyn Send + Sync>);
}
fn compose_guard(
    previous: Option<Arc<dyn Send + Sync>>,
    guard: Arc<dyn Send + Sync>,
) -> Arc<dyn Send + Sync> {
    match previous {
        Some(previous) => Arc::new((previous, guard)),
        None => guard,
    }
}

/// An owner-thread operation response. Polling after completion panics.
///
/// Dropping this future never cancels its admitted operation. Provider state
/// retains the operation until it terminalizes, and an operation permit held
/// by the response is released when the unobserved terminal output is
/// discarded.
#[must_use = "I/O operation futures must be awaited to observe their completion"]
pub struct LocalOperation<T> {
    cell: Rc<LocalCell<T>>,
    consumed: bool,
}

impl<T> Unpin for LocalOperation<T> {}

impl<T> Future for LocalOperation<T> {
    type Output = T;

    fn poll(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        assert!(
            !self.consumed,
            "I/O operation future polled after completion"
        );
        match self.cell.poll(context) {
            Poll::Ready(output) => {
                self.consumed = true;
                Poll::Ready(output)
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

impl<T> Drop for LocalOperation<T> {
    fn drop(&mut self) {
        self.cell.abandon();
    }
}

impl<T> CompletionGuard for LocalOperation<T> {
    fn attach_completion_guard(&mut self, guard: Arc<dyn Send + Sync>) {
        if self.consumed {
            return;
        }
        let mut inner = self.cell.inner.borrow_mut();
        inner.supplemental = Some(compose_guard(inner.supplemental.take(), guard));
    }
}

impl<T> LocalOperation<T> {
    pub(crate) fn from_cell(cell: Rc<LocalCell<T>>) -> Self {
        Self {
            cell,
            consumed: false,
        }
    }

    /// Creates a pending response and the cell a provider completes later.
    pub(crate) fn pending(admission: impl Into<LocalAdmission>) -> (Self, Rc<LocalCell<T>>) {
        let cell = Rc::new(LocalCell::new(admission));
        (Self::from_cell(Rc::clone(&cell)), cell)
    }

    /// Creates an already-completed response.
    pub(crate) fn ready(output: T) -> Self {
        Self::ready_with_permit(None, output)
    }

    /// Creates an already-completed response that still holds its admission.
    pub(crate) fn ready_with_permit(admission: impl Into<LocalAdmission>, output: T) -> Self {
        let cell = LocalCell::new(admission);
        cell.complete(output);
        Self::from_cell(Rc::new(cell))
    }
}

/// The owner-thread completion state shared by a provider and one response.
///
/// A completion is observable only once its output is stored, its scheduled
/// delivery delay (if any) has elapsed, and every closed gate has reopened.
pub(crate) struct LocalCell<T> {
    inner: RefCell<LocalInner<T>>,
}

struct LocalInner<T> {
    output: Option<T>,
    delay_elapsed: bool,
    closed_gates: usize,
    admission: LocalAdmission,
    supplemental: Option<Arc<dyn Send + Sync>>,
    waker: Option<Waker>,
}

impl<T> LocalInner<T> {
    fn take_ready_waker(&mut self) -> Option<Waker> {
        if self.delay_elapsed && self.closed_gates == 0 && self.output.is_some() {
            self.waker.take()
        } else {
            None
        }
    }
}

impl<T> LocalCell<T> {
    /// Creates a cell whose output is observable as soon as it is stored.
    pub(crate) fn new(admission: impl Into<LocalAdmission>) -> Self {
        Self::build(admission.into(), true)
    }

    /// Creates a cell whose output additionally waits for
    /// [`Self::mark_delay_elapsed`].
    pub(crate) fn with_delay(admission: impl Into<LocalAdmission>) -> Self {
        Self::build(admission.into(), false)
    }

    fn build(admission: LocalAdmission, delay_elapsed: bool) -> Self {
        Self {
            inner: RefCell::new(LocalInner {
                output: None,
                delay_elapsed,
                closed_gates: 0,
                admission,
                supplemental: None,
                waker: None,
            }),
        }
    }

    /// Stores the single output and wakes the response when it is observable.
    pub(crate) fn complete(&self, output: T) {
        let waker = {
            let mut inner = self.inner.borrow_mut();
            debug_assert!(inner.output.is_none(), "operation completed twice");
            inner.output = Some(output);
            inner.take_ready_waker()
        };
        if let Some(waker) = waker {
            wake_contained(waker);
        }
    }

    /// Blocks observability until a matching [`Self::open_gate`].
    pub(crate) fn close_gate(&self) {
        let mut inner = self.inner.borrow_mut();
        inner.closed_gates = inner
            .closed_gates
            .checked_add(1)
            .expect("admission bounds the number of completion gates");
    }

    /// Reopens one gate and wakes the response when it became observable.
    pub(crate) fn open_gate(&self) {
        let waker = {
            let mut inner = self.inner.borrow_mut();
            debug_assert!(inner.closed_gates != 0);
            inner.closed_gates = inner.closed_gates.saturating_sub(1);
            inner.take_ready_waker()
        };
        if let Some(waker) = waker {
            wake_contained(waker);
        }
    }

    /// Marks the scheduled delivery delay elapsed. Idempotent.
    pub(crate) fn mark_delay_elapsed(&self) {
        let waker = {
            let mut inner = self.inner.borrow_mut();
            inner.delay_elapsed = true;
            inner.take_ready_waker()
        };
        if let Some(waker) = waker {
            wake_contained(waker);
        }
    }

    fn poll(&self, context: &mut Context<'_>) -> Poll<T> {
        let ready = {
            let mut inner = self.inner.borrow_mut();
            if inner.delay_elapsed && inner.closed_gates == 0 {
                inner.output.take().map(|output| {
                    (
                        output,
                        std::mem::take(&mut inner.admission),
                        inner.supplemental.take(),
                        inner.waker.take(),
                    )
                })
            } else {
                None
            }
        };
        if let Some((output, admission, supplemental, waker)) = ready {
            drop(waker);
            drop(admission);
            drop(supplemental);
            return Poll::Ready(output);
        }
        let candidate = context.waker().clone();
        let stale = {
            let mut inner = self.inner.borrow_mut();
            if inner
                .waker
                .as_ref()
                .is_some_and(|waker| waker.will_wake(context.waker()))
            {
                Some(candidate)
            } else {
                inner.waker.replace(candidate)
            }
        };
        drop(stale);
        Poll::Pending
    }

    /// Discards the stored waker so completion cannot wake a stale task.
    fn abandon(&self) {
        let waker = self.inner.borrow_mut().waker.take();
        drop(waker);
    }
}

/// A thread-safe operation response. Polling after completion panics.
///
/// Dropping this future never cancels its admitted operation; provider state
/// retains it until terminal completion.
#[must_use = "I/O operation futures must be awaited to observe their completion"]
pub struct SyncOperation<T> {
    cell: Arc<SyncCell<T>>,
    consumed: bool,
}

impl<T> Unpin for SyncOperation<T> {}

impl<T> Future for SyncOperation<T> {
    type Output = T;

    fn poll(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        assert!(
            !self.consumed,
            "I/O operation future polled after completion"
        );
        match self.cell.poll(context) {
            Poll::Ready(output) => {
                self.consumed = true;
                Poll::Ready(output)
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

impl<T> Drop for SyncOperation<T> {
    fn drop(&mut self) {
        self.cell.abandon();
    }
}

impl<T> CompletionGuard for SyncOperation<T> {
    fn attach_completion_guard(&mut self, guard: Arc<dyn Send + Sync>) {
        if self.consumed {
            return;
        }
        let mut inner = lock_unpoisoned(&self.cell.inner);
        inner.supplemental = Some(compose_guard(inner.supplemental.take(), guard));
    }
}

impl<T> SyncOperation<T> {
    pub(crate) fn from_cell(cell: Arc<SyncCell<T>>) -> Self {
        Self {
            cell,
            consumed: false,
        }
    }

    /// Creates a pending response and the cell a provider completes later.
    pub(crate) fn pending(permit: Option<SyncPermit>) -> (Self, Arc<SyncCell<T>>) {
        let cell = Arc::new(SyncCell::new(permit));
        (Self::from_cell(Arc::clone(&cell)), cell)
    }

    /// Creates a pending response and the responder that completes it.
    ///
    /// This is the constructor for external providers; providers in this
    /// crate hold the completion cell directly.
    pub fn channel() -> (Self, SyncResponder<T>) {
        let (future, cell) = Self::pending(None);
        (future, SyncResponder { cell })
    }

    /// Creates a pending response with an external provider's admission guard.
    ///
    /// The guard is retained until the terminal output is consumed, or until
    /// both response and responder have been dropped. In particular, dropping
    /// the response while the provider still owns its responder does not
    /// release admission. Guards are dropped outside the completion lock;
    /// abandoned terminal outputs are destroyed before their guard is released.
    pub fn channel_with_guard<G: Send + 'static>(guard: G) -> (Self, SyncResponder<T>) {
        let cell = Arc::new(SyncCell::with_guard(None, Some(Box::new(guard))));
        (Self::from_cell(Arc::clone(&cell)), SyncResponder { cell })
    }

    /// Creates a guarded response with optional native diagnostics installed
    /// before the provider can publish. `None` performs no clock reads.
    /// Use a bounded shared metrics object; it must not retain this operation.
    pub fn channel_with_guard_and_metrics<G: Send + 'static>(
        guard: G,
        metrics: Option<Arc<CompletionMetrics>>,
    ) -> (Self, SyncResponder<T>) {
        let mut cell = SyncCell::with_guard(None, Some(Box::new(guard)));
        cell.inner
            .get_mut()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .observation = metrics.map(metrics::Observation::new);
        let cell = Arc::new(cell);
        (Self::from_cell(Arc::clone(&cell)), SyncResponder { cell })
    }

    /// Creates an already-completed response.
    pub fn ready(output: T) -> Self {
        let cell = SyncCell::new(None);
        let woken = cell.set_output(output);
        debug_assert!(woken.is_none(), "a fresh cell has no waker");
        Self::from_cell(Arc::new(cell))
    }

    /// Returns whether a terminal output is installed but not yet consumed.
    #[cfg(test)]
    pub(crate) fn output_installed(&self) -> bool {
        lock_unpoisoned(&self.cell.inner).output.is_some()
    }
}

/// The completing side of a [`SyncOperation::channel`] pair.
///
/// A response is completed exactly once. Dropping the responder without
/// completing abandons the response permanently; provider actors that must
/// guarantee a terminal output should wrap the responder in a guard that
/// completes on drop.
pub struct SyncResponder<T> {
    cell: Arc<SyncCell<T>>,
}

impl<T> SyncResponder<T> {
    /// Delivers the single output and wakes the response if it was polled.
    ///
    /// The wake is panic-contained: a caller-supplied waker cannot unwind
    /// through the completing actor.
    pub fn complete(self, output: T) {
        self.cell.complete(output);
    }
}

/// The thread-safe completion state shared by a provider and one response.
///
/// [`Self::set_output`] returns the waker instead of invoking it, so a
/// provider can complete operations while holding its own state lock and run
/// the wakes after releasing it. Outputs, permits, and wakers are likewise
/// only dropped outside the cell lock: destroying them can run arbitrary
/// caller code that may re-enter provider state.
pub(crate) struct SyncCell<T> {
    inner: Mutex<SyncInner<T>>,
}

struct SyncInner<T> {
    output: Option<T>,
    permit: Option<SyncPermit>,
    guard: Option<Box<dyn Send>>,
    supplemental: Option<Arc<dyn Send + Sync>>,
    waker: Option<SafeWaker>,
    observation: Option<metrics::Observation>,
}

impl<T> SyncCell<T> {
    pub(crate) fn new(permit: Option<SyncPermit>) -> Self {
        Self::with_guard(permit, None)
    }

    fn with_guard(permit: Option<SyncPermit>, guard: Option<Box<dyn Send>>) -> Self {
        Self {
            inner: Mutex::new(SyncInner {
                output: None,
                permit,
                guard,
                supplemental: None,
                waker: None,
                observation: None,
            }),
        }
    }

    /// Stores the single output and returns the waker for a deferred wake.
    pub(crate) fn set_output(&self, output: T) -> Option<SafeWaker> {
        let mut inner = lock_unpoisoned(&self.inner);
        debug_assert!(inner.output.is_none(), "operation completed twice");
        inner.output = Some(output);
        if let Some(observation) = &mut inner.observation {
            observation.publish();
        }
        inner.waker.take()
    }

    /// Stores the single output and wakes the response immediately.
    pub(crate) fn complete(&self, output: T) {
        if let Some(waker) = self.set_output(output) {
            waker.wake();
        }
    }

    fn poll(&self, context: &mut Context<'_>) -> Poll<T> {
        let ready = {
            let mut inner = lock_unpoisoned(&self.inner);
            inner.output.take().map(|output| {
                (
                    output,
                    inner.permit.take(),
                    inner.guard.take(),
                    inner.supplemental.take(),
                    inner.waker.take(),
                    inner.observation.take(),
                )
            })
        };
        if let Some((output, permit, guard, supplemental, waker, observation)) = ready {
            if let Some(observation) = observation {
                observation.finish(false);
            }
            drop(waker);
            drop(permit);
            drop(guard);
            drop(supplemental);
            return Poll::Ready(output);
        }

        let Some(candidate) = SafeWaker::clone_from(context.waker()) else {
            return Poll::Pending;
        };
        let (ready, stale) = {
            let mut inner = lock_unpoisoned(&self.inner);
            if let Some(output) = inner.output.take() {
                (
                    Some((
                        output,
                        inner.permit.take(),
                        inner.guard.take(),
                        inner.supplemental.take(),
                        inner.observation.take(),
                    )),
                    inner.waker.take(),
                )
            } else if inner
                .waker
                .as_ref()
                .is_some_and(|waker| waker.will_wake(context.waker()))
            {
                (None, Some(candidate))
            } else {
                (None, inner.waker.replace(candidate))
            }
        };
        drop(stale);
        match ready {
            Some((output, permit, guard, supplemental, observation)) => {
                if let Some(observation) = observation {
                    observation.finish(false);
                }
                drop(permit);
                drop(guard);
                drop(supplemental);
                Poll::Ready(output)
            }
            None => Poll::Pending,
        }
    }

    /// Discards the stored waker so completion cannot wake a stale task.
    fn abandon(&self) {
        let waker = lock_unpoisoned(&self.inner).waker.take();
        drop(waker);
    }
}

impl<T> Drop for SyncCell<T> {
    fn drop(&mut self) {
        let inner = self
            .inner
            .get_mut()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(observation) = inner.observation.take() {
            observation.finish(true);
        }
        drop(inner.waker.take());
    }
}

/// A waker whose clone, wake, and drop are all panic-contained.
pub(crate) struct SafeWaker(Option<Waker>);

impl SafeWaker {
    fn clone_from(waker: &Waker) -> Option<Self> {
        let mut cloned = None;
        contain_panic(|| cloned = Some(waker.clone()));
        cloned.map(|waker| Self(Some(waker)))
    }

    fn will_wake(&self, candidate: &Waker) -> bool {
        self.0
            .as_ref()
            .is_some_and(|waker| waker.will_wake(candidate))
    }

    pub(crate) fn wake(mut self) {
        if let Some(waker) = self.0.take() {
            contain_panic(|| waker.wake());
        }
    }
}

impl Drop for SafeWaker {
    fn drop(&mut self) {
        if let Some(waker) = self.0.take() {
            contain_panic(|| drop(waker));
        }
    }
}

/// Wakes without letting a caller-supplied waker unwind through the provider
/// task that delivers completions and strand later admitted operations.
pub(crate) fn wake_contained(waker: Waker) {
    contain_panic(|| waker.wake());
}

/// Every reservation one admitted owner-thread operation holds.
///
/// A provider that bounds more than one resource — an operation count and the
/// bytes those operations hold, say — acquires one permit per bound and hands
/// them over together. They release as a unit when the output is consumed or
/// discarded, so no bound can outlive another and none can be released twice.
#[derive(Default)]
pub(crate) struct LocalAdmission {
    // Held for their `Drop`, which returns each reservation to its pool. They
    // are never read: releasing is the entire purpose of retaining them.
    #[expect(dead_code, reason = "each permit releases its pool charge on drop")]
    operation: Option<LocalPermit>,
    #[expect(dead_code, reason = "each permit releases its pool charge on drop")]
    bytes: Option<LocalPermit>,
}

impl LocalAdmission {
    /// Reservations for an operation that also charges a byte budget.
    pub(crate) const fn with_bytes(operation: LocalPermit, bytes: LocalPermit) -> Self {
        Self {
            operation: Some(operation),
            bytes: Some(bytes),
        }
    }
}

impl From<Option<LocalPermit>> for LocalAdmission {
    fn from(operation: Option<LocalPermit>) -> Self {
        Self {
            operation,
            bytes: None,
        }
    }
}

impl From<LocalPermit> for LocalAdmission {
    fn from(operation: LocalPermit) -> Self {
        Self {
            operation: Some(operation),
            bytes: None,
        }
    }
}

/// An owner-thread bounded admission counter.
///
/// A permit carries the charge it acquired, so one pool can bound either a
/// count of operations (charge one) or a quantity they hold, such as
/// outstanding bytes. Both denominations release on drop, which is the
/// lifetime an in-flight reservation needs: admission to terminal consumption
/// or abandonment.
pub(crate) struct LocalPermitPool {
    in_use: Cell<usize>,
    limit: usize,
}

impl LocalPermitPool {
    pub(crate) const fn new(limit: usize) -> Self {
        Self {
            in_use: Cell::new(0),
            limit,
        }
    }

    pub(crate) fn acquire(self: &Rc<Self>) -> Option<LocalPermit> {
        self.acquire_many(1)
    }

    /// Reserves `charge` units, or returns `None` leaving the counter untouched.
    ///
    /// A charge that overflows the counter or exceeds the limit is refused
    /// rather than saturated: admission must reject, never silently admit less
    /// than it accounted for.
    pub(crate) fn acquire_many(self: &Rc<Self>, charge: usize) -> Option<LocalPermit> {
        let next = self.in_use.get().checked_add(charge)?;
        if next > self.limit {
            return None;
        }
        self.in_use.set(next);
        Some(LocalPermit {
            pool: Rc::clone(self),
            charge,
        })
    }

    pub(crate) fn in_use(&self) -> usize {
        self.in_use.get()
    }
}

/// One owner-thread admission reservation, released on drop.
pub(crate) struct LocalPermit {
    pool: Rc<LocalPermitPool>,
    charge: usize,
}

impl Drop for LocalPermit {
    fn drop(&mut self) {
        let current = self.pool.in_use.get();
        debug_assert!(current >= self.charge, "operation permit count underflowed");
        self.pool.in_use.set(current.saturating_sub(self.charge));
    }
}

/// A thread-safe bounded admission counter.
///
/// Carries a charge per permit for the same reason [`LocalPermitPool`] does:
/// one mechanism bounds both operation counts and the bytes those operations
/// hold in flight.
pub(crate) struct SyncPermitPool {
    in_use: AtomicUsize,
    limit: usize,
}

impl SyncPermitPool {
    pub(crate) const fn new(limit: usize) -> Self {
        Self {
            in_use: AtomicUsize::new(0),
            limit,
        }
    }

    pub(crate) fn acquire(self: &Arc<Self>) -> Option<SyncPermit> {
        self.acquire_many(1)
    }

    /// Reserves `charge` units, or returns `None` leaving the counter untouched.
    ///
    /// A charge that overflows the counter or exceeds the limit is refused
    /// rather than saturated: admission must reject, never silently admit less
    /// than it accounted for.
    pub(crate) fn acquire_many(self: &Arc<Self>, charge: usize) -> Option<SyncPermit> {
        let mut current = self.in_use.load(Ordering::Acquire);
        loop {
            let next = current.checked_add(charge)?;
            if next > self.limit {
                return None;
            }
            match self.in_use.compare_exchange_weak(
                current,
                next,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => {
                    return Some(SyncPermit {
                        pool: Arc::clone(self),
                        charge,
                    });
                }
                Err(observed) => current = observed,
            }
        }
    }

    pub(crate) fn in_use(&self) -> usize {
        self.in_use.load(Ordering::Acquire)
    }
}

/// One thread-safe admission reservation, released on drop.
pub(crate) struct SyncPermit {
    pool: Arc<SyncPermitPool>,
    charge: usize,
}

impl Drop for SyncPermit {
    fn drop(&mut self) {
        let previous = self.pool.in_use.fetch_sub(self.charge, Ordering::AcqRel);
        debug_assert!(
            previous >= self.charge,
            "operation permit count underflowed"
        );
    }
}

/// Locks a mutex, ignoring poisoning: provider state transitions are committed
/// before any foreign code can run, so a poisoned lock still guards a
/// consistent value.
pub(crate) fn lock_unpoisoned<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicUsize;
    use std::task::Wake;

    use super::*;

    struct CountingWake(Arc<AtomicUsize>);

    impl Wake for CountingWake {
        fn wake(self: Arc<Self>) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }

    struct PanickingWake;

    impl Wake for PanickingWake {
        fn wake(self: Arc<Self>) {
            panic!("completion wake panicked");
        }
    }

    fn counting_waker() -> (Waker, Arc<AtomicUsize>) {
        let wakes = Arc::new(AtomicUsize::new(0));
        (
            Waker::from(Arc::new(CountingWake(Arc::clone(&wakes)))),
            wakes,
        )
    }

    fn poll_once<T, F: Future<Output = T> + Unpin>(future: &mut F, waker: &Waker) -> Poll<T> {
        let mut context = Context::from_waker(waker);
        Pin::new(future).poll(&mut context)
    }

    #[test]
    fn local_response_completes_and_wakes_once() {
        let (waker, wakes) = counting_waker();
        let (mut future, cell) = LocalOperation::pending(None);
        assert!(poll_once(&mut future, &waker).is_pending());

        cell.complete(7);

        assert_eq!(wakes.load(Ordering::SeqCst), 1);
        assert_eq!(poll_once(&mut future, &waker), Poll::Ready(7));
    }

    #[test]
    fn abandoned_local_response_does_not_wake() {
        let (waker, wakes) = counting_waker();
        let (mut future, cell) = LocalOperation::pending(None);
        assert!(poll_once(&mut future, &waker).is_pending());

        drop(future);
        cell.complete(7);

        assert_eq!(wakes.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn local_delay_and_gates_block_observability() {
        let (waker, wakes) = counting_waker();
        let cell = Rc::new(LocalCell::with_delay(None));
        let mut future = LocalOperation::from_cell(Rc::clone(&cell));
        cell.close_gate();
        cell.complete(7);
        assert!(poll_once(&mut future, &waker).is_pending());

        cell.mark_delay_elapsed();
        assert_eq!(wakes.load(Ordering::SeqCst), 0);
        assert!(poll_once(&mut future, &waker).is_pending());

        cell.open_gate();
        assert_eq!(wakes.load(Ordering::SeqCst), 1);
        assert_eq!(poll_once(&mut future, &waker), Poll::Ready(7));
    }

    #[test]
    fn local_permit_is_released_when_the_output_is_consumed() {
        let pool = Rc::new(LocalPermitPool::new(1));
        let permit = pool.acquire().expect("first permit is available");
        assert!(pool.acquire().is_none());
        let (mut future, cell) = LocalOperation::pending(Some(permit));

        cell.complete(7);
        assert_eq!(pool.in_use(), 1);

        let (waker, _wakes) = counting_waker();
        assert_eq!(poll_once(&mut future, &waker), Poll::Ready(7));
        assert_eq!(pool.in_use(), 0);
    }

    #[test]
    fn a_refused_local_charge_leaves_the_counter_untouched() {
        let pool = Rc::new(LocalPermitPool::new(1_024));
        let permit = pool.acquire_many(1_000).expect("charge fits the limit");
        assert_eq!(pool.in_use(), 1_000);

        assert!(pool.acquire_many(25).is_none());
        assert_eq!(pool.in_use(), 1_000);

        drop(permit);
        assert_eq!(pool.in_use(), 0);
    }

    #[test]
    fn a_local_charge_larger_than_the_limit_is_refused_rather_than_clamped() {
        let pool = Rc::new(LocalPermitPool::new(64));
        assert!(pool.acquire_many(65).is_none());
        assert_eq!(pool.in_use(), 0);
    }

    #[test]
    fn a_local_charge_that_overflows_the_counter_is_refused() {
        let pool = Rc::new(LocalPermitPool::new(usize::MAX));
        let permit = pool.acquire_many(usize::MAX - 1).expect("charge fits");
        assert!(pool.acquire_many(2).is_none());
        assert_eq!(pool.in_use(), usize::MAX - 1);
        drop(permit);
    }

    #[test]
    fn a_zero_local_charge_is_admitted_against_an_exhausted_pool() {
        let pool = Rc::new(LocalPermitPool::new(8));
        let _full = pool.acquire_many(8).expect("charge fits the limit");
        let empty = pool
            .acquire_many(0)
            .expect("a zero charge reserves nothing");
        assert_eq!(pool.in_use(), 8);
        drop(empty);
        assert_eq!(pool.in_use(), 8);
    }

    #[test]
    fn a_refused_sync_charge_leaves_the_counter_untouched() {
        let pool = Arc::new(SyncPermitPool::new(1_024));
        let permit = pool.acquire_many(1_000).expect("charge fits the limit");
        assert_eq!(pool.in_use(), 1_000);

        assert!(pool.acquire_many(25).is_none());
        assert_eq!(pool.in_use(), 1_000);

        drop(permit);
        assert_eq!(pool.in_use(), 0);
    }

    #[test]
    fn a_sync_charge_that_overflows_the_counter_is_refused() {
        let pool = Arc::new(SyncPermitPool::new(usize::MAX));
        let permit = pool.acquire_many(usize::MAX - 1).expect("charge fits");
        assert!(pool.acquire_many(2).is_none());
        assert_eq!(pool.in_use(), usize::MAX - 1);
        drop(permit);
    }

    #[test]
    fn a_unit_acquire_charges_exactly_one() {
        let local = Rc::new(LocalPermitPool::new(4));
        let _local_permit = local.acquire().expect("a unit permit is available");
        assert_eq!(local.in_use(), 1);

        let sync = Arc::new(SyncPermitPool::new(4));
        let _sync_permit = sync.acquire().expect("a unit permit is available");
        assert_eq!(sync.in_use(), 1);
    }

    #[test]
    fn local_completion_contains_a_panicking_waker() {
        let waker = Waker::from(Arc::new(PanickingWake));
        let (mut future, cell) = LocalOperation::pending(None);
        assert!(poll_once(&mut future, &waker).is_pending());

        cell.complete(7);

        assert_eq!(poll_once(&mut future, &waker), Poll::Ready(7));
    }

    #[test]
    #[should_panic(expected = "polled after completion")]
    fn local_response_polled_after_completion_panics() {
        let mut future = LocalOperation::ready(7);
        let (waker, _wakes) = counting_waker();
        assert_eq!(poll_once(&mut future, &waker), Poll::Ready(7));
        let _ = poll_once(&mut future, &waker);
    }

    #[test]
    fn sync_response_defers_its_wake_to_the_completer() {
        let (waker, wakes) = counting_waker();
        let (mut future, cell) = SyncOperation::pending(None);
        assert!(poll_once(&mut future, &waker).is_pending());

        let pending_wake = cell.set_output(7).expect("polled response stored a waker");
        assert_eq!(wakes.load(Ordering::SeqCst), 0);
        pending_wake.wake();

        assert_eq!(wakes.load(Ordering::SeqCst), 1);
        assert_eq!(poll_once(&mut future, &waker), Poll::Ready(7));
    }

    #[test]
    fn abandoned_sync_response_does_not_wake() {
        let (waker, wakes) = counting_waker();
        let (mut future, cell) = SyncOperation::pending(None);
        assert!(poll_once(&mut future, &waker).is_pending());

        drop(future);
        assert!(cell.set_output(7).is_none());

        assert_eq!(wakes.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn sync_permit_is_released_when_the_output_is_consumed() {
        let pool = Arc::new(SyncPermitPool::new(1));
        let permit = pool.acquire().expect("first permit is available");
        assert!(pool.acquire().is_none());
        let (mut future, cell) = SyncOperation::pending(Some(permit));

        cell.complete(7);
        assert_eq!(pool.in_use(), 1);

        let (waker, _wakes) = counting_waker();
        assert_eq!(poll_once(&mut future, &waker), Poll::Ready(7));
        assert_eq!(pool.in_use(), 0);
    }

    #[test]
    fn supplemental_guards_follow_local_and_sync_provider_cells_after_observer_drop() {
        struct Ordered(Arc<Mutex<Vec<&'static str>>>, &'static str);
        impl Drop for Ordered {
            fn drop(&mut self) {
                self.0.lock().unwrap().push(self.1);
            }
        }
        for local in [false, true] {
            let order = Arc::new(Mutex::new(Vec::new()));
            if local {
                let (mut response, cell) = LocalOperation::pending(None);
                response.attach_completion_guard(Arc::new(Ordered(order.clone(), "first")));
                response.attach_completion_guard(Arc::new(Ordered(order.clone(), "second")));
                drop(response);
                assert!(order.lock().unwrap().is_empty());
                cell.complete(Ordered(order.clone(), "output"));
                assert!(order.lock().unwrap().is_empty());
                drop(cell);
            } else {
                let (mut response, responder) = SyncOperation::channel();
                response.attach_completion_guard(Arc::new(Ordered(order.clone(), "first")));
                response.attach_completion_guard(Arc::new(Ordered(order.clone(), "second")));
                drop(response);
                assert!(order.lock().unwrap().is_empty());
                responder.complete(Ordered(order.clone(), "output"));
            }
            assert_eq!(*order.lock().unwrap(), ["output", "first", "second"]);
        }
    }
    #[test]
    fn supplemental_guard_release_runs_outside_lock_and_consumed_attachment_is_immediate() {
        struct Reenter {
            cell: std::sync::Weak<SyncCell<usize>>,
            count: Arc<AtomicUsize>,
        }
        impl Drop for Reenter {
            fn drop(&mut self) {
                if let Some(cell) = self.cell.upgrade() {
                    assert!(
                        cell.inner.try_lock().is_ok(),
                        "supplemental guard dropped under lock"
                    );
                }
                self.count.fetch_add(1, Ordering::SeqCst);
            }
        }
        let (mut response, responder) = SyncOperation::channel();
        let count = Arc::new(AtomicUsize::new(0));
        let guard = || {
            Arc::new(Reenter {
                cell: Arc::downgrade(&responder.cell),
                count: count.clone(),
            })
        };
        response.attach_completion_guard(guard());
        response.attach_completion_guard(guard());
        responder.cell.complete(7);
        assert_eq!(count.load(Ordering::SeqCst), 0);
        assert_eq!(poll_once(&mut response, Waker::noop()), Poll::Ready(7));
        assert_eq!(count.load(Ordering::SeqCst), 2);
        response.attach_completion_guard(guard());
        assert_eq!(count.load(Ordering::SeqCst), 3);
    }

    #[test]
    fn external_sync_guard_survives_abandonment_until_terminal_output_is_destroyed() {
        struct OrderedDrop(Arc<Mutex<Vec<&'static str>>>, &'static str);
        impl Drop for OrderedDrop {
            fn drop(&mut self) {
                self.0.lock().unwrap().push(self.1);
            }
        }
        let order = Arc::new(Mutex::new(Vec::new()));
        let (response, responder) =
            SyncOperation::channel_with_guard(OrderedDrop(order.clone(), "guard"));
        drop(response);
        assert!(order.lock().unwrap().is_empty());
        responder.complete(OrderedDrop(order.clone(), "output"));
        assert_eq!(*order.lock().unwrap(), ["output", "guard"]);
    }

    #[test]
    fn external_sync_guard_holds_ready_admission_and_drops_outside_the_cell_lock() {
        struct Reenter {
            cell: Arc<Mutex<Option<std::sync::Weak<SyncCell<usize>>>>>,
            released: Arc<AtomicUsize>,
        }
        impl Drop for Reenter {
            fn drop(&mut self) {
                let cell = self
                    .cell
                    .lock()
                    .unwrap()
                    .as_ref()
                    .unwrap()
                    .upgrade()
                    .unwrap();
                assert!(
                    cell.inner.try_lock().is_ok(),
                    "guard dropped under completion lock"
                );
                self.released.fetch_add(1, Ordering::SeqCst);
            }
        }
        let cell = Arc::new(Mutex::new(None));
        let released = Arc::new(AtomicUsize::new(0));
        let (mut response, responder) = SyncOperation::channel_with_guard(Reenter {
            cell: cell.clone(),
            released: released.clone(),
        });
        *cell.lock().unwrap() = Some(Arc::downgrade(&response.cell));
        responder.complete(7);
        assert_eq!(released.load(Ordering::SeqCst), 0);
        let (waker, _) = counting_waker();
        assert_eq!(poll_once(&mut response, &waker), Poll::Ready(7));
        assert_eq!(released.load(Ordering::SeqCst), 1);
        drop(response);
        assert_eq!(released.load(Ordering::SeqCst), 1);
    }

    #[test]
    #[should_panic(expected = "polled after completion")]
    fn sync_response_polled_after_completion_panics() {
        let mut future = SyncOperation::ready(7);
        let (waker, _wakes) = counting_waker();
        assert_eq!(poll_once(&mut future, &waker), Poll::Ready(7));
        let _ = poll_once(&mut future, &waker);
    }
}

#[cfg(test)]
mod metrics_lifecycle_tests {
    use super::*;
    #[test]
    fn native_metrics_follow_consumed_abandoned_and_unpublished_cells() {
        let metrics = Arc::new(CompletionMetrics::default());
        let (mut response, provider) =
            SyncOperation::channel_with_guard_and_metrics((), Some(metrics.clone()));
        assert_eq!(metrics.snapshot().pending, 1);
        provider.complete(42);
        assert_eq!(metrics.snapshot().undrained, 1);
        assert_eq!(
            Pin::new(&mut response).poll(&mut Context::from_waker(Waker::noop())),
            Poll::Ready(42)
        );
        assert_eq!(metrics.snapshot().consumed, 1);
        drop(response);
        let (response, provider) =
            SyncOperation::channel_with_guard_and_metrics((), Some(metrics.clone()));
        drop(response);
        assert_eq!(metrics.snapshot().pending, 1);
        provider.complete(7);
        let snapshot = metrics.snapshot();
        assert_eq!(
            (snapshot.pending, snapshot.undrained, snapshot.abandoned),
            (0, 0, 1)
        );
        assert_eq!(snapshot.drain_delay_ns.iter().sum::<u64>(), 2);
        let (response, provider) =
            SyncOperation::<()>::channel_with_guard_and_metrics((), Some(metrics.clone()));
        drop(provider);
        assert_eq!(metrics.snapshot().pending, 1);
        drop(response);
        let snapshot = metrics.snapshot();
        assert_eq!(
            (
                snapshot.observed_operations,
                snapshot.published,
                snapshot.unpublished_discarded
            ),
            (3, 2, 1)
        );
        assert_eq!((snapshot.pending, snapshot.undrained), (0, 0));
        assert!(!snapshot.overflowed);
    }
    #[test]
    fn concurrent_publication_and_poll_conserve_counts() {
        let metrics = Arc::new(CompletionMetrics::default());
        for _ in 0..64 {
            let (mut response, provider) =
                SyncOperation::channel_with_guard_and_metrics((), Some(metrics.clone()));
            let thread = std::thread::spawn(move || provider.complete(42));
            loop {
                if Pin::new(&mut response)
                    .poll(&mut Context::from_waker(Waker::noop()))
                    .is_ready()
                {
                    break;
                }
                std::thread::yield_now();
            }
            thread.join().unwrap();
        }
        let snapshot = metrics.snapshot();
        assert_eq!(
            (
                snapshot.observed_operations,
                snapshot.published,
                snapshot.consumed
            ),
            (64, 64, 64)
        );
        assert_eq!(
            (snapshot.pending, snapshot.undrained, snapshot.abandoned),
            (0, 0, 0)
        );
    }
}
