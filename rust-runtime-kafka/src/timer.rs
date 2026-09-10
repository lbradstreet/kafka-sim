//! Timer registration state and the deadline-ordered timer store.

use crate::handle::RuntimeHandle;
use crate::task::TaskId;
use crate::time::{RuntimeInstant, TimeError};
use std::cell::{Cell, RefCell};
use std::collections::BTreeMap;
use std::future::Future;
use std::pin::Pin;
use std::rc::{Rc, Weak as RcWeak};
use std::task::{Context, Poll, Waker};

/// A stable identifier for a timer scoped to one runtime instance.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct TimerId(u64);

impl TimerId {
    pub(crate) const fn from_u64(value: u64) -> Self {
        Self(value)
    }

    /// Returns the numeric timer identifier.
    #[must_use]
    pub const fn get(self) -> u64 {
        self.0
    }
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct TimerKey {
    deadline: RuntimeInstant,
    sequence: u64,
}

#[derive(Clone, Copy, Eq, PartialEq)]
enum TimerStatus {
    Pending,
    Fired,
    Cancelled,
    Stopped,
}

pub(crate) struct TimerRegistration {
    id: TimerId,
    task: Cell<TaskId>,
    key: TimerKey,
    status: Cell<TimerStatus>,
    waker: RefCell<Option<Waker>>,
}

impl TimerRegistration {
    fn new(id: TimerId, task: TaskId, key: TimerKey, waker: &Waker) -> Self {
        Self {
            id,
            task: Cell::new(task),
            key,
            status: Cell::new(TimerStatus::Pending),
            waker: RefCell::new(Some(waker.clone())),
        }
    }

    pub(crate) const fn id(&self) -> TimerId {
        self.id
    }

    pub(crate) fn task(&self) -> TaskId {
        self.task.get()
    }

    fn set_waiter(&self, task: TaskId, waker: &Waker) {
        self.set_waiter_with(task, waker, Waker::clone);
    }

    fn set_waiter_with(
        &self,
        task: TaskId,
        waker: &Waker,
        clone_waker: impl FnOnce(&Waker) -> Waker,
    ) {
        let unchanged = self
            .waker
            .borrow()
            .as_ref()
            .is_some_and(|stored| stored.will_wake(waker));
        let previous = if unchanged {
            None
        } else {
            // Clone before committing either field: if user code panics, the
            // previous waiter and its attribution must remain paired.
            let replacement = clone_waker(waker);
            self.waker.borrow_mut().replace(replacement)
        };
        self.task.set(task);
        // A replaced waker may run arbitrary code on drop. Publish the new
        // waiter first and release the cell borrow before disposing of it.
        crate::task::drop_value_caught(previous);
    }

    fn fire(&self) -> Option<Waker> {
        if self.status.get() != TimerStatus::Pending {
            return None;
        }
        self.status.set(TimerStatus::Fired);
        self.waker.borrow_mut().take()
    }

    pub(crate) fn cancel(&self) -> bool {
        if self.status.get() != TimerStatus::Pending {
            return false;
        }
        self.status.set(TimerStatus::Cancelled);
        true
    }

    fn stop(&self) -> Option<Waker> {
        if self.status.get() != TimerStatus::Pending {
            return None;
        }
        self.status.set(TimerStatus::Stopped);
        self.waker.borrow_mut().take()
    }

    fn is_pending(&self) -> bool {
        self.status.get() == TimerStatus::Pending
    }

    pub(crate) fn is_fired(&self) -> bool {
        self.status.get() == TimerStatus::Fired
    }

    pub(crate) fn is_stopped(&self) -> bool {
        self.status.get() == TimerStatus::Stopped
    }
}

pub(crate) struct TimerStore {
    max_timers: usize,
    next_timer_sequence: u64,
    next_timer_id: u64,
    timers: BTreeMap<TimerKey, RcWeak<TimerRegistration>>,
}

/// A runtime timer future created by a simulation or host runtime handle.
///
/// While its runtime is active, polling from outside one of that runtime's
/// tasks returns [`TimeError::WrongRuntime`]. After shutdown, a timer that was
/// already eligible (or had fired) succeeds and any other timer returns
/// [`TimeError::RuntimeStopped`]. Registration failures are reported when the
/// future is polled.
///
/// A pending sleep may move between tasks in the same runtime. Each pending
/// poll installs that task as its waiter; firing, cancellation, and shutdown
/// diagnostics identify the latest waiter. Its initial scheduling event keeps
/// the task that first registered the timer.
pub struct Sleep {
    route: RuntimeHandle,
    deadline: Result<RuntimeInstant, TimeError>,
    created_while_running: bool,
    registration: Option<Rc<TimerRegistration>>,
}

impl Sleep {
    pub(crate) fn new(route: RuntimeHandle, deadline: Result<RuntimeInstant, TimeError>) -> Self {
        let created_while_running = !route.is_stopped();
        Self {
            route,
            deadline,
            created_while_running,
            registration: None,
        }
    }
}

impl Future for Sleep {
    type Output = Result<(), TimeError>;

    fn poll(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        if self.route.is_stopped() {
            if (self.created_while_running
                && self
                    .deadline
                    .is_ok_and(|deadline| self.route.now() >= deadline))
                || self
                    .registration
                    .as_ref()
                    .is_some_and(|registration| registration.is_fired())
            {
                return Poll::Ready(Ok(()));
            }
            return Poll::Ready(Err(TimeError::RuntimeStopped));
        }
        let Some(task) = self.route.current_task() else {
            return Poll::Ready(Err(TimeError::WrongRuntime));
        };
        let deadline = match self.deadline {
            Ok(deadline) => deadline,
            Err(error) => return Poll::Ready(Err(error)),
        };
        if self.route.now() >= deadline {
            return Poll::Ready(Ok(()));
        }

        if let Some(registration) = &self.registration {
            if registration.is_fired() {
                return Poll::Ready(Ok(()));
            }
            if registration.is_stopped() {
                return Poll::Ready(Err(TimeError::RuntimeStopped));
            }
            registration.set_waiter(task, context.waker());
            return Poll::Pending;
        }

        match self.route.register_timer(task, deadline, context.waker()) {
            Ok(registration) => {
                self.registration = Some(registration);
                Poll::Pending
            }
            Err(error) => Poll::Ready(Err(error)),
        }
    }
}

impl Drop for Sleep {
    fn drop(&mut self) {
        if let Some(registration) = &self.registration
            && registration.cancel()
        {
            self.route.cancel_timer(registration);
        }
    }
}

impl TimerStore {
    pub(crate) fn new(max_timers: usize) -> Self {
        Self {
            max_timers,
            next_timer_sequence: 0,
            next_timer_id: 0,
            timers: BTreeMap::new(),
        }
    }

    pub(crate) fn register(
        &mut self,
        task: TaskId,
        deadline: RuntimeInstant,
        waker: &Waker,
    ) -> Result<Rc<TimerRegistration>, TimeError> {
        if self.timers.len() >= self.max_timers {
            return Err(TimeError::ResourceExhausted {
                resource: "live timers",
                limit: self.max_timers,
            });
        }
        let id = TimerId::from_u64(self.next_timer_id);
        let next_timer_id = self
            .next_timer_id
            .checked_add(1)
            .ok_or(TimeError::TimerIdentifierExhausted)?;
        let sequence = self.next_timer_sequence;
        let next_timer_sequence = self
            .next_timer_sequence
            .checked_add(1)
            .ok_or(TimeError::TimerRegistrationSequenceExhausted)?;
        self.next_timer_id = next_timer_id;
        self.next_timer_sequence = next_timer_sequence;
        let key = TimerKey { deadline, sequence };
        let registration = Rc::new(TimerRegistration::new(id, task, key, waker));
        let previous = self.timers.insert(key, Rc::downgrade(&registration));
        debug_assert!(previous.is_none(), "timer sequences are unique");
        Ok(registration)
    }

    pub(crate) fn cancel(&mut self, registration: &TimerRegistration) -> bool {
        self.timers.remove(&registration.key).is_some()
    }

    pub(crate) fn next_deadline(&mut self) -> Option<RuntimeInstant> {
        loop {
            let (&key, registration) = self.timers.first_key_value()?;
            if registration
                .upgrade()
                .is_some_and(|registration| registration.is_pending())
            {
                return Some(key.deadline);
            }
            self.timers.pop_first();
        }
    }

    pub(crate) fn fire_deadline(
        &mut self,
        deadline: RuntimeInstant,
        mut on_fire: impl FnMut(TimerId, TaskId, Waker),
    ) {
        while self
            .timers
            .first_key_value()
            .is_some_and(|(key, _)| key.deadline == deadline)
        {
            let (_key, registration) = self.timers.pop_first().expect("timer was just observed");
            if let Some(registration) = registration.upgrade()
                && let Some(waker) = registration.fire()
            {
                on_fire(registration.id, registration.task(), waker);
            }
        }
    }

    pub(crate) fn stop_all(&mut self) -> Vec<(TaskId, Waker)> {
        let mut wakers = Vec::new();
        while let Some((_key, registration)) = self.timers.pop_first() {
            if let Some(registration) = registration.upgrade()
                && let Some(waker) = registration.stop()
            {
                wakers.push((registration.task(), waker));
            }
        }
        wakers
    }

    pub(crate) fn len(&self) -> usize {
        self.timers.len()
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.timers.is_empty()
    }

    pub(crate) const fn next_sequence(&self) -> u64 {
        self.next_timer_sequence
    }

    pub(crate) const fn next_id(&self) -> u64 {
        self.next_timer_id
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::panic::{AssertUnwindSafe, catch_unwind};
    use std::sync::Arc;
    use std::task::Wake;

    struct NoopWake;

    #[allow(
        clippy::manual_noop_waker,
        reason = "a distinct waker identity is required to exercise replacement cloning"
    )]
    impl Wake for NoopWake {
        fn wake(self: Arc<Self>) {}
    }

    #[test]
    fn failed_waiter_clone_preserves_the_previous_task_and_waker() {
        let mut timers = TimerStore::new(1);
        let original = TaskId::from_parts(0, 0);
        let original_waker = Waker::from(Arc::new(NoopWake));
        let registration = timers
            .register(original, RuntimeInstant::from_nanos(1), &original_waker)
            .expect("timer registers");

        let result = catch_unwind(AssertUnwindSafe(|| {
            registration.set_waiter_with(TaskId::from_parts(1, 0), Waker::noop(), |_| {
                panic!("replacement waker clone failed");
            });
        }));

        assert!(result.is_err());
        assert_eq!(registration.task(), original);
        assert!(
            registration
                .waker
                .borrow()
                .as_ref()
                .expect("original waker remains installed")
                .will_wake(&original_waker)
        );
        assert!(registration.is_pending());
        assert_eq!(timers.len(), 1);
    }

    #[test]
    fn replaced_waker_drop_panic_cannot_roll_back_the_new_waiter() {
        struct PanicOnDrop;

        #[allow(
            clippy::manual_noop_waker,
            reason = "an owned waker destructor is required to exercise replacement drop"
        )]
        impl Wake for PanicOnDrop {
            fn wake(self: Arc<Self>) {}
        }

        impl Drop for PanicOnDrop {
            fn drop(&mut self) {
                panic!("previous waker drop failed");
            }
        }

        let mut timers = TimerStore::new(1);
        let registration = timers
            .register(
                TaskId::from_parts(0, 0),
                RuntimeInstant::from_nanos(1),
                &Waker::from(Arc::new(PanicOnDrop)),
            )
            .expect("timer registers");
        let replacement = TaskId::from_parts(1, 0);

        registration.set_waiter(replacement, Waker::noop());

        assert_eq!(registration.task(), replacement);
        let stopped = timers.stop_all();
        assert_eq!(stopped.len(), 1);
        assert_eq!(stopped[0].0, replacement);
        assert!(stopped[0].1.will_wake(Waker::noop()));
    }

    #[test]
    fn dead_registration_is_counted_until_deadline_lookup_prunes_it() {
        let mut timers = TimerStore::new(1);
        let registration = timers
            .register(
                TaskId::from_parts(0, 0),
                RuntimeInstant::from_nanos(1),
                Waker::noop(),
            )
            .expect("timer registers");
        assert_eq!(timers.len(), 1);

        drop(registration);
        assert_eq!(timers.len(), 1, "the store retains the dead weak entry");
        assert_eq!(timers.next_deadline(), None);
        assert_eq!(timers.len(), 0);
    }

    #[test]
    fn equal_deadline_timers_fire_in_registration_order() {
        let mut timers = TimerStore::new(3);
        let deadline = RuntimeInstant::from_nanos(1);
        let registrations = (0..3)
            .map(|slot| {
                timers
                    .register(TaskId::from_parts(slot, 0), deadline, Waker::noop())
                    .expect("timer registers")
            })
            .collect::<Vec<_>>();
        let mut fired = Vec::new();

        timers.fire_deadline(deadline, |id, task, _waker| {
            fired.push((id.get(), task.slot()));
        });

        assert_eq!(fired, vec![(0, 0), (1, 1), (2, 2)]);
        assert!(timers.is_empty());
        assert!(registrations.iter().all(|timer| timer.is_fired()));
    }

    #[test]
    fn timer_identifier_exhaustion_does_not_mutate_registration_state() {
        let mut timers = TimerStore::new(usize::MAX);
        timers.next_timer_id = u64::MAX;
        timers.next_timer_sequence = 17;
        let waker = Waker::noop();

        let result = timers.register(
            TaskId::from_parts(0, 0),
            RuntimeInstant::from_nanos(1),
            waker,
        );

        assert!(matches!(result, Err(TimeError::TimerIdentifierExhausted)));
        assert_eq!(timers.next_timer_id, u64::MAX);
        assert_eq!(timers.next_timer_sequence, 17);
        assert!(timers.timers.is_empty());
    }

    #[test]
    fn timer_registration_sequence_exhaustion_does_not_mutate_registration_state() {
        let mut timers = TimerStore::new(usize::MAX);
        timers.next_timer_id = 17;
        timers.next_timer_sequence = u64::MAX;
        let waker = Waker::noop();

        let result = timers.register(
            TaskId::from_parts(0, 0),
            RuntimeInstant::from_nanos(1),
            waker,
        );

        assert!(matches!(
            result,
            Err(TimeError::TimerRegistrationSequenceExhausted)
        ));
        assert_eq!(timers.next_timer_id, 17);
        assert_eq!(timers.next_timer_sequence, u64::MAX);
        assert!(timers.timers.is_empty());
    }
}
