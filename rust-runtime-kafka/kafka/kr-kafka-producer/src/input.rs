//! Native producer-owned input allocations. Mutation is exclusive before commit;
//! committed ranges share the original allocation through encoder/provider work.
//! Last-owner release publishes into a passive, preallocated event queue.
pub(crate) mod memory;

use crate::{
    config::ProducerConfig,
    credit::{Claim, CreditError, HeldCredits, Resource, SharedCredits},
    lifecycle::EventEnvelope,
    pool::{Pool, Slot},
    types::{Event, LeaseId, TopicHandle},
};
use kr_runtime::RuntimeDuration;
use kr_shared_bytes::SharedBytes;
use std::{
    collections::VecDeque,
    fmt,
    ops::Range,
    sync::{
        Arc, Mutex, MutexGuard,
        atomic::{AtomicUsize, Ordering},
    },
    task::Waker,
};

#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum InputError {
    Credit(CreditError),
    ResourceExhausted {
        resource: &'static str,
        limit: usize,
    },
    AllocationFailed,
    InvalidCapacity,
    InvalidOwner,
    InvalidUsed {
        used: u32,
        capacity: usize,
    },
    InvalidRange {
        range: Range<u32>,
        len: usize,
    },
    StaleLease {
        lease: LeaseId,
    },
    NotCommitted,
    Closed,
}
impl fmt::Display for InputError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Credit(error) => error.fmt(f),
            Self::ResourceExhausted { resource, limit } => {
                write!(f, "{resource} exhausted (limit {limit})")
            }
            Self::AllocationFailed => f.write_str("input allocation failed"),
            Self::InvalidCapacity => f.write_str("native input capacity must be nonzero"),
            Self::InvalidOwner => {
                f.write_str("registered input must be uniquely owned and unguarded")
            }
            Self::InvalidUsed { used, capacity } => {
                write!(f, "committed length {used} exceeds capacity {capacity}")
            }
            Self::InvalidRange { range, len } => {
                write!(f, "input range {range:?} exceeds committed length {len}")
            }
            Self::StaleLease { lease } => write!(f, "stale input lease {}", lease.0),
            Self::NotCommitted => f.write_str("input lease has not been committed"),
            Self::Closed => f.write_str("input admission is closed"),
        }
    }
}
impl std::error::Error for InputError {}
impl From<CreditError> for InputError {
    fn from(error: CreditError) -> Self {
        Self::Credit(error)
    }
}

/// Null and empty remain distinct. Every non-null range addresses the committed
/// length of one explicitly supplied native lease, never an arbitrary pointer.
#[derive(Clone, Debug)]
pub struct LeasedHeader {
    pub key: Range<u32>,
    pub value: Option<Range<u32>>,
}

#[derive(Clone, Debug)]
pub struct LeasedRecordDescriptor<'a> {
    pub topic: TopicHandle,
    pub partition_hint: Option<i32>,
    pub lane_hint: Option<u8>,
    pub key: Option<Range<u32>>,
    pub value: Option<Range<u32>>,
    pub headers: &'a [LeasedHeader],
    pub timestamp_ms: i64,
    pub user_token: u64,
    pub delivery_timeout: Option<RuntimeDuration>,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct InputStatus {
    /// Slots remain live until their allocation actually releases and the owner
    /// drains the pending notification. Generation reuse cannot precede that.
    pub live: usize,
    pub acquired: usize,
    pub committed: usize,
    pub released_waiting: usize,
    pub pending_release_events: usize,
    /// Whole native allocations stay charged to their acquisition lane even
    /// when records subsequently route elsewhere. Diagnostic concurrent snapshot.
    pub allocation_bytes_by_lane: [usize; 4],
    pub closed: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Phase {
    Acquired,
    Committed,
    Released,
}

struct LeaseSlot {
    phase: Phase,
    acquired: bool,
    bytes: Option<SharedBytes>,
}
struct State {
    slots: Pool<LeaseSlot>,
    close_cursor: usize,
    close_end: usize,
    acquired: usize,
    committed: usize,
    released_waiting: usize,
    closed: bool,
}
struct ReleaseQueue {
    queue: Mutex<VecDeque<EventEnvelope>>,
    limit: usize,
    allocation_bytes_by_lane: [AtomicUsize; 4],
    wake: Mutex<ReleaseWakeState>,
}

#[derive(Default)]
struct ReleaseWakeState {
    waker: Option<Arc<Waker>>,
    deferred: usize,
    pending: bool,
}
impl ReleaseQueue {
    fn notify(&self) {
        let wake = {
            let mut state = lock(&self.wake);
            state.pending = true;
            if state.deferred == 0 {
                state.pending = false;
                state.waker.take()
            } else {
                None
            }
        };
        schedule_release(wake);
    }
}

fn schedule_release(wake: Option<Arc<Waker>>) {
    if let Some(wake) = wake {
        // Publication is already committed. Keep both notification and final
        // waker destruction behind separate unwind boundaries so a hostile
        // callback/panic-payload destructor cannot escape provider/FFI cleanup.
        kr_runtime::contain_panic(|| wake.wake_by_ref());
        kr_runtime::contain_panic(|| drop(wake));
    }
}

/// Outermost client transactions retain this until after unlocking admission.
/// Deferrals compose across nested/concurrent transactions without losing a
/// publication that occurs while wake delivery is suppressed.
pub(crate) struct ReleaseWakeDeferral {
    released: Arc<ReleaseQueue>,
}
impl Drop for ReleaseWakeDeferral {
    fn drop(&mut self) {
        let wake = {
            let mut state = lock(&self.released.wake);
            state.deferred -= 1;
            if state.deferred == 0 && state.pending {
                state.pending = false;
                state.waker.take()
            } else {
                None
            }
        };
        schedule_release(wake);
    }
}

/// Work spent removing registry references after the immediate close fence.
/// Outstanding caller/provider owners do not constitute remaining close work.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct InputCloseProgress {
    pub visited_slots: usize,
    pub released_leases: usize,
    pub remaining: bool,
}

impl State {
    fn release_slot(
        &mut self,
        key: Slot<LeaseSlot>,
        acquired_finished: bool,
    ) -> Option<SharedBytes> {
        let slot = self.slots.get_mut(key)?;
        if slot.phase != Phase::Released {
            self.committed -= usize::from(slot.phase == Phase::Committed);
            self.released_waiting += 1;
            slot.phase = Phase::Released;
        }
        if acquired_finished && slot.acquired {
            slot.acquired = false;
            self.acquired -= 1;
        }
        slot.bytes.take()
    }
}

/// Cloneable immutable-buffer registry shared with the client admission lock.
/// Foreign-memory validity is established in an audited binding before it
/// constructs the safe shared owner passed to `register_shared`.
#[derive(Clone)]
pub struct InputLeases {
    state: Arc<Mutex<State>>,
    released: Arc<ReleaseQueue>,
    credits: SharedCredits,
    lanes: u8,
}
impl fmt::Debug for InputLeases {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("InputLeases")
            .field("status", &self.status())
            .finish()
    }
}

impl InputLeases {
    pub(crate) fn belongs_to(&self, authority: &SharedCredits) -> bool {
        self.credits.same_authority(authority)
    }
    /// # Errors
    /// Rejects invalid pool limits and preallocation failure before publication.
    /// Fixed registry reservations with excess capacity are also rejected.
    pub fn new(config: &ProducerConfig, credits: SharedCredits) -> Result<Self, InputError> {
        let limit = config.max_live_leases as usize;
        let release_limit = config.release_event_capacity as usize;
        if limit == 0 || release_limit < limit || !(1..=4).contains(&config.lanes) {
            return Err(InputError::InvalidCapacity);
        }
        let slots = Pool::new(limit).map_err(|_| InputError::AllocationFailed)?;
        let queue =
            crate::fixed::try_deque(release_limit).map_err(|_| InputError::AllocationFailed)?;
        Ok(Self {
            state: Arc::new(Mutex::new(State {
                slots,
                close_cursor: 0,
                close_end: 0,
                acquired: 0,
                committed: 0,
                released_waiting: 0,
                closed: false,
            })),
            released: Arc::new(ReleaseQueue {
                queue: Mutex::new(queue),
                limit: release_limit,
                allocation_bytes_by_lane: [const { AtomicUsize::new(0) }; 4],
                wake: Mutex::new(ReleaseWakeState::default()),
            }),
            credits,
            lanes: config.lanes,
        })
    }

    /// Acquires the entire retained capacity and one eventual release-event
    /// credit. Zero initialization and Arc conversion occur before exposing a
    /// writable buffer; commit therefore never copies serialized bytes.
    ///
    /// # Errors
    /// Admission rejection leaves every pool and generation unchanged.
    pub fn acquire(&self, bytes: u32, lane: u8) -> Result<InputBuffer, InputError> {
        if bytes == 0 {
            return Err(InputError::InvalidCapacity);
        }
        if lane >= self.lanes {
            return Err(CreditError::InvalidLane {
                lane,
                lanes: self.lanes,
            }
            .into());
        }
        let mut state = lock(&self.state);
        if state.closed {
            return Err(InputError::Closed);
        }
        if state.slots.len() == state.slots.limit() {
            return Err(InputError::ResourceExhausted {
                resource: "input leases",
                limit: state.slots.limit(),
            });
        }
        let mut credit = self.credits.reserve(&[
            Claim {
                resource: Resource::InputBytes,
                amount: bytes as usize,
                lane,
            },
            Claim {
                resource: Resource::ReleaseEvents,
                amount: 1,
                lane,
            },
        ])?;
        let mut allocation = Vec::new();
        allocation
            .try_reserve_exact(bytes as usize)
            .map_err(|_| InputError::AllocationFailed)?;
        allocation.resize(bytes as usize, 0);
        let allocation = SharedBytes::from(allocation);
        let key = state
            .slots
            .insert(LeaseSlot {
                phase: Phase::Acquired,
                acquired: true,
                bytes: None,
            })
            .map_err(|_| InputError::ResourceExhausted {
                resource: "input leases",
                limit: state.slots.limit(),
            })?;
        state.acquired += 1;
        self.released.allocation_bytes_by_lane[usize::from(lane)]
            .fetch_add(bytes as usize, Ordering::Relaxed);
        drop(state);
        let lease = LeaseId(key.packed());
        let guard = Arc::new(AllocationRelease {
            input: Some(credit.take(Resource::InputBytes)),
            event: Some(credit),
            released: self.released.clone(),
            lease,
            lane,
            capacity: bytes as usize,
        });
        let bytes = allocation
            .attach_guard(guard)
            .expect("fresh native allocation has no prior guard");
        Ok(InputBuffer {
            state: self.state.clone(),
            lease,
            bytes: Some(bytes),
        })
    }

    /// Registers immutable storage without copying. The supplied view must be
    /// the unique strong owner and have no preexisting lifetime guard, so every
    /// later view carries this lease's release obligation. Full backing capacity
    /// is charged once to the registering lane until the last producer view
    /// retires after `release` or closure. Failure creates no release obligation.
    ///
    /// # Errors
    /// Rejects shared/guarded owners, invalid sizes, closure and exhausted pools.
    pub fn register_shared(&self, bytes: SharedBytes, lane: u8) -> Result<LeaseId, InputError> {
        if bytes.is_empty() || bytes.len() > u32::MAX as usize {
            return Err(InputError::InvalidCapacity);
        }
        if bytes.has_guard() || bytes.strong_count() != 1 {
            return Err(InputError::InvalidOwner);
        }
        if lane >= self.lanes {
            return Err(CreditError::InvalidLane {
                lane,
                lanes: self.lanes,
            }
            .into());
        }
        let capacity = bytes.retained_capacity();
        let mut state = lock(&self.state);
        if state.closed {
            return Err(InputError::Closed);
        }
        if state.slots.len() == state.slots.limit() {
            return Err(InputError::ResourceExhausted {
                resource: "input leases",
                limit: state.slots.limit(),
            });
        }
        let mut credit = self.credits.reserve(&[
            Claim {
                resource: Resource::InputBytes,
                amount: capacity,
                lane,
            },
            Claim {
                resource: Resource::ReleaseEvents,
                amount: 1,
                lane,
            },
        ])?;
        let key = state
            .slots
            .insert(LeaseSlot {
                phase: Phase::Committed,
                acquired: false,
                bytes: None,
            })
            .map_err(|_| InputError::ResourceExhausted {
                resource: "input leases",
                limit: state.slots.limit(),
            })?;
        state.committed += 1;
        self.released.allocation_bytes_by_lane[usize::from(lane)]
            .fetch_add(capacity, Ordering::Relaxed);
        let lease = LeaseId(key.packed());
        let guard = Arc::new(AllocationRelease {
            input: Some(credit.take(Resource::InputBytes)),
            event: Some(credit),
            released: self.released.clone(),
            lease,
            lane,
            capacity,
        });
        let bytes = bytes
            .attach_guard(guard)
            .expect("unguarded registration checked before admission");
        state
            .slots
            .get_mut(key)
            .expect("newly reserved lease slot")
            .bytes = Some(bytes);
        Ok(lease)
    }

    /// Prevents future submissions using this generation. Existing encoder or
    /// provider spans remain immutable and charged until their last owner drops.
    ///
    /// # Errors
    /// Rejects stale or already released IDs without touching any replacement.
    pub fn release(&self, lease: LeaseId) -> Result<(), InputError> {
        let bytes = {
            let mut state = lock(&self.state);
            let slot = state
                .slots
                .get_mut(Slot::from_packed(lease.0))
                .ok_or(InputError::StaleLease { lease })?;
            if slot.phase == Phase::Released {
                return Err(InputError::StaleLease { lease });
            }
            state.release_slot(Slot::from_packed(lease.0), false)
        };
        drop(bytes);
        Ok(())
    }

    /// Returns a retained immutable view for owner-side routing or inspection.
    /// This view itself delays InputReleased; later admission still revalidates
    /// the lease ID, so a retained view cannot revive a released generation.
    ///
    /// # Errors
    /// Rejects uncommitted, stale/released or out-of-range input.
    pub fn view(&self, lease: LeaseId, range: Range<u32>) -> Result<SharedBytes, InputError> {
        checked_slice(&self.snapshot(lease)?, range)
    }

    pub(crate) fn snapshot(&self, lease: LeaseId) -> Result<SharedBytes, InputError> {
        let state = lock(&self.state);
        if state.closed {
            return Err(InputError::Closed);
        }
        let slot = state
            .slots
            .get(Slot::from_packed(lease.0))
            .ok_or(InputError::StaleLease { lease })?;
        match slot.phase {
            Phase::Committed => Ok(slot
                .bytes
                .as_ref()
                .expect("committed slot owns its input")
                .clone()),
            Phase::Acquired => Err(InputError::NotCommitted),
            Phase::Released => Err(InputError::StaleLease { lease }),
        }
    }

    /// Immediately fences acquisition, commit and submission in O(1). Registry
    /// references are released by explicit `close_step` maintenance. Callers and
    /// providers retain their physical owners; Closed still requires live == 0.
    pub fn close(&self) {
        let mut state = lock(&self.state);
        if !state.closed {
            state.closed = true;
            state.close_end = if state.slots.is_empty() {
                0
            } else {
                state.slots.allocated_slots()
            };
        }
    }

    /// Whether the finite registry sweep still has slots to visit. This is false
    /// once the sweep finishes even if caller/provider references remain live.
    #[must_use]
    pub fn has_close_work(&self) -> bool {
        let state = lock(&self.state);
        state.closed && !state.slots.is_empty() && state.close_cursor < state.close_end
    }

    /// Visits at most `maximum` materialized slots, charging holes as work. Every
    /// allocation owner is dropped outside the state lock. Zero does no work;
    /// before `close` this is inert. Event removal and provider release may occur
    /// between visits without invalidating the monotonic cursor.
    pub fn close_step(&self, maximum: usize) -> InputCloseProgress {
        let mut progress = InputCloseProgress::default();
        while progress.visited_slots < maximum {
            let bytes = {
                let mut state = lock(&self.state);
                if !state.closed || state.slots.is_empty() || state.close_cursor == state.close_end
                {
                    break;
                }
                let index = state.close_cursor;
                state.close_cursor += 1;
                progress.visited_slots += 1;
                state.slots.key_at(index).and_then(|key| {
                    progress.released_leases += usize::from(
                        state.slots.get(key).expect("live indexed slot").phase != Phase::Released,
                    );
                    state.release_slot(key, false)
                })
            };
            drop(bytes);
        }
        progress.remaining = self.has_close_work();
        progress
    }

    /// Register once per owner poll, then drain/recheck release events. The waker
    /// is consumed by publication and should only schedule owner work. Provider
    /// final-reference drops can invoke it. The notifying call holds no registry
    /// lock; client transactions defer delivery until their admission lock has
    /// been released (unrelated concurrent callers may acquire it subsequently).
    /// Callback and panic-payload unwinds are contained after event publication.
    /// Registration changes clone/drop wakers outside registry locks.
    pub fn register_release_waker(&self, waker: &Waker) {
        if lock(&self.released.wake)
            .waker
            .as_ref()
            .is_some_and(|old| old.will_wake(waker))
        {
            return;
        }
        let replacement = Arc::new(waker.clone());
        let old = lock(&self.released.wake).waker.replace(replacement);
        kr_runtime::contain_panic(|| drop(old));
    }

    pub(crate) fn defer_release_wakes(&self) -> ReleaseWakeDeferral {
        let mut state = lock(&self.released.wake);
        state.deferred = state
            .deferred
            .checked_add(1)
            .expect("live Arc-owned deferrals fit usize");
        ReleaseWakeDeferral {
            released: self.released.clone(),
        }
    }

    /// Detach the owner scheduler during actor destruction. Provider-retained
    /// queues still publish release events, but no longer retain its waker.
    pub fn clear_release_waker(&self) {
        let old = lock(&self.released.wake).waker.take();
        kr_runtime::contain_panic(|| drop(old));
    }

    /// Moves a terminal lease notification to the engine's event queue, retaining
    /// its release-event guard until the application drains that queue.
    #[must_use]
    pub fn pop_released(&self) -> Option<EventEnvelope> {
        let event = lock(&self.released.queue).pop_front()?;
        let Event::InputReleased { lease } = event.event else {
            unreachable!("private queue contains only input release events")
        };
        let mut state = lock(&self.state);
        let removed = state
            .slots
            .remove(Slot::from_packed(lease.0))
            .expect("a live native allocation retains its generation until notification");
        debug_assert_eq!(removed.phase, Phase::Released);
        debug_assert!(!removed.acquired);
        state.released_waiting -= 1;
        Some(event)
    }

    #[must_use]
    pub fn status(&self) -> InputStatus {
        let state = lock(&self.state);
        let mut status = InputStatus {
            live: state.slots.len(),
            acquired: state.acquired,
            committed: state.committed,
            released_waiting: state.released_waiting,
            closed: state.closed,
            allocation_bytes_by_lane: std::array::from_fn(|lane| {
                self.released.allocation_bytes_by_lane[lane].load(Ordering::Acquire)
            }),
            ..InputStatus::default()
        };
        drop(state);
        status.pending_release_events = lock(&self.released.queue).len();
        status
    }
}

/// Exclusive serializer access before commit. Dropping an uncommitted buffer
/// cancels its lease and eventually emits exactly one InputReleased event.
pub struct InputBuffer {
    state: Arc<Mutex<State>>,
    lease: LeaseId,
    bytes: Option<SharedBytes>,
}
impl fmt::Debug for InputBuffer {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("InputBuffer")
            .field("lease", &self.lease)
            .field("capacity", &self.capacity())
            .finish()
    }
}
#[derive(Debug)]
pub struct CommitFailure {
    pub error: InputError,
    pub buffer: InputBuffer,
}

impl InputBuffer {
    #[must_use]
    pub const fn lease_id(&self) -> LeaseId {
        self.lease
    }
    #[must_use]
    pub fn capacity(&self) -> usize {
        self.bytes.as_ref().map_or(0, SharedBytes::len)
    }
    #[must_use]
    pub fn as_mut_slice(&mut self) -> &mut [u8] {
        self.bytes
            .as_mut()
            .expect("uncommitted buffer owns its allocation")
            .try_as_mut()
            .expect("mutable native buffer has never published a clone")
    }
    /// Freezes the used prefix without copying and relinquishes mutable access.
    /// Retained-capacity accounting includes the unused suffix until release.
    ///
    /// # Errors
    /// Returns the same still-owned mutable buffer for invalid used length,
    /// closure, or release racing this commit.
    pub fn commit(mut self, used: u32) -> Result<LeaseId, CommitFailure> {
        let error = if used as usize > self.capacity() {
            Some(InputError::InvalidUsed {
                used,
                capacity: self.capacity(),
            })
        } else {
            let mut state = lock(&self.state);
            if state.closed {
                Some(InputError::Closed)
            } else {
                match state.slots.get_mut(Slot::from_packed(self.lease.0)) {
                    Some(slot) if slot.phase == Phase::Acquired => {
                        let bytes = self.bytes.take().expect("uncommitted buffer owns input");
                        slot.bytes = Some(
                            bytes
                                .slice(0..used as usize)
                                .expect("used length checked against native capacity"),
                        );
                        slot.phase = Phase::Committed;
                        slot.acquired = false;
                        state.acquired -= 1;
                        state.committed += 1;
                        None
                    }
                    _ => Some(InputError::StaleLease { lease: self.lease }),
                }
            }
        };
        match error {
            Some(error) => Err(CommitFailure {
                error,
                buffer: self,
            }),
            None => Ok(self.lease),
        }
    }
}
impl Drop for InputBuffer {
    fn drop(&mut self) {
        if self.bytes.is_none() {
            return;
        }
        {
            let mut state = lock(&self.state);
            let bytes = state.release_slot(Slot::from_packed(self.lease.0), true);
            debug_assert!(bytes.is_none(), "uncommitted buffer owns the allocation");
        }
        // The field's later drop releases allocation before its shared guard.
    }
}

struct AllocationRelease {
    input: Option<HeldCredits>,
    event: Option<HeldCredits>,
    released: Arc<ReleaseQueue>,
    lease: LeaseId,
    lane: u8,
    capacity: usize,
}
impl Drop for AllocationRelease {
    fn drop(&mut self) {
        self.released.allocation_bytes_by_lane[usize::from(self.lane)]
            .fetch_sub(self.capacity, Ordering::AcqRel);
        drop(self.input.take());
        let event = EventEnvelope::new(
            Event::InputReleased { lease: self.lease },
            self.event
                .take()
                .expect("one release event was reserved at acquisition"),
        )
        .expect("native input owns exactly one release credit");
        let mut queue = lock(&self.released.queue);
        // Every queue entry retains a release-event credit. Acquiring this
        // allocation reserved one too, so this preallocated push cannot grow.
        debug_assert!(queue.len() < self.released.limit);
        queue.push_back(event);
        drop(queue);
        self.released.notify();
    }
}

pub(crate) fn checked_slice(
    bytes: &SharedBytes,
    range: Range<u32>,
) -> Result<SharedBytes, InputError> {
    bytes
        .slice(range.start as usize..range.end as usize)
        .map_err(|_| InputError::InvalidRange {
            range,
            len: bytes.len(),
        })
}
fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

#[cfg(test)]
mod tests;
