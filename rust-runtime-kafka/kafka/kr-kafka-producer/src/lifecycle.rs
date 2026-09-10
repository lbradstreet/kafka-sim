//! Bounded terminal watermarks and a single ordered event queue. Control-event
//! capacity is reserved separately, but control events cannot overtake deliveries.
use crate::{
    credit::{HeldCredits, Resource},
    types::{Event, RecordToken},
};
use std::{
    collections::{BTreeMap, VecDeque},
    fmt,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum LifecycleError {
    InvalidRange,
    DuplicateTerminal,
    UnknownToken,
    ResourceExhausted,
    AllocationFailed,
    MissingEventCredit,
}
impl fmt::Display for LifecycleError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "lifecycle error: {self:?}")
    }
}
impl std::error::Error for LifecycleError {}

/// Completed ranges are coalesced. Keeping one entry per completed token would
/// grow forever when one slow partition holds the oldest record while other
/// partitions continuously finish and reuse their descriptor credits.
#[derive(Debug)]
pub struct DeliveryTracker {
    accepted: u64,
    completed: u64,
    through: u64,
    live_limit: usize,
    ranges: BTreeMap<u64, u64>,
}
impl DeliveryTracker {
    #[must_use]
    pub fn new(live_limit: usize) -> Self {
        Self {
            accepted: 0,
            completed: 0,
            through: 0,
            live_limit,
            ranges: BTreeMap::new(),
        }
    }
    #[must_use]
    pub fn accepted(&self) -> RecordToken {
        RecordToken(self.accepted)
    }
    #[must_use]
    pub fn completed_through(&self) -> RecordToken {
        RecordToken(self.through)
    }
    #[must_use]
    pub fn live(&self) -> u64 {
        self.accepted - self.completed
    }
    #[must_use]
    pub fn completed_ranges(&self) -> usize {
        self.ranges.len()
    }
    /// Accepts a dense ordered token range after mailbox publication.
    /// # Errors
    /// Rejects gaps, overflow and more live obligations than descriptor capacity.
    pub fn accept(&mut self, first: RecordToken, count: u32) -> Result<(), LifecycleError> {
        if count == 0 || self.accepted.checked_add(1) != Some(first.0) {
            return Err(LifecycleError::InvalidRange);
        }
        let accepted = self
            .accepted
            .checked_add(u64::from(count))
            .ok_or(LifecycleError::InvalidRange)?;
        if accepted - self.completed > self.live_limit as u64 {
            return Err(LifecycleError::ResourceExhausted);
        }
        self.accepted = accepted;
        Ok(())
    }
    /// Records one terminal delivery exactly once, before publishing its event.
    /// # Errors
    /// Rejects unaccepted or already terminal tokens without changing the watermark.
    pub fn terminal(&mut self, token: RecordToken) -> Result<(), LifecycleError> {
        let token = token.0;
        if token == 0 || token > self.accepted {
            return Err(LifecycleError::UnknownToken);
        }
        if token <= self.through {
            return Err(LifecycleError::DuplicateTerminal);
        }
        let before = self
            .ranges
            .range(..=token)
            .next_back()
            .map(|(&start, &end)| (start, end));
        if before.is_some_and(|(_, end)| end >= token) {
            return Err(LifecycleError::DuplicateTerminal);
        }
        let mut start = token;
        let mut end = token;
        if let Some((previous, last)) = before
            && last.checked_add(1) == Some(token)
        {
            start = previous;
            self.ranges.remove(&previous);
        }
        if let Some((&next, &last)) = self.ranges.range(token..).next()
            && token.checked_add(1) == Some(next)
        {
            end = last;
            self.ranges.remove(&next);
        }
        self.ranges.insert(start, end);
        self.completed += 1;
        if let Some((&start, &end)) = self.ranges.first_key_value()
            && self.through.checked_add(1) == Some(start)
        {
            self.through = end;
            self.ranges.remove(&start);
        }
        debug_assert!(self.ranges.len() <= self.live_limit.saturating_add(1));
        Ok(())
    }
    #[must_use]
    pub fn reached(&self, watermark: RecordToken) -> bool {
        watermark.0 <= self.through
    }
}

/// Exactly one event credit accompanies each queued event. Draining drops that
/// guard, while a stopped consumer keeps the credit charged and admission bounded.
#[derive(Debug)]
pub struct EventEnvelope {
    pub event: Event,
    credit: HeldCredits,
}
impl EventEnvelope {
    pub(crate) fn belongs_to(&self, authority: &crate::credit::SharedCredits) -> bool {
        self.credit.belongs_to(authority)
    }
    /// # Errors
    /// Requires the event's independently reserved pool and exactly one credit.
    pub fn new(event: Event, credit: HeldCredits) -> Result<Self, LifecycleError> {
        let pool = match event {
            Event::Delivery(_) => Resource::DeliveryEvents,
            Event::InputReleased { .. } => Resource::ReleaseEvents,
            _ => Resource::ControlEvents,
        };
        if credit.amount(pool) != 1 {
            return Err(LifecycleError::MissingEventCredit);
        }
        Ok(Self { event, credit })
    }
    #[must_use]
    pub fn resource(&self) -> Resource {
        match self.event {
            Event::Delivery(_) => Resource::DeliveryEvents,
            Event::InputReleased { .. } => Resource::ReleaseEvents,
            _ => Resource::ControlEvents,
        }
    }
    #[must_use]
    pub fn reserved(&self) -> usize {
        self.credit.amount(self.resource())
    }
}

#[derive(Debug)]
pub struct EventQueue {
    queue: VecDeque<EventEnvelope>,
    limit: usize,
}
impl EventQueue {
    /// Exact retained envelope storage after successful construction, excluding
    /// referenced credit owners and construction transients.
    pub(crate) fn configured_storage_bytes(capacity: usize) -> Option<usize> {
        capacity.checked_mul(size_of::<EventEnvelope>())
    }

    #[cfg(test)]
    pub(crate) fn storage_capacity_bytes(&self) -> usize {
        self.queue.capacity() * size_of::<EventEnvelope>()
    }

    /// `capacity` is the checked sum of delivery, release and control capacities.
    /// # Errors
    /// Rejects zero/impossible capacity and allocation failure. A reservation with
    /// capacity above the requested limit is rejected before publication.
    pub fn new(capacity: usize) -> Result<Self, LifecycleError> {
        if capacity == 0 {
            return Err(LifecycleError::ResourceExhausted);
        }
        let queue =
            crate::fixed::try_deque(capacity).map_err(|_| LifecycleError::AllocationFailed)?;
        Ok(Self {
            queue,
            limit: capacity,
        })
    }
    /// # Errors
    /// Returns original ownership on capacity exhaustion. Correctly precharged
    /// producers cannot exhaust the sum independently of the three credit pools.
    #[allow(clippy::result_large_err)] // Exhaustion returns ownership without allocating.
    pub fn push(&mut self, event: EventEnvelope) -> Result<(), EventEnvelope> {
        if self.queue.len() == self.limit {
            return Err(event);
        }
        self.queue.push_back(event);
        Ok(())
    }
    #[must_use]
    pub fn len(&self) -> usize {
        self.queue.len()
    }
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.queue.is_empty()
    }
    /// Pops one event with its reservation so an actor can move it into the
    /// application ring without releasing the credit between queues.
    pub fn pop(&mut self) -> Option<EventEnvelope> {
        self.queue.pop_front()
    }
    /// Copies public event values and releases their queue credits in FIFO order.
    pub fn drain(&mut self, out: &mut [Event]) -> usize {
        let count = out.len().min(self.queue.len());
        for slot in &mut out[..count] {
            *slot = self.queue.pop_front().expect("bounded queue length").event;
        }
        count
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn event_storage_is_preserved_through_full_drain_and_refill() {
        assert!(EventQueue::configured_storage_bytes(usize::MAX).is_none());
        let credits = SharedCredits::new([7; Resource::COUNT], 1).unwrap();
        let mut events = EventQueue::new(7).unwrap();
        let storage = events.storage_capacity_bytes();
        assert_eq!(storage, EventQueue::configured_storage_bytes(7).unwrap());
        for _ in 0..100 {
            for _ in 0..7 {
                events
                    .push(
                        EventEnvelope::new(
                            Event::Closed { unresolved: 0 },
                            credits
                                .reserve(&[Claim {
                                    resource: Resource::ControlEvents,
                                    amount: 1,
                                    lane: 0,
                                }])
                                .unwrap(),
                        )
                        .unwrap(),
                    )
                    .unwrap();
            }
            while events.pop().is_some() {}
            assert_eq!(events.storage_capacity_bytes(), storage);
            assert!(credits.is_empty());
        }
    }
    use crate::{
        credit::{Claim, SharedCredits},
        types::*,
    };
    #[test]
    fn slow_old_record_does_not_accumulate_one_watermark_entry_per_completion() {
        let mut t = DeliveryTracker::new(2);
        t.accept(RecordToken(1), 1).unwrap();
        for token in 2..100_000 {
            t.accept(RecordToken(token), 1).unwrap();
            t.terminal(RecordToken(token)).unwrap();
            assert_eq!(t.completed_ranges(), 1);
            assert_eq!(t.completed_through(), RecordToken(0));
        }
        t.terminal(RecordToken(1)).unwrap();
        assert_eq!(t.completed_through(), RecordToken(99_999));
        assert_eq!(t.live(), 0);
        assert_eq!(t.completed_ranges(), 0);
    }
    #[test]
    fn seeded_out_of_order_terminals_match_a_dense_reference() {
        for seed in 1u64..128 {
            let mut state = seed;
            let mut t = DeliveryTracker::new(64);
            t.accept(RecordToken(1), 64).unwrap();
            let mut pending: Vec<u64> = (1..=64).collect();
            let mut complete = [false; 64];
            while !pending.is_empty() {
                state ^= state << 13;
                state ^= state >> 7;
                state ^= state << 17;
                let token = pending.swap_remove(state as usize % pending.len());
                t.terminal(RecordToken(token)).unwrap();
                complete[token as usize - 1] = true;
                let through = complete.iter().take_while(|&&b| b).count() as u64;
                assert_eq!(
                    t.completed_through(),
                    RecordToken(through),
                    "seed={seed} token={token}"
                );
                assert_eq!(
                    t.terminal(RecordToken(token)),
                    Err(LifecycleError::DuplicateTerminal)
                );
            }
        }
    }
    #[test]
    fn control_reserve_does_not_allow_flush_to_overtake_delivery() {
        let c = SharedCredits::new([2; Resource::COUNT], 1).unwrap();
        let mut q = EventQueue::new(2).unwrap();
        let delivery = Event::Delivery(DeliveryEvent {
            token: RecordToken(1),
            user_token: 1,
            topic: TopicHandle(1),
            partition: TopicPartition {
                topic: TopicId([1; 16]),
                partition: 0,
            },
            outcome: DeliveryOutcome::ACKED,
            base_offset: None.into(),
            timestamp: None.into(),
            attempts: 1,
        });
        for (event, resource) in [
            (delivery, Resource::DeliveryEvents),
            (
                Event::FlushDone {
                    token: FlushToken(1),
                },
                Resource::ControlEvents,
            ),
        ] {
            q.push(
                EventEnvelope::new(
                    event,
                    c.reserve(&[Claim {
                        resource,
                        amount: 1,
                        lane: 0,
                    }])
                    .unwrap(),
                )
                .unwrap(),
            )
            .unwrap();
        }
        assert!(!c.is_empty());
        let mut out = [Event::Closed { unresolved: 0 }; 2];
        assert_eq!(q.drain(&mut out), 2);
        assert_eq!(
            out,
            [
                delivery,
                Event::FlushDone {
                    token: FlushToken(1)
                }
            ]
        );
        assert!(c.is_empty());
    }
}
