use super::*;
/// Indexed queues make cancellation, expiry, and FIFO removal logarithmic even
/// when the configured descriptor/batch capacity is much larger than one poll.
#[derive(Default)]
pub(super) struct RecordQueue {
    records: BTreeMap<RecordToken, AdmittedRecord>,
    ages: BTreeSet<(RuntimeInstant, RecordToken)>,
    bytes: u64,
}
impl RecordQueue {
    pub(super) fn is_empty(&self) -> bool {
        self.records.is_empty()
    }
    pub(super) fn front(&self) -> Option<&AdmittedRecord> {
        self.records.first_key_value().map(|(_, record)| record)
    }
    pub(super) fn push_back(&mut self, record: AdmittedRecord) {
        self.push_front(record);
    }
    pub(super) fn push_front(&mut self, record: AdmittedRecord) {
        self.bytes += u64::from(record.standalone_encoded_bytes);
        self.ages.insert((record.accepted_at, record.token));
        assert!(
            self.records.insert(record.token, record).is_none(),
            "one queued owner per token"
        );
    }
    pub(super) fn pop_front(&mut self) -> Option<AdmittedRecord> {
        let token = *self.records.first_key_value()?.0;
        self.remove_token(token)
    }
    pub(super) fn remove_token(&mut self, token: RecordToken) -> Option<AdmittedRecord> {
        let record = self.records.remove(&token)?;
        self.bytes -= u64::from(record.standalone_encoded_bytes);
        self.ages.remove(&(record.accepted_at, record.token));
        Some(record)
    }
    pub(super) fn bytes(&self) -> u64 {
        self.bytes
    }
    pub(super) fn oldest(&self) -> Option<RuntimeInstant> {
        self.ages.first().map(|(at, _)| *at)
    }
}
impl IntoIterator for RecordQueue {
    type Item = AdmittedRecord;
    type IntoIter = std::collections::btree_map::IntoValues<RecordToken, AdmittedRecord>;
    fn into_iter(self) -> Self::IntoIter {
        self.records.into_values()
    }
}
impl<'a> IntoIterator for &'a RecordQueue {
    type Item = &'a AdmittedRecord;
    type IntoIter = std::collections::btree_map::Values<'a, RecordToken, AdmittedRecord>;
    fn into_iter(self) -> Self::IntoIter {
        self.records.values()
    }
}
#[derive(Default)]
pub(super) struct BatchQueue {
    order: BTreeMap<u64, BatchKey>,
    keys: BTreeMap<BatchKey, u64>,
    next: u64,
}
impl BatchQueue {
    pub(super) fn ordinal(&self, key: BatchKey) -> Option<u64> {
        self.keys.get(&key).copied()
    }
    pub(super) fn is_empty(&self) -> bool {
        self.order.is_empty()
    }
    pub(super) fn back(&self) -> Option<&BatchKey> {
        self.order.last_key_value().map(|(_, key)| key)
    }
    pub(super) fn iter(&self) -> impl Iterator<Item = &BatchKey> {
        self.order.values()
    }
    pub(super) fn push_back(&mut self, key: BatchKey) {
        let sequence = self.next;
        self.next = self
            .next
            .checked_add(1)
            .expect("batches bounded by accepted record tokens");
        assert!(
            self.keys.insert(key, sequence).is_none(),
            "one owner per batch key"
        );
        self.order.insert(sequence, key);
    }
    pub(super) fn remove(&mut self, key: BatchKey) {
        if let Some(sequence) = self.keys.remove(&key) {
            self.order.remove(&sequence);
        }
    }
}
impl<'a> IntoIterator for &'a BatchQueue {
    type Item = &'a BatchKey;
    type IntoIter = std::collections::btree_map::Values<'a, u64, BatchKey>;
    fn into_iter(self) -> Self::IntoIter {
        self.order.values()
    }
}

/// Preallocated intrusive FIFO: removal by a stable live slot never scans the
/// queue, and the free list reuses storage without a monotonic token counter.
pub(super) struct OrderQueue {
    slots: Vec<OrderSlot>,
    head: Option<usize>,
    tail: Option<usize>,
    free: Option<usize>,
    len: usize,
}
struct OrderSlot {
    value: Option<EngineOrder>,
    previous: Option<usize>,
    next: Option<usize>,
}
impl OrderQueue {
    pub(super) const fn slot_bytes() -> usize {
        core::mem::size_of::<OrderSlot>()
    }
    #[cfg(test)]
    pub(super) fn storage_capacity_bytes(&self) -> usize {
        self.slots.capacity() * Self::slot_bytes()
    }
    pub(super) fn new(capacity: usize) -> Result<Self> {
        let mut slots =
            crate::fixed::try_vec(capacity).map_err(|_| EngineError::AllocationFailed)?;
        for index in 0..capacity {
            slots.push(OrderSlot {
                value: None,
                previous: None,
                next: (index + 1 < capacity).then_some(index + 1),
            });
        }
        Ok(Self {
            slots,
            head: None,
            tail: None,
            free: (capacity != 0).then_some(0),
            len: 0,
        })
    }
    pub(super) fn len(&self) -> usize {
        self.len
    }
    pub(super) fn push_back(&mut self, order: EngineOrder) -> Result<usize> {
        let index = self
            .free
            .ok_or(EngineError::InvalidState("bounded order queue full"))?;
        self.free = self.slots[index].next;
        self.slots[index] = OrderSlot {
            value: Some(order),
            previous: self.tail,
            next: None,
        };
        if let Some(tail) = self.tail {
            self.slots[tail].next = Some(index);
        } else {
            self.head = Some(index);
        }
        self.tail = Some(index);
        self.len += 1;
        Ok(index)
    }
    pub(super) fn pop_front(&mut self) -> Option<(usize, EngineOrder)> {
        let index = self.head?;
        self.remove(index).map(|value| (index, value))
    }
    pub(super) fn remove(&mut self, index: usize) -> Option<EngineOrder> {
        let slot = self.slots.get_mut(index)?;
        let value = slot.value.take()?;
        let previous = slot.previous;
        let next = slot.next;
        if let Some(previous) = previous {
            self.slots[previous].next = next;
        } else {
            self.head = next;
        }
        if let Some(next) = next {
            self.slots[next].previous = previous;
        } else {
            self.tail = previous;
        }
        self.slots[index].previous = None;
        self.slots[index].next = self.free;
        self.free = Some(index);
        self.len -= 1;
        Some(value)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn indexed_order_removal_and_slot_reuse_preserve_fifo_without_growth() {
        let mut queue = OrderQueue::new(8).unwrap();
        let mut reference = VecDeque::new();
        let capacity = queue.slots.capacity();
        assert_eq!(capacity, 8);
        assert_eq!(queue.storage_capacity_bytes(), 8 * OrderQueue::slot_bytes());
        assert!(OrderQueue::new(usize::MAX).is_err());
        let mut empty = OrderQueue::new(0).unwrap();
        assert_eq!(empty.storage_capacity_bytes(), 0);
        assert!(empty.pop_front().is_none());
        let mut rng = 0x43eedu64;
        for value in 0..4096 {
            rng = rng.wrapping_mul(6364136223846793005).wrapping_add(1);
            if !reference.is_empty() && (rng & 3 == 0 || reference.len() == 8) {
                let position = (rng as usize) % reference.len();
                let (slot, expected) = reference.remove(position).unwrap();
                let EngineOrder::Metadata { handles } = queue.remove(slot).unwrap() else {
                    panic!("metadata")
                };
                assert_eq!(handles, [TopicHandle(expected)]);
            } else {
                let slot = queue
                    .push_back(EngineOrder::Metadata {
                        handles: vec![TopicHandle(value)],
                    })
                    .unwrap();
                reference.push_back((slot, value));
            }
            if rng & 7 == 3
                && let Some((slot, expected)) = reference.pop_front()
            {
                let (actual, EngineOrder::Metadata { handles }) = queue.pop_front().unwrap() else {
                    panic!("metadata")
                };
                assert_eq!(actual, slot);
                assert_eq!(handles, [TopicHandle(expected)]);
            }
            assert_eq!(queue.len(), reference.len());
            assert_eq!(queue.slots.capacity(), capacity);
        }
        for (slot, expected) in reference {
            let (actual, EngineOrder::Metadata { handles }) = queue.pop_front().unwrap() else {
                panic!("metadata")
            };
            assert_eq!(actual, slot);
            assert_eq!(handles, [TopicHandle(expected)]);
        }
        assert!(queue.pop_front().is_none());
    }
}
