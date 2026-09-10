//! Delivery publication follows admission within each routed partition. An
//! older record whose routing is unresolved temporarily blocks its topic: its
//! eventual partition is not yet known. Terminal payloads are released before
//! entering this gate; descriptor and event credits bound all retained owners.
use super::*;
use std::ops::Bound::{Excluded, Unbounded};

pub(super) struct HeldDelivery {
    pub record: RecordObligation,
    pub event: DeliveryEvent,
}

#[derive(Default)]
pub(super) struct TerminalOrder {
    partitions: BTreeMap<TopicPartition, BTreeSet<RecordToken>>,
    unrouted: BTreeMap<TopicHandle, BTreeSet<RecordToken>>,
    // One identity and one minimum per handle with unrouted descriptors. A
    // retired handle can still block a reopened handle bound to the same UUID.
    unrouted_ids: BTreeMap<TopicHandle, TopicId>,
    unrouted_heads: BTreeMap<TopicId, BTreeSet<(RecordToken, TopicHandle)>>,
    held: BTreeMap<RecordToken, HeldDelivery>,
    cursor: Option<RecordToken>,
    work: bool,
}

impl TerminalOrder {
    pub(super) fn accept(
        &mut self,
        topic: TopicHandle,
        token: RecordToken,
        partition: Option<TopicPartition>,
    ) {
        if let Some(partition) = partition.filter(|key| key.partition >= 0) {
            assert!(self.partitions.entry(partition).or_default().insert(token));
        } else {
            // Admission tokens increase, so insertion cannot change this
            // handle's existing minimum or its identity summary.
            assert!(self.unrouted.entry(topic).or_default().insert(token));
        }
        if let Some(partition) = partition {
            self.capture_id(topic, partition.topic);
        }
    }

    pub(super) fn capture_id(&mut self, topic: TopicHandle, id: TopicId) {
        // Pre-resolution failures use ZERO as an unknown event identity. It
        // must not bind other pending records that may still resolve normally.
        if id.is_zero() {
            return;
        }
        let Some(&first) = self.unrouted.get(&topic).and_then(|tokens| tokens.first()) else {
            return;
        };
        if let Some(old) = self.unrouted_ids.get(&topic) {
            assert_eq!(*old, id, "accepted topic identity never rebinds");
            return;
        }
        self.unrouted_ids.insert(topic, id);
        assert!(
            self.unrouted_heads
                .entry(id)
                .or_default()
                .insert((first, topic))
        );
    }

    pub(super) fn route(
        &mut self,
        topic: TopicHandle,
        token: RecordToken,
        partition: TopicPartition,
    ) {
        if partition.partition < 0 {
            return;
        }
        self.capture_id(topic, partition.topic);
        self.partitions.entry(partition).or_default().insert(token);
        if self.remove_unrouted(topic, token) {
            self.changed();
        }
    }

    fn remove_unrouted(&mut self, topic: TopicHandle, token: RecordToken) -> bool {
        let Some(tokens) = self.unrouted.get_mut(&topic) else {
            return false;
        };
        let first = *tokens.first().expect("nonempty unrouted index");
        if !tokens.remove(&token) {
            return false;
        }
        let next = tokens.first().copied();
        if let Some(&id) = self.unrouted_ids.get(&topic)
            && next != Some(first)
        {
            let heads = self.unrouted_heads.get_mut(&id).expect("identity summary");
            assert!(heads.remove(&(first, topic)));
            if let Some(next) = next {
                assert!(heads.insert((next, topic)));
            } else {
                self.unrouted_ids.remove(&topic);
            }
            if heads.is_empty() {
                self.unrouted_heads.remove(&id);
            }
        }
        if next.is_none() {
            self.unrouted.remove(&topic);
        }
        true
    }

    pub(super) fn ready(&self, event: &DeliveryEvent) -> bool {
        // A never-routed failure has no actual partition to order against.
        if event.partition.partition < 0 {
            return true;
        }
        self.partitions
            .get(&event.partition)
            .and_then(|tokens| tokens.first())
            == Some(&event.token)
            && self
                .unrouted
                .get(&event.topic)
                .and_then(|tokens| tokens.first())
                .is_none_or(|token| *token >= event.token)
            && self
                .unrouted_heads
                .get(&event.partition.topic)
                .and_then(|heads| heads.first())
                .is_none_or(|(token, _)| *token >= event.token)
    }

    pub(super) fn hold(&mut self, record: RecordObligation, event: DeliveryEvent) {
        assert!(
            self.held
                .insert(event.token, HeldDelivery { record, event })
                .is_none()
        );
        self.changed();
    }

    pub(super) fn release(&mut self, event: &DeliveryEvent) {
        if event.partition.partition >= 0 {
            let tokens = self
                .partitions
                .get_mut(&event.partition)
                .expect("ordered partition");
            assert!(tokens.remove(&event.token));
            if tokens.is_empty() {
                self.partitions.remove(&event.partition);
            }
        }
        self.remove_unrouted(event.topic, event.token);
        self.changed();
    }

    fn changed(&mut self) {
        self.cursor = None;
        self.work = !self.held.is_empty();
    }

    pub(super) fn has_work(&self) -> bool {
        self.work
    }

    /// One bounded visit, including blocked candidates. Park after a complete
    /// pass; only admission/routing/publication changes can wake this work.
    pub(super) fn step(&mut self) -> Option<HeldDelivery> {
        let candidate = self
            .held
            .range((self.cursor.map_or(Unbounded, Excluded), Unbounded))
            .next();
        let Some((&token, delivery)) = candidate else {
            self.work = false;
            return None;
        };
        self.cursor = Some(token);
        if self.ready(&delivery.event) {
            self.held.remove(&token)
        } else {
            None
        }
    }
}

impl ProducerEngine {
    pub(super) fn ordered_delivery_step(&mut self) -> bool {
        if !self.terminal_order.has_work() {
            return false;
        }
        if let Some(delivery) = self.terminal_order.step() {
            self.publish_delivery(delivery.record, delivery.event);
        }
        true
    }
}

#[cfg(test)]
mod tests;
