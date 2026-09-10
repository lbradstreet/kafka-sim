//! Per-handle cleanup indexed by accepted record ownership, never topic names.
use super::*;
use std::ops::Bound::{Excluded, Unbounded};

impl TopicRecords {
    fn capture_id(&mut self, id: Option<TopicId>) {
        if let Some(id) = id {
            assert!(
                self.captured_id.is_none_or(|old| old == id),
                "accepted topic identity never rebinds"
            );
            self.captured_id = Some(id);
        }
    }
}

impl ProducerEngine {
    pub(super) fn index_topic_record(&mut self, record: &AdmittedRecord) {
        let id = self
            .topics
            .get(record.topic)
            .ok()
            .and_then(|topic| topic.id);
        let partition = id.map(|topic| TopicPartition {
            topic,
            partition: record.partition_hint.unwrap_or(-1),
        });
        self.terminal_order
            .accept(record.topic, record.token, partition);
        let owner = self
            .topic_records
            .entry(record.topic)
            .or_insert_with(|| TopicRecords {
                records: BTreeMap::new(),
                captured_id: id,
                write_fence: crate::transport::WriteFence::new(),
                reason: None,
                cursor: None,
            });
        owner.capture_id(id);
        assert!(
            owner.records.insert(record.token, partition).is_none(),
            "one topic index entry per accepted record"
        );
    }
    pub(super) fn capture_topic_partition(
        &mut self,
        record: &AdmittedRecord,
        partition: TopicPartition,
    ) {
        self.terminal_order
            .route(record.topic, record.token, partition);
        let owner = self
            .topic_records
            .get_mut(&record.topic)
            .expect("accepted record retains its topic owner");
        owner.capture_id(Some(partition.topic));
        let captured = owner
            .records
            .get_mut(&record.token)
            .expect("accepted record retains its topic index");
        assert!(
            captured.is_none_or(|old| old.topic == partition.topic),
            "accepted topic identity never rebinds"
        );
        *captured = Some(partition);
    }
    pub(super) fn topic_is_settling(&self, handle: TopicHandle) -> bool {
        self.topic_records
            .get(&handle)
            .is_some_and(|owner| owner.reason.is_some())
    }
    pub(super) fn finish_topic_record(&mut self, handle: TopicHandle, token: RecordToken) {
        let owner = self
            .topic_records
            .get_mut(&handle)
            .expect("terminal record has a topic owner");
        assert!(
            owner.records.remove(&token).is_some(),
            "topic index removal is exactly once"
        );
        if owner.records.is_empty() {
            self.topic_records.remove(&handle);
            self.settling_topics.remove(&handle);
        }
    }
    pub(super) fn settle_topic(&mut self, handle: TopicHandle, reason: FailureReason) {
        // Books exist only for still-live accepted descriptors. Closing empty
        // handles therefore never accumulates jobs, even across rapid reopen.
        let id = self.topics.get(handle).ok().and_then(|topic| topic.id);
        if let Some(id) = id {
            self.terminal_order.capture_id(handle, id);
        }
        if let Some(owner) = self.topic_records.get_mut(&handle) {
            // Resolution can precede partition-policy/credit admission. Capture
            // once for all such pending records before the cache drops the ID.
            owner.capture_id(id);
            owner.write_fence.close();
            if owner.reason.is_none() {
                owner.reason = Some(reason);
                self.settling_topics.insert(handle);
            }
        }
        if reason == FailureReason::Closed
            && let Some(id) = id
        {
            self.queue_partition_cleanup(id);
        }
        if let Some(credit) = self.topic_event_credits.remove(&handle) {
            self.event(
                Event::TopicFailed {
                    topic: handle,
                    code: reason as u32,
                },
                credit,
            );
        }
    }
    pub(super) fn topic_settlement_step(&mut self) -> bool {
        let handle = self
            .settlement_cursor
            .and_then(|cursor| {
                self.settling_topics
                    .range((Excluded(cursor), Unbounded))
                    .next()
                    .copied()
            })
            .or_else(|| self.settling_topics.first().copied());
        let Some(handle) = handle else {
            return false;
        };
        self.settlement_cursor = Some(handle);
        let owner = self
            .topic_records
            .get_mut(&handle)
            .expect("settlement retains accepted descriptors");
        let next = match owner.cursor {
            Some(cursor) => owner
                .records
                .range((Excluded(cursor), Unbounded))
                .next()
                .map(|(&token, _)| token),
            None => owner.records.first_key_value().map(|(&token, _)| token),
        };
        let Some(token) = next else {
            self.settling_topics.remove(&handle);
            return true;
        };
        owner.cursor = Some(token);
        let reason = owner.reason.expect("active settlement has a cause");
        if let Some(record) = self.take_pending(token) {
            self.fail_record(record, reason);
        } else if let Some(&partition) = self.queued_locations.get(&token) {
            let record = self
                .partitions
                .get_mut(&partition)
                .expect("indexed partition")
                .records
                .remove_token(token)
                .expect("indexed queued record");
            self.fail_record(record, reason);
        } else if let Some(&batch) = self.batched_locations.get(&token) {
            // Already-terminal batches can retain their input/output cleanup and
            // delivery records. Their existing terminal result is not rewritten.
            if self.batches.get(batch).is_some() {
                self.expire_batch(batch, reason);
            }
        }
        true
    }
}

#[cfg(test)]
pub(super) mod tests;
