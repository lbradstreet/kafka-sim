//! Closed UUID cleanup visits one partition or one restart marker at a time.
//! Queues are never removed while a request/terminal payload owns their history.
//! Used sequence history requires a quiescent identity change before
//! erasure; merely closing and reopening a handle cannot reset its sequence.
use super::*;
use std::ops::Bound::{Excluded, Included};

#[derive(Default)]
pub(super) struct PartitionCleanup {
    topics: BTreeMap<TopicId, TopicCleanup>,
    ready: BTreeSet<TopicId>,
    cursor: Option<TopicId>,
    restart: bool,
    restart_cursor: Option<TopicId>,
}
#[derive(Default)]
struct TopicCleanup {
    partition: Option<i32>,
    dirty: bool,
}
impl PartitionCleanup {
    pub(super) fn has_work(&self) -> bool {
        self.restart || !self.ready.is_empty()
    }
}
impl ProducerEngine {
    fn first_topic_partition(&self, topic: TopicId, after: Option<i32>) -> Option<TopicPartition> {
        let lower = match after {
            Some(partition) => Excluded(TopicPartition { topic, partition }),
            None => Included(TopicPartition {
                topic,
                partition: 0,
            }),
        };
        self.partitions
            .range((
                lower,
                Included(TopicPartition {
                    topic,
                    partition: i32::MAX,
                }),
            ))
            .next()
            .map(|(&key, _)| key)
    }
    pub(super) fn queue_partition_cleanup(&mut self, topic: TopicId) {
        // Closing empty handles must not create an unbounded historical job map.
        if self.first_topic_partition(topic, None).is_none() {
            return;
        }
        self.partition_cleanup.topics.entry(topic).or_default();
        self.partition_cleanup_released(TopicPartition {
            topic,
            partition: 0,
        });
    }
    pub(super) fn partition_cleanup_released(&mut self, partition: TopicPartition) {
        let Some(job) = self.partition_cleanup.topics.get_mut(&partition.topic) else {
            return;
        };
        if self.partition_cleanup.ready.insert(partition.topic) {
            job.partition = None;
            job.dirty = false;
        } else {
            // Do not reset an active cursor or continuously changing ownership
            // at its beginning could starve the tail of the UUID's partition set.
            job.dirty = true;
        }
    }
    /// Identity installation changes the lazy history marker for every empty
    /// partition. Re-arm waiting topics over bounded visits, without a map walk.
    pub(super) fn partition_cleanup_at_capacity(&mut self) {
        if self.partitions.len() == self.config.max_batches as usize {
            self.partition_cleanup_identity_changed();
        }
    }
    pub(super) fn partition_cleanup_identity_changed(&mut self) {
        if !self.partition_cleanup.topics.is_empty() {
            self.partition_cleanup.restart = true;
            self.partition_cleanup.restart_cursor = None;
        }
    }
    pub(super) fn partition_cleanup_step(&mut self) -> bool {
        if self.partition_cleanup.restart {
            let next = match self.partition_cleanup.restart_cursor {
                Some(after) => self
                    .partition_cleanup
                    .topics
                    .range((Excluded(after), std::ops::Bound::Unbounded))
                    .next()
                    .map(|(&id, _)| id),
                None => self
                    .partition_cleanup
                    .topics
                    .first_key_value()
                    .map(|(&id, _)| id),
            };
            if let Some(topic) = next {
                self.partition_cleanup.restart_cursor = Some(topic);
                self.partition_cleanup_released(TopicPartition {
                    topic,
                    partition: 0,
                });
            } else {
                self.partition_cleanup.restart = false;
                self.partition_cleanup.restart_cursor = None;
            }
            return true;
        }
        let topic = self
            .partition_cleanup
            .cursor
            .and_then(|after| {
                self.partition_cleanup
                    .ready
                    .range((Excluded(after), std::ops::Bound::Unbounded))
                    .next()
                    .copied()
            })
            .or_else(|| self.partition_cleanup.ready.first().copied());
        let Some(topic) = topic else {
            return false;
        };
        self.partition_cleanup.cursor = Some(topic);
        // A reopened handle with the same UUID may reuse its queue and sequence
        // history. A later close explicitly re-enqueues cleanup for that UUID.
        if self.topics.by_id(topic).is_some() {
            self.partition_cleanup.ready.remove(&topic);
            self.partition_cleanup.topics.remove(&topic);
            return true;
        }
        let after = self.partition_cleanup.topics[&topic].partition;
        let Some(key) = self.first_topic_partition(topic, after) else {
            let no_partitions = self.first_topic_partition(topic, None).is_none();
            let job = self
                .partition_cleanup
                .topics
                .get_mut(&topic)
                .expect("ready cleanup job");
            job.partition = None;
            if no_partitions {
                self.partition_cleanup.topics.remove(&topic);
                self.partition_cleanup.ready.remove(&topic);
            } else if job.dirty {
                job.dirty = false;
            } else {
                // Only an actual owner release or a completed identity change
                // can make this parked UUID ready. No polling on external refs.
                self.partition_cleanup.ready.remove(&topic);
            }
            return true;
        };
        self.partition_cleanup
            .topics
            .get_mut(&topic)
            .expect("ready cleanup job")
            .partition = Some(key.partition);
        let queue = self.partitions.get(&key).expect("indexed partition");
        if !queue.records.is_empty()
            || !queue.batches.is_empty()
            || queue.request_owners != 0
            || queue.terminal_owners != 0
        {
            return true;
        }
        if let Some(ledger) = &mut self.ledger {
            match ledger.can_forget_partition(key) {
                Ok(true) => {
                    ledger.remove(key).expect("proved unused history");
                }
                Ok(false) => {
                    if self.partitions.len() == self.config.max_batches as usize {
                        // Quiescent epoch recovery makes old history forgettable.
                        // Only epoch exhaustion needs a fresh broker identity.
                        let _ = ledger.request_identity_refresh();
                    }
                    return true;
                }
                Err(LedgerError::UnknownPartition) => {}
                Err(error) => panic!("unexpected partition history query: {error}"),
            }
        }
        self.encoder_forget_partition(key);
        self.scheduler_forget_partition(key);
        self.deadlines.remove(DeadlineKey::Retry(key));
        let removed = self
            .partitions
            .remove(&key)
            .expect("validated empty partition");
        debug_assert!(removed.batch_ages.is_empty());
        true
    }
}

#[cfg(test)]
mod tests;
