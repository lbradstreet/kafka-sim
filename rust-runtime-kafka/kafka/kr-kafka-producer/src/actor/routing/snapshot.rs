//! Owner-local advisory collection. Each step visits at most its row budget;
//! generation validation is a separate bounded pass over at most one bulk's
//! topics immediately before invocation. No references into the engine survive.
use super::{TopicHandle, TopicId, TopicIdentity};
use crate::routing::{
    PartitionSnapshot, RecordMetadata, RoutingError, SnapshotCollection, TopicSnapshot,
    adaptive_weight,
};
use kr_runtime::RuntimeInstant;
use std::ops::Range;

#[derive(Clone, Debug)]
struct CollectedTopic {
    handle: TopicHandle,
    id: TopicId,
    generation: u32,
    rows: Range<usize>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum Step {
    Pending,
    Collected,
    Invalidated,
}

pub(super) struct Collector {
    rows: Vec<PartitionSnapshot>,
    prefix: Vec<u128>,
    topics: Vec<CollectedTopic>,
    requested: Vec<TopicHandle>,
    maximum_rows: usize,
    maximum_topics: usize,
    topic_cursor: usize,
    started_at: RuntimeInstant,
    run_bytes: u32,
}

impl Collector {
    pub(super) fn new(maximum_rows: usize, maximum_topics: usize) -> Result<Self, RoutingError> {
        let rows = crate::fixed::try_vec(maximum_rows).map_err(|_| RoutingError::PolicyFailed)?;
        let prefix = crate::fixed::try_vec(maximum_rows).map_err(|_| RoutingError::PolicyFailed)?;
        let topics =
            crate::fixed::try_vec(maximum_topics).map_err(|_| RoutingError::PolicyFailed)?;
        let requested =
            crate::fixed::try_vec(maximum_topics).map_err(|_| RoutingError::PolicyFailed)?;
        Ok(Self {
            rows,
            prefix,
            topics,
            requested,
            maximum_rows,
            maximum_topics,
            topic_cursor: 0,
            started_at: RuntimeInstant::ZERO,
            run_bytes: 1,
        })
    }

    pub(super) fn start(
        &mut self,
        now: RuntimeInstant,
        records: &[RecordMetadata],
        run_bytes: u32,
        mut needs_snapshot: impl FnMut(&RecordMetadata) -> bool,
        mut identity: impl FnMut(TopicHandle) -> Option<TopicIdentity>,
    ) -> Result<(), RoutingError> {
        self.rows.clear();
        self.prefix.clear();
        self.topics.clear();
        self.requested.clear();
        self.topic_cursor = 0;
        self.started_at = now;
        self.run_bytes = run_bytes;
        // records is bounded by the configured actor bulk item limit. Sorting
        // this small handle arena avoids an allocation per topic/tree node.
        for record in records.iter().filter(|record| needs_snapshot(record)) {
            if self.requested.len() == self.maximum_topics {
                return Err(RoutingError::PolicyFailed);
            }
            self.requested.push(record.topic);
        }
        self.requested.sort_unstable();
        self.requested.dedup();
        let mut end = 0_usize;
        for &handle in &self.requested {
            let Some(id) = identity(handle) else {
                continue;
            };
            let start = end;
            end = end
                .checked_add(id.partitions)
                .ok_or(RoutingError::PolicyFailed)?;
            if end > self.maximum_rows || id.partitions > i32::MAX as usize {
                return Err(RoutingError::PolicyFailed);
            }
            self.topics.push(CollectedTopic {
                handle,
                id: id.id,
                generation: id.generation,
                rows: start..end,
            });
        }
        Ok(())
    }

    pub(super) fn step(
        &mut self,
        budget: usize,
        mut identity: impl FnMut(TopicHandle) -> Option<TopicIdentity>,
        mut observe: impl FnMut(TopicId, i32) -> PartitionSnapshot,
    ) -> Step {
        let mut visits = 0;
        while self.topic_cursor < self.topics.len() && visits < budget {
            let topic = &self.topics[self.topic_cursor];
            if !Self::matches(topic, identity(topic.handle)) {
                return Step::Invalidated;
            }
            if self.rows.len() == topic.rows.end {
                self.topic_cursor += 1;
                // Empty topics also spend a visit; metadata normally forbids
                // them, but malformed provider fixtures cannot spin here.
                visits += 1;
                continue;
            }
            let index = self.rows.len() - topic.rows.start;
            let row = observe(topic.id, index as i32);
            let preceding = if index == 0 {
                0
            } else {
                self.prefix[self.prefix.len() - 1]
            };
            self.prefix
                .push(preceding + adaptive_weight(&row, self.run_bytes));
            self.rows.push(row);
            visits += 1;
            if self.rows.len() == topic.rows.end {
                self.topic_cursor += 1;
            }
        }
        if self.topic_cursor == self.topics.len() {
            Step::Collected
        } else {
            Step::Pending
        }
    }

    /// Call immediately before policy invocation, without an engine mutation in
    /// between. Topic count is bounded by the same input bulk item budget.
    pub(super) fn valid(
        &self,
        mut identity: impl FnMut(TopicHandle) -> Option<TopicIdentity>,
    ) -> bool {
        self.requested.iter().all(|&handle| {
            let current = identity(handle);
            match self
                .topics
                .binary_search_by_key(&handle, |topic| topic.handle)
            {
                Ok(index) => Self::matches(&self.topics[index], current),
                Err(_) => current.is_none(),
            }
        })
    }
    fn matches(topic: &CollectedTopic, identity: Option<TopicIdentity>) -> bool {
        identity.is_some_and(|id| {
            id.id == topic.id
                && id.generation == topic.generation
                && id.partitions == topic.rows.len()
        })
    }
    pub(super) fn collection(&self, completed_at: RuntimeInstant) -> SnapshotCollection {
        SnapshotCollection {
            started_at: self.started_at,
            completed_at,
        }
    }
    pub(super) fn topic(&self, handle: TopicHandle) -> Option<(TopicSnapshot<'_>, &[u128])> {
        let index = self
            .topics
            .binary_search_by_key(&handle, |topic| topic.handle)
            .ok()?;
        let topic = &self.topics[index];
        Some((
            TopicSnapshot {
                handle,
                id: topic.id,
                generation: topic.generation,
                partitions: &self.rows[topic.rows.clone()],
            },
            &self.prefix[topic.rows.clone()],
        ))
    }
    pub(super) fn views(&self) -> Result<Vec<TopicSnapshot<'_>>, RoutingError> {
        let mut views = Vec::new();
        // The public callback API requires a borrowed slice of borrowed views.
        // Safe Rust cannot persist that self-referential Vec in this owner.
        // Only this <=bulk-sized view array is transient; rows stay preallocated.
        views
            .try_reserve_exact(self.topics.len())
            .map_err(|_| RoutingError::PolicyFailed)?;
        for topic in &self.topics {
            views.push(TopicSnapshot {
                handle: topic.handle,
                id: topic.id,
                generation: topic.generation,
                partitions: &self.rows[topic.rows.clone()],
            });
        }
        Ok(views)
    }
    pub(super) fn configured_storage_bytes(rows: usize, topics: usize) -> Option<usize> {
        rows.checked_mul(size_of::<PartitionSnapshot>() + size_of::<u128>())?
            .checked_add(
                topics.checked_mul(size_of::<CollectedTopic>() + size_of::<TopicHandle>())?,
            )
    }
    pub(super) fn metadata_capacity_bytes(&self) -> usize {
        self.rows.capacity() * size_of::<PartitionSnapshot>()
            + self.prefix.capacity() * size_of::<u128>()
            + self.topics.capacity() * size_of::<CollectedTopic>()
            + self.requested.capacity() * size_of::<TopicHandle>()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use kr_runtime::RuntimeDuration;
    fn identity(handle: TopicHandle, count: usize, generation: u32) -> TopicIdentity {
        TopicIdentity {
            id: TopicId([handle.0 as u8; 16]),
            generation,
            partitions: count,
        }
    }
    fn record(handle: u32) -> RecordMetadata {
        RecordMetadata {
            topic: TopicHandle(handle),
            key_hash: None,
            encoded_bytes: 10,
            partition_hint: None,
        }
    }
    fn row(partition: i32, observed: u64) -> PartitionSnapshot {
        PartitionSnapshot {
            partition,
            lane: 0,
            queued_bytes: observed,
            oldest_age: RuntimeDuration::from_nanos(observed),
            open_batch_bytes: 0,
            throttled_until: RuntimeInstant::ZERO,
            drain_bytes_per_second: 100,
            available: true,
        }
    }
    #[test]
    fn tiny_budgets_collect_each_row_once_without_growing_and_report_the_interval() {
        for budget in [1, 7, 128] {
            let mut collector = Collector::new(4097, 1).unwrap();
            let capacity = collector.metadata_capacity_bytes();
            assert_eq!(Some(capacity), Collector::configured_storage_bytes(4097, 1));
            let begin = RuntimeInstant::from_nanos(100);
            collector
                .start(
                    begin,
                    &[record(1)],
                    100,
                    |_| true,
                    |_| Some(identity(TopicHandle(1), 4097, 4)),
                )
                .unwrap();
            let mut observed = Vec::new();
            let mut poll = 0;
            loop {
                poll += 1;
                let before = observed.len();
                let result = collector.step(
                    budget,
                    |_| Some(identity(TopicHandle(1), 4097, 4)),
                    |_, partition| {
                        observed.push(partition);
                        row(partition, poll)
                    },
                );
                assert!(observed.len() - before <= budget);
                assert_eq!(collector.metadata_capacity_bytes(), capacity);
                if result == Step::Collected {
                    break;
                }
                assert_eq!(result, Step::Pending);
            }
            assert_eq!(observed, (0..4097).collect::<Vec<_>>());
            assert_eq!(poll, 4097_usize.div_ceil(budget) as u64);
            assert!(collector.valid(|_| Some(identity(TopicHandle(1), 4097, 4))));
            let completed = RuntimeInstant::from_nanos(100 + poll);
            assert_eq!(
                collector.collection(completed),
                SnapshotCollection {
                    started_at: begin,
                    completed_at: completed
                }
            );
            let (topic, prefix) = collector.topic(TopicHandle(1)).unwrap();
            let mut sum = 0;
            for (index, row) in topic.partitions.iter().enumerate() {
                assert_eq!(row.oldest_age.as_nanos(), (index / budget + 1) as u64);
                sum += adaptive_weight(row, 100);
                assert_eq!(prefix[index], sum);
            }
        }
    }
    #[test]
    fn generations_uuid_count_and_newly_ready_topics_invalidate_before_publication() {
        let mut collector = Collector::new(20, 2).unwrap();
        let original = identity(TopicHandle(1), 9, 1);
        let lookup = |handle| (handle == TopicHandle(1)).then_some(original);
        collector
            .start(
                RuntimeInstant::ZERO,
                &[record(1), record(2)],
                100,
                |_| true,
                lookup,
            )
            .unwrap();
        assert_eq!(collector.step(1, lookup, |_, p| row(p, 0)), Step::Pending);
        assert_eq!(
            collector.step(
                1,
                |_| Some(TopicIdentity {
                    generation: 2,
                    ..original
                }),
                |_, _| panic!("stale rows observed")
            ),
            Step::Invalidated
        );
        collector
            .start(
                RuntimeInstant::ZERO,
                &[record(1), record(2)],
                100,
                |_| true,
                lookup,
            )
            .unwrap();
        assert_eq!(collector.step(9, lookup, |_, p| row(p, 0)), Step::Collected);
        for changed in [
            TopicIdentity {
                generation: 2,
                ..original
            },
            TopicIdentity {
                id: TopicId([3; 16]),
                ..original
            },
            TopicIdentity {
                partitions: 10,
                ..original
            },
        ] {
            assert!(!collector.valid(|handle| (handle == TopicHandle(1)).then_some(changed)));
        }
        assert!(!collector.valid(|handle| Some(identity(handle, 9, 1))));
        assert!(collector.valid(lookup));
    }
}
