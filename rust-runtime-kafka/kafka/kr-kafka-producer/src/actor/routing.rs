use super::*;
use crate::routing::{
    NativePartitioner, PartitionChoice, PartitionerConfig, RecordMetadata, RoutingError,
    RoutingLease, RoutingSnapshot, SnapshotCollection, StickyRouter, TopicSnapshot, murmur2,
};
use crate::topic::TopicState;

mod snapshot;
use snapshot::{Collector, Step};
mod work;
use work::{Entry, Work, cached_native_into, native_into, topic_identity};

pub(super) struct Routing {
    sticky: BTreeMap<TopicHandle, StickyRouter>,
    native: BTreeMap<TopicHandle, NativeRun>,
    work: Work,
}
struct NativeRun {
    generation: u32,
    lease: RoutingLease,
}
#[derive(Clone, Copy, Debug)]
struct TopicIdentity {
    id: TopicId,
    generation: u32,
    partitions: usize,
}

impl Routing {
    pub(super) fn forget(&mut self, topic: TopicHandle) {
        self.sticky.remove(&topic);
        self.native.remove(&topic);
    }

    // Preview the entire bulk before consuming any quota: a later record can
    // exhaust a cached run and require the native callback/full advisory view.
    // Unknown topics and hints retain their existing Pending/bypass semantics.
    #[cfg(test)]
    fn choose_cached_native(
        &mut self,
        records: &[RecordMetadata],
        mut topic: impl FnMut(TopicHandle) -> Option<TopicIdentity>,
    ) -> Option<Vec<PartitionChoice>> {
        let mut remaining = BTreeMap::new();
        let mut choices = vec![PartitionChoice::Pending; records.len()];
        for (record, choice) in records.iter().zip(&mut choices) {
            let Some(identity) = topic(record.topic) else {
                continue;
            };
            if let Some(hint) = record.partition_hint {
                *choice = PartitionChoice::Partition(hint);
                continue;
            }
            if record.key_hash.is_some() {
                return None;
            }
            let run = self.native.get(&record.topic)?;
            if run.generation != identity.generation
                || run.lease.topic != identity.id
                || run.lease.partition < 0
                || run.lease.partition as usize >= identity.partitions
            {
                return None;
            }
            let quota = remaining
                .entry(record.topic)
                .or_insert(run.lease.byte_quota);
            if *quota == 0 {
                return None;
            }
            *quota = quota.saturating_sub(record.encoded_bytes);
            *choice = PartitionChoice::Partition(run.lease.partition);
        }
        for (topic, quota) in remaining {
            self.native
                .get_mut(&topic)
                .expect("validated cached run")
                .lease
                .byte_quota = quota;
        }
        Some(choices)
    }

    #[cfg(test)]
    fn choose_native(
        &mut self,
        policy: &dyn NativePartitioner,
        records: &[RecordMetadata],
        snapshot: RoutingSnapshot<'_>,
    ) -> Vec<PartitionChoice> {
        let mut choices = vec![PartitionChoice::Pending; records.len()];
        let mut declined = BTreeSet::new();
        let mut failed = BTreeSet::new();
        let mut bulk = Vec::new();
        let mut indices = Vec::new();
        for (index, record) in records.iter().enumerate() {
            let Some(topic) = snapshot
                .topics
                .iter()
                .find(|t| t.handle == record.topic)
                .copied()
            else {
                continue;
            };
            if let Some(hint) = record.partition_hint {
                choices[index] = PartitionChoice::Partition(hint);
                continue;
            }
            if record.key_hash.is_none() {
                if failed.contains(&record.topic) {
                    choices[index] = PartitionChoice::Partition(-1);
                    continue;
                }
                let valid = self.native.get(&record.topic).is_some_and(|run| {
                    run.generation == topic.generation
                        && run.lease.topic == topic.id
                        && run.lease.byte_quota > 0
                        && run.lease.partition >= 0
                        && (run.lease.partition as usize) < topic.partitions.len()
                });
                if !valid {
                    self.native.remove(&record.topic);
                    if !declined.contains(&record.topic) {
                        let mut result = Err(RoutingError::PolicyFailed);
                        kr_runtime::contain_panic(|| result = policy.choose_run(topic, snapshot));
                        match result {
                            Ok(Some(lease))
                                if lease.topic == topic.id
                                    && lease.byte_quota > 0
                                    && lease.partition >= 0
                                    && (lease.partition as usize) < topic.partitions.len() =>
                            {
                                self.native.insert(
                                    record.topic,
                                    NativeRun {
                                        generation: topic.generation,
                                        lease,
                                    },
                                );
                            }
                            Ok(None) => {
                                declined.insert(record.topic);
                            }
                            _ => {
                                failed.insert(record.topic);
                                choices[index] = PartitionChoice::Partition(-1);
                                continue;
                            }
                        }
                    }
                }
                if let Some(run) = self.native.get_mut(&record.topic) {
                    choices[index] = PartitionChoice::Partition(run.lease.partition);
                    // Consume once for each already-admitted input. Overshoot is
                    // bounded to one record, and retries never enter this path.
                    run.lease.byte_quota =
                        run.lease.byte_quota.saturating_sub(record.encoded_bytes);
                    continue;
                }
            }
            bulk.push(*record);
            indices.push(index);
        }
        if !bulk.is_empty() {
            let mut routed = vec![PartitionChoice::Pending; bulk.len()];
            let mut result = Err(RoutingError::PolicyFailed);
            kr_runtime::contain_panic(|| {
                result = policy.choose_partitions(&bulk, snapshot, &mut routed);
            });
            if result.is_err() {
                routed.fill(PartitionChoice::Partition(-1));
            }
            for (index, choice) in indices.into_iter().zip(routed) {
                choices[index] = if choice == PartitionChoice::Pending {
                    PartitionChoice::Partition(-1)
                } else {
                    choice
                };
            }
        }
        choices
    }

    pub(super) fn new(config: &crate::config::ProducerConfig) -> Result<Self, RoutingError> {
        Ok(Self {
            sticky: BTreeMap::new(),
            native: BTreeMap::new(),
            work: Work::new(row_limit(config), config.max_completions_per_poll as usize)?,
        })
    }

    /// Continue the same owned ingress slice or live pending-token slice. No
    /// choice/RNG/quota is consumed while collection is incomplete. Removing an
    /// expired pending token restarts only the unpublished advisory work.
    pub(super) fn choose<'a>(
        &mut self,
        handle: &RuntimeHandle,
        engine: &ProducerEngine,
        now: RuntimeInstant,
        records: impl Iterator<Item = &'a AdmittedRecord> + Clone,
    ) -> bool {
        let work = &mut self.work;
        let unchanged = work.active
            && work.tokens.len() == records.clone().count()
            && work
                .tokens
                .iter()
                .zip(records.clone())
                .all(|(token, record)| *token == record.token);
        if !unchanged {
            work.active = false;
            work.entries.clear();
            work.tokens.clear();
            work.choices.clear();
            for (index, record) in records.enumerate() {
                assert!(index < work.maximum_records, "actor routing bulk bound");
                work.tokens.push(record.token);
                work.choices.push(PartitionChoice::Pending);
                work.entries.push(Entry {
                    index,
                    deadline: record.deadline,
                    metadata: RecordMetadata {
                        topic: record.topic,
                        key_hash: record
                            .record
                            .key
                            .as_ref()
                            .filter(|_| record.partition_hint.is_none())
                            .map(|key| murmur2(key.as_slice())),
                        encoded_bytes: record.standalone_encoded_bytes,
                        partition_hint: record.partition_hint,
                    },
                });
            }
        }
        work.live.clear();
        work.live_indices.clear();
        for entry in &work.entries {
            if entry.deadline > now && !engine.is_failed() {
                work.live.push(entry.metadata);
                work.live_indices.push(entry.index);
            }
        }
        if work.live.is_empty() {
            work.active = false;
            return true;
        }
        let identity = |handle| topic_identity(engine, handle);
        let native = matches!(engine.config().partitioner, PartitionerConfig::Native(_));
        if !work.active {
            if native
                && cached_native_into(
                    &mut self.native,
                    &work.live,
                    identity,
                    &mut work.routed,
                    &mut work.quotas,
                )
            {
                work.map_choices();
                return true;
            }
            let needs_snapshot = |record: &RecordMetadata| {
                native
                    || (matches!(engine.config().partitioner, PartitionerConfig::Builtin)
                        && matches!(
                            engine.config().unkeyed_policy,
                            crate::routing::UnkeyedPolicy::Adaptive { .. }
                        )
                        && record.partition_hint.is_none()
                        && record.key_hash.is_none())
            };
            if work
                .collector
                .start(
                    now,
                    &work.live,
                    engine.config().unkeyed_policy.run_bytes(),
                    needs_snapshot,
                    identity,
                )
                .is_err()
            {
                work.fail_choices();
                return true;
            }
            work.active = true;
        }
        match work.collector.step(
            engine.config().max_completions_per_poll as usize,
            identity,
            |topic, partition| engine.partition_snapshot(now, TopicPartition { topic, partition }),
        ) {
            Step::Pending => return false,
            Step::Invalidated => {
                work.active = false;
                return false;
            }
            Step::Collected => {}
        }
        if !work.collector.valid(identity) {
            work.active = false;
            return false;
        }
        if let PartitionerConfig::Native(policy) = &engine.config().partitioner {
            let Ok(topics) = work.collector.views() else {
                work.fail_choices();
                return true;
            };
            work.view_peak_bytes = work
                .view_peak_bytes
                .max(topics.capacity() * size_of::<TopicSnapshot<'_>>());
            let snapshot = RoutingSnapshot {
                now,
                topics: &topics,
            };
            native_into(
                &mut self.native,
                policy.as_ref(),
                &work.live,
                snapshot,
                work.collector.collection(now),
                &mut work.routed,
                &mut work.bulk,
                &mut work.bulk_indices,
                &mut work.bulk_choices,
                &mut work.declined,
                &mut work.failed,
            );
        } else {
            work.routed.clear();
            for record in &work.live {
                let Some(topic) = identity(record.topic) else {
                    work.routed.push(PartitionChoice::Pending);
                    continue;
                };
                let choice = if let Some(partition) = record.partition_hint {
                    PartitionChoice::Partition(partition)
                } else if matches!(engine.config().partitioner, PartitionerConfig::External) {
                    PartitionChoice::Partition(-1)
                } else if let Some(hash) = record.key_hash {
                    if topic.partitions == 0 {
                        PartitionChoice::Partition(-1)
                    } else {
                        PartitionChoice::Partition(
                            ((hash & 0x7fff_ffff) % topic.partitions as u32) as i32,
                        )
                    }
                } else {
                    let router = self.sticky.entry(record.topic).or_default();
                    // Preserve one explicit draw for every live unkeyed record,
                    // including continuation of an already admitted sticky run.
                    let lease = handle.random_u64().ok().and_then(|draw| {
                        match engine.config().unkeyed_policy {
                            crate::routing::UnkeyedPolicy::UniformBytes { run_bytes } => {
                                router.preview_uniform(topic.id, topic.partitions, run_bytes, draw)
                            }
                            crate::routing::UnkeyedPolicy::Adaptive { run_bytes } => {
                                let (topic, prefix) = work.collector.topic(record.topic)?;
                                router.preview_adaptive_indexed(topic, prefix, run_bytes, draw)
                            }
                        }
                        .ok()
                    });
                    match lease {
                        Some(lease) if router.commit(lease, record.encoded_bytes).is_ok() => {
                            PartitionChoice::Partition(lease.partition)
                        }
                        _ => PartitionChoice::Partition(-1),
                    }
                };
                work.routed.push(choice);
            }
        }
        work.map_choices();
        work.active = false;
        true
    }
    pub(super) fn reject_records(&mut self, count: usize) {
        self.work.active = false;
        self.work.choices.clear();
        self.work.choices.resize(count, PartitionChoice::Pending);
    }
    pub(super) fn choices(&self) -> &[PartitionChoice] {
        &self.work.choices
    }
    pub(super) fn reset(&mut self) {
        self.work.active = false;
    }
    pub(super) fn metadata_capacity_bytes(&self) -> usize {
        self.work.metadata_capacity_bytes()
    }
    pub(super) fn callback_view_peak_bytes(&self) -> usize {
        self.work.view_peak_bytes
    }
}

/// Exact retained routing backing after successful construction, excluding
/// ordered sticky/run indexes and
/// the separately measured bounded callback-view array.
pub(crate) fn configured_storage_bytes(config: &crate::config::ProducerConfig) -> Option<usize> {
    let count = config.max_completions_per_poll as usize;
    Work::configured_storage_bytes(row_limit(config), count)?
        .checked_add(count.checked_mul(size_of::<AdmittedRecord>() + size_of::<RecordToken>())?)
}

fn row_limit(config: &crate::config::ProducerConfig) -> usize {
    if matches!(config.partitioner, PartitionerConfig::Native(_))
        || matches!(config.partitioner, PartitionerConfig::Builtin)
            && matches!(
                config.unkeyed_policy,
                crate::routing::UnkeyedPolicy::Adaptive { .. }
            )
    {
        config.max_batches as usize
    } else {
        0
    }
}

#[cfg(test)]
impl Default for Routing {
    fn default() -> Self {
        Self {
            sticky: BTreeMap::new(),
            native: BTreeMap::new(),
            work: Work::new(128, 128).unwrap(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::routing::{PartitionSnapshot, RoutingError};
    use std::sync::atomic::{AtomicUsize, Ordering};

    struct Policy {
        runs: AtomicUsize,
        bulks: std::sync::Mutex<Vec<Vec<RecordMetadata>>>,
        mode: u8,
    }
    impl Policy {
        fn new(mode: u8) -> Self {
            Self {
                runs: AtomicUsize::new(0),
                bulks: Default::default(),
                mode,
            }
        }
    }
    impl NativePartitioner for Policy {
        fn choose_partitions(
            &self,
            records: &[RecordMetadata],
            _: RoutingSnapshot<'_>,
            out: &mut [PartitionChoice],
        ) -> Result<(), RoutingError> {
            self.bulks.lock().unwrap().push(records.to_vec());
            out.fill(PartitionChoice::Partition(2));
            Ok(())
        }
        fn choose_run(
            &self,
            topic: TopicSnapshot<'_>,
            _: RoutingSnapshot<'_>,
        ) -> Result<Option<RoutingLease>, RoutingError> {
            let run = self.runs.fetch_add(1, Ordering::Relaxed);
            match self.mode {
                1 => return Ok(None),
                2 => panic!("partitioner panic"),
                _ => {}
            }
            Ok(Some(RoutingLease {
                topic: if self.mode == 3 {
                    TopicId([9; 16])
                } else {
                    topic.id
                },
                partition: if self.mode == 4 { 3 } else { (run % 3) as i32 },
                byte_quota: if self.mode == 5 { 0 } else { 100 },
            }))
        }
    }
    fn parts() -> [PartitionSnapshot; 3] {
        std::array::from_fn(|index| PartitionSnapshot {
            partition: index as i32,
            lane: 0,
            queued_bytes: 0,
            oldest_age: RuntimeDuration::ZERO,
            open_batch_bytes: 0,
            throttled_until: RuntimeInstant::ZERO,
            drain_bytes_per_second: 0,
            available: true,
        })
    }
    fn record(bytes: u32) -> RecordMetadata {
        RecordMetadata {
            topic: TopicHandle(1),
            key_hash: None,
            encoded_bytes: bytes,
            partition_hint: None,
        }
    }
    fn topic(parts: &[PartitionSnapshot], generation: u32) -> TopicSnapshot<'_> {
        TopicSnapshot {
            handle: TopicHandle(1),
            id: TopicId([1; 16]),
            generation,
            partitions: parts,
        }
    }
    #[test]
    fn cached_native_bulk_uses_identity_only_and_fallback_does_not_consume_quota() {
        let id = TopicId([1; 16]);
        let mut routing = Routing::default();
        routing.native.insert(
            TopicHandle(1),
            NativeRun {
                generation: 3,
                lease: RoutingLease {
                    topic: id,
                    partition: 2,
                    byte_quota: 100,
                },
            },
        );
        let identity = |handle| {
            (handle == TopicHandle(1)).then_some(TopicIdentity {
                id,
                generation: 3,
                partitions: i32::MAX as usize,
            })
        };
        assert!(
            routing
                .choose_cached_native(&[record(100), record(1)], identity)
                .is_none()
        );
        assert_eq!(routing.native[&TopicHandle(1)].lease.byte_quota, 100);
        assert!(
            routing
                .choose_cached_native(
                    &[RecordMetadata {
                        key_hash: Some(0),
                        ..record(1)
                    }],
                    identity
                )
                .is_none()
        );
        assert_eq!(routing.native[&TopicHandle(1)].lease.byte_quota, 100);
        let records = [
            record(30),
            RecordMetadata {
                partition_hint: Some(9),
                key_hash: Some(0),
                ..record(999)
            },
            RecordMetadata {
                topic: TopicHandle(2),
                ..record(999)
            },
            record(80),
        ];
        assert_eq!(
            routing.choose_cached_native(&records, identity).unwrap(),
            [
                PartitionChoice::Partition(2),
                PartitionChoice::Partition(9),
                PartitionChoice::Pending,
                PartitionChoice::Partition(2)
            ]
        );
        assert_eq!(routing.native[&TopicHandle(1)].lease.byte_quota, 0);
        assert!(
            routing
                .choose_cached_native(&[record(1)], identity)
                .is_none()
        );
        routing
            .native
            .get_mut(&TopicHandle(1))
            .unwrap()
            .lease
            .byte_quota = 100;
        for changed in [
            TopicIdentity {
                generation: 4,
                ..identity(TopicHandle(1)).unwrap()
            },
            TopicIdentity {
                id: TopicId([2; 16]),
                ..identity(TopicHandle(1)).unwrap()
            },
            TopicIdentity {
                partitions: 2,
                ..identity(TopicHandle(1)).unwrap()
            },
        ] {
            assert!(
                routing
                    .choose_cached_native(&[record(1)], |_| Some(changed))
                    .is_none()
            );
            assert_eq!(routing.native[&TopicHandle(1)].lease.byte_quota, 100);
        }
    }

    #[test]
    fn buffered_native_default_hooks_preserve_bulk_run_and_quota_semantics() {
        let parts = parts();
        let topics = [topic(&parts, 1)];
        let snapshot = RoutingSnapshot {
            now: RuntimeInstant::from_nanos(9),
            topics: &topics,
        };
        let collection = SnapshotCollection {
            started_at: RuntimeInstant::from_nanos(1),
            completed_at: snapshot.now,
        };
        let records: Vec<_> = (0..71)
            .map(|index| RecordMetadata {
                key_hash: (index % 7 == 0).then_some(murmur2(b"key")),
                partition_hint: (index % 13 == 0).then_some(1),
                ..record(7 + (index * 13 % 67))
            })
            .collect();
        for mode in [0, 1, 3, 4, 5] {
            for chunk_size in [1, 2, 7, 71] {
                let legacy_policy = Policy::new(mode);
                let buffered_policy = Policy::new(mode);
                let mut legacy = Routing::default();
                let mut native = BTreeMap::new();
                let mut work = Work::new(3, 71).unwrap();
                let capacity = work.metadata_capacity_bytes();
                for chunk in records.chunks(chunk_size) {
                    let expected = legacy.choose_native(&legacy_policy, chunk, snapshot);
                    native_into(
                        &mut native,
                        &buffered_policy,
                        chunk,
                        snapshot,
                        collection,
                        &mut work.routed,
                        &mut work.bulk,
                        &mut work.bulk_indices,
                        &mut work.bulk_choices,
                        &mut work.declined,
                        &mut work.failed,
                    );
                    assert_eq!(work.routed, expected, "mode={mode}, chunk={chunk_size}");
                    assert_eq!(work.metadata_capacity_bytes(), capacity);
                }
                assert_eq!(
                    legacy_policy.runs.load(Ordering::Relaxed),
                    buffered_policy.runs.load(Ordering::Relaxed)
                );
                assert_eq!(
                    legacy_policy
                        .bulks
                        .lock()
                        .unwrap()
                        .iter()
                        .map(Vec::len)
                        .collect::<Vec<_>>(),
                    buffered_policy
                        .bulks
                        .lock()
                        .unwrap()
                        .iter()
                        .map(Vec::len)
                        .collect::<Vec<_>>()
                );
            }
        }
    }

    #[test]
    fn native_runs_follow_admitted_bytes_across_bulk_boundaries() {
        let parts = parts();
        let topics = [topic(&parts, 1)];
        let snapshot = RoutingSnapshot {
            now: RuntimeInstant::ZERO,
            topics: &topics,
        };
        let records: Vec<_> = (0..71).map(|i| record(7 + (i * 13 % 67))).collect();
        let mut expected = Vec::new();
        let mut remaining = 0_u32;
        let mut runs = 0;
        for record in &records {
            if remaining == 0 {
                remaining = 100;
                runs += 1;
            }
            expected.push(PartitionChoice::Partition((runs - 1) % 3));
            remaining = remaining.saturating_sub(record.encoded_bytes);
        }
        for chunk_size in [1, 2, 7, 71] {
            let policy = Policy::new(0);
            let mut routing = Routing::default();
            let actual: Vec<_> = records
                .chunks(chunk_size)
                .flat_map(|chunk| routing.choose_native(&policy, chunk, snapshot))
                .collect();
            assert_eq!(actual, expected, "chunk={chunk_size}");
            assert_eq!(policy.runs.load(Ordering::Relaxed), runs as usize);
            assert!(policy.bulks.lock().unwrap().is_empty());
        }
    }
    #[test]
    fn native_fallback_is_one_bulk_and_preserves_key_and_hint_semantics() {
        let parts = parts();
        let topics = [topic(&parts, 1)];
        let snapshot = RoutingSnapshot {
            now: RuntimeInstant::ZERO,
            topics: &topics,
        };
        let policy = Policy::new(1);
        let records = [
            record(10),
            record(20),
            RecordMetadata {
                key_hash: Some(murmur2(b"")),
                ..record(30)
            },
            RecordMetadata {
                partition_hint: Some(1),
                ..record(40)
            },
            RecordMetadata {
                topic: TopicHandle(2),
                ..record(50)
            },
        ];
        let actual = Routing::default().choose_native(&policy, &records, snapshot);
        assert_eq!(
            actual,
            [
                PartitionChoice::Partition(2),
                PartitionChoice::Partition(2),
                PartitionChoice::Partition(2),
                PartitionChoice::Partition(1),
                PartitionChoice::Pending
            ]
        );
        assert_eq!(policy.runs.load(Ordering::Relaxed), 1);
        let bulks = policy.bulks.lock().unwrap();
        assert_eq!(bulks.len(), 1);
        assert_eq!(bulks[0].len(), 3);
        assert!(bulks[0][2].key_hash.is_some());
    }
    #[test]
    fn native_generation_change_and_close_discard_unused_quota() {
        let parts = parts();
        let policy = Policy::new(0);
        let mut routing = Routing::default();
        for (generation, expected) in [(1, 0), (1, 0), (2, 1)] {
            let topics = [topic(&parts, generation)];
            assert_eq!(
                routing.choose_native(
                    &policy,
                    &[record(1)],
                    RoutingSnapshot {
                        now: RuntimeInstant::ZERO,
                        topics: &topics
                    }
                ),
                [PartitionChoice::Partition(expected)]
            );
        }
        routing.forget(TopicHandle(1));
        assert!(routing.native.is_empty());
        let topics = [topic(&parts, 2)];
        assert_eq!(
            routing.choose_native(
                &policy,
                &[record(1)],
                RoutingSnapshot {
                    now: RuntimeInstant::ZERO,
                    topics: &topics
                }
            ),
            [PartitionChoice::Partition(2)]
        );
    }
    #[test]
    fn invalid_or_panicking_run_is_contained_and_never_falls_back() {
        let parts = parts();
        let topics = [topic(&parts, 1)];
        let snapshot = RoutingSnapshot {
            now: RuntimeInstant::ZERO,
            topics: &topics,
        };
        for mode in 2..=5 {
            let policy = Policy::new(mode);
            let actual = Routing::default().choose_native(&policy, &[record(1); 4], snapshot);
            assert_eq!(actual, [PartitionChoice::Partition(-1); 4]);
            assert_eq!(policy.runs.load(Ordering::Relaxed), 1);
            assert!(policy.bulks.lock().unwrap().is_empty());
        }
    }

    #[test]
    fn policy_panic_payload_destructors_cannot_escape_the_routing_boundary() {
        struct Payload(Arc<AtomicUsize>);
        impl Drop for Payload {
            fn drop(&mut self) {
                self.0.fetch_add(1, Ordering::Relaxed);
                panic!("panic payload destructor");
            }
        }
        struct Adversarial {
            bulk: bool,
            dropped: Arc<AtomicUsize>,
        }
        impl NativePartitioner for Adversarial {
            fn choose_run(
                &self,
                _: TopicSnapshot<'_>,
                _: RoutingSnapshot<'_>,
            ) -> Result<Option<RoutingLease>, RoutingError> {
                if self.bulk {
                    Ok(None)
                } else {
                    std::panic::panic_any(Payload(self.dropped.clone()))
                }
            }
            fn choose_partitions(
                &self,
                _: &[RecordMetadata],
                _: RoutingSnapshot<'_>,
                choices: &mut [PartitionChoice],
            ) -> Result<(), RoutingError> {
                choices.fill(PartitionChoice::Partition(0));
                std::panic::panic_any(Payload(self.dropped.clone()))
            }
        }
        let parts = parts();
        let topics = [topic(&parts, 1)];
        let snapshot = RoutingSnapshot {
            now: RuntimeInstant::ZERO,
            topics: &topics,
        };
        for bulk in [false, true] {
            let dropped = Arc::new(AtomicUsize::new(0));
            let policy = Adversarial {
                bulk,
                dropped: dropped.clone(),
            };
            let result = std::panic::catch_unwind(|| {
                Routing::default().choose_native(&policy, &[record(1); 4], snapshot)
            });
            assert_eq!(result.unwrap(), [PartitionChoice::Partition(-1); 4]);
            assert_eq!(dropped.load(Ordering::Relaxed), 1);
            assert_eq!(
                Routing::default().choose_native(&Policy::new(0), &[record(1)], snapshot),
                [PartitionChoice::Partition(0)]
            );
        }
    }
}
