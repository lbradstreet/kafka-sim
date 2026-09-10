use super::*;

#[derive(Clone, Copy)]
pub(super) struct Entry {
    pub index: usize,
    pub deadline: RuntimeInstant,
    pub metadata: RecordMetadata,
}

pub(super) struct Work {
    pub collector: Collector,
    pub maximum_records: usize,
    pub active: bool,
    pub entries: Vec<Entry>,
    pub tokens: Vec<RecordToken>,
    pub choices: Vec<PartitionChoice>,
    pub live: Vec<RecordMetadata>,
    pub live_indices: Vec<usize>,
    pub routed: Vec<PartitionChoice>,
    pub bulk: Vec<RecordMetadata>,
    pub bulk_indices: Vec<usize>,
    pub bulk_choices: Vec<PartitionChoice>,
    pub declined: Vec<TopicHandle>,
    pub failed: Vec<TopicHandle>,
    pub quotas: Vec<(TopicHandle, u32)>,
    pub view_peak_bytes: usize,
}
fn reserve<T>(count: usize) -> Result<Vec<T>, RoutingError> {
    crate::fixed::try_vec(count).map_err(|_| RoutingError::PolicyFailed)
}

impl Work {
    pub(super) fn new(rows: usize, count: usize) -> Result<Self, RoutingError> {
        Ok(Self {
            collector: Collector::new(rows, count)?,
            maximum_records: count,
            active: false,
            entries: reserve(count)?,
            tokens: reserve(count)?,
            choices: reserve(count)?,
            live: reserve(count)?,
            live_indices: reserve(count)?,
            routed: reserve(count)?,
            bulk: reserve(count)?,
            bulk_indices: reserve(count)?,
            bulk_choices: reserve(count)?,
            declined: reserve(count)?,
            failed: reserve(count)?,
            quotas: reserve(count)?,
            view_peak_bytes: 0,
        })
    }
    pub(super) fn map_choices(&mut self) {
        for (&index, choice) in self.live_indices.iter().zip(&self.routed) {
            self.choices[index] = *choice;
        }
    }
    pub(super) fn fail_choices(&mut self) {
        for &index in &self.live_indices {
            self.choices[index] = PartitionChoice::Partition(-1);
        }
        self.active = false;
    }
    pub(super) fn configured_storage_bytes(rows: usize, count: usize) -> Option<usize> {
        let per_record = size_of::<Entry>()
            + size_of::<RecordToken>()
            + 3 * size_of::<PartitionChoice>()
            + 2 * size_of::<RecordMetadata>()
            + 2 * size_of::<usize>()
            + 2 * size_of::<TopicHandle>()
            + size_of::<(TopicHandle, u32)>();
        Collector::configured_storage_bytes(rows, count)?
            .checked_add(count.checked_mul(per_record)?)
    }
    pub(super) fn metadata_capacity_bytes(&self) -> usize {
        self.collector.metadata_capacity_bytes()
            + self.entries.capacity() * size_of::<Entry>()
            + self.tokens.capacity() * size_of::<RecordToken>()
            + self.choices.capacity() * size_of::<PartitionChoice>()
            + self.live.capacity() * size_of::<RecordMetadata>()
            + self.live_indices.capacity() * size_of::<usize>()
            + self.routed.capacity() * size_of::<PartitionChoice>()
            + self.bulk.capacity() * size_of::<RecordMetadata>()
            + self.bulk_indices.capacity() * size_of::<usize>()
            + self.bulk_choices.capacity() * size_of::<PartitionChoice>()
            + self.declined.capacity() * size_of::<TopicHandle>()
            + self.failed.capacity() * size_of::<TopicHandle>()
            + self.quotas.capacity() * size_of::<(TopicHandle, u32)>()
    }
}

pub(super) fn topic_identity(
    engine: &ProducerEngine,
    handle: TopicHandle,
) -> Option<TopicIdentity> {
    let topic = engine.topics().get(handle).ok()?;
    (topic.state == TopicState::Ready).then_some(TopicIdentity {
        id: topic.id?,
        generation: topic.generation,
        partitions: topic.partitions.len(),
    })
}

pub(super) fn cached_native_into(
    native: &mut BTreeMap<TopicHandle, NativeRun>,
    records: &[RecordMetadata],
    mut identity: impl FnMut(TopicHandle) -> Option<TopicIdentity>,
    choices: &mut Vec<PartitionChoice>,
    quotas: &mut Vec<(TopicHandle, u32)>,
) -> bool {
    choices.clear();
    quotas.clear();
    for record in records {
        let Some(topic) = identity(record.topic) else {
            choices.push(PartitionChoice::Pending);
            continue;
        };
        if let Some(hint) = record.partition_hint {
            choices.push(PartitionChoice::Partition(hint));
            continue;
        }
        if record.key_hash.is_some() {
            return false;
        }
        let Some(run) = native.get(&record.topic) else {
            return false;
        };
        if run.generation != topic.generation
            || run.lease.topic != topic.id
            || run.lease.partition < 0
            || run.lease.partition as usize >= topic.partitions
        {
            return false;
        }
        let index = match quotas.binary_search_by_key(&record.topic, |(topic, _)| *topic) {
            Ok(index) => index,
            Err(index) => {
                quotas.insert(index, (record.topic, run.lease.byte_quota));
                index
            }
        };
        let quota = &mut quotas[index].1;
        if *quota == 0 {
            return false;
        }
        *quota = quota.saturating_sub(record.encoded_bytes);
        choices.push(PartitionChoice::Partition(run.lease.partition));
    }
    for &(topic, quota) in quotas.iter() {
        native
            .get_mut(&topic)
            .expect("validated cached run")
            .lease
            .byte_quota = quota;
    }
    true
}

#[allow(clippy::too_many_arguments)]
pub(super) fn native_into(
    native: &mut BTreeMap<TopicHandle, NativeRun>,
    policy: &dyn NativePartitioner,
    records: &[RecordMetadata],
    snapshot: RoutingSnapshot<'_>,
    collection: SnapshotCollection,
    choices: &mut Vec<PartitionChoice>,
    bulk: &mut Vec<RecordMetadata>,
    indices: &mut Vec<usize>,
    routed: &mut Vec<PartitionChoice>,
    declined: &mut Vec<TopicHandle>,
    failed: &mut Vec<TopicHandle>,
) {
    choices.clear();
    choices.resize(records.len(), PartitionChoice::Pending);
    bulk.clear();
    indices.clear();
    declined.clear();
    failed.clear();
    for (index, record) in records.iter().enumerate() {
        let Ok(topic_index) = snapshot
            .topics
            .binary_search_by_key(&record.topic, |topic| topic.handle)
        else {
            continue;
        };
        let topic = snapshot.topics[topic_index];
        if let Some(hint) = record.partition_hint {
            choices[index] = PartitionChoice::Partition(hint);
            continue;
        }
        if record.key_hash.is_none() {
            if failed.contains(&record.topic) {
                choices[index] = PartitionChoice::Partition(-1);
                continue;
            }
            let valid = native.get(&record.topic).is_some_and(|run| {
                run.generation == topic.generation
                    && run.lease.topic == topic.id
                    && run.lease.byte_quota > 0
                    && run.lease.partition >= 0
                    && (run.lease.partition as usize) < topic.partitions.len()
            });
            if !valid {
                native.remove(&record.topic);
                if !declined.contains(&record.topic) {
                    let mut result = Err(RoutingError::PolicyFailed);
                    kr_runtime::contain_panic(|| {
                        result = policy.choose_run_collected(topic, snapshot, collection)
                    });
                    match result {
                        Ok(Some(lease))
                            if lease.topic == topic.id
                                && lease.byte_quota > 0
                                && lease.partition >= 0
                                && (lease.partition as usize) < topic.partitions.len() =>
                        {
                            native.insert(
                                record.topic,
                                NativeRun {
                                    generation: topic.generation,
                                    lease,
                                },
                            );
                        }
                        Ok(None) => declined.push(record.topic),
                        _ => {
                            failed.push(record.topic);
                            choices[index] = PartitionChoice::Partition(-1);
                            continue;
                        }
                    }
                }
            }
            if let Some(run) = native.get_mut(&record.topic) {
                choices[index] = PartitionChoice::Partition(run.lease.partition);
                run.lease.byte_quota = run.lease.byte_quota.saturating_sub(record.encoded_bytes);
                continue;
            }
        }
        bulk.push(*record);
        indices.push(index);
    }
    if !bulk.is_empty() {
        routed.clear();
        routed.resize(bulk.len(), PartitionChoice::Pending);
        let mut result = Err(RoutingError::PolicyFailed);
        kr_runtime::contain_panic(|| {
            result = policy.choose_partitions_collected(bulk, snapshot, collection, routed)
        });
        if result.is_err() {
            routed.fill(PartitionChoice::Partition(-1));
        }
        for (&index, &choice) in indices.iter().zip(routed.iter()) {
            choices[index] = if choice == PartitionChoice::Pending {
                PartitionChoice::Partition(-1)
            } else {
                choice
            };
        }
    }
}

#[cfg(test)]
mod storage_tests {
    use super::*;

    #[test]
    fn all_retained_routing_vectors_equal_the_configured_backing() {
        for (rows, count) in [(0, 0), (1, 1), (129, 7), (4097, 128)] {
            let work = Work::new(rows, count).unwrap();
            assert_eq!(
                Some(work.metadata_capacity_bytes()),
                Work::configured_storage_bytes(rows, count)
            );
            assert_eq!(work.view_peak_bytes, 0);
            assert_eq!(work.entries.len(), 0);
            assert_eq!(work.choices.len(), 0);
        }
        assert!(Work::new(usize::MAX, 1).is_err());
        assert!(Work::new(1, usize::MAX).is_err());
        assert!(Work::configured_storage_bytes(usize::MAX, 1).is_none());
        assert!(Work::configured_storage_bytes(1, usize::MAX).is_none());
    }
}
