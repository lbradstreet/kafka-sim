//! Bounded metadata cache whose handles bind once to an immutable Kafka UUID.
use crate::types::{TopicHandle, TopicId};
use kr_runtime::{RuntimeDuration, RuntimeInstant};
use std::{collections::BTreeMap, fmt};

mod snapshot;
mod update;
pub use snapshot::{MetadataBrokers, MetadataSnapshot};
pub(crate) use update::{TopicUpdate, UpdateProgress};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TopicState {
    Resolving,
    Ready,
    Deleted,
    Failed,
}

pub use kr_kafka_client::types::{MetadataSelector, PartitionMetadata};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TopicMetadata {
    pub snapshot: Option<std::sync::Arc<MetadataSnapshot>>,
    pub handle: TopicHandle,
    pub name: String,
    pub id: Option<TopicId>,
    pub state: TopicState,
    pub generation: u32,
    pub partitions: Vec<PartitionMetadata>,
    pub resolution_deadline: RuntimeInstant,
    pub refresh_at: RuntimeInstant,
}

#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum TopicError {
    InvalidName,
    AlreadyOpen,
    TopicClosed,
    TopicDeleted,
    TopicFailed,
    ResourceExhausted,
    TokenExhausted,
    TimeOverflow,
    InvalidIdentity,
    IdentityChanged,
    IdentityAlreadyOpen,
    InvalidPartitions,
    PartitionShrink,
    StaleLeaderEpoch,
    GenerationExhausted,
    AllocationFailed,
}
impl fmt::Display for TopicError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::InvalidName => "invalid Kafka topic name",
            Self::AlreadyOpen => "topic name is already open",
            Self::TopicClosed => "topic handle is closed",
            Self::TopicDeleted => "topic identity was deleted",
            Self::TopicFailed => "topic resolution failed",
            Self::ResourceExhausted => "metadata cache capacity exhausted",
            Self::TokenExhausted => "topic handle space exhausted",
            Self::TimeOverflow => "metadata deadline overflow",
            Self::InvalidIdentity => "zero Kafka topic identity",
            Self::IdentityChanged => "topic handle cannot change identity",
            Self::IdentityAlreadyOpen => "topic identity already has an open handle",
            Self::InvalidPartitions => "invalid partition metadata",
            Self::PartitionShrink => "topic partition count cannot shrink",
            Self::StaleLeaderEpoch => "leader epoch moved backwards",
            Self::GenerationExhausted => "topic metadata generation exhausted",
            Self::AllocationFailed => "metadata allocation failed",
        })
    }
}
impl std::error::Error for TopicError {}

/// Names are only used for the first resolution. Closing removes the live slot;
/// its monotonically allocated handle is never reused, including after failure.
#[derive(Debug)]
pub struct TopicCache {
    topics: BTreeMap<TopicHandle, TopicMetadata>,
    names: BTreeMap<String, TopicHandle>,
    ids: BTreeMap<TopicId, TopicHandle>,
    max_topics: usize,
    max_partitions: usize,
    partitions: usize,
    next_handle: u32,
    resolve_timeout: RuntimeDuration,
    max_age: RuntimeDuration,
}
impl TopicCache {
    /// Latches a validated control-plane failure without permitting a new UUID.
    pub fn mark_failed(&mut self, handle: TopicHandle, deleted: bool) -> Result<(), TopicError> {
        let topic = self
            .topics
            .get_mut(&handle)
            .ok_or(TopicError::TopicClosed)?;
        topic.state = if deleted && topic.id.is_some() {
            TopicState::Deleted
        } else {
            TopicState::Failed
        };
        Ok(())
    }
    /// Accepts a handle allocated under the client admission lock. Handles must
    /// arrive in strictly increasing order; gaps are allowed and never reused.
    /// # Errors
    /// Rejection leaves the cache and its ordinary handle allocator unchanged.
    pub fn open_reserved(
        &mut self,
        handle: TopicHandle,
        name: &str,
        now: RuntimeInstant,
    ) -> Result<TopicHandle, TopicError> {
        if handle.0 < self.next_handle || handle.0 == u32::MAX {
            return Err(TopicError::TokenExhausted);
        }
        let previous = self.next_handle;
        self.next_handle = handle.0;
        let result = self.open(name, now);
        if result.is_err() {
            self.next_handle = previous;
        }
        result
    }
    /// # Errors
    /// Rejects zero capacity or zero time limits before opening any topic.
    pub fn new(
        max_topics: usize,
        max_partitions: usize,
        resolve_timeout: RuntimeDuration,
        max_age: RuntimeDuration,
    ) -> Result<Self, TopicError> {
        if max_topics == 0
            || max_partitions == 0
            || resolve_timeout == RuntimeDuration::ZERO
            || max_age == RuntimeDuration::ZERO
        {
            return Err(TopicError::ResourceExhausted);
        }
        Ok(Self {
            topics: BTreeMap::new(),
            names: BTreeMap::new(),
            ids: BTreeMap::new(),
            max_topics,
            max_partitions,
            partitions: 0,
            next_handle: 1,
            resolve_timeout,
            max_age,
        })
    }
    /// # Errors
    /// Rejects invalid/already-open names and capacity, time, or handle overflow.
    pub fn open(&mut self, name: &str, now: RuntimeInstant) -> Result<TopicHandle, TopicError> {
        if name.is_empty()
            || name.len() > 249
            || name == "."
            || name == ".."
            || !name
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b"._-".contains(&b))
        {
            return Err(TopicError::InvalidName);
        }
        if self.names.contains_key(name) {
            return Err(TopicError::AlreadyOpen);
        }
        if self.topics.len() == self.max_topics {
            return Err(TopicError::ResourceExhausted);
        }
        let next = self
            .next_handle
            .checked_add(1)
            .ok_or(TopicError::TokenExhausted)?;
        let resolution_deadline = now
            .checked_add(self.resolve_timeout)
            .ok_or(TopicError::TimeOverflow)?;
        let handle = TopicHandle(self.next_handle);
        let topic = TopicMetadata {
            snapshot: None,
            handle,
            name: name.into(),
            id: None,
            state: TopicState::Resolving,
            generation: 0,
            partitions: Vec::new(),
            resolution_deadline,
            refresh_at: now,
        };
        self.topics.insert(handle, topic);
        self.names.insert(name.into(), handle);
        self.next_handle = next;
        Ok(handle)
    }
    /// The caller must first fence the handle's records and retain their captured
    /// identity and event obligations until resumable settlement completes.
    /// # Errors
    /// A stale/closed handle has no effect on another topic.
    pub fn close(&mut self, handle: TopicHandle) -> Result<TopicMetadata, TopicError> {
        let topic = self.topics.remove(&handle).ok_or(TopicError::TopicClosed)?;
        self.names.remove(&topic.name);
        if let Some(id) = topic.id {
            self.ids.remove(&id);
        }
        self.partitions -= topic.partitions.len();
        Ok(topic)
    }
    /// # Errors
    /// Rejects a closed handle. Deleted/failed entries remain inspectable.
    pub fn get(&self, handle: TopicHandle) -> Result<&TopicMetadata, TopicError> {
        self.topics.get(&handle).ok_or(TopicError::TopicClosed)
    }
    pub(crate) fn install_snapshot(
        &mut self,
        handle: TopicHandle,
        snapshot: std::sync::Arc<MetadataSnapshot>,
    ) {
        if let Some(topic) = self.topics.get_mut(&handle) {
            topic.snapshot = Some(snapshot);
        }
    }
    #[must_use]
    pub fn by_id(&self, id: TopicId) -> Option<TopicHandle> {
        self.ids.get(&id).copied()
    }
    #[must_use]
    pub fn len(&self) -> usize {
        self.topics.len()
    }
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.topics.is_empty()
    }
    #[must_use]
    pub fn partition_count(&self) -> usize {
        self.partitions
    }
    pub fn iter(&self) -> impl Iterator<Item = &TopicMetadata> {
        self.topics.values()
    }
    /// # Errors
    /// Terminal entries cannot be refreshed and can never be rebound by name.
    pub fn selector(&self, handle: TopicHandle) -> Result<MetadataSelector<'_>, TopicError> {
        let topic = self.get(handle)?;
        Self::live(topic)?;
        Ok(match topic.id {
            Some(id) => MetadataSelector::Id(id),
            None => MetadataSelector::Name(&topic.name),
        })
    }
    fn live(topic: &TopicMetadata) -> Result<(), TopicError> {
        match topic.state {
            TopicState::Resolving | TopicState::Ready => Ok(()),
            TopicState::Deleted => Err(TopicError::TopicDeleted),
            TopicState::Failed => Err(TopicError::TopicFailed),
        }
    }
    /// Atomically validates one complete topic snapshot. A failed update leaves
    /// the identity, leaders, generation and aggregate capacity unchanged.
    /// # Errors
    /// Rejects rebindings, shrinking arrays, stale epochs, invalid leaders and bounds.
    pub fn apply(
        &mut self,
        handle: TopicHandle,
        id: TopicId,
        partitions: &[PartitionMetadata],
        now: RuntimeInstant,
    ) -> Result<bool, TopicError> {
        let topic = self.get(handle)?;
        Self::live(topic)?;
        if id.is_zero() {
            return Err(TopicError::InvalidIdentity);
        }
        if topic.id.is_some_and(|old| old != id) {
            return Err(TopicError::IdentityChanged);
        }
        if self.ids.get(&id).is_some_and(|other| *other != handle) {
            return Err(TopicError::IdentityAlreadyOpen);
        }
        if partitions.is_empty()
            || partitions.len() > i32::MAX as usize
            || partitions
                .iter()
                .any(|p| p.leader < -1 || p.leader_epoch < -1)
        {
            return Err(TopicError::InvalidPartitions);
        }
        if partitions.len() < topic.partitions.len() {
            return Err(TopicError::PartitionShrink);
        }
        for (old, new) in topic.partitions.iter().zip(partitions) {
            if new.leader_epoch < old.leader_epoch
                || (new.leader_epoch == old.leader_epoch
                    && old.leader >= 0
                    && new.leader >= 0
                    && old.leader != new.leader)
            {
                return Err(TopicError::StaleLeaderEpoch);
            }
        }
        let total = self
            .partitions
            .checked_add(partitions.len() - topic.partitions.len())
            .ok_or(TopicError::ResourceExhausted)?;
        if total > self.max_partitions {
            return Err(TopicError::ResourceExhausted);
        }
        let changed = topic.id.is_none() || topic.partitions != partitions;
        let generation = if changed {
            topic
                .generation
                .checked_add(1)
                .ok_or(TopicError::GenerationExhausted)?
        } else {
            topic.generation
        };
        let refresh_at = now
            .checked_add(self.max_age)
            .ok_or(TopicError::TimeOverflow)?;
        let mut owned = Vec::new();
        owned
            .try_reserve_exact(partitions.len())
            .map_err(|_| TopicError::AllocationFailed)?;
        owned.extend_from_slice(partitions);
        let topic = self.topics.get_mut(&handle).expect("validated live handle");
        topic.id = Some(id);
        topic.state = TopicState::Ready;
        topic.generation = generation;
        topic.partitions = owned;
        topic.refresh_at = refresh_at;
        self.partitions = total;
        self.ids.insert(id, handle);
        Ok(changed)
    }
    /// KIP-951's current leader is accepted only for the same immutable UUID and
    /// a strictly newer epoch. An old in-flight response cannot roll routing back.
    /// # Errors
    /// Rejects unknown identity, partition, or invalid leader values.
    pub fn update_leader(
        &mut self,
        id: TopicId,
        partition: i32,
        leader: PartitionMetadata,
        now: RuntimeInstant,
    ) -> Result<bool, TopicError> {
        let handle = self.by_id(id).ok_or(TopicError::TopicClosed)?;
        let topic = self.get(handle)?;
        Self::live(topic)?;
        let old = topic
            .partitions
            .get(usize::try_from(partition).map_err(|_| TopicError::InvalidPartitions)?)
            .ok_or(TopicError::InvalidPartitions)?;
        if leader.leader < 0 || leader.leader_epoch < 0 {
            return Err(TopicError::InvalidPartitions);
        }
        if leader.leader_epoch <= old.leader_epoch {
            return Ok(false);
        }
        let generation = topic
            .generation
            .checked_add(1)
            .ok_or(TopicError::GenerationExhausted)?;
        let topic = self.topics.get_mut(&handle).expect("validated live handle");
        topic.partitions[partition as usize] = leader;
        topic.generation = generation;
        topic.refresh_at = now;
        Ok(true)
    }
    /// An unknown UUID is terminal, while an unknown initial name keeps resolving.
    /// # Errors
    /// Rejects closed or already terminal handles.
    pub fn unknown(
        &mut self,
        handle: TopicHandle,
        now: RuntimeInstant,
    ) -> Result<TopicState, TopicError> {
        let topic = self
            .topics
            .get_mut(&handle)
            .ok_or(TopicError::TopicClosed)?;
        Self::live(topic)?;
        if topic.id.is_some() {
            topic.state = TopicState::Deleted;
        } else if now >= topic.resolution_deadline {
            topic.state = TopicState::Failed;
        }
        Ok(topic.state)
    }
    /// # Errors
    /// Only live handles can request an immediate refresh.
    pub fn request_refresh(
        &mut self,
        handle: TopicHandle,
        now: RuntimeInstant,
    ) -> Result<(), TopicError> {
        let topic = self
            .topics
            .get_mut(&handle)
            .ok_or(TopicError::TopicClosed)?;
        Self::live(topic)?;
        topic.refresh_at = now;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn cache() -> TopicCache {
        TopicCache::new(
            2,
            4,
            RuntimeDuration::from_nanos(10),
            RuntimeDuration::from_nanos(100),
        )
        .unwrap()
    }
    fn parts(n: usize, leader: i32, epoch: i32) -> Vec<PartitionMetadata> {
        vec![
            PartitionMetadata {
                leader,
                leader_epoch: epoch
            };
            n
        ]
    }
    #[test]
    fn topic_name_never_rebinds_a_live_or_deleted_handle() {
        let mut c = cache();
        let h = c.open("orders", RuntimeInstant::ZERO).unwrap();
        assert_eq!(c.selector(h), Ok(MetadataSelector::Name("orders")));
        c.apply(h, TopicId([1; 16]), &parts(1, 1, 0), RuntimeInstant::ZERO)
            .unwrap();
        assert_eq!(c.selector(h), Ok(MetadataSelector::Id(TopicId([1; 16]))));
        assert_eq!(
            c.apply(h, TopicId([2; 16]), &parts(1, 1, 0), RuntimeInstant::ZERO),
            Err(TopicError::IdentityChanged)
        );
        assert_eq!(c.unknown(h, RuntimeInstant::ZERO), Ok(TopicState::Deleted));
        assert_eq!(
            c.apply(h, TopicId([2; 16]), &parts(1, 1, 0), RuntimeInstant::ZERO),
            Err(TopicError::TopicDeleted)
        );
        c.close(h).unwrap();
        let next = c.open("orders", RuntimeInstant::ZERO).unwrap();
        assert_ne!(h, next);
        c.apply(
            next,
            TopicId([2; 16]),
            &parts(1, 1, 0),
            RuntimeInstant::ZERO,
        )
        .unwrap();
        assert_eq!(c.get(h), Err(TopicError::TopicClosed));
    }
    #[test]
    fn expansion_and_leader_updates_are_atomic_and_monotonic() {
        let mut c = cache();
        let h = c.open("t", RuntimeInstant::ZERO).unwrap();
        let id = TopicId([1; 16]);
        c.apply(h, id, &parts(2, 1, 2), RuntimeInstant::ZERO)
            .unwrap();
        let before = c.get(h).unwrap().clone();
        for p in [parts(1, 1, 2), parts(2, 2, 1), parts(5, 1, 2)] {
            assert!(c.apply(h, id, &p, RuntimeInstant::ZERO).is_err());
            assert_eq!(c.get(h).unwrap(), &before);
            assert_eq!(c.partition_count(), 2);
        }
        c.apply(h, id, &parts(3, 2, 3), RuntimeInstant::ZERO)
            .unwrap();
        assert_eq!(c.get(h).unwrap().generation, 2);
        assert!(
            !c.update_leader(
                id,
                0,
                PartitionMetadata {
                    leader: 1,
                    leader_epoch: 2
                },
                RuntimeInstant::ZERO
            )
            .unwrap()
        );
        assert!(
            c.update_leader(
                id,
                0,
                PartitionMetadata {
                    leader: 3,
                    leader_epoch: 4
                },
                RuntimeInstant::ZERO
            )
            .unwrap()
        );
        assert_eq!(c.get(h).unwrap().partitions[0].leader, 3);
    }
    #[test]
    fn unresolved_deadline_and_closed_handle_exhaustion_are_exact() {
        let mut c = cache();
        let h = c.open("t", RuntimeInstant::ZERO).unwrap();
        assert_eq!(
            c.unknown(h, RuntimeInstant::from_nanos(9)),
            Ok(TopicState::Resolving)
        );
        assert_eq!(
            c.unknown(h, RuntimeInstant::from_nanos(10)),
            Ok(TopicState::Failed)
        );
        c.next_handle = u32::MAX;
        assert_eq!(
            c.open("u", RuntimeInstant::ZERO),
            Err(TopicError::TokenExhausted)
        );
        assert_eq!(c.len(), 1);
    }
}
