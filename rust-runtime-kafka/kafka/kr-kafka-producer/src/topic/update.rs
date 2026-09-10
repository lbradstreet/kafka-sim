//! Generation-checked, incremental preparation with one atomic topic commit.
use super::*;

pub(crate) struct TopicUpdate {
    handle: TopicHandle,
    id: TopicId,
    partitions: Vec<PartitionMetadata>,
    generation: u32,
    cursor: usize,
    changed: bool,
}
pub(crate) enum UpdateProgress {
    Pending,
    Applied { changed: bool },
}
impl TopicCache {
    fn update_start(
        &self,
        handle: TopicHandle,
        id: TopicId,
        count: usize,
    ) -> Result<(u32, bool), TopicError> {
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
        if count == 0 || count > i32::MAX as usize {
            return Err(TopicError::InvalidPartitions);
        }
        if count < topic.partitions.len() {
            return Err(TopicError::PartitionShrink);
        }
        let total = self
            .partitions
            .checked_add(count - topic.partitions.len())
            .ok_or(TopicError::ResourceExhausted)?;
        if total > self.max_partitions {
            return Err(TopicError::ResourceExhausted);
        }
        Ok((
            topic.generation,
            topic.id.is_none() || count != topic.partitions.len(),
        ))
    }
    /// Takes the already owned candidate without scanning or copying its rows.
    /// No cache state changes until every row validates under one generation.
    pub(crate) fn prepare_update(
        &self,
        handle: TopicHandle,
        id: TopicId,
        partitions: Vec<PartitionMetadata>,
    ) -> Result<TopicUpdate, TopicError> {
        let (generation, changed) = self.update_start(handle, id, partitions.len())?;
        Ok(TopicUpdate {
            handle,
            id,
            partitions,
            generation,
            cursor: 0,
            changed,
        })
    }
    /// One partition comparison, one real-generation restart, or an O(1) commit.
    /// The old/new arrays contain Copy elements; replacement drops one backing
    /// allocation, not a chain of per-partition owners.
    pub(crate) fn update_step(
        &mut self,
        update: &mut TopicUpdate,
        now: RuntimeInstant,
    ) -> Result<UpdateProgress, TopicError> {
        let (generation, changed) =
            self.update_start(update.handle, update.id, update.partitions.len())?;
        if generation != update.generation {
            update.generation = generation;
            update.cursor = 0;
            update.changed = changed;
            return Ok(UpdateProgress::Pending);
        }
        let topic = self.get(update.handle)?;
        if let Some(new) = update.partitions.get(update.cursor) {
            if new.leader < -1 || new.leader_epoch < -1 {
                return Err(TopicError::InvalidPartitions);
            }
            if let Some(old) = topic.partitions.get(update.cursor) {
                if new.leader_epoch < old.leader_epoch
                    || (new.leader_epoch == old.leader_epoch
                        && old.leader >= 0
                        && new.leader >= 0
                        && old.leader != new.leader)
                {
                    return Err(TopicError::StaleLeaderEpoch);
                }
                update.changed |= old != new;
            }
            update.cursor += 1;
            return Ok(UpdateProgress::Pending);
        }
        let total = self
            .partitions
            .checked_add(update.partitions.len() - topic.partitions.len())
            .ok_or(TopicError::ResourceExhausted)?;
        let generation = if update.changed {
            generation
                .checked_add(1)
                .ok_or(TopicError::GenerationExhausted)?
        } else {
            generation
        };
        let refresh_at = now
            .checked_add(self.max_age)
            .ok_or(TopicError::TimeOverflow)?;
        let topic = self
            .topics
            .get_mut(&update.handle)
            .expect("validated topic");
        topic.id = Some(update.id);
        topic.state = TopicState::Ready;
        topic.generation = generation;
        topic.partitions = std::mem::take(&mut update.partitions);
        topic.refresh_at = refresh_at;
        self.partitions = total;
        self.ids.insert(update.id, update.handle);
        Ok(UpdateProgress::Applied {
            changed: update.changed,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn cache() -> TopicCache {
        TopicCache::new(
            3,
            4,
            RuntimeDuration::from_nanos(10),
            RuntimeDuration::from_nanos(20),
        )
        .unwrap()
    }
    fn row(epoch: i32) -> PartitionMetadata {
        PartitionMetadata {
            leader: 0,
            leader_epoch: epoch,
        }
    }
    #[test]
    fn noop_update_preserves_generation_and_moves_owned_array_without_copy() {
        let mut cache = cache();
        let handle = cache.open("a", RuntimeInstant::ZERO).unwrap();
        let id = TopicId([1; 16]);
        cache
            .apply(handle, id, &[row(0); 2], RuntimeInstant::ZERO)
            .unwrap();
        let rows = vec![row(0); 2];
        let pointer = rows.as_ptr();
        let mut work = cache.prepare_update(handle, id, rows).unwrap();
        for _ in 0..2 {
            assert!(matches!(
                cache.update_step(&mut work, RuntimeInstant::ZERO),
                Ok(UpdateProgress::Pending)
            ));
        }
        assert!(matches!(
            cache.update_step(&mut work, RuntimeInstant::ZERO),
            Ok(UpdateProgress::Applied { changed: false })
        ));
        assert_eq!(cache.get(handle).unwrap().generation, 1);
        assert_eq!(cache.get(handle).unwrap().partitions.as_ptr(), pointer);
    }
    #[test]
    fn aggregate_capacity_is_revalidated_before_commit() {
        let mut cache = cache();
        let a = cache.open("a", RuntimeInstant::ZERO).unwrap();
        let b = cache.open("b", RuntimeInstant::ZERO).unwrap();
        let mut work = cache
            .prepare_update(a, TopicId([1; 16]), vec![row(0); 3])
            .unwrap();
        for _ in 0..3 {
            assert!(matches!(
                cache.update_step(&mut work, RuntimeInstant::ZERO),
                Ok(UpdateProgress::Pending)
            ));
        }
        cache
            .apply(b, TopicId([2; 16]), &[row(0); 2], RuntimeInstant::ZERO)
            .unwrap();
        assert!(matches!(
            cache.update_step(&mut work, RuntimeInstant::ZERO),
            Err(TopicError::ResourceExhausted)
        ));
        assert!(cache.get(a).unwrap().id.is_none());
        assert_eq!(cache.partitions, 2);
    }
}
