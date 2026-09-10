//! Coalescing scheduler invalidations; each cursor visits at most one partition.
use super::*;
use std::ops::Bound::{Excluded, Included, Unbounded};

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub(super) enum Group {
    Route(i32, u8),
    Credits,
    Identity,
    EncodingPass,
}

#[derive(Default)]
struct Sweep {
    cursor: Option<TopicPartition>,
    end: Option<TopicPartition>,
    restart: bool,
}

#[derive(Default)]
struct Members {
    partitions: BTreeSet<TopicPartition>,
    sweep: Option<Sweep>,
}

impl Members {
    fn changed(&mut self) {
        if self.partitions.is_empty() {
            return;
        }
        if let Some(sweep) = &mut self.sweep {
            sweep.restart = true;
        } else {
            self.sweep = Some(Sweep {
                cursor: None,
                end: self.partitions.last().copied(),
                restart: false,
            });
        }
    }

    fn next(&mut self) -> Option<Option<TopicPartition>> {
        let sweep = self.sweep.as_mut()?;
        let next = self
            .partitions
            .range((
                sweep.cursor.map_or(Unbounded, Excluded),
                sweep.end.map_or(Unbounded, Included),
            ))
            .next()
            .copied();
        if let Some(next) = next {
            sweep.cursor = Some(next);
            return Some(Some(next));
        }
        if sweep.restart && !self.partitions.is_empty() {
            self.sweep = Some(Sweep {
                cursor: None,
                end: self.partitions.last().copied(),
                restart: false,
            });
        } else {
            self.sweep = None;
        }
        Some(None)
    }
}

#[derive(Default)]
pub(super) struct Scheduler {
    pub(super) ready: dispatch_queue::WireReady,
    pub(super) dirty: BTreeSet<TopicPartition>,
    dirty_cursor: Option<TopicPartition>,
    waiting: BTreeMap<TopicPartition, Group>,
    groups: BTreeMap<Group, Members>,
    active_groups: BTreeSet<Group>,
    group_cursor: Option<Group>,
    topics: BTreeMap<TopicId, Members>,
    active_topics: BTreeSet<TopicId>,
    topic_cursor: Option<TopicId>,
    identity: Members,
    reconsider_phase: usize,
    pub(super) phase: usize,
    pub(super) observed_releases: [u64; 6],
    encoding_ready_at: Option<RuntimeInstant>,
}

impl Scheduler {
    pub(super) fn encoding_pass_finished(&mut self, ready_at: RuntimeInstant) {
        self.encoding_ready_at = Some(ready_at);
        self.changed(Group::EncodingPass);
    }

    pub(super) fn gather_ready_at(&self, partition: TopicPartition) -> Option<RuntimeInstant> {
        (self.waiting.get(&partition) == Some(&Group::EncodingPass))
            .then_some(self.encoding_ready_at)
            .flatten()
    }
    fn remove_wait(&mut self, partition: TopicPartition) {
        let Some(group) = self.waiting.remove(&partition) else {
            return;
        };
        let members = self.groups.get_mut(&group).expect("indexed waiter group");
        assert!(members.partitions.remove(&partition));
        if members.partitions.is_empty() {
            self.groups.remove(&group);
            self.active_groups.remove(&group);
        }
    }

    pub(super) fn next_dirty(&mut self) -> Option<TopicPartition> {
        let key = self
            .dirty_cursor
            .and_then(|after| {
                self.dirty
                    .range((Excluded(after), Unbounded))
                    .next()
                    .copied()
            })
            .or_else(|| self.dirty.first().copied())?;
        self.dirty_cursor = Some(key);
        self.dirty.remove(&key);
        Some(key)
    }

    pub(super) fn mark(&mut self, partition: TopicPartition) {
        self.ready.remove(partition);
        self.remove_wait(partition);
        self.dirty.insert(partition);
        self.topics
            .entry(partition.topic)
            .or_default()
            .partitions
            .insert(partition);
        self.identity.partitions.insert(partition);
    }

    pub(super) fn forget(&mut self, partition: TopicPartition) {
        self.ready.forget(partition);
        self.dirty.remove(&partition);
        self.remove_wait(partition);
        self.identity.partitions.remove(&partition);
        if self.identity.partitions.is_empty() {
            self.identity.sweep = None;
        }
        if let Some(topic) = self.topics.get_mut(&partition.topic) {
            topic.partitions.remove(&partition);
            if topic.partitions.is_empty() {
                self.topics.remove(&partition.topic);
                self.active_topics.remove(&partition.topic);
            }
        }
    }

    pub(super) fn wait(&mut self, partition: TopicPartition, group: Group) {
        self.ready.remove(partition);
        self.dirty.remove(&partition);
        self.remove_wait(partition);
        self.waiting.insert(partition, group);
        self.groups
            .entry(group)
            .or_default()
            .partitions
            .insert(partition);
    }

    pub(super) fn changed(&mut self, group: Group) {
        if let Some(members) = self.groups.get_mut(&group) {
            members.changed();
            if members.sweep.is_some() {
                self.active_groups.insert(group);
            }
        }
    }

    pub(super) fn topic_changed(&mut self, topic: TopicId) {
        if let Some(members) = self.topics.get_mut(&topic) {
            members.changed();
            if members.sweep.is_some() {
                self.active_topics.insert(topic);
            }
        }
    }

    pub(super) fn identity_changed(&mut self) {
        self.identity.changed();
    }

    pub(super) fn has_reconsideration(&self) -> bool {
        !self.active_groups.is_empty()
            || !self.active_topics.is_empty()
            || self.identity.sweep.is_some()
    }

    /// Group releases, metadata updates and identity changes get separate
    /// persistent turns. Repeated changes behind a cursor coalesce one restart.
    pub(super) fn reconsider(&mut self) -> Option<Option<TopicPartition>> {
        for offset in 0..3 {
            let phase = (self.reconsider_phase + offset) % 3;
            let next = match phase {
                0 => {
                    let group = self
                        .group_cursor
                        .and_then(|after| {
                            self.active_groups
                                .range((Excluded(after), Unbounded))
                                .next()
                                .copied()
                        })
                        .or_else(|| self.active_groups.first().copied());
                    group.map(|group| {
                        self.group_cursor = Some(group);
                        let members = self.groups.get_mut(&group).expect("active release group");
                        let next = members.next().expect("active release cursor");
                        if members.sweep.is_none() {
                            self.active_groups.remove(&group);
                        }
                        next
                    })
                }
                1 => {
                    let topic = self
                        .topic_cursor
                        .and_then(|after| {
                            self.active_topics
                                .range((Excluded(after), Unbounded))
                                .next()
                                .copied()
                        })
                        .or_else(|| self.active_topics.first().copied());
                    topic.map(|topic| {
                        self.topic_cursor = Some(topic);
                        let members = self.topics.get_mut(&topic).expect("active metadata group");
                        let next = members.next().expect("active metadata cursor");
                        if members.sweep.is_none() {
                            self.active_topics.remove(&topic);
                        }
                        next
                    })
                }
                _ => self.identity.next(),
            };
            if next.is_some() {
                self.reconsider_phase = (phase + 1) % 3;
                return next;
            }
        }
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn key(topic: u8, partition: i32) -> TopicPartition {
        TopicPartition {
            topic: TopicId([topic; 16]),
            partition,
        }
    }
    #[test]
    fn unrelated_releases_do_not_reconsider_waiters_and_repeated_changes_do_not_queue_jobs() {
        let mut scheduler = Scheduler::default();
        for partition in 0..100 {
            scheduler.mark(key(1, partition));
            scheduler.wait(key(1, partition), Group::Route(7, 0));
        }
        scheduler.changed(Group::Route(8, 0));
        scheduler.changed(Group::Credits);
        assert!(!scheduler.has_reconsideration());
        for _ in 0..1000 {
            scheduler.changed(Group::Route(7, 0));
        }
        assert_eq!(scheduler.active_groups.len(), 1);
        assert_eq!(scheduler.reconsider(), Some(Some(key(1, 0))));
        scheduler.mark(key(1, 0));
        scheduler.wait(key(1, 0), Group::Route(7, 0));
        let mut visits = 1;
        while let Some(next) = scheduler.reconsider() {
            visits += 1;
            if let Some(partition) = next {
                scheduler.mark(partition);
            }
        }
        assert!(visits <= 202);
        assert_eq!(scheduler.dirty.len(), 100);
    }

    #[test]
    fn metadata_and_identity_cursors_survive_deleted_entries_and_cleanup_reclaims_all_memberships()
    {
        let mut scheduler = Scheduler::default();
        for topic in 1..=3 {
            for partition in 0..10 {
                scheduler.mark(key(topic, partition));
            }
        }
        scheduler.topic_changed(TopicId([2; 16]));
        scheduler.identity_changed();
        scheduler.forget(key(2, 0));
        let mut visits = 0;
        while let Some(next) = scheduler.reconsider() {
            visits += 1;
            if let Some(partition) = next {
                scheduler.forget(partition);
            }
        }
        assert!(visits <= 40);
        assert!(scheduler.dirty.is_empty());
        assert!(scheduler.topics.is_empty());
        assert!(scheduler.waiting.is_empty());
        assert!(scheduler.identity.partitions.is_empty());
        assert!(!scheduler.has_reconsideration());
    }
}

#[cfg(test)]
mod dirty_tests {
    use super::*;
    #[test]
    fn a_repeated_hot_dirty_key_cannot_displace_the_next_cold_key() {
        let mut scheduler = Scheduler::default();
        let key = |partition| TopicPartition {
            topic: TopicId([1; 16]),
            partition,
        };
        for partition in 0..32 {
            scheduler.mark(key(partition));
        }
        for partition in 0..32 {
            assert_eq!(scheduler.next_dirty(), Some(key(partition)));
            scheduler.mark(key(0));
        }
        assert_eq!(scheduler.next_dirty(), Some(key(0)));
    }
}
