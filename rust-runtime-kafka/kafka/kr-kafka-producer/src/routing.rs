//! Bulk routing policies. Only admitted bytes consume an unkeyed routing lease.
use crate::types::{TopicHandle, TopicId};
use kr_runtime::{RuntimeDuration, RuntimeInstant};
use std::{fmt, sync::Arc};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum UnkeyedPolicy {
    UniformBytes { run_bytes: u32 },
    Adaptive { run_bytes: u32 },
}
impl UnkeyedPolicy {
    #[must_use]
    pub const fn run_bytes(self) -> u32 {
        match self {
            Self::UniformBytes { run_bytes } | Self::Adaptive { run_bytes } => run_bytes,
        }
    }
}
impl Default for UnkeyedPolicy {
    fn default() -> Self {
        Self::UniformBytes {
            run_bytes: 64 * 1024,
        }
    }
}
#[derive(Clone, Default)]
pub enum PartitionerConfig {
    #[default]
    Builtin,
    Native(Arc<dyn NativePartitioner>),
    External,
}
impl fmt::Debug for PartitionerConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Builtin => "Builtin",
            Self::Native(_) => "Native(..)",
            Self::External => "External",
        })
    }
}
#[derive(Clone, Copy, Debug)]
pub struct RecordMetadata {
    pub topic: TopicHandle,
    pub key_hash: Option<u32>,
    pub encoded_bytes: u32,
    pub partition_hint: Option<i32>,
}
#[derive(Clone, Copy, Debug)]
pub struct PartitionSnapshot {
    pub partition: i32,
    pub lane: u8,
    pub queued_bytes: u64,
    pub oldest_age: RuntimeDuration,
    pub open_batch_bytes: u32,
    pub throttled_until: RuntimeInstant,
    pub drain_bytes_per_second: u64,
    pub available: bool,
}
#[derive(Clone, Copy, Debug)]
pub struct TopicSnapshot<'a> {
    pub handle: TopicHandle,
    pub id: TopicId,
    pub generation: u32,
    pub partitions: &'a [PartitionSnapshot],
}
/// Collection interval for advisory partition telemetry. Every topic's UUID,
/// generation and partition count remain unchanged through collection and the
/// final validation immediately before policy invocation. Queue/drain/throttle
/// fields are samples taken during this interval, not an atomic engine view.
/// `PartitionSnapshot::oldest_age` is measured at that row's observation time;
/// `RoutingSnapshot::now` is the completion/callback time.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SnapshotCollection {
    pub started_at: RuntimeInstant,
    pub completed_at: RuntimeInstant,
}

/// Observed element backing for actor-owned routing scratch. Payload buffers,
/// ordered policy indexes, Arc headers and allocator overhead are excluded.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RoutingStorage {
    pub fixed_capacity_bytes: usize,
    pub callback_view_peak_bytes: usize,
}

#[derive(Clone, Copy, Debug)]
pub struct RoutingSnapshot<'a> {
    pub now: RuntimeInstant,
    pub topics: &'a [TopicSnapshot<'a>],
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PartitionChoice {
    Pending,
    Partition(i32),
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RoutingLease {
    pub topic: TopicId,
    pub partition: i32,
    pub byte_quota: u32,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum RoutingError {
    UnknownTopic,
    NoPartitions,
    InvalidPartition { partition: i32, count: usize },
    InvalidQuota,
    InvalidDecisionCount,
    StaleLease,
    PolicyFailed,
}
impl fmt::Display for RoutingError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnknownTopic => f.write_str("topic routing metadata unavailable"),
            Self::NoPartitions => f.write_str("topic has no partitions"),
            Self::InvalidPartition { partition, count } => {
                write!(f, "partition {partition} outside 0..{count}")
            }
            Self::InvalidQuota => f.write_str("routing byte quota must be positive"),
            Self::InvalidDecisionCount => {
                f.write_str("routing decision count differs from record count")
            }
            Self::StaleLease => f.write_str("routing lease belongs to stale topic metadata"),
            Self::PolicyFailed => f.write_str("partition policy failed"),
        }
    }
}
impl std::error::Error for RoutingError {}
/// Native policies execute outside engine mutation, once per bounded bulk.
/// Implementations must not block, perform I/O, or enter a managed runtime.
/// Partition telemetry is collected incrementally over a finite interval. The
/// default collection hooks preserve existing policies; override them to inspect
/// the interval explicitly. Topic identity/generation are validated at callback
/// time, while advisory queue fields can reflect different observation times.
pub trait NativePartitioner: Send + Sync {
    /// # Errors
    /// Returns a policy error; the engine separately validates every decision.
    fn choose_partitions(
        &self,
        records: &[RecordMetadata],
        snapshot: RoutingSnapshot<'_>,
        out: &mut [PartitionChoice],
    ) -> Result<(), RoutingError>;
    /// Timing-aware bulk hook. Existing implementations need not override it.
    ///
    /// # Errors
    /// Returns the underlying policy error.
    fn choose_partitions_collected(
        &self,
        records: &[RecordMetadata],
        snapshot: RoutingSnapshot<'_>,
        _collection: SnapshotCollection,
        out: &mut [PartitionChoice],
    ) -> Result<(), RoutingError> {
        self.choose_partitions(records, snapshot, out)
    }

    /// Optional routing for unkeyed inputs without a partition hint. The actor
    /// consumes the quota using already-admitted encoded bytes, allowing one
    /// record to cross the quota. The lease persists across bulk calls until its
    /// quota expires or topic metadata generation changes. Hinted records bypass
    /// the policy; keyed inputs always use the bulk method. Returning `None`
    /// delegates the topic's remaining unkeyed inputs to one bulk call.
    ///
    /// # Errors
    /// Returns a policy error; callers validate topic, partition, and byte quota.
    fn choose_run(
        &self,
        _topic: TopicSnapshot<'_>,
        _snapshot: RoutingSnapshot<'_>,
    ) -> Result<Option<RoutingLease>, RoutingError> {
        Ok(None)
    }

    /// Timing-aware run hook. Existing implementations need not override it.
    ///
    /// # Errors
    /// Returns the underlying policy error.
    fn choose_run_collected(
        &self,
        topic: TopicSnapshot<'_>,
        snapshot: RoutingSnapshot<'_>,
        _collection: SnapshotCollection,
    ) -> Result<Option<RoutingLease>, RoutingError> {
        self.choose_run(topic, snapshot)
    }
}

/// Java-compatible Kafka murmur2, including empty keys as real keyed input.
#[must_use]
pub fn murmur2(bytes: &[u8]) -> u32 {
    const M: u32 = 0x5bd1e995;
    let mut hash = 0x9747b28c_u32 ^ (bytes.len() as u32);
    let mut chunks = bytes.chunks_exact(4);
    for chunk in &mut chunks {
        let mut k = u32::from_le_bytes(chunk.try_into().expect("four-byte chunk"));
        k = k.wrapping_mul(M);
        k ^= k >> 24;
        k = k.wrapping_mul(M);
        hash = hash.wrapping_mul(M) ^ k;
    }
    let tail = chunks.remainder();
    if tail.len() == 3 {
        hash ^= u32::from(tail[2]) << 16;
    }
    if tail.len() >= 2 {
        hash ^= u32::from(tail[1]) << 8;
    }
    if !tail.is_empty() {
        hash ^= u32::from(tail[0]);
        hash = hash.wrapping_mul(M);
    }
    hash ^= hash >> 13;
    hash = hash.wrapping_mul(M);
    hash ^= hash >> 15;
    hash
}
/// # Errors
/// Rejects nonpositive partition counts.
pub fn keyed_partition(key: &[u8], count: i32) -> Result<i32, RoutingError> {
    if count <= 0 {
        return Err(RoutingError::NoPartitions);
    }
    Ok(((murmur2(key) & 0x7fff_ffff) % count as u32) as i32)
}

pub(crate) fn adaptive_weight(row: &PartitionSnapshot, run_bytes: u32) -> u128 {
    if !row.available {
        0
    } else {
        (u128::from(row.drain_bytes_per_second.max(1)) * u128::from(run_bytes))
            / (u128::from(row.queued_bytes) + u128::from(run_bytes))
    }
}

/// A single topic's byte-driven unkeyed state. Preview has no side effects;
/// rejection and repeated batch seals therefore cannot consume a quota.
#[derive(Clone, Copy, Debug, Default)]
pub struct StickyRouter {
    lease: Option<RoutingLease>,
    remaining: u32,
    rotations: u64,
}
impl StickyRouter {
    #[must_use]
    pub const fn rotations(&self) -> u64 {
        self.rotations
    }
    #[must_use]
    pub const fn remaining(&self) -> u32 {
        self.remaining
    }
    /// Uniform byte-sticky selection needs only immutable topic identity and the
    /// contiguous partition count, without materializing advisory queue stats.
    /// The caller supplies the same RNG draw used by `preview`, even while a
    /// current admitted run remains usable. Metadata generation alone does not
    /// rotate a run; UUID or an invalidated partition does.
    ///
    /// # Errors
    /// Rejects empty/oversized partition counts and zero byte quotas.
    pub fn preview_uniform(
        &self,
        topic: TopicId,
        partition_count: usize,
        run_bytes: u32,
        draw: u64,
    ) -> Result<RoutingLease, RoutingError> {
        if partition_count == 0 {
            return Err(RoutingError::NoPartitions);
        }
        if partition_count > i32::MAX as usize {
            return Err(RoutingError::InvalidPartition {
                partition: i32::MAX,
                count: partition_count,
            });
        }
        if run_bytes == 0 {
            return Err(RoutingError::InvalidQuota);
        }
        if let Some(lease) = self.lease
            && self.remaining > 0
            && lease.topic == topic
            && lease.partition >= 0
            && (lease.partition as usize) < partition_count
        {
            return Ok(RoutingLease {
                byte_quota: self.remaining,
                ..lease
            });
        }
        Ok(RoutingLease {
            topic,
            partition: (draw as usize % partition_count) as i32,
            byte_quota: run_bytes,
        })
    }

    /// `draw` comes from the actor's explicit RNG stream and is recorded in replay.
    ///
    /// # Errors
    /// Rejects empty/inconsistent metadata or a zero quota.
    pub fn preview(
        &self,
        topic: TopicSnapshot<'_>,
        policy: UnkeyedPolicy,
        draw: u64,
    ) -> Result<RoutingLease, RoutingError> {
        let count = topic.partitions.len();
        if count == 0 {
            return Err(RoutingError::NoPartitions);
        }
        if count > i32::MAX as usize {
            return Err(RoutingError::InvalidPartition {
                partition: i32::MAX,
                count,
            });
        }
        if policy.run_bytes() == 0 {
            return Err(RoutingError::InvalidQuota);
        }
        if let Some(lease) = self.lease
            && self.remaining > 0
            && lease.topic == topic.id
            && lease.partition >= 0
            && (lease.partition as usize) < count
        {
            return Ok(RoutingLease {
                byte_quota: self.remaining,
                ..lease
            });
        }
        let index = match policy {
            UnkeyedPolicy::UniformBytes { .. } => draw as usize % count,
            UnkeyedPolicy::Adaptive { run_bytes } => {
                let weight = |p: &PartitionSnapshot| -> u128 {
                    if !p.available {
                        0
                    } else {
                        (u128::from(p.drain_bytes_per_second.max(1)) * u128::from(run_bytes))
                            / (u128::from(p.queued_bytes) + u128::from(run_bytes))
                    }
                };
                let total = topic.partitions.iter().map(weight).sum::<u128>();
                if total == 0 {
                    draw as usize % count
                } else {
                    // Map the full draw interval onto the full weight interval.
                    // Modulo would make high-weight partitions unreachable when
                    // the sum exceeds u64, even though each weight fits u64.
                    let mut target = (total >> 64) * u128::from(draw)
                        + (((total & u128::from(u64::MAX)) * u128::from(draw)) >> 64);
                    let mut chosen = count - 1;
                    for (i, p) in topic.partitions.iter().enumerate() {
                        let w = weight(p);
                        if target < w {
                            chosen = i;
                            break;
                        }
                        target -= w;
                    }
                    chosen
                }
            }
        };
        // Metadata arrays are indexed by partition; reject malformed snapshots.
        if topic.partitions[index].partition != index as i32 {
            return Err(RoutingError::InvalidPartition {
                partition: topic.partitions[index].partition,
                count,
            });
        }
        Ok(RoutingLease {
            topic: topic.id,
            partition: index as i32,
            byte_quota: policy.run_bytes(),
        })
    }
    /// The owner builds this cumulative index one observed row at a time.
    /// Its private caller guarantees monotonic prefix values and one value per
    /// contiguous partition. Lookup is logarithmic, with exactly the same draw
    /// mapping and all-zero fallback as the public reference implementation.
    pub(crate) fn preview_adaptive_indexed(
        &self,
        topic: TopicSnapshot<'_>,
        prefix: &[u128],
        run_bytes: u32,
        draw: u64,
    ) -> Result<RoutingLease, RoutingError> {
        let uniform = self.preview_uniform(topic.id, topic.partitions.len(), run_bytes, draw)?;
        if prefix.len() != topic.partitions.len() {
            return Err(RoutingError::InvalidDecisionCount);
        }
        if self.remaining > 0
            && self.lease.is_some_and(|lease| {
                lease.topic == topic.id
                    && lease.partition >= 0
                    && (lease.partition as usize) < topic.partitions.len()
            })
        {
            return Ok(uniform);
        }
        let total = *prefix.last().ok_or(RoutingError::NoPartitions)?;
        let index = if total == 0 {
            uniform.partition as usize
        } else {
            let target = (total >> 64) * u128::from(draw)
                + (((total & u128::from(u64::MAX)) * u128::from(draw)) >> 64);
            prefix.partition_point(|weight| *weight <= target)
        };
        let selected = topic
            .partitions
            .get(index)
            .ok_or(RoutingError::InvalidDecisionCount)?;
        if selected.partition != index as i32 {
            return Err(RoutingError::InvalidPartition {
                partition: selected.partition,
                count: topic.partitions.len(),
            });
        }
        Ok(RoutingLease {
            partition: index as i32,
            ..uniform
        })
    }

    /// Records only accepted bytes. A record can overshoot the quota once;
    /// rotation occurs before the next admitted record, never mid-record.
    ///
    /// # Errors
    /// Rejects an inconsistent lease or exhausted diagnostic counter.
    pub fn commit(&mut self, lease: RoutingLease, accepted_bytes: u32) -> Result<(), RoutingError> {
        if accepted_bytes == 0 {
            return Ok(());
        }
        if lease.byte_quota == 0 || lease.partition < 0 || lease.topic.is_zero() {
            return Err(RoutingError::InvalidQuota);
        }
        let same = self.lease.is_some_and(|previous| {
            previous.topic == lease.topic && previous.partition == lease.partition
        }) && self.remaining > 0;
        if same && lease.byte_quota != self.remaining {
            return Err(RoutingError::StaleLease);
        }
        let rotations = if same {
            self.rotations
        } else {
            self.rotations
                .checked_add(1)
                .ok_or(RoutingError::PolicyFailed)?
        };
        self.remaining = lease.byte_quota.saturating_sub(accepted_bytes);
        self.lease = Some(lease);
        self.rotations = rotations;
        Ok(())
    }
}

/// # Errors
/// Rejects missing/extra decisions and any choice outside the matching topic.
pub fn validate_choices(
    records: &[RecordMetadata],
    snapshot: RoutingSnapshot<'_>,
    choices: &[PartitionChoice],
) -> Result<(), RoutingError> {
    if records.len() != choices.len() {
        return Err(RoutingError::InvalidDecisionCount);
    }
    for (record, choice) in records.iter().zip(choices) {
        let topic = snapshot.topics.iter().find(|t| t.handle == record.topic);
        match (topic, choice) {
            (None, PartitionChoice::Pending) => {}
            (Some(topic), PartitionChoice::Partition(p))
                if *p >= 0 && (*p as usize) < topic.partitions.len() => {}
            (Some(topic), PartitionChoice::Partition(p)) => {
                return Err(RoutingError::InvalidPartition {
                    partition: *p,
                    count: topic.partitions.len(),
                });
            }
            _ => return Err(RoutingError::UnknownTopic),
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    fn partitions() -> [PartitionSnapshot; 3] {
        std::array::from_fn(|i| PartitionSnapshot {
            partition: i as i32,
            lane: 0,
            queued_bytes: 0,
            oldest_age: RuntimeDuration::ZERO,
            open_batch_bytes: 0,
            throttled_until: RuntimeInstant::ZERO,
            drain_bytes_per_second: 100,
            available: true,
        })
    }
    #[test]
    fn quota_changes_only_for_accepted_bytes_not_preview_or_batch_seal() {
        let partitions = partitions();
        let topic = TopicSnapshot {
            handle: TopicHandle(1),
            id: TopicId([1; 16]),
            generation: 1,
            partitions: &partitions,
        };
        let policy = UnkeyedPolicy::UniformBytes { run_bytes: 10 };
        let mut router = StickyRouter::default();
        let first = router.preview(topic, policy, 1).unwrap();
        for _ in 0..100 {
            assert_eq!(router.preview(topic, policy, 1).unwrap(), first);
        }
        assert_eq!(router.rotations(), 0);
        router.commit(first, 6).unwrap();
        assert_eq!(router.preview(topic, policy, 2).unwrap().partition, 1);
        assert_eq!(router.remaining(), 4);
        let next = router.preview(topic, policy, 2).unwrap();
        router.commit(next, 5).unwrap();
        assert_eq!(router.remaining(), 0);
        assert_eq!(router.preview(topic, policy, 2).unwrap().partition, 2);
    }
    #[test]
    fn uniform_count_only_preview_matches_snapshot_routing_and_scales_without_rows() {
        let parts = partitions();
        let topic = TopicSnapshot {
            handle: TopicHandle(1),
            id: TopicId([1; 16]),
            generation: 1,
            partitions: &parts,
        };
        let mut indexed = StickyRouter::default();
        let mut snapshot = StickyRouter::default();
        let mut draw = 7u64;
        for index in 0..1000 {
            draw ^= draw << 13;
            draw ^= draw >> 7;
            draw ^= draw << 17;
            let a = indexed
                .preview_uniform(topic.id, parts.len(), 127, draw)
                .unwrap();
            let b = snapshot
                .preview(topic, UnkeyedPolicy::UniformBytes { run_bytes: 127 }, draw)
                .unwrap();
            assert_eq!(a, b);
            let bytes = 1 + index % 83;
            indexed.commit(a, bytes).unwrap();
            snapshot.commit(b, bytes).unwrap();
            assert_eq!(indexed.remaining(), snapshot.remaining());
            assert_eq!(indexed.rotations(), snapshot.rotations());
        }
        let count = i32::MAX as usize;
        let router = StickyRouter::default();
        assert_eq!(
            router
                .preview_uniform(topic.id, count, 127, (count - 1) as u64)
                .unwrap()
                .partition,
            i32::MAX - 1
        );
        assert_eq!(
            router.preview_uniform(topic.id, 0, 127, 0),
            Err(RoutingError::NoPartitions)
        );
        assert_eq!(
            router.preview_uniform(topic.id, 1, 0, 0),
            Err(RoutingError::InvalidQuota)
        );
    }

    #[test]
    fn incremental_adaptive_prefix_matches_reference_for_extreme_weights_and_replays() {
        let mut seed = 0x531ae41_u64;
        for count in [1_usize, 2, 7, 257] {
            for run_bytes in [1, 127, u32::MAX] {
                let mut rows = Vec::new();
                let mut prefix = Vec::new();
                let mut total = 0;
                for index in 0..count {
                    seed ^= seed << 13;
                    seed ^= seed >> 7;
                    seed ^= seed << 17;
                    let row = PartitionSnapshot {
                        partition: index as i32,
                        lane: 0,
                        queued_bytes: if index % 3 == 0 { 0 } else { seed },
                        oldest_age: RuntimeDuration::ZERO,
                        open_batch_bytes: 0,
                        throttled_until: RuntimeInstant::ZERO,
                        drain_bytes_per_second: if index % 5 == 0 {
                            u64::MAX
                        } else {
                            seed.rotate_left(7)
                        },
                        available: index % 7 != 0,
                    };
                    total += adaptive_weight(&row, run_bytes);
                    prefix.push(total);
                    rows.push(row);
                }
                let topic = TopicSnapshot {
                    handle: TopicHandle(1),
                    id: TopicId([1; 16]),
                    generation: 1,
                    partitions: &rows,
                };
                let mut indexed = StickyRouter::default();
                let mut reference = StickyRouter::default();
                for iteration in 0..1000 {
                    seed ^= seed << 13;
                    seed ^= seed >> 7;
                    seed ^= seed << 17;
                    let draw = match iteration % 13 {
                        0 => 0,
                        1 => u64::MAX,
                        _ => seed,
                    };
                    let actual = indexed
                        .preview_adaptive_indexed(topic, &prefix, run_bytes, draw)
                        .unwrap();
                    let expected = reference
                        .preview(topic, UnkeyedPolicy::Adaptive { run_bytes }, draw)
                        .unwrap();
                    assert_eq!(
                        actual, expected,
                        "count={count},run={run_bytes},draw={draw}"
                    );
                    let bytes = if iteration % 3 == 0 { u32::MAX } else { 17 };
                    indexed.commit(actual, bytes).unwrap();
                    reference.commit(expected, bytes).unwrap();
                    assert_eq!(indexed.rotations(), reference.rotations());
                    assert_eq!(indexed.remaining(), reference.remaining());
                }
            }
        }
    }

    #[test]
    fn metadata_generation_does_not_rotate_an_admitted_run() {
        let partitions = partitions();
        let topic = TopicSnapshot {
            handle: TopicHandle(1),
            id: TopicId([1; 16]),
            generation: 1,
            partitions: &partitions,
        };
        let mut r = StickyRouter::default();
        let p = UnkeyedPolicy::default();
        let lease = r.preview(topic, p, 1).unwrap();
        r.commit(lease, 1).unwrap();
        assert_eq!(
            r.preview(
                TopicSnapshot {
                    generation: 2,
                    ..topic
                },
                p,
                2
            )
            .unwrap()
            .partition,
            1
        );
    }
    #[test]
    fn java_keyed_hashes_preserve_empty_null_and_signed_hash_semantics() {
        assert_eq!(murmur2(b""), 275646681);
        assert_eq!(keyed_partition(b"", 3), Ok(0));
        assert!(keyed_partition(b"key", 0).is_err());
        for n in 1..100 {
            for key in [&b"key"[..], &b"kafka"[..], &b""[..], &[255, 0, 1, 128][..]] {
                let p = keyed_partition(key, n).unwrap();
                assert!(p >= 0 && p < n);
            }
        }
    }

    #[test]
    fn adaptive_draw_covers_weights_larger_than_u64_in_total() {
        let mut partitions = partitions();
        for p in &mut partitions {
            p.drain_bytes_per_second = u64::MAX;
        }
        let topic = TopicSnapshot {
            handle: TopicHandle(1),
            id: TopicId([1; 16]),
            generation: 1,
            partitions: &partitions,
        };
        let router = StickyRouter::default();
        let policy = UnkeyedPolicy::Adaptive { run_bytes: 65536 };
        assert_eq!(router.preview(topic, policy, 0).unwrap().partition, 0);
        assert_eq!(
            router
                .preview(topic, policy, u64::MAX / 2)
                .unwrap()
                .partition,
            1
        );
        assert_eq!(
            router.preview(topic, policy, u64::MAX).unwrap().partition,
            2
        );
    }
}
