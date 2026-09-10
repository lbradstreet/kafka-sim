//! Indexed encoder work and causal resource waiters.
use super::*;
use crate::accumulator::EncodeWait;
use encoding_queue::RawDrr;
use std::ops::Bound::{Excluded, Included, Unbounded};

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd)]
enum Target {
    Append(TopicPartition),
    Batch(BatchKey),
    Seal(BatchKey),
}
#[derive(Default)]
struct Sweep<T> {
    cursor: Option<T>,
    end: Option<T>,
    restart: bool,
}
#[derive(Default)]
struct Waiters {
    entries: BTreeSet<Target>,
    sweep: Option<Sweep<Target>>,
}
impl Waiters {
    fn changed(&mut self) {
        if self.entries.is_empty() {
            return;
        }
        if let Some(sweep) = &mut self.sweep {
            sweep.restart = true;
        } else {
            self.sweep = Some(Sweep {
                cursor: None,
                end: self.entries.last().copied(),
                restart: false,
            });
        }
    }
    fn next(&mut self) -> Option<Option<Target>> {
        let sweep = self.sweep.as_mut()?;
        let target = self
            .entries
            .range((
                sweep.cursor.map_or(Unbounded, Excluded),
                sweep.end.map_or(Unbounded, Included),
            ))
            .next()
            .copied();
        if let Some(target) = target {
            sweep.cursor = Some(target);
            return Some(Some(target));
        }
        if sweep.restart && !self.entries.is_empty() {
            self.sweep = Some(Sweep {
                cursor: None,
                end: self.entries.last().copied(),
                restart: false,
            });
        } else {
            self.sweep = None;
        }
        Some(None)
    }
}
#[derive(Default)]
struct PartitionWork {
    append: bool,
    batches: BTreeSet<(u64, BatchKey)>,
}
#[derive(Clone, Copy)]
struct BatchWork {
    partition: TopicPartition,
    ordinal: u64,
    waiting: Option<usize>,
}

#[derive(Default)]
pub(super) struct Encoder {
    #[cfg(test)]
    last_service: Option<(TopicPartition, bool, usize)>,
    pub(super) pending_suppressed: bool,
    ready: RawDrr,
    partitions: BTreeMap<TopicPartition, PartitionWork>,
    batches: BTreeMap<BatchKey, BatchWork>,
    context_owners: BTreeSet<(RuntimeInstant, BatchKey)>,
    context_ages: BTreeMap<BatchKey, RuntimeInstant>,
    reclaiming: Option<BatchKey>,
    // Compressed credit, codec credit/context, physical output, batch slots.
    waiters: [Waiters; 4],
    pending: BTreeMap<(TopicHandle, Option<i32>), Waiters>,
    pending_members: BTreeMap<TopicPartition, TopicHandle>,
    waiter_cursor: usize,
    pending_cursor: Option<(TopicHandle, Option<i32>)>,
    pending_active: BTreeSet<(TopicHandle, Option<i32>)>,
    seal_waiters: BTreeMap<(i32, u8), Waiters>,
    seal_routes: BTreeMap<BatchKey, (i32, u8)>,
    seal_active: BTreeSet<(i32, u8)>,
    seal_cursor: Option<(i32, u8)>,
    topic_waiters: BTreeMap<TopicId, Waiters>,
    topic_keys: BTreeMap<BatchKey, TopicId>,
    topic_active: BTreeSet<TopicId>,
    topic_cursor: Option<TopicId>,
    observed_releases: [u64; 2],
    observed_slots: usize,
    phase: usize,
}
impl Encoder {
    fn first_ready_batch(&self, partition: TopicPartition) -> Option<BatchKey> {
        let &(_, key) = self.partitions.get(&partition)?.batches.first()?;
        self.batches.get(&key)?.waiting.is_none().then_some(key)
    }
    fn sync_ready(&mut self, partition: TopicPartition, lane: u8) {
        let ready = self
            .partitions
            .get(&partition)
            .is_some_and(|work| work.append)
            || self.first_ready_batch(partition).is_some();
        if ready {
            self.ready.insert(partition, lane);
        } else {
            self.ready.remove(partition);
        }
    }
    fn remove_pending(&mut self, partition: TopicPartition) {
        if let Some(topic) = self.pending_members.remove(&partition) {
            for hint in [None, Some(partition.partition)] {
                let route = (topic, hint);
                if let Some(waiters) = self.pending.get_mut(&route) {
                    waiters.entries.remove(&Target::Append(partition));
                    if waiters.entries.is_empty() {
                        self.pending.remove(&route);
                        self.pending_active.remove(&route);
                    }
                }
            }
        }
    }
    fn remove_batch(&mut self, key: BatchKey) -> Option<TopicPartition> {
        if let Some(topic) = self.topic_keys.remove(&key)
            && let Some(waiters) = self.topic_waiters.get_mut(&topic)
        {
            waiters.entries.remove(&Target::Seal(key));
            if waiters.entries.is_empty() {
                self.topic_waiters.remove(&topic);
                self.topic_active.remove(&topic);
            }
        }
        if let Some(route) = self.seal_routes.remove(&key)
            && let Some(waiters) = self.seal_waiters.get_mut(&route)
        {
            waiters.entries.remove(&Target::Seal(key));
            if waiters.entries.is_empty() {
                self.seal_waiters.remove(&route);
                self.seal_active.remove(&route);
            }
        }
        if let Some(age) = self.context_ages.remove(&key) {
            self.context_owners.remove(&(age, key));
        }
        if self.reclaiming == Some(key) {
            self.reclaiming = None;
        }
        let work = self.batches.remove(&key)?;
        if let Some(wait) = work.waiting {
            self.waiters[wait].entries.remove(&Target::Batch(key));
        }
        if let Some(partition) = self.partitions.get_mut(&work.partition) {
            partition.batches.remove(&(work.ordinal, key));
        }
        Some(work.partition)
    }
    fn has_recheck(&self) -> bool {
        self.waiters.iter().any(|wait| wait.sweep.is_some())
            || !self.pending_active.is_empty()
            || !self.seal_active.is_empty()
            || !self.topic_active.is_empty()
    }
    fn recheck(&mut self) -> Option<Option<Target>> {
        for offset in 0..7 {
            let index = (self.waiter_cursor + offset) % 7;
            if index < 4 {
                if let Some(target) = self.waiters[index].next() {
                    self.waiter_cursor = (index + 1) % 7;
                    if let Some(target) = target {
                        self.waiters[index].entries.remove(&target);
                    }
                    return Some(target);
                }
            } else if index == 4 {
                let route = self
                    .pending_cursor
                    .and_then(|after| {
                        self.pending_active
                            .range((Excluded(after), Unbounded))
                            .next()
                            .copied()
                    })
                    .or_else(|| self.pending_active.first().copied());
                if let Some(route) = route {
                    self.pending_cursor = Some(route);
                    self.waiter_cursor = 5;
                    let wait = self.pending.get_mut(&route).expect("active pending wait");
                    let target = wait.next().expect("active pending cursor");
                    if wait.sweep.is_none() {
                        self.pending_active.remove(&route);
                    }
                    return Some(target);
                }
            } else if index == 5 {
                let route = self
                    .seal_cursor
                    .and_then(|after| {
                        self.seal_active
                            .range((Excluded(after), Unbounded))
                            .next()
                            .copied()
                    })
                    .or_else(|| self.seal_active.first().copied());
                if let Some(route) = route {
                    self.seal_cursor = Some(route);
                    self.waiter_cursor = 6;
                    let wait = self.seal_waiters.get_mut(&route).expect("active seal wait");
                    let target = wait.next().expect("active seal cursor");
                    if wait.sweep.is_none() {
                        self.seal_active.remove(&route);
                    }
                    return Some(target);
                }
            } else {
                let topic = self
                    .topic_cursor
                    .and_then(|after| {
                        self.topic_active
                            .range((Excluded(after), Unbounded))
                            .next()
                            .copied()
                    })
                    .or_else(|| self.topic_active.first().copied());
                if let Some(topic) = topic {
                    self.topic_cursor = Some(topic);
                    self.waiter_cursor = 0;
                    let wait = self
                        .topic_waiters
                        .get_mut(&topic)
                        .expect("active topic wait");
                    let target = wait.next().expect("active topic cursor");
                    if wait.sweep.is_none() {
                        self.topic_active.remove(&topic);
                    }
                    return Some(target);
                }
            }
        }
        None
    }
}

impl ProducerEngine {
    pub(super) fn encoder_topic_changed(&mut self, topic: TopicId) {
        if let Some(wait) = self.encoder.topic_waiters.get_mut(&topic) {
            wait.changed();
            if wait.sweep.is_some() {
                self.encoder.topic_active.insert(topic);
            }
        }
    }
    pub fn encode(&mut self, now: RuntimeInstant, budget: WorkBudget) -> Progress {
        self.encode_with_completion_delay(now, budget, RuntimeDuration::ZERO)
    }
    /// Advances bounded encoder work. Only newly sealed batches receive the
    /// supplied modeled completion delay; existing output keeps its timestamp.
    pub fn encode_with_completion_delay(
        &mut self,
        now: RuntimeInstant,
        budget: WorkBudget,
        delay: RuntimeDuration,
    ) -> Progress {
        self.observe_metrics_time(now);
        self.last_encode_work = crate::estimation::EncodeWork::default();
        self.last_encode_aborted = false;
        #[cfg(test)]
        {
            self.encoder.last_service = None;
        }
        let mut progress = Progress::default();
        if budget.items == 0 {
            return progress;
        }
        self.encoder_observe_resources();
        while progress.items < budget.items {
            let mut worked = false;
            // Cleanup, causal reconsideration and ready service each receive a
            // turn. Empty phases cost no item; their fixed three probes do not
            // walk a topology. The cursor survives one-item poll quotas.
            for offset in 0..3 {
                let phase = (self.encoder.phase + offset) % 3;
                let done = match phase {
                    0 => self.drain_terminal_records(1) != 0,
                    1 if self.failed.is_none() && !self.closed => self.encoder_recheck(now),
                    2 if self.failed.is_none() && !self.closed && progress.bytes < budget.bytes => {
                        self.encoder_service(
                            now,
                            delay,
                            budget.bytes as usize,
                            (budget.bytes - progress.bytes) as usize,
                            &mut progress,
                        )
                    }
                    _ => false,
                };
                if done {
                    self.encoder.phase = (phase + 1) % 3;
                    progress.items += 1;
                    self.encoder_observe_resources();
                    worked = true;
                    break;
                }
            }
            if !worked {
                break;
            }
        }
        self.complete_fences();
        self.scheduler
            .encoding_pass_finished(Self::deadline_after(now, delay));
        progress.remaining_immediate = self.has_terminal_work()
            || (self.failed.is_none()
                && !self.closed
                && (!self.encoder.ready.is_empty()
                    || self.encoder.has_recheck()
                    || self.encoder_can_reclaim_context()));
        progress
    }
    fn encoder_can_reclaim_context(&self) -> bool {
        self.encoder.reclaiming.is_none()
            && !self.encoder.waiters[1].entries.is_empty()
            && self.codecs.status().available == 0
            && !self.encoder.context_owners.is_empty()
    }
    fn encoder_service(
        &mut self,
        now: RuntimeInstant,
        delay: RuntimeDuration,
        quantum: usize,
        maximum: usize,
        progress: &mut Progress,
    ) -> bool {
        if self.encoder_can_reclaim_context() {
            let (_, key) = *self.encoder.context_owners.first().expect("context holder");
            self.encoder.reclaiming = Some(key);
            self.batches
                .get_mut(key)
                .expect("actual context holder")
                .seal_at(SealReason::ContextReclaimed, now);
            self.refresh_batch_deadline(key, now);
            self.metrics_batch(key);
            return true;
        }
        let Some(visit) = self.encoder.ready.visit(
            quantum,
            self.validated.effective_batch_payload_bytes as usize,
            maximum,
        ) else {
            return false;
        };
        let partition = visit.partition;
        let work = self
            .encoder
            .partitions
            .get(&partition)
            .expect("ready partition");
        let batch = self.encoder.first_ready_batch(partition);
        let codec = batch.is_some() && (visit.codec_turn || !work.append);
        if codec {
            let raw =
                self.encoder_run_batch(batch.expect("codec turn"), now, delay, visit.raw_bytes);
            // Refresh can remove an exhausted member, so charge before applying
            // its post-call readiness transition.
            self.encoder.ready.charge(partition, raw);
            progress.bytes += raw as u32;
            #[cfg(test)]
            {
                self.encoder.last_service = Some((partition, true, raw));
            }
            let key = batch.expect("codec turn");
            if self
                .batches
                .get(key)
                .is_some_and(|batch| batch.state() == BatchState::Failed)
            {
                self.finish_unassigned(key, FailureReason::CompressedTooLarge);
            } else if self.batches.get(key).is_some() {
                let wait = self.batches.get(key).and_then(Batch::encode_wait);
                self.refresh_batch_deadline(key, now);
                if let Some(wait) = wait {
                    self.encoder_wait_batch(key, wait);
                }
            }
        } else {
            self.encoder.ready.charge(partition, 0);
            self.encoder_append(partition, now);
            #[cfg(test)]
            {
                self.encoder.last_service = Some((partition, false, 0));
            }
        }
        self.encoder_refresh_append(partition);
        true
    }
    fn encoder_run_batch(
        &mut self,
        key: BatchKey,
        now: RuntimeInstant,
        delay: RuntimeDuration,
        bytes: usize,
    ) -> usize {
        let Some(batch) = self.batches.get(key) else {
            return 0;
        };
        if batch
            .records
            .first()
            .is_some_and(|record| self.topic_is_settling(record.topic))
        {
            return 0;
        }
        let partition = batch.partition();
        let dispatch = self.has_dispatch_credit(partition, now);
        let sparse = self.partitions[&partition]
            .arrival
            .below(self.config.linger_skip_below_rate);
        let batch = self.batches.get_mut(key).expect("live codec batch");
        batch.seal_due(now, dispatch, sparse);
        let watermark = self
            .flushes
            .back()
            .map(|flush| flush.watermark)
            .into_iter()
            .chain(self.close.as_ref().map(|close| close.watermark))
            .max();
        if watermark.is_some_and(|watermark| {
            batch
                .records
                .first()
                .is_some_and(|record| record.token <= watermark)
        }) {
            batch.seal_at(SealReason::Flush, now);
        }
        let was_sealed = batch.state() == BatchState::Sealed;
        let done = batch.encode_retained(
            &mut self.codecs,
            WorkBudget {
                bytes: bytes as u32,
                items: 1,
            },
        );
        let work = batch.last_encode_work();
        self.last_encode_work.raw_bytes += work.raw_bytes;
        self.last_encode_work.input_calls += work.input_calls;
        self.last_encode_work.seal_calls += work.seal_calls;
        self.last_encode_work.seals_completed += work.seals_completed;
        if !was_sealed && batch.state() == BatchState::Sealed {
            batch.defer_dispatch_until(Self::deadline_after(now, delay));
            if matches!(
                self.config.compression,
                crate::config::Compression::Zstd { .. }
            ) {
                self.partitions
                    .get_mut(&partition)
                    .expect("batch partition")
                    .compression_estimate
                    .observe(batch.raw_bytes(), batch.wire_bytes().expect("sealed") - 61);
            }
        }
        self.metrics_batch(key);
        done.bytes as usize
    }
    fn encoder_append(&mut self, partition: TopicPartition, now: RuntimeInstant) {
        let Some(front) = self
            .partitions
            .get(&partition)
            .and_then(|queue| queue.records.front())
        else {
            return;
        };
        let handle = front.topic;
        if self.topic_is_settling(handle) || self.pending_precedes_front(partition) {
            return;
        }
        if now >= front.deadline {
            let record = self
                .partitions
                .get_mut(&partition)
                .expect("partition")
                .records
                .pop_front()
                .expect("front");
            self.fail_record(record, FailureReason::Deadline);
            return;
        }
        let last = self.partitions[&partition]
            .batches
            .back()
            .copied()
            .filter(|key| {
                self.batches.get(*key).is_some_and(|batch| {
                    batch.state() == BatchState::Open
                        && batch
                            .records
                            .first()
                            .is_none_or(|record| record.topic == handle)
                })
            });
        let key = if let Some(key) = last {
            key
        } else {
            if self.batches.len() + self.terminal_records.len() >= self.config.max_batches as usize
            {
                return;
            }
            let lane = self.partitions[&partition].lane;
            let mut batch = match Batch::new(
                &self.config,
                self.validated.effective_batch_payload_bytes,
                partition,
                lane,
                self.output.clone(),
                self.credits.clone(),
            ) {
                Ok(batch) => batch,
                Err(_) => {
                    self.fail(FailureReason::InvalidRecord);
                    return;
                }
            };
            batch.set_compression_estimate(self.partitions[&partition].compression_estimate);
            batch.observe_seals(self.seal_counter.clone());
            batch.update_deadline_headroom(self.headroom_for(partition));
            let key = match self.batches.insert(batch) {
                Ok(key) => key,
                Err(_) => return,
            };
            self.partitions
                .get_mut(&partition)
                .expect("partition")
                .batches
                .push_back(key);
            key
        };
        let record = self
            .partitions
            .get_mut(&partition)
            .expect("partition")
            .records
            .pop_front()
            .expect("front");
        let token = record.token;
        let accepted_at = record.accepted_at;
        let batch = self.batches.get(key).expect("batch");
        let previous_raw = batch.raw_bytes();
        let previous_age = batch.first_accepted();
        match self.batches.get_mut(key).expect("batch").try_append_at(
            record,
            self.config.linger_max,
            now,
        ) {
            Ok(()) => {
                let batch = self.batches.get(key).expect("appended batch");
                let queue = self.partitions.get_mut(&partition).expect("partition");
                queue.batch_bytes += u64::from(batch.raw_bytes() - previous_raw);
                if let Some(at) = previous_age {
                    queue.batch_ages.remove(&(at, key));
                }
                queue
                    .batch_ages
                    .insert((batch.first_accepted().expect("nonempty batch"), key));
                self.batched_locations.insert(token, key);
                self.deadlines.remove(DeadlineKey::Pending(token));
                self.queued_locations.remove(&token);
                self.metrics_queue_wait(partition, accepted_at, now);
            }
            Err(rejected) => {
                if rejected.reason == AppendError::HardLimit
                    && self
                        .batches
                        .get(key)
                        .is_some_and(|batch| batch.record_count() != 0)
                {
                    self.partitions
                        .get_mut(&partition)
                        .expect("partition")
                        .records
                        .push_front(rejected.record);
                } else {
                    self.fail_record(rejected.record, FailureReason::InvalidRecord);
                }
            }
        }
        let dispatch = self.has_dispatch_credit(partition, now);
        let sparse = self.partitions[&partition]
            .arrival
            .below(self.config.linger_skip_below_rate);
        self.batches
            .get_mut(key)
            .expect("appended batch")
            .seal_due(now, dispatch, sparse);
        self.refresh_batch_deadline(key, now);
        self.metrics_batch(key);
    }
    pub(super) fn encoder_dispatch_changed(&mut self, broker: i32, lane: u8) {
        let route = (broker, lane);
        if let Some(wait) = self.encoder.seal_waiters.get_mut(&route) {
            wait.changed();
            if wait.sweep.is_some() {
                self.encoder.seal_active.insert(route);
            }
        }
    }
    pub(super) fn encoder_pending_changed(&mut self, topic: TopicHandle, hint: Option<i32>) {
        if self.encoder.pending_suppressed {
            return;
        }
        let route = (topic, hint);
        if let Some(wait) = self.encoder.pending.get_mut(&route) {
            wait.changed();
            if wait.sweep.is_some() {
                self.encoder.pending_active.insert(route);
            }
        }
    }
    pub(super) fn encoder_output_reclaimed(&mut self) {
        self.encoder.waiters[2].changed();
    }
    fn encoder_observe_resources(&mut self) {
        let pools = self.credits.snapshot();
        for (index, resource) in [Resource::CompressedBytes, Resource::CodecContexts]
            .into_iter()
            .enumerate()
        {
            let released = pools[resource as usize].released;
            if released != self.encoder.observed_releases[index] {
                self.encoder.observed_releases[index] = released;
                self.encoder.waiters[index].changed();
            }
        }
        let slots = self.batches.len() + self.terminal_records.len();
        if slots < self.encoder.observed_slots {
            self.encoder.waiters[3].changed();
        }
        self.encoder.observed_slots = slots;
    }
    /// Removes the fixed set of partition memberships after its owner queue has
    /// drained. Batch-owned memberships are removed when each batch terminates.
    pub(super) fn encoder_forget_partition(&mut self, partition: TopicPartition) {
        assert!(
            self.partitions
                .get(&partition)
                .is_none_or(|queue| queue.records.is_empty() && queue.batches.is_empty())
        );
        self.encoder.ready.remove(partition);
        self.encoder.remove_pending(partition);
        self.encoder.waiters[3]
            .entries
            .remove(&Target::Append(partition));
        if let Some(work) = self.encoder.partitions.remove(&partition) {
            assert!(
                work.batches.is_empty(),
                "empty owner has no codec membership"
            );
        }
    }
    pub(super) fn encoder_refresh_append(&mut self, partition: TopicPartition) {
        self.encoder.remove_pending(partition);
        self.encoder.waiters[3]
            .entries
            .remove(&Target::Append(partition));
        let Some(queue) = self.partitions.get(&partition) else {
            return;
        };
        if queue.records.is_empty() && queue.batches.is_empty() {
            self.encoder_forget_partition(partition);
            return;
        }
        let lane = queue.lane;
        let front = queue.records.front();
        let allowed = front.is_some_and(|record| !self.topic_is_settling(record.topic));
        let pending = allowed && self.pending_precedes_front(partition);
        let slot =
            self.batches.len() + self.terminal_records.len() < self.config.max_batches as usize
                || queue
                    .batches
                    .back()
                    .and_then(|key| self.batches.get(*key))
                    .is_some_and(|batch| {
                        batch.state() == BatchState::Open
                            && batch.records.first().is_none_or(|old| {
                                Some(old.topic) == front.map(|record| record.topic)
                            })
                    });
        self.encoder.partitions.entry(partition).or_default().append = allowed && !pending && slot;
        if pending {
            let topic = front.expect("pending front").topic;
            self.encoder.pending_members.insert(partition, topic);
            for hint in [None, Some(partition.partition)] {
                self.encoder
                    .pending
                    .entry((topic, hint))
                    .or_default()
                    .entries
                    .insert(Target::Append(partition));
            }
        } else if allowed && !slot {
            self.encoder.waiters[3]
                .entries
                .insert(Target::Append(partition));
        }
        self.encoder.sync_ready(partition, lane);
    }
    pub(super) fn encoder_remove_batch(&mut self, key: BatchKey) {
        if let Some(partition) = self.encoder.remove_batch(key)
            && let Some(queue) = self.partitions.get(&partition)
        {
            self.encoder.sync_ready(partition, queue.lane);
        }
    }
    pub(super) fn encoder_refresh_batch(&mut self, key: BatchKey, retry: bool) {
        let Some(batch) = self.batches.get(key) else {
            self.encoder_remove_batch(key);
            return;
        };
        let partition = batch.partition();
        let lane = batch.lane();
        let owner_age = (batch.state() == BatchState::Open && batch.holds_codec_context())
            .then(|| batch.first_accepted())
            .flatten();
        let existing_wait = self.encoder.batches.get(&key).and_then(|work| work.waiting);
        let seal_route = (batch.state() == BatchState::Open)
            .then(|| self.destination(partition))
            .flatten();
        let open = batch.state() == BatchState::Open;
        let reclaiming = self.encoder.reclaiming == Some(key) && batch.holds_codec_context();
        let wants = batch.has_codec_work()
            && !batch
                .records
                .first()
                .is_some_and(|record| self.topic_is_settling(record.topic));
        let ordinal = self.partitions[&partition]
            .batches
            .ordinal(key)
            .expect("batch queue index");
        // Keep a causal wait through deadline/headroom updates. A new attempt's
        // actual wait result is installed by encoder_run_batch below.
        self.encoder.remove_batch(key);
        if reclaiming {
            self.encoder.reclaiming = Some(key);
        }
        if let Some(route) = seal_route {
            self.encoder.seal_routes.insert(key, route);
            self.encoder
                .seal_waiters
                .entry(route)
                .or_default()
                .entries
                .insert(Target::Seal(key));
        }
        if open {
            self.encoder.topic_keys.insert(key, partition.topic);
            self.encoder
                .topic_waiters
                .entry(partition.topic)
                .or_default()
                .entries
                .insert(Target::Seal(key));
        }
        if let Some(age) = owner_age {
            self.encoder.context_ages.insert(key, age);
            self.encoder.context_owners.insert((age, key));
        }
        if wants {
            let wait = if retry { None } else { existing_wait };
            self.encoder.batches.insert(
                key,
                BatchWork {
                    partition,
                    ordinal,
                    waiting: wait,
                },
            );
            if let Some(wait) = wait {
                self.encoder.waiters[wait]
                    .entries
                    .insert(Target::Batch(key));
            }
            // A resource waiter stays in the FIFO encoding index. Otherwise a
            // later batch (including a lower reused slot) can consume the only
            // output reservation while its predecessor is parked, permanently
            // blocking both dispatch and the release that would wake the head.
            self.encoder
                .partitions
                .entry(partition)
                .or_default()
                .batches
                .insert((ordinal, key));
        }
        self.encoder.sync_ready(partition, lane);
    }
    fn encoder_wait_batch(&mut self, key: BatchKey, wait: EncodeWait) {
        let wait = match wait {
            EncodeWait::CompressedCredit => 0,
            EncodeWait::CodecCredit => 1,
            EncodeWait::Output => 2,
            EncodeWait::Other => {
                self.fail(FailureReason::ResourceExhausted);
                return;
            }
        };
        if let Some(work) = self.encoder.batches.get_mut(&key) {
            work.waiting = Some(wait);
            self.encoder.waiters[wait]
                .entries
                .insert(Target::Batch(key));
            let partition = work.partition;
            self.encoder
                .sync_ready(partition, self.partitions[&partition].lane);
        }
    }
    fn encoder_recheck(&mut self, now: RuntimeInstant) -> bool {
        let Some(target) = self.encoder.recheck() else {
            return false;
        };
        match target {
            Some(Target::Append(partition)) => self.encoder_refresh_append(partition),
            Some(Target::Batch(key)) => self.encoder_refresh_batch(key, true),
            Some(Target::Seal(key)) => {
                if let Some(batch) = self.batches.get(key) {
                    let partition = batch.partition();
                    let credit = self.has_dispatch_credit(partition, now);
                    let sparse = self.partitions[&partition]
                        .arrival
                        .below(self.config.linger_skip_below_rate);
                    self.batches
                        .get_mut(key)
                        .expect("live seal waiter")
                        .seal_due(now, credit, sparse);
                    self.refresh_batch_deadline(key, now);
                }
            }
            None => {}
        }
        true
    }
}

#[cfg(test)]
mod tests;
