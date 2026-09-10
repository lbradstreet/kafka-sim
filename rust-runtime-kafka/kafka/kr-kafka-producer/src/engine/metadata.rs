//! A response is structurally validated before publication, then normalized and
//! committed one topic at a time. Each visit processes one row or owned object.
//! Frame decoding remains a separate, byte-bounded control-codec quantum.
use super::*;
use crate::control::{MetadataPartition, MetadataTopic};
use crate::topic::{TopicUpdate, UpdateProgress};
use std::{mem::size_of, ops::Bound, vec::IntoIter};

#[derive(Clone, Copy, PartialEq, Eq)]
enum Phase {
    Start,
    Handles,
    Brokers,
    Topics,
    Rows,
    RemoveBrokers,
    InstallBrokers,
    InstallTopics,
    RemainingHandles,
    Discard,
}
struct Candidate {
    handle: TopicHandle,
    id: TopicId,
    snapshot: Arc<crate::topic::MetadataSnapshot>,
    row: usize,
    partitions: Vec<PartitionMetadata>,
    prepared: Option<TopicUpdate>,
    count: usize,
    old: TopicState,
}
pub(super) struct MetadataWork {
    now: RuntimeInstant,
    handles: Vec<TopicHandle>,
    raw: Option<MetadataUpdate>,
    brokers: IntoIter<BrokerNode>,
    snapshot_brokers: Option<Arc<crate::topic::MetadataBrokers>>,
    topics: IntoIter<MetadataTopic>,
    phase: Phase,
    failure_response: bool,
    cursor: usize,
    row: usize,
    broker_cursor: Option<i32>,
    broker_ids: BTreeSet<i32>,
    selected: BTreeSet<usize>,
    topic_ids: BTreeSet<TopicId>,
    handle_ids: BTreeSet<TopicHandle>,
    bytes: usize,
    partitions: usize,
    largest_topic: usize,
    throttle_ms: u32,
    candidate: Option<Candidate>,
    notice: Option<TopicHandle>,
    _guard: Option<Arc<HeldCredits>>,
}
impl MetadataWork {
    fn new(
        now: RuntimeInstant,
        handles: Vec<TopicHandle>,
        raw: MetadataUpdate,
        guard: Option<Arc<HeldCredits>>,
    ) -> Self {
        Self {
            now,
            handles,
            raw: Some(raw),
            brokers: Vec::new().into_iter(),
            snapshot_brokers: None,
            topics: Vec::new().into_iter(),
            phase: Phase::Start,
            failure_response: false,
            cursor: 0,
            row: 0,
            broker_cursor: None,
            broker_ids: BTreeSet::new(),
            selected: BTreeSet::new(),
            topic_ids: BTreeSet::new(),
            handle_ids: BTreeSet::new(),
            bytes: 0,
            partitions: 0,
            largest_topic: 0,
            throttle_ms: 0,
            candidate: None,
            notice: None,
            _guard: guard,
        }
    }
    fn charge(&mut self, bytes: usize, maximum: usize) -> Result<()> {
        self.bytes = self
            .bytes
            .checked_add(bytes)
            .ok_or(EngineError::InvalidState("metadata byte limit"))?;
        if self.bytes > maximum {
            return Err(EngineError::InvalidState("metadata byte limit"));
        }
        Ok(())
    }
    fn own_arrays(&mut self) {
        if let Some(raw) = self.raw.take() {
            self.throttle_ms = raw.throttle_ms;
            self.brokers = raw.brokers.into_iter();
            self.topics = raw.topics.into_iter();
        }
    }
    // No callback drops an entire tree or a vector of String-owning rows.
    fn discard_step(&mut self) -> bool {
        self.own_arrays();
        self.notice = None;
        if self.candidate.take().is_some() {
            return false;
        }
        if self.brokers.next().is_some() || self.topics.next().is_some() {
            return false;
        }
        if self.broker_ids.pop_first().is_some()
            || self.selected.pop_first().is_some()
            || self.topic_ids.pop_first().is_some()
            || self.handle_ids.pop_first().is_some()
        {
            return false;
        }
        true
    }
}
impl ProducerEngine {
    /// Invalidates the refresh timer while retaining the last immutable view.
    /// A concurrent topic close or in-flight metadata request makes this a no-op.
    pub fn request_metadata_refresh(&mut self, handle: TopicHandle, now: RuntimeInstant) {
        if !self.metadata_pending.contains(&handle)
            && self.failed.is_none()
            && self
                .topics
                .get(handle)
                .is_ok_and(|topic| matches!(topic.state, TopicState::Ready | TopicState::Resolving))
        {
            self.deadlines.set(DeadlineKey::Topic(handle), now);
        }
    }
    /// Retains one fully decoded response and its control working-storage lease.
    /// `on_deadline` performs at most its item budget; drain metadata notices to
    /// resume after a topic publication. No frame is copied at admission.
    pub fn begin_metadata(
        &mut self,
        now: RuntimeInstant,
        handles: Vec<TopicHandle>,
        update: MetadataUpdate,
        guard: Option<Arc<HeldCredits>>,
    ) -> Result<()> {
        if self.metadata_work.is_some() {
            return Err(EngineError::InvalidState("metadata update already pending"));
        }
        self.observe_metrics_time(now);
        self.metadata_work = Some(MetadataWork::new(now, handles, update, guard));
        Ok(())
    }
    /// Failed responses retire their requested-handle bookkeeping incrementally.
    pub fn begin_metadata_failure(
        &mut self,
        now: RuntimeInstant,
        handles: Vec<TopicHandle>,
        guard: Option<Arc<HeldCredits>>,
    ) -> Result<()> {
        self.begin_metadata(
            now,
            handles,
            MetadataUpdate {
                throttle_ms: 0,
                brokers: Vec::new(),
                topics: Vec::new(),
                cluster_id: None,
                controller_id: -1,
            },
            guard,
        )?;
        self.metadata_work
            .as_mut()
            .expect("admitted failure")
            .failure_response = true;
        Ok(())
    }
    pub fn has_metadata_work(&self) -> bool {
        self.metadata_work.is_some()
    }
    pub fn take_metadata_notice(&mut self) -> Option<TopicHandle> {
        self.metadata_work.as_mut()?.notice.take()
    }
    pub fn take_metadata_error(&mut self) -> Option<EngineError> {
        self.metadata_error.take()
    }
    /// Synchronous convenience for passive callers. Poll-driven owners use
    /// `begin_metadata` and budgeted maintenance instead.
    pub fn apply_metadata(
        &mut self,
        now: RuntimeInstant,
        handles: &[TopicHandle],
        update: MetadataUpdate,
    ) -> Result<()> {
        self.begin_metadata(now, handles.to_vec(), update, None)?;
        while self.has_metadata_work() {
            self.take_metadata_notice();
            self.metadata_step();
        }
        self.take_metadata_error().map_or(Ok(()), Err)
    }
    pub(super) fn metadata_step(&mut self) -> bool {
        let Some(mut work) = self.metadata_work.take() else {
            return false;
        };
        if self.failed.is_some() {
            work.phase = Phase::Discard;
        }
        if work.notice.is_some() && work.phase != Phase::Discard {
            self.metadata_work = Some(work);
            return false;
        }
        let complete = match self.metadata_visit(&mut work) {
            Ok(complete) => complete,
            Err(error) => {
                self.metadata_error.get_or_insert(error);
                work.phase = Phase::Discard;
                false
            }
        };
        if !complete {
            self.metadata_work = Some(work);
        } else {
            self.refresh_recovery(work.now);
            self.complete_fences();
        }
        true
    }
    fn metadata_visit(&mut self, work: &mut MetadataWork) -> Result<bool> {
        let cap = self.config.control_reserve_bytes;
        match work.phase {
            Phase::Start => {
                let raw = work.raw.as_ref().expect("unvalidated response");
                if raw.brokers.len() > self.config.brokers_max as usize
                    || raw.topics.len() > work.handles.len()
                    || work.handles.len() > self.config.max_open_topics as usize
                {
                    return Err(EngineError::InvalidState("metadata count limit"));
                }
                let bytes = work
                    .handles
                    .capacity()
                    .checked_mul(size_of::<TopicHandle>())
                    .and_then(|n| {
                        n.checked_add(
                            raw.brokers
                                .capacity()
                                .checked_mul(size_of::<BrokerNode>())?,
                        )
                    })
                    .and_then(|n| {
                        n.checked_add(
                            raw.topics
                                .capacity()
                                .checked_mul(size_of::<MetadataTopic>())?,
                        )
                    })
                    .and_then(|n| {
                        n.checked_add(raw.cluster_id.as_ref().map_or(0, String::capacity))
                    })
                    .ok_or(EngineError::InvalidState("metadata byte limit"))?;
                work.charge(bytes, cap)?;
                work.phase = if work.failure_response {
                    Phase::RemainingHandles
                } else {
                    Phase::Handles
                };
            }
            Phase::Handles => {
                if let Some(&handle) = work.handles.get(work.cursor) {
                    if !work.handle_ids.insert(handle) {
                        return Err(EngineError::InvalidState("duplicate metadata handle"));
                    }
                    work.cursor += 1;
                } else {
                    work.cursor = 0;
                    work.phase = Phase::Brokers;
                }
            }
            Phase::Brokers => {
                let raw = work.raw.as_ref().expect("unvalidated response");
                if let Some(node) = raw.brokers.get(work.cursor) {
                    if node.id < 0
                        || node.host.is_empty()
                        || node.host.len() > 253
                        || node.port == 0
                        || !work.broker_ids.insert(node.id)
                    {
                        return Err(EngineError::InvalidState("invalid broker metadata"));
                    }
                    let bytes = node
                        .host
                        .capacity()
                        .checked_add(node.rack.as_ref().map_or(0, String::capacity))
                        .ok_or(EngineError::InvalidState("metadata byte limit"))?;
                    work.charge(bytes, cap)?;
                    work.cursor += 1;
                } else {
                    if raw.controller_id < -1
                        || (raw.controller_id >= 0 && !work.broker_ids.contains(&raw.controller_id))
                    {
                        return Err(EngineError::InvalidState("invalid metadata controller"));
                    }
                    work.cursor = 0;
                    work.phase = Phase::Topics;
                }
            }
            Phase::Topics => {
                let raw = work.raw.as_ref().expect("unvalidated response");
                if let Some(topic) = raw.topics.get(work.cursor) {
                    if topic.requested_index >= work.handles.len()
                        || !work.selected.insert(topic.requested_index)
                        || (!topic.id.is_zero() && !work.topic_ids.insert(topic.id))
                    {
                        return Err(EngineError::InvalidState("metadata selector mismatch"));
                    }
                    if topic.error_code == code::NONE
                        && (topic.id.is_zero()
                            || topic.name.is_none()
                            || topic.partitions.is_empty())
                    {
                        return Err(EngineError::InvalidState("invalid topic metadata"));
                    }
                    if topic.name.as_ref().is_some_and(|name| name.len() > 249) {
                        return Err(EngineError::InvalidState("metadata topic name limit"));
                    }
                    work.partitions = work
                        .partitions
                        .checked_add(topic.partitions.len())
                        .ok_or(EngineError::InvalidState("metadata partition limit"))?;
                    if work.partitions > self.config.max_batches as usize {
                        return Err(EngineError::InvalidState("metadata partition limit"));
                    }
                    work.largest_topic = work.largest_topic.max(topic.partitions.len());
                    let bytes = topic
                        .partitions
                        .capacity()
                        .checked_mul(size_of::<MetadataPartition>())
                        .and_then(|n| {
                            n.checked_add(topic.name.as_ref().map_or(0, String::capacity))
                        })
                        .ok_or(EngineError::InvalidState("metadata byte limit"))?;
                    work.charge(bytes, cap)?;
                    work.row = 0;
                    work.phase = Phase::Rows;
                } else {
                    work.charge(
                        work.largest_topic
                            .checked_mul(size_of::<PartitionMetadata>())
                            .ok_or(EngineError::InvalidState("metadata byte limit"))?,
                        cap,
                    )?;
                    work.snapshot_brokers = Some(crate::topic::MetadataBrokers::retain(
                        &work.raw.as_ref().expect("validated response").brokers,
                        &self.credits,
                    )?);
                    work.own_arrays();
                    work.cursor = 0;
                    work.phase = Phase::RemoveBrokers;
                }
            }
            Phase::Rows => {
                let topic = &work.raw.as_ref().expect("unvalidated response").topics[work.cursor];
                if let Some(row) = topic.partitions.get(work.row) {
                    if row.index != work.row as i32
                        || row.metadata.leader < -1
                        || row.metadata.leader_epoch < -1
                    {
                        return Err(EngineError::InvalidState("non-dense partition metadata"));
                    }
                    work.row += 1;
                    let bytes =
                        (row.replicas.capacity() + row.isr.capacity() + row.offline.capacity())
                            .checked_mul(size_of::<i32>())
                            .ok_or(EngineError::InvalidState("metadata byte limit"))?;
                    for nodes in [&row.replicas, &row.isr, &row.offline] {
                        if nodes.len() > self.config.brokers_max as usize
                            || nodes.iter().any(|node| *node < 0)
                        {
                            return Err(EngineError::InvalidState("invalid metadata replicas"));
                        }
                    }
                    work.charge(bytes, cap)?;
                } else {
                    work.cursor += 1;
                    work.phase = Phase::Topics;
                }
            }
            Phase::RemoveBrokers => {
                let next = match work.broker_cursor {
                    Some(cursor) => self
                        .brokers
                        .range((Bound::Excluded(cursor), Bound::Unbounded))
                        .next(),
                    None => self.brokers.first_key_value(),
                };
                if let Some((&id, broker)) = next {
                    work.broker_cursor = Some(id);
                    if !work.broker_ids.contains(&id)
                        && broker.requests == 0
                        && !(0..self.config.lanes).any(|lane| self.routes.contains_key(&(id, lane)))
                    {
                        self.brokers.remove(&id);
                        self.deadlines.remove(DeadlineKey::Broker(id));
                    }
                } else {
                    work.phase = Phase::InstallBrokers;
                }
            }
            Phase::InstallBrokers => {
                if let Some(node) = work.brokers.next() {
                    self.install_metadata_broker(node, work.now, work.throttle_ms)?;
                } else {
                    work.phase = Phase::InstallTopics;
                }
            }
            Phase::InstallTopics => self.metadata_topic_step(work)?,
            Phase::RemainingHandles => {
                if let Some(&handle) = work.handles.get(work.cursor) {
                    work.cursor += 1;
                    if self.metadata_pending.remove(&handle) {
                        self.refresh_topic_deadline(handle, work.now);
                    }
                } else {
                    work.phase = Phase::Discard;
                }
            }
            Phase::Discard => return Ok(work.discard_step()),
        }
        Ok(false)
    }
    fn install_metadata_broker(
        &mut self,
        node: BrokerNode,
        now: RuntimeInstant,
        throttle_ms: u32,
    ) -> Result<()> {
        if !self.brokers.contains_key(&node.id)
            && self.brokers.len() >= self.config.brokers_max as usize
        {
            return Err(EngineError::InvalidState("broker capacity exhausted"));
        }
        let until = Self::deadline_after(
            now,
            RuntimeDuration::from_nanos(u64::from(throttle_ms) * 1_000_000),
        );
        let broker = match self.brokers.entry(node.id) {
            std::collections::btree_map::Entry::Occupied(entry) => {
                let broker = entry.into_mut();
                broker.node = node;
                broker
            }
            std::collections::btree_map::Entry::Vacant(entry) => entry.insert(BrokerState {
                metrics_scope: self.metrics.recorder.register_broker(node.id),
                round_trip: crate::estimation::RoundTripTime::default(),
                node,
                bytes: 0,
                requests: 0,
                throttle_until: RuntimeInstant::ZERO,
            }),
        };
        broker.throttle_until = broker.throttle_until.max(until);
        if throttle_ms != 0 {
            self.deadlines
                .set(DeadlineKey::Broker(broker.node.id), broker.throttle_until);
        }
        Ok(())
    }
    fn metadata_notice(&mut self, work: &mut MetadataWork, handle: TopicHandle) {
        self.metadata_pending.remove(&handle);
        self.refresh_topic_deadline(handle, work.now);
        work.notice = Some(handle);
    }
    fn metadata_topic_step(&mut self, work: &mut MetadataWork) -> Result<()> {
        if let Some(mut candidate) = work.candidate.take() {
            if let Some(prepared) = candidate.prepared.as_mut() {
                match self.topics.update_step(prepared, work.now) {
                    Ok(UpdateProgress::Pending) => {
                        work.candidate = Some(candidate);
                    }
                    Ok(UpdateProgress::Applied { changed }) => {
                        Arc::get_mut(&mut candidate.snapshot)
                            .expect("unpublished snapshot")
                            .routing_generation = self.topics.get(candidate.handle)?.generation;
                        self.topics
                            .install_snapshot(candidate.handle, candidate.snapshot.clone());
                        if changed {
                            self.retry_topology_changed();
                        }
                        self.encoder_topic_changed(candidate.id);
                        self.scheduler_topic_changed(candidate.id);
                        if candidate.old == TopicState::Resolving
                            && let Some(credit) = self.topic_event_credits.remove(&candidate.handle)
                        {
                            self.event(
                                Event::TopicReady {
                                    topic: candidate.handle,
                                    id: candidate.id,
                                    partitions: candidate.count as i32,
                                },
                                credit,
                            );
                        }
                        self.metadata_notice(work, candidate.handle);
                    }
                    Err(error) => {
                        self.metadata_topic_error(candidate.handle, error)?;
                        self.metadata_notice(work, candidate.handle);
                    }
                }
            } else if let Some(row) = candidate.snapshot.partitions.get(candidate.row) {
                candidate.row += 1;
                candidate.partitions.push(if row.error_code == 0 {
                    row.metadata
                } else {
                    PartitionMetadata {
                        leader: -1,
                        leader_epoch: row.metadata.leader_epoch,
                    }
                });
                work.candidate = Some(candidate);
            } else {
                match self.topics.prepare_update(
                    candidate.handle,
                    candidate.id,
                    std::mem::take(&mut candidate.partitions),
                ) {
                    Ok(prepared) => {
                        candidate.prepared = Some(prepared);
                        work.candidate = Some(candidate);
                    }
                    Err(error) => {
                        self.metadata_topic_error(candidate.handle, error)?;
                        self.metadata_notice(work, candidate.handle);
                    }
                }
            }
            return Ok(());
        }
        let Some(topic) = work.topics.next() else {
            work.cursor = 0;
            work.phase = Phase::RemainingHandles;
            return Ok(());
        };
        let handle = work.handles[topic.requested_index];
        let Ok(current) = self.topics.get(handle) else {
            self.metadata_notice(work, handle);
            return Ok(());
        };
        if !matches!(current.state, TopicState::Resolving | TopicState::Ready) {
            self.metadata_notice(work, handle);
            return Ok(());
        }
        if topic.error_code == code::NONE {
            let mut partitions = Vec::new();
            partitions
                .try_reserve_exact(topic.partitions.len())
                .map_err(|_| EngineError::AllocationFailed)?;
            // Account allocator over-reservation rather than assuming exact reserve.
            let extra = partitions
                .capacity()
                .saturating_sub(work.largest_topic)
                .checked_mul(size_of::<PartitionMetadata>())
                .ok_or(EngineError::InvalidState("metadata byte limit"))?;
            work.charge(extra, self.config.control_reserve_bytes)?;
            work.candidate = Some(Candidate {
                handle,
                id: topic.id,
                count: topic.partitions.len(),
                old: current.state,
                snapshot: crate::topic::MetadataSnapshot::retain(
                    topic.id,
                    current
                        .snapshot
                        .as_ref()
                        .map_or(Some(1), |snapshot| snapshot.generation.checked_add(1))
                        .ok_or(EngineError::InvalidState(
                            "metadata snapshot generation exhausted",
                        ))?,
                    work.snapshot_brokers
                        .as_ref()
                        .expect("validated brokers")
                        .clone(),
                    topic.partitions,
                    &self.credits,
                )?,
                row: 0,
                partitions,
                prepared: None,
            });
        } else {
            if matches!(
                topic.error_code,
                code::UNKNOWN_TOPIC_ID | code::UNKNOWN_TOPIC_OR_PARTITION
            ) {
                let state = self.topics.unknown(handle, work.now)?;
                if matches!(state, TopicState::Deleted | TopicState::Failed) {
                    self.settle_topic(handle, FailureReason::TopicDeleted);
                }
            } else {
                self.topics.mark_failed(handle, false)?;
                self.settle_topic(handle, FailureReason::TopicResolution);
            }
            self.metadata_notice(work, handle);
        }
        Ok(())
    }
    fn metadata_topic_error(&mut self, handle: TopicHandle, error: TopicError) -> Result<()> {
        if matches!(
            error,
            TopicError::StaleLeaderEpoch | TopicError::TopicClosed
        ) {
            return Ok(());
        }
        if self
            .topics
            .get(handle)
            .is_ok_and(|topic| matches!(topic.state, TopicState::Failed | TopicState::Deleted))
        {
            return Ok(());
        }
        self.topics.mark_failed(handle, true)?;
        self.settle_topic(handle, FailureReason::TopicDeleted);
        Ok(())
    }
}

#[cfg(test)]
mod tests;
