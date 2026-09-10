use super::*;

impl ProducerEngine {
    pub fn open_topic(&mut self, name: &str, now: RuntimeInstant) -> Result<TopicHandle> {
        self.observe_metrics_time(now);
        if self.close.is_some() || self.failed.is_some() {
            return Err(EngineError::Closed);
        }
        let credit = self.control_credit()?;
        let handle = self.topics.open(name, now)?;
        self.topic_event_credits.insert(handle, credit);
        self.deadlines.set(DeadlineKey::Topic(handle), now);
        Ok(handle)
    }
    pub fn open_topic_reserved(
        &mut self,
        handle: TopicHandle,
        name: &str,
        now: RuntimeInstant,
    ) -> Result<TopicHandle> {
        let credit = self.control_credit()?;
        self.open_topic_reserved_credit(handle, name, now, credit)
    }
    /// Consumes a control-event reservation from this engine's authority.
    /// # Errors
    /// A foreign guard is released to its source authority without changing
    /// this engine or publishing an event.
    pub fn open_topic_reserved_credit(
        &mut self,
        handle: TopicHandle,
        name: &str,
        now: RuntimeInstant,
        credit: HeldCredits,
    ) -> Result<TopicHandle> {
        if !credit.belongs_to(&self.credits) {
            return Err(CreditError::ForeignCredit.into());
        }
        self.observe_metrics_time(now);
        if credit.amount(Resource::ControlEvents) != 1 {
            return Err(EngineError::InvalidState(
                "topic open requires one control event credit",
            ));
        }
        // An emergency close can overtake an already accepted open command.
        // The reserved command still receives its terminal event and handle.
        if self.failed.is_some() || self.closed {
            self.event(
                Event::TopicFailed {
                    topic: handle,
                    code: FailureReason::Closed as u32,
                },
                credit,
            );
            return Err(EngineError::Closed);
        }
        match self.topics.open_reserved(handle, name, now) {
            Ok(handle) => {
                self.topic_event_credits.insert(handle, credit);
                self.deadlines.set(DeadlineKey::Topic(handle), now);
                Ok(handle)
            }
            Err(error) => {
                self.event(
                    Event::TopicFailed {
                        topic: handle,
                        code: FailureReason::TopicResolution as u32,
                    },
                    credit,
                );
                Err(error.into())
            }
        }
    }
    pub fn close_topic(&mut self, handle: TopicHandle, now: RuntimeInstant) -> Result<()> {
        self.observe_metrics_time(now);
        self.topics.get(handle)?;
        self.settle_topic(handle, FailureReason::Closed);
        self.deadlines.remove(DeadlineKey::Topic(handle));
        self.metadata_pending.remove(&handle);
        self.topic_event_credits.remove(&handle);
        self.topics.close(handle)?;
        self.refresh_recovery(now);
        Ok(())
    }

    pub fn admit(
        &mut self,
        now: RuntimeInstant,
        batch: SubmissionBatch,
        choices: &[PartitionChoice],
    ) -> Result<Admitted> {
        self.admit_records(now, batch.drain(), choices)
    }

    /// Accept an owner-sliced prefix without copying its owned payloads.
    pub fn admit_records(
        &mut self,
        now: RuntimeInstant,
        mut records: Vec<AdmittedRecord>,
        choices: &[PartitionChoice],
    ) -> Result<Admitted> {
        self.admit_records_buffered(now, &mut records, choices)
    }
    /// Accepts a validated prefix while retaining the caller's descriptor
    /// capacity for its next bounded owner turn.
    /// # Errors
    /// Foreign reservations and invalid token ranges leave the supplied records
    /// unchanged. Records must come from admission sharing this engine's credit
    /// authority. Once accepted, every descriptor moves into the engine or its
    /// terminal delivery queue.
    pub fn admit_records_buffered(
        &mut self,
        now: RuntimeInstant,
        records: &mut Vec<AdmittedRecord>,
        choices: &[PartitionChoice],
    ) -> Result<Admitted> {
        if records
            .iter()
            .any(|record| !record.belongs_to(&self.credits))
        {
            return Err(CreditError::ForeignCredit.into());
        }
        self.observe_metrics_time(now);
        if records.is_empty() {
            return Ok(Admitted::default());
        }
        let count = u32::try_from(records.len())
            .map_err(|_| EngineError::InvalidState("submission exceeds u32"))?;
        let first = records[0].token;
        if records
            .iter()
            .enumerate()
            .any(|(index, record)| first.0.checked_add(index as u64) != Some(record.token.0))
        {
            return Err(EngineError::InvalidState(
                "submission token range is not dense",
            ));
        }
        self.tracker.accept(first, count)?;
        let choices_valid = choices.len() == records.len();
        let mut result = Admitted {
            records: count,
            ..Admitted::default()
        };
        for (index, record) in records.drain(..).enumerate() {
            self.index_topic_record(&record);
            let reason = if !choices_valid {
                Some(FailureReason::InvalidRecord)
            } else if self.failed.is_some() {
                Some(self.failed.unwrap_or(FailureReason::PartitionFailed))
            } else if self
                .close
                .as_ref()
                .is_some_and(|close| record.token > close.watermark)
                || self.closed
            {
                Some(FailureReason::Closed)
            } else if now >= record.deadline {
                Some(FailureReason::Deadline)
            } else {
                None
            };
            if let Some(reason) = reason {
                self.fail_record(record, reason);
                result.failed += 1;
                continue;
            }
            match self.route_record(now, record, choices[index]) {
                RouteResult::Queued => {}
                RouteResult::Pending => result.pending += 1,
                RouteResult::Failed => result.failed += 1,
            }
        }
        self.complete_fences();
        Ok(result)
    }

    /// Snapshot used by both native and built-in routing policies. A known
    /// leader is usable before a connection exists; throttling remains explicit.
    pub fn partition_snapshot(
        &self,
        now: RuntimeInstant,
        key: TopicPartition,
    ) -> PartitionSnapshot {
        let queue = self.partitions.get(&key);
        let metadata = self
            .topics
            .by_id(key.topic)
            .and_then(|handle| self.topics.get(handle).ok())
            .filter(|topic| topic.state == TopicState::Ready)
            .and_then(|topic| {
                usize::try_from(key.partition)
                    .ok()
                    .and_then(|index| topic.partitions.get(index))
            });
        let broker = metadata.and_then(|metadata| self.brokers.get(&metadata.leader));
        let lane = queue.map_or_else(
            || (key.partition.max(0) as u32 % u32::from(self.config.lanes)) as u8,
            |queue| queue.lane,
        );
        let mut queued_bytes = 0u64;
        let mut oldest = None;
        let mut open_batch_bytes = 0u32;
        if let Some(queue) = queue {
            queued_bytes = queue.records.bytes();
            oldest = queue.records.oldest();
            queued_bytes += queue.batch_bytes;
            if let Some(&(at, _)) = queue.batch_ages.first() {
                oldest = Some(oldest.map_or(at, |old| old.min(at)));
            }
            if let Some(batch) = queue.batches.back().and_then(|key| self.batches.get(*key))
                && batch.state() == BatchState::Open
            {
                open_batch_bytes = batch.raw_bytes();
            }
        }
        let throttled_until = broker
            .map_or(RuntimeInstant::ZERO, |broker| broker.throttle_until)
            .max(queue.map_or(RuntimeInstant::ZERO, |queue| queue.retry_at));
        PartitionSnapshot {
            partition: key.partition,
            lane,
            queued_bytes,
            oldest_age: oldest
                .and_then(|at| now.checked_duration_since(at))
                .unwrap_or(RuntimeDuration::ZERO),
            open_batch_bytes,
            throttled_until,
            drain_bytes_per_second: queue.map_or(0, |queue| queue.drain_bytes_per_second),
            available: self.failed.is_none()
                && metadata.is_some_and(|metadata| metadata.leader >= 0)
                && broker.is_some()
                && throttled_until <= now,
        }
    }
    /// Before batching cancellation removes one descriptor. An immutable batch
    /// is the cancellation unit afterwards, preserving sequence and wire bytes.
    pub fn cancel(&mut self, now: RuntimeInstant, token: RecordToken) -> Result<()> {
        self.observe_metrics_time(now);
        if let Some(record) = self.take_pending(token) {
            self.fail_record(record, FailureReason::Cancelled);
        } else if let Some(partition) = self.queued_locations.get(&token).copied() {
            if let Some(queue) = self.partitions.get_mut(&partition)
                && let Some(record) = queue.records.remove_token(token)
            {
                self.fail_record(record, FailureReason::Cancelled);
            }
        } else {
            let key = self.batched_locations.get(&token).copied();
            if let Some(key) = key {
                self.expire_batch(key, FailureReason::Cancelled);
            } else if token.0 == 0 || token > self.tracker.accepted() {
                return Err(EngineError::InvalidState("unknown record token"));
            }
        }
        self.refresh_recovery(now);
        self.complete_fences();
        Ok(())
    }

    pub fn pending_records(&self) -> impl Iterator<Item = &AdmittedRecord> {
        self.pending.values()
    }
    #[must_use]
    pub fn pending_record(&self, token: RecordToken) -> Option<&AdmittedRecord> {
        self.pending.get(&token)
    }
    /// Ordered raw frontier for resumable owner work. Unresolved records still
    /// count as visits; callers must bound before filtering for routable topics.
    pub fn pending_records_after(
        &self,
        after: Option<RecordToken>,
    ) -> impl Iterator<Item = &AdmittedRecord> {
        use std::ops::Bound::{Excluded, Unbounded};
        self.pending
            .range((after.map_or(Unbounded, Excluded), Unbounded))
            .map(|(_, record)| record)
    }
    pub fn route_pending(
        &mut self,
        now: RuntimeInstant,
        token: RecordToken,
        choice: PartitionChoice,
    ) -> Result<()> {
        self.observe_metrics_time(now);
        let before = self
            .pending
            .get(&token)
            .ok_or(EngineError::InvalidState("unknown pending record"))?;
        let topic = before.topic;
        let routes = [
            before.partition_hint,
            match choice {
                PartitionChoice::Pending => before.partition_hint,
                PartitionChoice::Partition(index) => Some(index),
            },
        ];
        let fronts = routes.map(|hint| {
            self.pending_routes
                .get(&(topic, hint))
                .and_then(|tokens| tokens.first())
                .copied()
        });
        self.encoder.pending_suppressed = true;
        let record = self
            .take_pending(token)
            .ok_or(EngineError::InvalidState("unknown pending record"))?;
        if now >= record.deadline {
            self.fail_record(record, FailureReason::Deadline);
        } else {
            self.route_record(now, record, choice);
        }
        self.encoder.pending_suppressed = false;
        for (index, hint) in routes.into_iter().enumerate() {
            let after = self
                .pending_routes
                .get(&(topic, hint))
                .and_then(|tokens| tokens.first())
                .copied();
            if fronts[index] != after {
                self.encoder_pending_changed(topic, hint);
            }
        }
        self.complete_fences();
        Ok(())
    }
    fn route_record(
        &mut self,
        now: RuntimeInstant,
        mut record: AdmittedRecord,
        choice: PartitionChoice,
    ) -> RouteResult {
        let topic = match self.topics.get(record.topic) {
            Ok(topic) => topic,
            Err(_) => {
                self.fail_record(record, FailureReason::Closed);
                return RouteResult::Failed;
            }
        };
        if matches!(topic.state, TopicState::Deleted | TopicState::Failed) {
            self.fail_record(record, FailureReason::TopicDeleted);
            return RouteResult::Failed;
        }
        if topic.state == TopicState::Resolving || choice == PartitionChoice::Pending {
            return self.hold_pending(record);
        }
        let PartitionChoice::Partition(partition) = choice else {
            return self.hold_pending(record);
        };
        if partition < 0
            || partition as usize >= topic.partitions.len()
            || record.partition_hint.is_some_and(|hint| hint != partition)
        {
            self.fail_record(record, FailureReason::InvalidRecord);
            return RouteResult::Failed;
        }
        let key = TopicPartition {
            topic: topic.id.expect("ready topic has immutable UUID"),
            partition,
        };
        self.capture_topic_partition(&record, key);
        record.partition_hint = Some(partition);
        let lane = (partition as u32 % u32::from(self.config.lanes)) as u8;
        if record.lane != lane && record.set_lane(lane).is_err() {
            return self.hold_pending(record);
        }
        if !self.partitions.contains_key(&key) {
            if self.partitions.len() == self.config.max_batches as usize {
                self.fail_record(record, FailureReason::ResourceExhausted);
                return RouteResult::Failed;
            }
            if let Some(ledger) = &mut self.ledger
                && ledger.register(key).is_err()
            {
                self.fail_record(record, FailureReason::PartitionFailed);
                return RouteResult::Failed;
            }
            self.partitions.insert(
                key,
                PartitionQueue {
                    compression_estimate: Default::default(),
                    metrics_scope: self
                        .metrics
                        .recorder
                        .register_partition(key.topic.0, key.partition),
                    lane,
                    records: RecordQueue::default(),
                    batches: BatchQueue::default(),
                    batch_bytes: 0,
                    request_owners: 0,
                    terminal_owners: 0,
                    batch_ages: BTreeSet::new(),
                    arrival: ArrivalRate::default(),
                    deficit: 0,
                    retry_at: RuntimeInstant::ZERO,
                    drain_bytes_per_second: 0,
                    last_drain: None,
                },
            );
            self.partition_cleanup_at_capacity();
            self.retry_topology_changed();
        }
        let queue = self.partitions.get_mut(&key).expect("registered partition");
        queue.arrival.observe(now);
        let token = record.token;
        let deadline = record.deadline;
        queue.records.push_back(record);
        self.queued_locations.insert(token, key);
        self.deadlines.set(DeadlineKey::Pending(token), deadline);
        self.encoder_refresh_append(key);
        self.scheduler_mark(key);
        RouteResult::Queued
    }
    fn hold_pending(&mut self, record: AdmittedRecord) -> RouteResult {
        let count = self.pending_counts.entry(record.topic).or_default();
        if *count == self.config.pending_records_per_topic as usize {
            self.fail_record(record, FailureReason::ResourceExhausted);
            return RouteResult::Failed;
        }
        *count += 1;
        self.deadlines
            .set(DeadlineKey::Pending(record.token), record.deadline);
        let route = (record.topic, record.partition_hint);
        let old = self
            .pending_routes
            .get(&route)
            .and_then(|tokens| tokens.first())
            .copied();
        self.pending_routes
            .entry((record.topic, record.partition_hint))
            .or_default()
            .insert(record.token);
        self.pending.insert(record.token, record);
        let new = self
            .pending_routes
            .get(&route)
            .and_then(|tokens| tokens.first())
            .copied();
        if old != new {
            self.encoder_pending_changed(route.0, route.1);
        }
        RouteResult::Pending
    }
    pub(super) fn take_pending(&mut self, token: RecordToken) -> Option<AdmittedRecord> {
        let record = self.pending.remove(&token)?;
        let route = (record.topic, record.partition_hint);
        let old = self
            .pending_routes
            .get(&route)
            .and_then(|tokens| tokens.first())
            .copied();
        if let Some(tokens) = self.pending_routes.get_mut(&route) {
            tokens.remove(&token);
            if tokens.is_empty() {
                self.pending_routes.remove(&route);
            }
        }
        if let Some(count) = self.pending_counts.get_mut(&record.topic) {
            *count -= 1;
            if *count == 0 {
                self.pending_counts.remove(&record.topic);
            }
        }
        self.deadlines.remove(DeadlineKey::Pending(token));
        let new = self
            .pending_routes
            .get(&route)
            .and_then(|tokens| tokens.first())
            .copied();
        if old != new {
            self.encoder_pending_changed(route.0, route.1);
        }
        Some(record)
    }

    pub fn metadata_failed(&mut self, handles: &[TopicHandle], now: RuntimeInstant) {
        self.observe_metrics_time(now);
        for &handle in handles {
            self.metadata_pending.remove(&handle);
            self.refresh_topic_deadline(handle, now);
        }
    }
    pub(super) fn refresh_topic_deadline(&mut self, handle: TopicHandle, now: RuntimeInstant) {
        if let Ok(topic) = self.topics.get(handle) {
            let next = match topic.state {
                TopicState::Resolving => Some(
                    topic
                        .resolution_deadline
                        .min(Self::deadline_after(now, self.config.retry_backoff_min)),
                ),
                TopicState::Ready => Some(if topic.refresh_at > now {
                    topic.refresh_at
                } else {
                    Self::deadline_after(now, self.config.retry_backoff_min)
                }),
                _ => None,
            };
            if let Some(at) = next {
                self.deadlines.set(DeadlineKey::Topic(handle), at);
            } else {
                self.deadlines.remove(DeadlineKey::Topic(handle));
            }
        }
    }
}
enum RouteResult {
    Queued,
    Pending,
    Failed,
}

#[cfg(test)]
mod authority_tests;
