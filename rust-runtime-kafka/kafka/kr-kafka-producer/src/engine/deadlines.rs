use super::*;

impl ProducerEngine {
    pub(super) fn pending_precedes_front(&self, key: TopicPartition) -> bool {
        self.partitions
            .get(&key)
            .and_then(|queue| queue.records.front())
            .is_some_and(|front| {
                [None, Some(key.partition)].into_iter().any(|hint| {
                    self.pending_routes
                        .get(&(front.topic, hint))
                        .and_then(|tokens| tokens.first())
                        .is_some_and(|token| *token < front.token)
                })
            })
    }
    pub fn on_deadline(&mut self, now: RuntimeInstant, budget: WorkBudget) -> Progress {
        self.observe_metrics_time(now);
        let mut progress = Progress::default();
        while progress.items < budget.items {
            let Some(key) = self.deadlines.due(now) else {
                break;
            };
            progress.items += 1;
            match key {
                DeadlineKey::Batch(packed) => {
                    let key = Slot::from_packed(packed);
                    if let Some(batch) = self.batches.get(key) {
                        if batch
                            .oldest_deadline()
                            .is_some_and(|deadline| now >= deadline)
                        {
                            self.expire_batch(key, FailureReason::Deadline);
                        } else {
                            let partition = batch.partition();
                            let credit = self.has_dispatch_credit(partition, now);
                            let sparse = self.partitions[&partition]
                                .arrival
                                .below(self.config.linger_skip_below_rate);
                            if let Some(batch) = self.batches.get_mut(key) {
                                batch.seal_due(now, credit, sparse);
                            }
                            self.refresh_batch_deadline(key, now);
                        }
                    }
                }
                DeadlineKey::Pending(token) => {
                    if let Some(record) = self.take_pending(token) {
                        self.fail_record(record, FailureReason::Deadline);
                    } else if let Some(partition) = self.queued_locations.get(&token).copied()
                        && let Some(queue) = self.partitions.get_mut(&partition)
                        && let Some(record) = queue.records.remove_token(token)
                    {
                        self.fail_record(record, FailureReason::Deadline);
                    }
                }
                DeadlineKey::Topic(handle) => {
                    let state = self
                        .topics
                        .get(handle)
                        .ok()
                        .map(|topic| (topic.state, topic.resolution_deadline));
                    if let Some((TopicState::Resolving, deadline)) = state
                        && now >= deadline
                    {
                        let _ = self.topics.unknown(handle, now);
                        self.settle_topic(handle, FailureReason::TopicResolution);
                    } else if state.is_some_and(|(state, _)| {
                        matches!(state, TopicState::Resolving | TopicState::Ready)
                    }) && !self.metadata_pending.contains(&handle)
                        && self.failed.is_none()
                    {
                        if self
                            .order(EngineOrder::Metadata {
                                handles: vec![handle],
                            })
                            .is_ok()
                        {
                            self.metadata_pending.insert(handle);
                        }
                        if let Some((TopicState::Resolving, deadline)) = state {
                            self.deadlines.set(DeadlineKey::Topic(handle), deadline);
                        }
                    }
                }
                DeadlineKey::Request(key) => {
                    if let Some(request) = self.requests.get(Slot::from_packed(key.0))
                        && now >= request.deadline
                    {
                        let connection = request.connection;
                        let correlation = request.correlation;
                        self.retry_connection(connection, now);
                        self.retire_connection(connection, RetireReason::Deadline { correlation });
                    }
                }
                DeadlineKey::Retry(partition) => {
                    if let Some(queue) = self.partitions.get_mut(&partition) {
                        queue.retry_at = now;
                    }
                    self.scheduler_mark(partition);
                }
                DeadlineKey::Broker(id) => {
                    if let Some(broker) = self.brokers.get_mut(&id) {
                        broker.throttle_until = now;
                    }
                    for lane in 0..self.config.lanes {
                        self.encoder_dispatch_changed(id, lane);
                        self.scheduler_connection_changed(id, lane);
                    }
                }
                DeadlineKey::Close => {
                    if self
                        .close
                        .as_ref()
                        .is_some_and(|close| now >= close.deadline)
                    {
                        self.fail_all(FailureReason::Closed);
                    }
                }
            }
        }
        while progress.items < budget.items && self.failure_step() {
            progress.items += 1;
        }
        while progress.items < budget.items && self.metadata_step() {
            progress.items += 1;
        }
        while progress.items < budget.items && self.retry_step() {
            progress.items += 1;
        }
        while progress.items < budget.items && self.identity_step() {
            progress.items += 1;
        }
        while progress.items < budget.items && self.topic_settlement_step() {
            progress.items += 1;
        }
        while progress.items < budget.items && self.partition_cleanup_step() {
            progress.items += 1;
        }
        if progress.items < budget.items && self.headroom_sweep.is_some() {
            // Continuous timing samples cannot monopolize maintenance. With a
            // one-item quota, alternate refresh priority with seals/fences;
            // larger quotas spend all remaining spare capacity below.
            let first = self.headroom_first;
            self.headroom_first = !first;
            if first && self.headroom_step(now) {
                progress.items += 1;
            }
        }
        while progress.items < budget.items && self.seal_step(now) {
            progress.items += 1;
        }
        let reclaimed = self
            .output
            .reclaim_step((budget.items - progress.items) as usize);
        progress.items += reclaimed.work_items as u32;
        if reclaimed.work_items != 0 {
            self.encoder_output_reclaimed();
        }
        self.complete_fences();
        while progress.items < budget.items && self.fence_step() {
            progress.items += 1;
        }
        while progress.items < budget.items && self.headroom_step(now) {
            progress.items += 1;
        }
        self.refresh_recovery(now);
        self.complete_fences();
        progress.remaining_immediate = self.has_maintenance_work()
            || self.deadlines.next().is_some_and(|at| at <= now)
            || self.has_terminal_work();
        progress
    }
    pub(super) fn refresh_batch_deadline(&mut self, key: BatchKey, now: RuntimeInstant) {
        self.metrics_batch(key);
        self.encoder_refresh_batch(key, false);
        if let Some(batch) = self.batches.get(key) {
            self.scheduler_mark(batch.partition());
        }
        let Some(batch) = self.batches.get(key) else {
            return;
        };
        let dispatch = self.has_dispatch_credit(batch.partition(), now);
        let at = batch
            .oldest_deadline()
            .into_iter()
            .chain(batch.next_seal_deadline(dispatch))
            .chain(
                (matches!(batch.state(), BatchState::Sealed | BatchState::Ready)
                    && batch.dispatch_ready_at() > now)
                    .then_some(batch.dispatch_ready_at()),
            )
            .min();
        if let Some(at) = at {
            self.deadlines.set(DeadlineKey::Batch(key.packed()), at);
        } else {
            self.deadlines.remove(DeadlineKey::Batch(key.packed()));
        }
    }
    pub(super) fn retry_connection(&mut self, key: ConnectionKey, now: RuntimeInstant) {
        self.schedule_connection_retry(key, now);
    }
}
