//! Bounded wire scheduling over active indexes and causal invalidations.
use super::*;
use dispatch_queue::Ready;
use dispatch_size::RequestSize;
use dispatch_work::Group;

impl ProducerEngine {
    pub(super) fn scheduler_mark(&mut self, partition: TopicPartition) {
        if self
            .partitions
            .get(&partition)
            .is_none_or(|queue| queue.records.is_empty() && queue.batches.is_empty())
        {
            self.scheduler.forget(partition);
        } else {
            self.scheduler
                .ready
                .track_route(partition, self.destination(partition));
            self.scheduler.mark(partition);
        }
    }

    pub(super) fn scheduler_forget_partition(&mut self, partition: TopicPartition) {
        assert!(
            self.partitions
                .get(&partition)
                .is_none_or(|queue| { queue.records.is_empty() && queue.batches.is_empty() })
        );
        self.scheduler.forget(partition);
    }

    pub(super) fn scheduler_connection_changed(&mut self, broker: i32, lane: u8) {
        self.scheduler.changed(Group::Route(broker, lane));
    }

    pub(super) fn scheduler_topic_changed(&mut self, topic: TopicId) {
        self.scheduler.topic_changed(topic);
    }

    pub(super) fn scheduler_identity_changed(&mut self) {
        self.scheduler.identity_changed();
    }

    fn scheduler_observe_resources(&mut self) {
        let snapshot = self.credits.snapshot();
        let mut changed = false;
        for (index, resource) in [
            Resource::RxBytes,
            Resource::StagingBytes,
            Resource::TlsBytes,
            Resource::RequestSlots,
            Resource::WireWindow,
            Resource::RequestMetadata,
        ]
        .into_iter()
        .enumerate()
        {
            let released = snapshot[resource as usize].released;
            changed |= self.scheduler.observed_releases[index] != released;
            self.scheduler.observed_releases[index] = released;
        }
        if changed {
            self.scheduler.changed(Group::Credits);
        }
    }

    fn scheduler_classify(&mut self, partition: TopicPartition, now: RuntimeInstant) {
        self.scheduler.dirty.remove(&partition);
        let Some(queue) = self.partitions.get(&partition) else {
            self.scheduler.forget(partition);
            return;
        };
        if queue.records.is_empty() && queue.batches.is_empty() {
            self.scheduler.forget(partition);
            return;
        }
        if queue.retry_at > now {
            return;
        }
        if self.ledger.as_ref().is_none_or(|ledger| {
            matches!(
                ledger.recovery_state(),
                RecoveryState::RefreshingIdentity | RecoveryState::FailedClosed
            )
        }) {
            self.scheduler.wait(partition, Group::Identity);
            return;
        }
        let first = queue
            .batches
            .iter()
            .next()
            .and_then(|key| self.batches.get(*key));
        let deadline = first
            .and_then(Batch::oldest_deadline)
            .or_else(|| queue.records.front().map(|record| record.deadline))
            .unwrap_or(now);
        let accepted_at = first
            .and_then(Batch::first_accepted)
            .or_else(|| queue.records.front().map(|record| record.accepted_at))
            .unwrap_or(now);
        let mut ready = Ready {
            route: None,
            lane: queue.lane,
            deadline,
            accepted_at,
        };
        let Some(route) = self
            .destination(partition)
            .filter(|route| self.brokers.contains_key(&route.0))
        else {
            // Exactly one metadata request is scheduled per missing endpoint.
            // Subsequent work waits for the topic's causal metadata update.
            if self.topics.by_id(partition.topic).is_some_and(|handle| {
                !self.metadata_pending.contains(&handle)
                    && self
                        .deadlines
                        .current
                        .get(&DeadlineKey::Topic(handle))
                        .is_none_or(|at| *at > now)
            }) {
                self.scheduler.ready.insert(partition, ready);
            }
            return;
        };
        ready.route = Some(route);
        if self.retry_pending(route.0, route.1) {
            self.scheduler
                .wait(partition, Group::Route(route.0, route.1));
            return;
        }
        let Some(connection) = self.routes.get(&route).copied() else {
            if self.connections.len() == self.validated.max_connections {
                self.scheduler.wait(partition, Group::Credits);
            } else {
                self.scheduler.ready.insert(partition, ready);
            }
            return;
        };
        if !self.connection_credit(connection, now) {
            self.scheduler
                .wait(partition, Group::Route(route.0, route.1));
            return;
        }
        let Some(head) = self.dispatch_head(partition, now) else {
            if self
                .ledger
                .as_ref()
                .is_some_and(|ledger| ledger.recovery_state() == RecoveryState::NeedsIdentity)
            {
                self.scheduler.wait(partition, Group::Identity);
            } else {
                self.scheduler
                    .wait(partition, Group::Route(route.0, route.1));
            }
            return;
        };
        if self.requests.len() == self.validated.credits[Resource::RequestSlots as usize] {
            self.scheduler.wait(partition, Group::Credits);
            return;
        }
        let batch = self.batches.get(head).expect("eligible head");
        ready.deadline = batch.oldest_deadline().expect("nonempty dispatch head");
        ready.accepted_at = batch.first_accepted().expect("nonempty dispatch head");
        self.scheduler.ready.insert(partition, ready);
    }

    pub fn schedule(&mut self, now: RuntimeInstant, budget: WorkBudget) -> Progress {
        self.observe_metrics_time(now);
        let mut progress = Progress::default();
        if self.failed.is_some()
            || self.closed
            || budget.items == 0
            || budget.bytes == 0
            || self.ledger.as_ref().is_none_or(|ledger| {
                matches!(
                    ledger.recovery_state(),
                    RecoveryState::RefreshingIdentity | RecoveryState::FailedClosed
                )
            })
        {
            return progress;
        }
        self.scheduler_observe_resources();
        while progress.items < budget.items
            && progress.bytes < budget.bytes
            && self.failed.is_none()
        {
            let mut worked = false;
            for offset in 0..3 {
                let phase = (self.scheduler.phase + offset) % 3;
                let done = match phase {
                    0 => {
                        if let Some(partition) = self.scheduler.next_dirty() {
                            progress.items += 1;
                            self.scheduler_classify(partition, now);
                            true
                        } else {
                            false
                        }
                    }
                    1 => {
                        if let Some(partition) = self.scheduler.reconsider() {
                            progress.items += 1;
                            if let Some(partition) = partition {
                                if let Some(ready_at) = self.scheduler.gather_ready_at(partition)
                                    && let Some(head) = self.dispatch_head(partition, now)
                                {
                                    self.batches
                                        .get_mut(head)
                                        .expect("gather anchor")
                                        .defer_dispatch_until(ready_at.max(now));
                                    self.refresh_batch_deadline(head, now);
                                }
                                self.scheduler_mark(partition);
                            }
                            true
                        } else {
                            false
                        }
                    }
                    _ => self.scheduler_service(now, budget, &mut progress),
                };
                if done {
                    self.scheduler.phase = (phase + 1) % 3;
                    self.scheduler_observe_resources();
                    worked = true;
                    break;
                }
            }
            if !worked {
                break;
            }
        }
        self.refresh_recovery(now);
        progress.remaining_immediate |= self.failed.is_none()
            && !self.closed
            && (!self.scheduler.dirty.is_empty()
                || self.scheduler.has_reconsideration()
                || !self.scheduler.ready.is_empty());
        progress
    }

    fn scheduler_service(
        &mut self,
        now: RuntimeInstant,
        budget: WorkBudget,
        progress: &mut Progress,
    ) -> bool {
        let cap = self.config.request_hard_bytes as usize;
        let Some((partition, quantum)) = self
            .scheduler
            .ready
            .visit(self.config.request_target_bytes as usize, cap)
        else {
            return false;
        };
        progress.items += 1;
        match self.scheduler_select(partition, now, budget, progress, quantum) {
            Ok(bytes) => progress.bytes = progress.bytes.saturating_add(bytes as u32),
            Err(EngineError::Credit(crate::credit::CreditError::ResourceExhausted { .. })) => {
                self.scheduler.wait(partition, Group::Credits);
            }
            Err(EngineError::Ledger(LedgerError::WindowFull | LedgerError::EarlierBatchReady)) => {
                if let Some((broker, lane)) = self.destination(partition) {
                    self.scheduler.wait(partition, Group::Route(broker, lane));
                } else {
                    self.scheduler_mark(partition);
                }
            }
            Err(EngineError::Ledger(LedgerError::RecoveryPending)) => {
                self.scheduler.wait(partition, Group::Identity);
            }
            Err(EngineError::AllocationFailed | EngineError::Pool(_) | EngineError::Credit(_)) => {
                self.fail(FailureReason::ResourceExhausted);
            }
            Err(_) => self.fail(FailureReason::ProtocolViolation),
        }
        true
    }

    fn scheduler_select(
        &mut self,
        partition: TopicPartition,
        now: RuntimeInstant,
        budget: WorkBudget,
        progress: &mut Progress,
        quantum: usize,
    ) -> Result<usize> {
        // Cached priority never substitutes for checking fences, current
        // metadata, connection credit and ledger order at actual admission.
        let Some(queue) = self.partitions.get(&partition) else {
            self.scheduler.forget(partition);
            return Ok(0);
        };
        if queue.retry_at > now {
            self.scheduler_mark(partition);
            return Ok(0);
        }
        let Some((broker, lane)) = self
            .destination(partition)
            .filter(|route| self.brokers.contains_key(&route.0))
        else {
            self.refresh_partition_metadata(partition, now);
            self.scheduler_mark(partition);
            return Ok(0);
        };
        if self.retry_pending(broker, lane) {
            self.scheduler.wait(partition, Group::Route(broker, lane));
            return Ok(0);
        }
        let Some(connection) = self.routes.get(&(broker, lane)).copied() else {
            self.connect(broker, lane)?;
            self.scheduler.wait(partition, Group::Route(broker, lane));
            return Ok(0);
        };
        if !self.connection_credit(connection, now) {
            self.scheduler.wait(partition, Group::Route(broker, lane));
            return Ok(0);
        }
        let Some(head) = self.dispatch_head(partition, now) else {
            self.scheduler_mark(partition);
            return Ok(0);
        };
        if self
            .attempt_counts
            .get(&head.packed())
            .copied()
            .unwrap_or(0)
            >= u32::from(self.config.max_attempts)
        {
            self.expire_batch(head, FailureReason::BrokerRejected);
            self.scheduler_mark(partition);
            return Ok(0);
        }
        let cap = self.config.request_hard_bytes as usize;
        let mut measure =
            RequestSize::new(self.config.client_id.len()).ok_or(EngineError::AllocationFailed)?;
        let first = self.scheduler_measure(&measure, head)?;
        if first.bytes > cap {
            return Err(EngineError::InvalidState("legal head exceeds request"));
        }
        let queue = self
            .partitions
            .get_mut(&partition)
            .expect("selected partition");
        queue.deficit = queue.deficit.saturating_add(quantum).min(cap);
        if queue.deficit < first.bytes || self.scheduler.ready.lane_deficit(lane) < first.bytes {
            return Ok(0);
        }
        let policy = self.config.request_batching_policy;
        let partition_limit = policy.partition_limit(self.config.request_max_partitions);
        if policy.prepares_open()
            && partition_limit > 1
            && self.scheduler.ready.route_len((broker, lane)) > 1
            && self.batches.get(head).expect("dispatch head").state() == BatchState::Sealed
            && !self
                .batches
                .get(head)
                .expect("dispatch head")
                .gather_attempted
        {
            self.batches
                .get_mut(head)
                .expect("dispatch head")
                .gather_attempted = true;
            if self.scheduler_prepare_neighbors(head, now, budget, progress)? {
                self.scheduler.wait(partition, Group::EncodingPass);
                // The actor's encoder pass precedes scheduling. Signal the
                // newly queued finish work once, even if the scheduler itself
                // now has only passive waiters and no other I/O can wake us.
                progress.remaining_immediate = true;
                return Ok(0);
            }
        }
        let mut selected = Vec::new();
        let mut charges = Vec::new();
        let maximum = partition_limit.min((budget.items - progress.items + 1) as usize);
        selected
            .try_reserve_exact(maximum)
            .map_err(|_| EngineError::AllocationFailed)?;
        charges
            .try_reserve_exact(maximum)
            .map_err(|_| EngineError::AllocationFailed)?;
        selected.push(head);
        charges.push((partition, first.bytes));
        measure.commit(first);
        let visits = self
            .scheduler
            .ready
            .route_len((broker, lane))
            .min((budget.items - progress.items) as usize);
        for _ in 0..visits {
            if selected.len() == partition_limit
                || measure.bytes() >= self.config.request_target_bytes as usize
            {
                break;
            }
            let Some(candidate) = self.scheduler.ready.gather_after((broker, lane), partition)
            else {
                break;
            };
            progress.items += 1;
            if candidate == partition
                || self.destination(candidate) != Some((broker, lane))
                || self
                    .partitions
                    .get(&candidate)
                    .is_none_or(|queue| queue.retry_at > now)
            {
                if candidate != partition {
                    self.scheduler_mark(candidate);
                }
                continue;
            }
            let Some(key) = self.dispatch_head(candidate, now) else {
                self.scheduler_mark(candidate);
                continue;
            };
            if self.attempt_counts.get(&key.packed()).copied().unwrap_or(0)
                >= u32::from(self.config.max_attempts)
            {
                self.expire_batch(key, FailureReason::BrokerRejected);
                self.scheduler_mark(candidate);
                if self.failed.is_some() {
                    return Ok(0);
                }
                continue;
            }
            let addition = self.scheduler_measure(&measure, key)?;
            let charge = addition.bytes - measure.bytes();
            // Gathering earns only this partition's quantum. The enclosing
            // lane was credited once at its primary visit; candidate count
            // cannot multiply that lane's share of transmitted bytes.
            let queue = self
                .partitions
                .get_mut(&candidate)
                .expect("candidate partition");
            queue.deficit = queue.deficit.saturating_add(quantum).min(cap);
            if queue.deficit < charge || addition.bytes > self.scheduler.ready.lane_deficit(lane) {
                continue;
            }
            if addition.bytes > cap
                || addition.bytes > ((budget.bytes - progress.bytes) as usize).max(first.bytes)
                || addition.segments > addition.partitions * 3 + 4
            {
                break;
            }
            selected.push(key);
            charges.push((candidate, charge));
            measure.commit(addition);
        }
        let bytes = measure.bytes();
        self.commit_request(now, broker, lane, connection, bytes, selected)?;
        self.scheduler.ready.charge(lane, bytes);
        for (partition, charge) in charges {
            let queue = self
                .partitions
                .get_mut(&partition)
                .expect("admitted batch retains its partition");
            queue.deficit = queue
                .deficit
                .checked_sub(charge)
                .expect("every selected wire charge is reserved");
        }
        Ok(bytes)
    }

    fn scheduler_measure(
        &self,
        measure: &RequestSize,
        key: BatchKey,
    ) -> Result<dispatch_size::Addition> {
        let batch = self
            .batches
            .get(key)
            .ok_or(EngineError::InvalidState("candidate vanished"))?;
        measure
            .preview(
                batch.partition().topic,
                batch
                    .wire_bytes()
                    .ok_or(EngineError::InvalidState("candidate lacks wire bytes"))?,
                batch
                    .chunk_count()
                    .ok_or(EngineError::InvalidState("candidate lacks output spans"))?,
            )
            .ok_or(EngineError::AllocationFailed)
    }
}

#[cfg(test)]
mod tests;
