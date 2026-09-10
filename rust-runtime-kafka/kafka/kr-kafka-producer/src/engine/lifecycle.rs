use super::*;

impl ProducerEngine {
    pub fn flush(&mut self, now: RuntimeInstant, watermark: RecordToken) -> Result<FlushToken> {
        if self.close.is_some() || self.closed {
            return Err(EngineError::Closed);
        }
        let token = FlushToken(self.next_flush);
        let credit = self.control_credit()?;
        self.flush_reserved(token, watermark, now, credit)?;
        Ok(token)
    }
    /// Installs a fence backed by this engine's control-event authority.
    /// # Errors
    /// Foreign guards are released to their source without consuming a flush
    /// token, installing a fence or publishing an event in this engine.
    pub fn flush_reserved(
        &mut self,
        token: FlushToken,
        watermark: RecordToken,
        now: RuntimeInstant,
        credit: HeldCredits,
    ) -> Result<()> {
        if !credit.belongs_to(&self.credits) {
            return Err(CreditError::ForeignCredit.into());
        }
        self.observe_metrics_time(now);
        if credit.amount(Resource::ControlEvents) != 1 || token.0 < self.next_flush {
            return Err(EngineError::InvalidState("invalid reserved flush"));
        }
        self.next_flush = token
            .0
            .checked_add(1)
            .ok_or(EngineError::InvalidState("flush token exhausted"))?;
        if self
            .close
            .as_ref()
            .is_some_and(|close| watermark > close.watermark)
            || self.closed
        {
            return Err(EngineError::Closed);
        }
        self.flushes.push_back(FlushFence {
            token,
            watermark,
            credit,
        });
        self.seal_through(watermark, SealReason::Flush, now);
        self.complete_fences();
        Ok(())
    }
    pub fn close(
        &mut self,
        now: RuntimeInstant,
        deadline: RuntimeInstant,
        watermark: RecordToken,
    ) -> Result<()> {
        self.observe_metrics_time(now);
        if self.close.is_some() || self.closed {
            return Err(EngineError::Closed);
        }
        self.close = Some(CloseFence {
            deadline,
            watermark,
        });
        self.output.request_cache_clear();
        self.deadlines.set(DeadlineKey::Close, deadline.max(now));
        self.seal_through(watermark, SealReason::Flush, now);
        self.complete_fences();
        Ok(())
    }
    pub fn fail_producer(&mut self, reason: FailureReason) {
        self.fail_all(reason);
    }
    /// Explicit terminal time for passive callers without an actor poll context.
    pub fn fail_producer_at(&mut self, now: RuntimeInstant, reason: FailureReason) {
        self.observe_metrics_time(now);
        self.fail_producer(reason);
    }
    pub(super) fn seal_through(
        &mut self,
        watermark: RecordToken,
        reason: SealReason,
        now: RuntimeInstant,
    ) {
        let watermark = self
            .seal_sweep
            .as_ref()
            .map_or(watermark, |old| old.watermark.max(watermark));
        self.seal_sweep = Some(SealSweep {
            cursor: 0,
            end: self.batches.allocated_slots(),
            watermark,
            reason,
        });
        let _ = now;
    }
    pub(super) fn seal_step(&mut self, now: RuntimeInstant) -> bool {
        let Some(sweep) = &mut self.seal_sweep else {
            return false;
        };
        if sweep.cursor >= sweep.end {
            self.seal_sweep = None;
            return false;
        }
        let index = sweep.cursor;
        sweep.cursor += 1;
        let watermark = sweep.watermark;
        let reason = sweep.reason;
        if let Some(key) = self.batches.key_at(index) {
            if let Some(batch) = self.batches.get_mut(key)
                && batch
                    .records
                    .first()
                    .is_some_and(|record| record.token <= watermark)
            {
                batch.seal_at(reason, now);
            }
            self.refresh_batch_deadline(key, now);
            self.metrics_batch(key);
        }
        if self
            .seal_sweep
            .as_ref()
            .is_some_and(|sweep| sweep.cursor == sweep.end)
        {
            self.seal_sweep = None;
        }
        true
    }
    pub(super) fn complete_fences(&mut self) {
        self.fence_work = self
            .flushes
            .front()
            .is_some_and(|flush| self.tracker.reached(flush.watermark))
            || (!self.closed
                && self
                    .close
                    .as_ref()
                    .is_some_and(|close| self.tracker.reached(close.watermark))
                && (!self.close_retire_done || self.is_quiescent()));
    }
    pub(super) fn fence_step(&mut self) -> bool {
        if !self.fence_work {
            return false;
        }
        if self
            .flushes
            .front()
            .is_some_and(|flush| self.tracker.reached(flush.watermark))
        {
            let flush = self.flushes.pop_front().expect("ready flush head");
            self.event(Event::FlushDone { token: flush.token }, flush.credit);
        } else if !self.close_retire_done {
            if self.close_retire_cursor < self.connections.allocated_slots() {
                let index = self.close_retire_cursor;
                self.close_retire_cursor += 1;
                if let Some(key) = self.connections.key_at(index) {
                    self.retire_connection(ConnectionKey(key.packed()), RetireReason::Requested);
                }
            } else {
                self.close_retire_done = true;
            }
        } else if self.is_quiescent() {
            // Releasing maps, queued plans, and reserved events is real owner
            // work too. Remove one obligation per item instead of bulk clear.
            if let Some((&key, _)) = self.deadlines.current.first_key_value() {
                self.deadlines.remove(key);
            } else if self.metadata_pending.pop_first().is_some() || self.pop_order().is_some() {
            } else if let Some((topic, credit)) = self.topic_event_credits.pop_first() {
                self.event(
                    Event::TopicFailed {
                        topic,
                        code: FailureReason::Closed as u32,
                    },
                    credit,
                );
            } else {
                self.fatal_credit = None;
                self.identity_pending = false;
                self.seal_sweep = None;
                self.closed = true;
                if let Some(credit) = self.closed_credit.take() {
                    self.event(
                        Event::Closed {
                            unresolved: self.unknown.min(u64::from(u32::MAX)) as u32,
                        },
                        credit,
                    );
                }
            }
        }
        self.complete_fences();
        true
    }
    pub(super) fn fail_record(&mut self, record: AdmittedRecord, reason: FailureReason) {
        let partition = self
            .topic_records
            .get(&record.topic)
            .and_then(|owner| {
                owner
                    .records
                    .get(&record.token)
                    .copied()
                    .flatten()
                    .or_else(|| {
                        owner.captured_id.map(|topic| TopicPartition {
                            topic,
                            partition: record.partition_hint.unwrap_or(-1),
                        })
                    })
            })
            .unwrap_or_else(|| TopicPartition {
                topic: self
                    .topics
                    .get(record.topic)
                    .ok()
                    .and_then(|topic| topic.id)
                    .unwrap_or(TopicId::ZERO),
                partition: record.partition_hint.unwrap_or(-1),
            });
        self.deadlines.remove(DeadlineKey::Pending(record.token));
        let queued_partition = self.queued_locations.remove(&record.token);
        let (payload, obligation) = record.into_parts();
        drop(payload);
        if let Some(partition) = queued_partition {
            self.encoder_refresh_append(partition);
            self.scheduler_mark(partition);
        }
        self.deliver(
            obligation,
            partition,
            DeliveryOutcome::not_written(reason),
            None,
            None,
            0,
        );
    }
    fn deliver(
        &mut self,
        mut record: RecordObligation,
        partition: TopicPartition,
        outcome: DeliveryOutcome,
        offset: Option<i64>,
        timestamp: Option<i64>,
        attempts: u32,
    ) {
        // Topic deletion is the concrete cause for every still-undelivered
        // never-written record under that fixed ID, even when one ambiguous
        // batch also trips the producer-wide sequence failure barrier.
        let outcome = if outcome.kind == DeliveryKind::NotWritten
            && (self
                .topic_records
                .get(&record.topic)
                .is_some_and(|owner| owner.reason == Some(FailureReason::TopicDeleted))
                || self
                    .topics
                    .get(record.topic)
                    .is_ok_and(|topic| topic.state == TopicState::Deleted))
        {
            DeliveryOutcome::not_written(FailureReason::TopicDeleted)
        } else {
            outcome
        };
        self.metrics_delivery(partition, record.accepted_at, outcome.kind);
        self.batched_locations.remove(&record.token);
        let event = DeliveryEvent {
            token: record.token,
            user_token: record.user_token,
            topic: record.topic,
            partition,
            outcome,
            base_offset: offset.into(),
            timestamp: timestamp.into(),
            attempts,
        };
        // Terminal bytes no longer belong to the encoder or transport. Return
        // input ownership now even when an earlier partition delivery blocks
        // publication; the retained descriptor/event guards bound this gate.
        record.input_consumed();
        self.terminal_order
            .route(record.topic, record.token, partition);
        if self.terminal_order.ready(&event) {
            self.publish_delivery(record, event);
        } else {
            self.terminal_order.hold(record, event);
        }
    }
    pub(super) fn publish_delivery(&mut self, record: RecordObligation, event: DeliveryEvent) {
        self.terminal_order.release(&event);
        self.tracker
            .terminal(record.token)
            .expect("terminal ownership corresponds to one accepted record");
        self.finish_topic_record(record.topic, record.token);
        self.partition_cleanup_released(event.partition);
        if event.outcome.kind == DeliveryKind::Unknown {
            self.unknown = self
                .unknown
                .checked_add(1)
                .expect("unknown count bounded by the u64 admission token space");
        }
        self.event(Event::Delivery(event), record.terminal());
    }
    pub(super) fn finish_unassigned(&mut self, key: BatchKey, reason: FailureReason) {
        self.metrics_batch(key);
        self.abort_seal_timing();
        let Some(batch) = self.batches.get(key) else {
            return;
        };
        let terminal = TerminalBatch {
            assignment: crate::sequence::Assignment {
                partition: batch.partition(),
                batch: key.packed(),
                identity: self
                    .ledger
                    .as_ref()
                    .map(ProducerLedger::identity)
                    .unwrap_or(ProducerIdentity {
                        producer_id: -1,
                        epoch: -1,
                    }),
                base_sequence: Sequence::ZERO,
                record_count: batch.record_count() as u32,
            },
            outcome: DeliveryOutcome::not_written(reason),
            base_offset: None,
            timestamp: None,
            attempts: 0,
            transmitted: false,
        };
        self.finish_batch(terminal);
    }
    pub(super) fn finish_batch(&mut self, terminal: TerminalBatch) {
        let key = Slot::from_packed(terminal.assignment.batch);
        self.metrics_batch(key);
        self.encoder_remove_batch(key);
        let Ok(batch) = self.batches.remove(key) else {
            return;
        };
        self.attempt_counts.remove(&key.packed());
        self.deadlines.remove(DeadlineKey::Batch(key.packed()));
        if let Some(partition) = self.partitions.get_mut(&terminal.assignment.partition) {
            partition.batches.remove(key);
            partition.batch_bytes -= u64::from(batch.raw_bytes());
            partition.terminal_owners += 1;
            if let Some(at) = batch.first_accepted() {
                partition.batch_ages.remove(&(at, key));
            }
        }
        let (records, payload) = batch.into_terminal_staged();
        self.terminal_records.push_back(TerminalRecords {
            payload: Some(payload),
            terminal,
            records: records.into(),
            index: 0,
        });
        self.encoder_refresh_append(terminal.assignment.partition);
        self.scheduler_mark(terminal.assignment.partition);
    }
    pub(super) fn drain_terminal_records(&mut self, maximum: u32) -> u32 {
        let mut drained = 0;
        while drained < maximum {
            if self.ordered_delivery_step() {
                drained += 1;
                continue;
            }
            let Some(batch) = self.terminal_records.front_mut() else {
                break;
            };
            if let Some(payload) = &mut batch.payload {
                let progress = payload.abort_step((maximum - drained) as usize);
                drained += (progress.records_released + progress.chunks_released).max(1) as u32;
                if progress.done {
                    batch.payload = None;
                }
                continue;
            }
            let Some(record) = batch.records.pop_front() else {
                self.retire_terminal_records();
                continue;
            };
            let terminal = batch.terminal;
            let index = batch.index;
            batch.index += 1;
            let offset = terminal
                .base_offset
                .and_then(|offset| offset.checked_add(i64::from(index)));
            self.deliver(
                record,
                terminal.assignment.partition,
                terminal.outcome,
                offset,
                terminal.timestamp,
                terminal.attempts,
            );
            drained += 1;
            if self
                .terminal_records
                .front()
                .is_some_and(|batch| batch.records.is_empty())
            {
                self.retire_terminal_records();
            }
        }
        self.complete_fences();
        drained
    }
    fn retire_terminal_records(&mut self) {
        let terminal = self.terminal_records.pop_front().expect("terminal owner");
        debug_assert!(terminal.records.is_empty() && terminal.payload.is_none());
        let key = terminal.terminal.assignment.partition;
        let queue = self
            .partitions
            .get_mut(&key)
            .expect("terminal retains partition");
        queue.terminal_owners = queue
            .terminal_owners
            .checked_sub(1)
            .expect("one terminal owner per batch");
        self.partition_cleanup_released(key);
    }
    pub(super) fn apply_change(&mut self, change: LedgerChange) {
        for terminal in change.terminal {
            self.finish_batch(terminal);
        }
        if change.recovery == RecoveryState::FailedClosed && self.failed.is_none() {
            let reason = self
                .ledger
                .as_ref()
                .and_then(ProducerLedger::failure_reason)
                .expect("failed ledger retains its fatal reason");
            self.fail_all(reason);
        }
    }
    pub(super) fn expire_batch(&mut self, key: BatchKey, reason: FailureReason) {
        // A finalized frame may be queued but never provider-admitted. Quarantine
        // its entire connection before publishing NotWritten so it cannot start
        // writing a record after that terminal delivery.
        if let Some(request) = self
            .batch_requests
            .get(&key)
            .and_then(|key| self.requests.get(Slot::from_packed(key.0)))
        {
            self.retire_connection(request.connection, RetireReason::Requested);
        }
        let Some(batch) = self.batches.get(key) else {
            return;
        };
        let partition = batch.partition();
        if self
            .ledger
            .as_ref()
            .is_some_and(|ledger| ledger.assignment(partition, key.packed()).is_ok())
        {
            if let Some(ledger) = &mut self.ledger
                && let Ok(change) = ledger.expire_deferred(partition, key.packed(), reason)
            {
                self.apply_change(change);
            }
        } else {
            self.finish_unassigned(key, reason);
        }
    }
    pub(super) fn fail_all(&mut self, reason: FailureReason) {
        self.abort_seal_timing();
        self.headroom_sweep = None;
        if self.failed.is_some() {
            return;
        }
        self.failed = Some(reason);
        self.output.request_cache_clear();
        self.failure_work = true;
        self.failure_batch_cursor = 0;
        self.failure_connection_cursor = 0;
        if let Some(ledger) = &mut self.ledger {
            ledger.begin_failure(reason);
        }
        if let Some(credit) = self.fatal_credit.take() {
            self.event(
                Event::Fatal {
                    code: reason as u32,
                },
                credit,
            );
        }
        self.identity_pending = false;
        self.identity_refresh = None;
    }
    pub(super) fn failure_step(&mut self) -> bool {
        if !self.failure_work {
            return false;
        }
        let reason = self.failed.expect("failure work has a reason");
        if let Some(ledger) = &mut self.ledger
            && ledger.has_failed_entries()
        {
            let terminal = ledger.drain_failed(1).pop().expect("failed ledger entry");
            self.finish_batch(terminal);
            return true;
        }
        if let Some((&token, _)) = self.pending.first_key_value() {
            let record = self.take_pending(token).expect("indexed pending record");
            self.fail_record(record, reason);
            return true;
        }
        if let Some((&token, &partition)) = self.queued_locations.first_key_value() {
            let record = self
                .partitions
                .get_mut(&partition)
                .expect("indexed queue")
                .records
                .remove_token(token)
                .expect("indexed record");
            self.fail_record(record, reason);
            return true;
        }
        if self.failure_batch_cursor < self.batches.allocated_slots() {
            let index = self.failure_batch_cursor;
            self.failure_batch_cursor += 1;
            if let Some(key) = self.batches.key_at(index) {
                self.finish_unassigned(key, reason);
            }
            return true;
        }
        if self.failure_connection_cursor < self.connections.allocated_slots() {
            let index = self.failure_connection_cursor;
            self.failure_connection_cursor += 1;
            if let Some(key) = self.connections.key_at(index) {
                self.retire_connection(ConnectionKey(key.packed()), RetireReason::Requested);
            }
            return true;
        }
        if self.metadata_pending.pop_first().is_some() {
            return true;
        }
        self.failure_work = false;
        true
    }
}
