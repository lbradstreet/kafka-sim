use super::*;

impl ProducerEngine {
    pub(super) fn commit_request(
        &mut self,
        now: RuntimeInstant,
        broker: i32,
        lane: u8,
        connection: ConnectionKey,
        bytes: usize,
        selected: Vec<BatchKey>,
    ) -> Result<usize> {
        let mut credit = self.credits.reserve(&[
            Claim {
                resource: Resource::RequestSlots,
                amount: 1,
                lane,
            },
            Claim {
                resource: Resource::WireWindow,
                amount: bytes,
                lane,
            },
            Claim {
                resource: Resource::RequestMetadata,
                amount: self.validated.request_metadata_bytes,
                lane,
            },
        ])?;
        for &key in &selected {
            let batch = self
                .batches
                .get(key)
                .ok_or(EngineError::InvalidState("selected batch disappeared"))?;
            if batch.state() == BatchState::Sealed {
                let ledger = self.ledger.as_mut().expect("identity checked");
                ledger.register(batch.partition())?;
                let assignment =
                    ledger.assign(batch.partition(), key.packed(), batch.record_count() as u32)?;
                self.batches
                    .get_mut(key)
                    .expect("selected batch")
                    .finalize(kr_kafka_record::Identity {
                        producer_id: assignment.identity.producer_id,
                        producer_epoch: assignment.identity.epoch,
                        base_sequence: assignment.base_sequence.get(),
                    })?;
            }
        }
        let correlation = self.next_correlation;
        self.next_correlation = correlation.checked_add(1).ok_or(EngineError::InvalidState(
            "correlation identifier exhausted",
        ))?;
        let mut plan = self.build_plan(&selected, correlation)?;
        if plan.len() != bytes {
            return Err(EngineError::InvalidState(
                "request measure and generated plan disagree",
            ));
        }
        let retained = plan
            .metadata_bytes()
            .checked_add(plan.coalesced_bytes())
            .ok_or(EngineError::AllocationFailed)?;
        credit.shrink(Resource::RequestMetadata, retained)?;
        plan.retain_metadata_guard(Arc::new(credit.take(Resource::RequestMetadata)));
        let deadline = selected
            .iter()
            .filter_map(|key| self.batches.get(*key).and_then(Batch::oldest_deadline))
            .fold(
                Self::deadline_after(now, self.config.request_timeout),
                RuntimeInstant::min,
            );
        let attempt = self.next_attempt;
        self.next_attempt = attempt
            .checked_add(1)
            .ok_or(EngineError::InvalidState("attempt identifier exhausted"))?;
        let mut request_partitions = Vec::new();
        request_partitions
            .try_reserve_exact(selected.len())
            .map_err(|_| EngineError::AllocationFailed)?;
        request_partitions.extend(
            selected
                .iter()
                .map(|key| self.batches.get(*key).expect("selected batch").partition()),
        );
        let slot = self
            .requests
            .insert(RequestState {
                connection,
                correlation,
                batches: selected.clone(),
                partitions: request_partitions,
                deadline,
                bytes,
                confirmed: 0,
                sent_at: None,
                admitted: false,
                certainty: CompletionCertainty::NotApplied,
                attempt,
                initial_write_unresolved: false,
                _credit: credit,
            })
            .map_err(|failure| EngineError::Pool(failure.error))?;
        let request = RequestKey(slot.packed());
        for key in &selected {
            let partition = self.batches.get(*key).expect("selected batch").partition();
            self.partitions
                .get_mut(&partition)
                .expect("registered partition")
                .request_owners += 1;
            self.batch_requests.insert(*key, request);
        }
        let connection_state = self
            .connections
            .get_mut(Slot::from_packed(connection.0))
            .ok_or(EngineError::StaleConnection)?;
        connection_state.fifo.push_back(request);
        connection_state.writing = Some(request);
        connection_state.bytes += bytes;
        let broker_state = self.brokers.get_mut(&broker).expect("connected broker");
        broker_state.bytes += bytes;
        broker_state.requests += 1;
        self.metrics_request_depth(Some(broker), bytes, true);
        for key in selected {
            let batch = self.batches.get_mut(key).expect("selected batch");
            batch.in_flight();
            self.refresh_batch_deadline(key, now);
        }
        self.deadlines.set(DeadlineKey::Request(request), deadline);
        self.order(EngineOrder::Dispatch {
            connection,
            request,
            correlation,
            plan,
            deadline,
        })?;
        Ok(bytes)
    }
    pub(super) fn dispatch_head(
        &self,
        partition: TopicPartition,
        now: RuntimeInstant,
    ) -> Option<BatchKey> {
        self.dispatch_candidate(partition, now, false)
    }
    pub(super) fn dispatch_candidate(
        &self,
        partition: TopicPartition,
        now: RuntimeInstant,
        gather_open: bool,
    ) -> Option<BatchKey> {
        let queue = self.partitions.get(&partition)?;
        let &key = queue
            .batches
            .iter()
            .take(usize::from(self.config.max_in_flight_per_connection) + 1)
            .find(|&&key| {
                self.batches
                    .get(key)
                    .is_some_and(|batch| batch.state() != BatchState::InFlight)
            })?;
        let batch = self.batches.get(key)?;
        if batch
            .records
            .first()
            .is_some_and(|record| self.topic_is_settling(record.topic))
        {
            return None;
        }
        if now < batch.dispatch_ready_at()
            || batch
                .oldest_deadline()
                .is_some_and(|deadline| now >= deadline)
            || !(matches!(batch.state(), BatchState::Sealed | BatchState::Ready)
                || (gather_open && batch.can_gather_open()))
        {
            return None;
        }
        let ledger = self.ledger.as_ref()?;
        if batch.state() == BatchState::Ready
            && !ledger.attempt_order_ready(partition, key.packed()).ok()?
        {
            return None;
        }
        if batch.state() != BatchState::Ready {
            if ledger.recovery_state() != RecoveryState::Active
                || ledger
                    .unresolved(partition)
                    .or_else(|error| match error {
                        LedgerError::UnknownPartition => Ok(0),
                        other => Err(other),
                    })
                    .ok()?
                    >= self.config.max_in_flight_per_connection as usize
            {
                return None;
            }
        } else if ledger.recovery_state() == RecoveryState::NeedsIdentity
            && !ledger.transmitted(partition, key.packed()).ok()?
        {
            let later_transmitted = ledger
                .later_requires_old_identity(partition, key.packed())
                .ok()?;
            if !later_transmitted {
                return None;
            }
        } else if matches!(
            ledger.recovery_state(),
            RecoveryState::RefreshingIdentity | RecoveryState::FailedClosed
        ) {
            return None;
        }
        Some(key)
    }
    pub(super) fn destination(&self, partition: TopicPartition) -> Option<(i32, u8)> {
        let topic = self.topics.get(self.topics.by_id(partition.topic)?).ok()?;
        if topic.state != TopicState::Ready {
            return None;
        }
        let metadata = topic.partitions.get(partition.partition as usize)?;
        if metadata.leader < 0 {
            return None;
        }
        Some((metadata.leader, self.partitions.get(&partition)?.lane))
    }
    pub(super) fn connection_credit(&self, key: ConnectionKey, now: RuntimeInstant) -> bool {
        let Some(connection) = self.connections.get(Slot::from_packed(key.0)) else {
            return false;
        };
        !self.retry_pending(connection.broker, connection.lane)
            && connection.phase == ConnectionPhase::Active
            && connection.writing.is_none()
            && connection.fifo.len() < self.config.max_in_flight_per_connection as usize
            && self.brokers.get(&connection.broker).is_some_and(|broker| {
                broker.throttle_until <= now
                    && broker.bytes < self.config.connection_wire_window_bytes as usize
            })
    }
    pub(super) fn has_dispatch_credit(
        &self,
        partition: TopicPartition,
        now: RuntimeInstant,
    ) -> bool {
        self.destination(partition)
            .and_then(|route| self.routes.get(&route))
            .is_some_and(|key| self.connection_credit(*key, now))
    }
    pub(super) fn connect(&mut self, broker: i32, lane: u8) -> Result<()> {
        let node = self
            .brokers
            .get(&broker)
            .ok_or(EngineError::InvalidState("unknown broker"))?
            .node
            .clone();
        let mut claims = vec![
            Claim {
                resource: Resource::RxBytes,
                amount: self.config.rx_bytes_per_connection as usize,
                lane,
            },
            Claim {
                resource: Resource::StagingBytes,
                amount: self.config.staging_bytes_per_connection as usize,
                lane,
            },
        ];
        if !matches!(
            self.config.security,
            crate::config::SecurityConfig::Plaintext
        ) {
            claims.push(Claim {
                resource: Resource::TlsBytes,
                amount: self.config.tls_plaintext_bytes as usize
                    + self.config.tls_ciphertext_bytes as usize,
                lane,
            });
        }
        let credit = self.credits.reserve(&claims)?;
        let mut fifo = VecDeque::new();
        fifo.try_reserve_exact(self.config.max_in_flight_per_connection as usize)
            .map_err(|_| EngineError::AllocationFailed)?;
        let credit = Arc::new(credit);
        let lifetime_guard = credit.clone();
        let key = ConnectionKey(
            self.connections
                .insert(ConnectionState {
                    retry_scheduled: false,
                    broker,
                    lane,
                    phase: ConnectionPhase::Connecting,
                    fifo,
                    pending_orders: [None; 3],
                    writing: None,
                    bytes: 0,
                    credit,
                })
                .map_err(|failure| EngineError::Pool(failure.error))?
                .packed(),
        );
        self.routes.insert((broker, lane), key);
        self.order(EngineOrder::Connect {
            key,
            node,
            lane,
            lifetime_guard,
        })
    }
    fn measure_request(&self, keys: &[BatchKey]) -> Result<usize> {
        let mut measure = dispatch_size::RequestSize::new(self.config.client_id.len())
            .ok_or(EngineError::AllocationFailed)?;
        for &key in keys {
            let batch = self
                .batches
                .get(key)
                .ok_or(EngineError::InvalidState("selected batch disappeared"))?;
            let addition = measure
                .preview(
                    batch.partition().topic,
                    batch
                        .wire_bytes()
                        .ok_or(EngineError::InvalidState("selected batch has no bytes"))?,
                    batch
                        .chunk_count()
                        .ok_or(EngineError::InvalidState("selected batch has no chunks"))?,
                )
                .ok_or(EngineError::AllocationFailed)?;
            measure.commit(addition);
        }
        Ok(measure.bytes())
    }
    fn build_plan(&self, keys: &[BatchKey], correlation: i32) -> Result<OwnedSendPlan> {
        use kr_kafka_protocol::produce_request::{
            self,
            v13::{PartitionProduceData, ProduceRequest, TopicProduceData},
        };
        let mut chunks: BTreeMap<TopicId, Vec<(i32, Vec<kr_shared_bytes::SharedBytes>)>> =
            BTreeMap::new();
        for &key in keys {
            let batch = self.batches.get(key).expect("selected batch");
            let retained = batch.retained_chunks()?;
            if !(2..=3).contains(&retained.len())
                || retained
                    .first()
                    .is_none_or(|header| header.len() != kr_kafka_record::BATCH_HEADER_BYTES)
            {
                return Err(EngineError::InvalidState(
                    "record output must contain one header and at most two payload chunks",
                ));
            }
            chunks
                .entry(batch.partition().topic)
                .or_default()
                .push((batch.partition().partition, retained));
        }
        let partitions: Vec<_> = chunks
            .iter()
            .map(|(topic, parts)| {
                (
                    *topic,
                    parts
                        .iter()
                        .map(|(index, records)| PartitionProduceData {
                            index: *index,
                            records: Some(Records::HeaderAndChunks {
                                header: records[0].as_slice(),
                                chunks: &records[1..],
                            }),
                            ..Default::default()
                        })
                        .collect::<Vec<_>>(),
                )
            })
            .collect();
        let topics: Vec<_> = partitions
            .iter()
            .map(|(id, parts)| TopicProduceData {
                topic_id: id.0,
                partition_data: WireSequence::from_slice(parts),
                ..Default::default()
            })
            .collect();
        let request = kr_kafka_protocol::Request::ProduceRequest(produce_request::View::V13(
            ProduceRequest {
                transactional_id: None,
                acks: -1,
                timeout_ms: (self.config.request_timeout.as_nanos() / 1_000_000) as i32,
                topic_data: WireSequence::from_slice(&topics),
                ..Default::default()
            },
        ));
        let record_bytes = keys
            .iter()
            .try_fold(0usize, |sum, key| {
                sum.checked_add(self.batches.get(*key)?.wire_bytes()?)
            })
            .ok_or(EngineError::InvalidState("record length overflow"))?;
        let metadata_bytes = self
            .measure_request(keys)?
            .checked_sub(record_bytes)
            .and_then(|bytes| bytes.checked_add(keys.len() * kr_kafka_record::BATCH_HEADER_BYTES))
            .filter(|bytes| *bytes <= self.validated.request_metadata_bytes)
            .ok_or(EngineError::InvalidState("request metadata bound"))?;
        let protocol = request
            .plan_frame(
                13,
                correlation,
                Some(&self.config.client_id),
                EncodeLimits {
                    max_bytes: self.config.request_hard_bytes as usize,
                    max_metadata_bytes: metadata_bytes,
                    max_segments: keys.len() * 3 + 4,
                    max_array_elements: self.config.request_max_partitions as usize * 2,
                    ..EncodeLimits::default()
                },
            )
            .map_err(|error| EngineError::Transport(error.into()))?;
        let protocol = protocol
            .try_into_owned()
            .map_err(|_| EngineError::InvalidState("producer plan borrowed record bytes"))?;
        let mut plan = OwnedSendPlan::from_protocol(
            protocol,
            PlanLimits {
                max_bytes: self.config.request_hard_bytes as usize,
                max_segments: keys.len() * 3 + 4,
                max_coalesced_bytes: self.validated.request_metadata_bytes,
                coalesce_below_bytes: self.config.coalesce_below_bytes as usize,
            },
        )?;
        let mut fences = Vec::new();
        fences
            .try_reserve_exact(keys.len())
            .map_err(|_| EngineError::AllocationFailed)?;
        for key in keys {
            let handle = self
                .batches
                .get(*key)
                .and_then(|batch| batch.records.first())
                .ok_or(EngineError::InvalidState(
                    "planned batch has no topic owner",
                ))?
                .topic;
            let owner = self
                .topic_records
                .get(&handle)
                .ok_or(EngineError::InvalidState(
                    "planned topic has no accepted records",
                ))?;
            fences.push(owner.write_fence.clone());
        }
        plan.retain_write_fences(fences)
            .map_err(|_| EngineError::InvalidState("request plan already fenced"))?;
        Ok(plan)
    }

    pub(crate) fn connection_is_retiring(&self, key: ConnectionKey) -> bool {
        self.connections
            .get(Slot::from_packed(key.0))
            .is_some_and(|connection| connection.phase == ConnectionPhase::Retiring)
    }

    pub fn on_write_admitted(&mut self, key: RequestKey) -> Result<()> {
        let request = self
            .requests
            .get_mut(Slot::from_packed(key.0))
            .ok_or(EngineError::StaleRequest)?;
        if request.admitted {
            return Ok(());
        }
        if self
            .connections
            .get(Slot::from_packed(request.connection.0))
            .is_none_or(|connection| connection.phase != ConnectionPhase::Active)
            || request
                .batches
                .iter()
                .any(|key| self.batches.get(*key).is_none())
        {
            return Err(EngineError::InvalidState(
                "write admission after connection quarantine",
            ));
        }
        let ledger = self
            .ledger
            .as_mut()
            .ok_or(EngineError::InvalidState("request without identity"))?;
        for &batch_key in &request.batches {
            if let Some(batch) = self.batches.get(batch_key) {
                ledger.start_attempt(batch.partition(), batch_key.packed(), request.attempt)?;
                *self.attempt_counts.entry(batch_key.packed()).or_default() += 1;
            }
        }
        request.admitted = true;
        request.initial_write_unresolved = true;
        Ok(())
    }
    pub fn on_write(
        &mut self,
        connection: ConnectionKey,
        correlation: i32,
        confirmed: usize,
        certainty: CompletionCertainty,
    ) -> Result<()> {
        let key = self
            .request_for(connection, correlation)
            .ok_or(EngineError::StaleRequest)?;
        let request = self
            .requests
            .get_mut(Slot::from_packed(key.0))
            .ok_or(EngineError::StaleRequest)?;
        if !request.admitted || confirmed < request.confirmed || confirmed > request.bytes {
            return Err(EngineError::InvalidState("invalid write progress"));
        }
        request.confirmed = confirmed;
        request.initial_write_unresolved = false;
        request.certainty = match (request.certainty, certainty) {
            (CompletionCertainty::MayHaveApplied, _) | (_, CompletionCertainty::MayHaveApplied) => {
                CompletionCertainty::MayHaveApplied
            }
            (CompletionCertainty::Applied, _) | (_, CompletionCertainty::Applied) => {
                CompletionCertainty::Applied
            }
            _ => CompletionCertainty::NotApplied,
        };
        if certainty != CompletionCertainty::NotApplied || confirmed > 0 {
            for &batch_key in &request.batches {
                if let Some(batch) = self.batches.get_mut(batch_key) {
                    batch.mark_transmitted();
                    if let Some(ledger) = &mut self.ledger {
                        ledger.mark_transmitted(
                            batch.partition(),
                            batch_key.packed(),
                            request.attempt,
                        )?;
                    }
                }
            }
        }
        if confirmed == request.bytes
            && let Some(connection) = self.connections.get_mut(Slot::from_packed(connection.0))
        {
            connection.writing = None;
            let route = (connection.broker, connection.lane);
            self.encoder_dispatch_changed(route.0, route.1);
            self.scheduler_connection_changed(route.0, route.1);
        }
        Ok(())
    }
    /// Records full confirmed-send time for response-latency estimation. The
    /// original untimed API remains available to passive callers without a
    /// timing observation. Repeated/full later fragments cannot restart time.
    /// # Errors
    /// Returns the same stale-key and invalid-progress errors as `on_write`.
    pub fn on_write_at(
        &mut self,
        now: RuntimeInstant,
        connection: ConnectionKey,
        correlation: i32,
        confirmed: usize,
        certainty: CompletionCertainty,
    ) -> Result<()> {
        self.observe_metrics_time(now);
        self.on_write(connection, correlation, confirmed, certainty)?;
        let key = self
            .request_for(connection, correlation)
            .ok_or(EngineError::StaleRequest)?;
        let request = self
            .requests
            .get_mut(Slot::from_packed(key.0))
            .ok_or(EngineError::StaleRequest)?;
        if request.confirmed == request.bytes && request.sent_at.is_none() {
            request.sent_at = Some(now);
        }
        Ok(())
    }
    pub fn on_frame(
        &mut self,
        connection: ConnectionKey,
        now: RuntimeInstant,
        bytes: &[u8],
    ) -> Result<()> {
        self.observe_metrics_time(now);
        let state = self
            .connections
            .get(Slot::from_packed(connection.0))
            .ok_or(EngineError::StaleConnection)?;
        let key = *state.fifo.front().ok_or(EngineError::StaleRequest)?;
        let broker = state.broker;
        let request = self
            .requests
            .get(Slot::from_packed(key.0))
            .ok_or(EngineError::StaleRequest)?;
        let expected: Vec<_> = request
            .batches
            .iter()
            .filter_map(|key| self.batches.get(*key).map(Batch::partition))
            .collect();
        let update = match self
            .control
            .parse_produce13(bytes, request.correlation, &expected)
        {
            Ok(update) => update,
            Err(error) => {
                self.retry_connection(connection, now);
                self.retire_connection(connection, RetireReason::Requested);
                return Err(error.into());
            }
        };
        if !request.admitted {
            return Err(EngineError::InvalidState(
                "response for never-admitted request",
            ));
        }
        // The codec restores the exact expected order above. Reuse the same
        // generation-checked live-batch iterator, and finish every offset check
        // before removing the request or applying any successful response row.
        let offset_overflow = update
            .partitions
            .iter()
            .zip(
                request
                    .batches
                    .iter()
                    .filter_map(|key| self.batches.get(*key)),
            )
            .any(|(response, batch)| {
                debug_assert_eq!(response.partition, batch.partition());
                response.base_offset.is_some_and(|offset| {
                    offset
                        .checked_add(batch.record_count().saturating_sub(1) as i64)
                        .is_none()
                })
            });
        if offset_overflow {
            self.retire_connection(connection, RetireReason::Requested);
            return Err(EngineError::InvalidState("response offset overflow"));
        }
        // A rejected FIFO head may need to be replayed before a younger frame
        // already planned on this connection. Quarantine before the actor can
        // admit that younger write: ledger start_attempt cannot undo bytes that
        // escaped while an earlier batch was ready to retry.
        let replay_fence = update.partitions.iter().any(|partition| {
            !matches!(
                code::classify(partition.error_code),
                code::ErrorClass::Success | code::ErrorClass::DuplicateSequence
            )
        });
        let request = self.remove_request(key)?;
        self.metrics_request_rtt(broker, request.sent_at, now);
        if let Some(elapsed) = request
            .sent_at
            .and_then(|at| now.checked_duration_since(at))
            && let Some(state) = self.brokers.get_mut(&broker)
            && state.round_trip.observe(elapsed)
        {
            self.invalidate_headroom();
        }
        if replay_fence {
            self.retry_connection(connection, now);
            self.retire_connection(connection, RetireReason::Requested);
        }
        if update.throttle_ms > 0
            && let Some(broker) = self.brokers.get_mut(&broker)
        {
            broker.throttle_until = broker.throttle_until.max(Self::deadline_after(
                now,
                RuntimeDuration::from_nanos(u64::from(update.throttle_ms) * 1_000_000),
            ));
            self.deadlines
                .set(DeadlineKey::Broker(broker.node.id), broker.throttle_until);
        }
        // KIP-951 endpoint additions are bounded independently of a full metadata refresh.
        for node in update.brokers {
            if let Some(existing) = self.brokers.get_mut(&node.id) {
                existing.node = node;
            } else if self.brokers.len() < self.config.brokers_max as usize {
                self.brokers.insert(
                    node.id,
                    BrokerState {
                        metrics_scope: self.metrics.recorder.register_broker(node.id),
                        round_trip: crate::estimation::RoundTripTime::default(),
                        node,
                        bytes: 0,
                        requests: 0,
                        throttle_until: RuntimeInstant::ZERO,
                    },
                );
            }
        }
        for response in update.partitions {
            let Some(&key) = request.batches.iter().find(|&&key| {
                self.batches
                    .get(key)
                    .is_some_and(|batch| batch.partition() == response.partition)
            }) else {
                continue;
            };
            let class = code::classify(response.error_code);
            if matches!(
                class,
                code::ErrorClass::Success | code::ErrorClass::DuplicateSequence
            ) {
                let batch = self.batches.get(key).expect("response batch");
                let bytes = u64::from(batch.raw_bytes());
                let accepted = batch.first_accepted();
                if let Some(queue) = self.partitions.get_mut(&response.partition) {
                    if let Some(elapsed) = queue
                        .last_drain
                        .or(accepted)
                        .and_then(|at| now.checked_duration_since(at))
                        && elapsed.as_nanos() != 0
                    {
                        let sample = (u128::from(bytes) * 1_000_000_000
                            / u128::from(elapsed.as_nanos()))
                        .min(u128::from(u64::MAX)) as u64;
                        queue.drain_bytes_per_second = if queue.last_drain.is_none() {
                            sample
                        } else {
                            ((u128::from(queue.drain_bytes_per_second) * 7 + u128::from(sample))
                                / 8) as u64
                        };
                    }
                    queue.last_drain = Some(now);
                }
            }
            if let Some(leader) = response.current_leader
                && self.topics.update_leader(
                    response.partition.topic,
                    response.partition.partition,
                    leader,
                    now,
                ) == Ok(true)
            {
                self.retry_topology_changed();
            }
            let outcome = match class {
                code::ErrorClass::Success => BrokerOutcome::Success {
                    base_offset: response.base_offset,
                    timestamp: response.timestamp,
                },
                code::ErrorClass::DuplicateSequence => BrokerOutcome::Duplicate,
                code::ErrorClass::Retry
                | code::ErrorClass::RefreshMetadata
                | code::ErrorClass::RefreshTopicId => BrokerOutcome::Retry,
                code::ErrorClass::SequenceRecovery => BrokerOutcome::SequenceError,
                code::ErrorClass::DefinitiveNotWritten => {
                    let reason = match response.error_code {
                        code::MESSAGE_TOO_LARGE | code::RECORD_LIST_TOO_LARGE => {
                            FailureReason::CompressedTooLarge
                        }
                        _ => FailureReason::BrokerRejected,
                    };
                    BrokerOutcome::DefinitiveRejection(reason)
                }
                code::ErrorClass::TopicFatal => BrokerOutcome::Fatal(FailureReason::TopicDeleted),
                _ => BrokerOutcome::Fatal(FailureReason::ProducerFenced),
            };
            if matches!(
                class,
                code::ErrorClass::RefreshMetadata | code::ErrorClass::RefreshTopicId
            ) {
                self.refresh_partition_metadata(response.partition, now);
            }
            if let Some(ledger) = &mut self.ledger {
                let change = ledger.response_deferred(
                    response.partition,
                    key.packed(),
                    request.attempt,
                    outcome,
                )?;
                self.apply_change(change);
            }
            let sequence_retry = matches!(outcome, BrokerOutcome::SequenceError)
                && self.ledger.as_ref().is_some_and(|ledger| {
                    ledger
                        .sequence_retry_pending(response.partition, key.packed())
                        .unwrap_or(false)
                });
            if (matches!(outcome, BrokerOutcome::Retry) || sequence_retry)
                && self.batches.get(key).is_some()
            {
                self.batches.get_mut(key).expect("retry batch").retry();
                let attempts =
                    u64::from(self.attempt_counts.get(&key.packed()).copied().unwrap_or(1));
                let backoff = self.retry_delay(attempts);
                let at = Self::deadline_after(now, backoff);
                if let Some(partition) = self.partitions.get_mut(&response.partition) {
                    partition.retry_at = at;
                }
                self.deadlines
                    .set(DeadlineKey::Retry(response.partition), at);
            }
            self.scheduler_mark(response.partition);
        }
        self.refresh_recovery(now);
        self.complete_fences();
        Ok(())
    }
    pub fn on_connection(
        &mut self,
        key: ConnectionKey,
        now: RuntimeInstant,
        event: ConnectionEvent,
    ) -> Result<()> {
        self.observe_metrics_time(now);
        match event {
            ConnectionEvent::Active => {
                let connection = self
                    .connections
                    .get_mut(Slot::from_packed(key.0))
                    .ok_or(EngineError::StaleConnection)?;
                if connection.phase == ConnectionPhase::Connecting {
                    connection.phase = ConnectionPhase::Active;
                } else {
                    return Err(EngineError::InvalidState("unexpected active connection"));
                }
                let route = (connection.broker, connection.lane);
                self.encoder_dispatch_changed(route.0, route.1);
                self.scheduler_connection_changed(route.0, route.1);
            }
            ConnectionEvent::Retiring { reason } => {
                if self.connections.get(Slot::from_packed(key.0)).is_none() {
                    return Err(EngineError::StaleConnection);
                }
                self.retry_connection(key, now);
                self.retire_connection(key, reason);
            }
            ConnectionEvent::Released => {
                self.retire_connection(key, RetireReason::Requested);
                let requests: Vec<_> = self
                    .connections
                    .get(Slot::from_packed(key.0))
                    .ok_or(EngineError::StaleConnection)?
                    .fifo
                    .iter()
                    .copied()
                    .collect();
                for request in requests {
                    self.reconcile_retired(request)?;
                }
                self.remove_connection_order(key, 0);
                self.remove_connection_order(key, 2);
                let connection = self.connections.remove(Slot::from_packed(key.0))?;
                if self.routes.get(&(connection.broker, connection.lane)) == Some(&key) {
                    self.routes.remove(&(connection.broker, connection.lane));
                }
                drop(connection.credit);
                self.scheduler_connection_changed(connection.broker, connection.lane);
                self.request_identity_if_retired()?;
            }
        }
        self.refresh_recovery(now);
        self.complete_fences();
        Ok(())
    }
    pub(super) fn retire_connection(&mut self, key: ConnectionKey, reason: RetireReason) {
        let Some(connection) = self.connections.get_mut(Slot::from_packed(key.0)) else {
            return;
        };
        if connection.phase == ConnectionPhase::Retiring {
            return;
        }
        connection.phase = ConnectionPhase::Retiring;
        // Quarantine immediately, but retain each request until the driver's
        // terminal evidence arrives. Dropping an observing future is not evidence.
        for request in &connection.fifo {
            self.deadlines.remove(DeadlineKey::Request(*request));
        }
        self.remove_connection_order(key, 1);
        let _ = self.order(EngineOrder::Retire {
            connection: key,
            reason,
        });
    }
    /// Reconcile a driver's terminal request evidence after all admitted writes
    /// for it have actually completed. Never-admitted queued frames are allowed.
    pub fn on_request_retired(
        &mut self,
        connection: ConnectionKey,
        correlation: i32,
        now: RuntimeInstant,
        confirmed: usize,
        certainty: CompletionCertainty,
    ) -> Result<()> {
        self.observe_metrics_time(now);
        let key = self
            .request_for(connection, correlation)
            .ok_or(EngineError::StaleRequest)?;
        let admitted = self
            .requests
            .get(Slot::from_packed(key.0))
            .ok_or(EngineError::StaleRequest)?
            .admitted;
        if admitted {
            self.on_write(connection, correlation, confirmed, certainty)?;
        } else if confirmed != 0 || certainty != CompletionCertainty::NotApplied {
            return Err(EngineError::InvalidState(
                "never-admitted request has write evidence",
            ));
        }
        self.reconcile_retired(key)?;
        self.refresh_recovery(now);
        self.complete_fences();
        Ok(())
    }
    fn reconcile_retired(&mut self, key: RequestKey) -> Result<()> {
        let request = self.remove_request(key)?;
        for batch_key in request.batches {
            let partition = self.batches.get(batch_key).map(Batch::partition);
            if let Some(batch) = self.batches.get_mut(batch_key) {
                if request.admitted
                    && let Some(ledger) = &mut self.ledger
                {
                    ledger.retire_attempt(
                        batch.partition(),
                        batch_key.packed(),
                        request.attempt,
                        request.confirmed > 0
                            || request.certainty != CompletionCertainty::NotApplied
                            || request.initial_write_unresolved,
                    )?;
                }
                batch.retry();
            }
            if let Some(partition) = partition {
                self.scheduler_mark(partition);
            }
        }
        Ok(())
    }
    fn remove_request(&mut self, key: RequestKey) -> Result<RequestState> {
        let request = self.requests.remove(Slot::from_packed(key.0))?;
        let metric_broker = self
            .connections
            .get(Slot::from_packed(request.connection.0))
            .map(|c| c.broker);
        for batch in &request.batches {
            if self.batch_requests.get(batch) == Some(&key) {
                self.batch_requests.remove(batch);
            }
        }
        for &partition in &request.partitions {
            let queue = self
                .partitions
                .get_mut(&partition)
                .expect("request retains partition");
            queue.request_owners = queue
                .request_owners
                .checked_sub(1)
                .expect("one request owner per partition");
            self.partition_cleanup_released(partition);
        }
        self.deadlines.remove(DeadlineKey::Request(key));
        if let Some(connection) = self
            .connections
            .get_mut(Slot::from_packed(request.connection.0))
        {
            connection.fifo.retain(|candidate| *candidate != key);
            if connection.writing == Some(key) {
                connection.writing = None;
            }
            connection.bytes = connection.bytes.saturating_sub(request.bytes);
            if let Some(broker) = self.brokers.get_mut(&connection.broker) {
                broker.bytes = broker.bytes.saturating_sub(request.bytes);
                broker.requests = broker.requests.saturating_sub(1);
            }
            let route = (connection.broker, connection.lane);
            self.encoder_dispatch_changed(route.0, route.1);
            for lane in 0..self.config.lanes {
                self.scheduler_connection_changed(route.0, lane);
            }
        }
        self.metrics_request_depth(metric_broker, request.bytes, false);
        Ok(request)
    }
    pub(super) fn refresh_partition_metadata(
        &mut self,
        partition: TopicPartition,
        now: RuntimeInstant,
    ) {
        if let Some(handle) = self.topics.by_id(partition.topic) {
            let _ = self.topics.request_refresh(handle, now);
            self.deadlines.set(DeadlineKey::Topic(handle), now);
        }
    }
    pub(super) fn retry_delay(&self, attempt: u64) -> RuntimeDuration {
        retry::delay(&self.config, attempt, self.retry_jitter)
    }
}
