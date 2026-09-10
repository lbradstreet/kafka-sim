//! Passive-engine transitions against the independent, wire-speaking broker model.
#[path = "engine/epoch_recovery.rs"]
mod epoch_recovery;
#[path = "engine/rejection.rs"]
mod rejection;
#[path = "engine/sequence_retry.rs"]
mod sequence_retry;
#[path = "engine/terminal_order.rs"]
mod terminal_order;
use kr_kafka_broker_model::{BrokerAction, BrokerConfig, BrokerModel, FaultPlan};
use kr_kafka_producer::{
    admission::{Admission, SubmissionBatch},
    config::{Compression, ProducerConfig},
    control::{BrokerNode, ControlCodec, MetadataPartition, MetadataTopic, MetadataUpdate},
    credit::{Resource, SharedCredits},
    engine::{ConnectionEvent, ConnectionKey, EngineOrder, ProducerEngine, RequestKey},
    routing::PartitionChoice,
    topic::{PartitionMetadata, TopicState},
    transport::{OwnedSendPlan, RetireReason},
    types::*,
};
use kr_kafka_protocol::{errors as code, frame::decode_request, wire::DecodeLimits};
use kr_runtime::{CompletionCertainty as Certainty, RuntimeDuration, RuntimeInstant};

fn at(n: u64) -> RuntimeInstant {
    RuntimeInstant::from_nanos(n)
}
fn config() -> ProducerConfig {
    ProducerConfig {
        compression: Compression::None,
        codec_contexts: 0,
        record_descriptors: 64,
        delivery_event_capacity: 64,
        release_event_capacity: 16,
        max_live_leases: 16,
        mailbox_capacity: 8,
        max_batches: 32,
        max_open_topics: 4,
        pending_records_per_topic: 64,
        brokers_max: 2,
        request_max_partitions: 4,
        input_bytes: 65536,
        compressed_bytes: 65536,
        batch_target_bytes: 64,
        // Pin request membership for the wire/sequence transition fixtures.
        batch_target_mode: kr_kafka_producer::config::BatchTargetMode::Raw,
        batch_hard_bytes: 4096,
        progressive_threshold: 32,
        output_chunk_bytes: 4096,
        request_target_bytes: 4096,
        request_hard_bytes: 8192,
        connection_wire_window_bytes: 8192,
        staging_bytes_per_connection: 4096,
        rx_bytes_per_connection: 8192,
        control_reserve_bytes: 65536,
        retry_backoff_min: RuntimeDuration::from_nanos(10),
        retry_backoff_max: RuntimeDuration::from_nanos(100),
        linger_skip_below_rate: None,
        ..ProducerConfig::default()
    }
}
fn budget() -> WorkBudget {
    WorkBudget {
        bytes: 65536,
        items: 64,
    }
}
fn node(id: i32) -> BrokerNode {
    BrokerNode {
        id,
        host: "broker".into(),
        port: 9092,
        rack: None,
    }
}
fn metadata(id: TopicId, leaders: &[i32], epoch: i32) -> MetadataUpdate {
    MetadataUpdate {
        throttle_ms: 0,
        brokers: vec![node(0), node(1)],
        cluster_id: None,
        controller_id: 0,
        topics: vec![MetadataTopic {
            requested_index: 0,
            id,
            name: Some("test".into()),
            error_code: 0,
            partitions: leaders
                .iter()
                .enumerate()
                .map(|(index, &leader)| MetadataPartition {
                    replicas: Vec::new(),
                    isr: Vec::new(),
                    offline: Vec::new(),
                    index: index as i32,
                    error_code: 0,
                    metadata: PartitionMetadata {
                        leader,
                        leader_epoch: epoch,
                    },
                })
                .collect(),
        }],
    }
}
struct Fixture {
    engine: ProducerEngine,
    admission: Admission,
    broker: BrokerModel,
    topic: TopicHandle,
    id: TopicId,
}
impl Fixture {
    fn new(config: ProducerConfig, partitions: usize) -> Self {
        Self::with_epoch(config, partitions, 0)
    }
    fn with_epoch(config: ProducerConfig, partitions: usize, epoch: i16) -> Self {
        let mut engine = ProducerEngine::new(
            config.clone(),
            Some(ProducerIdentity {
                producer_id: 7,
                epoch,
            }),
        )
        .unwrap();
        let topic = engine.open_topic("test", at(0)).unwrap();
        let mut broker = BrokerModel::new(BrokerConfig::default()).unwrap();
        broker
            .add_broker(kr_kafka_broker_model::BrokerEndpoint {
                id: 0,
                host: "broker".into(),
                port: 9092,
            })
            .unwrap();
        let id = TopicId(broker.create_topic("test", &vec![0; partitions]).unwrap());
        engine
            .apply_metadata(at(0), &[topic], metadata(id, &vec![0; partitions], 0))
            .unwrap();
        assert!(matches!(
            engine.pop_event().unwrap().event,
            Event::TopicReady { .. }
        ));
        let admission = Admission::new(
            &config,
            engine.credits(),
            config.validate().unwrap().effective_batch_payload_bytes,
        );
        Self {
            engine,
            admission,
            broker,
            topic,
            id,
        }
    }
    fn prepare(&mut self, n: usize, partition: i32, now: u64) -> SubmissionBatch {
        let records: Vec<_> = (0..n)
            .map(|index| RecordDescriptor {
                topic: self.topic,
                partition_hint: Some(partition),
                lane_hint: None,
                key: None,
                value: Some(&[42; 80]),
                headers: &[],
                timestamp_ms: now as i64,
                user_token: index as u64,
                delivery_timeout: None,
            })
            .collect();
        let (result, batch) = self
            .admission
            .prepare_copy(at(now), &records, &vec![Ok(0); n]);
        assert_eq!(result.accepted as usize, n, "{:?}", result.error);
        batch.unwrap()
    }
    fn submit(&mut self, n: usize, partition: i32, now: u64) {
        let batch = self.prepare(n, partition, now);
        self.engine
            .admit(
                at(now),
                batch,
                &vec![PartitionChoice::Partition(partition); n],
            )
            .unwrap();
    }
    fn dispatch(&mut self, now: u64) -> Dispatch {
        for _ in 0..64 {
            self.engine.on_deadline(at(now), budget());
            self.engine.encode(at(now), budget());
            self.engine.schedule(at(now), budget());
            while let Some(order) = self.engine.pop_order() {
                match order {
                    EngineOrder::Connect { key, .. } => self
                        .engine
                        .on_connection(key, at(now), ConnectionEvent::Active)
                        .unwrap(),
                    EngineOrder::Dispatch {
                        connection,
                        request,
                        correlation,
                        plan,
                        ..
                    } => {
                        return Dispatch {
                            connection,
                            request,
                            correlation,
                            plan,
                        };
                    }
                    EngineOrder::Retire { connection, .. } => self
                        .engine
                        .on_connection(connection, at(now), ConnectionEvent::Released)
                        .unwrap(),
                    EngineOrder::InitProducerId { previous } => {
                        let codec = ControlCodec::from_config(self.engine.config()).unwrap();
                        let request = codec.init_producer_id_request(99, previous).unwrap();
                        let BrokerAction::Reply(response) = self
                            .broker
                            .handle_frame(0, &request, FaultPlan::default())
                            .unwrap()
                        else {
                            panic!("identity response")
                        };
                        let identity = codec
                            .parse_identity(&response, 99)
                            .unwrap()
                            .identity
                            .unwrap();
                        self.engine.install_identity(identity).unwrap();
                    }
                    _ => {}
                }
            }
        }
        panic!("no dispatch: {:?}", self.engine.status());
    }
    fn answer(&mut self, dispatch: Dispatch, now: u64, fault: FaultPlan) -> Vec<Event> {
        self.answer_with_response(dispatch, now, fault).0
    }
    fn answer_with_response(
        &mut self,
        dispatch: Dispatch,
        now: u64,
        fault: FaultPlan,
    ) -> (Vec<Event>, Option<(i32, Vec<u8>)>) {
        self.engine.on_write_admitted(dispatch.request).unwrap();
        self.engine
            .on_write(
                dispatch.connection,
                dispatch.correlation,
                dispatch.plan.len(),
                Certainty::Applied,
            )
            .unwrap();
        let bytes = dispatch.bytes();
        let response = self.broker.handle_frame(0, &bytes, fault).unwrap();
        let observed = if let BrokerAction::Reply(response) = response {
            self.engine
                .on_frame(dispatch.connection, at(now), &response)
                .unwrap();
            Some((dispatch.correlation, response))
        } else {
            None
        };
        drop(dispatch);
        (self.events(), observed)
    }
    fn events(&mut self) -> Vec<Event> {
        let mut events = Vec::new();
        // Passive test driver explicitly charges each terminal cleanup step;
        // the production actor performs this work under its per-poll budget.
        for _ in 0..10_000 {
            if let Some(envelope) = self.engine.pop_event() {
                events.push(envelope.event);
            } else if self.engine.has_maintenance_work() {
                self.engine
                    .on_deadline(at(0), WorkBudget { bytes: 1, items: 1 });
            } else if !self.engine.has_terminal_work() {
                return events;
            }
        }
        panic!("terminal cleanup driving bound");
    }
}
struct Dispatch {
    connection: ConnectionKey,
    request: RequestKey,
    correlation: i32,
    plan: OwnedSendPlan,
}
impl Dispatch {
    fn bytes(&self) -> Vec<u8> {
        self.plan
            .segments()
            .iter()
            .flat_map(|segment| segment.as_slice())
            .copied()
            .collect()
    }
}
fn deliveries(events: &[Event]) -> Vec<DeliveryEvent> {
    events
        .iter()
        .filter_map(|event| {
            if let Event::Delivery(d) = event {
                Some(*d)
            } else {
                None
            }
        })
        .collect()
}

#[test]
fn actual_produce13_ack_offsets_and_event_credit_lifetime() {
    let mut f = Fixture::new(config(), 1);
    let metadata_bytes = f.engine.credits().snapshot()[Resource::InputBytes as usize].held;
    f.submit(3, 0, 0);
    let request = f.dispatch(1);
    let bytes = request.bytes();
    let decoded = decode_request(&bytes, DecodeLimits::default()).unwrap();
    assert_eq!(
        (decoded.api_key, decoded.version, decoded.correlation_id),
        (0, 13, request.correlation)
    );
    assert_eq!(
        f.engine.credits().snapshot()[Resource::InputBytes as usize].held,
        metadata_bytes
    );
    let events = f.answer(request, 1_000_000, FaultPlan::default());
    assert_eq!(deliveries(&events).len(), 1); // target=64 forms one record per immutable batch.
    for expected in 1..3 {
        let request = f.dispatch(1_000_001 + expected);
        let events = f.answer(request, 2_000_000 + expected, FaultPlan::default());
        assert_eq!(
            deliveries(&events)[0].base_offset.get(),
            Some(expected as i64)
        );
    }
    let snapshot = f.engine.partition_snapshot(
        at(3_000_000),
        TopicPartition {
            topic: f.id,
            partition: 0,
        },
    );
    assert_eq!(snapshot.queued_bytes, 0);
    assert!(snapshot.drain_bytes_per_second > 0);
    assert_eq!(f.broker.log().len(), 3);
    assert_eq!(f.engine.status().terminal, 3);
    assert_eq!(
        f.engine.credits().snapshot()[Resource::DeliveryEvents as usize].held,
        0
    );
}

#[test]
fn full_five_request_window_is_shared_across_reconnect_and_fifo() {
    let mut f = Fixture::new(config(), 1);
    f.submit(6, 0, 0);
    let mut requests = Vec::new();
    for _ in 0..5 {
        let request = f.dispatch(1);
        f.engine.on_write_admitted(request.request).unwrap();
        f.engine
            .on_write(
                request.connection,
                request.correlation,
                request.plan.len(),
                Certainty::Applied,
            )
            .unwrap();
        requests.push(request);
    }
    for _ in 0..8 {
        f.engine.schedule(at(2), budget());
    }
    assert_eq!(f.engine.status().requests, 5);
    assert_eq!(f.engine.status().queued_orders, 0);
    let connection = requests[0].connection;
    f.engine
        .on_connection(
            connection,
            at(3),
            ConnectionEvent::Retiring {
                reason: RetireReason::Requested,
            },
        )
        .unwrap();
    assert_eq!(f.engine.status().requests, 5);
    for request in &requests {
        f.engine
            .on_request_retired(
                connection,
                request.correlation,
                at(4),
                request.plan.len(),
                Certainty::Applied,
            )
            .unwrap();
    }
    assert_eq!(f.engine.status().requests, 0);
    // An old provider still owns all immutable plans; no replacement connection until Released.
    for _ in 0..8 {
        f.engine.schedule(at(1000), budget());
    }
    assert_eq!(f.engine.status().connections, 1);
    f.engine
        .on_connection(connection, at(1000), ConnectionEvent::Released)
        .unwrap();
    let retry = f.dispatch(1000);
    assert_ne!(retry.connection, connection);
    assert!(f.engine.on_write_admitted(requests[0].request).is_err());
    assert_eq!(
        retry.bytes(),
        requests[0]
            .bytes()
            .into_iter()
            .enumerate()
            .map(|(i, b)| if (8..12).contains(&i) {
                retry.bytes()[i]
            } else {
                b
            })
            .collect::<Vec<_>>()
    );
}

#[test]
fn retirement_waits_for_terminal_certainty_and_unsent_is_not_written() {
    for admitted in [false, true] {
        for certainty in [Certainty::NotApplied, Certainty::MayHaveApplied] {
            if !admitted && certainty != Certainty::NotApplied {
                continue;
            }
            let mut f = Fixture::new(config(), 1);
            f.submit(1, 0, 0);
            let request = f.dispatch(1);
            if admitted {
                f.engine.on_write_admitted(request.request).unwrap();
            }
            f.engine
                .on_connection(
                    request.connection,
                    at(2),
                    ConnectionEvent::Retiring {
                        reason: RetireReason::Requested,
                    },
                )
                .unwrap();
            assert_eq!(
                f.engine
                    .request_for(request.connection, request.correlation),
                Some(request.request)
            );
            assert!(f.events().is_empty());
            f.engine
                .on_request_retired(request.connection, request.correlation, at(3), 0, certainty)
                .unwrap();
            f.engine.cancel(at(4), RecordToken(1)).unwrap();
            let events = f.events();
            let delivery = deliveries(&events)[0];
            assert_eq!(
                delivery.outcome.kind,
                if certainty == Certainty::NotApplied {
                    DeliveryKind::NotWritten
                } else {
                    DeliveryKind::Unknown
                }
            );
            assert!(!f.engine.status().failed);
        }
    }
}

#[test]
fn flush_and_emergency_close_include_accepted_commands_not_yet_engine_admitted() {
    let mut f = Fixture::new(config(), 1);
    let batch = f.prepare(2, 0, 0);
    let flush = f.engine.flush(at(1), RecordToken(2)).unwrap();
    f.engine
        .close(at(1), at(10_000_000), RecordToken(2))
        .unwrap();
    assert!(!f.engine.status().closed);
    f.engine
        .admit_records(at(2), batch.drain(), &[PartitionChoice::Partition(0); 2])
        .unwrap();
    let mut events = Vec::new();
    for _ in 0..2 {
        let request = f.dispatch(3);
        events.extend(f.answer(request, 4, FaultPlan::default()));
    }
    assert_eq!(deliveries(&events).len(), 2);
    let flush_position = events
        .iter()
        .position(|event| matches!(event, Event::FlushDone {token} if *token == flush))
        .unwrap();
    assert!(
        events[..flush_position]
            .iter()
            .filter(|event| matches!(event, Event::Delivery(_)))
            .count()
            == 2
    );
    let mut connections = Vec::new();
    while let Some(order) = f.engine.pop_order() {
        if let EngineOrder::Retire { connection, .. } = order {
            connections.push(connection);
        }
    }
    for connection in connections {
        f.engine
            .on_connection(connection, at(5), ConnectionEvent::Released)
            .unwrap();
    }
    events.extend(f.events());
    assert!(matches!(
        events.last(),
        Some(Event::Closed { unresolved: 0 })
    ));
    assert!(f.engine.is_quiescent());
}

#[test]
fn deadline_index_never_accumulates_stale_entries_and_pending_resolution_expires() {
    let mut engine = ProducerEngine::new(
        config(),
        Some(ProducerIdentity {
            producer_id: 1,
            epoch: 0,
        }),
    )
    .unwrap();
    let topic = engine.open_topic("unresolved", at(0)).unwrap();
    for n in 0..1000 {
        engine.metadata_failed(&[topic], at(n));
        assert!(engine.status().deadlines <= 1);
    }
    let deadline = engine.topics().get(topic).unwrap().resolution_deadline;
    let progress = engine.on_deadline(deadline, WorkBudget { bytes: 1, items: 1 });
    assert_eq!(progress.items, 1);
    assert_eq!(
        engine.topics().get(topic).unwrap().state,
        TopicState::Failed
    );
    assert!(matches!(
        engine.pop_event().unwrap().event,
        Event::TopicFailed { .. }
    ));
}

#[test]
fn metadata_leader_epochs_and_topic_recreation_never_redirect_an_old_handle() {
    let mut f = Fixture::new(config(), 1);
    f.engine
        .apply_metadata(at(10), &[f.topic], metadata(f.id, &[1], 4))
        .unwrap();
    f.engine
        .apply_metadata(at(11), &[f.topic], metadata(f.id, &[0], 3))
        .unwrap();
    assert_eq!(
        f.engine.topics().get(f.topic).unwrap().partitions[0].leader,
        1
    );
    f.submit(1, 0, 12);
    f.engine
        .apply_metadata(at(13), &[f.topic], metadata(TopicId([99; 16]), &[0], 0))
        .unwrap();
    assert_eq!(f.engine.topics().get(f.topic).unwrap().id, Some(f.id));
    assert_eq!(
        f.engine.topics().get(f.topic).unwrap().state,
        TopicState::Deleted
    );
    assert_eq!(
        deliveries(&f.events())[0].outcome.kind,
        DeliveryKind::NotWritten
    );
}

#[test]
fn actual_throttle_blocks_every_lane_and_snapshot_contains_real_backlog() {
    let mut c = config();
    c.lanes = 2;
    let mut f = Fixture::new(c, 2);
    f.submit(1, 0, 0);
    let request = f.dispatch(1);
    f.answer(
        request,
        2,
        FaultPlan {
            throttle_time_ms: 10,
            ..FaultPlan::default()
        },
    );
    f.submit(1, 1, 3);
    let snapshot = f.engine.partition_snapshot(
        at(4),
        TopicPartition {
            topic: f.id,
            partition: 1,
        },
    );
    assert!(snapshot.queued_bytes > 80);
    assert_eq!(snapshot.oldest_age, RuntimeDuration::from_nanos(1));
    assert_eq!(snapshot.lane, 1);
    assert!(!snapshot.available);
    for _ in 0..4 {
        f.engine.encode(at(4), budget());
        f.engine.schedule(at(4), budget());
    }
    while let Some(order) = f.engine.pop_order() {
        match order {
            EngineOrder::Connect { key, .. } => f
                .engine
                .on_connection(key, at(4), ConnectionEvent::Active)
                .unwrap(),
            EngineOrder::Dispatch { .. } => panic!("dispatch during broker throttle"),
            _ => {}
        }
    }
    f.engine.on_deadline(at(10_000_002), budget());
    let request = f.dispatch(10_000_002);
    assert_eq!(
        decode_request(&request.bytes(), DecodeLimits::default())
            .unwrap()
            .version,
        13
    );
}

#[test]
fn request_and_connection_guards_survive_engine_drop_until_actual_span_release() {
    let mut c = config();
    c.coalesce_below_bytes = 0;
    let mut f = Fixture::new(c, 1);
    let credits: SharedCredits = f.engine.credits();
    f.submit(1, 0, 0);
    f.engine.encode(at(0), budget());
    f.engine.schedule(at(0), budget());
    let (connection, guard) = match f.engine.pop_order().unwrap() {
        EngineOrder::Connect {
            key,
            lifetime_guard,
            ..
        } => (key, lifetime_guard),
        other => panic!("{other:?}"),
    };
    f.engine
        .on_connection(connection, at(1), ConnectionEvent::Active)
        .unwrap();
    let request = f.dispatch(1);
    let spans = request.plan.segments().to_vec();
    drop(request);
    drop(f);
    let held = credits.snapshot();
    assert!(held[Resource::CompressedBytes as usize].held > 0);
    assert!(held[Resource::RequestMetadata as usize].held > 0);
    assert!(held[Resource::RxBytes as usize].held > 0);
    drop(spans);
    assert_eq!(
        credits.snapshot()[Resource::RequestMetadata as usize].held,
        0
    );
    assert_eq!(
        credits.snapshot()[Resource::CompressedBytes as usize].held,
        0
    );
    drop(guard);
    assert!(credits.is_empty());
}

#[test]
fn sequence_recovery_bumps_the_epoch_without_changing_producer_id() {
    let mut f = Fixture::new(config(), 1);
    f.submit(2, 0, 0);
    let request = f.dispatch(1);
    let events = f.answer(
        request,
        2,
        FaultPlan {
            reject_before_commit: Some(code::OUT_OF_ORDER_SEQUENCE_NUMBER),
            ..FaultPlan::default()
        },
    );
    assert_eq!(
        deliveries(&events)[0].outcome.kind,
        DeliveryKind::NotWritten
    );
    assert!(!f.engine.status().failed);
    // Dispatch drives local epoch installation without a broker identity request.
    let next = f.dispatch(1000);
    let identity = f.engine.status().identity.unwrap();
    assert_eq!(identity.producer_id, 7);
    assert_eq!(identity.epoch, 1);
    let events = f.answer(next, 2000, FaultPlan::default());
    assert_eq!(deliveries(&events)[0].outcome.kind, DeliveryKind::Acked);
    let batch = &f.broker.log()[0];
    assert_eq!(batch.identity.producer_id, identity.producer_id);
    assert_eq!(batch.identity.base_sequence, 0);
}

#[test]
fn one_item_encode_keeps_linger_deadline_and_does_not_self_wake_after_input_consumption() {
    let mut c = config();
    c.batch_target_bytes = 4096;
    let mut f = Fixture::new(c, 1);
    f.submit(1, 0, 0);
    f.engine.encode(
        at(0),
        WorkBudget {
            bytes: 4096,
            items: 1,
        },
    );
    assert!(f.engine.next_deadline().is_some());
    f.engine.schedule(at(0), budget());
    let EngineOrder::Connect { key, .. } = f.engine.pop_order().unwrap() else {
        panic!("connect before linger")
    };
    f.engine
        .on_connection(key, at(0), ConnectionEvent::Active)
        .unwrap();
    let mut progress = Progress::default();
    for _ in 0..8 {
        progress = f.engine.encode(at(0), budget());
    }
    assert!(
        !progress.remaining_immediate,
        "open batch with fully consumed input must wait for its timer"
    );
    assert_eq!(f.engine.next_deadline(), Some(at(500_000)));
    f.engine.on_deadline(at(500_000), budget());
    let request = f.dispatch(500_000);
    assert_eq!(
        deliveries(&f.answer(request, 500_001, FaultPlan::default()))[0]
            .outcome
            .kind,
        DeliveryKind::Acked
    );
}

#[test]
fn zero_poll_and_stopped_event_consumer_preserve_all_credit_obligations() {
    let mut f = Fixture::new(config(), 1);
    f.submit(1, 0, 0);
    let zero = WorkBudget { bytes: 0, items: 0 };
    assert_eq!(f.engine.encode(at(0), zero), Progress::default());
    assert_eq!(f.engine.schedule(at(0), zero), Progress::default());
    let request = f.dispatch(1);
    f.engine.on_write_admitted(request.request).unwrap();
    f.engine
        .on_write(
            request.connection,
            request.correlation,
            request.plan.len(),
            Certainty::Applied,
        )
        .unwrap();
    let BrokerAction::Reply(response) = f
        .broker
        .handle_frame(0, &request.bytes(), FaultPlan::default())
        .unwrap()
    else {
        panic!("reply")
    };
    f.engine
        .on_frame(request.connection, at(2), &response)
        .unwrap();
    f.engine.encode(at(2), budget());
    let credits = f.engine.credits();
    assert_eq!(
        credits.snapshot()[Resource::DeliveryEvents as usize].held,
        1
    );
    let envelope = f.engine.pop_event().unwrap();
    assert_eq!(
        credits.snapshot()[Resource::DeliveryEvents as usize].held,
        1
    );
    drop(envelope);
    assert_eq!(
        credits.snapshot()[Resource::DeliveryEvents as usize].held,
        0
    );
}

#[test]
fn seeded_fault_campaign_checks_actual_log_deliveries_and_credit_conservation() {
    use kr_kafka_broker_model::{
        AcceptedRecord, CreditObservation, DeliveryOracle, ObservedDelivery, ObservedOutcome,
        ObservedResponse, OracleLimits,
    };
    for seed in 1..=64u64 {
        let mut random = seed;
        let mut f = Fixture::new(config(), 2);
        let credits = f.engine.credits();
        let mut oracle = DeliveryOracle::new(OracleLimits::default());
        let mut accepted = 0;
        for token in 1..=8u64 {
            random = random
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            let case = (random >> 32) % 5;
            let partition = (token % 2) as i32;
            let now = token * 1_000_000;
            let mut value = [0u8; 80];
            value[..8].copy_from_slice(&token.to_be_bytes());
            let descriptor = RecordDescriptor {
                topic: f.topic,
                partition_hint: Some(partition),
                lane_hint: None,
                key: None,
                value: Some(&value),
                headers: &[],
                timestamp_ms: now as i64,
                user_token: token,
                delivery_timeout: None,
            };
            let (result, batch) = f.admission.prepare_copy(at(now), &[descriptor], &[Ok(0)]);
            assert_eq!(result.accepted, 1, "seed {seed}");
            oracle
                .accept(AcceptedRecord {
                    token,
                    topic: f.id.0,
                    partition,
                    lease: None,
                    returned_at: token * 10,
                })
                .unwrap();
            accepted += 1;
            f.engine
                .admit(
                    at(now),
                    batch.unwrap(),
                    &[PartitionChoice::Partition(partition)],
                )
                .unwrap();
            let (events, transmitted, ambiguous, response) = if case == 0 {
                f.engine.cancel(at(now + 1), RecordToken(token)).unwrap();
                (f.events(), false, false, None)
            } else {
                let request = f.dispatch(now + 1);
                if case == 1 {
                    let (events, response) =
                        f.answer_with_response(request, now + 2, FaultPlan::default());
                    (events, true, false, response)
                } else if case == 2 {
                    f.engine.on_write_admitted(request.request).unwrap();
                    f.engine
                        .on_connection(
                            request.connection,
                            at(now + 2),
                            ConnectionEvent::Retiring {
                                reason: RetireReason::Requested,
                            },
                        )
                        .unwrap();
                    f.engine
                        .on_request_retired(
                            request.connection,
                            request.correlation,
                            at(now + 3),
                            0,
                            Certainty::NotApplied,
                        )
                        .unwrap();
                    f.engine
                        .on_connection(request.connection, at(now + 4), ConnectionEvent::Released)
                        .unwrap();
                    f.engine.cancel(at(now + 5), RecordToken(token)).unwrap();
                    drop(request);
                    (f.events(), false, false, None)
                } else {
                    f.engine.on_write_admitted(request.request).unwrap();
                    f.engine
                        .on_write(
                            request.connection,
                            request.correlation,
                            request.plan.len(),
                            Certainty::Applied,
                        )
                        .unwrap();
                    assert!(matches!(
                        f.broker
                            .handle_frame(
                                0,
                                &request.bytes(),
                                FaultPlan {
                                    drop_after_commit: true,
                                    ..FaultPlan::default()
                                }
                            )
                            .unwrap(),
                        BrokerAction::DropResponse { .. }
                    ));
                    f.engine
                        .on_connection(
                            request.connection,
                            at(now + 2),
                            ConnectionEvent::Retiring {
                                reason: RetireReason::Requested,
                            },
                        )
                        .unwrap();
                    f.engine
                        .on_request_retired(
                            request.connection,
                            request.correlation,
                            at(now + 3),
                            request.plan.len(),
                            Certainty::Applied,
                        )
                        .unwrap();
                    f.engine
                        .on_connection(request.connection, at(now + 4), ConnectionEvent::Released)
                        .unwrap();
                    drop(request);
                    if case == 3 {
                        let retry = f.dispatch(now + 1000);
                        let (events, response) =
                            f.answer_with_response(retry, now + 1001, FaultPlan::default());
                        (events, true, true, response)
                    } else {
                        f.engine.cancel(at(now + 5), RecordToken(token)).unwrap();
                        (f.events(), true, true, None)
                    }
                }
            };
            let deliveries = deliveries(&events);
            assert_eq!(deliveries.len(), 1, "seed {seed} token {token} case {case}");
            let delivery = deliveries[0];
            let response = response.map(|(correlation, bytes)| {
                let codec = ControlCodec::from_config(f.engine.config()).unwrap();
                let parsed = codec
                    .parse_produce13(
                        &bytes,
                        correlation,
                        &[TopicPartition {
                            topic: f.id,
                            partition,
                        }],
                    )
                    .unwrap();
                assert_eq!(parsed.partitions.len(), 1);
                let parsed = &parsed.partitions[0];
                match parsed.error_code {
                    code::NONE => ObservedResponse::Success {
                        at: token * 10 + 1,
                        // This fixture sends exactly one record in each batch.
                        offset: parsed.base_offset.unwrap(),
                        timestamp: parsed.timestamp,
                    },
                    code::DUPLICATE_SEQUENCE_NUMBER => {
                        ObservedResponse::Duplicate { at: token * 10 + 1 }
                    }
                    error => panic!("unexpected response error {error}"),
                }
            });
            oracle.input_consumed(token, token * 10 + 1).unwrap();
            oracle
                .delivery(ObservedDelivery {
                    token,
                    topic: delivery.partition.topic.0,
                    partition: delivery.partition.partition,
                    outcome: match delivery.outcome.kind {
                        DeliveryKind::Acked => ObservedOutcome::Acked,
                        DeliveryKind::NotWritten => ObservedOutcome::NotWritten,
                        DeliveryKind::Unknown => ObservedOutcome::Unknown,
                    },
                    offset: delivery.base_offset.get(),
                    timestamp: delivery.timestamp.get(),
                    attempts: delivery.attempts,
                    parsed_attempts: match case {
                        1 | 4 => 1,
                        3 => 2,
                        _ => 0,
                    },
                    at: token * 10 + 2,
                    transmitted,
                    definitive_broker_rejection: false,
                    prior_ambiguous_attempt: ambiguous,
                    response,
                })
                .unwrap();
            if f.engine.status().failed {
                break;
            }
        }
        f.engine
            .close(at(99_000_000), at(100_000_000), RecordToken(accepted))
            .unwrap();
        for _ in 0..8 {
            f.engine.on_deadline(at(99_000_001), budget());
            while let Some(order) = f.engine.pop_order() {
                if let EngineOrder::Retire { connection, .. } = order {
                    f.engine
                        .on_connection(connection, at(99_000_001), ConnectionEvent::Released)
                        .unwrap();
                }
            }
            f.engine.encode(at(99_000_001), budget());
        }
        assert!(
            f.events()
                .iter()
                .any(|event| matches!(event, Event::Closed { .. })),
            "seed {seed}: {:?}",
            f.engine.status()
        );
        drop(f.engine);
        drop(f.admission);
        for (index, pool) in credits.snapshot().iter().enumerate() {
            oracle
                .credits(CreditObservation {
                    pool: index as u32,
                    capacity: pool.limit as u64,
                    reserved: pool.reserved,
                    released: pool.released,
                    held: pool.held as u64,
                })
                .unwrap();
        }
        oracle.closed().unwrap();
        let report = oracle
            .finish(f.broker.log(), |record| {
                record
                    .value
                    .as_ref()
                    .and_then(|value| value.get(..8))
                    .map(|bytes| u64::from_be_bytes(bytes.try_into().unwrap()))
            })
            .unwrap();
        assert_eq!(report.accepted, accepted as usize, "seed {seed}");
    }
}

#[test]
fn drr_advances_past_a_hot_partition_even_when_one_connection_serializes_writes() {
    let mut c = config();
    c.request_max_partitions = 1;
    let mut f = Fixture::new(c, 2);
    f.submit(16, 0, 0);
    f.submit(1, 1, 0);
    let mut partitions = Vec::new();
    for n in 0..2 {
        let request = f.dispatch(n + 1);
        let events = f.answer(request, n + 2, FaultPlan::default());
        partitions.extend(
            deliveries(&events)
                .into_iter()
                .map(|delivery| delivery.partition.partition),
        );
    }
    partitions.sort_unstable();
    assert_eq!(partitions, vec![0, 1]);
}

#[test]
fn delivery_deadline_observes_the_last_actual_evidence_and_preserves_parsed_success() {
    for case in 0..3 {
        let mut c = config();
        c.delivery_timeout = RuntimeDuration::from_nanos(100);
        c.request_timeout = RuntimeDuration::from_nanos(50);
        let mut f = Fixture::new(c, 1);
        f.submit(1, 0, 0);
        let request = f.dispatch(1);
        f.engine.on_write_admitted(request.request).unwrap();
        let mut events = Vec::new();
        if case == 0 {
            f.engine
                .on_connection(
                    request.connection,
                    at(50),
                    ConnectionEvent::Retiring {
                        reason: RetireReason::Requested,
                    },
                )
                .unwrap();
            f.engine
                .on_request_retired(
                    request.connection,
                    request.correlation,
                    at(99),
                    0,
                    Certainty::NotApplied,
                )
                .unwrap();
        } else if case == 2 {
            f.engine
                .on_write(
                    request.connection,
                    request.correlation,
                    request.plan.len(),
                    Certainty::Applied,
                )
                .unwrap();
            let BrokerAction::Reply(response) = f
                .broker
                .handle_frame(0, &request.bytes(), FaultPlan::default())
                .unwrap()
            else {
                panic!("response")
            };
            f.engine
                .on_frame(request.connection, at(99), &response)
                .unwrap();
        }
        let mut progress = f
            .engine
            .on_deadline(at(100), WorkBudget { bytes: 1, items: 1 });
        for _ in 0..8 {
            if !progress.remaining_immediate {
                break;
            }
            assert!(progress.items <= 1);
            progress = f
                .engine
                .on_deadline(at(100), WorkBudget { bytes: 1, items: 1 });
        }
        events.extend(f.events());
        let expected = [
            DeliveryKind::NotWritten,
            DeliveryKind::Unknown,
            DeliveryKind::Acked,
        ][case];
        assert_eq!(deliveries(&events)[0].outcome.kind, expected, "case {case}");
        assert_eq!(deliveries(&events).len(), 1);
    }
}

#[test]
fn invalid_route_count_settles_every_previously_accepted_record() {
    let mut f = Fixture::new(config(), 1);
    let batch = f.prepare(3, 0, 0);
    let result = f.engine.admit(at(1), batch, &[]).unwrap();
    assert_eq!((result.records, result.failed), (3, 3));
    let events = f.events();
    assert_eq!(deliveries(&events).len(), 3);
    assert!(
        deliveries(&events).iter().all(
            |event| event.outcome == DeliveryOutcome::not_written(FailureReason::InvalidRecord)
        )
    );
    assert_eq!(f.engine.status().terminal, 3);
}

#[test]
fn cancellation_of_a_finalized_never_admitted_frame_quarantines_it_before_notwritten() {
    let mut f = Fixture::new(config(), 1);
    f.submit(1, 0, 0);
    let request = f.dispatch(1);
    f.engine.cancel(at(2), RecordToken(1)).unwrap();
    let events = f.events();
    assert_eq!(
        deliveries(&events)[0].outcome.kind,
        DeliveryKind::NotWritten
    );
    assert!(f.engine.on_write_admitted(request.request).is_err());
    assert!(
        matches!(f.engine.pop_order(),Some(EngineOrder::Retire {connection,..}) if connection == request.connection)
    );
    f.engine
        .on_request_retired(
            request.connection,
            request.correlation,
            at(3),
            0,
            Certainty::NotApplied,
        )
        .unwrap();
    f.engine
        .on_connection(request.connection, at(4), ConnectionEvent::Released)
        .unwrap();
    assert!(!f.engine.status().failed);
}

#[test]
fn old_unresolved_routing_cannot_be_overtaken_by_a_newly_ready_submission() {
    let mut f = Fixture::new(config(), 1);
    let first = f.prepare(1, 0, 0);
    f.engine
        .admit(at(0), first, &[PartitionChoice::Pending])
        .unwrap();
    f.submit(1, 0, 1);
    let progress = f.engine.encode(at(1), budget());
    assert_eq!(f.engine.status().batches, 0);
    assert!(!progress.remaining_immediate);
    f.engine
        .route_pending(at(2), RecordToken(1), PartitionChoice::Partition(0))
        .unwrap();
    let first = f.dispatch(3);
    assert_eq!(
        deliveries(&f.answer(first, 4, FaultPlan::default()))[0].token,
        RecordToken(1)
    );
    let second = f.dispatch(5);
    assert_eq!(
        deliveries(&f.answer(second, 6, FaultPlan::default()))[0].token,
        RecordToken(2)
    );
}

#[test]
fn request_metadata_and_tls_have_independent_named_credit_pools() {
    use kr_kafka_producer::{
        config::{SecurityConfig, TlsConfig},
        credit::Claim,
    };
    let mut c = config();
    c.security = SecurityConfig::Tls {
        tls: TlsConfig {
            use_system_roots: true,
            ..TlsConfig::default()
        },
    };
    let v = c.validate().unwrap();
    let tls_each = c.tls_plaintext_bytes as usize + c.tls_ciphertext_bytes as usize;
    assert_eq!(
        v.credits[Resource::TlsBytes as usize],
        v.max_connections * tls_each
    );
    assert_eq!(
        v.credits[Resource::RequestMetadata as usize],
        v.memory.request_metadata
    );
    let mut f = Fixture::new(c, 1);
    let credits = f.engine.credits();
    let control = credits
        .reserve(&[Claim {
            resource: Resource::ControlReserve,
            amount: v.memory.control,
            lane: 0,
        }])
        .unwrap();
    f.submit(1, 0, 0);
    let request = f.dispatch(1);
    assert_eq!(
        credits.snapshot()[Resource::TlsBytes as usize].held,
        tls_each
    );
    assert_eq!(
        deliveries(&f.answer(request, 2, FaultPlan::default()))[0]
            .outcome
            .kind,
        DeliveryKind::Acked
    );
    drop(control);
    let metadata = credits
        .reserve(&[Claim {
            resource: Resource::RequestMetadata,
            amount: v.memory.request_metadata,
            lane: 0,
        }])
        .unwrap();
    f.submit(1, 0, 3);
    f.engine.encode(at(3), budget());
    let progress = f.engine.schedule(at(3), budget());
    assert!(!progress.remaining_immediate);
    assert_eq!(f.engine.status().requests, 0);
    assert!(
        credits
            .reserve(&[Claim {
                resource: Resource::ControlReserve,
                amount: v.memory.control,
                lane: 0
            }])
            .is_ok()
    );
    drop(metadata);
    let request = f.dispatch(4);
    assert_eq!(
        deliveries(&f.answer(request, 5, FaultPlan::default()))[0]
            .outcome
            .kind,
        DeliveryKind::Acked
    );
}

#[test]
fn flush_sealing_and_close_control_events_obey_one_item_maintenance_budget() {
    let mut cfg = config();
    cfg.batch_target_bytes = 4096;
    cfg.request_target_bytes = 8192;
    let mut f = Fixture::new(cfg, 8);
    for partition in 0..8 {
        f.submit(1, partition, 0);
    }
    for _ in 0..8 {
        f.engine.encode(
            at(0),
            WorkBudget {
                bytes: 4096,
                items: 1,
            },
        );
    }
    assert_eq!(f.engine.status().batches, 8);
    assert_eq!(
        f.engine.status().batch_seals.by_reason.iter().sum::<u64>(),
        0
    );
    f.engine.flush(at(1), RecordToken(8)).unwrap();
    assert_eq!(
        f.engine.status().batch_seals.by_reason.iter().sum::<u64>(),
        0,
        "flush command only records a bounded sweep"
    );
    for expected in 1..=8 {
        let progress = f
            .engine
            .on_deadline(at(1), WorkBudget { bytes: 1, items: 1 });
        assert_eq!(progress.items, 1);
        assert_eq!(
            f.engine.status().batch_seals.by_reason.iter().sum::<u64>(),
            expected
        );
    }
    let mut empty = ProducerEngine::new(
        config(),
        Some(ProducerIdentity {
            producer_id: 1,
            epoch: 0,
        }),
    )
    .unwrap();
    for _ in 0..8 {
        empty.flush(at(0), RecordToken(0)).unwrap();
    }
    empty.close(at(0), at(100), RecordToken(0)).unwrap();
    assert_eq!(empty.status().queued_events, 0);
    let zero = empty.on_deadline(at(0), WorkBudget { bytes: 0, items: 0 });
    assert_eq!(zero.items, 0);
    assert_eq!(empty.status().queued_events, 0);
    for expected in 1..=8 {
        let progress = empty.on_deadline(at(0), WorkBudget { bytes: 1, items: 1 });
        assert_eq!(progress.items, 1);
        assert_eq!(empty.status().queued_events, expected);
    }
    for _ in 0..8 {
        empty.on_deadline(at(0), WorkBudget { bytes: 1, items: 1 });
    }
    assert!(empty.status().closed);
    let mut flushes = 0;
    while let Some(event) = empty.pop_event() {
        match event.event {
            Event::FlushDone { .. } => flushes += 1,
            Event::Closed { .. } => assert_eq!(flushes, 8),
            other => panic!("unexpected {other:?}"),
        }
    }
}

#[test]
fn producer_failure_retains_encoder_inputs_and_drains_each_owner_under_budget() {
    let mut cfg = config();
    cfg.batch_target_bytes = 4096;
    // Keep every payload pending in the builder: this fixture isolates abort
    // ownership from the encoder's descriptor/codec service alternation.
    cfg.progressive_threshold = 4096;
    cfg.request_target_bytes = 8192;
    let mut f = Fixture::new(cfg, 1);
    let metadata_bytes = f.engine.credits().snapshot()[Resource::InputBytes as usize].held;
    f.submit(16, 0, 0);
    for _ in 0..16 {
        let p = f.engine.encode(
            at(0),
            WorkBudget {
                bytes: 4096,
                items: 1,
            },
        );
        assert_eq!(p.items, 1);
    }
    assert_eq!(f.engine.status().batches, 1);
    let credits = f.engine.credits();
    let held = credits.snapshot()[Resource::InputBytes as usize].held;
    assert!(held > 0);
    f.engine.fail_producer(FailureReason::RuntimeFailed);
    assert!(f.engine.is_failed());
    assert_eq!(f.engine.status().terminal, 0);
    assert_eq!(credits.snapshot()[Resource::InputBytes as usize].held, held);
    for _ in 0..4 {
        f.engine
            .on_deadline(at(1), WorkBudget { bytes: 1, items: 1 });
    }
    assert_eq!(f.engine.status().batches, 0);
    assert_eq!(
        credits.snapshot()[Resource::InputBytes as usize].held,
        held,
        "moving a failed batch does not drop its retained record queue"
    );
    for _ in 0..16 {
        let progress = f.engine.encode(at(1), WorkBudget { bytes: 1, items: 1 });
        assert_eq!(progress.items, 1);
        assert_eq!(
            f.engine.status().terminal,
            0,
            "payload owners drain before record credits and events"
        );
    }
    for expected in 1..=16 {
        let progress = f.engine.encode(at(1), WorkBudget { bytes: 1, items: 1 });
        assert_eq!(progress.items, 1);
        assert_eq!(f.engine.status().terminal, expected);
    }
    assert_eq!(
        credits.snapshot()[Resource::InputBytes as usize].held,
        metadata_bytes
    );
    assert!(!f.engine.has_terminal_work());
    let events = f.events();
    assert_eq!(deliveries(&events).len(), 16);
    assert!(
        deliveries(&events)
            .iter()
            .all(|event| event.outcome.kind == DeliveryKind::NotWritten)
    );
}

#[test]
fn split_header_is_charged_as_metadata_for_long_client_without_coalescing() {
    let mut cfg = config();
    cfg.client_id = "c".repeat(32767);
    cfg.coalesce_below_bytes = 0;
    cfg.request_hard_bytes = 65536;
    cfg.request_target_bytes = 65536;
    cfg.connection_wire_window_bytes = 65536;
    let mut f = Fixture::new(cfg, 1);
    f.submit(1, 0, 0);
    let request = f.dispatch(1);
    let credits = f.engine.credits();
    assert_eq!(request.plan.coalesced_bytes(), 0);
    assert!(request.plan.metadata_bytes() >= 32767 + kr_kafka_record::BATCH_HEADER_BYTES);
    assert_eq!(
        credits.snapshot()[Resource::RequestMetadata as usize].held,
        request.plan.metadata_bytes()
    );
    assert!(
        request.plan.segments().len() <= 7,
        "actual one-partition bound"
    );
    let metadata = request.plan.segments()[0].clone();
    let held = credits.snapshot()[Resource::RequestMetadata as usize].held;
    let events = f.answer(request, 1_000_000, FaultPlan::default());
    assert_eq!(deliveries(&events).len(), 1);
    assert_eq!(
        credits.snapshot()[Resource::RequestMetadata as usize].held,
        held,
        "provider view retains copied header allocation and its credit"
    );
    drop(metadata);
    assert_eq!(
        credits.snapshot()[Resource::RequestMetadata as usize].held,
        0
    );
}

#[test]
fn provider_final_payload_release_wakes_budgeted_pool_reaping_without_busy_polling() {
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };
    struct Wake(AtomicUsize);
    impl std::task::Wake for Wake {
        fn wake(self: Arc<Self>) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
        fn wake_by_ref(self: &Arc<Self>) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }
    let mut cfg = config();
    cfg.coalesce_below_bytes = 0;
    let mut f = Fixture::new(cfg, 1);
    f.submit(1, 0, 0);
    let request = f.dispatch(1);
    let provider_payload = request.plan.segments()[1].clone();
    let wake = Arc::new(Wake(AtomicUsize::new(0)));
    f.engine
        .register_reclaim_waker(&std::task::Waker::from(wake.clone()));
    assert_eq!(
        deliveries(&f.answer(request, 1_000_000, FaultPlan::default())).len(),
        1
    );
    assert!(
        !f.engine.has_maintenance_work(),
        "provider-held references are not ready owner work"
    );
    assert_eq!(wake.0.load(Ordering::SeqCst), 0);
    std::thread::spawn(move || drop(provider_payload))
        .join()
        .unwrap();
    assert_eq!(wake.0.load(Ordering::SeqCst), 1);
    assert!(f.engine.has_maintenance_work());
    assert_eq!(
        f.engine
            .on_deadline(at(1_000_001), WorkBudget { bytes: 0, items: 0 })
            .items,
        0
    );
    assert!(
        f.engine.has_maintenance_work(),
        "zero work does not reclaim an output reservation"
    );
    let progress = f
        .engine
        .on_deadline(at(1_000_001), WorkBudget { bytes: 1, items: 1 });
    assert_eq!(progress.items, 1);
    assert!(!f.engine.has_maintenance_work());
}

#[test]
fn unordered_response_offset_checks_use_matching_batch_counts_before_any_ack() {
    use kr_kafka_protocol::{
        Request, Response, produce_request,
        produce_response::{self as api, v13::*},
    };
    for overflow in [false, true] {
        let mut c = config();
        c.batch_target_bytes = 4096;
        let mut f = Fixture::new(c, 2);
        // Different record counts make accidentally pairing raw wire order
        // with batch order observable at the signed-offset boundary.
        f.submit(2, 0, 0);
        f.submit(3, 1, 0);
        f.engine.encode(at(1), budget());
        f.engine.flush(at(1), RecordToken(5)).unwrap();
        let request = f.dispatch(2);
        let request_bytes = request.bytes();
        let Request::ProduceRequest(produce_request::View::V13(body)) =
            decode_request(&request_bytes, DecodeLimits::default())
                .unwrap()
                .body
        else {
            panic!("expected Produce13 request")
        };
        assert_eq!(body.topic_data.len(), 1);
        let topic = body.topic_data.iter().next().unwrap().unwrap();
        let expected: Vec<_> = topic
            .partition_data
            .iter()
            .map(|row| row.unwrap().index)
            .collect();
        assert_eq!(
            expected.len(),
            2,
            "both immutable batches must share this request"
        );
        assert_ne!(expected[0], expected[1]);
        let response_rows: Vec<_> = expected
            .iter()
            .enumerate()
            .rev()
            .map(|(position, &partition)| {
                let records = i64::from(partition + 2);
                PartitionProduceResponse {
                    index: partition,
                    base_offset: if overflow && position == expected.len() - 1 {
                        i64::MAX
                    } else {
                        i64::MAX - (records - 1)
                    },
                    ..Default::default()
                }
            })
            .collect();
        let topic_rows = [TopicProduceResponse {
            topic_id: f.id.0,
            partition_responses: response_rows.as_slice().into(),
            ..Default::default()
        }];
        let frame = Response::ProduceResponse(api::View::V13(ProduceResponse {
            responses: topic_rows.as_slice().into(),
            ..Default::default()
        }))
        .plan_frame(13, request.correlation, Default::default())
        .unwrap()
        .to_vec()
        .unwrap();
        f.engine.on_write_admitted(request.request).unwrap();
        f.engine
            .on_write(
                request.connection,
                request.correlation,
                request.plan.len(),
                Certainty::Applied,
            )
            .unwrap();
        let before = f.engine.status();
        let result = f.engine.on_frame(request.connection, at(3), &frame);
        if overflow {
            assert!(matches!(
                result,
                Err(kr_kafka_producer::engine::EngineError::InvalidState(
                    "response offset overflow"
                ))
            ));
            let after = f.engine.status();
            assert_eq!(
                after.terminal, before.terminal,
                "the earlier valid row cannot acknowledge records"
            );
            assert_eq!(
                after.requests, before.requests,
                "provider/request ownership survives rejection"
            );
            assert_eq!(after.batches, before.batches);
            assert_eq!(
                f.engine
                    .request_for(request.connection, request.correlation),
                Some(request.request)
            );
            while let Some(event) = f.engine.pop_event() {
                assert!(!matches!(event.event, Event::Delivery(_)));
            }
        } else {
            result.unwrap();
            let events = f.events();
            let rows = deliveries(&events);
            assert_eq!(rows.len(), 5);
            for partition in 0..2 {
                let offsets: Vec<_> = rows
                    .iter()
                    .filter(|row| row.partition.partition == partition)
                    .map(|row| {
                        assert_eq!(row.outcome.kind, DeliveryKind::Acked);
                        row.base_offset.get().unwrap()
                    })
                    .collect();
                assert_eq!(
                    offsets,
                    (i64::MAX - i64::from(partition + 1)..=i64::MAX).collect::<Vec<_>>()
                );
            }
            assert_eq!(f.engine.status().terminal, 5);
        }
    }
}
