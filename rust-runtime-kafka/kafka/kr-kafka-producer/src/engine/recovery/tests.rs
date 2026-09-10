use super::*;
use crate::{
    admission::Admission,
    control::{MetadataPartition, MetadataTopic},
};
use kr_kafka_broker_model::{BrokerConfig, BrokerModel};

fn at(n: u64) -> RuntimeInstant {
    RuntimeInstant::from_nanos(n)
}
fn budget(items: u32) -> WorkBudget {
    WorkBudget {
        bytes: 65536,
        items,
    }
}

fn setup(partitions: usize, initial: bool) -> (ProducerEngine, BrokerModel) {
    setup_epoch(partitions, initial, 0)
}
fn setup_epoch(partitions: usize, initial: bool, epoch: i16) -> (ProducerEngine, BrokerModel) {
    let config = ProducerConfig {
        compression: Compression::None,
        codec_contexts: 0,
        record_descriptors: 32,
        delivery_event_capacity: 32,
        pending_records_per_topic: 32,
        max_batches: 32,
        max_open_topics: 2,
        max_live_leases: 4,
        release_event_capacity: 4,
        input_bytes: 65536,
        compressed_bytes: 1024 * 1024,
        batch_target_bytes: 4096,
        batch_hard_bytes: 4096,
        progressive_threshold: 32,
        output_chunk_bytes: 4096,
        request_target_bytes: 8192,
        request_hard_bytes: 8192,
        ..ProducerConfig::default()
    };
    let mut engine = ProducerEngine::new(
        config.clone(),
        initial.then_some(ProducerIdentity {
            producer_id: 7,
            epoch,
        }),
    )
    .unwrap();
    let mut broker = BrokerModel::new(BrokerConfig::default()).unwrap();
    broker
        .add_broker(kr_kafka_broker_model::BrokerEndpoint {
            id: 0,
            host: "broker".into(),
            port: 9092,
        })
        .unwrap();
    if partitions == 0 {
        return (engine, broker);
    }
    let id = TopicId(broker.create_topic("test", &vec![0; partitions]).unwrap());
    let topic = engine.open_topic("test", at(0)).unwrap();
    engine
        .apply_metadata(
            at(0),
            &[topic],
            MetadataUpdate {
                throttle_ms: 0,
                cluster_id: None,
                controller_id: 0,
                brokers: vec![BrokerNode {
                    id: 0,
                    host: "broker".into(),
                    port: 9092,
                    rack: None,
                }],
                topics: vec![MetadataTopic {
                    requested_index: 0,
                    id,
                    name: Some("test".into()),
                    error_code: 0,
                    partitions: (0..partitions)
                        .map(|p| MetadataPartition {
                            replicas: Vec::new(),
                            isr: Vec::new(),
                            offline: Vec::new(),
                            index: p as i32,
                            error_code: 0,
                            metadata: PartitionMetadata {
                                leader: 0,
                                leader_epoch: 0,
                            },
                        })
                        .collect(),
                }],
            },
        )
        .unwrap();
    let mut admission = Admission::new(
        &config,
        engine.credits(),
        engine.validated.effective_batch_payload_bytes,
    );
    let records: Vec<_> = (0..partitions)
        .map(|p| RecordDescriptor {
            topic,
            partition_hint: Some(p as i32),
            lane_hint: None,
            key: None,
            value: Some(b"a"),
            headers: &[],
            timestamp_ms: 0,
            user_token: p as u64,
            delivery_timeout: None,
        })
        .collect();
    let (_, batch) = admission.prepare_copy(at(0), &records, &vec![Ok(0); partitions]);
    engine
        .admit(
            at(0),
            batch.unwrap(),
            &(0..partitions)
                .map(|p| PartitionChoice::Partition(p as i32))
                .collect::<Vec<_>>(),
        )
        .unwrap();
    for _ in 0..64 {
        engine.on_deadline(at(0), budget(64));
        engine.encode(at(0), budget(64));
    }
    assert_eq!(engine.batches.len(), partitions);
    (engine, broker)
}

fn refresh(engine: &mut ProducerEngine) {
    engine
        .ledger
        .as_mut()
        .unwrap()
        .request_identity_refresh()
        .unwrap();
    engine.refresh_recovery(at(0));
}
fn fresh() -> ProducerIdentity {
    ProducerIdentity {
        producer_id: 8,
        epoch: 0,
    }
}

#[test]
fn initial_identity_installs_without_registering_queued_topology() {
    let (mut engine, _) = setup(16, false);
    assert_eq!(engine.partitions.len(), 16);
    engine.install_identity(fresh()).unwrap();
    assert_eq!(engine.ledger.as_ref().unwrap().stats().partitions, 0);
    assert!(!engine.has_identity_work());
    engine.flush(at(0), engine.tracker.accepted()).unwrap();
    let mut dispatched = false;
    for _ in 0..64 {
        engine.on_deadline(at(0), budget(64));
        engine.encode(at(0), budget(64));
        engine.schedule(at(0), budget(64));
        while let Some(order) = engine.pop_order() {
            match order {
                EngineOrder::Connect { key, .. } => engine
                    .on_connection(key, at(0), ConnectionEvent::Active)
                    .unwrap(),
                EngineOrder::Dispatch { .. } => dispatched = true,
                _ => {}
            }
        }
        if dispatched {
            break;
        }
    }
    assert!(dispatched, "lazily registered partitions did not dispatch");
    assert!(engine.ledger.as_ref().unwrap().stats().partitions > 0);
}

#[test]
fn local_and_returned_identity_refinalize_only_one_batch_per_maintenance_item() {
    for epoch in [0, i16::MAX] {
        let (mut engine, _) = setup_epoch(16, true, epoch);
        engine.flush(at(0), engine.tracker.accepted()).unwrap();
        for _ in 0..64 {
            engine.on_deadline(at(0), budget(64));
            engine.encode(at(0), budget(64));
        }
        let keys: Vec<_> = engine.batches.iter().map(|(key, _)| key).collect();
        for &key in &keys {
            let batch = engine.batches.get_mut(key).unwrap();
            let assignment = engine
                .ledger
                .as_mut()
                .unwrap()
                .assign(batch.partition(), key.packed(), batch.record_count() as u32)
                .unwrap();
            batch
                .finalize(kr_kafka_record::Identity {
                    producer_id: assignment.identity.producer_id,
                    producer_epoch: assignment.identity.epoch,
                    base_sequence: assignment.base_sequence.get(),
                })
                .unwrap();
        }
        refresh(&mut engine);
        let expected = if epoch == i16::MAX {
            while engine.identity_step() {}
            engine.install_identity(fresh()).unwrap();
            fresh()
        } else {
            ProducerIdentity {
                producer_id: 7,
                epoch: 1,
            }
        };
        assert_eq!(engine.on_deadline(at(0), budget(0)).items, 0);
        assert_eq!(engine.status().identity.unwrap().producer_id, 7);
        assert_eq!(engine.on_deadline(at(0), budget(1)).items, 1);
        assert_eq!(
            keys.iter()
                .filter(|&&key| {
                    let partition = engine.batches.get(key).unwrap().partition();
                    engine
                        .ledger
                        .as_ref()
                        .unwrap()
                        .assignment(partition, key.packed())
                        .unwrap()
                        .identity
                        == expected
                })
                .count(),
            1
        );
        assert_eq!(engine.schedule(at(0), budget(64)).items, 0);
        engine.cancel(at(0), RecordToken(1)).unwrap();
        for _ in 0..128 {
            let progress = engine.on_deadline(at(0), budget(1));
            assert!(progress.items <= 1);
            if !engine.has_identity_work() {
                break;
            }
        }
        assert!(!engine.has_identity_work());
        assert_eq!(engine.status().identity, Some(expected));
        assert!(!engine.identity_pending);
        assert!(!engine.is_failed());
        let mut delivered = Vec::new();
        for _ in 0..128 {
            if let Some(event) = engine.pop_event() {
                if let Event::Delivery(delivery) = event.event {
                    delivered.push((delivery.token, delivery.outcome));
                }
            } else if !engine.has_terminal_work() {
                break;
            }
        }
        assert_eq!(delivered.len(), 1);
        assert_eq!(delivered[0].0, RecordToken(1));
        assert_eq!(delivered[0].1.kind, DeliveryKind::NotWritten);
    }
}
#[test]
fn connection_retirement_is_budgeted_and_identity_waits_for_actual_release() {
    let (mut engine, _) = setup_epoch(0, true, i16::MAX);
    let mut keys = Vec::new();
    for broker in 0..5 {
        let credit = Arc::new(engine.credits.reserve(&[]).unwrap());
        let slot = engine
            .connections
            .insert(ConnectionState {
                retry_scheduled: false,
                broker,
                lane: 0,
                phase: ConnectionPhase::Active,
                fifo: VecDeque::new(),
                pending_orders: [None; 3],
                writing: None,
                bytes: 0,
                credit,
            })
            .map_err(|failure| failure.error)
            .unwrap();
        keys.push(ConnectionKey(slot.packed()));
    }
    // Released slots still cost one cursor visit instead of disappearing from
    // the poll budget; remaining drivers model independently retained providers.
    engine
        .on_connection(keys[1], at(0), ConnectionEvent::Released)
        .unwrap();
    engine
        .on_connection(keys[3], at(0), ConnectionEvent::Released)
        .unwrap();
    while engine.pop_order().is_some() {}
    refresh(&mut engine);
    assert_eq!(
        engine
            .connections
            .iter()
            .filter(|(_, c)| c.phase == ConnectionPhase::Retiring)
            .count(),
        0
    );
    assert_eq!(engine.on_deadline(at(0), budget(0)).items, 0);
    let mut retire_orders = Vec::new();
    for _ in 0..engine.connections.allocated_slots() + 1 {
        assert_eq!(engine.on_deadline(at(0), budget(1)).items, 1);
        let mut count = 0;
        while let Some(order) = engine.pop_order() {
            match order {
                EngineOrder::Retire { connection, .. } => {
                    count += 1;
                    retire_orders.push(connection);
                }
                EngineOrder::InitProducerId { .. } => {
                    panic!("identity requested before old providers released")
                }
                _ => {}
            }
        }
        assert!(count <= 1);
    }
    assert_eq!(retire_orders.len(), 3);
    assert!(
        !engine.has_identity_work(),
        "provider-held connections must park"
    );
    assert!(!engine.identity_step());
    for (index, key) in retire_orders.into_iter().enumerate() {
        engine
            .on_connection(key, at(0), ConnectionEvent::Released)
            .unwrap();
        let orders: Vec<_> = std::iter::from_fn(|| engine.pop_order()).collect();
        assert_eq!(
            orders
                .iter()
                .filter(|o| matches!(o, EngineOrder::InitProducerId { .. }))
                .count(),
            usize::from(index == 2)
        );
    }
    assert!(!engine.identity_step());
    assert!(engine.pop_order().is_none());
}

#[test]
fn local_bump_retains_idle_connections_and_fatal_reason_survives() {
    let (mut engine, _) = setup(0, true);
    let slot = engine
        .connections
        .insert(ConnectionState {
            retry_scheduled: false,
            broker: 0,
            lane: 0,
            phase: ConnectionPhase::Active,
            fifo: VecDeque::new(),
            pending_orders: [None; 3],
            writing: None,
            bytes: 0,
            credit: Arc::new(engine.credits.reserve(&[]).unwrap()),
        })
        .map_err(|failure| failure.error)
        .unwrap();
    let key = ConnectionKey(slot.packed());
    refresh(&mut engine);
    assert!(
        engine.install_identity(fresh()).is_err(),
        "broker installation still requires connection release"
    );
    for _ in 0..16 {
        assert!(engine.on_deadline(at(0), budget(1)).items <= 1);
    }
    assert_eq!(
        engine.status().identity,
        Some(ProducerIdentity {
            producer_id: 7,
            epoch: 1
        })
    );
    assert_eq!(
        engine.connections.get(slot).unwrap().phase,
        ConnectionPhase::Active
    );
    assert!(engine.pop_order().is_none());
    assert!(engine.connections.get(Slot::from_packed(key.0)).is_some());
    let change = engine
        .ledger
        .as_mut()
        .unwrap()
        .fail_closed(FailureReason::ProtocolViolation);
    engine.apply_change(change);
    assert!(engine.is_failed());
    assert!(
        matches!(engine.pop_event().unwrap().event, Event::Fatal { code } if code == FailureReason::ProtocolViolation as u32)
    );
    let ledger = engine.ledger.as_mut().unwrap();
    assert_eq!(
        ledger.request_identity_refresh(),
        Err(LedgerError::FailedClosed)
    );
    assert_eq!(
        ledger.failure_reason(),
        Some(FailureReason::ProtocolViolation)
    );
}

#[test]
fn repeated_unknowns_count_past_u32_without_failing_the_producer() {
    let (mut engine, _) = setup(2, true);
    let (connection, request, _, plan) =
        super::super::topic_settlement::tests::dispatch(&mut engine);
    engine.on_write_admitted(request).unwrap();
    // Model a long-running recovered producer immediately below the old limit.
    engine.unknown = u64::from(u32::MAX) - 1;
    let now = at(1_000_001);
    engine.cancel(now, RecordToken(1)).unwrap();
    engine.cancel(now, RecordToken(2)).unwrap();
    let mut unknown = 0;
    for _ in 0..256 {
        engine.on_deadline(now, budget(1));
        while let Some(event) = engine.pop_event() {
            if let Event::Delivery(delivery) = event.event {
                assert_eq!(delivery.outcome.kind, DeliveryKind::Unknown);
                unknown += 1;
            }
        }
    }
    assert_eq!(unknown, 2);
    assert_eq!(engine.status().unknown, u64::from(u32::MAX) + 1);
    assert!(!engine.is_failed());
    drop(plan);
    engine
        .on_connection(connection, now, ConnectionEvent::Released)
        .unwrap();
    engine.close(now, now, RecordToken(2)).unwrap();
    let mut closed = false;
    for _ in 0..512 {
        engine.on_deadline(now, budget(1));
        engine.encode(now, budget(1));
        while let Some(event) = engine.pop_event() {
            if let Event::Closed { unresolved } = event.event {
                assert_eq!(unresolved, u32::MAX);
                closed = true;
            }
        }
    }
    assert!(closed);
}
