use super::*;
use crate::{
    admission::Admission,
    control::{MetadataPartition, MetadataTopic},
};
use kr_kafka_broker_model::{BrokerAction, BrokerConfig, BrokerModel, FaultPlan};

fn at(n: u64) -> RuntimeInstant {
    RuntimeInstant::from_nanos(n)
}
fn ns(n: u64) -> RuntimeDuration {
    RuntimeDuration::from_nanos(n)
}
fn budget(items: u32) -> WorkBudget {
    WorkBudget {
        bytes: 65536,
        items,
    }
}

fn setup(partitions: usize) -> (ProducerEngine, BrokerModel) {
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
        compressed_bytes: 65536,
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
        Some(ProducerIdentity {
            producer_id: 7,
            epoch: 0,
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
    engine.encode(at(0), budget(64));
    assert_eq!(engine.batches.len(), partitions);
    (engine, broker)
}

fn dispatch(engine: &mut ProducerEngine) -> (ConnectionKey, RequestKey, i32, OwnedSendPlan) {
    engine.flush(at(0), engine.tracker.accepted()).unwrap();
    for _ in 0..64 {
        engine.on_deadline(at(0), budget(64));
        engine.encode(at(0), budget(64));
        engine.schedule(at(0), budget(64));
        while let Some(order) = engine.pop_order() {
            match order {
                EngineOrder::Connect { key, .. } => engine
                    .on_connection(key, at(0), ConnectionEvent::Active)
                    .unwrap(),
                EngineOrder::Dispatch {
                    connection,
                    request,
                    correlation,
                    plan,
                    ..
                } => return (connection, request, correlation, plan),
                EngineOrder::Metadata { .. } => {}
                other => panic!("unexpected order {other:?}"),
            }
        }
    }
    panic!("no dispatch: {:?}", engine.status());
}

#[test]
fn existing_batch_refresh_is_budgeted_and_samples_behind_cursor_request_one_more_sweep() {
    let (mut engine, _) = setup(3);
    let keys: Vec<_> = engine.batches.iter().map(|(key, _)| key).collect();
    let original = engine.config.request_timeout;
    assert!(
        keys.iter()
            .all(|key| engine.batches.get(*key).unwrap().deadline_headroom() == original)
    );
    engine
        .brokers
        .get_mut(&0)
        .unwrap()
        .round_trip
        .observe(ns(80));
    engine.invalidate_headroom();
    let before = engine.deadlines.ordered.clone();
    assert_eq!(engine.on_deadline(at(0), budget(0)).items, 0);
    assert_eq!(engine.deadlines.ordered, before);
    assert_eq!(engine.on_deadline(at(0), budget(1)).items, 1);
    assert_eq!(
        keys.iter()
            .filter(|key| engine.batches.get(**key).unwrap().deadline_headroom() == ns(80))
            .count(),
        1
    );
    engine
        .brokers
        .get_mut(&0)
        .unwrap()
        .round_trip
        .observe(ns(160));
    engine.invalidate_headroom();
    let latest = ns(90);
    let mut turns = 0;
    while engine.headroom_sweep.is_some() {
        turns += 1;
        assert!(turns <= 8, "bounded frontier failed to finish");
        assert!(engine.on_deadline(at(0), budget(1)).items <= 1);
    }
    for key in keys {
        let batch = engine.batches.get(key).unwrap();
        assert_eq!(batch.deadline_headroom(), latest);
        let deadline = batch.oldest_deadline().unwrap().as_nanos() - latest.as_nanos();
        assert_eq!(
            engine.deadlines.current[&DeadlineKey::Batch(key.packed())],
            at(deadline)
        );
    }
    assert!(!engine.has_maintenance_work());
    assert!(engine.next_deadline().unwrap() > at(0));
}

#[test]
fn rtt_requires_full_confirmed_send_and_valid_matching_frame_and_never_restarts() {
    for mode in 0..5 {
        let (mut engine, mut broker) = setup(1);
        let (connection, request, correlation, plan) = dispatch(&mut engine);
        let frame: Vec<_> = plan
            .segments()
            .iter()
            .flat_map(|s| s.as_slice())
            .copied()
            .collect();
        let BrokerAction::Reply(mut reply) = broker
            .handle_frame(0, &frame, FaultPlan::default())
            .unwrap()
        else {
            panic!("broker response")
        };
        engine.on_write_admitted(request).unwrap();
        match mode {
            0 => engine
                .on_write(
                    connection,
                    correlation,
                    plan.len(),
                    CompletionCertainty::Applied,
                )
                .unwrap(),
            1 => engine
                .on_write_at(
                    at(100),
                    connection,
                    correlation,
                    plan.len() - 1,
                    CompletionCertainty::Applied,
                )
                .unwrap(),
            _ => {
                engine
                    .on_write_at(
                        at(100),
                        connection,
                        correlation,
                        plan.len(),
                        CompletionCertainty::Applied,
                    )
                    .unwrap();
                engine
                    .on_write_at(
                        at(190),
                        connection,
                        correlation,
                        plan.len(),
                        CompletionCertainty::Applied,
                    )
                    .unwrap();
            }
        }
        assert_eq!(
            engine.brokers[&0].round_trip.estimate(),
            None,
            "socket writes do not train RTT"
        );
        if mode == 3 {
            reply[4..8].copy_from_slice(&(correlation + 1).to_be_bytes());
        }
        let received = if mode == 4 { at(50) } else { at(200) };
        let result = engine.on_frame(connection, received, &reply);
        if mode == 3 {
            assert!(result.is_err());
        } else {
            result.unwrap();
        }
        let expected = if mode == 2 { Some(ns(100)) } else { None };
        assert_eq!(
            engine.brokers[&0].round_trip.estimate(),
            expected,
            "mode={mode}"
        );
        assert!(engine.on_frame(connection, at(1_000), &reply).is_err());
        assert_eq!(
            engine.brokers[&0].round_trip.estimate(),
            expected,
            "stale response must not retrain"
        );
    }
}

#[test]
fn timing_invocations_are_consumed_once_and_aborted_end_work_does_not_train_next_seal() {
    let (mut engine, _) = setup(1);
    assert!(engine.last_encode_work().is_empty());
    let before = engine.encoding_cost;
    engine.observe_encode_elapsed(ns(u64::MAX)).unwrap();
    assert_eq!(
        engine.encoding_cost, before,
        "idle traversal is not a sample"
    );
    engine.last_encode_work = EncodeWork {
        seal_calls: 1,
        ..EncodeWork::default()
    };
    engine.observe_encode_elapsed(ns(1000)).unwrap();
    engine.abort_seal_timing();
    engine.last_encode_aborted = false;
    engine.last_encode_work = EncodeWork {
        seal_calls: 1,
        seals_completed: 1,
        ..EncodeWork::default()
    };
    engine.observe_encode_elapsed(ns(10)).unwrap();
    assert_eq!(engine.encoding_cost.estimate(0), ns(10));
    let learned = engine.encoding_cost;
    engine.observe_encode_elapsed(ns(9999)).unwrap();
    assert_eq!(engine.encoding_cost, learned);
}

#[test]
fn continuous_estimate_updates_do_not_starve_flush_fences_with_one_item_quota() {
    let (mut engine, _) = setup(3);
    let token = engine.flush(at(0), RecordToken(0)).unwrap();
    let mut published = false;
    for turn in 0..16 {
        engine
            .brokers
            .get_mut(&0)
            .unwrap()
            .round_trip
            .observe(ns(100 + turn * 100));
        engine.invalidate_headroom();
        assert!(engine.on_deadline(at(0), budget(1)).items <= 1);
        while let Some(event) = engine.pop_event() {
            if event.event == (Event::FlushDone { token }) {
                published = true;
            }
        }
        if published {
            break;
        }
    }
    assert!(
        published,
        "repeated headroom invalidation starved a ready flush watermark"
    );
}

#[test]
fn modeled_seal_completion_gates_only_new_output_and_arms_its_exact_deadline() {
    let (mut engine, _) = setup(2);
    let keys: Vec<_> = engine.batches.iter().map(|(key, _)| key).collect();
    engine
        .batches
        .get_mut(keys[0])
        .unwrap()
        .seal(SealReason::Flush);
    engine.refresh_batch_deadline(keys[0], at(0));
    engine.encode(at(0), budget(64));
    assert_eq!(
        engine.batches.get(keys[0]).unwrap().state(),
        BatchState::Sealed
    );
    engine
        .batches
        .get_mut(keys[1])
        .unwrap()
        .seal(SealReason::Flush);
    engine.refresh_batch_deadline(keys[1], at(10));
    engine.encode_with_completion_delay(at(10), budget(64), ns(1_000));
    assert_eq!(
        engine.batches.get(keys[0]).unwrap().dispatch_ready_at(),
        at(0)
    );
    assert_eq!(
        engine.batches.get(keys[1]).unwrap().dispatch_ready_at(),
        at(1_010)
    );
    assert_eq!(
        engine.deadlines.current[&DeadlineKey::Batch(keys[1].packed())],
        at(1_010)
    );
    let mut first_request = None;
    for _ in 0..3 {
        engine.schedule(at(10), budget(64));
        while let Some(order) = engine.pop_order() {
            match order {
                EngineOrder::Connect { key, .. } => engine
                    .on_connection(key, at(10), ConnectionEvent::Active)
                    .unwrap(),
                EngineOrder::Dispatch { request, .. } => {
                    let state = engine.requests.get(Slot::from_packed(request.0)).unwrap();
                    assert_eq!(
                        state.batches,
                        vec![keys[0]],
                        "new delayed output must not join older eligible output"
                    );
                    first_request = Some(request);
                }
                EngineOrder::Metadata { .. } => {}
                other => panic!("unexpected order {other:?}"),
            }
        }
    }
    let first = first_request.expect("old ready batch should dispatch immediately");
    engine.on_write_admitted(first).unwrap();
    let request = engine.requests.get(Slot::from_packed(first.0)).unwrap();
    engine
        .on_write(
            request.connection,
            request.correlation,
            request.bytes,
            CompletionCertainty::Applied,
        )
        .unwrap();
    assert!(!engine.schedule(at(1_009), budget(64)).remaining_immediate);
    assert!(engine.pop_order().is_none());
    engine.on_deadline(at(1_010), budget(64));
    engine.schedule(at(1_010), budget(64));
    let Some(EngineOrder::Dispatch { request, .. }) = engine.pop_order() else {
        panic!("completion deadline must release delayed output")
    };
    assert_eq!(
        engine
            .requests
            .get(Slot::from_packed(request.0))
            .unwrap()
            .batches,
        vec![keys[1]]
    );
}

#[test]
fn delivery_deadline_can_expire_while_modeled_output_is_not_yet_visible() {
    let (mut engine, _) = setup(1);
    let key = engine.batches.iter().next().unwrap().0;
    let deadline = engine.batches.get(key).unwrap().oldest_deadline().unwrap();
    engine.batches.get_mut(key).unwrap().seal(SealReason::Flush);
    engine.refresh_batch_deadline(key, at(0));
    engine.encode_with_completion_delay(at(0), budget(64), ns(deadline.as_nanos() + 1));
    assert_eq!(
        engine.deadlines.current[&DeadlineKey::Batch(key.packed())],
        deadline
    );
    engine.schedule(at(0), budget(64));
    while let Some(order) = engine.pop_order() {
        assert!(!matches!(order, EngineOrder::Dispatch { .. }));
    }
    engine.on_deadline(deadline, budget(64));
    let mut delivery = None;
    for _ in 0..16 {
        engine.encode(deadline, budget(64));
        while let Some(event) = engine.pop_event() {
            if let Event::Delivery(event) = event.event {
                delivery = Some(event);
            }
        }
    }
    let event = delivery.expect("deadline produces one terminal delivery");
    assert_eq!(
        event.outcome,
        DeliveryOutcome::not_written(FailureReason::Deadline)
    );
    assert_eq!(event.attempts, 0);
    assert_eq!(engine.status().terminal, 1);
}
