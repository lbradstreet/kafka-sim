use super::*;
use crate::{
    admission::Admission,
    control::{MetadataPartition, MetadataTopic},
};
use kr_kafka_broker_model::{BrokerAction, BrokerConfig, BrokerModel};

fn at(n: u64) -> RuntimeInstant {
    RuntimeInstant::from_nanos(n)
}
fn budget(items: u32) -> WorkBudget {
    WorkBudget {
        bytes: 65536,
        items,
    }
}

fn setup(partitions: usize) -> (ProducerEngine, BrokerModel) {
    let config = ProducerConfig {
        metrics: crate::telemetry::metrics::MetricsConfig {
            max_broker_scopes: 2,
            max_partition_scopes: 4,
            significant_digits: 2,
            ..Default::default()
        },
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

fn take(engine: &mut ProducerEngine, now: u64) -> crate::telemetry::metrics::MetricsSnapshot {
    let reader = engine.metrics();
    reader.request_snapshot().unwrap();
    assert!(engine.publish_metrics(at(now)));
    reader.try_take_snapshot().unwrap()
}

#[test]
fn actual_pipeline_records_each_stage_once_and_separates_request_depths() {
    use crate::telemetry::metrics::Scope;
    let (mut engine, mut broker) = setup(1);
    let (connection, request, correlation, plan) = dispatch(&mut engine);
    let wire: Vec<_> = plan
        .segments()
        .iter()
        .flat_map(|s| s.as_slice())
        .copied()
        .collect();
    let BrokerAction::Reply(reply) = broker.handle_frame(0, &wire, Default::default()).unwrap()
    else {
        panic!("expected response");
    };
    engine.on_write_admitted(request).unwrap();
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
    engine.on_frame(connection, at(200), &reply).unwrap();
    for _ in 0..8 {
        engine.encode(at(200), budget(64));
    }
    assert!(engine.on_frame(connection, at(200), &reply).is_err());
    let snapshot = take(&mut engine, 200);
    let global = |m| snapshot.distribution(Scope::Global, m).unwrap();
    for metric in [
        Metric::QueueWaitNanos,
        Metric::BatchFillNanos,
        Metric::BatchRawBytes,
        Metric::BatchWireBytes,
        Metric::RecordsPerBatch,
        Metric::DeliveryAckedNanos,
        Metric::ProduceRttNanos,
    ] {
        assert_eq!(global(metric).count(), 1, "metric={metric:?}");
    }
    assert_eq!(global(Metric::ProduceRttNanos).exact_max(), Some(100));
    assert_eq!(global(Metric::DeliveryAckedNanos).exact_max(), Some(200));
    assert_eq!(global(Metric::InFlightRequests).count(), 2);
    assert_eq!(global(Metric::InFlightRequests).exact_max(), Some(1));
    assert_eq!(global(Metric::InFlightWireBytes).count(), 2);
    assert_eq!(
        global(Metric::InFlightWireBytes).exact_max(),
        Some(plan.len() as u64)
    );
    assert_eq!(
        snapshot
            .distribution(Scope::Broker(0), Metric::ProduceRttNanos)
            .unwrap()
            .count(),
        1
    );
    assert_eq!(
        snapshot
            .distribution(Scope::Broker(0), Metric::InFlightRequests)
            .unwrap()
            .count(),
        2
    );
    assert_eq!(snapshot.invalid_time_samples(), 0);
    assert_eq!(snapshot.invalid_depth_samples(), 0);
}

#[test]
fn seal_timestamp_survives_delayed_observation_and_repeated_visits() {
    use crate::telemetry::metrics::Scope;
    let (mut engine, _) = setup(1);
    let key = engine.batches.iter().next().unwrap().0;
    engine
        .batches
        .get_mut(key)
        .unwrap()
        .seal_at(SealReason::Flush, at(10));
    engine.observe_metrics_time(at(100));
    engine.metrics_batch(key);
    engine
        .batches
        .get_mut(key)
        .unwrap()
        .seal_at(SealReason::Deadline, at(200));
    engine.metrics_batch(key);
    let snapshot = take(&mut engine, 200);
    let fill = snapshot
        .distribution(Scope::Global, Metric::BatchFillNanos)
        .unwrap();
    assert_eq!((fill.count(), fill.exact_max()), (1, Some(10)));
    assert_eq!(snapshot.invalid_time_samples(), 0);
}

#[test]
fn missing_terminal_time_is_visible_and_explicit_failure_time_is_used() {
    use crate::telemetry::metrics::Scope;
    let mut empty = ProducerEngine::new(ProducerConfig::default(), None).unwrap();
    empty.metrics_delivery(
        TopicPartition {
            topic: TopicId::ZERO,
            partition: -1,
        },
        at(0),
        DeliveryKind::NotWritten,
    );
    let snapshot = take(&mut empty, 0);
    assert_eq!(snapshot.missing_time_samples(), 1);
    assert_eq!(
        snapshot
            .distribution(Scope::Global, Metric::DeliveryNotWrittenNanos)
            .unwrap()
            .count(),
        0
    );
    let (mut engine, _) = setup(1);
    engine.fail_producer_at(at(50), FailureReason::RuntimeFailed);
    for _ in 0..16 {
        engine.on_deadline(at(50), budget(64));
        engine.encode(at(50), budget(64));
    }
    let snapshot = take(&mut engine, 50);
    let failed = snapshot
        .distribution(Scope::Global, Metric::DeliveryNotWrittenNanos)
        .unwrap();
    assert_eq!((failed.count(), failed.exact_max()), (1, Some(50)));
}
