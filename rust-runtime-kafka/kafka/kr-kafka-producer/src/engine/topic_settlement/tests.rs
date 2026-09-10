use super::*;
use crate::admission::Admission;
use crate::control::{MetadataPartition, MetadataTopic, MetadataUpdate};
use crate::topic::PartitionMetadata;
use crate::transport::{ConnectionDriver, DriverConfig, DriverEvent, SendRequest, WriteMode};
use kr_runtime::SimRuntime;
use kr_runtime_io::network::{
    MemoryNetwork, MemoryNetworkConfig, NetworkConfig, NetworkOperationKind, NodeId, ScriptedFault,
    SimNetwork,
};
use std::future::poll_fn;
use std::task::{Context, Poll, Waker};

pub(in crate::engine) fn setup(descriptors: u32) -> (ProducerEngine, Admission) {
    let config = ProducerConfig {
        compression: Compression::None,
        codec_contexts: 0,
        record_descriptors: descriptors,
        delivery_event_capacity: descriptors,
        pending_records_per_topic: descriptors,
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
    let engine = ProducerEngine::new(
        config.clone(),
        Some(ProducerIdentity {
            producer_id: 1,
            epoch: 0,
        }),
    )
    .unwrap();
    let admission = Admission::new(
        &config,
        engine.credits(),
        engine.validated.effective_batch_payload_bytes,
    );
    (engine, admission)
}
pub(in crate::engine) fn ready(
    engine: &mut ProducerEngine,
    name: &str,
    id: TopicId,
) -> TopicHandle {
    let topic = engine.open_topic(name, RuntimeInstant::ZERO).unwrap();
    resolve(engine, topic, name, id);
    topic
}
pub(in crate::engine) fn resolve(
    engine: &mut ProducerEngine,
    topic: TopicHandle,
    name: &str,
    id: TopicId,
) {
    engine
        .apply_metadata(
            RuntimeInstant::ZERO,
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
                    name: Some(name.into()),
                    error_code: 0,
                    partitions: vec![MetadataPartition {
                        replicas: Vec::new(),
                        isr: Vec::new(),
                        offline: Vec::new(),
                        index: 0,
                        error_code: 0,
                        metadata: PartitionMetadata {
                            leader: 0,
                            leader_epoch: 0,
                        },
                    }],
                }],
            },
        )
        .unwrap();
    assert!(matches!(
        engine.pop_event().unwrap().event,
        Event::TopicReady { .. }
    ));
}
pub(in crate::engine) fn submit(
    engine: &mut ProducerEngine,
    admission: &mut Admission,
    topic: TopicHandle,
    count: usize,
    choice: PartitionChoice,
) {
    let records: Vec<_> = (0..count)
        .map(|_| RecordDescriptor {
            topic,
            partition_hint: Some(0),
            lane_hint: None,
            key: None,
            value: Some(&[7; 16]),
            headers: &[],
            timestamp_ms: 0,
            user_token: 0,
            delivery_timeout: None,
        })
        .collect();
    let (result, batch) =
        admission.prepare_copy(RuntimeInstant::ZERO, &records, &vec![Ok(0); count]);
    assert_eq!(result.accepted as usize, count, "{:?}", result.error);
    engine
        .admit(RuntimeInstant::ZERO, batch.unwrap(), &vec![choice; count])
        .unwrap();
}
pub(in crate::engine) fn one() -> WorkBudget {
    WorkBudget { bytes: 1, items: 1 }
}

#[test]
fn resolved_topic_id_survives_close_while_partition_policy_keeps_records_pending() {
    let (mut engine, mut admission) = setup(8);
    let topic = engine
        .open_topic("resolving", RuntimeInstant::ZERO)
        .unwrap();
    submit(
        &mut engine,
        &mut admission,
        topic,
        2,
        PartitionChoice::Pending,
    );
    assert_eq!(engine.topic_records[&topic].captured_id, None);
    let id = TopicId([10; 16]);
    resolve(&mut engine, topic, "resolving", id);
    let tokens: Vec<_> = engine
        .pending_records()
        .map(|record| record.token)
        .collect();
    for token in tokens {
        engine
            .route_pending(RuntimeInstant::ZERO, token, PartitionChoice::Pending)
            .unwrap();
    }
    assert_eq!(engine.pending_records().count(), 2);
    assert!(
        engine.topic_records[&topic]
            .records
            .values()
            .all(Option::is_none)
    );
    engine.close_topic(topic, RuntimeInstant::ZERO).unwrap();
    assert!(engine.topics.get(topic).is_err());
    assert_eq!(engine.topic_records[&topic].captured_id, Some(id));
    assert_eq!(engine.status().terminal, 0);
    for terminal in 1..=2 {
        assert_eq!(engine.on_deadline(RuntimeInstant::ZERO, one()).items, 1);
        assert_eq!(engine.status().terminal, terminal);
        let Event::Delivery(delivery) = engine.pop_event().unwrap().event else {
            panic!("delivery")
        };
        assert_eq!(delivery.topic, topic);
        assert_eq!(
            delivery.partition,
            TopicPartition {
                topic: id,
                partition: 0
            }
        );
        assert_eq!(
            delivery.outcome,
            DeliveryOutcome::not_written(FailureReason::Closed)
        );
    }
    assert!(!engine.topic_records.contains_key(&topic));
}

pub(in crate::engine) fn dispatch(
    engine: &mut ProducerEngine,
) -> (ConnectionKey, RequestKey, i32, OwnedSendPlan) {
    dispatch_at(engine, RuntimeInstant::from_nanos(1_000_000))
}
pub(in crate::engine) fn dispatch_at(
    engine: &mut ProducerEngine,
    now: RuntimeInstant,
) -> (ConnectionKey, RequestKey, i32, OwnedSendPlan) {
    let budget = WorkBudget {
        bytes: 65536,
        items: 64,
    };
    for _ in 0..64 {
        engine.on_deadline(now, budget);
        engine.encode(now, budget);
        engine.schedule(now, budget);
        while let Some(order) = engine.pop_order() {
            match order {
                EngineOrder::Connect { key, .. } => {
                    engine
                        .on_connection(key, now, ConnectionEvent::Active)
                        .unwrap();
                }
                EngineOrder::Dispatch {
                    connection,
                    request,
                    correlation,
                    plan,
                    ..
                } => {
                    return (connection, request, correlation, plan);
                }
                EngineOrder::Metadata { .. } => {}
                EngineOrder::InitProducerId { previous } => {
                    engine
                        .install_identity(ProducerIdentity {
                            producer_id: previous.unwrap().producer_id + 1,
                            epoch: 0,
                        })
                        .unwrap();
                }
                other => panic!("unexpected order {other:?}"),
            }
        }
    }
    panic!("no dispatch: {:?}", engine.status());
}

pub(in crate::engine) fn cleanup_one(engine: &mut ProducerEngine, now: RuntimeInstant) -> Progress {
    if engine.terminal_records.is_empty() {
        engine.on_deadline(now, one())
    } else {
        engine.encode(now, one())
    }
}

pub(in crate::engine) fn driver_config() -> DriverConfig {
    DriverConfig {
        mode: WriteMode::Vectored,
        staging_bytes: 8192,
        max_operation_bytes: 8192,
        max_inflight_requests: 5,
        rx_bytes: 8192,
    }
}

#[test]
fn closed_topic_fences_cold_multi_topic_request_before_any_cleanup_and_other_topic_can_retry() {
    let (mut engine, mut admission) = setup(8);
    let old = ready(&mut engine, "old", TopicId([6; 16]));
    let other = ready(&mut engine, "other", TopicId([7; 16]));
    submit(
        &mut engine,
        &mut admission,
        old,
        1,
        PartitionChoice::Partition(0),
    );
    submit(
        &mut engine,
        &mut admission,
        other,
        1,
        PartitionChoice::Partition(0),
    );
    let (connection, request, correlation, plan) = dispatch(&mut engine);
    assert_eq!(
        engine
            .requests
            .get(Slot::from_packed(request.0))
            .unwrap()
            .batches
            .len(),
        2
    );
    let network = MemoryNetwork::new(MemoryNetworkConfig {
        max_operation_bytes: 8192,
        ..MemoryNetworkConfig::default()
    })
    .unwrap();
    let (left, _right) = network.connected_pair().unwrap();
    let mut driver = ConnectionDriver::new(left, driver_config()).unwrap();
    driver
        .enqueue(SendRequest {
            correlation,
            deadline: RuntimeInstant::MAX,
            plan,
        })
        .unwrap();
    let now = RuntimeInstant::from_nanos(1_000_000);
    engine.close_topic(old, now).unwrap();
    assert_eq!(engine.status().terminal, 0);
    let mut runtime = SimRuntime::default();
    runtime
        .block_on(async {
            loop {
                let released = poll_fn(|cx| {
                    let Poll::Ready(event) = driver.poll_event(cx, now) else {
                        return Poll::Pending;
                    };
                    match event.unwrap() {
                        DriverEvent::Retiring { reason } => {
                            engine
                                .on_connection(
                                    connection,
                                    now,
                                    ConnectionEvent::Retiring { reason },
                                )
                                .unwrap();
                        }
                        DriverEvent::RequestRetired {
                            correlation: id,
                            confirmed,
                            certainty,
                        } => {
                            assert_eq!(id, correlation);
                            assert_eq!(
                                (confirmed, certainty),
                                (0, CompletionCertainty::NotApplied)
                            );
                            engine
                                .on_request_retired(connection, id, now, confirmed, certainty)
                                .unwrap();
                        }
                        DriverEvent::Released => {
                            engine
                                .on_connection(connection, now, ConnectionEvent::Released)
                                .unwrap();
                            return Poll::Ready(true);
                        }
                        event => panic!("cold closed-topic plan submitted: {event:?}"),
                    }
                    Poll::Ready(false)
                })
                .await;
                if released {
                    break;
                }
                assert_eq!(network.status().buffered_bytes, 0);
            }
        })
        .unwrap();
    assert_eq!(network.status().buffered_bytes, 0);
    for _ in 0..256 {
        let before = engine.status().terminal;
        assert!(cleanup_one(&mut engine, now).items <= 1);
        assert!(engine.status().terminal <= before + 1);
        if engine.status().terminal == 1 {
            break;
        }
    }
    let Event::Delivery(delivery) = engine.pop_event().unwrap().event else {
        panic!("delivery")
    };
    assert_eq!(delivery.topic, old);
    assert_eq!(
        delivery.outcome,
        DeliveryOutcome::not_written(FailureReason::Closed)
    );
    assert_eq!(engine.topic_records[&other].records.len(), 1);
    assert!(engine.failed.is_none());
    let (_, retry, _, _) = dispatch_at(&mut engine, RuntimeInstant::from_nanos(100_000_000));
    let batches = &engine
        .requests
        .get(Slot::from_packed(retry.0))
        .unwrap()
        .batches;
    assert_eq!(batches.len(), 1);
    assert_eq!(
        engine.batches.get(batches[0]).unwrap().records[0].topic,
        other
    );
}

#[test]
fn close_after_provider_admission_is_unknown_and_keeps_payload_until_actual_release() {
    let (mut engine, mut admission) = setup(8);
    // Preserve the original payload spans so their byte credits, rather than a
    // coalesced request-arena copy, demonstrate actual provider ownership.
    engine.config.coalesce_below_bytes = 0;
    let old = ready(&mut engine, "old", TopicId([8; 16]));
    let other = ready(&mut engine, "other", TopicId([9; 16]));
    submit(
        &mut engine,
        &mut admission,
        old,
        1,
        PartitionChoice::Partition(0),
    );
    submit(
        &mut engine,
        &mut admission,
        other,
        1,
        PartitionChoice::Partition(0),
    );
    let (_, request, correlation, plan) = dispatch(&mut engine);
    assert_eq!(
        engine
            .requests
            .get(Slot::from_packed(request.0))
            .unwrap()
            .batches
            .len(),
        2
    );
    let mut runtime = SimRuntime::default();
    let network = SimNetwork::new(
        runtime.handle(),
        NetworkConfig {
            max_operation_bytes: 8192,
            ..NetworkConfig::default()
        },
    )
    .unwrap();
    let (left, right) = network.connected_pair(NodeId(1), NodeId(2)).unwrap();
    network
        .push_fault(ScriptedFault::stall(NetworkOperationKind::Write))
        .unwrap();
    let mut driver = ConnectionDriver::new(left, driver_config()).unwrap();
    driver
        .enqueue(SendRequest {
            correlation,
            deadline: RuntimeInstant::MAX,
            plan,
        })
        .unwrap();
    let now = RuntimeInstant::from_nanos(1_000_000);
    assert!(matches!(
        driver.poll_event(&mut Context::from_waker(Waker::noop()), now),
        Poll::Ready(Some(DriverEvent::WriteAdmitted { .. }))
    ));
    engine.on_write_admitted(request).unwrap();
    // The provider owns the operation before its response receives any poll.
    engine.close_topic(old, now).unwrap();
    let mut deliveries = Vec::new();
    for _ in 0..512 {
        let before = engine.status().terminal;
        assert!(cleanup_one(&mut engine, now).items <= 1);
        assert!(engine.status().terminal <= before + 1);
        while let Some(event) = engine.pop_event() {
            if let Event::Delivery(delivery) = event.event {
                deliveries.push(delivery);
            }
        }
        if deliveries.len() == 1 {
            break;
        }
    }
    assert_eq!(deliveries.len(), 1);
    assert!(
        !engine.is_failed(),
        "closing one topic must preserve the other pending topic"
    );
    assert!(
        deliveries
            .iter()
            .all(|delivery| delivery.outcome.kind == DeliveryKind::Unknown)
    );
    assert!(deliveries.iter().any(|delivery| delivery.topic == old));
    assert!(!engine.is_quiescent());
    let old_partition = TopicPartition {
        topic: TopicId([8; 16]),
        partition: 0,
    };
    // Batch slots and application deliveries may be gone while an actual
    // provider still owns this old UUID's submitted request and ciphertext.
    for _ in 0..256 {
        assert!(cleanup_one(&mut engine, now).items <= 1);
    }
    let retained = &engine.partitions[&old_partition];
    assert!(retained.records.is_empty() && retained.batches.is_empty());
    assert_eq!(retained.terminal_owners, 0);
    assert_eq!(retained.request_owners, 1);
    assert!(
        !engine.partition_cleanup.has_work(),
        "provider wait must park"
    );
    let credits = engine.credits.clone();
    let held = || credits.snapshot()[Resource::CompressedBytes as usize].held;
    assert!(held() > 0);
    drop(driver);
    assert!(
        held() > 0,
        "abandoned response released provider-held output"
    );
    drop(right);
    drop(network);
    runtime.run_until_stalled().unwrap();
    runtime.finish().unwrap();
    assert!(held() > 0, "the other topic retains its live batch");
    engine.close_topic(other, now).unwrap();
    for _ in 0..512 {
        cleanup_one(&mut engine, now);
        while engine.pop_event().is_some() {}
    }
    assert_eq!(held(), 0);
}

#[test]
fn topic_close_visits_only_its_records_and_keeps_uuid_after_cache_removal() {
    let (mut engine, mut admission) = setup(32);
    let id = TopicId([1; 16]);
    let topic = ready(&mut engine, "closing", id);
    let other = ready(&mut engine, "other", TopicId([2; 16]));
    submit(
        &mut engine,
        &mut admission,
        topic,
        3,
        PartitionChoice::Pending,
    );
    submit(
        &mut engine,
        &mut admission,
        topic,
        5,
        PartitionChoice::Partition(0),
    );
    submit(
        &mut engine,
        &mut admission,
        other,
        16,
        PartitionChoice::Partition(0),
    );
    let unrelated = engine.partitions[&TopicPartition {
        topic: TopicId([2; 16]),
        partition: 0,
    }]
        .records
        .bytes();
    // Keep the immutable metadata view pinned to isolate record-credit settlement.
    let _snapshot = engine.topics.get(topic).unwrap().snapshot.clone();
    let input = engine.credits.snapshot()[Resource::InputBytes as usize].held;
    engine.close_topic(topic, RuntimeInstant::ZERO).unwrap();
    assert!(engine.topics.get(topic).is_err());
    assert_eq!(engine.status().terminal, 0);
    assert_eq!(
        engine.credits.snapshot()[Resource::InputBytes as usize].held,
        input
    );
    assert_eq!(
        engine
            .on_deadline(RuntimeInstant::ZERO, WorkBudget { bytes: 0, items: 0 })
            .items,
        0
    );
    for terminal in 1..=8 {
        assert_eq!(engine.on_deadline(RuntimeInstant::ZERO, one()).items, 1);
        assert_eq!(engine.status().terminal, terminal);
        let Event::Delivery(event) = engine.pop_event().unwrap().event else {
            panic!("delivery")
        };
        assert_eq!(event.topic, topic);
        assert_eq!(
            event.partition,
            TopicPartition {
                topic: id,
                partition: 0
            }
        );
        assert_eq!(
            event.outcome,
            DeliveryOutcome::not_written(FailureReason::Closed)
        );
        assert_eq!(
            engine.partitions[&TopicPartition {
                topic: TopicId([2; 16]),
                partition: 0
            }]
                .records
                .bytes(),
            unrelated
        );
    }
    assert!(!engine.topic_records.contains_key(&topic));
    assert!(engine.settling_topics.is_empty());
    assert_eq!(engine.topic_records[&other].records.len(), 16);
}

#[test]
fn reopened_handle_cannot_join_an_old_handles_open_batch() {
    let (mut engine, mut admission) = setup(16);
    let id = TopicId([3; 16]);
    let old = ready(&mut engine, "same", id);
    submit(
        &mut engine,
        &mut admission,
        old,
        4,
        PartitionChoice::Partition(0),
    );
    let partition = TopicPartition {
        topic: id,
        partition: 0,
    };
    for _ in 0..64 {
        engine.encode(
            RuntimeInstant::ZERO,
            WorkBudget {
                bytes: 4096,
                items: 1,
            },
        );
        if engine.partitions[&partition].records.is_empty() {
            break;
        }
    }
    assert!(engine.partitions[&partition].records.is_empty());
    assert_eq!(engine.batches.len(), 1);
    engine.close_topic(old, RuntimeInstant::ZERO).unwrap();
    let new = ready(&mut engine, "same", id);
    assert_ne!(old, new);
    submit(
        &mut engine,
        &mut admission,
        new,
        4,
        PartitionChoice::Partition(0),
    );
    let partition = TopicPartition {
        topic: id,
        partition: 0,
    };
    for _ in 0..64 {
        engine.encode(
            RuntimeInstant::ZERO,
            WorkBudget {
                bytes: 4096,
                items: 1,
            },
        );
        if engine.partitions[&partition].records.is_empty() {
            break;
        }
    }
    assert!(engine.partitions[&partition].records.is_empty());
    assert_eq!(engine.batches.len(), 2);
    for (_, batch) in engine.batches.iter() {
        let handle = batch.records[0].topic;
        assert!(batch.records.iter().all(|record| record.topic == handle));
    }
    for _ in 0..128 {
        engine.on_deadline(RuntimeInstant::ZERO, one());
        engine.encode(RuntimeInstant::ZERO, one());
        if !engine.topic_records.contains_key(&old) {
            break;
        }
    }
    assert!(!engine.topic_records.contains_key(&old));
    assert_eq!(engine.topic_records[&new].records.len(), 4);
    assert!(engine.topic_records[&new].reason.is_none());
}

#[test]
fn queued_settlements_are_bounded_by_live_descriptors_across_reopen_generations() {
    let (mut engine, mut admission) = setup(4);
    for _ in 0..100 {
        let empty = ready(&mut engine, "empty", TopicId([4; 16]));
        engine.close_topic(empty, RuntimeInstant::ZERO).unwrap();
        assert!(engine.topic_records.is_empty() && engine.settling_topics.is_empty());
    }
    for generation in 0..4 {
        let handle = ready(&mut engine, "busy", TopicId([generation as u8 + 5; 16]));
        submit(
            &mut engine,
            &mut admission,
            handle,
            1,
            PartitionChoice::Pending,
        );
        engine.close_topic(handle, RuntimeInstant::ZERO).unwrap();
        assert_eq!(engine.topic_records.len(), generation + 1);
        assert_eq!(engine.settling_topics.len(), generation + 1);
    }
    assert_eq!(engine.status().accepted, 4);
    for remaining in (0..4).rev() {
        engine.on_deadline(RuntimeInstant::ZERO, one());
        assert!(engine.pop_event().is_some());
        assert_eq!(engine.topic_records.len(), remaining);
        assert_eq!(engine.settling_topics.len(), remaining);
    }
}

#[test]
fn global_failure_preserves_deleted_topic_cause_and_removes_each_index_once() {
    let (mut engine, mut admission) = setup(8);
    let id = TopicId([9; 16]);
    let topic = ready(&mut engine, "deleted", id);
    submit(
        &mut engine,
        &mut admission,
        topic,
        4,
        PartitionChoice::Partition(0),
    );
    engine.topics.mark_failed(topic, true).unwrap();
    engine.settle_topic(topic, FailureReason::TopicDeleted);
    engine.topics.close(topic).unwrap();
    engine.fail_producer(FailureReason::RuntimeFailed);
    assert!(matches!(
        engine.pop_event().unwrap().event,
        Event::Fatal { .. }
    ));
    for _ in 0..4 {
        let before = engine.status().terminal;
        assert_eq!(engine.on_deadline(RuntimeInstant::ZERO, one()).items, 1);
        assert_eq!(engine.status().terminal, before + 1);
        let Event::Delivery(event) = engine.pop_event().unwrap().event else {
            panic!("delivery")
        };
        assert_eq!(event.partition.topic, id);
        assert_eq!(
            event.outcome,
            DeliveryOutcome::not_written(FailureReason::TopicDeleted)
        );
    }
    assert!(engine.topic_records.is_empty() && engine.settling_topics.is_empty());
}
