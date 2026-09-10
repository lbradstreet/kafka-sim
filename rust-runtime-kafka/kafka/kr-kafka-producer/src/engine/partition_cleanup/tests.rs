use super::*;
use crate::engine::topic_settlement::tests::{cleanup_one, dispatch, one, ready, setup, submit};
use kr_kafka_broker_model::{BrokerAction, BrokerConfig, BrokerModel, FaultPlan};

fn at() -> RuntimeInstant {
    RuntimeInstant::from_nanos(1_000_000)
}
fn drain(engine: &mut ProducerEngine) -> Vec<DeliveryEvent> {
    let mut events = Vec::new();
    for _ in 0..4096 {
        let before = engine.partitions.len();
        assert!(cleanup_one(engine, at()).items <= 1);
        assert!(engine.partitions.len() + 1 >= before);
        while let Some(event) = engine.pop_event() {
            if let Event::Delivery(delivery) = event.event {
                events.push(delivery);
            }
        }
        if !engine.has_terminal_work() && !engine.has_maintenance_work() {
            return events;
        }
    }
    panic!("maintenance failed to park: {:?}", engine.status());
}
fn broker() -> BrokerModel {
    let mut broker = BrokerModel::new(BrokerConfig::default()).unwrap();
    broker
        .add_broker(kr_kafka_broker_model::BrokerEndpoint {
            id: 0,
            host: "broker".into(),
            port: 9092,
        })
        .unwrap();
    broker
}
fn ack(engine: &mut ProducerEngine, broker: &mut BrokerModel, expected_sequence: i32) {
    let (connection, request, correlation, plan) = dispatch(engine);
    let state = engine.requests.get(Slot::from_packed(request.0)).unwrap();
    let batch = engine.batches.get(state.batches[0]).unwrap();
    let assigned = engine
        .ledger
        .as_ref()
        .unwrap()
        .assignment(batch.partition(), state.batches[0].packed())
        .unwrap();
    assert_eq!(assigned.base_sequence.get(), expected_sequence);
    let bytes: Vec<u8> = plan
        .segments()
        .iter()
        .flat_map(|segment| segment.as_slice().iter().copied())
        .collect();
    engine.on_write_admitted(request).unwrap();
    engine
        .on_write(
            connection,
            correlation,
            plan.len(),
            CompletionCertainty::Applied,
        )
        .unwrap();
    let BrokerAction::Reply(response) = broker
        .handle_frame(0, &bytes, FaultPlan::default())
        .unwrap()
    else {
        panic!("broker response")
    };
    engine.on_frame(connection, at(), &response).unwrap();
    drop(plan);
    let events = drain(engine);
    assert!(!events.is_empty());
    assert!(
        events
            .iter()
            .all(|event| event.outcome.kind == DeliveryKind::Acked)
    );
}

#[test]
fn fresh_uuid_churn_reuses_partition_metadata_and_record_credits_past_capacity() {
    let (mut engine, mut admission) = setup(8);
    let capacity = engine.config.max_batches as usize;
    for generation in 0..capacity * 4 {
        let id = TopicId((generation as u128 + 1).to_be_bytes());
        let topic = ready(&mut engine, "recreated", id);
        submit(
            &mut engine,
            &mut admission,
            topic,
            1,
            PartitionChoice::Partition(0),
        );
        assert_eq!(engine.partitions.len(), 1);
        engine.close_topic(topic, at()).unwrap();
        let events = drain(&mut engine);
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].partition.topic, id);
        assert_eq!(
            events[0].outcome,
            DeliveryOutcome::not_written(FailureReason::Closed)
        );
        assert!(engine.partitions.is_empty());
        assert_eq!(engine.ledger.as_ref().unwrap().stats().partitions, 0);
        assert!(engine.partition_cleanup.topics.is_empty());
        assert!(engine.partition_cleanup.ready.is_empty());
        let credits = engine.credits.snapshot();
        for resource in [
            Resource::InputBytes,
            Resource::Descriptors,
            Resource::DeliveryEvents,
            Resource::CompressedBytes,
        ] {
            assert_eq!(
                credits[resource as usize].held, 0,
                "generation {generation}, {resource:?}"
            );
        }
        while let Some(order) = engine.pop_order() {
            assert!(matches!(order, EngineOrder::Metadata { .. }));
        }
    }
}

#[test]
fn acknowledged_uuid_reopen_continues_sequence() {
    let (mut engine, mut admission) = setup(8);
    let mut broker = broker();
    let id = TopicId(broker.create_topic("same", &[0]).unwrap());
    let old = ready(&mut engine, "same", id);
    submit(
        &mut engine,
        &mut admission,
        old,
        3,
        PartitionChoice::Partition(0),
    );
    ack(&mut engine, &mut broker, 0);
    engine.close_topic(old, at()).unwrap();
    assert!(drain(&mut engine).is_empty());
    let partition = TopicPartition {
        topic: id,
        partition: 0,
    };
    assert_eq!(engine.partitions.len(), 1);
    assert!(
        !engine
            .ledger
            .as_ref()
            .unwrap()
            .can_forget_partition(partition)
            .unwrap()
    );
    assert!(
        !engine.partition_cleanup.has_work(),
        "used history must park"
    );
    let reopened = ready(&mut engine, "same", id);
    assert_ne!(old, reopened);
    submit(
        &mut engine,
        &mut admission,
        reopened,
        1,
        PartitionChoice::Partition(0),
    );
    ack(&mut engine, &mut broker, 3);
    assert_eq!(
        broker
            .log()
            .iter()
            .map(|batch| batch.records.len())
            .sum::<usize>(),
        4
    );
    assert!(!engine.partition_cleanup.topics.contains_key(&id));
}

#[test]
fn historical_capacity_bumps_epoch_then_reaps_one_partition_per_visit() {
    let (mut engine, mut admission) = setup(8);
    let mut broker = broker();
    let capacity = engine.config.max_batches as usize;
    for generation in 0..capacity - 1 {
        let name = format!("topic-{generation}");
        let id = TopicId(broker.create_topic(&name, &[0]).unwrap());
        let topic = ready(&mut engine, &name, id);
        submit(
            &mut engine,
            &mut admission,
            topic,
            1,
            PartitionChoice::Partition(0),
        );
        ack(&mut engine, &mut broker, 0);
        engine.close_topic(topic, at()).unwrap();
        assert!(drain(&mut engine).is_empty());
    }
    let live_id = TopicId(broker.create_topic("live", &[0]).unwrap());
    let live = ready(&mut engine, "live", live_id);
    submit(
        &mut engine,
        &mut admission,
        live,
        1,
        PartitionChoice::Partition(0),
    );
    assert_eq!(engine.partitions.len(), capacity);
    let old_identity = engine.ledger.as_ref().unwrap().identity();
    for _ in 0..8192 {
        let before = engine.partitions.len();
        assert!(engine.on_deadline(at(), one()).items <= 1);
        assert!(engine.partitions.len() + 1 >= before);
        while let Some(order) = engine.pop_order() {
            match order {
                EngineOrder::Retire { connection, .. } => engine
                    .on_connection(connection, at(), ConnectionEvent::Released)
                    .unwrap(),
                EngineOrder::InitProducerId { .. } => {
                    panic!("capacity reclaim below epoch limit must be local")
                }
                EngineOrder::Metadata { .. } => {}
                other => panic!("unexpected refresh order: {other:?}"),
            }
        }
        while let Some(event) = engine.pop_event() {
            assert!(!matches!(event.event, Event::Fatal { .. }));
        }
        if engine.partitions.len() == 1 && !engine.has_maintenance_work() {
            break;
        }
    }
    assert_eq!(engine.status().identity, old_identity.next_epoch());
    assert_eq!(engine.partitions.len(), 1);
    assert_eq!(engine.ledger.as_ref().unwrap().stats().partitions, 1);
    assert!(engine.partition_cleanup.topics.is_empty());
    ack(&mut engine, &mut broker, 0);
    engine.close_topic(live, at()).unwrap();
    drain(&mut engine);
    let new = ready(&mut engine, "next", TopicId([255; 16]));
    submit(
        &mut engine,
        &mut admission,
        new,
        1,
        PartitionChoice::Partition(0),
    );
    assert_eq!(engine.partitions.len(), 2);
    assert_eq!(engine.status().pending_records, 1);
}
