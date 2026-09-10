use super::*;
use crate::engine::topic_settlement::tests::{ready, resolve, setup, submit};
use crate::input::{InputLeases, LeasedRecordDescriptor};

fn drain(engine: &mut ProducerEngine) -> Vec<DeliveryEvent> {
    let mut result = Vec::new();
    for _ in 0..1024 {
        if let Some(envelope) = engine.pop_event() {
            if let Event::Delivery(event) = envelope.event {
                result.push(event);
            }
        } else if !engine.has_terminal_work() {
            return result;
        }
    }
    panic!("terminal publication did not park: {:?}", engine.status());
}

#[test]
fn blocked_terminals_release_payloads_retain_descriptor_bounds_and_park() {
    let (mut engine, mut admission) = setup(8);
    let topic = ready(&mut engine, "ordered", TopicId([17; 16]));
    let baseline = engine.credits.snapshot()[Resource::InputBytes as usize].held;
    submit(
        &mut engine,
        &mut admission,
        topic,
        8,
        PartitionChoice::Partition(0),
    );
    let initial = engine.credits.snapshot()[Resource::InputBytes as usize].held;
    assert!(initial > 0);
    for token in (2..=8).rev() {
        engine
            .cancel(RuntimeInstant::ZERO, RecordToken(token))
            .unwrap();
        assert!(drain(&mut engine).is_empty());
        assert!(
            !engine.has_terminal_work(),
            "a blocked gate must not self-wake"
        );
        assert_eq!(engine.terminal_order.held.len(), (9 - token) as usize);
        assert_eq!(
            engine.credits.snapshot()[Resource::Descriptors as usize].held,
            8
        );
    }
    assert_eq!(engine.status().terminal, 0);
    assert_eq!(
        engine.credits.snapshot()[Resource::InputBytes as usize].held,
        baseline + (initial - baseline) / 8
    );
    engine.cancel(RuntimeInstant::ZERO, RecordToken(1)).unwrap();
    assert_eq!(
        drain(&mut engine)
            .iter()
            .map(|event| event.token.0)
            .collect::<Vec<_>>(),
        (1..=8).collect::<Vec<_>>()
    );
    assert!(engine.terminal_order.held.is_empty());
    assert!(engine.terminal_order.partitions.is_empty());
    assert!(engine.terminal_order.unrouted.is_empty());
    assert_eq!(
        engine.credits.snapshot()[Resource::Descriptors as usize].held,
        0
    );
    assert_eq!(
        engine.credits.snapshot()[Resource::DeliveryEvents as usize].held,
        0
    );
}

#[test]
fn seeded_terminal_histories_match_admission_order_after_every_operation() {
    for seed in 1u64..128 {
        let (mut engine, mut admission) = setup(32);
        let topic = ready(&mut engine, "ordered", TopicId([18; 16]));
        submit(
            &mut engine,
            &mut admission,
            topic,
            32,
            PartitionChoice::Partition(0),
        );
        let mut random = seed;
        let mut remaining: Vec<u64> = (1..=32).collect();
        let mut terminal = [false; 32];
        let mut published = Vec::new();
        let mut history = Vec::new();
        while !remaining.is_empty() {
            random ^= random << 13;
            random ^= random >> 7;
            random ^= random << 17;
            let token = remaining.swap_remove(random as usize % remaining.len());
            history.push(token);
            engine
                .cancel(RuntimeInstant::ZERO, RecordToken(token))
                .unwrap();
            terminal[token as usize - 1] = true;
            let events = drain(&mut engine);
            assert!(events.iter().all(
                |event| event.outcome == DeliveryOutcome::not_written(FailureReason::Cancelled)
            ));
            published.extend(events.iter().map(|event| event.token.0));
            let expected = terminal.iter().take_while(|&&done| done).count() as u64;
            assert_eq!(
                published,
                (1..=expected).collect::<Vec<_>>(),
                "seed={seed} history={history:?}"
            );
            assert_eq!(
                engine.status().terminal,
                expected,
                "seed={seed} history={history:?}"
            );
            assert!(engine.terminal_order.held.len() + engine.queued_locations.len() <= 32);
        }
        assert!(engine.topic_records.is_empty());
    }
}

#[test]
fn held_delivery_does_not_retain_a_released_native_lease() {
    let (mut engine, mut admission) = setup(8);
    let topic = ready(&mut engine, "leased", TopicId([19; 16]));
    submit(
        &mut engine,
        &mut admission,
        topic,
        1,
        PartitionChoice::Partition(0),
    );
    let inputs = InputLeases::new(engine.config(), engine.credits()).unwrap();
    let mut buffer = inputs.acquire(64, 0).unwrap();
    buffer.as_mut_slice()[..4].copy_from_slice(b"data");
    let lease = buffer.commit(4).unwrap();
    let record = LeasedRecordDescriptor {
        topic,
        partition_hint: Some(0),
        lane_hint: None,
        key: None,
        value: Some(0..4),
        headers: &[],
        timestamp_ms: 0,
        user_token: 0,
        delivery_timeout: None,
    };
    let (submitted, batch) =
        admission.prepare_leased(RuntimeInstant::ZERO, &inputs, lease, &[record], &[Ok(0)]);
    assert_eq!(submitted.accepted, 1);
    engine
        .admit(
            RuntimeInstant::ZERO,
            batch.unwrap(),
            &[PartitionChoice::Partition(0)],
        )
        .unwrap();
    inputs.release(lease).unwrap();
    assert!(inputs.pop_released().is_none());
    engine.cancel(RuntimeInstant::ZERO, RecordToken(2)).unwrap();
    assert!(drain(&mut engine).is_empty());
    let released = inputs
        .pop_released()
        .expect("terminal payload release precedes ordered delivery");
    assert_eq!(released.event, Event::InputReleased { lease });
    assert_eq!(inputs.status().live, 0);
    assert_eq!(engine.terminal_order.held.len(), 1);
    assert_eq!(
        engine.credits.snapshot()[Resource::Descriptors as usize].held,
        2
    );
    drop(released);
    engine.cancel(RuntimeInstant::ZERO, RecordToken(1)).unwrap();
    assert_eq!(
        drain(&mut engine)
            .iter()
            .map(|event| event.token.0)
            .collect::<Vec<_>>(),
        [1, 2]
    );
}

#[test]
fn resolution_before_routing_keeps_the_earlier_admission_in_the_gate() {
    let (mut engine, mut admission) = setup(8);
    let topic = engine
        .open_topic("resolving", RuntimeInstant::ZERO)
        .unwrap();
    submit(
        &mut engine,
        &mut admission,
        topic,
        1,
        PartitionChoice::Pending,
    );
    resolve(&mut engine, topic, "resolving", TopicId([20; 16]));
    submit(
        &mut engine,
        &mut admission,
        topic,
        1,
        PartitionChoice::Partition(0),
    );
    engine.cancel(RuntimeInstant::ZERO, RecordToken(2)).unwrap();
    assert!(drain(&mut engine).is_empty());
    engine
        .route_pending(
            RuntimeInstant::ZERO,
            RecordToken(1),
            PartitionChoice::Partition(0),
        )
        .unwrap();
    assert!(drain(&mut engine).is_empty());
    engine.cancel(RuntimeInstant::ZERO, RecordToken(1)).unwrap();
    assert_eq!(
        drain(&mut engine)
            .iter()
            .map(|event| event.token.0)
            .collect::<Vec<_>>(),
        [1, 2]
    );
}

#[test]
fn reopen_same_uuid_does_not_bypass_old_partition_terminal_ownership() {
    let (mut engine, mut admission) = setup(8);
    let id = TopicId([21; 16]);
    let old = ready(&mut engine, "reopened", id);
    submit(
        &mut engine,
        &mut admission,
        old,
        1,
        PartitionChoice::Partition(0),
    );
    engine.close_topic(old, RuntimeInstant::ZERO).unwrap();
    let reopened = ready(&mut engine, "reopened", id);
    assert_ne!(old, reopened);
    submit(
        &mut engine,
        &mut admission,
        reopened,
        1,
        PartitionChoice::Partition(0),
    );
    engine.cancel(RuntimeInstant::ZERO, RecordToken(2)).unwrap();
    assert!(drain(&mut engine).is_empty());
    engine.cancel(RuntimeInstant::ZERO, RecordToken(1)).unwrap();
    let delivered = drain(&mut engine);
    assert_eq!(
        delivered
            .iter()
            .map(|event| event.token.0)
            .collect::<Vec<_>>(),
        [1, 2]
    );
    assert_eq!(delivered[0].topic, old);
    assert_eq!(delivered[1].topic, reopened);
}

#[test]
fn reopen_same_uuid_keeps_resolved_but_unrouted_admission_order() {
    let (mut engine, mut admission) = setup(8);
    let id = TopicId([22; 16]);
    let old = engine.open_topic("reopened", RuntimeInstant::ZERO).unwrap();
    submit(
        &mut engine,
        &mut admission,
        old,
        1,
        PartitionChoice::Pending,
    );
    resolve(&mut engine, old, "reopened", id);
    engine.close_topic(old, RuntimeInstant::ZERO).unwrap();
    let reopened = ready(&mut engine, "reopened", id);
    submit(
        &mut engine,
        &mut admission,
        reopened,
        1,
        PartitionChoice::Partition(0),
    );
    engine.cancel(RuntimeInstant::ZERO, RecordToken(2)).unwrap();
    assert!(
        drain(&mut engine).is_empty(),
        "retired unresolved head blocks the same UUID"
    );
    engine.cancel(RuntimeInstant::ZERO, RecordToken(1)).unwrap();
    let delivered = drain(&mut engine);
    assert_eq!(
        delivered
            .iter()
            .map(|event| event.token.0)
            .collect::<Vec<_>>(),
        [1, 2]
    );
    assert!(delivered.iter().all(|event| event.partition
        == TopicPartition {
            topic: id,
            partition: 0
        }));
    assert!(engine.terminal_order.unrouted_ids.is_empty());
    assert!(engine.terminal_order.unrouted_heads.is_empty());
}

#[test]
fn seeded_retired_unrouted_heads_match_the_uuid_partition_model() {
    for seed in 1u64..64 {
        let (mut engine, mut admission) = setup(32);
        let id = TopicId([23; 16]);
        for _ in 0..8 {
            let old = engine.open_topic("reopened", RuntimeInstant::ZERO).unwrap();
            submit(
                &mut engine,
                &mut admission,
                old,
                3,
                PartitionChoice::Pending,
            );
            resolve(&mut engine, old, "reopened", id);
            engine.close_topic(old, RuntimeInstant::ZERO).unwrap();
        }
        let current = ready(&mut engine, "reopened", id);
        submit(
            &mut engine,
            &mut admission,
            current,
            8,
            PartitionChoice::Partition(0),
        );
        let mut remaining: Vec<u64> = (1..=32).collect();
        let mut random = seed;
        let mut completed = [false; 32];
        let mut published = Vec::new();
        let mut history = Vec::new();
        while !remaining.is_empty() {
            random ^= random << 13;
            random ^= random >> 7;
            random ^= random << 17;
            let token = remaining.swap_remove(random as usize % remaining.len());
            history.push(token);
            engine
                .cancel(RuntimeInstant::ZERO, RecordToken(token))
                .unwrap();
            completed[token as usize - 1] = true;
            let events = drain(&mut engine);
            assert!(events.iter().all(|event| event.partition
                == TopicPartition {
                    topic: id,
                    partition: 0
                }));
            published.extend(events.iter().map(|event| event.token.0));
            let expected = completed.iter().take_while(|&&done| done).count() as u64;
            assert_eq!(
                published,
                (1..=expected).collect::<Vec<_>>(),
                "seed={seed} history={history:?}"
            );
            assert_eq!(engine.status().terminal, expected);
            assert!(engine.terminal_order.unrouted_ids.len() <= 32 - published.len());
            assert!(
                engine
                    .terminal_order
                    .unrouted_heads
                    .values()
                    .map(BTreeSet::len)
                    .sum::<usize>()
                    <= 32 - published.len()
            );
        }
        assert!(engine.terminal_order.unrouted_ids.is_empty());
        assert!(engine.terminal_order.unrouted_heads.is_empty());
        assert!(engine.terminal_order.partitions.is_empty());
        assert!(engine.topic_records.is_empty());
        for resource in [Resource::Descriptors, Resource::DeliveryEvents] {
            assert_eq!(engine.credits.snapshot()[resource as usize].held, 0);
        }
    }
}

#[test]
fn retired_unrouted_identity_does_not_block_a_recreated_uuid() {
    let (mut engine, mut admission) = setup(8);
    let old = engine
        .open_topic("recreated", RuntimeInstant::ZERO)
        .unwrap();
    submit(
        &mut engine,
        &mut admission,
        old,
        1,
        PartitionChoice::Pending,
    );
    resolve(&mut engine, old, "recreated", TopicId([24; 16]));
    engine.close_topic(old, RuntimeInstant::ZERO).unwrap();
    let new = ready(&mut engine, "recreated", TopicId([25; 16]));
    submit(
        &mut engine,
        &mut admission,
        new,
        1,
        PartitionChoice::Partition(0),
    );
    engine.cancel(RuntimeInstant::ZERO, RecordToken(2)).unwrap();
    assert_eq!(
        drain(&mut engine)
            .iter()
            .map(|event| event.token.0)
            .collect::<Vec<_>>(),
        [2]
    );
    engine.cancel(RuntimeInstant::ZERO, RecordToken(1)).unwrap();
    assert_eq!(
        drain(&mut engine)
            .iter()
            .map(|event| event.token.0)
            .collect::<Vec<_>>(),
        [1]
    );
    assert!(engine.terminal_order.unrouted_ids.is_empty());
    assert!(engine.terminal_order.unrouted_heads.is_empty());
}

#[test]
fn pre_resolution_failure_does_not_bind_remaining_records_to_unknown_uuid() {
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
    engine.cancel(RuntimeInstant::ZERO, RecordToken(2)).unwrap();
    assert!(drain(&mut engine).is_empty());
    assert!(engine.terminal_order.unrouted_ids.is_empty());
    resolve(&mut engine, topic, "resolving", TopicId([26; 16]));
    engine
        .route_pending(
            RuntimeInstant::ZERO,
            RecordToken(1),
            PartitionChoice::Partition(0),
        )
        .unwrap();
    assert_eq!(
        drain(&mut engine)
            .iter()
            .map(|event| event.token.0)
            .collect::<Vec<_>>(),
        [2]
    );
    engine.cancel(RuntimeInstant::ZERO, RecordToken(1)).unwrap();
    assert_eq!(
        drain(&mut engine)
            .iter()
            .map(|event| event.token.0)
            .collect::<Vec<_>>(),
        [1]
    );
    assert!(engine.terminal_order.unrouted_ids.is_empty());
    assert!(engine.terminal_order.unrouted_heads.is_empty());
}
