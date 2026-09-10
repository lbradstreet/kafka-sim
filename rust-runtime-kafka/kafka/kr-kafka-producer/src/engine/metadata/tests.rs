use super::*;
use crate::engine::topic_settlement::tests::{dispatch, one, ready, setup, submit};
use crate::routing::PartitionChoice;

fn update(names: &[&str], partitions: usize, broker: i32, epoch: i32) -> MetadataUpdate {
    MetadataUpdate {
        throttle_ms: 10,
        controller_id: broker,
        cluster_id: Some("cluster".into()),
        brokers: vec![BrokerNode {
            id: broker,
            host: format!("broker-{broker}"),
            port: 9092,
            rack: None,
        }],
        topics: names
            .iter()
            .enumerate()
            .map(|(index, name)| MetadataTopic {
                requested_index: index,
                id: TopicId([index as u8 + 1; 16]),
                name: Some((*name).into()),
                error_code: 0,
                partitions: (0..partitions)
                    .map(|partition| MetadataPartition {
                        replicas: Vec::new(),
                        isr: Vec::new(),
                        offline: Vec::new(),
                        index: partition as i32,
                        error_code: 0,
                        metadata: PartitionMetadata {
                            leader: broker,
                            leader_epoch: epoch,
                        },
                    })
                    .collect(),
            })
            .collect(),
    }
}
fn drain(engine: &mut ProducerEngine) -> Vec<TopicHandle> {
    let mut notices = Vec::new();
    for _ in 0..4096 {
        if let Some(handle) = engine.take_metadata_notice() {
            notices.push(handle);
        }
        if !engine.has_metadata_work() {
            return notices;
        }
        assert!(engine.on_deadline(RuntimeInstant::ZERO, one()).items <= 1);
    }
    panic!("metadata did not finish");
}
#[test]
fn whole_response_validation_precedes_one_row_visits_and_atomic_topic_publication() {
    let (mut engine, _) = setup(8);
    let a = engine.open_topic("a", RuntimeInstant::ZERO).unwrap();
    let b = engine.open_topic("b", RuntimeInstant::ZERO).unwrap();
    engine
        .begin_metadata(
            RuntimeInstant::ZERO,
            vec![a, b],
            update(&["a", "b"], 7, 0, 0),
            None,
        )
        .unwrap();
    let progress = engine.on_deadline(RuntimeInstant::ZERO, WorkBudget { bytes: 0, items: 0 });
    assert_eq!(progress.items, 0);
    assert!(progress.remaining_immediate);
    let mut visits = 0;
    while engine.metadata_work.as_ref().unwrap().phase != Phase::RemoveBrokers {
        let work = engine.metadata_work.as_ref().unwrap();
        let previous = (work.cursor, work.row, work.phase);
        assert!(engine.on_deadline(RuntimeInstant::ZERO, one()).items <= 1);
        let work = engine.metadata_work.as_ref().unwrap();
        if previous.2 == Phase::Rows && work.phase == Phase::Rows {
            assert_eq!(work.row, previous.1 + 1);
        }
        assert!(engine.brokers.is_empty());
        assert_eq!(engine.topics.get(a).unwrap().generation, 0);
        assert_eq!(engine.topics.get(b).unwrap().generation, 0);
        visits += 1;
        assert!(visits < 128);
    }
    while engine.metadata_work.as_ref().unwrap().notice.is_none() {
        assert!(engine.on_deadline(RuntimeInstant::ZERO, one()).items <= 1);
        let topic = engine.topics.get(a).unwrap();
        assert!(topic.partitions.is_empty() || topic.partitions.len() == 7);
    }
    assert_eq!(engine.topics.get(a).unwrap().partitions.len(), 7);
    assert!(engine.topics.get(b).unwrap().partitions.is_empty());
    assert!(
        !engine.metadata_step(),
        "notice slot parks publication work"
    );
    assert_eq!(drain(&mut engine), vec![a, b]);
    assert!(engine.take_metadata_error().is_none());
    assert_eq!(engine.topics.get(b).unwrap().partitions.len(), 7);
}
#[test]
fn malformed_last_row_does_not_publish_earlier_valid_topic_or_broker() {
    let (mut engine, _) = setup(8);
    let a = ready(&mut engine, "a", TopicId([1; 16]));
    let b = engine.open_topic("b", RuntimeInstant::ZERO).unwrap();
    let old = engine.topics.get(a).unwrap().clone();
    let mut response = update(&["a", "b"], 4, 1, 1);
    response.topics[1].partitions[3].index = 9;
    engine
        .begin_metadata(RuntimeInstant::ZERO, vec![a, b], response, None)
        .unwrap();
    assert!(drain(&mut engine).is_empty());
    assert!(matches!(
        engine.take_metadata_error(),
        Some(EngineError::InvalidState("non-dense partition metadata"))
    ));
    assert_eq!(engine.topics.get(a).unwrap(), &old);
    assert!(engine.topics.get(b).unwrap().id.is_none());
    assert_eq!(engine.brokers.keys().copied().collect::<Vec<_>>(), vec![0]);
}
#[test]
fn candidate_generation_restart_preserves_newer_kip951_leader() {
    let (mut engine, _) = setup(8);
    let a = engine.open_topic("a", RuntimeInstant::ZERO).unwrap();
    engine
        .apply_metadata(RuntimeInstant::ZERO, &[a], update(&["a"], 4, 0, 0))
        .unwrap();
    let id = TopicId([1; 16]);
    engine
        .begin_metadata(RuntimeInstant::ZERO, vec![a], update(&["a"], 4, 0, 1), None)
        .unwrap();
    while engine
        .metadata_work
        .as_ref()
        .unwrap()
        .candidate
        .as_ref()
        .is_none_or(|c| c.prepared.is_none())
    {
        engine.metadata_step();
    }
    engine.metadata_step(); // First comparison belongs to generation one.
    engine
        .topics
        .update_leader(
            id,
            0,
            PartitionMetadata {
                leader: 2,
                leader_epoch: 2,
            },
            RuntimeInstant::ZERO,
        )
        .unwrap();
    let generation = engine.topics.get(a).unwrap().generation;
    assert_eq!(drain(&mut engine), vec![a]);
    assert!(engine.take_metadata_error().is_none());
    let current = engine.topics.get(a).unwrap();
    assert_eq!(current.generation, generation);
    assert_eq!(
        current.partitions[0],
        PartitionMetadata {
            leader: 2,
            leader_epoch: 2
        }
    );
    assert_eq!(
        current.partitions[1].leader_epoch, 0,
        "candidate cannot partially publish"
    );
}
#[test]
fn closing_a_handle_during_normalization_cannot_rebind_reopened_name() {
    let (mut engine, _) = setup(8);
    let a = engine.open_topic("a", RuntimeInstant::ZERO).unwrap();
    let b = engine.open_topic("b", RuntimeInstant::ZERO).unwrap();
    engine
        .begin_metadata(
            RuntimeInstant::ZERO,
            vec![a, b],
            update(&["a", "b"], 4, 0, 0),
            None,
        )
        .unwrap();
    while engine.metadata_work.as_ref().unwrap().candidate.is_none() {
        engine.metadata_step();
    }
    engine.close_topic(a, RuntimeInstant::ZERO).unwrap();
    let reopened = engine.open_topic("a", RuntimeInstant::ZERO).unwrap();
    assert_ne!(a, reopened);
    assert_eq!(drain(&mut engine), vec![a, b]);
    assert!(engine.take_metadata_error().is_none());
    assert!(engine.topics.get(reopened).unwrap().id.is_none());
    assert_eq!(engine.topics.get(b).unwrap().id, Some(TopicId([2; 16])));
}
#[test]
fn retired_broker_deadlines_do_not_accumulate_and_request_owners_block_reclamation() {
    let (mut engine, mut admission) = setup(8);
    let a = ready(&mut engine, "a", TopicId([1; 16]));
    submit(
        &mut engine,
        &mut admission,
        a,
        1,
        PartitionChoice::Partition(0),
    );
    let (connection, _, correlation, plan) = dispatch(&mut engine);
    let mut refresh = update(&["a"], 1, 1, 1);
    engine
        .apply_metadata(RuntimeInstant::ZERO, &[a], refresh.clone())
        .unwrap();
    assert!(
        engine.brokers.contains_key(&0),
        "live request and route retain old node"
    );
    assert_eq!(engine.brokers[&0].requests, 1);
    engine
        .on_request_retired(
            connection,
            correlation,
            RuntimeInstant::ZERO,
            0,
            CompletionCertainty::NotApplied,
        )
        .unwrap();
    engine
        .apply_metadata(RuntimeInstant::ZERO, &[a], refresh.clone())
        .unwrap();
    assert!(
        engine.brokers.contains_key(&0),
        "idle live route still retains node"
    );
    engine
        .on_connection(connection, RuntimeInstant::ZERO, ConnectionEvent::Released)
        .unwrap();
    drop(plan);
    engine
        .apply_metadata(RuntimeInstant::ZERO, &[a], refresh.clone())
        .unwrap();
    assert!(!engine.brokers.contains_key(&0));
    for broker in 2..128 {
        refresh.brokers[0].id = broker;
        refresh.controller_id = broker;
        refresh.topics[0].partitions[0].metadata = PartitionMetadata {
            leader: broker,
            leader_epoch: broker,
        };
        engine
            .apply_metadata(RuntimeInstant::ZERO, &[a], refresh.clone())
            .unwrap();
        assert_eq!(engine.brokers.len(), 1);
        assert_eq!(
            engine
                .deadlines
                .current
                .keys()
                .filter(|key| matches!(key, DeadlineKey::Broker(_)))
                .count(),
            1
        );
    }
}
#[test]
fn retained_response_guard_survives_notice_and_failure_until_bounded_discard() {
    let (mut engine, _) = setup(8);
    let a = engine.open_topic("a", RuntimeInstant::ZERO).unwrap();
    let credits = engine.credits();
    let guard = Arc::new(
        credits
            .reserve(&[Claim {
                resource: Resource::ControlReserve,
                amount: 256,
                lane: 0,
            }])
            .unwrap(),
    );
    engine
        .begin_metadata(
            RuntimeInstant::ZERO,
            vec![a],
            update(&["a"], 4, 0, 0),
            Some(guard.clone()),
        )
        .unwrap();
    drop(guard);
    while engine.metadata_work.as_ref().unwrap().notice.is_none() {
        engine.metadata_step();
    }
    assert_eq!(
        credits.snapshot()[Resource::ControlReserve as usize].held,
        256
    );
    assert!(!engine.is_quiescent());
    engine.fail_producer(FailureReason::RuntimeFailed);
    assert_eq!(
        credits.snapshot()[Resource::ControlReserve as usize].held,
        256
    );
    for _ in 0..512 {
        assert!(engine.on_deadline(RuntimeInstant::ZERO, one()).items <= 1);
        if !engine.has_metadata_work() {
            break;
        }
    }
    assert!(!engine.has_metadata_work());
    assert_eq!(
        credits.snapshot()[Resource::ControlReserve as usize].held,
        0
    );
    assert!(engine.take_metadata_notice().is_none());
}
#[test]
fn physical_capacity_limits_are_checked_before_publication() {
    let (mut engine, _) = setup(8);
    let a = engine.open_topic("a", RuntimeInstant::ZERO).unwrap();
    let mut response = update(&["a"], 1, 0, 0);
    response.brokers[0]
        .host
        .reserve_exact(engine.config.control_reserve_bytes);
    assert!(
        engine
            .apply_metadata(RuntimeInstant::ZERO, &[a], response)
            .is_err()
    );
    assert!(engine.brokers.is_empty());
    assert!(engine.topics.get(a).unwrap().id.is_none());
}
#[test]
fn failed_response_handle_retirement_obeys_each_item_budget() {
    let (mut engine, _) = setup(8);
    let a = engine.open_topic("a", RuntimeInstant::ZERO).unwrap();
    let b = engine.open_topic("b", RuntimeInstant::ZERO).unwrap();
    engine.metadata_pending.extend([a, b]);
    engine
        .begin_metadata_failure(RuntimeInstant::ZERO, vec![a, b], None)
        .unwrap();
    assert!(engine.metadata_step()); // Validate retained count/capacity first.
    assert!(engine.metadata_step());
    assert!(!engine.metadata_pending.contains(&a));
    assert!(engine.metadata_pending.contains(&b));
    assert!(engine.metadata_step());
    assert!(engine.metadata_pending.is_empty());
    assert!(drain(&mut engine).is_empty());
}

#[test]
fn seeded_immutable_snapshots_pin_full_topology_until_the_last_reader_releases() {
    for seed in 0..16u32 {
        let (mut engine, _) = setup(8);
        let topic = engine.open_topic("a", RuntimeInstant::ZERO).unwrap();
        let mut held = Vec::new();
        let mut random = seed + 1;
        for step in 1..=24 {
            random = random.wrapping_mul(1664525).wrapping_add(1013904223);
            let broker = (random % 3) as i32;
            let mut response = update(&["a"], 1 + step / 4, broker, step as i32);
            response.brokers[0].rack = Some(format!("rack-{seed}-{step}"));
            for row in &mut response.topics[0].partitions {
                row.replicas = vec![2, 0, 1];
                row.isr = if random & 1 == 0 {
                    vec![0, 1]
                } else {
                    vec![2, 1]
                };
                row.offline = vec![3];
            }
            // The original owned update is independent evidence for every row.
            let expected_brokers = response.brokers.clone();
            let expected_rows = response.topics[0].partitions.clone();
            engine
                .apply_metadata(RuntimeInstant::ZERO, &[topic], response)
                .unwrap();
            let snapshot = engine.topics.get(topic).unwrap().snapshot.clone().unwrap();
            assert_eq!(snapshot.generation, step as u64, "seed={seed} step={step}");
            held.push((snapshot, expected_brokers, expected_rows));
            for (snapshot, brokers, rows) in &held {
                assert_eq!(&snapshot.brokers.rows, brokers, "seed={seed} step={step}");
                assert_eq!(&snapshot.partitions, rows, "seed={seed} step={step}");
            }
            if held.len() == 4 {
                let before = engine.credits.snapshot()[Resource::InputBytes as usize].held;
                held.remove(0);
                assert!(engine.credits.snapshot()[Resource::InputBytes as usize].held < before);
            }
        }
        engine.close_topic(topic, RuntimeInstant::ZERO).unwrap();
        let pinned = engine.credits.snapshot()[Resource::InputBytes as usize].held;
        assert!(pinned > 0);
        held.clear();
        assert_eq!(
            engine.credits.snapshot()[Resource::InputBytes as usize].held,
            0
        );
    }
}
#[test]
fn replica_only_refresh_changes_snapshot_generation_and_unknown_leaders_are_preserved() {
    let (mut engine, _) = setup(8);
    let topic = engine.open_topic("a", RuntimeInstant::ZERO).unwrap();
    let mut response = update(&["a"], 2, 0, 0);
    response.topics[0].partitions[1].metadata.leader = -1;
    response.topics[0].partitions[1].error_code = 5;
    response.topics[0].partitions[0].replicas = vec![0, 1];
    engine
        .apply_metadata(RuntimeInstant::ZERO, &[topic], response.clone())
        .unwrap();
    let old = engine.topics.get(topic).unwrap().snapshot.clone().unwrap();
    let routing_generation = engine.topics.get(topic).unwrap().generation;
    response.topics[0].partitions[0].isr = vec![1];
    engine
        .apply_metadata(RuntimeInstant::ZERO, &[topic], response)
        .unwrap();
    let current = engine.topics.get(topic).unwrap();
    assert_eq!(current.generation, routing_generation);
    let new = current.snapshot.as_ref().unwrap();
    assert_eq!((old.generation, new.generation), (1, 2));
    assert!(old.partitions[0].isr.is_empty());
    assert_eq!(new.partitions[0].isr, vec![1]);
    assert_eq!(new.partitions[1].metadata.leader, -1);
    assert_eq!(new.partitions[1].error_code, 5);
}
#[test]
fn retention_exhaustion_preserves_previous_complete_snapshot_and_releases_failed_work() {
    let (mut engine, _) = setup(8);
    let topic = engine.open_topic("a", RuntimeInstant::ZERO).unwrap();
    engine
        .apply_metadata(RuntimeInstant::ZERO, &[topic], update(&["a"], 2, 0, 0))
        .unwrap();
    let old = engine.topics.get(topic).unwrap().snapshot.clone().unwrap();
    let state = engine.credits.snapshot()[Resource::InputBytes as usize];
    let pressure = engine
        .credits
        .reserve(&[Claim {
            resource: Resource::InputBytes,
            amount: engine.config.input_bytes - state.held,
            lane: 0,
        }])
        .unwrap();
    assert!(
        engine
            .apply_metadata(RuntimeInstant::ZERO, &[topic], update(&["a"], 3, 1, 1))
            .is_err()
    );
    assert_eq!(
        engine.topics.get(topic).unwrap().snapshot.as_ref(),
        Some(&old)
    );
    drop(pressure);
    assert_eq!(
        engine.credits.snapshot()[Resource::InputBytes as usize].held,
        state.held
    );
    engine
        .apply_metadata(RuntimeInstant::ZERO, &[topic], update(&["a"], 3, 1, 1))
        .unwrap();
    assert_eq!(
        engine
            .topics
            .get(topic)
            .unwrap()
            .snapshot
            .as_ref()
            .unwrap()
            .generation,
        2
    );
}
