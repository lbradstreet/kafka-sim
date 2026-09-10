use super::*;
use crate::control::{MetadataPartition, MetadataTopic};

fn at(n: u64) -> RuntimeInstant {
    RuntimeInstant::from_nanos(n)
}
fn budget(items: u32) -> WorkBudget {
    WorkBudget {
        bytes: 65536,
        items,
    }
}
fn partition(index: i32) -> TopicPartition {
    TopicPartition {
        topic: TopicId([7; 16]),
        partition: index,
    }
}
fn update(count: usize) -> MetadataUpdate {
    MetadataUpdate {
        throttle_ms: 0,
        cluster_id: None,
        controller_id: 0,
        brokers: (0..2)
            .map(|id| BrokerNode {
                id,
                host: "broker".into(),
                port: 9092,
                rack: None,
            })
            .collect(),
        topics: vec![MetadataTopic {
            requested_index: 0,
            id: TopicId([7; 16]),
            name: Some("retry".into()),
            error_code: 0,
            partitions: (0..count)
                .map(|index| MetadataPartition {
                    replicas: Vec::new(),
                    isr: Vec::new(),
                    offline: Vec::new(),
                    index: index as i32,
                    error_code: 0,
                    metadata: PartitionMetadata {
                        leader: (index % 2) as i32,
                        leader_epoch: 0,
                    },
                })
                .collect(),
        }],
    }
}
fn insert_partition(engine: &mut ProducerEngine, index: i32) {
    let key = partition(index);
    engine.ledger.as_mut().unwrap().register(key).unwrap();
    engine.partitions.insert(
        key,
        PartitionQueue {
            compression_estimate: Default::default(),
            metrics_scope: crate::telemetry::metrics::ScopeToken::GLOBAL,
            lane: 0,
            records: RecordQueue::default(),
            batches: BatchQueue::default(),
            batch_bytes: 0,
            request_owners: 0,
            terminal_owners: 0,
            batch_ages: BTreeSet::new(),
            arrival: ArrivalRate::default(),
            deficit: 0,
            retry_at: RuntimeInstant::ZERO,
            drain_bytes_per_second: 0,
            last_drain: None,
        },
    );
    engine.retry_topology_changed();
}
fn connection(engine: &mut ProducerEngine, broker: i32) -> ConnectionKey {
    let credit = Arc::new(engine.credits.reserve(&[]).unwrap());
    let key = ConnectionKey(
        engine
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
            .unwrap()
            .packed(),
    );
    engine.routes.insert((broker, 0), key);
    key
}
fn setup(count: usize) -> (ProducerEngine, TopicHandle) {
    let config = ProducerConfig {
        compression: Compression::None,
        codec_contexts: 0,
        brokers_max: 2,
        max_batches: 1024,
        record_descriptors: 16,
        delivery_event_capacity: 16,
        max_live_leases: 4,
        release_event_capacity: 4,
        pending_records_per_topic: 16,
        retry_backoff_min: RuntimeDuration::from_nanos(10),
        retry_backoff_max: RuntimeDuration::from_nanos(100),
        ..ProducerConfig::default()
    };
    let mut engine = ProducerEngine::new(
        config,
        Some(ProducerIdentity {
            producer_id: 1,
            epoch: 0,
        }),
    )
    .unwrap();
    let handle = engine.open_topic("retry", at(0)).unwrap();
    engine
        .apply_metadata(at(0), &[handle], update(count))
        .unwrap();
    for index in 0..count {
        insert_partition(&mut engine, index as i32);
    }
    for _ in 0..8 {
        engine.on_deadline(at(0), budget(1024));
    }
    (engine, handle)
}

#[test]
fn large_attempt_backoff_caps_without_shift_truncation_and_uses_captured_jitter() {
    let config = ProducerConfig {
        retry_backoff_min: RuntimeDuration::from_nanos(10),
        retry_backoff_max: RuntimeDuration::from_nanos(100),
        ..ProducerConfig::default()
    };
    assert_eq!(delay(&config, 1, 7).as_nanos(), 17);
    assert_eq!(delay(&config, 2, 7).as_nanos(), 27);
    assert_eq!(delay(&config, 64, 107).as_nanos(), 107);
    assert_eq!(delay(&config, u64::MAX, 107).as_nanos(), 107);
}

#[test]
fn empty_registered_window_has_no_admitted_attempts() {
    let partition = TopicPartition {
        topic: TopicId([7; 16]),
        partition: 0,
    };
    let mut ledger = ProducerLedger::new(
        ProducerIdentity {
            producer_id: 1,
            epoch: 0,
        },
        1,
        5,
    )
    .unwrap();
    assert!(ledger.max_attempts(partition).is_err());
    ledger.register(partition).unwrap();
    for batch in 1..=5 {
        ledger.assign(partition, batch, 1).unwrap();
    }
    assert_eq!(ledger.max_attempts(partition), Ok(0));
    ledger.start_attempt(partition, 1, 1).unwrap();
    assert_eq!(ledger.max_attempts(partition), Ok(1));
}

#[test]
fn raw_partition_visits_respect_zero_one_quota_and_capture_failure_time_and_jitter() {
    let (mut engine, _) = setup(512);
    let key = connection(&mut engine, 0);
    engine.set_retry_jitter(7);
    engine.retry_connection(key, at(10));
    engine.set_retry_jitter(999);
    assert!(engine.retry_pending(0, 0));
    assert!(
        !engine.connection_credit(key, at(10)),
        "destination fenced before any sweep item"
    );
    assert_eq!(engine.on_deadline(at(0), budget(0)).items, 0);
    assert!(engine.retry.jobs[&(0, 0)].cursor.is_none());
    for index in 0..512 {
        let before = engine
            .partitions
            .values()
            .filter(|queue| queue.retry_at != at(0))
            .count();
        let progress = engine.on_deadline(at(0), budget(1));
        assert_eq!(progress.items, 1);
        assert_eq!(engine.retry.jobs[&(0, 0)].cursor, Some(partition(index)));
        let after = engine
            .partitions
            .values()
            .filter(|queue| queue.retry_at != at(0))
            .count();
        assert_eq!(after - before, usize::from(index % 2 == 0));
        if index % 2 == 0 {
            assert_eq!(engine.partitions[&partition(index)].retry_at, at(27));
        }
    }
    assert!(engine.retry_pending(0, 0));
    assert_eq!(engine.on_deadline(at(0), budget(1)).items, 1);
    assert!(!engine.retry_pending(0, 0));
    assert!(!engine.has_retry_work());
    assert!(
        engine
            .next_deadline()
            .is_some_and(|deadline| deadline > at(0))
    );
}

#[test]
fn duplicate_failure_and_unchanged_metadata_do_not_restart_or_extend_backoff() {
    let (mut engine, handle) = setup(8);
    let key = connection(&mut engine, 0);
    engine.retry_connection(key, at(10));
    let epoch = engine.retry.topology;
    for step in 0..9 {
        engine.retry_connection(key, at(100 + step));
        engine.apply_metadata(at(0), &[handle], update(8)).unwrap();
        assert_eq!(engine.retry.topology, epoch);
        assert!(engine.retry_step());
    }
    assert!(!engine.has_retry_work());
    engine.retry_connection(key, at(1000));
    assert!(!engine.has_retry_work());
    assert_eq!(engine.partitions[&partition(0)].retry_at, at(20));
}

#[test]
fn behind_cursor_insert_and_removed_future_keys_coalesce_one_restart() {
    let (mut engine, _) = setup(4);
    engine.partitions.remove(&partition(0));
    let key = connection(&mut engine, 0);
    engine.retry_connection(key, at(10));
    assert!(engine.retry_step());
    assert_eq!(engine.retry.jobs[&(0, 0)].cursor, Some(partition(1)));
    insert_partition(&mut engine, 0);
    engine.partitions.remove(&partition(3));
    // Two further real topology changes before the boundary still require only
    // one fresh sweep, not a queued restart entry per notification.
    engine.retry_topology_changed();
    engine.retry_topology_changed();
    let mut visits = 1;
    while engine.has_retry_work() {
        assert!(visits < 8);
        assert!(engine.retry_step());
        visits += 1;
    }
    assert_eq!(visits, 7);
    assert_eq!(engine.partitions[&partition(0)].retry_at, at(20));
}

#[test]
fn jobs_rotate_fairly_and_failure_or_fully_drained_close_clear_one_per_item() {
    for fail in [false, true] {
        let (mut engine, _) = setup(8);
        let first = connection(&mut engine, 0);
        let second = connection(&mut engine, 1);
        engine.retry_connection(first, at(10));
        engine.retry_connection(second, at(10));
        engine.retry_step();
        engine.retry_step();
        assert_eq!(engine.retry.jobs[&(0, 0)].cursor, Some(partition(0)));
        assert_eq!(engine.retry.jobs[&(1, 0)].cursor, Some(partition(0)));
        if fail {
            engine.fail_producer_at(at(10), FailureReason::RuntimeFailed);
        } else {
            engine.close(at(10), at(100), RecordToken(0)).unwrap();
        }
        assert_eq!(engine.on_deadline(at(0), budget(0)).items, 0);
        assert_eq!(engine.retry.jobs.len(), 2);
        assert!(engine.retry_step());
        assert_eq!(engine.retry.jobs.len(), 1);
        assert!(engine.retry_step());
        assert!(!engine.has_retry_work());
    }
}

#[test]
fn impossible_job_capacity_fails_closed_before_an_unfenced_route_can_dispatch() {
    let (mut engine, _) = setup(2);
    let first = connection(&mut engine, 0);
    let second = connection(&mut engine, 1);
    engine.validated.max_connections = 1;
    engine.retry_connection(first, at(10));
    engine.retry_connection(second, at(10));
    assert!(engine.is_failed());
    assert_eq!(engine.retry.jobs.len(), 1);
}
