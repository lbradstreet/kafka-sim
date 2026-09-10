//! Actor-level topic/control lifecycle regressions with exact replay and the
//! independent committed-log, delivery, release, flush and credit oracles.
mod support;
use kr_kafka_producer::types::{DeliveryKind, FailureReason};
use kr_kafka_sim::{DomainEvent, TopicSpec, Workload};
use kr_runtime::RuntimeDuration;
use support::{MS, record, run, scenario, settle};

fn close() -> Workload {
    Workload::Close {
        deadline_ns: 2_000 * MS,
    }
}

#[test]
fn initially_absent_topic_created_before_resolution_deadline_delivers() {
    let mut manifest = scenario();
    manifest.initially_absent_topics = vec![0];
    manifest.producer.topic_resolve_timeout = RuntimeDuration::from_nanos(500 * MS);
    manifest.workload = vec![
        Workload::Submit {
            records: vec![record(1, 0, 0)],
        },
        Workload::Sleep { nanos: 30 * MS },
        Workload::CreateTopic { topic: 0 },
        settle(1, true),
        Workload::Flush,
        Workload::AwaitFlush {
            timeout_ns: 500 * MS,
            require_acked: true,
        },
        close(),
    ];
    let report = run(&manifest);
    assert_eq!(report.coverage.acked, 1);
    assert!(
        report.history.entries.iter().any(|entry| {
            matches!(&entry.event, DomainEvent::BrokerRequest { api: 3, .. })
                && entry.now_ns < manifest.start_ns + 30 * MS
        }),
        "Metadata must actually observe the absent topic before creation"
    );
    assert!(report.history.entries.iter().any(|entry| {
        matches!(&entry.event, DomainEvent::TopicReady { id, .. } if *id == manifest.topics[0].id)
    }));
}

#[test]
fn unresolved_topic_deadline_terminates_and_flush_is_still_a_completion_barrier() {
    let mut manifest = scenario();
    manifest.require_all_acked = false;
    manifest.minimum_acked = 0;
    manifest.initially_absent_topics = vec![0];
    manifest.producer.topic_resolve_timeout = RuntimeDuration::from_nanos(80 * MS);
    manifest.workload = vec![
        Workload::Submit {
            records: vec![record(1, 0, 0), record(2, 0, 0)],
        },
        Workload::Flush,
        Workload::AwaitFlush {
            timeout_ns: 500 * MS,
            require_acked: false,
        },
        settle(2, false),
        close(),
    ];
    let report = run(&manifest);
    assert_eq!(report.coverage.not_written, 2);
    assert_eq!(report.coverage.unknown, 0);
    let deliveries: Vec<_> = report
        .history
        .entries
        .iter()
        .filter(|entry| matches!(entry.event, DomainEvent::Delivery { .. }))
        .collect();
    for entry in &deliveries {
        assert!(entry.now_ns >= manifest.start_ns + 80 * MS);
        assert!(matches!(entry.event, DomainEvent::Delivery {
            outcome, reason, attempts: 0, offset: None, timestamp: None, ..
        } if outcome == DeliveryKind::NotWritten as u32 && reason == FailureReason::TopicResolution as u32));
    }
    let done = report
        .history
        .entries
        .iter()
        .find(|entry| matches!(entry.event, DomainEvent::FlushDone { .. }))
        .unwrap();
    assert!(
        deliveries
            .iter()
            .all(|delivery| delivery.ordinal < done.ordinal)
    );
    assert!(
        !report
            .history
            .entries
            .iter()
            .any(|entry| matches!(entry.event, DomainEvent::BrokerRequest { api: 0, .. }))
    );
}

#[test]
fn closing_topic_after_real_commit_retains_ambiguity_and_unrelated_progress() {
    use kr_kafka_sim::faults::{Effects, Match, Phase, ScriptRule};
    let mut manifest = scenario();
    manifest.require_all_acked = false;
    manifest.topics.push(TopicSpec {
        id: [42; 16],
        name: "unrelated".into(),
        leaders: vec![manifest.brokers[1].id],
    });
    manifest.faults.scripts.push(ScriptRule {
        matcher: Match {
            api: Some(0),
            broker: Some(manifest.brokers[0].id),
            round: Some(1),
            phase: Phase::BeforeResponse,
            ..Match::default()
        },
        skip: 0,
        take: 1,
        effects: Effects {
            delay_ns: 100 * MS,
            ..Effects::default()
        },
    });
    manifest.workload = vec![
        Workload::Submit {
            records: vec![record(1, 0, 0), record(2, 1, 0)],
        },
        settle(2, true),
        Workload::BeginRound { round: 1 },
        Workload::Submit {
            records: vec![record(3, 0, 0), record(4, 1, 0)],
        },
        // Explicit virtual delay lets both small requests reach their brokers;
        // the actual positive commit and still-withheld response are checked.
        Workload::Sleep { nanos: 20 * MS },
        Workload::CloseTopic { topic: 0 },
        Workload::Flush,
        Workload::AwaitFlush {
            timeout_ns: 2_000 * MS,
            require_acked: false,
        },
        settle(4, false),
        close(),
    ];
    let report = run(&manifest);
    let fence = report.history.entries.iter().find(|entry| {
        matches!(&entry.event, DomainEvent::WorkloadStep { action, .. } if action == "CloseTopic:0")
    }).unwrap();
    let (connection, correlation) = report
        .history
        .entries
        .iter()
        .find_map(|entry| match &entry.event {
            DomainEvent::BrokerRequest {
                connection,
                correlation,
                api: 0,
                records,
                ..
            } if records.contains(&3) => Some((*connection, *correlation)),
            _ => None,
        })
        .expect("closing record actually reached its broker");
    assert!(
        report.history.entries.iter().any(|entry| {
            entry.ordinal < fence.ordinal
                && matches!(entry.event, DomainEvent::BrokerCommit {
            connection: found, correlation: found_correlation, records: 1, batches: 1,
        } if found == connection && found_correlation == correlation)
        }),
        "the withheld response follows an actual append, not an error/dedup reply"
    );
    assert!(!report.history.entries.iter().any(|entry| {
        entry.ordinal < fence.ordinal
            && matches!(entry.event, DomainEvent::ResponseRead {
            connection: found, correlation: found_correlation,
        } if found == connection && found_correlation == correlation)
    }));
    for entry in &report.history.entries {
        if let DomainEvent::Delivery {
            record_id,
            outcome,
            reason,
            attempts,
            topic,
            ..
        } = entry.event
        {
            if record_id == 3 {
                assert!(entry.ordinal > fence.ordinal);
                assert_eq!(topic, manifest.topics[0].id);
                assert_eq!(outcome, DeliveryKind::Unknown as u32);
                assert_eq!(reason, FailureReason::Closed as u32);
                assert_eq!(attempts, 1);
            } else {
                assert_eq!(outcome, DeliveryKind::Acked as u32);
            }
        }
    }
    assert_eq!((report.coverage.acked, report.coverage.unknown), (3, 1));
}

#[test]
fn closing_one_topic_preserves_other_topic_and_reopen_preserves_uuid() {
    let mut manifest = scenario();
    manifest.require_all_acked = false;
    let first = manifest.topics[0].id;
    manifest.topics.push(TopicSpec {
        id: [42; 16],
        name: "other".into(),
        leaders: vec![manifest.brokers[1].id],
    });
    manifest.workload = vec![
        Workload::Submit {
            records: vec![record(1, 0, 0), record(2, 1, 0)],
        },
        settle(2, true),
        // The application admits these before publishing the close fence.
        Workload::Submit {
            records: vec![record(3, 0, 0), record(4, 1, 0)],
        },
        Workload::CloseTopic { topic: 0 },
        Workload::Flush,
        Workload::AwaitFlush {
            timeout_ns: 2_000 * MS,
            require_acked: false,
        },
        settle(4, false),
        Workload::OpenTopic { topic: 0 },
        Workload::Submit {
            records: vec![record(5, 0, 0), record(6, 1, 0)],
        },
        settle(6, false),
        close(),
    ];
    let report = run(&manifest);
    for entry in &report.history.entries {
        if let DomainEvent::Delivery {
            record_id,
            topic,
            outcome,
            reason,
            ..
        } = entry.event
        {
            if record_id == 3 {
                assert_eq!(topic, first);
                assert_eq!(outcome, DeliveryKind::NotWritten as u32);
                assert_eq!(reason, FailureReason::Closed as u32);
            } else {
                assert_eq!(outcome, DeliveryKind::Acked as u32, "record {record_id}");
                if record_id == 1 || record_id == 5 {
                    assert_eq!(topic, first);
                }
            }
        }
    }
    assert_eq!(report.coverage.acked, 5);
    assert_eq!(report.coverage.not_written, 1);
}

#[test]
fn recreate_and_partition_growth_route_new_records_to_new_identity_and_partition() {
    let mut manifest = scenario();
    let original = manifest.topics[0].id;
    let replacement = [73; 16];
    manifest.workload = vec![
        Workload::Submit {
            records: vec![record(1, 0, 0)],
        },
        settle(1, true),
        Workload::CloseTopic { topic: 0 },
        Workload::Flush,
        Workload::AwaitFlush {
            timeout_ns: 2_000 * MS,
            require_acked: true,
        },
        Workload::RecreateTopic {
            topic: 0,
            new_id: replacement,
        },
        Workload::OpenTopic { topic: 0 },
        Workload::Submit {
            records: vec![record(2, 0, 0)],
        },
        settle(2, true),
        Workload::AddPartitions {
            topic: 0,
            additional_leaders: vec![manifest.brokers[1].id],
        },
        Workload::CloseTopic { topic: 0 },
        Workload::Flush,
        Workload::AwaitFlush {
            timeout_ns: 2_000 * MS,
            require_acked: true,
        },
        Workload::OpenTopic { topic: 0 },
        Workload::Submit {
            records: vec![record(3, 0, 1), record(4, 0, 0)],
        },
        settle(4, true),
        Workload::Flush,
        Workload::AwaitFlush {
            timeout_ns: 2_000 * MS,
            require_acked: true,
        },
        close(),
    ];
    let report = run(&manifest);
    assert_eq!(report.coverage.acked, 4);
    for entry in &report.history.entries {
        if let DomainEvent::Delivery {
            record_id,
            topic,
            partition,
            ..
        } = entry.event
        {
            assert_eq!(
                topic,
                if record_id == 1 {
                    original
                } else {
                    replacement
                }
            );
            assert_eq!(partition, i32::from(record_id == 3));
        }
    }
}
