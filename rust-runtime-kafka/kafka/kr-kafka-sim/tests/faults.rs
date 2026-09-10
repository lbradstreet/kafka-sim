//! Fault cuts are tested against actual broker commits and provider retirement.
mod support;
use kr_kafka_sim::{DomainEvent, Workload, faults::*};
use support::{MS, record, run, scenario, settle};
fn sequence() -> Vec<Workload> {
    vec![
        Workload::Submit {
            records: vec![record(1, 0, 0)],
        },
        settle(1, true),
        Workload::Close {
            deadline_ns: 2_000 * MS,
        },
    ]
}

#[test]
fn bootstrap_endpoints_select_the_actual_broker_during_an_outage() {
    for first_down in [false, true] {
        let mut manifest = scenario();
        manifest.topics[0].leaders = vec![2];
        manifest.faults.isolations = vec![IsolationWindow {
            broker: 1,
            start_ns: 0,
            end_ns: 5_000 * MS,
        }];
        manifest.producer.bootstrap = manifest
            .brokers
            .iter()
            .filter(|broker| first_down || broker.id == 2)
            .map(|broker| kr_kafka_producer::config::BrokerEndpoint {
                host: broker.host.clone(),
                port: broker.port,
            })
            .collect();
        manifest.workload = sequence();
        let report = run(&manifest);
        assert_eq!(report.coverage.acked, 1);
        let mut live = std::collections::BTreeSet::new();
        let mut recovered = false;
        for entry in &report.history.entries {
            match &entry.event {
                DomainEvent::ConnectionOpened {
                    connection, broker, ..
                } => {
                    assert!(live.insert(*connection));
                    if *broker == 2 && entry.now_ns < manifest.start_ns + 1_000 * MS {
                        recovered = true;
                    }
                }
                DomainEvent::ConnectionClosed { connection, .. } => {
                    assert!(live.remove(connection))
                }
                DomainEvent::BrokerRequest { connection, .. } => assert!(live.contains(connection)),
                _ => {}
            }
        }
        assert!(recovered, "first_down={first_down}");
        assert!(live.is_empty());
        assert_eq!(report.fault_stats.setup_failures > 0, first_down);
    }
}

#[test]
fn broker_endpoints_are_unambiguous_before_connection_setup() {
    let mut manifest = scenario();
    manifest.workload = sequence();
    manifest.brokers[1].port = manifest.brokers[0].port;
    assert!(manifest.validate().is_err());
}
fn rule(phase: Phase, outcome: Outcome) -> ScriptRule {
    ScriptRule {
        matcher: Match {
            phase,
            api: Some(0),
            ..Default::default()
        },
        skip: 0,
        take: 1,
        effects: Effects {
            outcome,
            ..Default::default()
        },
    }
}
#[test]
fn request_loss_and_committed_response_loss_have_distinct_append_evidence() {
    for phase in [
        Phase::BeforeAppend,
        Phase::AfterAppend,
        Phase::BeforeResponse,
    ] {
        for outcome in [Outcome::Drop, Outcome::Disconnect] {
            let mut manifest = scenario();
            manifest.workload = sequence();
            manifest.faults.scripts = vec![rule(phase, outcome)];
            let report = run(&manifest);
            assert_eq!(report.coverage.acked, 1);
            let decision = report
                .manifest
                .fault_decisions
                .as_ref()
                .unwrap()
                .iter()
                .find(|decision| decision.effects.outcome == outcome)
                .unwrap();
            let matching_commit = report.history.entries.iter().find(|entry| {
                matches!(entry.event, DomainEvent::BrokerCommit {connection, correlation, records: 1, batches: 1}
                    if connection == decision.hook.connection && Some(correlation) == decision.hook.correlation)
            });
            assert_eq!(matching_commit.is_some(), phase != Phase::BeforeAppend);
            assert_eq!(
                report.fault_stats.committed_response_losses,
                u64::from(phase != Phase::BeforeAppend)
            );
            let commits: u32 = report
                .history
                .entries
                .iter()
                .filter_map(|entry| match entry.event {
                    DomainEvent::BrokerCommit { records, .. } => Some(records),
                    _ => None,
                })
                .sum();
            assert_eq!(commits, 1, "retry cannot create a second committed record");
            assert!(report.coverage.retries > 0);
            if let Some(commit) = matching_commit {
                let cut = report
                    .history
                    .entries
                    .iter()
                    .find(|entry| {
                        matches!(&entry.event,
                    DomainEvent::FaultDecision(found) if found.hook_id == decision.hook_id)
                    })
                    .unwrap();
                assert!(commit.ordinal < cut.ordinal);
            }
        }
    }
}
#[test]
fn isolation_closes_idle_connections_at_start_without_waiting_for_next_request() {
    let mut manifest = scenario();
    manifest.faults.isolations = vec![IsolationWindow {
        broker: manifest.brokers[0].id,
        start_ns: 20 * MS,
        end_ns: 50 * MS,
    }];
    manifest.workload = vec![
        Workload::Submit {
            records: vec![record(1, 0, 0)],
        },
        settle(1, true),
        Workload::Sleep { nanos: 60 * MS },
        Workload::Submit {
            records: vec![record(2, 0, 0)],
        },
        settle(2, true),
        Workload::Close {
            deadline_ns: 2_000 * MS,
        },
    ];
    let report = run(&manifest);
    assert_eq!(report.coverage.acked, 2);
    assert!(report.fault_stats.isolation_closed > 0);
    let tape = report.manifest.fault_decisions.as_ref().unwrap();
    let start = tape
        .iter()
        .find(|decision| decision.hook.phase == Phase::IsolationStart)
        .unwrap();
    let end = tape
        .iter()
        .find(|decision| decision.hook.phase == Phase::IsolationEnd)
        .unwrap();
    assert_eq!(start.hook.now_ns, 20 * MS);
    assert_eq!(end.hook.now_ns, 50 * MS);
    assert!(
        report.history.entries.iter().any(|entry| matches!(
            entry.event,
            DomainEvent::IsolationClosed { .. }
        ) && entry.now_ns >= manifest.start_ns + 20 * MS
            && entry.now_ns < manifest.start_ns + 50 * MS),
        "idle provider reads must notice the timer's actual close before another request"
    );
}
#[test]
fn setup_crossing_isolation_is_closed_and_finite_failure_recovers() {
    let mut manifest = scenario();
    manifest.workload = sequence();
    manifest.faults.scripts = vec![ScriptRule {
        matcher: Match {
            phase: Phase::Setup,
            connection: Some(1),
            ..Default::default()
        },
        skip: 0,
        take: 1,
        effects: Effects {
            delay_ns: 30 * MS,
            ..Default::default()
        },
    }];
    manifest.faults.isolations = vec![IsolationWindow {
        broker: 1,
        start_ns: 10 * MS,
        end_ns: 60 * MS,
    }];
    let report = run(&manifest);
    assert_eq!(report.coverage.acked, 1);
    let tape = report.manifest.fault_decisions.as_ref().unwrap();
    assert!(
        tape.iter()
            .any(|decision| decision.hook.phase == Phase::Setup
                && decision.effects.outcome == Outcome::SetupFailure)
    );
    assert!(
        !report.history.entries.iter().any(|entry| matches!(
            entry.event,
            DomainEvent::BrokerRequest { connection: 1, .. }
        )),
        "isolated in-progress setup cannot reach broker API parsing"
    );
}
#[test]
fn pure_setup_failure_is_finite_and_fault_source_replay_remains_exact() {
    let mut manifest = scenario();
    manifest.workload = sequence();
    manifest.faults.scripts = vec![ScriptRule {
        matcher: Match {
            phase: Phase::Setup,
            ..Default::default()
        },
        skip: 0,
        take: 1,
        effects: Effects {
            outcome: Outcome::SetupFailure,
            ..Default::default()
        },
    }];
    manifest.faults.random = vec![RandomRule {
        matcher: Match {
            phase: Phase::BeforeResponse,
            api: Some(0),
            ..Default::default()
        },
        probability_ppm: 0,
        max_delay_ns: MS,
        outcome: Outcome::Disconnect,
    }];
    let report = kr_kafka_sim::verify_trace_transparency(&manifest).unwrap();
    assert_eq!(report.coverage.acked, 1);
    let tape = report.manifest.fault_decisions.as_ref().unwrap();
    assert_eq!(
        tape.iter()
            .filter(|decision| decision.effects.outcome == Outcome::SetupFailure)
            .count(),
        1
    );
    assert!(
        tape.iter()
            .any(|decision| decision.draws.len() == 2 && decision.effects == Effects::default())
    );
    let mut corrupt = report.manifest;
    corrupt
        .fault_decisions
        .as_mut()
        .unwrap()
        .iter_mut()
        .find(|row| !row.draws.is_empty())
        .unwrap()
        .draws[0] ^= 1;
    let error = kr_kafka_sim::run(&corrupt).unwrap_err();
    assert!(
        error.reason.contains("fault replay RNG draw mismatch"),
        "{}",
        error.reason
    );
}

#[test]
fn modeled_setup_delay_applies_to_failures_and_respects_connection_deadline() {
    for (delay_ns, outcome, expected) in [
        (30 * MS, Outcome::SetupFailure, 30 * MS),
        (1_000 * MS, Outcome::Continue, 200 * MS),
        (1_000 * MS, Outcome::SetupFailure, 200 * MS),
    ] {
        let mut manifest = scenario();
        manifest.workload = sequence();
        manifest.faults.scripts = vec![ScriptRule {
            matcher: Match {
                phase: Phase::Setup,
                connection: Some(1),
                ..Default::default()
            },
            skip: 0,
            take: 1,
            effects: Effects {
                delay_ns,
                outcome,
                ..Default::default()
            },
        }];
        let report = run(&manifest);
        let starts: Vec<_> = report
            .manifest
            .fault_decisions
            .as_ref()
            .unwrap()
            .iter()
            .filter(|decision| decision.hook.phase == Phase::Setup)
            .collect();
        assert!(starts.len() >= 2);
        let elapsed = starts[1].hook.now_ns - starts[0].hook.now_ns;
        assert!(
            elapsed >= expected,
            "modeled delay was skipped: {elapsed} < {expected}"
        );
        assert!(
            elapsed < expected + 100 * MS,
            "setup exceeded deadline plus bounded retry: {elapsed}"
        );
        assert_eq!(report.fault_stats.script_firings, [1]);
        assert_eq!(report.coverage.acked, 1);
    }
}
