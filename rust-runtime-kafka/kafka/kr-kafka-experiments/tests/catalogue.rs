use kr_kafka_experiments::{Size, catalogue, run_scenario};
use std::collections::BTreeSet;
#[test]
fn scenario_ids_and_variant_names_are_unique_and_manifests_validate() {
    let mut ids = BTreeSet::new();
    for s in catalogue() {
        assert!(ids.insert(s.id));
        let mut names = BTreeSet::new();
        for v in &s.variants {
            assert!(names.insert(&v.name));
            for size in [Size::Test, Size::Full] {
                s.build(v, 0, size)
                    .unwrap_or_else(|e| panic!("{} / {} / {size:?}: {e}", s.id, v.name));
            }
        }
    }
}

#[test]
fn independent_pressure_gate_rejects_missing_healthy_acknowledgments() {
    use kr_kafka_sim::DomainEvent as E;
    let s = catalogue()
        .into_iter()
        .find(|s| s.id == "hard.partition-admission-isolation")
        .unwrap();
    let v = s
        .variants
        .iter()
        .find(|v| v.params.partition_pressure == Some(true))
        .unwrap();
    let m = s.build(v, 0, Size::Test).unwrap();
    let mut r = kr_kafka_sim::run_replayed(&m).unwrap();
    s.invariants(&r).unwrap();
    r.history.entries.retain(|e| {
        !matches!(e.event,E::Delivery {partition:1,outcome:0,..}
        if (m.start_ns+10_000_000_000..m.start_ns+10_100_000_000).contains(&e.now_ns))
    });
    assert!(
        s.invariants(&r)
            .unwrap_err()
            .contains("healthy progress gate")
    );
}

#[test]
fn sparse_trial_respects_existing_model_and_report_capacity_bounds() {
    let s = catalogue()
        .into_iter()
        .find(|s| s.id == "baseline.partition-admission-skew")
        .unwrap();
    let v = s
        .variants
        .iter()
        .find(|v| v.name == "sparse1024-rate8000-pressure")
        .unwrap();
    let mut m = s.build(v, 0, Size::Test).unwrap();
    m.topics[0].leaders.push(1);
    assert!(m.validate().is_err());
    let r = run_scenario(&s, v, 0, Size::Test).unwrap();
    assert_eq!(r.topology["partitions"].as_array().unwrap().len(), 1024);
    assert!(r.to_json().unwrap().len() <= kr_kafka_experiments::report::MAX_REPORT_BYTES);
    let mut corrupt = r;
    corrupt.topology["partitions"][0]["partition"] = serde_json::json!(1024);
    assert!(corrupt.validate().is_err());
}
#[test]
fn every_test_fixture_replays_and_satisfies_invariants_and_required_phase_coverage() {
    for s in catalogue() {
        for v in &s.variants {
            let r = run_scenario(&s, v, 0, Size::Test)
                .unwrap_or_else(|e| panic!("{} / {}: {e}", s.id, v.name));
            assert!(!r.phase_evidence.is_empty());
        }
    }
}

#[test]
fn request_policies_preserve_catalogue_safety_replay_and_partition_isolation() {
    use kr_kafka_producer::config::RequestBatchingPolicy::{BrokerReady, SinglePartition};
    for policy in [SinglePartition, BrokerReady] {
        for s in catalogue() {
            for v in &s.variants {
                let mut manifest = s.build(v, 0, Size::Test).unwrap();
                manifest.producer.request_batching_policy = policy;
                let run = kr_kafka_sim::run_replayed(&manifest)
                    .unwrap_or_else(|e| panic!("{policy:?} {} / {}: {e}", s.id, v.name));
                kr_kafka_experiments::derive_checked(&s, v, Size::Test, true, &run)
                    .unwrap_or_else(|e| panic!("{policy:?} {} / {}: {e}", s.id, v.name));
                if policy == SinglePartition {
                    for event in &run.history.entries {
                        if let kr_kafka_sim::DomainEvent::ClientRequestDispatched {
                            api: 0,
                            batches,
                            ..
                        } = &event.event
                        {
                            assert_eq!(batches.len(), 1, "{} / {}", s.id, v.name);
                        }
                    }
                }
            }
        }
    }
}
#[test]
fn report_derivation_is_stable_for_a_pinned_scenario() {
    let s = catalogue().remove(0);
    let r = run_scenario(&s, &s.variants[0], 0, Size::Test).unwrap();
    assert_eq!(r.summary["records"]["offered"], 128);
    assert_eq!(r.summary["records"]["accepted"], 128);
    assert_eq!(r.summary["records"]["acked"], 128);
    assert_eq!(r.summary["records"]["refused"], 0);
    assert_eq!(r.summary["client_requests"], 145);
    assert_eq!(r.summary["broker_requests"], 145);
    assert_eq!(r.summary["bytes_wire"], 87_383);
    assert_eq!(r.summary["first_dispatch_batch_bytes"]["count"], 128);
    assert_eq!(r.summary["first_dispatch_batch_bytes"]["sum"], 79_360);
}

#[test]
fn characterized_comparisons_require_full_size_seed_and_both_rate_endpoints() {
    use kr_kafka_experiments::{ExperimentBundle, report::BUNDLE_SCHEMA};
    use serde_json::json;
    let s = catalogue()
        .into_iter()
        .find(|s| s.id == "baseline.open-loop-rate")
        .unwrap();
    let mut runs = vec![];
    for name in ["rate500", "rate32000"] {
        let v = s.variants.iter().find(|v| v.name == name).unwrap();
        runs.push(run_scenario(&s, v, 0, Size::Test).unwrap());
    }
    let mut bundle = ExperimentBundle {
        schema: BUNDLE_SCHEMA.into(),
        scenario: runs[0].meta["scenario"].clone(),
        variants: s
            .variants
            .iter()
            .enumerate()
            .map(|(i, v)| json!({"name":v.name,"deltas":v.params,"order":i}))
            .collect(),
        seeds: vec!["0".into()],
        runs,
        comparisons: vec![],
        page: json!({"index":0,"count":1,"total_runs":2}),
    };
    bundle.validate().unwrap();
    assert!(
        s.comparisons(&bundle)
            .iter()
            .all(|c| c.status == "not-applicable")
    );
    // Synthetic prerequisite/threshold test. Full population/phase verification
    // is exercised separately by the measured release pilots and CLI full suite.
    for (i, r) in bundle.runs.iter_mut().enumerate() {
        r.meta["size"] = json!("full");
        r.meta["workload"]["planned_offers"] = json!(if i == 0 { 10_000 } else { 640_000 });
    }
    assert!(s.comparisons(&bundle).iter().all(|c| c.status == "passed"));
    bundle.runs[0].summary["latency_acked"]["p99"] = json!(20_000_000);
    assert!(s.comparisons(&bundle).iter().any(|c| c.status == "failed"));
    bundle.runs[0].meta["seed"] = json!("1");
    assert!(
        s.comparisons(&bundle)
            .iter()
            .all(|c| c.status == "not-applicable")
    );
}

#[test]
fn outage_phase_evidence_rejects_missing_healthy_progress_and_uses_exact_boundaries() {
    use kr_kafka_sim::DomainEvent as E;
    let s = catalogue()
        .into_iter()
        .find(|s| s.id == "hard.crash-restart-closed")
        .unwrap();
    let m = s.build(&s.variants[0], 0, Size::Test).unwrap();
    let r = kr_kafka_sim::run_replayed(&m).unwrap();
    s.phase_coverage(&r).unwrap();
    let mut connections = std::collections::BTreeMap::new();
    for e in &r.history.entries {
        if let E::ConnectionOpened {
            connection, broker, ..
        } = e.event
        {
            connections.insert(connection, broker);
        }
    }
    let start = m.start_ns + 10_000_000_000;
    let end = m.start_ns + 13_000_000_000;
    let mut no_progress = r.clone();
    no_progress.history.entries.retain(|e|!matches!(e.event,E::BrokerCommit{connection,..} if connections[&connection]!=1&&(start..end).contains(&e.now_ns)));
    assert!(s.phase_coverage(&no_progress).is_err());
    let commit = r
        .history
        .entries
        .iter()
        .find(|e| matches!(e.event,E::BrokerCommit{connection,..} if connections[&connection]==1))
        .unwrap();
    let mut edge = r.clone();
    let mut injected = commit.clone();
    injected.now_ns = end;
    edge.history.entries.push(injected.clone());
    s.invariants(&edge).unwrap();
    injected.now_ns = start;
    edge.history.entries.push(injected);
    assert!(s.invariants(&edge).is_err());
}

#[test]
fn long_outage_evidence_requires_every_post_expiry_probe_to_acknowledge() {
    use kr_kafka_sim::DomainEvent as E;
    let s = catalogue()
        .into_iter()
        .find(|s| s.id == "hard.short-vs-long-outage")
        .unwrap();
    for v in s.variants.iter().filter(|v| v.name.starts_with("outage8s")) {
        let m = s.build(v, 0, Size::Test).unwrap();
        let mut r = kr_kafka_sim::run_replayed(&m).unwrap();
        s.phase_coverage(&r).unwrap();
        let end = m.start_ns + m.faults.isolations[0].end_ns;
        let probe = r
            .history
            .entries
            .iter()
            .position(|e| e.now_ns >= end && matches!(e.event, E::Delivery { outcome: 0, .. }))
            .expect("post-expiry acknowledged probe");
        r.history.entries.remove(probe);
        assert!(
            s.phase_coverage(&r)
                .unwrap_err()
                .contains("post-expiry recovery probe")
        );
    }
}

#[test]
fn soft_fault_evidence_requires_hooks_commits_and_consumed_throttle_eligibility() {
    use kr_kafka_sim::DomainEvent as E;
    for id in ["soft.throttle-window", "soft.one-way-loss-responses"] {
        let s = catalogue().into_iter().find(|s| s.id == id).unwrap();
        let m = s.build(&s.variants[0], 0, Size::Test).unwrap();
        let r = kr_kafka_sim::run_replayed(&m).unwrap();
        s.phase_coverage(&r).unwrap();
        let mut broken = r.clone();
        if id == "soft.throttle-window" {
            broken
                .history
                .entries
                .retain(|e| !matches!(e.event, E::FaultDecision(_)));
            assert!(s.phase_coverage(&broken).is_err());
            let throttled: BTreeSet<_> = r
                .history
                .entries
                .iter()
                .filter_map(|e| match &e.event {
                    E::FaultDecision(d) if d.effects.throttle_ms > 0 => {
                        Some((d.hook.connection, d.hook.correlation.unwrap()))
                    }
                    _ => None,
                })
                .collect();
            let (connection, at) = r
                .history
                .entries
                .iter()
                .find_map(|e| match &e.event {
                    E::ClientRequestFinished {
                        connection,
                        correlation,
                        result,
                        ..
                    } if result == "Response"
                        && throttled.contains(&(*connection, *correlation)) =>
                    {
                        Some((*connection, e.now_ns))
                    }
                    _ => None,
                })
                .unwrap();
            let mut late = r.history.entries.iter().find(|e| matches!(e.event, E::ClientRequestDispatched{connection:c,api:0,..} if c==connection)).unwrap().clone();
            late.now_ns = at + 1;
            let mut broken = r.clone();
            let index = broken
                .history
                .entries
                .iter()
                .position(|e| e.now_ns > at)
                .unwrap();
            broken.history.entries.insert(index, late);
            assert!(
                s.phase_coverage(&broken)
                    .unwrap_err()
                    .contains("consumed connection throttle")
            );
        } else {
            let connections: BTreeSet<_> = r
                .history
                .entries
                .iter()
                .filter_map(|e| match e.event {
                    E::ConnectionOpened {
                        connection,
                        broker: 1,
                        ..
                    } => Some(connection),
                    _ => None,
                })
                .collect();
            let w = &m.faults.link_outages[0];
            broken.history.entries.retain(|e| !matches!(e.event, E::BrokerCommit{connection,..} if connections.contains(&connection) && (w.start_ns..w.end_ns).contains(&(e.now_ns-m.start_ns))));
            assert!(s.phase_coverage(&broken).is_err());
        }
    }
}

#[test]
fn resource_phase_evidence_rejects_unproved_pressure_and_consumption_during_pause() {
    use kr_kafka_sim::DomainEvent as E;
    for id in [
        "resources.memory-bounded-overload",
        "resources.stop-polling-backpressure",
    ] {
        let s = catalogue().into_iter().find(|s| s.id == id).unwrap();
        let m = s.build(&s.variants[0], 0, Size::Test).unwrap();
        let r = kr_kafka_sim::run_replayed(&m).unwrap();
        s.phase_coverage(&r).unwrap();
        let mut broken = r.clone();
        if id == "resources.memory-bounded-overload" {
            for i in 0..broken.history.entries.len() - 1 {
                if matches!(broken.history.entries[i].event, E::Refused { .. })
                    && let E::Credits { held, .. } = &mut broken.history.entries[i + 1].event
                {
                    held.fill(0);
                }
            }
            assert!(
                s.phase_coverage(&broken)
                    .unwrap_err()
                    .contains("specific admission pressure")
            );
        } else {
            let mut delivery = r
                .history
                .entries
                .iter()
                .find(|e| matches!(e.event, E::Delivery { .. }))
                .unwrap()
                .clone();
            delivery.now_ns = m.start_ns + 10_000_000_000;
            broken.history.entries.push(delivery);
            assert!(
                s.phase_coverage(&broken)
                    .unwrap_err()
                    .contains("paused consumption")
            );
        }
    }
}

#[test]
fn reopened_topic_cohort_requires_the_replacement_identity() {
    use kr_kafka_sim::DomainEvent as E;
    let s = catalogue()
        .into_iter()
        .find(|s| s.id == "topology.delete-recreate")
        .unwrap();
    let m = s.build(&s.variants[1], 0, Size::Test).unwrap();
    let mut r = kr_kafka_sim::run_replayed(&m).unwrap();
    s.phase_coverage(&r).unwrap();
    let first = m
        .experiment
        .as_ref()
        .unwrap()
        .loads
        .last()
        .unwrap()
        .template
        .first_id;
    let row = r
        .history
        .entries
        .iter_mut()
        .find(|e| matches!(e.event,E::Delivery{record_id,outcome:0,..} if record_id>=first))
        .unwrap();
    if let E::Delivery { topic, .. } = &mut row.event {
        *topic = m.topics[0].id;
    }
    assert!(s.phase_coverage(&r).unwrap_err().contains("old identity"));
}
