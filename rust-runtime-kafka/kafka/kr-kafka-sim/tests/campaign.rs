use kr_kafka_sim::{
    CampaignLimits, Coverage, ReplayManifest, Workload, run_replayed, verify_trace_transparency,
};
#[test]
fn seed_zero_real_actor_baseline_replays() {
    let manifest = ReplayManifest::from_seed(0, CampaignLimits::default()).unwrap();
    let report = run_replayed(&manifest)
        .unwrap_or_else(|failure| panic!("{failure}: {:?}", failure.history.entries.last()));
    assert!(report.coverage.acked > 0);
    assert!(report.coverage.partial_writes > 0);
    assert!(report.coverage.input_releases > 0);
    assert!(report.coverage.linger_seals > 0);
    assert!(report.coverage.target_seals > 0);
    assert!(report.batch_raw_bytes > 0);
    assert!(report.batch_target_bytes > 0);
}
#[test]
fn pinned_legacy_faults_replay_with_nonzero_aggregate_gates() {
    let mut total = Coverage::default();
    for seed in [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 12, 36] {
        let manifest = ReplayManifest::from_seed(seed, CampaignLimits::default()).unwrap();
        let report = run_replayed(&manifest)
            .unwrap_or_else(|failure| panic!("{failure}: {:?}", failure.history.entries.last()));
        assert!(report.coverage.acked > 0);
        assert!(!report.manifest.realized_faults.unwrap().is_empty());
        total.merge(&report.coverage).unwrap();
    }
    let mut old = ReplayManifest::from_seed(0, CampaignLimits::default()).unwrap();
    old.produce_max_version = 9;
    old.workload.truncate(2);
    old.workload.push(Workload::Close {
        deadline_ns: 2_000_000_000,
    });
    let old = run_replayed(&old).unwrap();
    assert_eq!(old.coverage.produce13, 0);
    assert_eq!(old.coverage.acked, 0);
    assert!(old.coverage.capability_rejections > 0);
    total.merge(&old.coverage).unwrap();
    total.require_campaign_gates().unwrap();
    let mut insufficient = total;
    insufficient.recreates = 0;
    assert!(insufficient.require_campaign_gates().is_err());
}
#[test]
fn recording_preserves_complete_history_and_runtime_checkpoint() {
    for seed in [0, 9, 36] {
        verify_trace_transparency(
            &ReplayManifest::from_seed(seed, CampaignLimits::default()).unwrap(),
        )
        .unwrap();
    }
    verify_trace_transparency(
        &kr_kafka_sim::campaign_manifest(1, kr_kafka_sim::CampaignVariant::FiniteFault).unwrap(),
    )
    .unwrap();
}

#[test]
fn sustained_broker_and_send_pressure_grow_batches_before_new_requests() {
    use kr_kafka_producer::config::Compression;
    use kr_kafka_sim::DomainEvent;
    use kr_runtime::RuntimeDuration;
    let mut baseline = ReplayManifest::from_seed(
        0,
        CampaignLimits {
            records: 64,
            ..CampaignLimits::default()
        },
    )
    .unwrap();
    baseline.topics[0].leaders = vec![baseline.brokers[0].id];
    baseline.producer.lanes = 1;
    baseline.producer.compression = Compression::None;
    baseline.producer.codec_contexts = 0;
    baseline.producer.record_descriptors = 128;
    baseline.producer.delivery_event_capacity = 128;
    baseline.producer.pending_records_per_topic = 128;
    baseline.producer.batch_target_bytes = 4096;
    baseline.producer.max_in_flight_per_connection = 1;
    baseline.producer.request_max_partitions = 1;
    baseline.producer.request_timeout = RuntimeDuration::from_nanos(1_000_000_000);
    baseline.producer.delivery_timeout = RuntimeDuration::from_nanos(3_000_000_000);
    baseline.producer.linger_max = RuntimeDuration::from_nanos(5_000_000);
    baseline.driver.encode_bytes = 4096;
    baseline.driver.chunk_bytes = 65536;
    baseline.driver.pipe_bytes = 65536;
    baseline.driver.service_delay_ns = 0;
    baseline.driver.link_latency_ns = 0;
    baseline.driver.jitter_ns = 0;
    let records: Vec<_> = baseline
        .workload
        .iter()
        .filter_map(|step| match step {
            Workload::Submit { records } => Some(records),
            _ => None,
        })
        .flatten()
        .cloned()
        .map(|mut record| {
            record.partition = 0;
            record.key = None;
            record.headers = kr_kafka_sim::identity_headers(record.id);
            record.native = false;
            record.value.as_mut().unwrap().truncate(128);
            record
        })
        .collect();
    baseline.workload = vec![
        Workload::Submit {
            records: vec![records[0].clone()],
        },
        Workload::WaitDeliveries { count: 1 },
    ];
    // The connection is warm before this paced stream starts. With dispatch
    // credit, five milliseconds of linger makes small batches. A full request
    // window or an unresolved write keeps the next batch open across that time.
    for record in &records[1..] {
        baseline.workload.push(Workload::Submit {
            records: vec![record.clone()],
        });
        baseline.workload.push(Workload::Sleep { nanos: 1_000_000 });
    }
    baseline
        .workload
        .push(Workload::WaitDeliveries { count: 64 });
    baseline.workload.push(Workload::Close {
        deadline_ns: 4_000_000_000,
    });
    let fast = run_replayed(&baseline)
        .unwrap_or_else(|error| panic!("{error}: {:?}", error.history.entries.last()));
    let mut slow_broker = baseline.clone();
    slow_broker.driver.service_delay_ns = 50_000_000;
    let broker = run_replayed(&slow_broker)
        .unwrap_or_else(|error| panic!("{error}: {:?}", error.history.entries.last()));
    let mut slow_send = baseline.clone();
    slow_send.producer.max_in_flight_per_connection = 5;
    slow_send.driver.chunk_bytes = 64;
    slow_send.driver.link_latency_ns = 1_000_000;
    let send = run_replayed(&slow_send)
        .unwrap_or_else(|error| panic!("{error}: {:?}", error.history.entries.last()));
    let batches = |report: &kr_kafka_sim::RunReport| -> Vec<usize> {
        report
            .history
            .entries
            .iter()
            .filter_map(|entry| match &entry.event {
                DomainEvent::BrokerRequest {
                    api: 0, records, ..
                } if !records.contains(&1) => Some(records.len()),
                _ => None,
            })
            .collect()
    };
    let fast_batches = batches(&fast);
    let broker_batches = batches(&broker);
    let send_batches = batches(&send);
    let mut raw_pressure = slow_broker.clone();
    raw_pressure.producer.batch_target_mode = kr_kafka_producer::config::BatchTargetMode::Raw;
    let raw_pressure = run_replayed(&raw_pressure).unwrap();
    assert_eq!(raw_pressure.coverage.acked, 64);
    assert!(
        broker_batches.iter().max() > batches(&raw_pressure).iter().max(),
        "default policy must grow beyond the legacy soft target while blocked"
    );
    assert_eq!(fast.coverage.acked, 64);
    assert_eq!(fast_batches.iter().sum::<usize>(), 63);
    assert_eq!(
        broker.pool_peaks[kr_kafka_producer::credit::Resource::RequestSlots as usize],
        1,
        "broker case reaches its one-request window"
    );
    assert!(
        send.pool_peaks[kr_kafka_producer::credit::Resource::RequestSlots as usize] < 5,
        "send case grows batches without filling the five-request window"
    );
    assert!(send.coverage.partial_writes > fast.coverage.partial_writes);
    for (label, report, batches) in [
        ("broker window", &broker, &broker_batches),
        ("send completion", &send, &send_batches),
    ] {
        assert_eq!(report.coverage.acked, 64, "{label}");
        assert_eq!(batches.iter().sum::<usize>(), 63, "{label}");
        assert!(
            batches.len() < fast_batches.len(),
            "{label}: {batches:?} vs {fast_batches:?}"
        );
        assert!(
            batches.iter().max().unwrap() >= &(2 * fast_batches.iter().max().unwrap()),
            "{label}: {batches:?} vs {fast_batches:?}"
        );
        // Close may force the accumulated batch before dispatch becomes ready;
        // broker-observed membership above is the pressure-growth witness.
    }
}

#[test]
fn pinned_command_histories_replay_with_complete_recovery() {
    use kr_kafka_sim::{CampaignVariant, PINNED_CASES, campaign_manifest};
    let mut faults = 0;
    let mut isolation = 0;
    let mut random = 0;
    let mut realized_orderings = std::collections::BTreeSet::new();
    for &(seed, variant) in PINNED_CASES {
        let manifest = campaign_manifest(seed, variant).unwrap();
        let report = run_replayed(&manifest).unwrap_or_else(|error| {
            panic!(
                "{} variant={}: {:?}",
                error,
                variant.as_str(),
                error.history.entries.last()
            )
        });
        assert_eq!(
            report.coverage.acked, 192,
            "seed={seed} variant={variant:?}"
        );
        assert_eq!(
            report.fetched_records, 192,
            "Fetch round trip seed={seed} variant={variant:?}"
        );
        assert_eq!(report.coverage.unknown, 0);
        assert_eq!(report.coverage.not_written, 0);
        let starts: Vec<_> = report
            .history
            .entries
            .iter()
            .enumerate()
            .filter_map(|(index, entry)| match &entry.event {
                kr_kafka_sim::DomainEvent::WorkloadStep { action, .. }
                    if action.starts_with("BeginRound:") =>
                {
                    Some(index)
                }
                _ => None,
            })
            .collect();
        assert_eq!(starts.len(), 7, "warmup and six realized rounds");
        for round in 1..=6 {
            let end = starts
                .get(round + 1)
                .copied()
                .unwrap_or(report.history.entries.len());
            let entries = &report.history.entries[starts[round]..end];
            let order: Vec<_> = entries
                .iter()
                .filter_map(|entry| {
                    if let kr_kafka_sim::DomainEvent::WorkloadStep { action, .. } = &entry.event {
                        Some(action.as_str())
                    } else {
                        None
                    }
                })
                .take_while(|action| *action != "AwaitFlush")
                .filter_map(|action| {
                    if action.starts_with("Submit:") {
                        Some("burst")
                    } else if action.starts_with("MoveLeader:") {
                        Some("leader")
                    } else if action == "Flush" {
                        Some("flush")
                    } else {
                        None
                    }
                })
                .collect();
            assert_eq!(
                order.iter().filter(|command| **command == "burst").count(),
                3
            );
            realized_orderings.insert(order);
            let prefix = entries
                .iter()
                .find(|entry| matches!(entry.event, kr_kafka_sim::DomainEvent::Flush { .. }))
                .unwrap();
            assert!(
                entries.iter().any(|entry| entry.ordinal > prefix.ordinal
                    && matches!(entry.event, kr_kafka_sim::DomainEvent::Accepted { .. })),
                "seed={seed} round={round}: prefix flush must exclude a later accepted burst"
            );
        }
        if variant != CampaignVariant::Clean {
            assert!(
                report.fault_stats.committed_response_losses > 0,
                "actual appended response loss seed={seed}"
            );
            assert!(
                report.fault_stats.script_effects >= 5,
                "required scripted rounds seed={seed}"
            );
        }
        if variant == CampaignVariant::Isolation {
            assert!(
                report.fault_stats.isolation_closed > 0,
                "real isolation closes seed={seed}"
            );
        }
        faults += report.fault_stats.script_effects;
        random += report.fault_stats.random_effects;
        isolation += report.fault_stats.isolation_closed;
    }
    assert!(faults > 0 && isolation > 0 && random > 0);
    assert!(
        realized_orderings.len() >= 4,
        "pinned executions must exercise distinct command orderings"
    );
}
