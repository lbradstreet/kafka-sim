#[allow(dead_code)]
mod support;
use kr_kafka_sim::{
    DomainEvent, ExperimentWorkload, LanePolicy, LoadShape, LoadSpec, Partitioning, PollingPause,
    RecordTemplate, ScheduledAction, TimedControl, ValuePattern,
};
use support::{MS, run, scenario};

fn load(first_id: u64, shape: LoadShape) -> LoadSpec {
    LoadSpec {
        template: RecordTemplate {
            first_id,
            topic: 0,
            partitioning: Partitioning::RoundRobin,
            value_bytes: 160,
            key_bytes: 8,
            lane: LanePolicy::Fixed(0),
            native: false,
            value_pattern: ValuePattern::Compressible,
        },
        shape,
    }
}
fn config(loads: Vec<LoadSpec>) -> ExperimentWorkload {
    ExperimentWorkload {
        loads,
        scheduled_actions: vec![],
        polling_pauses: vec![],
        offer_deadline_ns: 1_000 * MS,
        settle_timeout_ns: 8_000 * MS,
        close_timeout_ns: 2_000 * MS,
        require_acked: true,
    }
}

#[test]
fn compression_entropy_changes_preserve_all_payloads_with_bounded_contexts() {
    use kr_kafka_producer::config::{BatchTargetMode, Compression};
    use kr_kafka_producer::credit::Resource;
    for mode in [BatchTargetMode::Raw, BatchTargetMode::EstimatedWire] {
        let mut m = scenario();
        m.limits.records = 384;
        m.limits.record_bytes = 2048;
        m.topics[0].leaders = vec![m.brokers[0].id; 2];
        m.producer.batch_target_mode = mode;
        m.producer.compression = Compression::Zstd { level: 1 };
        m.producer.codec_contexts = 1;
        m.producer.batch_target_bytes = 4096;
        m.producer.batch_hard_bytes = 32 * 1024;
        m.producer.request_hard_bytes = 64 * 1024;
        m.producer.output_chunk_bytes = 16 * 1024;
        m.producer.record_descriptors = 256;
        m.producer.delivery_event_capacity = 256;
        m.producer.pending_records_per_topic = 256;
        m.producer.linger_skip_below_rate = None;
        let loads = (0..3)
            .map(|phase| {
                let mut source = load(
                    1 + phase * 128,
                    LoadShape::ClosedLoop {
                        start_ns: phase * 100 * MS,
                        count: 128,
                        outstanding: 32,
                    },
                );
                source.template.value_bytes = 2048;
                source.template.value_pattern = if phase == 1 {
                    ValuePattern::Incompressible { salt: 0x1234 }
                } else {
                    ValuePattern::Compressible
                };
                source
            })
            .collect();
        m.experiment = Some(config(loads));
        let report = run(&m);
        assert_eq!(report.coverage.acked, 384, "{mode:?}");
        assert_eq!(report.pool_peaks[Resource::CodecContexts as usize], 1);
        assert!(
            report.pool_peaks[Resource::CompressedBytes as usize]
                <= m.producer.compressed_bytes as u64
        );
    }
}

#[test]
fn periodic_metrics_preserve_interval_counts_scopes_replay_and_final_bank() {
    use kr_kafka_sim::{MetricScope, MetricsSampling};
    for duration in [20, 450] {
        let mut m = scenario();
        m.limits.records = 64;
        m.metrics_sampling = Some(MetricsSampling {
            interval_ns: 100 * MS,
        });
        m.producer.metrics.max_broker_scopes = 3;
        m.producer.metrics.max_partition_scopes = 2;
        m.producer.metrics.max_storage_bytes = 128 * 1024 * 1024;
        m.experiment = Some(config(vec![load(
            1,
            LoadShape::OpenLoop {
                start_ns: 0,
                end_ns: duration * MS,
                rate_per_s: 100,
            },
        )]));
        let r = run(&m);
        kr_kafka_sim::verify_trace_transparency(&m).unwrap();
        assert!(!r.metrics_samples.is_empty());
        assert!(r.missed_metrics_requests.is_empty());
        assert_eq!(r.metrics_samples[0].requested_ns.is_some(), duration > 100);
        let mut counts = [0u64; 11];
        let mut broker = false;
        let mut partition = false;
        for (i, sample) in r.metrics_samples.iter().enumerate() {
            assert_eq!(sample.epoch, i as u64 + 1);
            if let (Some(start), Some(end)) = (sample.start_ns, sample.end_ns) {
                assert!(start <= end && end <= sample.taken_ns);
            }
            for scope in &sample.scopes {
                broker |= matches!(scope.scope, MetricScope::Broker(_));
                partition |= matches!(scope.scope, MetricScope::Partition { topic_id, partition: 0 } if topic_id == m.topics[0].id);
                if scope.scope == MetricScope::Global {
                    for d in &scope.distributions {
                        counts[d.metric as usize] += d.count;
                    }
                }
            }
        }
        assert!(broker && partition);
        assert_eq!(counts, r.metrics_counts);
        assert_eq!(r.metrics_samples.len() > 1, duration > 100);
    }
}

#[test]
fn metrics_sampling_rejects_unbounded_or_disabled_configuration() {
    use kr_kafka_sim::MetricsSampling;
    let mut m = scenario();
    m.experiment = Some(config(vec![load(
        1,
        LoadShape::ClosedLoop {
            start_ns: 0,
            count: 16,
            outstanding: 1,
        },
    )]));
    for interval_ns in [0, 999_999, MS] {
        m.metrics_sampling = Some(MetricsSampling { interval_ns });
        assert!(m.validate().is_err());
    }
    m.metrics_sampling = Some(MetricsSampling {
        interval_ns: 100 * MS,
    });
    m.validate().unwrap();
    m.producer.metrics.enabled = false;
    assert!(m.validate().is_err());
}

#[test]
fn open_loop_keeps_its_origin_during_a_polling_pause_and_settles_only_accepted() {
    let mut m = scenario();
    m.limits.records = 64;
    m.producer.delivery_event_capacity = 4;
    m.producer.record_descriptors = 4;
    m.producer.pending_records_per_topic = 4;
    let mut e = config(vec![load(
        1,
        LoadShape::OpenLoop {
            start_ns: 0,
            end_ns: 64 * MS,
            rate_per_s: 1_000,
        },
    )]);
    e.polling_pauses = vec![PollingPause {
        start_ns: 10 * MS,
        end_ns: 50 * MS,
    }];
    m.experiment = Some(e);
    let r = run(&m);
    assert_eq!(r.coverage.offered, 64);
    assert_eq!(r.coverage.offered, r.coverage.accepted + r.coverage.refused);
    assert_eq!(r.coverage.acked, r.coverage.accepted);
    assert!(r.coverage.refused > 0);
    let mut offers = 0;
    for entry in &r.history.entries {
        match entry.event {
            DomainEvent::Offered {
                record_id, due_ns, ..
            } => {
                offers += 1;
                assert_eq!(due_ns, m.start_ns + (record_id - 1) * MS);
                assert_eq!(entry.now_ns, due_ns);
            }
            DomainEvent::Delivery { .. } => {
                assert!(!(m.start_ns + 10 * MS..m.start_ns + 50 * MS).contains(&entry.now_ns))
            }
            _ => {}
        }
    }
    assert_eq!(offers, 64);
}

#[test]
fn concurrent_sources_and_controls_have_stable_boundary_order_and_close_cancels_future_ids() {
    let mut m = scenario();
    m.limits.records = 100;
    let mut e = config(vec![
        load(
            1,
            LoadShape::ClosedLoop {
                start_ns: 0,
                count: 50,
                outstanding: 1,
            },
        ),
        load(
            100,
            LoadShape::OpenLoop {
                start_ns: 10 * MS,
                end_ns: 60 * MS,
                rate_per_s: 1_000,
            },
        ),
    ]);
    e.polling_pauses = vec![PollingPause {
        start_ns: 10 * MS,
        end_ns: 20 * MS,
    }];
    e.scheduled_actions = vec![
        ScheduledAction {
            at_ns: 10 * MS,
            action: TimedControl::MoveLeader {
                topic: 0,
                partition: 0,
                broker: m.brokers[1].id,
            },
        },
        ScheduledAction {
            at_ns: 10 * MS,
            action: TimedControl::Flush,
        },
        ScheduledAction {
            at_ns: 30 * MS,
            action: TimedControl::Close {
                deadline_ns: 2_000 * MS,
            },
        },
    ];
    m.experiment = Some(e);
    let r = run(&m);
    let boundary: Vec<_> = r
        .history
        .entries
        .iter()
        .filter(|e| e.now_ns == m.start_ns + 10 * MS)
        .collect();
    let pause = boundary
        .iter()
        .position(|e| matches!(e.event, DomainEvent::PollingChanged { paused: true, .. }))
        .unwrap();
    let first = boundary
        .iter()
        .position(|e| matches!(e.event, DomainEvent::ScheduledControl { index: 0, .. }))
        .unwrap();
    let second = boundary
        .iter()
        .position(|e| matches!(e.event, DomainEvent::ScheduledControl { index: 1, .. }))
        .unwrap();
    let offer = boundary
        .iter()
        .position(|e| matches!(e.event, DomainEvent::Offered { record_id: 100, .. }))
        .unwrap();
    assert!(pause < first && first < second && second < offer);
    assert!(
        r.history
            .entries
            .iter()
            .filter(|e| matches!(e.event, DomainEvent::Offered { .. }))
            .all(|e| e.now_ns < m.start_ns + 30 * MS)
    );
    assert!(r.history.entries.iter().any(|e| matches!(e.event, DomainEvent::OffersStopped { planned: 100, offered, cancelled } if offered + cancelled == 100 && cancelled > 0)));
}

#[test]
fn closed_loop_retries_one_logical_candidate_under_pressure() {
    let mut m = scenario();
    m.producer.delivery_event_capacity = 2;
    m.producer.record_descriptors = 2;
    m.producer.pending_records_per_topic = 2;
    m.experiment = Some(config(vec![load(
        u64::MAX - 15,
        LoadShape::ClosedLoop {
            start_ns: 0,
            count: 16,
            outstanding: 16,
        },
    )]));
    let r = run(&m);
    assert_eq!(
        (r.coverage.offered, r.coverage.accepted, r.coverage.acked),
        (16, 16, 16)
    );
    assert_eq!(r.coverage.refused, 0);
    assert!(r.coverage.backpressure > 0);
    assert!(
        r.history.entries.iter().any(
            |e| matches!(e.event, DomainEvent::AdmissionAttempt { attempt, .. } if attempt > 1)
        )
    );
}

#[test]
fn a_timed_close_resolves_a_pending_closed_loop_candidate_once() {
    let mut m = scenario();
    m.producer.delivery_event_capacity = 2;
    m.producer.record_descriptors = 2;
    m.producer.pending_records_per_topic = 2;
    let mut e = config(vec![load(
        1,
        LoadShape::ClosedLoop {
            start_ns: 0,
            count: 16,
            outstanding: 16,
        },
    )]);
    e.polling_pauses = vec![PollingPause {
        start_ns: 0,
        end_ns: 50 * MS,
    }];
    e.scheduled_actions = vec![ScheduledAction {
        at_ns: MS,
        action: TimedControl::Close {
            deadline_ns: 2_000 * MS,
        },
    }];
    m.experiment = Some(e);
    let r = run(&m);
    assert_eq!(r.coverage.refused, 1);
    assert_eq!(r.coverage.offered, r.coverage.accepted + 1);
    assert!(
        r.history
            .entries
            .iter()
            .any(|e| matches!(&e.event, DomainEvent::Refused { error, .. } if error == "Closed"))
    );
}

#[test]
fn sustained_closed_loop_cannot_silently_finish_before_its_window() {
    let mut m = scenario();
    m.experiment = Some(config(vec![load(
        1,
        LoadShape::ClosedLoopUntil {
            start_ns: 0,
            end_ns: 500 * MS,
            max_offers: 16,
            outstanding: 16,
        },
    )]));
    let failure = kr_kafka_sim::run(&m).unwrap_err();
    assert!(
        failure.reason.contains("offer budget exhausted"),
        "{failure}"
    );
}

#[test]
fn manifest_rejects_bad_ranges_deadlines_and_control_topology_before_running() {
    let mut m = scenario();
    m.experiment = Some(config(vec![load(
        1,
        LoadShape::OpenLoop {
            start_ns: 0,
            end_ns: 10 * MS,
            rate_per_s: 1_000,
        },
    )]));
    m.validate().unwrap();
    let mut bad = m.clone();
    bad.experiment.as_mut().unwrap().loads.push(load(
        10,
        LoadShape::ClosedLoop {
            start_ns: 0,
            count: 1,
            outstanding: 1,
        },
    ));
    assert!(bad.validate().unwrap_err().contains("overlapping"));
    let mut bad = m.clone();
    bad.experiment.as_mut().unwrap().loads[0].template.first_id = u64::MAX;
    assert!(bad.validate().is_err());
    let mut bad = m.clone();
    bad.experiment.as_mut().unwrap().offer_deadline_ns = u64::MAX;
    assert!(bad.validate().is_err());
    let mut bad = m.clone();
    bad.experiment
        .as_mut()
        .unwrap()
        .scheduled_actions
        .push(ScheduledAction {
            at_ns: 0,
            action: TimedControl::MoveLeader {
                topic: 0,
                partition: 1,
                broker: m.brokers[0].id,
            },
        });
    assert!(bad.validate().is_err());
    let mut bad = m;
    bad.experiment.as_mut().unwrap().polling_pauses = vec![
        PollingPause {
            start_ns: 0,
            end_ns: 10 * MS,
        },
        PollingPause {
            start_ns: MS,
            end_ns: 20 * MS,
        },
    ];
    assert!(bad.validate().is_err());
}

#[test]
fn rational_offer_count_and_due_times_use_checked_integer_arithmetic() {
    let shape = LoadShape::OpenLoop {
        start_ns: 7,
        end_ns: 1_000_000_008,
        rate_per_s: 3,
    };
    assert_eq!(shape.offer_budget().unwrap(), 4);
    assert_eq!(
        (0..4).map(|i| shape.due_ns(i).unwrap()).collect::<Vec<_>>(),
        vec![7, 333_333_340, 666_666_673, 1_000_000_007]
    );
    assert!(
        LoadShape::OpenLoop {
            start_ns: 0,
            end_ns: u64::MAX,
            rate_per_s: u64::MAX
        }
        .offer_budget()
        .is_err()
    );
    assert!(
        LoadShape::OpenLoop {
            start_ns: 0,
            end_ns: 1,
            rate_per_s: 0
        }
        .offer_budget()
        .is_err()
    );
}

#[test]
fn native_open_loop_pressure_is_a_refusal_and_releases_every_accepted_lease() {
    let mut m = scenario();
    m.producer.input_bytes = 1024;
    let warmup = load(
        1,
        LoadShape::ClosedLoop {
            start_ns: 0,
            count: 1,
            outstanding: 1,
        },
    );
    let mut native = load(
        2,
        LoadShape::OpenLoop {
            start_ns: 20 * MS,
            end_ns: 20 * MS + 1,
            rate_per_s: 15_000_000_000,
        },
    );
    native.template.native = true;
    m.experiment = Some(config(vec![warmup, native]));
    let r = run(&m);
    assert_eq!(r.coverage.offered, 16);
    assert_eq!(r.coverage.offered, r.coverage.accepted + r.coverage.refused);
    assert!(r.coverage.refused > 0);
    assert_eq!(r.coverage.input_releases, r.coverage.accepted - 1);
}

#[test]
fn ending_a_sustained_load_resolves_its_waiting_candidate_and_touching_pauses_stay_paused() {
    let mut m = scenario();
    m.producer.record_descriptors = 2;
    m.producer.pending_records_per_topic = 2;
    m.producer.delivery_event_capacity = 2;
    let mut e = config(vec![load(
        1,
        LoadShape::ClosedLoopUntil {
            start_ns: 0,
            end_ns: 10 * MS,
            max_offers: 16,
            outstanding: 16,
        },
    )]);
    e.polling_pauses = vec![
        PollingPause {
            start_ns: 0,
            end_ns: 5 * MS,
        },
        PollingPause {
            start_ns: 5 * MS,
            end_ns: 20 * MS,
        },
    ];
    m.experiment = Some(e);
    let r = run(&m);
    assert_eq!(r.coverage.refused, 1);
    assert!(
        r.history.entries.iter().any(
            |e| matches!(&e.event, DomainEvent::Refused { error, .. } if error == "LoadEnded")
        )
    );
    assert!(
        r.history
            .entries
            .iter()
            .filter(|e| matches!(e.event, DomainEvent::Delivery { .. }))
            .all(|e| e.now_ns >= m.start_ns + 20 * MS)
    );
}

#[test]
fn finite_gate_rejects_an_experiment_even_with_old_size_limits() {
    let mut m = kr_kafka_sim::campaign_manifest(0, kr_kafka_sim::CampaignVariant::Clean).unwrap();
    m.workload.clear();
    m.experiment = Some(config(vec![load(
        1,
        LoadShape::ClosedLoop {
            start_ns: 0,
            count: 16,
            outstanding: 1,
        },
    )]));
    assert!(m.validate().unwrap_err().contains("excludes experiment"));
}

#[test]
fn setup_traverses_propagation_and_waits_only_until_reopening_or_its_actual_deadline() {
    use kr_kafka_sim::{BrokerLink, LinkDirection, LinkOutage, OutageMode};
    for (end_ms, request_ms) in [(50, 200), (500, 20)] {
        let mut m = scenario();
        m.producer.request_timeout = kr_runtime::RuntimeDuration::from_nanos(request_ms * MS);
        m.experiment = Some(config(vec![load(
            1,
            LoadShape::ClosedLoop {
                start_ns: 0,
                count: 2,
                outstanding: 2,
            },
        )]));
        m.faults.links = vec![BrokerLink {
            broker: 1,
            to_broker_latency_ns: MS,
            from_broker_latency_ns: 2 * MS,
            chunk_bytes: 4096,
        }];
        m.faults.link_outages = vec![LinkOutage {
            broker: 1,
            direction: LinkDirection::Both,
            mode: OutageMode::BlackHole,
            start_ns: 0,
            end_ns: end_ms * MS,
        }];
        let r = run(&m);
        assert_eq!(r.coverage.acked, 2);
        assert!(r.history.entries.iter().any(|e| matches!(&e.event, DomainEvent::SetupFinished { result, elapsed_ns, .. } if result == "Ready" && *elapsed_ns >= 3 * MS)));
        for e in &r.history.entries {
            if let DomainEvent::BrokerRequest { connection, .. } = e.event {
                let broker = r
                    .history
                    .entries
                    .iter()
                    .find_map(|e| match e.event {
                        DomainEvent::ConnectionOpened {
                            connection: id,
                            broker,
                            ..
                        } if id == connection => Some(broker),
                        _ => None,
                    })
                    .unwrap();
                if broker == 1 {
                    assert!(e.now_ns >= m.start_ns + end_ms * MS);
                }
            }
        }
        if request_ms == 20 {
            assert!(r.history.entries.iter().any(|e| matches!(&e.event,
                DomainEvent::SetupFinished { started_ns, deadline_ns, resolved_ns, elapsed_ns, result, broker: 1, .. }
                if result == "Timeout" && resolved_ns == deadline_ns && *elapsed_ns == deadline_ns - started_ns + m.driver.link_latency_ns && *elapsed_ns < end_ms * MS)), "{:?}", r.history.entries.iter().filter(|e| matches!(e.event, DomainEvent::SetupFinished { .. })).collect::<Vec<_>>());
        }
        assert_eq!(
            r.checkpoint
                .random
                .iter()
                .find(|r| r.stream == "Fault")
                .unwrap()
                .draws,
            0
        );
    }
}

#[test]
fn fail_fast_setup_is_immediate_and_boundaries_precede_all_same_time_observations() {
    use kr_kafka_sim::{LinkDirection, LinkOutage, OutageMode};
    let mut m = scenario();
    m.experiment = Some(config(vec![load(
        1,
        LoadShape::OpenLoop {
            start_ns: 0,
            end_ns: 100 * MS,
            rate_per_s: 100,
        },
    )]));
    m.faults.link_outages = vec![LinkOutage {
        broker: 1,
        direction: LinkDirection::ToBroker,
        mode: OutageMode::FailFast,
        start_ns: 0,
        end_ns: 50 * MS,
    }];
    let r = run(&m);
    assert_eq!(r.coverage.acked, 10);
    assert!(r.fault_stats.setup_failures > 0);
    assert!(
        r.history
            .entries
            .iter()
            .any(|e| matches!(&e.event,DomainEvent::SetupFinished {
        elapsed_ns: 0, result, broker: 1, .. } if result.contains("Partitioned")))
    );
    for at in [0, 50 * MS] {
        let first = r
            .history
            .entries
            .iter()
            .find(|e| e.now_ns == m.start_ns + at)
            .unwrap();
        assert!(matches!(first.event, DomainEvent::LinkStateChanged { .. }));
    }
}

#[test]
fn generated_close_during_setup_black_hole_does_not_wait_for_the_outage_end() {
    use kr_kafka_sim::{LinkDirection, LinkOutage, OutageMode};
    let mut m = scenario();
    m.observe_requests = true;
    m.require_all_acked = false;
    m.minimum_acked = 0;
    let mut e = config(vec![load(
        1,
        LoadShape::ClosedLoop {
            start_ns: 0,
            count: 16,
            outstanding: 16,
        },
    )]);
    e.scheduled_actions.push(ScheduledAction {
        at_ns: 10 * MS,
        action: TimedControl::Close {
            deadline_ns: 5 * MS,
        },
    });
    m.experiment = Some(e);
    m.faults.link_outages = vec![LinkOutage {
        broker: 1,
        direction: LinkDirection::Both,
        mode: OutageMode::BlackHole,
        start_ns: 0,
        end_ns: 500 * MS,
    }];
    // A single bootstrap endpoint constructs a pending setup during close.
    m.producer.bootstrap.truncate(1);
    let r = run(&m);
    assert!(r.checkpoint.now_ns < m.start_ns + 500 * MS);
    assert_eq!(
        r.coverage.accepted,
        r.coverage.acked + r.coverage.not_written + r.coverage.unknown
    );
}

#[test]
fn response_black_hole_preserves_commits_and_reconciles_retries_after_recovery() {
    use kr_kafka_sim::{BrokerLink, LinkDirection, LinkOutage, OutageMode};
    let mut m = scenario();
    m.observe_requests = true;
    m.producer.request_timeout = kr_runtime::RuntimeDuration::from_nanos(20 * MS);
    m.experiment = Some(config(vec![
        load(
            1,
            LoadShape::ClosedLoop {
                start_ns: 0,
                count: 1,
                outstanding: 1,
            },
        ),
        load(
            2,
            LoadShape::OpenLoop {
                start_ns: 50 * MS,
                end_ns: 200 * MS,
                rate_per_s: 100,
            },
        ),
    ]));
    m.faults.links = vec![BrokerLink {
        broker: 1,
        to_broker_latency_ns: MS,
        from_broker_latency_ns: 2 * MS,
        chunk_bytes: 4096,
    }];
    m.faults.link_outages = vec![LinkOutage {
        broker: 1,
        direction: LinkDirection::FromBroker,
        mode: OutageMode::BlackHole,
        start_ns: 50 * MS,
        end_ns: 130 * MS,
    }];
    let r = run(&m);
    assert_eq!(r.coverage.acked, 16);
    assert!(r.coverage.retries > 0);
    // Normal broker dedup returns the original successful offset; error 46
    // is an optional scripted response, so prove dedup from committed tokens.
    let committed = r
        .history
        .entries
        .iter()
        .find(|e| {
            matches!(e.event,
        DomainEvent::BrokerCommit { records, .. } if records > 0)
                && (m.start_ns + 50 * MS..m.start_ns + 130 * MS).contains(&e.now_ns)
        })
        .unwrap();
    let DomainEvent::BrokerCommit {
        connection,
        correlation,
        ..
    } = committed.event
    else {
        unreachable!()
    };
    let cohort = r
        .history
        .entries
        .iter()
        .find_map(|e| match &e.event {
            DomainEvent::BrokerRequest {
                connection: c,
                correlation: r,
                records,
                ..
            } if *c == connection && *r == correlation => Some(records),
            _ => None,
        })
        .unwrap();
    assert!(r.history.entries.iter().any(|e| e.ordinal > committed.ordinal && matches!(&e.event,
        DomainEvent::BrokerRequest { records, .. } if records.iter().any(|token| cohort.contains(token)))));
    assert!(
        r.history
            .entries
            .iter()
            .any(|e| matches!(e.event, DomainEvent::BrokerCommit { .. })
                && (m.start_ns + 50 * MS..m.start_ns + 130 * MS).contains(&e.now_ns))
    );
    assert!(
        !r.history
            .entries
            .iter()
            .any(|e| matches!(e.event, DomainEvent::ResponseRead { .. })
                && (m.start_ns + 50 * MS..m.start_ns + 130 * MS).contains(&e.now_ns))
    );
}

#[test]
fn link_outage_validation_and_setup_replay_enforce_the_declared_environment() {
    use kr_kafka_sim::{
        LinkDirection, LinkOutage, OutageMode,
        faults::{FaultConfig, FaultEngine, Hook, Phase},
    };
    let mut config = FaultConfig::default();
    config.link_outages.push(LinkOutage {
        broker: 1,
        direction: LinkDirection::ToBroker,
        mode: OutageMode::FailFast,
        start_ns: 0,
        end_ns: MS,
    });
    let hook = Hook {
        phase: Phase::Setup,
        now_ns: 0,
        broker: 1,
        connection: 1,
        frame: 0,
        api: None,
        correlation: None,
    };
    let mut engine = FaultEngine::new(config.clone(), None).unwrap();
    let decision = engine
        .decide(hook.clone(), &mut || {
            Err("environment must not draw RNG".into())
        })
        .unwrap();
    assert_eq!(
        decision.effects.outcome,
        kr_kafka_sim::faults::Outcome::SetupFailure
    );
    let mut replay = FaultEngine::new(FaultConfig::default(), Some(vec![decision])).unwrap();
    assert!(
        replay
            .decide(hook, &mut || Err("no draws".into()))
            .unwrap_err()
            .contains("uncharged")
    );
    let mut overlap = config.clone();
    overlap.link_outages.push(LinkOutage {
        broker: 1,
        direction: LinkDirection::Both,
        mode: OutageMode::BlackHole,
        start_ns: MS / 2,
        end_ns: 2 * MS,
    });
    assert!(overlap.validate().is_err());
    config.link_outages.push(LinkOutage {
        broker: 1,
        direction: LinkDirection::ToBroker,
        mode: OutageMode::BlackHole,
        start_ns: MS,
        end_ns: 2 * MS,
    });
    config.validate().unwrap();
}

#[test]
fn client_observations_are_complete_correlated_and_behaviorally_transparent() {
    for (vectored, native, compressed) in [
        (false, false, false),
        (true, false, false),
        (true, true, false),
        (true, false, true),
    ] {
        let mut m = scenario();
        m.driver.vectored = vectored;
        m.driver.chunk_bytes = 64;
        if compressed {
            m.producer.compression = kr_kafka_producer::config::Compression::Zstd { level: 1 };
            m.producer.codec_contexts = 1;
        }
        let mut source = load(
            u64::MAX - 15,
            LoadShape::ClosedLoop {
                start_ns: 0,
                count: 16,
                outstanding: 8,
            },
        );
        source.template.native = native;
        m.experiment = Some(config(vec![source]));
        let plain = run(&m);
        m.observe_requests = true;
        let observed =
            kr_kafka_sim::verify_trace_transparency(&m).unwrap_or_else(|e| panic!("{e}"));
        assert_eq!(plain.checkpoint, observed.checkpoint);
        assert_eq!(plain.coverage, observed.coverage);
        assert_eq!(plain.metrics_counts, observed.metrics_counts);
        let mut projected = observed.history.clone();
        projected.entries.retain(|e| {
            !matches!(
                e.event,
                DomainEvent::ClientRequestDispatched { .. }
                    | DomainEvent::ClientRequestWriteCompleted { .. }
                    | DomainEvent::ClientRequestFinished { .. }
            )
        });
        for (index, e) in projected.entries.iter_mut().enumerate() {
            e.ordinal = index as u64 + 1;
        }
        assert_eq!(plain.history, projected);
        let mut requests = std::collections::BTreeMap::new();
        let mut written = std::collections::BTreeSet::new();
        let mut finished = std::collections::BTreeSet::new();
        let mut tokens = std::collections::BTreeSet::<u64>::new();
        let mut last_time = 0;
        for e in &observed.history.entries {
            assert!(e.now_ns >= last_time);
            last_time = e.now_ns;
            match &e.event {
                DomainEvent::ClientRequestDispatched {
                    connection,
                    correlation,
                    request_id,
                    api,
                    tokens: batch_tokens,
                    batches,
                    wire_bytes,
                } => {
                    assert!(
                        requests
                            .insert(*request_id, (*connection, *correlation, e.now_ns))
                            .is_none()
                    );
                    assert!(*wire_bytes > 0);
                    if *api == 0 {
                        assert_eq!(
                            batch_tokens.len(),
                            batches.iter().map(|b| b.records as usize).sum::<usize>()
                        );
                        assert!(batches.iter().all(|b| b.raw_bytes > 0 && b.wire_bytes > 61));
                        assert!(
                            batch_tokens.iter().all(|token| (1..=16).contains(token)),
                            "workload IDs must become accepted tokens"
                        );
                        tokens.extend(batch_tokens.iter().copied());
                    }
                }
                DomainEvent::ClientRequestWriteCompleted {
                    request_id,
                    connection,
                    correlation,
                } => {
                    let (c, r, dispatch) = requests[request_id];
                    assert_eq!((*connection, *correlation), (c, r));
                    assert!(e.now_ns >= dispatch);
                    assert!(written.insert(*request_id));
                }
                DomainEvent::ClientRequestFinished {
                    request_id,
                    connection,
                    correlation,
                    result,
                    ..
                } => {
                    let (c, r, _) = requests[request_id];
                    assert_eq!((*connection, *correlation), (c, r));
                    assert_eq!(result, "Response");
                    assert!(written.contains(request_id));
                    assert!(finished.insert(*request_id));
                }
                _ => {}
            }
        }
        assert_eq!(tokens.len(), 16);
        assert_eq!(requests.len(), finished.len());
    }
}

#[test]
fn client_dispatch_counts_include_produce_attempts_that_never_reach_the_broker() {
    use kr_kafka_sim::{BrokerLink, LinkDirection, LinkOutage, OutageMode};
    let mut m = scenario();
    m.observe_requests = true;
    m.producer.request_timeout = kr_runtime::RuntimeDuration::from_nanos(20 * MS);
    m.experiment = Some(config(vec![
        load(
            1,
            LoadShape::ClosedLoop {
                start_ns: 0,
                count: 1,
                outstanding: 1,
            },
        ),
        load(
            2,
            LoadShape::OpenLoop {
                start_ns: 50 * MS,
                end_ns: 200 * MS,
                rate_per_s: 100,
            },
        ),
    ]));
    m.faults.links = vec![BrokerLink {
        broker: 1,
        to_broker_latency_ns: MS,
        from_broker_latency_ns: MS,
        chunk_bytes: 64,
    }];
    m.faults.link_outages = vec![LinkOutage {
        broker: 1,
        direction: LinkDirection::ToBroker,
        mode: OutageMode::BlackHole,
        start_ns: 50 * MS,
        end_ns: 130 * MS,
    }];
    let r = run(&m);
    assert_eq!(r.coverage.acked, 16);
    let received: std::collections::BTreeSet<_> = r
        .history
        .entries
        .iter()
        .filter_map(|e| match e.event {
            DomainEvent::BrokerRequest {
                connection,
                correlation,
                api: 0,
                ..
            } => Some((connection, correlation)),
            _ => None,
        })
        .collect();
    let unseen: Vec<_> = r
        .history
        .entries
        .iter()
        .filter_map(|e| match &e.event {
            DomainEvent::ClientRequestDispatched {
                connection,
                correlation,
                api: 0,
                request_id,
                tokens,
                ..
            } if !received.contains(&(*connection, *correlation)) => Some((*request_id, tokens)),
            _ => None,
        })
        .collect();
    assert!(!unseen.is_empty());
    for (request_id, tokens) in unseen {
        assert!(!tokens.is_empty());
        assert!(
            r.history
                .entries
                .iter()
                .any(|e| matches!(&e.event,DomainEvent::ClientRequestFinished {
            request_id: id, result, .. } if *id == request_id && result.starts_with("Retired:")))
        );
        assert!(tokens.iter().all(|token| r.history.entries.iter().any(|e| matches!(&e.event,
            DomainEvent::ClientRequestDispatched { request_id: later, tokens, .. } if *later > request_id && tokens.contains(token)))));
    }
    let setup_connections: std::collections::BTreeSet<_> = r
        .history
        .entries
        .iter()
        .filter_map(|e| match e.event {
            DomainEvent::ClientRequestDispatched {
                connection,
                correlation: -1,
                ..
            } => Some(connection),
            _ => None,
        })
        .collect();
    assert!(
        setup_connections.len() > 2,
        "replacement connections must reuse setup correlation -1"
    );
}

#[test]
fn sustained_environment_rejections_replay_and_recover_without_a_finite_fault_budget() {
    use kr_kafka_sim::faults::{Effects, EnvironmentRule, Phase};
    let mut m = scenario();
    m.observe_requests = true;
    m.experiment = Some(config(vec![
        load(
            1,
            LoadShape::ClosedLoop {
                start_ns: 0,
                count: 1,
                outstanding: 1,
            },
        ),
        load(
            2,
            LoadShape::OpenLoop {
                start_ns: 50 * MS,
                end_ns: 200 * MS,
                rate_per_s: 100,
            },
        ),
    ]));
    m.faults.budget_per_round = 0;
    m.faults.max_random_effects_per_round = 0;
    m.faults.environment = vec![EnvironmentRule {
        broker: Some(1),
        api: Some(0),
        phase: Phase::BeforeAppend,
        start_ns: 50 * MS,
        end_ns: 100 * MS,
        probability_ppm: 1_000_000,
        effects: Effects {
            reject_error: Some(19),
            ..Effects::default()
        },
        ramp: None,
    }];
    let r = run(&m);
    assert_eq!(r.coverage.acked, 16);
    assert!(r.fault_stats.environment_effects > 1);
    assert_eq!(
        (r.fault_stats.script_effects, r.fault_stats.random_effects),
        (0, 0)
    );
    assert!(
        r.history
            .entries
            .iter()
            .any(|e| matches!(e.event, DomainEvent::Delivery { .. })
                && e.now_ns >= m.start_ns + 100 * MS)
    );
    for e in &r.history.entries {
        if let DomainEvent::FaultDecision(d) = &e.event {
            assert_eq!(
                (d.effects_applied, d.budget_remaining, d.random_remaining),
                (0, 0, 0)
            );
            if d.environment_effects != 0 {
                assert!((50 * MS..100 * MS).contains(&d.hook.now_ns));
                assert_eq!(d.effects.reject_error, Some(19));
                assert!(d.draws.is_empty());
            }
        }
    }
    let mut finite =
        kr_kafka_sim::campaign_manifest(0, kr_kafka_sim::CampaignVariant::Clean).unwrap();
    finite.faults.environment = m.faults.environment;
    assert!(finite.validate().is_err());
}

#[test]
fn complete_experiment_tapes_have_a_separate_json_envelope_from_legacy_runs() {
    let mut m = scenario();
    let mut bytes = serde_json::to_vec(&m).unwrap();
    bytes.resize(17 * 1024 * 1024, b' ');
    assert!(
        kr_kafka_sim::ReplayManifest::from_json(&bytes)
            .unwrap_err()
            .contains("legacy manifest byte limit")
    );
    m.experiment = Some(config(vec![load(
        1,
        LoadShape::ClosedLoop {
            start_ns: 0,
            count: 16,
            outstanding: 1,
        },
    )]));
    let mut bytes = serde_json::to_vec(&m).unwrap();
    bytes.resize(17 * 1024 * 1024, b' ');
    kr_kafka_sim::ReplayManifest::from_json(&bytes).unwrap();
    m.versions.driver += 1;
    let mut bytes = serde_json::to_vec(&m).unwrap();
    bytes.resize(17 * 1024 * 1024, b' ');
    assert!(kr_kafka_sim::ReplayManifest::from_json(&bytes).is_err());
}

#[test]
fn unresolved_keyed_admission_keeps_the_unassigned_partition_failure_sentinel() {
    let mut m = scenario();
    m.require_all_acked = false;
    m.minimum_acked = 0;
    m.topics[0].leaders = vec![1; 6];
    m.producer.topic_resolve_timeout = kr_runtime::RuntimeDuration::from_nanos(100 * MS);
    m.faults.isolations = vec![kr_kafka_sim::faults::IsolationWindow {
        broker: 1,
        start_ns: 0,
        end_ns: 5_000 * MS,
    }];
    let mut source = load(
        1,
        LoadShape::OpenLoop {
            start_ns: 0,
            end_ns: 16 * MS,
            rate_per_s: 1000,
        },
    );
    source.template.partitioning = Partitioning::Keyed {
        keys: 64,
        skew_ppm: 0,
    };
    let mut e = config(vec![source]);
    e.require_acked = false;
    m.experiment = Some(e);
    let r = run(&m);
    assert_eq!(r.coverage.not_written, 16);
    let deliveries = r
        .history
        .entries
        .iter()
        .filter_map(|e| match e.event {
            DomainEvent::Delivery {
                partition,
                attempts,
                outcome,
                reason,
                ..
            } => Some((partition, attempts, outcome, reason)),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(deliveries.len(), 16);
    assert!(
        deliveries
            .iter()
            .all(|&(partition, attempts, outcome, reason)| partition == -1
                && attempts == 0
                && outcome == 1
                && [3, 4].contains(&reason))
    );
    assert!(deliveries.iter().any(|d| d.3 == 4));
}

#[test]
fn crash_isolation_abandons_preappend_service_even_when_it_spans_recovery() {
    use kr_kafka_sim::faults::{Effects, EnvironmentRule, IsolationWindow, Phase};
    for delayed_hook in [false, true] {
        let mut m = scenario();
        m.producer.linger_max = kr_runtime::RuntimeDuration::ZERO;
        m.faults.crash_on_isolation = true;
        m.observe_requests = true;
        m.faults.isolations = vec![IsolationWindow {
            broker: 1,
            start_ns: 20 * MS,
            end_ns: 50 * MS,
        }];
        if delayed_hook {
            m.driver.service_delay_ns = 0;
            m.faults.environment = vec![EnvironmentRule {
                broker: Some(1),
                api: Some(0),
                phase: Phase::BeforeAppend,
                start_ns: 18 * MS,
                end_ns: 20 * MS,
                probability_ppm: 1_000_000,
                effects: Effects {
                    delay_ns: 80 * MS,
                    ..Default::default()
                },
                ramp: None,
            }];
        }
        let mut e = config(vec![
            load(
                1,
                LoadShape::ClosedLoop {
                    start_ns: 0,
                    count: 1,
                    outstanding: 1,
                },
            ),
            load(
                2,
                LoadShape::ClosedLoop {
                    start_ns: 19 * MS + 500_000,
                    count: 1,
                    outstanding: 1,
                },
            ),
        ]);
        e.scheduled_actions = vec![ScheduledAction {
            at_ns: 110 * MS,
            action: TimedControl::Flush,
        }];
        m.experiment = Some(e);
        let r = run(&m);
        assert_eq!(r.coverage.acked, 2);
        let abandoned = r
            .history
            .entries
            .iter()
            .find(|e| matches!(e.event, DomainEvent::BrokerFrameAbandoned { broker: 1, .. }))
            .expect("interrupted service witness");
        assert!(abandoned.now_ns >= m.start_ns + if delayed_hook { 50 * MS } else { 20 * MS });
        assert!(!r.history.entries.iter().any(|e| matches!(
            e.event,
            DomainEvent::BrokerCommit { records: 1, .. }
        )
            && (m.start_ns + 20 * MS..m.start_ns + 50 * MS).contains(&e.now_ns)));
        let mut legacy = m.clone();
        legacy.experiment = None;
        legacy.observe_requests = false;
        legacy.faults.environment.clear();
        assert!(legacy.validate().is_err());
    }
}
