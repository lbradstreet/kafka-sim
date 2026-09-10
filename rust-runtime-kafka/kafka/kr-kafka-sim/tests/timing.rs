//! Retry timing is measured from observed response arrival, with the model's
//! complete request arrival kept separate from delayed application time.
mod support;
use kr_kafka_sim::{
    DomainEvent, Workload,
    faults::{Effects, Match, Outcome, Phase, ScriptRule},
};
use kr_runtime::RuntimeDuration;
use support::{MS, record, run, scenario, settle};

#[test]
fn metadata_identity_and_setup_failures_recover_before_finite_deadline() {
    for api in [Some(3), Some(22), None] {
        let mut manifest = scenario();
        manifest.faults.scripts.push(ScriptRule {
            matcher: Match {
                phase: if api.is_some() {
                    Phase::BeforeResponse
                } else {
                    Phase::Setup
                },
                api,
                ..Match::default()
            },
            skip: 0,
            take: 1,
            effects: if api.is_some() {
                Effects {
                    delay_ns: 300 * MS,
                    ..Effects::default()
                }
            } else {
                Effects {
                    outcome: Outcome::SetupFailure,
                    ..Effects::default()
                }
            },
        });
        manifest.workload = vec![
            Workload::Submit {
                records: vec![record(1, 0, 0), record(2, 0, 0)],
            },
            Workload::Flush,
            Workload::AwaitFlush {
                timeout_ns: 8_000 * MS,
                require_acked: true,
            },
            settle(2, true),
            Workload::Close {
                deadline_ns: 2_000 * MS,
            },
        ];
        let report = run(&manifest);
        assert_eq!(report.coverage.acked, 2, "API {api:?}");
        let applied: Vec<_> = report
            .history
            .entries
            .iter()
            .filter_map(|entry| match &entry.event {
                DomainEvent::FaultDecision(decision) if decision.effects_applied != 0 => {
                    Some(decision)
                }
                _ => None,
            })
            .collect();
        assert_eq!(applied.len(), 1, "API {api:?}");
        assert_eq!(applied[0].hook.api, api);
        if let Some(api) = api {
            let requests = report.history.entries.iter().filter(|entry| {
                matches!(entry.event, DomainEvent::BrokerRequest { api: observed, .. } if observed == api)
            }).count();
            assert!(
                requests >= 2,
                "API {api} must actually retry after its delayed response"
            );
        } else {
            assert_eq!(applied[0].effects.outcome, Outcome::SetupFailure);
        }
    }
}

#[test]
fn produce_retry_arrivals_obey_capped_base_plus_jitter_and_flush_waits() {
    let mut manifest = scenario();
    manifest.producer.retry_backoff_max = RuntimeDuration::from_nanos(20 * MS);
    manifest.faults.scripts.push(ScriptRule {
        matcher: Match {
            api: Some(0),
            phase: Phase::BeforeAppend,
            ..Match::default()
        },
        skip: 0,
        take: 4,
        effects: Effects {
            reject_error: Some(kr_kafka_protocol::errors::NOT_ENOUGH_REPLICAS),
            ..Effects::default()
        },
    });
    manifest.workload = vec![
        Workload::Submit {
            records: vec![record(1, 0, 0)],
        },
        Workload::Flush,
        Workload::AwaitFlush {
            timeout_ns: 2_000 * MS,
            require_acked: true,
        },
        settle(1, true),
        Workload::Close {
            deadline_ns: 2_000 * MS,
        },
    ];
    let report = run(&manifest);
    let requests: Vec<_> = report
        .history
        .entries
        .iter()
        .filter_map(|entry| {
            if let DomainEvent::BrokerTiming {
                connection,
                correlation,
                api: 0,
                phase: Phase::BeforeAppend,
                arrived_ns,
                ..
            } = entry.event
            {
                Some((connection, correlation, arrived_ns))
            } else {
                None
            }
        })
        .collect();
    assert_eq!(
        requests.len(),
        5,
        "four real rejections and one successful request"
    );
    let mut exceeded_base_cap = false;
    for (attempt, pair) in requests.windows(2).enumerate() {
        let previous = pair[0];
        let response_at = report
            .history
            .entries
            .iter()
            .find_map(|entry| {
                matches!(entry.event, DomainEvent::ResponseRead { connection, correlation }
                if connection == previous.0 && correlation == previous.1)
                .then_some(entry.now_ns)
            })
            .expect("each retry has an actually received rejection response");
        let interval = pair[1]
            .2
            .checked_sub(response_at)
            .expect("retry follows rejection");
        let base = (10 * MS * (1 << attempt)).min(20 * MS);
        // Link completions can trail peer visibility, but this small fixture has
        // no blocking work. Five milliseconds is an explicit transport/owner
        // allowance, independent of the configured data retry base.
        let transport_allowance = 5 * MS;
        assert!(interval >= base, "attempt {attempt}: {interval} < {base}");
        assert!(
            interval < 2 * base + transport_allowance,
            "attempt {attempt}: {interval} exceeds capped base+jitter envelope"
        );
        exceeded_base_cap |= interval > 20 * MS + transport_allowance;
    }
    assert!(
        exceeded_base_cap,
        "the cap applies to the exponential base before additive jitter"
    );
    let delivery = report
        .history
        .entries
        .iter()
        .find(|entry| matches!(entry.event, DomainEvent::Delivery { .. }))
        .unwrap();
    assert!(matches!(
        delivery.event,
        DomainEvent::Delivery { attempts: 5, .. }
    ));
    let flush_done = report
        .history
        .entries
        .iter()
        .find(|entry| matches!(entry.event, DomainEvent::FlushDone { .. }))
        .unwrap();
    assert!(delivery.ordinal < flush_done.ordinal);
}
