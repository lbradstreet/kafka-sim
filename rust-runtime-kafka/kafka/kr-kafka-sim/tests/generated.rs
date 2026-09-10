mod support;
use kr_kafka_sim::{DomainEvent, Workload};
use support::{MS, record, run, scenario};

#[test]
fn native_admission_pressure_retries_without_losing_the_accepted_watermark() {
    let mut manifest = scenario();
    manifest.producer.input_bytes = 1024;
    // Metadata snapshots also use input credit. Resolve the topic before
    // measuring native admission pressure against the remaining input pool.
    manifest.workload = vec![
        Workload::Submit {
            records: vec![record(1, 0, 0)],
        },
        support::settle(1, true),
        Workload::Submit {
            records: (2..=16)
                .map(|id| {
                    let mut record = record(id, 0, 0);
                    record.native = true;
                    record
                })
                .collect(),
        },
        Workload::SettleAllAccepted {
            timeout_ns: 8_000 * MS,
            require_acked: true,
        },
        Workload::SleepUntil { at_ns: 50 * MS },
        Workload::Close {
            deadline_ns: 2_000 * MS,
        },
    ];
    let report = run(&manifest);
    assert_eq!(report.coverage.accepted, 16);
    assert_eq!(report.coverage.acked, 16);
    assert_eq!(report.coverage.input_releases, 15);
    assert!(report.coverage.backpressure > 0);
    let settled = report
        .history
        .entries
        .iter()
        .position(|entry| {
            matches!(
        &entry.event, DomainEvent::WorkloadStep { action, .. } if action.starts_with("SleepUntil:"))
        })
        .unwrap();
    assert_eq!(
        report.history.entries[..settled]
            .iter()
            .filter(|entry| matches!(entry.event, DomainEvent::Delivery { .. }))
            .count(),
        16
    );
    let closing = report
        .history
        .entries
        .iter()
        .find(|entry| {
            matches!(
        &entry.event, DomainEvent::WorkloadStep { action, .. } if action == "Close")
        })
        .unwrap();
    assert!(closing.now_ns >= manifest.start_ns + 50 * MS);
}

#[test]
fn an_empty_accepted_watermark_is_already_settled() {
    let mut manifest = scenario();
    manifest.minimum_acked = 0;
    manifest.workload = vec![
        Workload::SettleAllAccepted {
            timeout_ns: MS,
            require_acked: true,
        },
        Workload::Close {
            deadline_ns: 2_000 * MS,
        },
    ];
    let report = run(&manifest);
    assert_eq!(report.coverage.accepted, 0);
}
