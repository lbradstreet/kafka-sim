use super::*;
use crate::{Size, catalogue};
use kr_kafka_sim::{DomainEvent as E, HistoryEntry};
use serde_json::json;

#[test]
fn imported_report_means_preserve_the_original_floating_point_bits() {
    let mut report = synthetic();
    // These observed Full-run means round one ULP away with the default fast
    // JSON parser. Saved replay compares all report fields exactly.
    report.buckets["global"]["records_per_request_mean"][0] = json!(80.0 / 11.0);
    report.buckets["global"]["records_per_request_mean"][1] = json!(159.0 / 22.0);
    let bytes = report.to_json().unwrap();
    assert_eq!(ExperimentReport::from_json(&bytes).unwrap(), report);
}
fn actual() -> (crate::Scenario, crate::Variant, kr_kafka_sim::RunReport) {
    let s = catalogue().remove(0);
    let v = s.variants[0].clone();
    let m = s.build(&v, 0, Size::Test).unwrap();
    let run = kr_kafka_sim::run_replayed(&m).unwrap();
    (s, v, run)
}
fn synthetic() -> ExperimentReport {
    let (s, v, mut run) = actual();
    run.metrics_samples.clear();
    run.missed_metrics_requests.clear();
    let origin = u64::MAX - 1_000_000_000;
    run.manifest.start_ns = origin;
    run.manifest.seed = u64::MAX;
    run.checkpoint.now_ns = origin + 90_000_000;
    let topic = run.manifest.topics[0].id;
    let mut entries = vec![];
    let mut push = |ms: u64, event| {
        entries.push(HistoryEntry {
            ordinal: entries.len() as u64,
            now_ns: origin + ms * 1_000_000,
            event,
        })
    };
    for id in [1, 2, u64::MAX] {
        push(
            0,
            E::Offered {
                load: 0,
                record_id: id,
                due_ns: origin,
            },
        );
    }
    push(
        0,
        E::Accepted {
            record_id: 1,
            token: 1,
            topic,
            partition: 0,
            lease: None,
        },
    );
    push(
        0,
        E::Accepted {
            record_id: 2,
            token: 2,
            topic,
            partition: 1,
            lease: None,
        },
    );
    push(
        0,
        E::Refused {
            record_id: u64::MAX,
            due_ns: origin,
            offered_ns: origin,
            error: "Credit".into(),
        },
    );
    push(
        1,
        E::ConnectionOpened {
            connection: 1,
            broker: 1,
            lane: 0,
        },
    );
    let dispatch = |connection, correlation, request_id, token| E::ClientRequestDispatched {
        connection,
        correlation,
        api: 0,
        request_id,
        tokens: vec![token],
        wire_bytes: 100,
        batches: vec![kr_kafka_sim::DispatchBatch {
            batch_id: token,
            topic,
            partition: (token - 1) as i32,
            records: 1,
            raw_bytes: 80,
            wire_bytes: 90,
        }],
    };
    let finish = |connection, correlation, request_id| E::ClientRequestFinished {
        request_id,
        connection,
        correlation,
        confirmed: 100,
        result: "Response".into(),
        certainty: "All".into(),
    };
    push(2, dispatch(1, 7, 1, 1));
    push(3, dispatch(1, 8, 2, 2));
    push(
        4,
        E::WriteAdmitted {
            operation: 1,
            connection: 1,
            bytes: 70,
            segments: 2,
        },
    );
    push(
        5,
        E::WriteCompleted {
            operation: 1,
            bytes: 40,
            certainty: "Some".into(),
        },
    );
    push(
        6,
        E::WriteAdmitted {
            operation: 2,
            connection: 1,
            bytes: 60,
            segments: 2,
        },
    );
    push(
        7,
        E::WriteCompleted {
            operation: 2,
            bytes: 60,
            certainty: "All".into(),
        },
    );
    push(
        8,
        E::BrokerRequest {
            connection: 1,
            api: 0,
            version: 13,
            correlation: 8,
            records: vec![2],
        },
    );
    push(20, finish(1, 7, 1));
    push(
        21,
        E::ConnectionOpened {
            connection: 2,
            broker: 2,
            lane: 0,
        },
    );
    push(22, dispatch(2, 7, 3, 1));
    push(
        30,
        E::ResponseRead {
            connection: 1,
            correlation: 8,
        },
    );
    push(
        35,
        E::ClientRequestWriteCompleted {
            connection: 1,
            correlation: 8,
            request_id: 2,
        },
    );
    push(36, finish(1, 8, 2));
    push(
        37,
        E::ConnectionClosed {
            connection: 1,
            reason: "Retired".into(),
        },
    );
    push(
        40,
        E::Delivery {
            token: 2,
            record_id: 2,
            topic,
            partition: 1,
            outcome: 1,
            reason: 1,
            offset: None,
            timestamp: None,
            attempts: 1,
        },
    );
    push(
        45,
        E::ClientRequestWriteCompleted {
            connection: 2,
            correlation: 7,
            request_id: 3,
        },
    );
    push(
        50,
        E::BrokerRequest {
            connection: 2,
            api: 0,
            version: 13,
            correlation: 7,
            records: vec![1],
        },
    );
    push(
        60,
        E::ResponseRead {
            connection: 2,
            correlation: 7,
        },
    );
    push(61, finish(2, 7, 3));
    push(
        70,
        E::Delivery {
            token: 1,
            record_id: 1,
            topic,
            partition: 0,
            outcome: 0,
            reason: 0,
            offset: Some(i64::MAX),
            timestamp: None,
            attempts: 2,
        },
    );
    push(
        80,
        E::ConnectionClosed {
            connection: 2,
            reason: "Retired".into(),
        },
    );
    push(
        85,
        E::OffersStopped {
            planned: 3,
            offered: 3,
            cancelled: 0,
        },
    );
    push(90, E::Closed { unknown: 0 });
    run.history.entries = entries;
    let report = derive(&s, &v, Size::Test, true, &run, &[]).unwrap();
    report.validate().unwrap();
    report
}
#[test]
fn request_identity_partial_writes_early_responses_refusals_and_exact_decimals() {
    let r = synthetic();
    assert_eq!(r.summary["client_requests"], 3);
    assert_eq!(r.summary["broker_requests"], 2);
    assert_eq!(r.summary["client_retry_requests"], 1);
    assert_eq!(r.summary["client_retry_records"], 1);
    assert_eq!(r.summary["bytes_wire"], 100);
    assert_eq!(r.summary["dispatch_rtt"]["count"], 2);
    assert_eq!(r.summary["dispatch_rtt"]["max"], 38_000_000);
    assert_eq!(r.summary["full_write_rtt"]["count"], 1);
    assert_eq!(r.summary["full_write_rtt"]["max"], 15_000_000);
    assert_eq!(r.summary["first_dispatch_batch_bytes"]["count"], 2);
    assert_eq!(r.summary["first_dispatch_batch_bytes"]["sum"], 180);
    assert_eq!(r.records["record_id"][2], u64::MAX.to_string());
    assert_eq!(r.records["offset"][0], i64::MAX.to_string());
    assert!(r.records["deliver"][2].is_null());
    assert_eq!(r.records["broker"][0], 2);
    assert_eq!(r.buckets["brokers"][0]["client_inflight_max"][3], 2);
    assert_eq!(
        r,
        ExperimentReport::from_json(&r.to_json().unwrap()).unwrap()
    );
}
#[test]
fn validation_rejects_corrupt_counts_times_ids_enums_shapes_and_credit_bounds() {
    let report = synthetic();
    let mutations: Vec<(&str, Value)> = vec![
        ("/schema", json!("bad")),
        ("/meta/origin_ns", json!("01")),
        ("/meta/end_ns", json!("0")),
        ("/meta/seed", json!(9007199254740992u64)),
        ("/records/record_id/1", json!("1")),
        ("/records/outcome/0", json!(4)),
        ("/records/deliver/0", json!(0)),
        ("/records/accept/2", json!(0)),
        ("/records/offset/0", json!("-0")),
        ("/records/partition/0", json!(256)),
        ("/records/count", json!(65537)),
        ("/records/population_count", json!(1_000_001)),
        ("/summary/records/accepted", json!(3)),
        ("/buckets/count", json!(4097)),
        ("/buckets/global/accepted/0", json!(0)),
        (
            "/buckets/global/credits/held_observed_max/0/0",
            json!(u32::MAX),
        ),
        (
            "/distributions/latency_ecdf/points/0/cumulative_count",
            json!(0),
        ),
        ("/summary/latency_acked/p50", json!(80_000_000)),
        ("/buckets/brokers/0/broker", json!(999)),
    ];
    for (path, value) in mutations {
        let mut value_report = serde_json::to_value(&report).unwrap();
        *value_report.pointer_mut(path).unwrap() = value;
        let mutated: ExperimentReport = serde_json::from_value(value_report).unwrap();
        assert!(mutated.validate().is_err(), "{path}");
    }
    let mut r = report.clone();
    r.records["record_id"] = json!([]);
    assert!(r.validate().is_err());
}
#[test]
fn sampled_record_rows_do_not_change_exact_population_totals() {
    let (s, v, mut run) = actual();
    run.metrics_samples.clear();
    let origin = run.manifest.start_ns;
    run.checkpoint.now_ns = origin + 1_000_000;
    run.history.entries.clear();
    for id in 0..70_000 {
        for event in [
            E::Offered {
                load: 0,
                record_id: id,
                due_ns: origin,
            },
            E::Refused {
                record_id: id,
                due_ns: origin,
                offered_ns: origin,
                error: "Credit".into(),
            },
        ] {
            run.history.entries.push(HistoryEntry {
                ordinal: run.history.entries.len() as u64,
                now_ns: origin,
                event,
            });
        }
    }
    run.history.entries.push(HistoryEntry {
        ordinal: 140_000,
        now_ns: origin + 1_000_000,
        event: E::OffersStopped {
            planned: 70_000,
            offered: 70_000,
            cancelled: 0,
        },
    });
    let r = derive(&s, &v, Size::Full, true, &run, &[]).unwrap();
    r.validate().unwrap();
    assert_eq!(r.summary["records"]["offered"], 70_000);
    assert_eq!(r.records["population_count"], 70_000);
    assert_eq!(r.records["complete"], false);
    assert!(r.records["count"].as_u64().unwrap() <= 65_536);
    assert!(r.to_json().unwrap().len() <= MAX_REPORT_BYTES);
    let mut corrupt = r.clone();
    corrupt.records["complete"] = json!(true);
    assert!(corrupt.validate().is_err());
}
#[test]
fn hdr_intervals_preserve_empty_ranges_and_reject_corrupt_epochs_scopes_and_lengths() {
    let (s, v, run) = actual();
    let r = derive(&s, &v, Size::Test, true, &run, &[]).unwrap();
    r.validate().unwrap();
    assert!(!r.hdr.is_null());
    for (path, value) in [
        ("/hdr/intervals/epoch/0", json!("00")),
        ("/hdr/series/0/metric", json!(11)),
        ("/hdr/series/0/count", json!([])),
        ("/hdr/intervals/count", json!(1025)),
        ("/hdr/scopes/0/kind", json!("unknown")),
    ] {
        let mut r = serde_json::to_value(&r).unwrap();
        *r.pointer_mut(path).unwrap() = value;
        assert!(
            serde_json::from_value::<ExperimentReport>(r)
                .unwrap()
                .validate()
                .is_err(),
            "{path}"
        );
    }
}
#[test]
fn wrappers_keep_decimal_precision_and_html_terminators_inert() {
    let hostile = json!({"text":"</script><SCRIPT>alert('x')</ScRiPt>&\"\u{2028}\u{2029}","seed":crate::js_wrapper::DecimalU64(u64::MAX)});
    let encoded = crate::js_wrapper::html_json(&hostile).unwrap();
    assert!(!encoded.contains('<') && !encoded.contains('&') && !encoded.contains('\u{2028}'));
    assert_eq!(serde_json::from_str::<Value>(&encoded).unwrap(), hostile);
    let js = crate::js_wrapper::standalone(&hostile).unwrap();
    assert!(js.starts_with(crate::js_wrapper::COMMENT));
    assert!(js.contains("18446744073709551615"));
    assert!(serde_json::from_str::<crate::js_wrapper::DecimalU64>("\"01\"").is_err());
}
#[test]
fn bundle_pagination_and_identity_validation() {
    let r = synthetic();
    let mut b = ExperimentBundle {
        schema: BUNDLE_SCHEMA.into(),
        scenario: r.meta["scenario"].clone(),
        variants: vec![json!({"name":r.meta["variant"]["name"],"deltas":{},"order":0})],
        seeds: vec![],
        runs: vec![],
        comparisons: vec![],
        page: json!({"index":0,"count":1,"total_runs":33}),
    };
    for seed in 0..33 {
        let mut r = r.clone();
        r.meta["seed"] = json!(seed.to_string());
        b.seeds.push(seed.to_string());
        b.runs.push(r);
    }
    assert!(b.validate().is_err());
    let pages = b.paginate().unwrap();
    assert_eq!(pages.len(), 2);
    assert_eq!(pages[0].runs.len(), 32);
    assert_eq!(pages[1].runs.len(), 1);
    for p in &pages {
        p.validate().unwrap();
    }
    let mut bad = pages[0].clone();
    bad.runs[1] = bad.runs[0].clone();
    assert!(bad.validate().is_err());
}

#[test]
fn unresolved_bootstrap_failures_keep_routes_null_without_inventing_a_topic() {
    let s = catalogue().remove(0);
    let v = &s.variants[0];
    let mut m = s.build(v, 0, Size::Test).unwrap();
    m.producer.bootstrap.truncate(1);
    m.producer.topic_resolve_timeout = kr_runtime::RuntimeDuration::from_nanos(1_000_000_000);
    m.faults.isolations = vec![kr_kafka_sim::faults::IsolationWindow {
        broker: 1,
        start_ns: 0,
        end_ns: 5_000_000_000,
    }];
    m.experiment.as_mut().unwrap().loads[0].shape = kr_kafka_sim::LoadShape::OpenLoop {
        start_ns: 0,
        end_ns: 20_000_000,
        rate_per_s: 1000,
    };
    let run = kr_kafka_sim::run_replayed(&m).unwrap();
    assert_eq!(run.coverage.not_written, 20);
    let r = derive(&s, v, Size::Test, true, &run, &[]).unwrap();
    r.validate().unwrap();
    assert!(
        r.topology["partitions"]
            .as_array()
            .unwrap()
            .iter()
            .all(|p| p["topic_id"] != "00000000000000000000000000000000")
    );
    assert!(
        r.records["partition"]
            .as_array()
            .unwrap()
            .iter()
            .all(Value::is_null)
    );
    assert_eq!(
        r.partitions["unrouted_not_written"]
            .as_array()
            .unwrap()
            .iter()
            .map(|v| v.as_u64().unwrap())
            .sum::<u64>(),
        20
    );
}

#[test]
fn import_rejects_config_and_hdr_enums_that_the_browser_cannot_display() {
    let r = synthetic();
    for (path, value) in [
        ("/config/lanes", json!(0)),
        ("/config/compression", json!("invalid")),
        ("/config/batch_target_mode", json!("invalid")),
        ("/config/driver/transport_model", json!("invented")),
        ("/config/driver/propagation_links/0/broker", json!(999)),
    ] {
        let mut value_tree = serde_json::to_value(&r).unwrap();
        *value_tree.pointer_mut(path).unwrap() = value;
        assert!(ExperimentReport::from_json(&serde_json::to_vec(&value_tree).unwrap()).is_err());
    }
    let (s, v, run) = actual();
    let mut r = derive(&s, &v, Size::Test, true, &run, &[]).unwrap();
    r.hdr["metric_names"][0] = json!("FakeMetric");
    assert!(r.validate().is_err());
}
