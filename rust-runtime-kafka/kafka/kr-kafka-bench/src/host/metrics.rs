//! Reader-side producer HDR export, outside the benchmark's measured interval.
use kr_kafka_producer::telemetry::metrics::{Metric, MetricUnit, MetricsSnapshot, Scope};
use serde_json::{Value, json};

pub(super) fn summary(snapshot: &MetricsSnapshot) -> Value {
    let bounds = snapshot.bounds();
    let distributions: Vec<_> = Metric::ALL
        .into_iter()
        .map(|metric| {
            let distribution = snapshot
                .distribution(Scope::Global, metric)
                .expect("enabled metrics always contain global distributions");
            let quantile = |rank| {
                distribution
                    .quantile(rank)
                    .map(|range| json!({"low": range.low, "high": range.high}))
            };
            json!({
                "metric": name(metric),
                "unit": match metric.unit() {
                    MetricUnit::Nanoseconds => "ns",
                    MetricUnit::Bytes => "bytes",
                    MetricUnit::Count => "count",
                },
                "count": distribution.count(),
                "exact_max": distribution.exact_max(),
                "highest_trackable": distribution.highest_trackable(),
                "significant_digits": distribution.significant_digits(),
                "out_of_range": distribution.out_of_range(),
                "count_overflow": distribution.count_overflow(),
                "diagnostic_overflow": distribution.diagnostic_overflow(),
                "p50": quantile(500_000), "p95": quantile(950_000),
                "p99": quantile(990_000), "p999": quantile(999_000),
            })
        })
        .collect();
    json!({
        "schema_version": snapshot.schema_version(), "epoch": snapshot.epoch(),
        "scope": "producer lifetime; setup, offered interval, and drain",
        "interval_start_runtime_ns": bounds.start.map(|time| time.as_nanos()),
        "interval_end_runtime_ns": bounds.end.map(|time| time.as_nanos()),
        "quantiles": "inclusive HDR equivalent-value ranges; null for empty distributions",
        "depth_weighting": "event-weighted",
        "distributions": distributions,
        "diagnostics": {
            "omitted_scope_samples": snapshot.omitted_scope_samples(),
            "scope_capacity_rejections": snapshot.scope_capacity_rejections(),
            "invalid_scope_samples": snapshot.invalid_scope_samples(),
            "invalid_time_samples": snapshot.invalid_time_samples(),
            "missing_time_samples": snapshot.missing_time_samples(),
            "invalid_depth_samples": snapshot.invalid_depth_samples(),
            "overflowed": snapshot.diagnostic_overflow(),
        }
    })
}

fn name(metric: Metric) -> &'static str {
    match metric {
        Metric::ProduceRttNanos => "produce_rtt",
        Metric::BatchFillNanos => "batch_fill",
        Metric::BatchRawBytes => "batch_raw_bytes",
        Metric::BatchWireBytes => "batch_wire_bytes",
        Metric::RecordsPerBatch => "records_per_batch",
        Metric::QueueWaitNanos => "queue_wait",
        Metric::DeliveryAckedNanos => "delivery_acked",
        Metric::DeliveryNotWrittenNanos => "delivery_not_written",
        Metric::DeliveryUnknownNanos => "delivery_unknown",
        Metric::InFlightRequests => "in_flight_requests",
        Metric::InFlightWireBytes => "in_flight_wire_bytes",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use kr_kafka_producer::telemetry::metrics::{MetricsConfig, MetricsRecorder, ScopeToken};
    use kr_runtime::RuntimeInstant;

    #[test]
    fn final_export_preserves_sample_counts_ranges_rejections_and_runtime_bounds() {
        let mut recorder = MetricsRecorder::new(MetricsConfig::default()).unwrap();
        let reader = recorder.reader();
        recorder.observe_time(RuntimeInstant::from_nanos(100));
        recorder.record(Metric::ProduceRttNanos, ScopeToken::GLOBAL, 12_345);
        recorder.record(Metric::ProduceRttNanos, ScopeToken::GLOBAL, u64::MAX);
        recorder.record(Metric::RecordsPerBatch, ScopeToken::GLOBAL, 0);
        recorder.missing_time();
        recorder.observe_time(RuntimeInstant::from_nanos(99));
        recorder.observe_time(RuntimeInstant::from_nanos(200));
        assert!(reader.try_take_snapshot().is_none());
        drop(recorder);
        let report = summary(&reader.try_take_snapshot().unwrap());
        let report: Value = serde_json::from_str(&report.to_string()).unwrap();
        assert_eq!(report["interval_start_runtime_ns"], 100);
        assert_eq!(report["interval_end_runtime_ns"], 200);
        assert_eq!(report["diagnostics"]["missing_time_samples"], 1);
        assert_eq!(report["diagnostics"]["invalid_time_samples"], 1);
        let distributions = report["distributions"].as_array().unwrap();
        assert_eq!(distributions.len(), Metric::COUNT);
        let rtt = &distributions[0];
        assert_eq!(rtt["metric"], "produce_rtt");
        assert_eq!(rtt["unit"], "ns");
        assert_eq!(rtt["count"], 1);
        assert_eq!(rtt["exact_max"], 12_345);
        assert_eq!(rtt["out_of_range"], 1);
        for percentile in ["p50", "p95", "p99", "p999"] {
            assert!(rtt[percentile]["low"].as_u64().unwrap() <= 12_345);
            assert!(rtt[percentile]["high"].as_u64().unwrap() >= 12_345);
        }
        assert_eq!(distributions[1]["count"], 0);
        assert!(distributions[1]["exact_max"].is_null());
        assert!(distributions[1]["p99"].is_null());
        assert_eq!(distributions[4]["exact_max"], 0);
        assert_eq!(distributions[4]["p99"], json!({"low": 0, "high": 0}));
    }

    #[test]
    fn export_does_not_invent_missing_interval_times() {
        let recorder = MetricsRecorder::new(MetricsConfig::default()).unwrap();
        let reader = recorder.reader();
        drop(recorder);
        let report = summary(&reader.try_take_snapshot().unwrap());
        assert!(report["interval_start_runtime_ns"].is_null());
        assert!(report["interval_end_runtime_ns"].is_null());
    }
}
