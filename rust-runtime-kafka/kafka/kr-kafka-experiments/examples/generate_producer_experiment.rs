//! A compact diagnostic source, distinct from the Full benchmark catalogue.
use kr_kafka_experiments::{
    Category, ExperimentBundle, Params, Scenario, Size, Variant, build, derive_checked, js_wrapper,
};
use kr_kafka_sim::{LoadShape, LoadSpec, MetricsSampling, Partitioning, faults::IsolationWindow};
use serde_json::json;
use std::path::PathBuf;
const MAX_SAMPLE_BYTES: usize = 150 * 1024;
fn generated() -> Result<String, String> {
    let params = Params {
        outstanding: Some(8),
        ..Default::default()
    };
    let v = Variant {
        name: "sample".into(),
        summary: "48 records across a 30 ms broker outage".into(),
        params,
    };
    let s = Scenario {
        id: "sample.broker-crash",
        title: "Broker crash and recovery",
        category: Category::Hard,
        description: "A compact diagnostic run: broker 1 is isolated from 50 to 80 ms while broker 3 continues to accept records.",
        what_to_look_for: "Follow the interrupted broker-1 request, queued records, healthy-broker progress and acknowledgments after recovery.",
        variants: vec![v.clone()],
    };
    let mut m = build::base(0, Size::Test, 3)?;
    m.faults.crash_on_isolation = true;
    m.faults.isolations.push(IsolationWindow {
        broker: 1,
        start_ns: 50 * build::MS,
        end_ns: 80 * build::MS,
    });
    m.metrics_sampling = Some(MetricsSampling {
        interval_ns: 10 * build::MS,
    });
    m.limits.elapsed_ns = 2 * build::SECOND;
    m.experiment.as_mut().unwrap().offer_deadline_ns = build::SECOND;
    m.experiment.as_mut().unwrap().settle_timeout_ns = 500 * build::MS;
    m.experiment.as_mut().unwrap().close_timeout_ns = 100 * build::MS;
    m.producer.metrics.max_partition_scopes = 0;
    for (i, (at, partition)) in [
        (0, 0),
        (49_500_000, 0),
        (51_000_000, 0),
        (52_000_000, 2),
        (90_000_000, 0),
        (91_000_000, 2),
    ]
    .into_iter()
    .enumerate()
    {
        let mut template = build::template(1 + i as u64 * 8, &v.params);
        template.partitioning = Partitioning::Fixed { partition };
        m.experiment.as_mut().unwrap().loads.push(LoadSpec {
            template,
            shape: LoadShape::ClosedLoop {
                start_ns: at,
                count: 8,
                outstanding: 8,
            },
        });
    }
    m.validate()?;
    let run = kr_kafka_sim::run_replayed(&m).map_err(|e| e.to_string())?;
    let mut report = derive_checked(&s, &v, Size::Test, true, &run)?;
    report.meta["generated_by"] = json!("generate_producer_experiment");
    report.meta["workload"]["test_adjustments"] = json!(
        "Standalone sample: six eight-record cohorts, a 50–80 ms crash interval, and 10 ms global/broker HDR sampling. Full benchmark comparisons do not apply."
    );
    let bundle = ExperimentBundle {
        schema: kr_kafka_experiments::report::BUNDLE_SCHEMA.into(),
        scenario: report.meta["scenario"].clone(),
        variants: vec![json!({"name":v.name,"deltas":v.params,"order":0})],
        seeds: vec!["0".into()],
        runs: vec![report],
        comparisons: vec![],
        page: json!({"index":0,"count":1,"total_runs":1}),
    };
    bundle.validate()?;
    let text = js_wrapper::standalone(&bundle)?;
    if text.len() > MAX_SAMPLE_BYTES {
        return Err(format!("sample exceeds 150 KiB: {}", text.len()));
    }
    Ok(text)
}
fn destination() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../tools/trace-tool/producer-experiment-data.js")
}
fn main() -> Result<(), Box<dyn std::error::Error>> {
    let out = std::env::args_os()
        .nth(1)
        .map(PathBuf::from)
        .unwrap_or_else(destination);
    let text = generated()?;
    std::fs::write(&out, text)?;
    println!("{}", out.display());
    Ok(())
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn generated_producer_experiment_is_deterministic_bounded_and_covers_recovery() {
        let first = generated().unwrap();
        assert_eq!(first, generated().unwrap());
        assert!(first.len() <= MAX_SAMPLE_BYTES);
        assert_eq!(first, std::fs::read_to_string(destination()).unwrap());
        let raw = first
            .strip_prefix(&format!(
                "{}\n{}",
                js_wrapper::COMMENT,
                js_wrapper::ASSIGNMENT
            ))
            .unwrap()
            .strip_suffix(";\n")
            .unwrap();
        let bundle: ExperimentBundle = serde_json::from_str(raw).unwrap();
        bundle.validate().unwrap();
        let r = &bundle.runs[0];
        assert_eq!(r.summary["records"]["acked"], 48);
        assert_eq!(r.environment["bands"][0]["start"], 50_000_000);
        assert_eq!(r.environment["bands"][0]["end"], 80_000_000);
        assert!(r.phase_evidence.iter().any(|p| {
            p.phase == "isolation-0-during"
                && p.exact_counts
                    .get("other_commit_records")
                    .copied()
                    .unwrap_or(0)
                    > 0
        }));
        assert!(r.meta["replay_verified"].as_bool().unwrap());
        assert!(!r.hdr.is_null());
    }
}
