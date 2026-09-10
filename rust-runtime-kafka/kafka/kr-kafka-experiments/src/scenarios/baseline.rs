use super::*;
use crate::build::{MS, SECOND};
use kr_kafka_sim::{LoadShape, LoadSpec, Partitioning, ValuePattern};
fn variant(name: String, params: Params) -> Variant {
    Variant {
        summary: name.replace('-', " "),
        name,
        params,
    }
}
fn scenario(
    id: &'static str,
    title: &'static str,
    description: &'static str,
    what: &'static str,
    variants: Vec<Variant>,
) -> Scenario {
    Scenario {
        id,
        title,
        category: Category::Baseline,
        description,
        what_to_look_for: what,
        variants,
    }
}
pub(super) fn catalogue() -> Vec<Scenario> {
    let mut result = vec![];
    result.push(scenario(
        "baseline.closed-loop-inflight",
        "Closed-loop request concurrency",
        "Finite keyed load at a bounded accepted-record depth.",
        "Compare latency and delivered throughput at each outstanding and request limit.",
        [1, 4, 16, 64, 256]
            .into_iter()
            .flat_map(|k| {
                [1, 5].map(move |i| {
                    variant(
                        format!("k{k}-i{i}"),
                        Params {
                            outstanding: Some(k),
                            in_flight: Some(i),
                            ..Default::default()
                        },
                    )
                })
            })
            .collect(),
    ));
    result.push(scenario(
        "baseline.open-loop-rate",
        "Open-loop offered rate",
        "Immutable scheduled offers for 20 seconds, with 64 record descriptors.",
        "Due and actual offer rates separate admission pressure from delivery latency.",
        [500, 2000, 8000, 32000]
            .map(|rate| {
                variant(
                    format!("rate{rate}"),
                    Params {
                        rate_per_s: Some(rate),
                        ..Default::default()
                    },
                )
            })
            .to_vec(),
    ));
    result.push(scenario("baseline.linger-sweep","Linger and sparse traffic","Closed-loop traffic under eight linger and sparse-seal policies.","Compare seal reasons and batching; linger is an upper waiting bound, not a universal minimum latency.",[0,1,5,20].into_iter().flat_map(|linger|[0,1].map(move|skip|variant(format!("linger{linger}-skip{skip}"),Params{linger_ns:Some(linger*MS),extra:BTreeMap::from([("skip".into(),skip)]),..Default::default()}))).collect()));
    result.push(scenario(
        "baseline.partition-fanout-skew",
        "Partition fanout and key skew",
        "A reproducible 64-key corpus routed through the producer partition hash.",
        "Key skew and partition share differ when keys collide; compare exact route counts.",
        [1, 6, 16]
            .into_iter()
            .flat_map(|partitions| {
                [0, 50, 90].map(move |skew| {
                    variant(
                        format!("p{partitions}-skew{skew}"),
                        Params {
                            extra: BTreeMap::from([
                                ("partitions".into(), partitions),
                                ("skew".into(), skew * 10_000),
                            ]),
                            ..Default::default()
                        },
                    )
                })
            })
            .collect(),
    ));
    result.push(scenario("baseline.compression","Compression by data corpus","Two-KiB repeated and deterministic random values with none or Zstd compression.","Compare bytes of unique first-dispatched batches within each corpus; random data need not shrink.",[0,1].into_iter().flat_map(|random|[0,1,3].map(move|compression|variant(format!("random{random}-zstd{compression}"),Params{compression:Some(compression),value_bytes:Some(2048),extra:BTreeMap::from([("random".into(),random)]),..Default::default()}))).collect()));
    result.push(scenario(
        "baseline.bursty-onoff",
        "Bursts and idle gaps",
        "Closed-loop bursts start every two seconds and retry admission until accepted.",
        "Look for descriptor backpressure and latency sawteeth while every admitted record drains.",
        [50, 200, 800]
            .map(|burst| {
                variant(
                    format!("burst{burst}"),
                    Params {
                        outstanding: Some(burst),
                        extra: BTreeMap::from([("burst".into(), u64::from(burst))]),
                        ..Default::default()
                    },
                )
            })
            .to_vec(),
    ));
    result.push(scenario("baseline.asymmetric-wan-broker","One distant broker","Broker 3 has 20 milliseconds of propagation in each direction.","Compare broker RTTs and request depth; a wider byte window helps only when it is limiting.",[1,5].into_iter().flat_map(|i|[64,1024].map(move|window|variant(format!("i{i}-window{window}k"),Params{in_flight:Some(i),wire_window_bytes:Some(window*1024),..Default::default()}))).collect()));
    result
}
pub(super) fn build(
    s: &Scenario,
    v: &Variant,
    seed: u64,
    size: Size,
) -> Result<ReplayManifest, String> {
    let mut m = build::base(seed, size, 3)?;
    build::apply(&mut m, &v.params);
    build::closed(&mut m, &v.params, size);
    match s.id {
        "baseline.closed-loop-inflight" => {
            if let LoadShape::ClosedLoop { count, .. } =
                &mut m.experiment.as_mut().unwrap().loads[0].shape
                && size == Size::Test
            {
                *count = 128.max(v.params.outstanding.unwrap() * 2).min(512);
            }
        }
        "baseline.open-loop-rate" => {
            let rate = v.params.rate_per_s.unwrap();
            let e = m.experiment.as_mut().unwrap();
            e.loads[0].shape = LoadShape::OpenLoop {
                start_ns: 0,
                end_ns: if size == Size::Test {
                    512 * SECOND / rate
                } else {
                    20 * SECOND
                },
                rate_per_s: rate,
            };
            m.producer.record_descriptors = 64;
            m.producer.pending_records_per_topic = 64;
        }
        "baseline.linger-sweep" => {
            m.producer.linger_skip_below_rate = if v.params.extra["skip"] == 0 {
                None
            } else {
                Some(1_000_000)
            };
        }
        "baseline.partition-fanout-skew" => {
            m.topics[0].leaders = (0..v.params.extra["partitions"])
                .map(|p| 1 + (p % 3) as i32)
                .collect();
            m.experiment.as_mut().unwrap().loads[0]
                .template
                .partitioning = Partitioning::Keyed {
                keys: 64,
                skew_ppm: v.params.extra["skew"] as u32,
            };
        }
        "baseline.compression" => {
            if v.params.extra["random"] != 0 {
                m.experiment.as_mut().unwrap().loads[0]
                    .template
                    .value_pattern = ValuePattern::Incompressible { salt: 0x93ae1107 };
            }
        }
        "baseline.bursty-onoff" => {
            let burst = v.params.extra["burst"] as u32;
            let count = if size == Size::Test {
                burst.min(128)
            } else {
                burst
            };
            let times = if size == Size::Test { 3 } else { 10 };
            let e = m.experiment.as_mut().unwrap();
            e.loads.clear();
            for i in 0..times {
                e.loads.push(LoadSpec {
                    template: build::template(1 + u64::from(i * count), &v.params),
                    shape: LoadShape::ClosedLoop {
                        start_ns: u64::from(i) * 2 * SECOND,
                        count,
                        outstanding: count,
                    },
                });
            }
            if size == Size::Test && burst == 800 {
                m.producer.record_descriptors = 16;
                m.producer.pending_records_per_topic = 16;
            }
        }
        "baseline.asymmetric-wan-broker" => {
            let link = &mut m.faults.links[2];
            link.to_broker_latency_ns = 20 * MS;
            link.from_broker_latency_ns = 20 * MS;
        }
        _ => return Err("unknown baseline".into()),
    }
    m.validate()?;
    Ok(m)
}
pub(super) fn invariants(s: &Scenario, r: &RunReport) -> Result<(), String> {
    if r.coverage.acked != r.coverage.accepted {
        return Err("baseline did not acknowledge every accepted record".into());
    }
    if s.id != "baseline.open-loop-rate" && r.coverage.refused != 0 {
        return Err("closed-loop baseline permanently refused an offer".into());
    }
    if s.id == "baseline.partition-fanout-skew" {
        let template = &r.manifest.experiment.as_ref().unwrap().loads[0].template;
        let mut expected = BTreeMap::new();
        let mut actual = BTreeMap::new();
        for e in &r.history.entries {
            if let DomainEvent::Accepted {
                record_id,
                partition,
                ..
            } = e.event
            {
                let generated = template.materialize(
                    (record_id - template.first_id) as u32,
                    r.manifest.topics[0].leaders.len(),
                    r.manifest.producer.lanes,
                )?;
                if generated.partition != partition {
                    return Err("key hash route mismatch".into());
                }
                *expected.entry(generated.partition).or_insert(0) += 1;
                *actual.entry(partition).or_insert(0) += 1;
            }
        }
        if actual != expected {
            return Err("partition multiplicity mismatch".into());
        }
    }
    Ok(())
}

pub(super) fn comparisons(s: &Scenario, bundle: &ExperimentBundle) -> Vec<ExpectationResult> {
    let result = |name: &str, status: &str, detail: &str| ExpectationResult {
        name: name.into(),
        status: status.into(),
        detail: detail.into(),
    };
    if bundle.runs.iter().any(|r| r.meta["size"] != "full") {
        return vec![result(
            "Full comparison scope",
            "not-applicable",
            "Test fixtures use reduced populations/capacities; Full trends do not apply",
        )];
    }
    if s.id != "baseline.open-loop-rate" {
        return vec![result(
            "throughput and latency trend",
            "observation",
            "Whole-run measurements are available; no general monotonicity assertion is characterized",
        )];
    }
    let find = |name: &str| {
        bundle
            .runs
            .iter()
            .find(|r| r.meta["variant"]["name"] == name && r.meta["seed"] == "0")
    };
    let (Some(low), Some(high)) = (find("rate500"), find("rate32000")) else {
        return vec![result(
            "rate endpoints",
            "not-applicable",
            "Requires seed 0 Full reports for rate500 and rate32000",
        )];
    };
    for r in [low, high] {
        let canonical = s
            .variants
            .iter()
            .find(|v| r.meta["variant"]["name"] == v.name)
            .unwrap();
        if r.meta["variant"]["deltas"] != serde_json::to_value(&canonical.params).unwrap()
            || r.config["linger_max"] != 5 * MS
            || r.config["driver"]["service_delay"] != MS
            || r.config["lanes"] != 2
        {
            return vec![result(
                "rate endpoint configuration",
                "not-applicable",
                "Characterization requires the catalogue defaults without sweep overrides",
            )];
        }
    }
    if low.meta["workload"]["planned_offers"] != 10_000
        || high.meta["workload"]["planned_offers"] != 640_000
    {
        return vec![result(
            "rate endpoint population",
            "not-applicable",
            "Requires both complete 20-second Full offer schedules",
        )];
    }
    let low_ok = low.summary["records"]["refused"] == 0
        && low.summary["latency_acked"]["p99"]
            .as_u64()
            .is_some_and(|p| p < 2 * (5 * MS + 400_000 + MS));
    let credits = &high.buckets["global"]["credits"];
    let descriptor = credits["pools"]
        .as_array()
        .and_then(|pools| pools.iter().position(|p| p == "Descriptors"));
    let saturated = descriptor.is_some_and(|i| {
        credits["held_observed_max"][i]
            .as_array()
            .is_some_and(|values| values.iter().any(|v| *v == credits["capacity"][i]))
    });
    let high_ok = high.summary["records"]["refused"]
        .as_u64()
        .is_some_and(|n| n > 0)
        && saturated;
    vec![
        result(
            "low-rate admission and latency",
            if low_ok { "passed" } else { "failed" },
            "Seed 0, Full 0–20 s, global: zero refusals and accepted-to-consumed p99 below 12.8 ms",
        ),
        result(
            "high-rate descriptor saturation",
            if high_ok { "passed" } else { "failed" },
            "Seed 0, Full 0–20 s, global: permanent admission refusals and an observed full descriptor pool",
        ),
    ]
}
