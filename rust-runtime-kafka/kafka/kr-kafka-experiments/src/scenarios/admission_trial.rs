use super::{common::*, *};
use crate::build::{MS, SECOND};
use kr_kafka_producer::config::DescriptorAdmissionPolicy;
use kr_kafka_sim::{DomainEvent as E, faults::IsolationWindow};

pub(super) const ISOLATION: &str = "hard.partition-admission-isolation";
pub(super) const SKEW: &str = "baseline.partition-admission-skew";
pub(super) fn owns(s: &Scenario) -> bool {
    matches!(s.id, ISOLATION | SKEW)
}
pub(super) fn catalogue() -> Vec<Scenario> {
    let policies = |rate, shape: &str| {
        [false, true].map(|enabled| {
            variant(
                format!(
                    "{shape}-rate{rate}-{}",
                    if enabled { "pressure" } else { "shared" }
                ),
                Params {
                    rate_per_s: Some(rate),
                    partition_pressure: Some(enabled),
                    extra: BTreeMap::from([(
                        "shape".into(),
                        match shape {
                            "hot" => 0,
                            "skew90" => 1,
                            "sparse1024" => 2,
                            _ => 3,
                        },
                    )]),
                    ..Default::default()
                },
            )
        })
    };
    vec![
        Scenario {
            id: ISOLATION,
            title: "Partition admission during broker failure",
            category: Category::Hard,
            description: "Six independent fixed-partition open sources continue across broker 1's 10–13 second isolation. Shared and pressure-based descriptor admission use the same total capacity and offered demand.",
            what_to_look_for: "Inspect healthy offered, refused and acknowledged traffic throughout the outage, plus recovery of accepted failed-broker records. Refused offers are outside delivery latency.",
            variants: [1000, 4000, 16000]
                .into_iter()
                .flat_map(|rate| policies(rate, "independent"))
                .collect(),
        },
        Scenario {
            id: SKEW,
            title: "Admission sharing with skew and idle partitions",
            category: Category::Baseline,
            description: "After metadata warmup, independent open sources offer either entirely to one partition or 90% to a hot partition. The sparse case has 1,024 configured partitions and only six receiving traffic. Both policies retain 64 descriptors and one lane.",
            what_to_look_for: "Compare steady acknowledgments at matched offered rates, refusal counts and descriptor utilization. Idle topology should not reduce a hot partition's allowance; pressure may intentionally leave capacity unused.",
            variants: ["hot", "skew90", "sparse1024"]
                .into_iter()
                .flat_map(|shape| {
                    [8000, 32000]
                        .into_iter()
                        .flat_map(move |rate| policies(rate, shape))
                })
                .collect(),
        },
    ]
}
pub(super) fn build(
    s: &Scenario,
    v: &Variant,
    seed: u64,
    size: Size,
) -> Result<ReplayManifest, String> {
    let mut m = crate::build::base(seed, size, 3)?;
    let p = &v.params;
    crate::build::apply(&mut m, p);
    if size == Size::Test {
        m.limits.records = 16_384;
        m.model.log_records = 16_384;
        m.model.log_batches = 16_384;
        m.model.log_bytes = 32 * 1024 * 1024;
    }
    if s.id == ISOLATION {
        m.faults.crash_on_isolation = true;
        m.faults.isolations.push(IsolationWindow {
            broker: 1,
            start_ns: 10 * SECOND,
            end_ns: 13 * SECOND,
        });
        let (start, end, rate) = if size == Size::Full {
            (0, 30 * SECOND, p.rate_per_s.unwrap())
        } else {
            m.producer.record_descriptors = 32;
            m.producer.pending_records_per_topic = 32;
            (9800 * MS, 13200 * MS, p.rate_per_s.unwrap().min(1000))
        };
        // Rotate residual rate and simultaneous-offer order, identically in both arms.
        for index in 0..6 {
            let partition = ((index + seed % 6) % 6) as i32;
            let share = rate / 6 + u64::from(index < rate % 6);
            open(&mut m, p, start, end, share, Some(partition));
        }
    } else {
        m.producer.lanes = 1;
        m.producer.record_descriptors = 64;
        m.producer.delivery_event_capacity = 64;
        m.producer.pending_records_per_topic = 64;
        m.producer.metadata_max_age = crate::build::ns(60 * SECOND);
        let shape = p.extra["shape"];
        if shape == 2 {
            m.topics[0].leaders = (0..1024).map(|p| 1 + p % 3).collect();
            m.producer.max_batches = 1024;
            m.model.partitions = 1024;
        }
        finite(&mut m, p, 0, 8, Some(0));
        let rate = p.rate_per_s.unwrap();
        let end = if size == Size::Full {
            10200 * MS
        } else {
            400 * MS
        };
        let hot = if shape == 0 { rate } else { rate * 9 / 10 };
        open(&mut m, p, 200 * MS, end, hot, Some(0));
        if shape != 0 {
            let parts = if shape == 2 {
                [1, 256, 512, 768, 1023]
            } else {
                [1, 2, 3, 4, 5]
            };
            for (i, partition) in parts.into_iter().enumerate() {
                let share = (rate - hot) / 5 + u64::from((i as u64) < (rate - hot) % 5);
                open(&mut m, p, 200 * MS, end, share, Some(partition));
            }
        }
    }
    finish(&mut m)?;
    Ok(m)
}

/// The original keyed crash and independent fixture share the same exact gate.
pub(super) fn isolation_counts(r: &RunReport) -> Result<(u64, [[u64; 30]; 6]), String> {
    let m = &r.manifest;
    let mut refusals = 0;
    let mut ack_windows = [[0; 30]; 6];
    let loads = &m.experiment.as_ref().ok_or("missing experiment")?.loads;
    for entry in &r.history.entries {
        let at = entry.now_ns - m.start_ns;
        if !(10 * SECOND..13 * SECOND).contains(&at) {
            continue;
        }
        match entry.event {
            E::Refused { record_id, .. } => {
                let load = loads
                    .iter()
                    .find(|l| {
                        record_id >= l.template.first_id
                            && record_id - l.template.first_id
                                < u64::from(l.shape.offer_budget().unwrap())
                    })
                    .ok_or("refused source")?;
                let record = load.template.materialize(
                    (record_id - load.template.first_id) as u32,
                    6,
                    m.producer.lanes,
                )?;
                if m.topics[0].leaders[record.partition as usize] != 1 {
                    refusals += 1;
                }
            }
            E::Delivery {
                partition,
                outcome: 0,
                ..
            } if (0..6).contains(&partition) => {
                ack_windows[partition as usize][((at - 10 * SECOND) / (100 * MS)) as usize] += 1;
            }
            _ => {}
        }
    }
    Ok((refusals, ack_windows))
}
pub(super) fn isolation_gate(r: &RunReport) -> Result<(), String> {
    if r.manifest.producer.descriptor_admission_policy
        != DescriptorAdmissionPolicy::PartitionPressure
    {
        return Ok(());
    }
    let (refusals, windows) = isolation_counts(r)?;
    if refusals != 0 {
        return Err(format!(
            "healthy admission gate: {refusals} outage refusals"
        ));
    }
    for (partition, windows) in windows.iter().enumerate() {
        if r.manifest.topics[0].leaders[partition] != 1 && windows.contains(&0) {
            return Err(format!(
                "healthy progress gate: partition {partition} lacks an ACK in a 100ms outage interval"
            ));
        }
    }
    Ok(())
}
pub(super) fn invariants(s: &Scenario, r: &RunReport) -> Result<(), String> {
    if r.coverage.accepted != r.coverage.acked {
        return Err("trial accepted records did not all acknowledge".into());
    }
    if s.id == ISOLATION {
        isolation_gate(r)?;
    }
    Ok(())
}
pub(super) fn coverage(s: &Scenario, r: &RunReport) -> Result<Vec<PhaseEvidence>, String> {
    if s.id != ISOLATION {
        return Ok(vec![]);
    }
    let (refused, windows) = isolation_counts(r)?;
    Ok(vec![PhaseEvidence {phase:"independent-outage-demand".into(),start:10*SECOND,end:13*SECOND,
        exact_counts:BTreeMap::from([("healthy_refused".into(),refused),("healthy_ack_windows".into(),windows.iter().enumerate().filter(|(p,_)|r.manifest.topics[0].leaders[*p]!=1).map(|(_,w)|w.iter().filter(|n|**n!=0).count() as u64).sum())]),
        witnesses:BTreeMap::new(),check_results:vec![ExpectationResult {name:"independent demand measured".into(),status:"passed".into(),detail:"Every partition has its own immutable open-loop source; healthy refusal counts and 100ms acknowledgment intervals use unsampled history.".into()}]}])
}
