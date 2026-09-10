use super::common::*;
use super::*;
use crate::build::{MS, SECOND};
use kr_kafka_sim::{
    LanePolicy, LoadShape, LoadSpec, Partitioning, TimedControl,
    faults::{Effects, Phase},
};
fn scenario(
    id: &'static str,
    title: &'static str,
    description: &'static str,
    variants: Vec<Variant>,
) -> Scenario {
    Scenario {
        id,
        title,
        description,
        category: Category::Topology,
        what_to_look_for: "Follow immutable topic identities and the partition leader at each control boundary. Phase checks use complete record and request histories.",
        variants,
    }
}
pub(super) fn catalogue() -> Vec<Scenario> {
    vec![
        scenario(
            "topology.leader-rebalance-churn",
            "Repeated leader rotation",
            "One partition leader rotates every two seconds for thirty changes during active traffic.",
            [1, 5]
                .map(|i| {
                    variant(
                        format!("i{i}"),
                        Params {
                            in_flight: Some(i),
                            ..Default::default()
                        },
                    )
                })
                .to_vec(),
        ),
        scenario(
            "topology.partition-expansion",
            "Partition expansion",
            "Six partitions are added at 15 seconds; a second generated source follows the refreshed twelve-partition topology.",
            [20, 1000]
                .map(|age| {
                    variant(
                        format!("metadata{age}ms"),
                        Params {
                            metadata_max_age_ns: Some(age * MS),
                            ..Default::default()
                        },
                    )
                })
                .to_vec(),
        ),
        scenario(
            "topology.delete-recreate",
            "Topic deletion and recreation",
            "The topic is recreated with a new immutable identity at 15 seconds, with or without closing and reopening the client handle.",
            [0, 1]
                .map(|reopen| {
                    variant(
                        format!("reopen{reopen}"),
                        Params {
                            extra: BTreeMap::from([("reopen".into(), reopen)]),
                            ..Default::default()
                        },
                    )
                })
                .to_vec(),
        ),
        scenario(
            "topology.multi-topic-isolation",
            "A slow leader for one topic",
            "Topic A uses broker 1, delayed by 200 ms during 10–15 seconds; topic B uses broker 2 and an independent source.",
            [1, 4]
                .map(|lanes| {
                    variant(
                        format!("lanes{lanes}"),
                        Params {
                            lanes: Some(lanes),
                            request_timeout_ns: Some(SECOND),
                            ..Default::default()
                        },
                    )
                })
                .to_vec(),
        ),
    ]
}
pub(super) fn build(
    s: &Scenario,
    v: &Variant,
    seed: u64,
    size: Size,
) -> Result<ReplayManifest, String> {
    let p = &v.params;
    let mut m = build::base(
        seed,
        size,
        if s.id == "topology.leader-rebalance-churn" {
            5
        } else {
            3
        },
    )?;
    build::apply(&mut m, p);
    match s.id {
        "topology.leader-rebalance-churn" => {
            let mut leaders = m.topics[0].leaders.clone();
            for i in 0..30 {
                let at = (i + 1) * 2 * SECOND;
                let partition = (i % 6) as i32;
                leaders[partition as usize] =
                    leaders[partition as usize] % m.brokers.len() as i32 + 1;
                control(
                    &mut m,
                    at,
                    TimedControl::MoveLeader {
                        topic: 0,
                        partition,
                        broker: leaders[partition as usize],
                    },
                );
                if size == Size::Test {
                    finite(&mut m, p, at - 50 * MS, 4, Some(partition));
                    finite(&mut m, p, at + 10 * MS, 4, Some(partition));
                }
            }
            if size == Size::Full {
                sustained(&mut m, p, 62 * SECOND);
            } else {
                finite(&mut m, p, 0, 16, None);
                finite(&mut m, p, 61 * SECOND, 16, None);
            }
        }
        "topology.partition-expansion" => {
            finite(
                &mut m,
                p,
                0,
                if size == Size::Test { 64 } else { 4096 },
                None,
            );
            finite(&mut m, p, 15 * SECOND - 100 * MS, 16, None);
            control(
                &mut m,
                15 * SECOND,
                TimedControl::AddPartitions {
                    topic: 0,
                    additional_leaders: vec![1, 2, 3, 1, 2, 3],
                },
            );
            open(
                &mut m,
                p,
                15 * SECOND + 10 * MS,
                if size == Size::Test {
                    17 * SECOND + 10 * MS
                } else {
                    20 * SECOND + 10 * MS
                },
                if size == Size::Test { 64 } else { 1000 },
                None,
            );
            m.experiment
                .as_mut()
                .unwrap()
                .loads
                .last_mut()
                .unwrap()
                .template
                .partitioning = Partitioning::RoundRobin;
        }
        "topology.delete-recreate" => {
            finite(
                &mut m,
                p,
                0,
                if size == Size::Test { 64 } else { 4096 },
                None,
            );
            finite(&mut m, p, 15 * SECOND - 100 * MS, 16, None);
            let mut new_id = m.topics[0].id;
            new_id[0] ^= 0x80;
            if p.extra["reopen"] != 0 {
                control(
                    &mut m,
                    15 * SECOND - 1,
                    TimedControl::CloseTopic { topic: 0 },
                );
            }
            control(
                &mut m,
                15 * SECOND,
                TimedControl::RecreateTopic { topic: 0, new_id },
            );
            if p.extra["reopen"] != 0 {
                control(
                    &mut m,
                    15 * SECOND + MS,
                    TimedControl::OpenTopic { topic: 0 },
                );
            }
            open(
                &mut m,
                p,
                15 * SECOND + 10 * MS,
                15 * SECOND + 110 * MS,
                if size == Size::Test { 1000 } else { 4000 },
                None,
            );
        }
        "topology.multi-topic-isolation" => {
            m.topics[0].name = "topic-a".into();
            m.topics[0].leaders = vec![1; 6];
            let mut second = m.topics[0].clone();
            second.name = "topic-b".into();
            second.id[0] ^= 0x80;
            second.leaders = vec![2; 6];
            m.topics.push(second);
            super::soft::rule(
                &mut m,
                10 * SECOND,
                15 * SECOND,
                Phase::BeforeAppend,
                Effects {
                    delay_ns: 200 * MS,
                    ..Default::default()
                },
            );
            m.driver.service_delay_ns = 4 * MS;
            for topic in 0..2 {
                let first = m.experiment.as_ref().unwrap().loads.len();
                if size == Size::Full {
                    let template = build::template(next_id(&m), p);
                    m.experiment.as_mut().unwrap().loads.push(LoadSpec {
                        template,
                        shape: LoadShape::ClosedLoopUntil {
                            start_ns: 0,
                            end_ns: 17 * SECOND,
                            max_offers: 400_000,
                            outstanding: 16,
                        },
                    });
                } else {
                    for at in [
                        9 * SECOND,
                        10 * SECOND + 10 * MS,
                        12 * SECOND,
                        15 * SECOND + 10 * MS,
                    ] {
                        finite(&mut m, p, at, 32, Some(0));
                    }
                }
                for load in &mut m.experiment.as_mut().unwrap().loads[first..] {
                    load.template.topic = topic;
                    load.template.partitioning = Partitioning::Fixed { partition: 0 };
                    load.template.lane = LanePolicy::Fixed(topic as u8 % m.producer.lanes);
                }
            }
        }
        _ => return Err("unknown topology fixture".into()),
    }
    finish(&mut m)?;
    Ok(m)
}
pub(super) fn invariants(s: &Scenario, r: &RunReport) -> Result<(), String> {
    if s.id != "topology.delete-recreate" && r.coverage.accepted != r.coverage.acked {
        return Err("topology fixture did not acknowledge accepted records".into());
    }
    if s.id == "topology.leader-rebalance-churn" && r.coverage.leader_moves != 30 {
        return Err("leader rotation control count".into());
    }
    if s.id == "topology.partition-expansion" && r.coverage.expands != 1 {
        return Err("partition expansion count".into());
    }
    Ok(())
}
