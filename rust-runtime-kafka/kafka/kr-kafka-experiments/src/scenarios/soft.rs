use super::common::*;
use super::*;
use crate::build::{MS, SECOND};
use kr_kafka_sim::{
    LinkDirection, LinkOutage, OutageMode, TimedControl,
    faults::{Effects, EnvironmentRule, IsolationWindow, Outcome, Phase, Ramp},
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
        category: Category::Soft,
        what_to_look_for: "Inspect exact fault opportunities and firings, client request lifetimes, healthy-broker progress and recovery. Cross-variant latency trends remain observations.",
        variants,
    }
}
fn product(a: &[u64], b: &[u64], make: impl Fn(u64, u64) -> (String, Params)) -> Vec<Variant> {
    a.iter()
        .flat_map(|&a| {
            b.iter()
                .map(|&b| {
                    let (name, p) = make(a, b);
                    variant(name, p)
                })
                .collect::<Vec<_>>()
        })
        .collect()
}
fn backoff(slow: u64) -> (u64, u64) {
    if slow == 0 {
        (10 * MS, 100 * MS)
    } else {
        (100 * MS, SECOND)
    }
}
pub(super) fn catalogue() -> Vec<Scenario> {
    vec![
        scenario(
            "soft.slow-broker-window",
            "A temporarily slow broker",
            "Broker 1 delays each pre-append Produce hook by 150 ms from 10 to 20 seconds.",
            product(&[1, 4], &[1, 5], |lanes, i| {
                (
                    format!("lanes{lanes}-i{i}"),
                    Params {
                        lanes: Some(lanes as u8),
                        in_flight: Some(i as u8),
                        ..Default::default()
                    },
                )
            }),
        ),
        scenario(
            "soft.degrading-broker-ramp",
            "Gradually degrading broker",
            "Broker 1's pre-append delay grows from zero to 400 ms over 5–45 seconds.",
            [200, 1000]
                .map(|r| {
                    variant(
                        format!("request{r}ms"),
                        Params {
                            request_timeout_ns: Some(r * MS),
                            ..Default::default()
                        },
                    )
                })
                .to_vec(),
        ),
        scenario(
            "soft.blackhole-vs-failfast",
            "Black hole versus immediate failure",
            "The broker-bound link to broker 1 is unavailable from 10 to 13 seconds.",
            product(&[0, 1], &[200, 1000], |mode, r| {
                (
                    format!(
                        "{}-request{r}ms",
                        if mode == 0 { "blackhole" } else { "failfast" }
                    ),
                    Params {
                        request_timeout_ns: Some(r * MS),
                        extra: BTreeMap::from([("mode".into(), mode)]),
                        ..Default::default()
                    },
                )
            }),
        ),
        scenario(
            "soft.one-way-loss-responses",
            "Responses stalled after commit",
            "Broker 1's return link holds response bytes while appends continue.",
            product(&[300, 3000], &[1, 5], |duration, i| {
                (
                    format!("outage{duration}ms-i{i}"),
                    Params {
                        in_flight: Some(i as u8),
                        linger_ns: Some(0),
                        extra: BTreeMap::from([("duration_ms".into(), duration)]),
                        ..Default::default()
                    },
                )
            }),
        ),
        scenario(
            "soft.sustained-random-loss",
            "Sustained probabilistic loss",
            "Independent pre-append and pre-response drop opportunities span the active run.",
            product(&[1, 5], &[0, 1], |loss, slow| {
                (
                    format!("loss{loss}-slow{slow}"),
                    Params {
                        backoff_ns: Some(backoff(slow)),
                        extra: BTreeMap::from([("loss_percent".into(), loss)]),
                        ..Default::default()
                    },
                )
            }),
        ),
        scenario(
            "soft.throttle-window",
            "Broker throttle window",
            "Broker 1 returns a 500 ms Produce throttle from 10 to 15 seconds.",
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
            "soft.retriable-error-storm",
            "Retriable broker errors",
            "Broker 1 rejects Produce for two seconds with error 19, 7, or 6; error 6 also moves its leaders.",
            product(&[19, 7, 6], &[0, 1], |code, slow| {
                (
                    format!("error{code}-slow{slow}"),
                    Params {
                        backoff_ns: Some(backoff(slow)),
                        extra: BTreeMap::from([("code".into(), code)]),
                        ..Default::default()
                    },
                )
            }),
        ),
        scenario(
            "soft.disconnect-storm",
            "Probabilistic disconnect storm",
            "Broker 1 disconnects at 20 percent of pre-response Produce hooks from 10 to 15 seconds.",
            [1, 5]
                .map(|i| {
                    variant(
                        format!("i{i}"),
                        Params {
                            in_flight: Some(i),
                            linger_ns: Some(0),
                            ..Default::default()
                        },
                    )
                })
                .to_vec(),
        ),
        scenario(
            "soft.slow-setup",
            "Slow connection setup",
            "After a short isolation, broker 1 delays setup by 900 ms during 10–15 seconds.",
            [200, 2000]
                .map(|r| {
                    variant(
                        format!("request{r}ms"),
                        Params {
                            request_timeout_ns: Some(r * MS),
                            ..Default::default()
                        },
                    )
                })
                .to_vec(),
        ),
        scenario(
            "soft.high-jitter",
            "Completion jitter",
            "One-millisecond propagation in each direction with zero or five milliseconds of local completion jitter.",
            product(&[0, 5], &[1, 5], |jitter, i| {
                (
                    format!("jitter{jitter}ms-i{i}"),
                    Params {
                        in_flight: Some(i as u8),
                        extra: BTreeMap::from([("jitter_ms".into(), jitter)]),
                        ..Default::default()
                    },
                )
            }),
        ),
        scenario(
            "soft.tiny-chunk-transport",
            "Tiny transport chunks",
            "64-byte chunks cross a 4 KiB pipe with one-millisecond propagation in each direction.",
            product(&[64, 1024], &[1, 5], |window, i| {
                (
                    format!("window{window}k-i{i}"),
                    Params {
                        in_flight: Some(i as u8),
                        wire_window_bytes: Some(window as u32 * 1024),
                        ..Default::default()
                    },
                )
            }),
        ),
        scenario(
            "soft.metadata-loss-during-move",
            "Metadata loss during leader movement",
            "Broker 1 drops Metadata requests during 10–12 seconds while its leaders move to broker 2.",
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
    ]
}
pub(super) fn rule(m: &mut ReplayManifest, start: u64, end: u64, phase: Phase, effects: Effects) {
    m.faults.environment.push(EnvironmentRule {
        broker: Some(1),
        api: if phase == Phase::Setup { None } else { Some(0) },
        phase,
        start_ns: start,
        end_ns: end,
        probability_ppm: 1_000_000,
        effects,
        ramp: None,
    });
}
fn move_leaders(m: &mut ReplayManifest, at: u64) {
    for partition in [0, 3] {
        control(
            m,
            at,
            TimedControl::MoveLeader {
                topic: 0,
                partition,
                broker: 2,
            },
        );
    }
}
pub(super) fn build(
    s: &Scenario,
    v: &Variant,
    seed: u64,
    size: Size,
) -> Result<ReplayManifest, String> {
    let p = &v.params;
    let mut m = build::base(seed, size, 3)?;
    build::apply(&mut m, p);
    let mut start = 10 * SECOND;
    let mut end = 15 * SECOND;
    let mut finite_only = false;
    match s.id {
        "soft.slow-broker-window" => {
            end = 20 * SECOND;
            rule(
                &mut m,
                start,
                end,
                Phase::BeforeAppend,
                Effects {
                    delay_ns: 150 * MS,
                    ..Default::default()
                },
            );
        }
        "soft.degrading-broker-ramp" => {
            start = 5 * SECOND;
            end = 45 * SECOND;
            rule(&mut m, start, end, Phase::BeforeAppend, Effects::default());
            m.faults.environment[0].ramp = Some(Ramp {
                end_delay_ns: 400 * MS,
            });
        }
        "soft.blackhole-vs-failfast" => {
            end = 13 * SECOND;
            m.faults.link_outages.push(LinkOutage {
                broker: 1,
                direction: LinkDirection::ToBroker,
                mode: if p.extra["mode"] == 0 {
                    OutageMode::BlackHole
                } else {
                    OutageMode::FailFast
                },
                start_ns: start,
                end_ns: end,
            });
        }
        "soft.one-way-loss-responses" => {
            end = start + p.extra["duration_ms"] * MS;
            m.faults.link_outages.push(LinkOutage {
                broker: 1,
                direction: LinkDirection::FromBroker,
                mode: OutageMode::BlackHole,
                start_ns: start,
                end_ns: end,
            });
        }
        "soft.sustained-random-loss" => {
            start = 0;
            for phase in [Phase::BeforeAppend, Phase::BeforeResponse] {
                rule(
                    &mut m,
                    start,
                    end,
                    phase,
                    Effects {
                        outcome: Outcome::Drop,
                        ..Default::default()
                    },
                );
                m.faults.environment.last_mut().unwrap().probability_ppm =
                    p.extra["loss_percent"] as u32 * 10_000;
            }
        }
        "soft.throttle-window" => {
            rule(
                &mut m,
                start,
                end,
                Phase::BeforeAppend,
                Effects {
                    throttle_ms: 500,
                    ..Default::default()
                },
            );
        }
        "soft.retriable-error-storm" => {
            end = 12 * SECOND;
            rule(
                &mut m,
                start,
                end,
                Phase::BeforeAppend,
                Effects {
                    reject_error: Some(p.extra["code"] as i16),
                    ..Default::default()
                },
            );
            if p.extra["code"] == 6 {
                move_leaders(&mut m, start);
            }
        }
        "soft.disconnect-storm" => {
            rule(
                &mut m,
                start,
                end,
                Phase::BeforeResponse,
                Effects {
                    outcome: Outcome::Disconnect,
                    ..Default::default()
                },
            );
            m.faults.environment[0].probability_ppm = 200_000;
        }
        "soft.slow-setup" => {
            rule(
                &mut m,
                start,
                end,
                Phase::Setup,
                Effects {
                    delay_ns: 900 * MS,
                    ..Default::default()
                },
            );
            m.faults.isolations.push(IsolationWindow {
                broker: 1,
                start_ns: start,
                end_ns: start + 100 * MS,
            });
        }
        "soft.high-jitter" => {
            finite_only = true;
            m.driver.jitter_ns = p.extra["jitter_ms"] * MS;
            m.driver.link_latency_ns = MS;
            for link in &mut m.faults.links {
                link.to_broker_latency_ns = MS;
                link.from_broker_latency_ns = MS;
            }
        }
        "soft.tiny-chunk-transport" => {
            finite_only = true;
            m.driver.chunk_bytes = 64;
            m.driver.pipe_bytes = 4096;
            for link in &mut m.faults.links {
                link.chunk_bytes = 64;
                link.to_broker_latency_ns = MS;
                link.from_broker_latency_ns = MS;
            }
        }
        "soft.metadata-loss-during-move" => {
            end = 12 * SECOND;
            rule(
                &mut m,
                start,
                end,
                Phase::BeforeAppend,
                Effects {
                    outcome: Outcome::Drop,
                    ..Default::default()
                },
            );
            m.faults.environment[0].api = Some(3);
            move_leaders(&mut m, start);
        }
        _ => return Err("unknown soft fixture".into()),
    }
    if finite_only {
        build::closed(&mut m, p, size);
    } else if size == Size::Full {
        sustained(&mut m, p, end + 2 * SECOND);
        healthy_during(&mut m, p, 1, start, end + SECOND);
    } else if s.id == "soft.sustained-random-loss" {
        open(&mut m, p, 0, 10 * SECOND, 32, None);
        finite(&mut m, p, end + 10 * MS, 16, None);
    } else if s.id == "soft.disconnect-storm" {
        finite(&mut m, p, start - 100 * MS, 16, None);
        open(&mut m, p, start + 10 * MS, end, 64, Some(0));
        finite(&mut m, p, start + 20 * MS, 16, Some(2));
        finite(&mut m, p, end + 10 * MS, 16, None);
    } else {
        phase_fixture(&mut m, p, 1, start, end, 16);
        if s.id == "soft.degrading-broker-ramp" {
            for seconds in [15, 25, 35, 44] {
                finite(&mut m, p, seconds * SECOND, 16, Some(0));
            }
        }
    }
    // These explicit cohorts create committed requests whose response visibility
    // crosses the return-link outage, independent of the main source's phase.
    if s.id == "soft.one-way-loss-responses" {
        finite(&mut m, p, start + MS, 8, Some(0));
    }
    finish(&mut m)?;
    Ok(m)
}
pub(super) fn invariants(s: &Scenario, r: &RunReport) -> Result<(), String> {
    if r.coverage.accepted != r.coverage.acked {
        return Err(format!(
            "soft failure did not recover all accepted records: {:?}",
            r.coverage
        ));
    }
    if s.id == "soft.tiny-chunk-transport" && r.coverage.partial_writes == 0 {
        return Err("tiny transport never produced a partial write".into());
    }
    Ok(())
}
