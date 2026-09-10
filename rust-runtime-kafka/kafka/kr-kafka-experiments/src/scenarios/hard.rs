use super::common::*;
use super::*;
use crate::build::{MS, SECOND};
use kr_kafka_sim::{TimedControl, faults::IsolationWindow};
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
        category: Category::Hard,
        description,
        what_to_look_for: what,
        variants,
    }
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
            "hard.crash-restart-closed",
            "Broker crash and restart, closed loop",
            "Broker 1 is isolated from 10 to 13 seconds during an active closed loop.",
            "Follow the pending broker-1 cohort, progress elsewhere, and recovery after the isolation ends.",
            [16, 64]
                .into_iter()
                .flat_map(|k| {
                    [0, 1].map(move |slow| {
                        variant(
                            format!("k{k}-slow{slow}"),
                            Params {
                                outstanding: Some(k),
                                backoff_ns: Some(backoff(slow)),
                                ..Default::default()
                            },
                        )
                    })
                })
                .collect(),
        ),
        scenario(
            "hard.crash-restart-open",
            "Broker crash and restart, open loop",
            "Immutable 30-second offer schedules cross broker 1's 10–13 second isolation.",
            "Compare refusal timing with the isolation and subsequent drain, separately from delivered outcomes.",
            [1000, 4000, 16000]
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
        ),
        scenario(
            "hard.leader-failover-during-outage",
            "Leader failover during isolation",
            "Broker 1 is isolated for 15 seconds; its partition leaders move to broker 2 during the outage.",
            "The pre-move broker-1 cohort must reroute and acknowledge before the old broker returns.",
            [1, 10]
                .into_iter()
                .flat_map(|offset| {
                    [20, 1000].map(move |metadata| {
                        variant(
                            format!("move{offset}s-metadata{metadata}ms"),
                            Params {
                                metadata_max_age_ns: Some(metadata * MS),
                                extra: BTreeMap::from([("move_seconds".into(), offset)]),
                                ..Default::default()
                            },
                        )
                    })
                })
                .collect(),
        ),
        scenario(
            "hard.bootstrap-down-at-start",
            "An unavailable bootstrap endpoint",
            "Broker 1 is isolated for the first five seconds while records arrive during topic resolution.",
            "Compare endpoint fallback with a single-endpoint resolution timeout; failed admissions remain separate.",
            [0, 1, 2]
                .map(|order| {
                    variant(
                        ["b1-first", "b2-first", "single"][order as usize].into(),
                        Params {
                            extra: BTreeMap::from([("order".into(), order)]),
                            ..Default::default()
                        },
                    )
                })
                .to_vec(),
        ),
        scenario(
            "hard.rolling-restart",
            "Five-broker rolling restart",
            "Five three-second isolation windows, with either five-second spacing or one-second overlap.",
            "Each isolated broker stops committing while other brokers progress; follow recovery for every window.",
            [0, 1]
                .map(|overlap| {
                    variant(
                        format!("overlap{overlap}s"),
                        Params {
                            extra: BTreeMap::from([("overlap".into(), overlap)]),
                            ..Default::default()
                        },
                    )
                })
                .to_vec(),
        ),
        scenario(
            "hard.short-vs-long-outage",
            "Outage duration versus delivery deadline",
            "A five-second delivery deadline is compared with two- and eight-second outages.",
            "Inspect explicitly dispatched and never-written cohorts separately; Unknown needs possible prior application.",
            [2, 8]
                .into_iter()
                .flat_map(|duration| {
                    [200, 2000].map(move |request| {
                        variant(
                            format!("outage{duration}s-request{request}ms"),
                            Params {
                                delivery_timeout_ns: Some(5 * SECOND),
                                request_timeout_ns: Some(request * MS),
                                linger_ns: Some(0),
                                extra: BTreeMap::from([("duration".into(), duration)]),
                                ..Default::default()
                            },
                        )
                    })
                })
                .collect(),
        ),
        scenario(
            "hard.flapping-broker",
            "Repeated broker flaps",
            "Broker 1 is isolated every second for twenty windows beginning at ten seconds.",
            "Measure connection attempts and retry timing against backoff and 50/10 percent outage duty cycles.",
            [50, 10]
                .into_iter()
                .flat_map(|duty| {
                    [0, 1].map(move |slow| {
                        variant(
                            format!("duty{duty}-slow{slow}"),
                            Params {
                                rate_per_s: Some(2000),
                                backoff_ns: Some(backoff(slow)),
                                extra: BTreeMap::from([("duty".into(), duty)]),
                                ..Default::default()
                            },
                        )
                    })
                })
                .collect(),
        ),
        scenario(
            "hard.close-during-outage",
            "Close while a broker is isolated",
            "Broker 1 is isolated from ten to twenty seconds; Close stops offers at twelve seconds.",
            "Compare a two-second close deadline with a fifteen-second deadline that spans recovery.",
            [2, 15]
                .map(|deadline| {
                    variant(
                        format!("deadline{deadline}s"),
                        Params {
                            linger_ns: Some(0),
                            extra: BTreeMap::from([("close_seconds".into(), deadline)]),
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
        if s.id == "hard.rolling-restart" { 5 } else { 3 },
    )?;
    build::apply(&mut m, p);
    m.faults.crash_on_isolation = true;
    match s.id {
        "hard.bootstrap-down-at-start" => {
            m.faults.isolations.push(IsolationWindow {
                broker: 1,
                start_ns: 0,
                end_ns: 5 * SECOND,
            });
            match p.extra["order"] {
                1 => {
                    m.producer.bootstrap.swap(0, 1);
                    m.producer.bootstrap.truncate(2);
                }
                2 => m.producer.bootstrap.truncate(1),
                _ => m.producer.bootstrap.truncate(2),
            }
            m.producer.topic_resolve_timeout = build::ns(SECOND);
            open(&mut m, p, 0, 100 * MS, 1000, None);
            open(
                &mut m,
                p,
                5 * SECOND + 100 * MS,
                5 * SECOND + 200 * MS,
                1000,
                None,
            );
        }
        "hard.rolling-restart" => {
            let stride = if p.extra["overlap"] == 0 {
                5 * SECOND
            } else {
                2 * SECOND
            };
            for i in 0..5 {
                let start = 10 * SECOND + i * stride;
                let end = start + 3 * SECOND;
                m.faults.isolations.push(IsolationWindow {
                    broker: i as i32 + 1,
                    start_ns: start,
                    end_ns: end,
                });
                if size == Size::Test {
                    phase_fixture(&mut m, p, i as i32 + 1, start, end, 8);
                }
            }
            if size == Size::Full {
                sustained(&mut m, p, 15 * SECOND + 4 * stride);
            }
        }
        "hard.flapping-broker" => {
            for i in 0..20 {
                let start = 10 * SECOND + i * SECOND;
                let end = start + p.extra["duty"] * 10 * MS;
                m.faults.isolations.push(IsolationWindow {
                    broker: 1,
                    start_ns: start,
                    end_ns: end,
                });
                if size == Size::Test {
                    let partition = affected_partition(&m, 1);
                    finite(&mut m, p, start + 10 * MS, 4, Some(partition));
                    finite(&mut m, p, start + 20 * MS, 4, Some(2));
                    finite(&mut m, p, end + 10 * MS, 4, None);
                    m.experiment
                        .as_mut()
                        .unwrap()
                        .loads
                        .last_mut()
                        .unwrap()
                        .template
                        .partitioning = kr_kafka_sim::Partitioning::RoundRobin;
                }
            }
            if size == Size::Full {
                open(&mut m, p, 0, 31 * SECOND, 2000, None);
            } else {
                finite(&mut m, p, 10 * SECOND - 200 * MS, 16, None);
                finite(&mut m, p, 31 * SECOND, 16, None);
            }
        }
        "hard.close-during-outage" => {
            m.faults.isolations.push(IsolationWindow {
                broker: 1,
                start_ns: 10 * SECOND,
                end_ns: 20 * SECOND,
            });
            if size == Size::Full {
                sustained(&mut m, p, 25 * SECOND);
            } else {
                finite(&mut m, p, 0, 16, None);
                finite(&mut m, p, 10 * SECOND - 100 * MS, 8, Some(0));
                finite(&mut m, p, 10 * SECOND - 100_000, 8, Some(0));
                finite(&mut m, p, 10 * SECOND + 10 * MS, 8, Some(0));
                finite(&mut m, p, 11 * SECOND, 16, Some(1));
                finite(&mut m, p, 21 * SECOND, 16, None);
            }
            control(
                &mut m,
                12 * SECOND,
                TimedControl::Close {
                    deadline_ns: p.extra["close_seconds"] * SECOND,
                },
            );
        }
        _ => {
            let start = if s.id == "hard.short-vs-long-outage" {
                5 * SECOND
            } else {
                10 * SECOND
            };
            let end = if s.id == "hard.leader-failover-during-outage" {
                25 * SECOND
            } else if s.id == "hard.short-vs-long-outage" {
                start + p.extra["duration"] * SECOND
            } else {
                13 * SECOND
            };
            m.faults.isolations.push(IsolationWindow {
                broker: 1,
                start_ns: start,
                end_ns: end,
            });
            if size == Size::Full {
                if s.id == "hard.crash-restart-open" {
                    open(&mut m, p, 0, 30 * SECOND, p.rate_per_s.unwrap(), None);
                } else {
                    if s.id == "hard.short-vs-long-outage" && p.extra["duration"] == 8 {
                        // Stop the active closed loop before ambiguous expiry;
                        // fixed post-outage probes verify recovered and healthy
                        // destinations remain usable after epoch recovery.
                        sustained(&mut m, p, start + SECOND);
                        finite(&mut m, p, end + 10 * MS, 16, Some(0));
                        finite(&mut m, p, end + 20 * MS, 16, Some(1));
                    } else {
                        sustained(&mut m, p, end + 2 * SECOND);
                    }
                }
            } else {
                if s.id == "hard.crash-restart-open" {
                    phase_fixture(&mut m, p, 1, start, end, 4);
                    m.producer.record_descriptors = 32;
                    m.producer.pending_records_per_topic = 32;
                    open(&mut m, p, start + 50 * MS, start + 306 * MS, 1000, Some(0));
                } else {
                    phase_fixture(&mut m, p, 1, start, end, 16);
                }
            }
            if s.id == "hard.short-vs-long-outage" {
                finite(&mut m, p, start - 100_000, 8, Some(0));
                finite(&mut m, p, start + 100_000, 8, Some(0));
            }
            if s.id == "hard.leader-failover-during-outage" {
                let at = start + p.extra["move_seconds"] * SECOND;
                for partition in [0, 3] {
                    control(
                        &mut m,
                        at,
                        TimedControl::MoveLeader {
                            topic: 0,
                            partition,
                            broker: 2,
                        },
                    );
                }
                if size == Size::Test {
                    finite(&mut m, p, at + 20 * MS, 16, Some(0));
                }
            }
        }
    }
    if size == Size::Full
        && m.experiment
            .as_ref()
            .unwrap()
            .loads
            .iter()
            .any(|l| matches!(l.shape, kr_kafka_sim::LoadShape::ClosedLoopUntil { .. }))
    {
        let primary_end = m
            .experiment
            .as_ref()
            .unwrap()
            .loads
            .iter()
            .filter_map(|l| match l.shape {
                kr_kafka_sim::LoadShape::ClosedLoopUntil { end_ns, .. } => Some(end_ns),
                _ => None,
            })
            .max()
            .unwrap();
        for window in m.faults.isolations.clone() {
            healthy_during(
                &mut m,
                p,
                window.broker,
                window.start_ns,
                (window.end_ns + SECOND).min(primary_end),
            );
        }
    }
    finish(&mut m)?;
    Ok(m)
}
pub(super) fn invariants(s: &Scenario, r: &RunReport) -> Result<(), String> {
    let m = &r.manifest;
    let allow_loss = s.id == "hard.short-vs-long-outage"
        || s.id == "hard.close-during-outage"
        || s.id == "hard.bootstrap-down-at-start" && m.producer.bootstrap.len() == 1;
    if !allow_loss && r.coverage.acked != r.coverage.accepted {
        return Err("hard recovery did not acknowledge every accepted record".into());
    }
    let mut connections = BTreeMap::new();
    for e in &r.history.entries {
        match e.event {
            DomainEvent::ConnectionOpened {
                connection, broker, ..
            } => {
                connections.insert(connection, broker);
            }
            DomainEvent::BrokerCommit { connection, .. } => {
                let broker = connections[&connection];
                let at = e.now_ns - m.start_ns;
                if m.faults
                    .isolations
                    .iter()
                    .any(|w| w.broker == broker && (w.start_ns..w.end_ns).contains(&at))
                {
                    return Err("broker committed during isolation".into());
                }
            }
            _ => {}
        }
    }
    if s.id == "hard.crash-restart-open"
        && r.history.entries.iter().any(|e| {
            matches!(e.event, DomainEvent::Refused { .. }) && e.now_ns < m.start_ns + 10 * SECOND
        })
    {
        return Err("open crash fixture refused before the outage".into());
    }
    if s.id == "hard.bootstrap-down-at-start"
        && m.producer.bootstrap.len() == 1
        && r.coverage.not_written == 0
    {
        return Err("bootstrap timeout cohort missing".into());
    }
    if s.id == "hard.close-during-outage" {
        let close = m.start_ns + 12 * SECOND;
        if r.history
            .entries
            .iter()
            .any(|e| e.now_ns >= close && matches!(e.event, DomainEvent::Offered { .. }))
        {
            return Err("offers continued after Close".into());
        }
    }
    Ok(())
}
