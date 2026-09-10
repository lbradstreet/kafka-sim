use super::evidence::{check, count, identities, phase};
use super::*;
use crate::build::SECOND;
use kr_kafka_producer::credit::Resource;
use kr_kafka_sim::{DomainEvent as E, TimedControl};

pub(super) fn coverage(s: &Scenario, r: &RunReport) -> Result<Vec<PhaseEvidence>, String> {
    let m = &r.manifest;
    let ids = identities(r);
    let mut out = vec![];
    match s.id {
        "topology.leader-rebalance-churn" => {
            let mut accepted = BTreeMap::new();
            let mut delivered = BTreeMap::new();
            for e in &r.history.entries {
                match e.event {
                    E::Accepted {
                        record_id,
                        partition,
                        ..
                    } => {
                        accepted.insert(record_id, partition);
                    }
                    E::Delivery {
                        record_id,
                        outcome: 0,
                        ..
                    } => {
                        delivered.insert(record_id, e.now_ns - m.start_ns);
                    }
                    _ => {}
                }
            }
            for (i, c) in m
                .experiment
                .as_ref()
                .unwrap()
                .scheduled_actions
                .iter()
                .enumerate()
            {
                let TimedControl::MoveLeader {
                    partition, broker, ..
                } = c.action
                else {
                    continue;
                };
                let mut p = phase(
                    r,
                    &ids,
                    format!("leader-rotation-{i}"),
                    c.at_ns,
                    c.at_ns + 2 * SECOND,
                    broker,
                );
                let witnesses: Vec<_> = delivered
                    .iter()
                    .filter(|(id, at)| {
                        accepted.get(id) == Some(&partition)
                            && ids.last_broker(**id) == Some(broker)
                            && (c.at_ns..c.at_ns + 2 * SECOND).contains(at)
                    })
                    .map(|(id, _)| *id)
                    .collect();
                p.exact_counts
                    .insert("affected_partition_acks".into(), witnesses.len() as u64);
                p.witnesses.insert(
                    "affected_partition_acks".into(),
                    witnesses.iter().take(4).map(u64::to_string).collect(),
                );
                let active =
                    count(&p, "offered") > 0 && count(&p, "accepted") > 0 && !witnesses.is_empty();
                check(
                    &mut p,
                    "traffic through each leader phase",
                    active,
                    "The two-second phase has actual offers/admissions and acknowledged records from the moved partition through the new leader",
                )?;
                out.push(p);
            }
        }
        "topology.partition-expansion" => {
            let mut p = phase(
                r,
                &ids,
                "expanded-partition-traffic".into(),
                15 * SECOND,
                r.checkpoint.now_ns - m.start_ns + 1,
                1,
            );
            let expanded: Vec<_> = r
                .history
                .entries
                .iter()
                .filter_map(|e| match e.event {
                    E::Accepted {
                        record_id,
                        partition,
                        ..
                    } if partition >= 6 => Some((record_id, e.now_ns - m.start_ns)),
                    _ => None,
                })
                .collect();
            p.exact_counts.insert(
                "expanded_partition_admissions".into(),
                expanded.len() as u64,
            );
            p.witnesses.insert(
                "expanded_partition_record_ids".into(),
                expanded
                    .iter()
                    .take(4)
                    .map(|(id, _)| id.to_string())
                    .collect(),
            );
            check(
                &mut p,
                "new partitions become routable",
                !expanded.is_empty() && expanded.iter().all(|(_, at)| *at >= 15 * SECOND),
                "At least one record is admitted to a new partition and none precedes AddPartitions",
            )?;
            out.push(p);
        }
        "topology.delete-recreate" => {
            let reopened = m
                .experiment
                .as_ref()
                .unwrap()
                .scheduled_actions
                .iter()
                .any(|c| matches!(c.action, TimedControl::OpenTopic { .. }));
            let mut p = phase(
                r,
                &ids,
                "recreated-topic-cohort".into(),
                15 * SECOND,
                r.checkpoint.now_ns - m.start_ns + 1,
                1,
            );
            let mut new_id = [0; 16];
            for c in &m.experiment.as_ref().unwrap().scheduled_actions {
                if let TimedControl::RecreateTopic { new_id: id, .. } = c.action {
                    new_id = id;
                }
            }
            let second = m.experiment.as_ref().unwrap().loads.last().unwrap();
            let first = second.template.first_id;
            let planned = u64::from(second.shape.offer_budget()?);
            let mut acked = 0;
            let mut deleted = 0;
            for e in &r.history.entries {
                if let E::Delivery {
                    record_id,
                    outcome,
                    reason,
                    topic,
                    ..
                } = e.event
                {
                    if record_id < first {
                        continue;
                    }
                    if outcome == 0 {
                        if reopened && topic != new_id {
                            return Err("reopened cohort acknowledged under old identity".into());
                        }
                        acked += 1;
                    }
                    if outcome == 1 && reason == 3 {
                        deleted += 1;
                    }
                }
            }
            p.exact_counts.insert("new_cohort_acked".into(), acked);
            p.exact_counts
                .insert("old_handle_topic_deleted".into(), deleted);
            check(
                &mut p,
                "recreation handle semantics",
                if reopened {
                    acked == planned
                } else {
                    deleted > 0
                },
                "A reopened second cohort acknowledges entirely under the replacement UUID; an unreopened handle produces explicit TopicDeleted NotWritten results",
            )?;
            out.push(p);
        }
        "topology.multi-topic-isolation" => {
            out.extend(super::soft_evidence::coverage(s, r)?);
            let topic = m.topics[1].id;
            let mut p = phase(
                r,
                &ids,
                "independent-topic-during-delay".into(),
                10 * SECOND,
                15 * SECOND,
                2,
            );
            let acks: Vec<_> = r
                .history
                .entries
                .iter()
                .filter_map(|e| match e.event {
                    E::Delivery {
                        record_id,
                        topic: id,
                        outcome: 0,
                        ..
                    } if id == topic
                        && (10 * SECOND..15 * SECOND).contains(&(e.now_ns - m.start_ns)) =>
                    {
                        Some(record_id)
                    }
                    _ => None,
                })
                .collect();
            p.exact_counts
                .insert("topic_b_acks".into(), acks.len() as u64);
            p.witnesses.insert(
                "topic_b_acks".into(),
                acks.iter().take(4).map(u64::to_string).collect(),
            );
            check(
                &mut p,
                "independent topic progress",
                !acks.is_empty(),
                "Topic B acknowledges while topic A's leader delays appends",
            )?;
            out.push(p);
        }
        "resources.memory-bounded-overload" => {
            out.push(pressure(
                r,
                if m.experiment.as_ref().unwrap().loads[0].template.value_bytes == 512 {
                    Resource::Descriptors
                } else {
                    Resource::InputBytes
                },
            )?);
        }
        "resources.wire-window-vs-latency" => {
            out.push(super::evidence::wire(r)?);
        }
        "resources.delivery-timeout-tuning" => {
            let mut p = phase(
                r,
                &ids,
                "delivery-deadline-outage".into(),
                10 * SECOND,
                14 * SECOND,
                1,
            );
            let active = count(&p, "offered") > 0
                && count(&p, "accepted") > 0
                && count(&p, "affected_admitted") > 0
                && count(&p, "other_commit_records") > 0;
            check(
                &mut p,
                "active isolated and healthy cohorts",
                active,
                "Actual admission targets broker 1 during isolation while other brokers commit",
            )?;
            let excluded = count(&p, "affected_commit_records") == 0;
            check(
                &mut p,
                "crash excludes new appends",
                excluded,
                "Broker 1 commits no records in the exact isolation interval",
            )?;
            out.push(p);
            if m.producer.delivery_timeout.as_nanos() == 2 * SECOND {
                out.push(super::evidence::deadline_cohorts(r, &ids)?);
            }
        }
        "resources.stop-polling-backpressure" => {
            let mut p = phase(r, &ids, "polling-pause".into(), 10 * SECOND, 12 * SECOND, 1);
            let consumed = r.history.entries.iter().any(|e| {
                (10 * SECOND..12 * SECOND).contains(&(e.now_ns - m.start_ns))
                    && matches!(
                        e.event,
                        E::Delivery { .. }
                            | E::InputReleased { .. }
                            | E::FlushDone { .. }
                            | E::TopicReady { .. }
                            | E::TopicFailed { .. }
                            | E::Fatal { .. }
                            | E::Closed { .. }
                    )
            });
            let active = count(&p, "offered") > 0 && count(&p, "accepted") > 0;
            check(
                &mut p,
                "paused consumption with active offers",
                !consumed && active,
                "No client events are consumed inside the polling pause; the independent source still offers and admits records",
            )?;
            out.push(p);
            out.push(pressure(r, Resource::DeliveryEvents)?);
            let mut after = phase(
                r,
                &ids,
                "polling-resumed".into(),
                12 * SECOND,
                r.checkpoint.now_ns - m.start_ns + 1,
                1,
            );
            let recovered = count(&after, "offered") > 0 && count(&after, "acked") > 0;
            check(
                &mut after,
                "drain after polling resumes",
                recovered,
                "New offers and acknowledged event consumption resume after the pause",
            )?;
            out.push(after);
        }
        _ => {}
    }
    Ok(out)
}

/// The next Credits entry follows the failed single-record copy admission in
/// the same synchronous driver turn. It is an exact post-rollback observation.
fn pressure(r: &RunReport, resource: Resource) -> Result<PhaseEvidence, String> {
    let m = &r.manifest;
    let limit = m.producer.validate().map_err(|e| e.to_string())?.credits[resource as usize] as u64;
    let mut count_refused = 0;
    let mut global_exhaustion = 0;
    let mut witness = vec![];
    let mut postload_acked = false;
    let primary = m
        .experiment
        .as_ref()
        .unwrap()
        .loads
        .iter()
        .find(|l| matches!(l.shape, kr_kafka_sim::LoadShape::OpenLoop { .. }))
        .ok_or("pressure source must be open loop")?;
    let end = primary.shape.end_ns().unwrap_or(0);
    let mut input_sizes = vec![];
    for load in &m.experiment.as_ref().unwrap().loads {
        let record = load.template.materialize(
            0,
            m.topics[load.template.topic as usize].leaders.len(),
            m.producer.lanes,
        )?;
        let input = record.key.as_ref().map_or(0, Vec::len)
            + record.value.as_ref().map_or(0, Vec::len)
            + record
                .headers
                .iter()
                .map(|h| h.key.len() + h.value.as_ref().map_or(0, Vec::len))
                .sum::<usize>()
            + record.headers.len() * std::mem::size_of::<kr_kafka_record::OwnedHeader>();
        input_sizes.push((
            load.template.first_id,
            u64::from(load.shape.offer_budget()?),
            input as u64,
        ));
    }
    let needle = format!("resource: \"{}\"", resource.name());
    for (i, e) in r.history.entries.iter().enumerate() {
        match &e.event {
            E::Refused {
                record_id, error, ..
            } if error.contains(&needle) => {
                count_refused += 1;
                let next = r
                    .history
                    .entries
                    .get(i + 1)
                    .ok_or("missing post-refusal credit observation")?;
                let E::Credits { held, .. } = &next.event else {
                    return Err("refusal lacks immediate credit observation".into());
                };
                if next.now_ns != e.now_ns {
                    return Err("post-refusal credit observation changed time".into());
                }
                let required = if resource == Resource::InputBytes {
                    input_sizes
                        .iter()
                        .find(|(first, count, _)| (*first..*first + *count).contains(record_id))
                        .ok_or("refusal template identity")?
                        .2
                } else {
                    1
                };
                if required > limit - held[resource as usize] {
                    global_exhaustion += 1;
                    if witness.len() < 4 {
                        witness.push(record_id.to_string());
                    }
                }
            }
            E::Delivery { outcome: 0, .. } if e.now_ns - m.start_ns >= end => {
                postload_acked = true;
            }
            _ => {}
        }
    }
    let mut p = PhaseEvidence {
        phase: format!("{}-pressure", resource.name()),
        start: 0,
        end: r.checkpoint.now_ns - m.start_ns,
        exact_counts: BTreeMap::from([
            ("resource_refusals".into(), count_refused),
            (
                "global_capacity_exhaustion_witnesses".into(),
                global_exhaustion,
            ),
            ("capacity".into(), limit),
        ]),
        witnesses: BTreeMap::from([("refused_record_ids".into(), witness)]),
        check_results: vec![],
    };
    check(
        &mut p,
        "specific admission pressure",
        count_refused > 0 && global_exhaustion > 0,
        "The named pool refuses records; selected exact post-rollback snapshots prove the requested credit exceeds global availability. Additional refusals can reflect lane fairness; byte pools need not be completely full",
    )?;
    check(
        &mut p,
        "post-load delivery progress",
        postload_acked,
        "Acknowledged delivery occurs after the primary offered load ends",
    )?;
    Ok(p)
}
