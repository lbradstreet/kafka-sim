//! Phase assertions inspect exact event times before bucketing or row sampling.
use super::*;
use crate::build::SECOND;
use kr_kafka_sim::{
    DomainEvent as E, TimedControl,
    faults::{Outcome, Phase},
};
use std::collections::BTreeSet;
#[derive(Default)]
pub(super) struct Identities {
    connections: BTreeMap<u64, i32>,
    tokens: BTreeMap<u64, u64>,
    last_broker: BTreeMap<u64, i32>,
}
impl Identities {
    pub(super) fn last_broker(&self, record_id: u64) -> Option<i32> {
        self.last_broker.get(&record_id).copied()
    }
}
pub(super) fn identities(r: &RunReport) -> Identities {
    let mut ids = Identities::default();
    for e in &r.history.entries {
        match &e.event {
            E::ConnectionOpened {
                connection, broker, ..
            } => {
                ids.connections.insert(*connection, *broker);
            }
            E::Accepted {
                record_id, token, ..
            } => {
                ids.tokens.insert(*token, *record_id);
            }
            E::ClientRequestDispatched {
                connection,
                tokens,
                api: 0,
                ..
            } => {
                let broker = ids.connections[connection];
                for token in tokens {
                    ids.last_broker.insert(ids.tokens[token], broker);
                }
            }
            _ => {}
        }
    }
    ids
}
pub(super) fn phase(
    r: &RunReport,
    ids: &Identities,
    name: String,
    start: u64,
    end: u64,
    broker: i32,
) -> PhaseEvidence {
    let mut p = PhaseEvidence {
        phase: name,
        start,
        end,
        exact_counts: BTreeMap::new(),
        witnesses: BTreeMap::new(),
        check_results: vec![],
    };
    let mut add = |name: &str, count: u64, id: u64| {
        *p.exact_counts.entry(name.into()).or_default() += count;
        let witness = p.witnesses.entry(name.into()).or_default();
        if witness.len() < 4 {
            witness.push(id.to_string());
        }
    };
    for entry in &r.history.entries {
        let at = entry.now_ns - r.manifest.start_ns;
        if !(start..end).contains(&at) {
            continue;
        }
        match &entry.event {
            E::Offered { record_id, .. } => add("offered", 1, *record_id),
            E::Refused { record_id, .. } => add("refused", 1, *record_id),
            E::Accepted {
                record_id,
                partition,
                ..
            } => {
                add("accepted", 1, *record_id);
                if r.manifest.topics[0].leaders.get(*partition as usize) == Some(&broker) {
                    add("affected_admitted", 1, *record_id);
                }
            }
            E::ClientRequestDispatched {
                connection,
                request_id,
                api: 0,
                ..
            } => add(
                if ids.connections[connection] == broker {
                    "affected_dispatches"
                } else {
                    "other_dispatches"
                },
                1,
                *request_id,
            ),
            E::BrokerCommit {
                connection,
                records,
                ..
            } => add(
                if ids.connections[connection] == broker {
                    "affected_commit_records"
                } else {
                    "other_commit_records"
                },
                u64::from(*records),
                *connection,
            ),
            E::Delivery {
                record_id,
                outcome: 0,
                ..
            } => {
                add("acked", 1, *record_id);
                add(
                    if ids.last_broker.get(record_id) == Some(&broker) {
                        "affected_acks"
                    } else {
                        "other_acks"
                    },
                    1,
                    *record_id,
                );
            }
            E::Delivery {
                record_id,
                outcome: 1,
                ..
            } => add("not_written", 1, *record_id),
            E::Delivery {
                record_id,
                outcome: 2,
                ..
            } => add("unknown", 1, *record_id),
            E::FaultDecision(d)
                if d.hook.broker == broker
                    && d.hook.phase == Phase::Setup
                    && d.effects.outcome == Outcome::SetupFailure =>
            {
                add("affected_setup_failures", 1, d.hook.connection)
            }
            E::IsolationClosed {
                broker: b,
                connection,
            } if *b == broker => add("isolated_connections", 1, *connection),
            E::BrokerFrameAbandoned {
                broker: b,
                connection,
                ..
            } if *b == broker => add("abandoned_frames", 1, *connection),
            _ => {}
        }
    }
    p
}
pub(super) fn count(p: &PhaseEvidence, name: &str) -> u64 {
    p.exact_counts.get(name).copied().unwrap_or(0)
}
pub(super) fn check(
    p: &mut PhaseEvidence,
    name: &str,
    ok: bool,
    detail: &str,
) -> Result<(), String> {
    p.check_results.push(ExpectationResult {
        name: name.into(),
        status: if ok { "passed" } else { "failed" }.into(),
        detail: detail.into(),
    });
    if ok {
        Ok(())
    } else {
        Err(format!(
            "{} / {name}: {detail}; {:?}",
            p.phase, p.exact_counts
        ))
    }
}
pub(super) fn hard(s: &Scenario, r: &RunReport) -> Result<Vec<PhaseEvidence>, String> {
    let ids = identities(r);
    let m = &r.manifest;
    let duration = r.checkpoint.now_ns - m.start_ns;
    let close = m
        .experiment
        .as_ref()
        .unwrap()
        .scheduled_actions
        .iter()
        .find_map(|c| matches!(c.action, TimedControl::Close { .. }).then_some(c.at_ns));
    let mut phases = vec![];
    for (i, w) in m.faults.isolations.iter().enumerate() {
        if w.start_ns > 0 {
            let mut pre = phase(
                r,
                &ids,
                format!("isolation-{i}-before"),
                w.start_ns.saturating_sub(SECOND),
                w.start_ns,
                w.broker,
            );
            let witnessed = count(&pre, "offered") > 0 && count(&pre, "acked") > 0;
            check(
                &mut pre,
                "pre-fault traffic",
                witnessed,
                "Offers and acknowledgments must precede the exact isolation boundary",
            )?;
            phases.push(pre);
        }
        let mut during = phase(
            r,
            &ids,
            format!("isolation-{i}-during"),
            w.start_ns,
            w.end_ns,
            w.broker,
        );
        let progress = count(&during, "offered") > 0 && count(&during, "accepted") > 0;
        check(
            &mut during,
            "active outage load",
            progress,
            "The declared band contains actual offered and accepted records",
        )?;
        let no_commits = count(&during, "affected_commit_records") == 0;
        check(
            &mut during,
            "isolated append exclusion",
            no_commits,
            "The crashed broker must append no new records inside [start,end)",
        )?;
        if w.start_ns > 0 {
            let progress = count(&during, "other_commit_records") > 0;
            check(
                &mut during,
                "healthy broker progress",
                progress,
                "A different broker commits records during this exact band",
            )?;
        }
        // Setup failure is the attempt witness for requests unable to reach a
        // transport at all. B2-first bootstrap can intentionally avoid B1 setup.
        if s.id != "hard.bootstrap-down-at-start"
            || m.producer.bootstrap[0].host == m.brokers[0].host
        {
            let attempted = count(&during, "affected_dispatches")
                + count(&during, "affected_setup_failures")
                + count(&during, "isolated_connections")
                > 0;
            check(
                &mut during,
                "affected attempt",
                attempted,
                "An affected client attempt, setup failure or closed established connection must be observed",
            )?;
        }
        phases.push(during);
        let mut recovery = phase(
            r,
            &ids,
            format!("isolation-{i}-recovery"),
            w.end_ns,
            (w.end_ns + SECOND).min(duration + 1).max(w.end_ns + 1),
            w.broker,
        );
        if close.is_some_and(|at| at < w.end_ns)
            || s.id == "hard.bootstrap-down-at-start" && m.producer.bootstrap.len() == 1
        {
            recovery.check_results.push(ExpectationResult{name:"post-outage load".into(),status:"not-applicable".into(),detail:"Close or the deliberately failed topic handle prevents new acknowledged recovery traffic".into()});
        } else if s.id == "hard.short-vs-long-outage"
            && w.end_ns - w.start_ns > m.producer.delivery_timeout.as_nanos()
        {
            let progress = count(&recovery, "offered") > 0
                && count(&recovery, "acked") == count(&recovery, "offered")
                && count(&recovery, "not_written") == 0
                && count(&recovery, "unknown") == 0;
            check(
                &mut recovery,
                "post-expiry recovery probe",
                progress,
                "Every fixed post-outage offer acknowledges after ambiguous expiry and epoch recovery",
            )?;
        } else {
            let progress = count(&recovery, "offered") > 0 && count(&recovery, "acked") > 0;
            check(
                &mut recovery,
                "recovery traffic",
                progress,
                "Offers and acknowledged delivery resume after the exact isolation end",
            )?;
        }
        phases.push(recovery);
    }
    if s.id == "hard.leader-failover-during-outage" {
        let move_at = m
            .experiment
            .as_ref()
            .unwrap()
            .scheduled_actions
            .iter()
            .find_map(|c| matches!(c.action, TimedControl::MoveLeader { .. }).then_some(c.at_ns))
            .unwrap();
        let cohort: BTreeSet<_> = r
            .history
            .entries
            .iter()
            .filter_map(|e| match e.event {
                E::Accepted {
                    record_id,
                    partition: 0 | 3,
                    ..
                } if (10 * SECOND..move_at).contains(&(e.now_ns - m.start_ns)) => Some(record_id),
                _ => None,
            })
            .collect();
        let acked: Vec<_> = r
            .history
            .entries
            .iter()
            .filter_map(|e| match e.event {
                E::Delivery {
                    record_id,
                    outcome: 0,
                    ..
                } if cohort.contains(&record_id)
                    && (move_at..25 * SECOND).contains(&(e.now_ns - m.start_ns)) =>
                {
                    Some(record_id)
                }
                _ => None,
            })
            .collect();
        if cohort.is_empty() || acked.len() != cohort.len() {
            return Err("pre-move cohort did not recover before old broker returned".into());
        }
        let mut evidence = phase(
            r,
            &ids,
            "leader-failover-cohort".into(),
            move_at,
            25 * SECOND,
            2,
        );
        evidence
            .exact_counts
            .insert("cohort_accepted".into(), cohort.len() as u64);
        evidence.exact_counts.insert(
            "cohort_acked_before_old_broker_recovery".into(),
            acked.len() as u64,
        );
        evidence.witnesses.insert(
            "cohort".into(),
            acked.iter().take(4).map(u64::to_string).collect(),
        );
        check(
            &mut evidence,
            "new leader cohort",
            acked.iter().all(|id| ids.last_broker.get(id) == Some(&2)),
            "The pre-move cohort uses broker 2 and acknowledges before 25 seconds",
        )?;
        phases.push(evidence);
    }
    if s.id == "hard.short-vs-long-outage" || s.id == "hard.close-during-outage" {
        let short = if s.id == "hard.short-vs-long-outage" {
            m.faults.isolations[0].end_ns - m.faults.isolations[0].start_ns < 5 * SECOND
        } else {
            m.experiment.as_ref().unwrap().scheduled_actions.iter().any(
                |c| matches!(c.action,TimedControl::Close{deadline_ns} if deadline_ns>10*SECOND),
            )
        };
        if short {
            if r.coverage.acked != r.coverage.accepted {
                return Err("recovery-capable deadline fixture lost accepted records".into());
            }
        } else {
            phases.push(deadline_cohorts(r, &ids)?);
        }
    }
    if s.id == "hard.crash-restart-open" && r.coverage.refused == 0 {
        return Err("open outage fixture never reached admission pressure".into());
    }
    Ok(phases)
}

pub(super) fn deadline_cohorts(r: &RunReport, ids: &Identities) -> Result<PhaseEvidence, String> {
    struct Attempt {
        broker: i32,
        at: u64,
        written: Option<u64>,
        finished: Option<u64>,
        possible: bool,
        tokens: Vec<u64>,
    }
    let m = &r.manifest;
    let window = &m.faults.isolations[0];
    let mut requests = BTreeMap::<u64, Attempt>::new();
    let mut accepted = BTreeMap::new();
    let mut unknown = BTreeSet::new();
    let mut unwritten = BTreeSet::new();
    for e in &r.history.entries {
        let at = e.now_ns - m.start_ns;
        match &e.event {
            E::Accepted {
                token,
                record_id,
                partition,
                ..
            } => {
                accepted.insert(*token, (*record_id, at, *partition));
            }
            E::ClientRequestDispatched {
                request_id,
                connection,
                tokens,
                api: 0,
                ..
            } => {
                requests.insert(
                    *request_id,
                    Attempt {
                        broker: ids.connections[connection],
                        at,
                        written: None,
                        finished: None,
                        possible: false,
                        tokens: tokens.clone(),
                    },
                );
            }
            E::ClientRequestWriteCompleted { request_id, .. } => {
                if let Some(r) = requests.get_mut(request_id) {
                    r.written = Some(at);
                }
            }
            E::ClientRequestFinished {
                request_id,
                certainty,
                ..
            } => {
                if let Some(r) = requests.get_mut(request_id) {
                    r.finished = Some(at);
                    r.possible = certainty == "Applied" || certainty == "MayHaveApplied";
                }
            }
            E::Delivery {
                token, outcome: 2, ..
            } => {
                unknown.insert(*token);
            }
            E::Delivery {
                token,
                outcome: 1,
                attempts: 0,
                ..
            } => {
                unwritten.insert(*token);
            }
            _ => {}
        }
    }
    let dispatched: BTreeSet<_> = requests
        .values()
        .flat_map(|r| r.tokens.iter().copied())
        .collect();
    let possible: BTreeSet<_> = requests
        .values()
        .filter(|r| r.possible)
        .flat_map(|r| r.tokens.iter().copied())
        .collect();
    if !unknown.is_subset(&possible) {
        return Err(
            "Unknown lacks an independently observed possibly applied client attempt".into(),
        );
    }
    let interrupted: BTreeSet<_> = requests
        .values()
        .filter(|r| {
            r.broker == window.broker
                && r.at < window.start_ns
                && r.written.is_some_and(|at| at < window.start_ns)
                && r.finished.is_none_or(|at| at >= window.start_ns)
        })
        .flat_map(|r| r.tokens.iter().copied())
        .collect();
    let ambiguous: Vec<_> = unknown.intersection(&interrupted).copied().collect();
    let queued: Vec<_> = unwritten
        .difference(&dispatched)
        .filter(|token| {
            let (_, at, p) = accepted[token];
            (window.start_ns..window.end_ns).contains(&at)
                && m.topics[0].leaders.get(p as usize) == Some(&window.broker)
        })
        .copied()
        .collect();
    let mut evidence = PhaseEvidence {
        phase: "deadline-cohorts".into(),
        start: window.start_ns,
        end: r.checkpoint.now_ns - m.start_ns + 1,
        exact_counts: BTreeMap::from([
            ("interrupted_written_unknown".into(), ambiguous.len() as u64),
            ("never_dispatched_not_written".into(), queued.len() as u64),
            (
                "unknown_with_possible_application".into(),
                unknown.len() as u64,
            ),
        ]),
        witnesses: BTreeMap::from([
            (
                "ambiguous_record_ids".into(),
                ambiguous
                    .iter()
                    .take(4)
                    .map(|t| ids.tokens[t].to_string())
                    .collect(),
            ),
            (
                "queued_record_ids".into(),
                queued
                    .iter()
                    .take(4)
                    .map(|t| ids.tokens[t].to_string())
                    .collect(),
            ),
        ]),
        check_results: vec![],
    };
    check(
        &mut evidence,
        "written and queued expiry witnesses",
        !ambiguous.is_empty() && !queued.is_empty(),
        "A fully written affected request crossing the outage resolves Unknown; an accepted during-band cohort never dispatched resolves NotWritten",
    )?;
    Ok(evidence)
}

/// Complete Produce request bytes remain charged until the observed request
/// finishes. Control traffic has separate transport accounting.
pub(super) fn wire(r: &RunReport) -> Result<PhaseEvidence, String> {
    let limit = u64::from(r.manifest.producer.connection_wire_window_bytes);
    let mut requests = BTreeMap::new();
    let mut current = BTreeMap::<u64, u64>::new();
    let mut maximum = BTreeMap::<u64, u64>::new();
    let mut peak_global = 0;
    let mut total = 0;
    for entry in &r.history.entries {
        match entry.event {
            E::ClientRequestDispatched {
                connection,
                request_id,
                wire_bytes,
                api: 0,
                ..
            } => {
                requests.insert(request_id, (connection, wire_bytes));
                let now = current.entry(connection).or_default();
                *now += wire_bytes;
                if *now > limit {
                    return Err(format!(
                        "connection {connection} retained {now} Produce bytes above its {limit} wire window"
                    ));
                }
                maximum
                    .entry(connection)
                    .and_modify(|max| *max = (*max).max(*now))
                    .or_insert(*now);
                total += wire_bytes;
                peak_global = peak_global.max(total);
            }
            E::ClientRequestFinished { request_id, .. } => {
                if let Some((connection, bytes)) = requests.remove(&request_id) {
                    *current.get_mut(&connection).unwrap() -= bytes;
                    total -= bytes;
                }
            }
            _ => {}
        }
    }
    if total != 0 {
        return Err("unfinished Produce byte obligations".into());
    }
    let mut p = PhaseEvidence {
        phase: "observed-produce-wire-bounds".into(),
        start: 0,
        end: r.checkpoint.now_ns - r.manifest.start_ns,
        exact_counts: BTreeMap::from([
            ("peak_global_bytes".into(), peak_global),
            ("per_connection_limit_bytes".into(), limit),
            (
                "peak_connection_bytes".into(),
                maximum.values().copied().max().unwrap_or(0),
            ),
        ]),
        witnesses: BTreeMap::from([(
            "connections".into(),
            maximum.keys().take(4).map(u64::to_string).collect(),
        )]),
        check_results: vec![],
    };
    check(
        &mut p,
        "per-connection Produce byte bounds",
        !maximum.is_empty(),
        "Every observed Produce request contributes its full wire size from first submission through terminal completion; each connection stays within its window and all obligations retire",
    )?;
    Ok(p)
}
