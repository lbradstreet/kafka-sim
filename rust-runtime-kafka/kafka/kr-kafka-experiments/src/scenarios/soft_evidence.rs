//! Exact hook opportunities and client request witnesses for soft faults.
use super::evidence::{check, count, identities, phase};
use super::*;
use crate::build::{MS, SECOND};
use kr_kafka_sim::{DomainEvent as E, LinkDirection};

pub(super) fn coverage(s: &Scenario, r: &RunReport) -> Result<Vec<PhaseEvidence>, String> {
    let ids = identities(r);
    let m = &r.manifest;
    let mut phases = vec![];
    for (i, rule) in m.faults.environment.iter().enumerate() {
        let mut p = phase(
            r,
            &ids,
            format!("environment-{i}-during"),
            rule.start_ns,
            rule.end_ns,
            rule.broker.unwrap_or(1),
        );
        let mut opportunities = 0;
        let mut fired = 0;
        let mut draws = 0;
        for entry in &r.history.entries {
            let E::FaultDecision(d) = &entry.event else {
                continue;
            };
            let h = &d.hook;
            if h.phase != rule.phase
                || rule.broker.is_some_and(|b| b != h.broker)
                || rule.api.is_some_and(|a| Some(a) != h.api)
                || !(rule.start_ns..rule.end_ns).contains(&h.now_ns)
            {
                continue;
            }
            opportunities += 1;
            fired += u64::from(d.environment_indices.contains(&i));
            draws += u64::from(rule.probability_ppm < 1_000_000);
            if p.witnesses
                .entry("matching_connections".into())
                .or_default()
                .len()
                < 4
            {
                p.witnesses
                    .get_mut("matching_connections")
                    .unwrap()
                    .push(h.connection.to_string());
            }
        }
        p.exact_counts
            .insert("matching_hook_opportunities".into(), opportunities);
        p.exact_counts.insert("environment_firings".into(), fired);
        p.exact_counts.insert("rule_rng_draws".into(), draws);
        let load = count(&p, "offered") > 0 && count(&p, "accepted") > 0;
        check(
            &mut p,
            "active fault load",
            load,
            "Actual offers and admissions occur inside the exact rule interval",
        )?;
        check(
            &mut p,
            "matching hook coverage",
            opportunities > 0,
            "The configured broker/API/phase has matching hooks inside [start,end)",
        )?;
        if rule.probability_ppm == 1_000_000 {
            check(
                &mut p,
                "forced rule coverage",
                fired == opportunities,
                "Every matching deterministic rule opportunity fires; replay independently validates source and effects",
            )?;
        } else {
            p.check_results.push(ExpectationResult{name:"probabilistic firing rate".into(),status:"observation".into(),detail:format!("{fired}/{opportunities} matching opportunities fired at {} ppm; conditional phase reachability is counted separately",rule.probability_ppm)});
        }
        phases.push(p);
        if rule.start_ns >= SECOND {
            let mut before = phase(
                r,
                &ids,
                format!("environment-{i}-before"),
                rule.start_ns - SECOND,
                rule.start_ns,
                rule.broker.unwrap_or(1),
            );
            let active = count(&before, "offered") > 0 && count(&before, "acked") > 0;
            check(
                &mut before,
                "pre-fault load",
                active,
                "Offered and acknowledged traffic precedes the rule",
            )?;
            phases.push(before);
        }
        let mut after = phase(
            r,
            &ids,
            format!("environment-{i}-recovery"),
            rule.end_ns,
            rule.end_ns + 2 * SECOND,
            rule.broker.unwrap_or(1),
        );
        let recovered = count(&after, "offered") > 0 && count(&after, "acked") > 0;
        check(
            &mut after,
            "post-fault recovery",
            recovered,
            "Offers and acknowledged delivery occur after the rule ends",
        )?;
        phases.push(after);
    }
    for (i, w) in m.faults.link_outages.iter().enumerate() {
        let mut during = phase(
            r,
            &ids,
            format!("link-{i}-during"),
            w.start_ns,
            w.end_ns,
            w.broker,
        );
        let mut connections = BTreeMap::new();
        let mut active_at_start = BTreeMap::new();
        for e in &r.history.entries {
            if e.now_ns - m.start_ns >= w.start_ns {
                break;
            }
            match &e.event {
                E::ConnectionOpened {
                    connection, broker, ..
                } => {
                    connections.insert(*connection, *broker);
                }
                E::ClientRequestDispatched {
                    connection,
                    request_id,
                    api: 0,
                    ..
                } if connections.get(connection) == Some(&w.broker) => {
                    active_at_start.insert(*request_id, *connection);
                }
                E::ClientRequestFinished { request_id, .. } => {
                    active_at_start.remove(request_id);
                }
                _ => {}
            }
        }
        during.exact_counts.insert(
            "affected_active_at_start".into(),
            active_at_start.len() as u64,
        );
        during.witnesses.insert(
            "affected_active_at_start".into(),
            active_at_start.keys().take(4).map(u64::to_string).collect(),
        );
        let active = count(&during, "offered") > 0
            && count(&during, "accepted") > 0
            && (count(&during, "affected_dispatches")
                + count(&during, "affected_setup_failures")
                + count(&during, "affected_active_at_start"))
                > 0;
        check(
            &mut during,
            "affected link load",
            active,
            "Offers and admissions span the unavailable direction; an affected Produce request crossing the boundary, during-band dispatch, or failed setup proves an attempted route",
        )?;
        if w.direction == LinkDirection::FromBroker {
            let committed = count(&during, "affected_commit_records") > 0;
            check(
                &mut during,
                "append before response visibility",
                committed,
                "The broker commits new records while its return direction is holding response bytes",
            )?;
        }
        phases.push(during);
        let mut after = phase(
            r,
            &ids,
            format!("link-{i}-recovery"),
            w.end_ns,
            w.end_ns + 2 * SECOND,
            w.broker,
        );
        let recovered = count(&after, "offered") > 0 && count(&after, "affected_acks") > 0;
        check(
            &mut after,
            "affected link recovery",
            recovered,
            "New offers and affected-broker acknowledgments follow restoration",
        )?;
        phases.push(after);
    }
    if s.id == "soft.slow-setup" && m.producer.request_timeout.as_nanos() < 900 * MS {
        let failures = r
            .history
            .entries
            .iter()
            .filter(|e| {
                matches!(&e.event,E::SetupFinished{broker:1,result,..} if result!="Ok")
                    && (10 * SECOND..15 * SECOND).contains(&(e.now_ns - m.start_ns))
            })
            .count();
        if failures == 0 {
            return Err("short setup deadline has no failed completion witness".into());
        }
    }
    if s.id == "soft.tiny-chunk-transport" {
        phases.push(super::evidence::wire(r)?);
    }
    if s.id == "soft.throttle-window" {
        phases.push(throttle(r)?);
    }
    Ok(phases)
}
fn throttle(r: &RunReport) -> Result<PhaseEvidence, String> {
    let mut throttled = std::collections::BTreeSet::new();
    for e in &r.history.entries {
        match &e.event {
            E::FaultDecision(d) if d.effects.throttle_ms > 0 => {
                let cid = d.hook.correlation.ok_or("throttle frame identity")?;
                throttled.insert((d.hook.connection, cid));
            }
            _ => {}
        }
    }
    let mut embargo = BTreeMap::<u64, u64>::new();
    let mut responses = 0;
    let mut p = PhaseEvidence {
        phase: "consumed-throttle-eligibility".into(),
        start: 10 * SECOND,
        end: r.checkpoint.now_ns - r.manifest.start_ns,
        exact_counts: BTreeMap::new(),
        witnesses: BTreeMap::new(),
        check_results: vec![],
    };
    for e in &r.history.entries {
        let at = e.now_ns - r.manifest.start_ns;
        match &e.event {
            E::ClientRequestFinished {
                connection,
                correlation,
                result,
                ..
            } if result == "Response" && throttled.contains(&(*connection, *correlation)) => {
                responses += 1;
                embargo
                    .entry(*connection)
                    .and_modify(|end| *end = (*end).max(at + 500 * MS))
                    .or_insert(at + 500 * MS);
            }
            E::ClientRequestDispatched {
                connection,
                api: 0,
                request_id,
                ..
            } if embargo.get(connection).is_some_and(|end| at < *end) => {
                return Err(format!(
                    "Produce request {request_id} dispatched during consumed connection throttle"
                ));
            }
            _ => {}
        }
    }
    p.exact_counts
        .insert("throttled_responses_consumed".into(), responses);
    check(
        &mut p,
        "consumed throttle dispatch eligibility",
        responses > 0,
        "No subsequent Produce dispatch on an affected connection precedes the consumed throttle deadline; previously dispatched requests may arrive during the embargo",
    )?;
    Ok(p)
}
