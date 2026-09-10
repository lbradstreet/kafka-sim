use crate::*;
use kr_kafka_sim::{DomainEvent, RunReport};
mod admission_trial;
mod baseline;
mod common;
mod evidence;
mod hard;
mod resources;
mod soft;
mod soft_evidence;
mod topology;
mod topology_resources_evidence;
pub(super) fn catalogue() -> Vec<Scenario> {
    let mut scenarios = baseline::catalogue();
    scenarios.extend(hard::catalogue());
    scenarios.extend(soft::catalogue());
    scenarios.extend(topology::catalogue());
    scenarios.extend(resources::catalogue());
    scenarios.extend(admission_trial::catalogue());
    scenarios
}
pub(super) fn build(
    s: &Scenario,
    v: &Variant,
    seed: u64,
    size: Size,
) -> Result<ReplayManifest, String> {
    if admission_trial::owns(s) {
        return admission_trial::build(s, v, seed, size);
    }
    match s.category {
        Category::Baseline => baseline::build(s, v, seed, size),
        Category::Hard => hard::build(s, v, seed, size),
        Category::Soft => soft::build(s, v, seed, size),
        Category::Topology => topology::build(s, v, seed, size),
        Category::Resources => resources::build(s, v, seed, size),
    }
}
pub(super) fn invariants(s: &Scenario, r: &RunReport) -> Result<(), String> {
    let c = &r.coverage;
    if c.offered != c.accepted + c.refused || c.accepted != c.acked + c.not_written + c.unknown {
        return Err("terminal population accounting".into());
    }
    let mut connections = BTreeMap::<u64, u64>::new();
    let mut requests = BTreeMap::new();
    for entry in &r.history.entries {
        match entry.event {
            DomainEvent::ConnectionOpened { connection, .. } => {
                connections.insert(connection, 0);
            }
            DomainEvent::ClientRequestDispatched {
                connection,
                request_id,
                ..
            } => {
                let depth = connections
                    .get_mut(&connection)
                    .ok_or("request connection")?;
                *depth += 1;
                if *depth > u64::from(r.manifest.producer.max_in_flight_per_connection) {
                    return Err("live connection request limit exceeded".into());
                }
                requests.insert(request_id, connection);
            }
            DomainEvent::ClientRequestFinished { request_id, .. } => {
                let connection = requests.remove(&request_id).ok_or("request lifetime")?;
                *connections.get_mut(&connection).unwrap() -= 1;
            }
            _ => {}
        }
    }
    if !requests.is_empty() {
        return Err("unfinished observed requests".into());
    }
    if admission_trial::owns(s) {
        return admission_trial::invariants(s, r);
    }
    if s.id == "hard.crash-restart-open"
        && r.manifest.experiment.as_ref().is_some_and(|e| {
            matches!(
                e.loads[0].shape,
                kr_kafka_sim::LoadShape::OpenLoop {
                    start_ns: 0,
                    end_ns: 30_000_000_000,
                    ..
                }
            )
        })
    {
        admission_trial::isolation_gate(r)?;
    }
    match s.category {
        Category::Baseline => baseline::invariants(s, r),
        Category::Hard => hard::invariants(s, r),
        Category::Soft => soft::invariants(s, r),
        Category::Topology => topology::invariants(s, r),
        Category::Resources => resources::invariants(s, r),
    }
}
pub(super) fn phase_coverage(s: &Scenario, r: &RunReport) -> Result<Vec<PhaseEvidence>, String> {
    let e = r.manifest.experiment.as_ref().ok_or("missing experiment")?;
    let mut phases = Vec::new();
    for (i, load) in e.loads.iter().enumerate() {
        let start = load.shape.start_ns();
        let end = load
            .shape
            .end_ns()
            .unwrap_or(r.checkpoint.now_ns - r.manifest.start_ns);
        let mut counts = BTreeMap::new();
        let mut witnesses = BTreeMap::new();
        for entry in &r.history.entries {
            let at = entry.now_ns - r.manifest.start_ns;
            if at < start || at >= end {
                continue;
            }
            let pair = match &entry.event {
                DomainEvent::Offered {
                    load, record_id, ..
                } if *load as usize == i => Some(("offered", *record_id)),
                DomainEvent::Accepted { record_id, .. } => Some(("accepted", *record_id)),
                DomainEvent::ClientRequestDispatched {
                    request_id, api: 0, ..
                } => Some(("produce_dispatch", *request_id)),
                DomainEvent::Delivery {
                    record_id,
                    outcome: 0,
                    ..
                } => Some(("acked", *record_id)),
                _ => None,
            };
            if let Some((name, id)) = pair {
                *counts.entry(name.into()).or_insert(0) += 1;
                let selected = witnesses.entry(name.into()).or_insert_with(Vec::new);
                if selected.len() < 4 {
                    selected.push(id.to_string());
                }
            }
        }
        if counts.get("offered").copied().unwrap_or(0) == 0 {
            if e.scheduled_actions.iter().any(|c| {
                matches!(c.action, kr_kafka_sim::TimedControl::Close { .. }) && c.at_ns <= start
            }) {
                continue;
            }
            return Err(format!("load {i} has no phase witness"));
        }
        phases.push(PhaseEvidence {
            phase: format!("load-{i}"),
            start,
            end,
            exact_counts: counts,
            witnesses,
            check_results: vec![ExpectationResult {
                name: "active load".into(),
                status: "passed".into(),
                detail: "Offered events observed inside the declared half-open interval".into(),
            }],
        });
    }
    if admission_trial::owns(s) {
        if s.category == Category::Hard {
            phases.extend(evidence::hard(s, r)?);
        }
        phases.extend(admission_trial::coverage(s, r)?);
        return Ok(phases);
    }
    if s.category == Category::Hard {
        phases.extend(evidence::hard(s, r)?);
    }
    if s.category == Category::Soft {
        phases.extend(soft_evidence::coverage(s, r)?);
    }
    if matches!(s.category, Category::Topology | Category::Resources) {
        phases.extend(topology_resources_evidence::coverage(s, r)?);
    }
    Ok(phases)
}
pub(super) fn comparisons(s: &Scenario, bundle: &ExperimentBundle) -> Vec<ExpectationResult> {
    baseline::comparisons(s, bundle)
}
