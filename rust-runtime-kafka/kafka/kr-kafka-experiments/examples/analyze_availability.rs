//! Exact, demand-aware availability audit of saved experiment executions.
//! Presentation row samples are never used for these measurements.
use kr_kafka_experiments::{
    ExperimentReport,
    cli::{Index, io},
};
use kr_kafka_sim::{
    DomainEvent as E, LoadShape, ReplayManifest, RunReport, TerminalCheckpoint, TimedControl,
};
use serde::Serialize;
use serde_json::{Value, json};
use std::{
    collections::{BTreeMap, BTreeSet},
    path::PathBuf,
};
const SECOND: u64 = 1_000_000_000;
#[derive(Default)]
struct Record {
    id: u64,
    token: u64,
    load: usize,
    offer: u64,
    accept: Option<u64>,
    deliver: Option<u64>,
    topic: [u8; 16],
    partition: i32,
    intended_topic: [u8; 16],
    intended_partition: i32,
    first_dispatch: Option<u64>,
    last_dispatch: Option<u64>,
    first_arrival: Option<u64>,
    first_success: Option<u64>,
    last_broker: Option<i32>,
    dispatches: u64,
    outcome: Option<u32>,
    reason: u32,
    accept_ordinal: u64,
    deliver_ordinal: u64,
}
#[derive(Clone, Serialize, Debug)]
struct Gap {
    start: u64,
    end: u64,
    duration: u64,
    ended_by: &'static str,
    pending_at_end: u64,
}
#[derive(Default, Serialize)]
struct GapSummary {
    busy_ns: u64,
    longest: Vec<Gap>,
}
/// Split whenever an acknowledgment makes progress; restart only while demand
/// remains. Empty intervals and gaps between independent bursts are excluded.
fn pending_gaps(events: &[(u64, u64, i8)]) -> GapSummary {
    let mut pending = 0u64;
    let mut since = None;
    let mut busy_since = None;
    let mut result = GapSummary::default();
    for &(at, _, kind) in events {
        if kind == 1 {
            if pending == 0 {
                since = Some(at);
                busy_since = Some(at);
            }
            pending += 1;
            continue;
        }
        assert!(pending > 0);
        pending -= 1;
        if kind == 0 || pending == 0 {
            if let Some(start) = since {
                result.longest.push(Gap {
                    start,
                    end: at,
                    duration: at - start,
                    ended_by: if kind == 0 {
                        "acked"
                    } else {
                        "terminal-without-ack"
                    },
                    pending_at_end: pending,
                });
            }
            since = if pending > 0 { Some(at) } else { None };
        }
        if pending == 0 {
            result.busy_ns += at - busy_since.take().unwrap();
        }
    }
    assert_eq!(pending, 0);
    result
}
fn top(gaps: &[Gap], limit: usize) -> Vec<Gap> {
    let mut v = gaps.to_vec();
    v.sort_by_key(|g| (std::cmp::Reverse(g.duration), g.start));
    v.truncate(limit);
    v
}
fn quantiles(mut values: Vec<u64>) -> Value {
    values.sort_unstable();
    let n = values.len();
    if n == 0 {
        return json!({"count":0,"p50":null,"p99":null,"max":null});
    }
    json!({"count":n,"p50":values[(n*50).div_ceil(100)-1],"p99":values[(n*99).div_ceil(100)-1],"max":values[n-1]})
}
fn hex(id: [u8; 16]) -> String {
    id.iter().map(|b| format!("{b:02x}")).collect()
}
fn range_count(times: &[u64], a: u64, b: u64) -> usize {
    times.partition_point(|t| *t < b) - times.partition_point(|t| *t < a)
}
fn interval_pending(events: &[(u64, u64, i8)], a: u64) -> u64 {
    events
        .iter()
        .take_while(|(t, _, _)| *t < a)
        .map(|(_, _, k)| if *k == 1 { 1i64 } else { -1 })
        .sum::<i64>() as u64
}
fn phase_summary(
    records: &[&Record],
    events: &[(u64, u64, i8)],
    gaps: &[Gap],
    a: u64,
    b: u64,
) -> Value {
    let count = |f: fn(&Record) -> Option<u64>| {
        records
            .iter()
            .filter(|r| f(r).is_some_and(|t| a <= t && t < b))
            .count()
    };
    let acked = records
        .iter()
        .filter(|r| r.outcome == Some(0) && r.deliver.is_some_and(|t| a <= t && t < b))
        .count();
    let span = gaps
        .iter()
        .filter_map(|g| {
            let s = g.start.max(a);
            let e = g.end.min(b);
            (s < e).then(|| (e - s, s, e))
        })
        .max();
    let latency = quantiles(
        records
            .iter()
            .filter(|r| r.outcome == Some(0) && r.accept.is_some_and(|t| a <= t && t < b))
            .map(|r| r.deliver.unwrap() - r.accept.unwrap())
            .collect(),
    );
    json!({"accepted":count(|r|r.accept),"acked":acked,"delivered":count(|r|r.deliver),"first_dispatches":count(|r|r.first_dispatch),"pending_before_start":interval_pending(events,a),"pending_before_end":interval_pending(events,b),"longest_pending_no_ack":span.map(|(duration,start,end)|json!({"start":start,"end":end,"duration":duration})),"accepted_cohort_acked_latency":latency})
}
fn record_json(r: &Record) -> Value {
    json!({"id":r.id.to_string(),"token":r.token.to_string(),"load":r.load,"offer":r.offer,"accept":r.accept,"deliver":r.deliver,"topic":hex(r.topic),"partition":r.partition,"intended_topic":hex(r.intended_topic),"intended_partition":r.intended_partition,"first_dispatch":r.first_dispatch,"last_dispatch":r.last_dispatch,"first_broker_arrival":r.first_arrival,"first_success_response_visible":r.first_success,"last_broker":r.last_broker,"dispatches":r.dispatches,"outcome":r.outcome,"reason":r.reason,"latency":r.accept.zip(r.deliver).map(|(a,d)|d-a),"first_dispatch_delay":r.accept.zip(r.first_dispatch).map(|(a,d)|d-a),"success_to_consumption":r.first_success.zip(r.deliver).and_then(|(a,d)|d.checked_sub(a))})
}
fn analyze(run: &RunReport, report: &ExperimentReport) -> Result<Value, String> {
    let m = &run.manifest;
    let exp = m.experiment.as_ref().ok_or("experiment required")?;
    let origin = m.start_ns;
    let end = run.checkpoint.now_ns - origin;
    let mut rows = Vec::<Record>::new();
    let mut ids = BTreeMap::new();
    let mut tokens = BTreeMap::new();
    let mut connections = BTreeMap::new();
    let mut live_topics: Vec<_> = m.topics.iter().map(|t| (t.id, t.leaders.len())).collect();
    let mut offers = vec![vec![]; exp.loads.len()];
    let mut refusals = vec![vec![]; exp.loads.len()];
    let mut offered_routes = BTreeMap::<([u8; 16], i32), Vec<u64>>::new();
    let mut refused_routes = BTreeMap::<([u8; 16], i32), Vec<u64>>::new();
    let mut close_at = None;
    let mut closed_at = None;
    let mut controls = vec![];
    let mut refused_reasons = BTreeMap::<String, u64>::new();
    let mut max_open_lateness = 0;
    for e in &run.history.entries {
        let at = e.now_ns - origin;
        match &e.event {
            E::ConnectionOpened {
                connection, broker, ..
            } => {
                connections.insert(*connection, *broker);
            }
            E::ScheduledControl { action, .. } => {
                controls.push(json!({"at":at,"action":action}));
                match action {
                    TimedControl::RecreateTopic { topic, new_id } => {
                        live_topics[*topic as usize].0 = *new_id
                    }
                    TimedControl::AddPartitions {
                        topic,
                        additional_leaders,
                    } => live_topics[*topic as usize].1 += additional_leaders.len(),
                    TimedControl::Close { .. } => {
                        close_at = Some(at);
                    }
                    _ => {}
                }
            }
            E::Closed { .. } => closed_at = Some(at),
            E::Offered {
                load,
                record_id,
                due_ns,
            } => {
                let load = *load as usize;
                let spec = &exp.loads[load];
                let (topic, np) = live_topics[spec.template.topic as usize];
                let mut template = spec.template.clone();
                template.value_bytes = 0;
                let materialized = template.materialize(
                    u32::try_from(record_id - template.first_id).map_err(|e| e.to_string())?,
                    np,
                    m.producer.lanes,
                )?;
                offers[load].push(at);
                offered_routes
                    .entry((topic, materialized.partition))
                    .or_default()
                    .push(at);
                if matches!(spec.shape, LoadShape::OpenLoop { .. }) {
                    max_open_lateness = max_open_lateness.max(e.now_ns - due_ns);
                }
                assert!(ids.insert(*record_id, rows.len()).is_none());
                rows.push(Record {
                    id: *record_id,
                    load,
                    offer: at,
                    intended_topic: topic,
                    intended_partition: materialized.partition,
                    partition: -1,
                    ..Default::default()
                });
            }
            E::Refused {
                record_id, error, ..
            } => {
                let r = &mut rows[ids[record_id]];
                r.outcome = Some(3);
                refusals[r.load].push(at);
                refused_routes
                    .entry((r.intended_topic, r.intended_partition))
                    .or_default()
                    .push(at);
                *refused_reasons.entry(error.clone()).or_default() += 1;
            }
            E::Accepted {
                record_id,
                token,
                topic,
                partition,
                ..
            } => {
                let index = ids[record_id];
                tokens.insert(*token, index);
                let r = &mut rows[index];
                r.token = *token;
                r.accept = Some(at);
                r.accept_ordinal = e.ordinal;
                r.topic = *topic;
                r.partition = *partition;
            }
            E::ClientRequestDispatched {
                connection,
                tokens: ts,
                api: 0,
                ..
            } => {
                for token in ts {
                    let r = &mut rows[tokens[token]];
                    r.first_dispatch.get_or_insert(at);
                    r.last_dispatch = Some(at);
                    r.last_broker = Some(connections[connection]);
                    r.dispatches += 1;
                }
            }
            E::BrokerRequest {
                records, api: 0, ..
            } => {
                // The broker audit calls these `records`, but carries accepted
                // producer tokens, which differ from workload record IDs.
                for token in records {
                    rows[tokens[token]].first_arrival.get_or_insert(at);
                }
            }
            E::ProduceResponse { token, .. } => {
                rows[tokens[token]].first_success.get_or_insert(at);
            }
            E::Delivery {
                record_id,
                topic,
                partition,
                outcome,
                reason,
                ..
            } => {
                let r = &mut rows[ids[record_id]];
                r.deliver = Some(at);
                r.deliver_ordinal = e.ordinal;
                r.topic = *topic;
                r.partition = *partition;
                r.outcome = Some(*outcome);
                r.reason = *reason;
            }
            _ => {}
        }
    }
    let mut counts = [0u64; 4];
    for r in &rows {
        counts[r.outcome.ok_or("unterminated offer")? as usize] += 1;
    }
    for (i, name) in ["acked", "not_written", "unknown", "refused"]
        .iter()
        .enumerate()
    {
        if counts[i]
            != report.summary["records"][name]
                .as_u64()
                .ok_or("summary count")?
        {
            return Err("history/report count mismatch".into());
        }
    }
    let mut phases = vec![];
    if report.meta["scenario"]["id"] == "baseline.partition-admission-skew" && end > SECOND {
        phases.push(("steady".into(), SECOND, 10_200_000_000.min(end)));
    }
    for (i, b) in report.environment["bands"]
        .as_array()
        .ok_or("bands")?
        .iter()
        .enumerate()
    {
        let a = b["start"].as_u64().unwrap();
        let z = b["end"].as_u64().unwrap();
        for (label, start, finish) in [
            ("before", a.saturating_sub(SECOND), a),
            ("during", a, z.min(end)),
            ("after", z, (z + SECOND).min(end)),
        ] {
            if start < finish {
                phases.push((format!("band-{i}-{label}"), start, finish));
            }
        }
    }
    let mut grouped = BTreeMap::<([u8; 16], i32), Vec<&Record>>::new();
    for r in &rows {
        if r.accept.is_some() {
            grouped.entry((r.topic, r.partition)).or_default().push(r);
        }
    }
    for p in report.topology["partitions"].as_array().unwrap() {
        let uuid = p["topic_id"].as_str().unwrap();
        let mut id = [0; 16];
        for (i, b) in id.iter_mut().enumerate() {
            *b = u8::from_str_radix(&uuid[i * 2..i * 2 + 2], 16).unwrap();
        }
        grouped
            .entry((id, p["partition"].as_i64().unwrap() as i32))
            .or_default();
    }
    let mut partitions = vec![];
    let mut selected = BTreeSet::new();
    let mut global_longest = 0;
    for ((topic, partition), rs) in grouped {
        let mut events = vec![];
        for r in &rs {
            events.push((r.accept.unwrap(), r.accept_ordinal, 1));
            events.push((
                r.deliver.unwrap(),
                r.deliver_ordinal,
                if r.outcome == Some(0) { 0 } else { -1 },
            ));
        }
        events.sort_unstable();
        let gaps = pending_gaps(&events);
        let top_gaps = top(&gaps.longest, 8);
        global_longest = global_longest.max(top_gaps.first().map(|g| g.duration).unwrap_or(0));
        let mut acks: Vec<_> = rs
            .iter()
            .filter(|r| r.outcome == Some(0))
            .map(|r| r.deliver.unwrap())
            .collect();
        acks.sort_unstable();
        let raw_gaps: Vec<_> = acks
            .windows(2)
            .map(|w| Gap {
                start: w[0],
                end: w[1],
                duration: w[1] - w[0],
                ended_by: "acked",
                pending_at_end: 0,
            })
            .collect();
        let mut worst = rs.clone();
        worst.sort_by_key(|r| std::cmp::Reverse(r.deliver.unwrap() - r.accept.unwrap()));
        for r in worst.iter().take(2) {
            selected.insert(r.id);
        }
        let offered = offered_routes
            .get(&(topic, partition))
            .cloned()
            .unwrap_or_default();
        let refused = refused_routes
            .get(&(topic, partition))
            .cloned()
            .unwrap_or_default();
        let phases: Vec<_> = phases
            .iter()
            .map(|(name, a, b)| {
                let mut v = phase_summary(&rs, &events, &gaps.longest, *a, *b);
                v["name"] = json!(name);
                v["start"] = json!(a);
                v["end"] = json!(b);
                v["intended_offers"] = json!(range_count(&offered, *a, *b));
                v["intended_refusals"] = json!(range_count(&refused, *a, *b));
                v
            })
            .collect();
        let outage_ack_windows: Vec<_> = (0..30)
            .map(|i| {
                range_count(
                    &acks,
                    10 * SECOND + i * 100_000_000,
                    10 * SECOND + (i + 1) * 100_000_000,
                )
            })
            .collect();
        partitions.push(json!({"topic":hex(topic),"partition":partition,"accepted":rs.len(),"acked":acks.len(),"not_written":rs.iter().filter(|r|r.outcome==Some(1)).count(),"unknown":rs.iter().filter(|r|r.outcome==Some(2)).count(),"first_accept":events.first().map(|e|e.0),"last_delivery":events.last().map(|e|e.0),"busy_ns":gaps.busy_ns,"pending_no_ack_spans":top_gaps,"raw_ack_gaps":top(&raw_gaps,4),"latency_acked":quantiles(rs.iter().filter(|r|r.outcome==Some(0)).map(|r|r.deliver.unwrap()-r.accept.unwrap()).collect()),"latency_all":quantiles(rs.iter().map(|r|r.deliver.unwrap()-r.accept.unwrap()).collect()),"accept_to_first_dispatch":quantiles(rs.iter().filter_map(|r|r.first_dispatch.map(|d|d-r.accept.unwrap())).collect()),"success_to_consumption":quantiles(rs.iter().filter_map(|r|r.first_success.and_then(|s|r.deliver.unwrap().checked_sub(s))).collect()),"longest_records":worst.iter().take(4).map(|r|record_json(r)).collect::<Vec<_>>(),"outage_100ms_ack_counts":outage_ack_windows,"phases":phases}));
    }
    let mut loads = vec![];
    for (i, spec) in exp.loads.iter().enumerate() {
        let times = &offers[i];
        let stop = spec
            .shape
            .end_ns()
            .map(|t| t.min(close_at.unwrap_or(end)).min(end));
        let mut gaps: Vec<_> = times
            .windows(2)
            .map(|w| Gap {
                start: w[0],
                end: w[1],
                duration: w[1] - w[0],
                ended_by: "next-offer",
                pending_at_end: 0,
            })
            .collect();
        if let (Some(last), Some(stop)) = (times.last(), stop)
            && *last < stop
        {
            gaps.push(Gap {
                start: *last,
                end: stop,
                duration: stop - last,
                ended_by: "source-end",
                pending_at_end: 0,
            });
        }
        let rs: Vec<_> = rows.iter().filter(|r| r.load == i).collect();
        let phases:Vec<_>=phases.iter().map(|(name,a,b)|json!({"name":name,"start":a,"end":b,"offered":range_count(times,*a,*b),"refused":range_count(&refusals[i],*a,*b),"acked":rs.iter().filter(|r|r.outcome==Some(0)&&r.deliver.is_some_and(|t|*a<=t&&t<*b)).count()})).collect();
        loads.push(json!({"load":i,"spec":spec,"offered":times.len(),"refused":refusals[i].len(),"first_offer":times.first(),"last_offer":times.last(),"offer_gaps":top(&gaps,8),"phases":phases}));
    }
    // Retain complete correlated attempts for only the selected slow records.
    let selected_tokens: BTreeSet<_> = selected.iter().map(|id| rows[ids[id]].token).collect();
    let mut requests = BTreeMap::<u64, Value>::new();
    let mut correlated = BTreeMap::new();
    for e in &run.history.entries {
        let at = e.now_ns - origin;
        match &e.event {
            E::ClientRequestDispatched {
                request_id,
                connection,
                correlation,
                api: 0,
                tokens: ts,
                wire_bytes,
                batches,
            } if ts.iter().any(|t| selected_tokens.contains(t)) => {
                correlated.insert((*connection, *correlation), *request_id);
                requests.insert(*request_id,json!({"request_id":request_id.to_string(),"connection":connection.to_string(),"correlation":correlation,"broker":connections[connection],"dispatch":at,"wire_bytes":wire_bytes,"record_ids":ts.iter().filter(|t|selected_tokens.contains(t)).map(|t|rows[tokens[t]].id.to_string()).collect::<Vec<_>>(),"batches":batches,"events":[]}));
            }
            E::ClientRequestWriteCompleted { request_id, .. } => {
                if let Some(r) = requests.get_mut(request_id) {
                    r["full_write"] = json!(at);
                }
            }
            E::ClientRequestFinished {
                request_id,
                result,
                confirmed,
                certainty,
                ..
            } => {
                if let Some(r) = requests.get_mut(request_id) {
                    r["finish"] = json!(at);
                    r["result"] = json!(result);
                    r["confirmed"] = json!(confirmed);
                    r["certainty"] = json!(certainty);
                }
            }
            E::BrokerRequest {
                connection,
                correlation,
                api: 0,
                ..
            } => {
                if let Some(id) = correlated.get(&(*connection, *correlation)) {
                    requests.get_mut(id).unwrap()["arrival"] = json!(at);
                }
            }
            E::BrokerCommit {
                connection,
                correlation,
                records,
                batches,
            } => {
                if let Some(id) = correlated.get(&(*connection, *correlation)) {
                    requests.get_mut(id).unwrap()["commit"] =
                        json!({"at":at,"records":records,"batches":batches});
                }
            }
            E::ResponseRead {
                connection,
                correlation,
            } => {
                if let Some(id) = correlated.get(&(*connection, *correlation)) {
                    requests.get_mut(id).unwrap()["response_visible"] = json!(at);
                }
            }
            E::BrokerFrameAbandoned {
                connection,
                correlation,
                ..
            } => {
                if let Some(id) = correlated.get(&(*connection, *correlation)) {
                    requests.get_mut(id).unwrap()["abandoned"] = json!(at);
                }
            }
            E::FaultDecision(d) => {
                if let Some(c) = d.hook.correlation
                    && let Some(id) = correlated.get(&(d.hook.connection, c))
                {
                    let effects = &d.effects;
                    let evt = json!({"at":at,"phase":d.hook.phase,"delay_ns":effects.delay_ns,"outcome":effects.outcome,"reject_error":effects.reject_error,"throttle_ms":effects.throttle_ms,"environment_indices":d.environment_indices});
                    requests.get_mut(id).unwrap()["events"]
                        .as_array_mut()
                        .unwrap()
                        .push(evt);
                }
            }
            _ => {}
        }
    }
    if requests.len() > 16384 {
        return Err("analysis request witness bound".into());
    }
    let witnesses = selected
        .iter()
        .map(|id| record_json(&rows[ids[id]]))
        .collect::<Vec<_>>();
    let mut pressure_witnesses = Vec::new();
    let mut pressure_counts = BTreeMap::<String, u64>::new();
    for entry in &run.history.entries {
        if let E::DescriptorPressure {
            record_id,
            topic,
            partition,
            capacity,
            shared_limit,
            total_held,
            class_held,
        } = &entry.event
        {
            if *total_held < *shared_limit
                || *total_held > *capacity
                || class_held < &(capacity - total_held)
            {
                return Err("invalid descriptor-pressure decision witness".into());
            }
            *pressure_counts
                .entry(format!(
                    "{}:{}",
                    topic.map_or_else(|| "unclassified".into(), hex),
                    partition.map_or_else(|| "?".into(), |p| p.to_string())
                ))
                .or_default() += 1;
            if pressure_witnesses.len() < 16 {
                pressure_witnesses.push(json!({"record_id":record_id.to_string(),"at":entry.now_ns-origin,"topic":topic.map(hex),"partition":partition,"capacity":capacity,"shared_limit":shared_limit,"total_held":total_held,"class_held":class_held}));
            }
        }
    }
    Ok(
        json!({"schema":"kr-kafka-availability-analysis/v1","scenario":report.meta["scenario"],"variant":report.meta["variant"],"seed":report.meta["seed"],"source":m.versions,"checkpoint_verified":true,"history_events":run.history.entries.len(),"duration":end,"close_at":close_at,"closed_at":closed_at,"runtime_after_closed":closed_at.map(|t|end-t),"max_open_loop_offer_lateness":max_open_lateness,"summary":report.summary,"environment":report.environment,"controls":controls,"config":report.config,"counts":counts,"refused_reasons":refused_reasons,"descriptor_pressure":{"counts":pressure_counts,"witnesses":pressure_witnesses},"longest_partition_pending_no_ack":global_longest,"partitions":partitions,"loads":loads,"witnesses":witnesses,"witness_requests":requests.into_values().collect::<Vec<_>>()}),
    )
}
fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<_> = std::env::args().collect();
    let source = PathBuf::from(
        args.get(1)
            .map(String::as_str)
            .unwrap_or("target/experiments/full-suite"),
    );
    let out = PathBuf::from(
        args.get(2)
            .map(String::as_str)
            .unwrap_or("target/experiments/availability-analysis"),
    );
    let filter = args.get(3);
    let index = Index::load(&source)?;
    let mut saved = vec![];
    for row in index.runs {
        let key = format!("{}/{}", row.scenario, row.variant.name);
        if filter.is_some_and(|s| !key.contains(s)) {
            continue;
        }
        let path = out.join(format!("{}/{}.json", row.scenario, row.variant.name));
        let bytes = io::read(
            &io::imported(
                &source,
                row.replay_manifest.as_ref().ok_or("missing replay")?,
            )?,
            kr_kafka_sim::MAX_EXPERIMENT_MANIFEST_BYTES,
        )?;
        let m = ReplayManifest::from_json(&bytes)?;
        if m.fault_decisions.is_none() {
            return Err("realized tape required".into());
        }
        drop(bytes);
        let expected: TerminalCheckpoint = serde_json::from_slice(&io::read(
            &io::imported(&source, row.checkpoint.as_ref().ok_or("checkpoint")?)?,
            1024 * 1024,
        )?)?;
        let report = ExperimentReport::from_json(&io::read(
            &io::imported(&source, row.report.as_ref().ok_or("report")?)?,
            5 * 1024 * 1024,
        )?)?;
        eprintln!("analyzing {key}");
        let run = kr_kafka_sim::run(&m).map_err(|e| e.to_string())?;
        if run.checkpoint != expected {
            return Err(format!("saved checkpoint mismatch: {key}").into());
        }
        let data = analyze(&run, &report)?;
        io::write_json(&path, &data, 16 * 1024 * 1024)?;
        saved.push(path.to_string_lossy().into_owned());
        eprintln!(
            "analyzed {key}: max pending no-ack {} ms",
            data["longest_partition_pending_no_ack"].as_u64().unwrap() / 1_000_000
        );
    }
    io::write_json(
        &out.join("index.json"),
        &json!({"schema":"kr-kafka-availability-analysis-index/v1","runs":saved}),
        1024 * 1024,
    )?;
    Ok(())
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn pending_gaps_exclude_idle_bursts_and_include_terminal_failure() {
        let e = [
            (1, 0, 1),
            (5, 1, 0),
            (100, 2, 1),
            (110, 3, 1),
            (120, 4, 0),
            (150, 5, -1),
        ];
        let g = pending_gaps(&e);
        assert_eq!(g.busy_ns, 54);
        assert_eq!(
            g.longest
                .iter()
                .map(|g| (g.start, g.end, g.ended_by))
                .collect::<Vec<_>>(),
            vec![
                (1, 5, "acked"),
                (100, 120, "acked"),
                (120, 150, "terminal-without-ack")
            ]
        );
    }
    #[test]
    fn same_time_events_keep_ordinal_order_and_quantiles_use_exact_ranks() {
        let mut events = vec![(5, 4, 0), (1, 1, 1), (5, 3, 1), (5, 2, 0)];
        events.sort_unstable();
        let g = pending_gaps(&events);
        assert_eq!(g.busy_ns, 4);
        assert_eq!(g.longest.len(), 2);
        assert_eq!(g.longest[1].duration, 0);
        assert_eq!(quantiles(vec![9, 1, 4, 2])["p50"], 2);
        assert_eq!(quantiles(vec![9, 1, 4, 2])["p99"], 9);
    }
    #[test]
    fn phase_clipping_excludes_disjoint_and_touching_gaps() {
        let gaps = [Gap {
            start: 5,
            end: 15,
            duration: 10,
            ended_by: "acked",
            pending_at_end: 0,
        }];
        for (a, b) in [(0, 4), (0, 5), (15, 20), (20, 30)] {
            assert!(phase_summary(&[], &[], &gaps, a, b)["longest_pending_no_ack"].is_null());
        }
        assert_eq!(
            phase_summary(&[], &[], &gaps, 10, 20)["longest_pending_no_ack"],
            json!({"duration":5,"start":10,"end":15})
        );
    }
}
