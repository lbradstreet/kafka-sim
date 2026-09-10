use super::*;
use crate::{Scenario, Size, Variant};
use kr_kafka_producer::{credit::Resource, telemetry::metrics::Metric};
use kr_kafka_sim::{DomainEvent as E, MetricScope, RunReport, TimedControl};
use serde_json::{Map, json};
use std::collections::{BTreeMap, BTreeSet};
const GLOBAL: &[&str] = &[
    "offered_due",
    "offered_actual",
    "accepted",
    "refused",
    "acked",
    "not_written",
    "unknown",
    "client_requests",
    "client_produce_requests",
    "client_retry_requests",
    "client_retry_records",
    "broker_requests",
    "responses",
    "bytes_wire",
    "commit_batches",
    "commit_records",
];
const BROKER: &[&str] = &[
    "client_requests",
    "broker_requests",
    "responses",
    "bytes_wire",
    "disconnects",
    "setup_failures",
    "drops",
    "delayed_hooks",
    "acked",
];
#[derive(Clone, Default)]
struct Row {
    id: u64,
    due: u64,
    offer: u64,
    accept: Option<u64>,
    deliver: Option<u64>,
    outcome: u32,
    reason: String,
    attempts: u32,
    dispatches: u32,
    partition: Option<usize>,
    broker: Option<i32>,
    offset: Option<i64>,
    first_dispatch: Option<u64>,
}
struct Request {
    connection: u64,
    broker: usize,
    correlation: i32,
    at: u64,
    write: Option<u64>,
    responded: bool,
    finished: bool,
}
struct Series {
    values: BTreeMap<String, Vec<u64>>,
}
impl Series {
    fn new(names: &[&str], n: usize) -> Self {
        Self {
            values: names.iter().map(|s| (s.to_string(), vec![0; n])).collect(),
        }
    }
    fn add(&mut self, name: &str, b: usize, value: u64) {
        self.values.get_mut(name).unwrap()[b] += value;
    }
    fn json(&self) -> Map<String, Value> {
        self.values
            .iter()
            .map(|(k, v)| (k.clone(), json!(v)))
            .collect()
    }
    fn sum(&self, name: &str) -> u64 {
        self.values[name].iter().sum()
    }
}
fn hex(id: &[u8; 16]) -> String {
    id.iter().map(|v| format!("{v:02x}")).collect()
}
fn stats(values: &mut [u64]) -> Value {
    values.sort_unstable();
    let q = |ppm: u64| {
        if values.is_empty() {
            None
        } else {
            Some(
                values[((values.len() as u128 * ppm as u128)
                    .div_ceil(1_000_000)
                    .max(1)
                    - 1) as usize],
            )
        }
    };
    json!({"count":values.len(),"p50":q(500_000),"p90":q(900_000),"p99":q(990_000),"p999":q(999_000),"max":values.last(),"mean":if values.is_empty(){None}else{Some(values.iter().map(|v|*v as u128).sum::<u128>() as f64/values.len() as f64)}})
}
fn bucket_quantiles(values: &mut [Vec<u64>], names: &[&str]) -> Value {
    let summaries: Vec<_> = values.iter_mut().map(|v| stats(v)).collect();
    Value::Object(
        names
            .iter()
            .map(|name| {
                (
                    name.to_string(),
                    Value::Array(summaries.iter().map(|s| s[*name].clone()).collect()),
                )
            })
            .collect(),
    )
}
fn marker(
    at: u64,
    kind: &str,
    broker: Option<i32>,
    connection: Option<u64>,
    detail: Value,
) -> Value {
    json!({"kind":kind,"at":at,"broker":broker,"partition":null,"connection":connection.map(|n|n.to_string()),"label":kind.replace('_'," "),"detail":detail})
}
fn record_columns(rows: &BTreeMap<u64, Row>, limit: usize) -> Value {
    let all: Vec<_> = rows.values().collect();
    let count = all.len().min(limit);
    let selected: Vec<_> = (0..count).map(|i| all[i * all.len() / count]).collect();
    let reasons: Vec<_> = rows
        .values()
        .map(|r| r.reason.clone())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect();
    json!({"population_count":all.len(),"count":count,"complete":count==all.len(),"sampling":{"method":"evenly-spaced-rank-in-record-id-order","rank_formula":"floor(i * population_count / count)"},"outcome_names":["acked","not_written","unknown","refused"],"reason_names":reasons,
        "record_id":selected.iter().map(|r|r.id.to_string()).collect::<Vec<_>>(),"due":selected.iter().map(|r|r.due).collect::<Vec<_>>(),"offer":selected.iter().map(|r|r.offer).collect::<Vec<_>>(),"accept":selected.iter().map(|r|r.accept).collect::<Vec<_>>(),"deliver":selected.iter().map(|r|r.deliver).collect::<Vec<_>>(),"outcome":selected.iter().map(|r|r.outcome).collect::<Vec<_>>(),"reason":selected.iter().map(|r|reasons.binary_search(&r.reason).unwrap()).collect::<Vec<_>>(),"producer_attempts":selected.iter().map(|r|if r.accept.is_some(){Some(r.attempts)}else{None}).collect::<Vec<_>>(),"client_dispatches":selected.iter().map(|r|r.dispatches).collect::<Vec<_>>(),"partition":selected.iter().map(|r|r.partition).collect::<Vec<_>>(),"broker":selected.iter().map(|r|r.broker).collect::<Vec<_>>(),"offset":selected.iter().map(|r|r.offset.map(|o|o.to_string())).collect::<Vec<_>>()})
}

pub fn derive(
    s: &Scenario,
    v: &Variant,
    size: Size,
    replay_verified: bool,
    r: &RunReport,
    phases: &[PhaseEvidence],
) -> Result<ExperimentReport, String> {
    let m = &r.manifest;
    let origin = m.start_ns;
    let duration = r
        .checkpoint
        .now_ns
        .checked_sub(origin)
        .ok_or("checkpoint precedes origin")?;
    let nb = m.brokers.len();
    let broker_index: BTreeMap<_, _> = m
        .brokers
        .iter()
        .enumerate()
        .map(|(i, b)| (b.id, i))
        .collect();
    let mut partitions = BTreeMap::new();
    for t in &m.topics {
        for p in 0..t.leaders.len() {
            partitions.insert((t.id, p as i32), 0);
        }
    }
    for e in &r.history.entries {
        if let E::Accepted {
            topic, partition, ..
        }
        | E::Delivery {
            topic, partition, ..
        } = e.event
            && topic != [0; 16]
            && partition >= 0
        {
            partitions.insert((topic, partition), 0);
        }
    }
    for sample in &r.metrics_samples {
        for scope in &sample.scopes {
            if let MetricScope::Partition {
                topic_id,
                partition,
            } = scope.scope
            {
                partitions.insert((topic_id, partition), 0);
            }
        }
    }
    for (i, index) in partitions.values_mut().enumerate() {
        *index = i;
    }
    let np = partitions.len();
    // Keep sparse large topologies inside the same presentation byte envelope.
    // Exact phase checks and record measurements still use unsampled history.
    let max_buckets = 1024.min(131_072 / np.max(1));
    let width = [1, 2, 5, 10, 20, 50, 100, 200, 500]
        .into_iter()
        .map(|n| n * 1_000_000)
        .find(|w| duration / w < max_buckets as u64)
        .ok_or("bucket time bound")?;
    let n = (duration / width + 1) as usize;
    let mut leaders = vec![None; np];
    for (key, &i) in &partitions {
        if let Some(t) = m.topics.iter().find(|t| t.id == key.0) {
            leaders[i] = t.leaders.get(key.1 as usize).copied();
        }
    }
    let mut topic_ids: Vec<_> = m.topics.iter().map(|t| t.id).collect();
    let mut pa = vec![vec![0u64; n]; np];
    let mut pn = pa.clone();
    let mut unrouted_not_written = vec![0u64; n];
    let mut pl = vec![vec![None; n]; np];
    let mut global = Series::new(GLOBAL, n);
    let mut brokers: Vec<_> = (0..nb).map(|_| Series::new(BROKER, n)).collect();
    let mut lat = vec![vec![]; n];
    let mut first = lat.clone();
    let mut acked_lat = vec![];
    let mut all_lat = vec![];
    let mut dispatch_rtt = vec![];
    let mut write_rtt = vec![];
    let mut dr = vec![vec![vec![]; n]; nb];
    let mut wr = dr.clone();
    let mut active = vec![0u64; nb];
    let mut depth = active.clone();
    let mut active_series = vec![vec![0; n]; nb];
    let mut max_depth = active_series.clone();
    let mut end_depth = active_series.clone();
    let mut outstanding = vec![0; n];
    let mut pending = 0u64;
    let mut peak_outstanding = 0u64;
    let capacity: Vec<_> = m
        .producer
        .validate()
        .map_err(|e| e.to_string())?
        .credits
        .into_iter()
        .map(|v| v as u64)
        .collect();
    let mut held = vec![0; capacity.len()];
    let mut held_max = vec![vec![0; n]; capacity.len()];
    let mut held_last = held_max.clone();
    let mut cursor = 0usize;
    let mut rows = BTreeMap::<u64, Row>::new();
    let mut tokens = BTreeMap::new();
    let mut connections = BTreeMap::<u64, usize>::new();
    let mut requests = BTreeMap::<u64, Request>::new();
    let mut pairs = BTreeMap::new();
    let mut operations = BTreeMap::new();
    let mut batches = BTreeSet::new();
    let mut batch_raw = 0u64;
    let mut batch_wire = 0u64;
    let mut dispatch_records = vec![0u64; n];
    let mut produce_count = vec![0u64; n];
    let mut markers = vec![];
    let mut markers_truncated = 0;
    let mut close_at = None;
    let mut planned = 0;
    let mut cancelled = 0;
    let reason_names = [
        "None",
        "Deadline",
        "Cancelled",
        "TopicDeleted",
        "TopicResolution",
        "PartitionFailed",
        "CompressedTooLarge",
        "InvalidRecord",
        "BrokerRejected",
        "ProducerFenced",
        "ProtocolViolation",
        "Transport",
        "RuntimeFailed",
        "SequenceUnresolved",
        "Closed",
        "ResourceExhausted",
        "Authentication",
    ];
    for entry in &r.history.entries {
        let at = entry
            .now_ns
            .checked_sub(origin)
            .filter(|at| *at <= duration)
            .ok_or("event outside report time")?;
        let b = (at / width) as usize;
        while cursor < b {
            for j in 0..nb {
                end_depth[j][cursor] = depth[j];
                active_series[j][cursor] = active[j];
                max_depth[j][cursor + 1] = depth[j];
            }
            for j in 0..held.len() {
                held_last[j][cursor] = held[j];
                held_max[j][cursor + 1] = held[j];
            }
            for p in 0..np {
                pl[p][cursor] = leaders[p];
            }
            outstanding[cursor] = pending;
            cursor += 1;
        }
        let mut mark = None;
        match &entry.event {
            E::Offered {
                record_id, due_ns, ..
            } => {
                let due = due_ns
                    .checked_sub(origin)
                    .filter(|t| *t <= duration)
                    .ok_or("offer due time")?;
                if rows
                    .insert(
                        *record_id,
                        Row {
                            id: *record_id,
                            due,
                            offer: at,
                            outcome: 3,
                            reason: "Unresolved".into(),
                            ..Row::default()
                        },
                    )
                    .is_some()
                {
                    return Err("duplicate offered ID".into());
                }
                global.add("offered_due", (due / width) as usize, 1);
                global.add("offered_actual", b, 1);
            }
            E::Accepted {
                record_id,
                token,
                topic,
                partition,
                ..
            } => {
                let row = rows.get_mut(record_id).ok_or("accept without offer")?;
                row.accept = Some(at);
                row.partition = partitions.get(&(*topic, *partition)).copied();
                tokens.insert(*token, *record_id);
                pending += 1;
                peak_outstanding = peak_outstanding.max(pending);
                global.add("accepted", b, 1);
            }
            E::Refused {
                record_id, error, ..
            } => {
                let row = rows.get_mut(record_id).ok_or("refusal without offer")?;
                row.reason = error.clone();
                global.add("refused", b, 1);
            }
            E::Delivery {
                record_id,
                outcome,
                reason,
                attempts,
                offset,
                topic,
                partition,
                ..
            } => {
                let row = rows.get_mut(record_id).ok_or("delivery without offer")?;
                let latency = at
                    .checked_sub(row.accept.ok_or("delivery without acceptance")?)
                    .ok_or("negative delivery latency")?;
                row.deliver = Some(at);
                row.outcome = *outcome;
                row.reason = reason_names
                    .get(*reason as usize)
                    .ok_or("delivery reason code")?
                    .to_string();
                row.attempts = *attempts;
                row.offset = *offset;
                row.partition = partitions.get(&(*topic, *partition)).copied();
                pending = pending.checked_sub(1).ok_or("outstanding underflow")?;
                global.add(
                    ["acked", "not_written", "unknown"]
                        .get(*outcome as usize)
                        .ok_or("delivery outcome code")?,
                    b,
                    1,
                );
                all_lat.push(latency);
                if *outcome == 0 {
                    acked_lat.push(latency);
                    lat[b].push(latency);
                    pa[row.partition.ok_or("ack without immutable route")?][b] += 1;
                    if let Some(broker) = row.broker {
                        brokers[broker_index[&broker]].add("acked", b, 1);
                    }
                } else if *outcome == 1 {
                    if let Some(partition) = row.partition {
                        pn[partition][b] += 1;
                    } else {
                        unrouted_not_written[b] += 1;
                    }
                }
            }
            E::ConnectionOpened {
                connection, broker, ..
            } => {
                let j = *broker_index
                    .get(broker)
                    .ok_or("unknown connection broker")?;
                if connections.insert(*connection, j).is_some() {
                    return Err("duplicate connection lifetime".into());
                }
                active[j] += 1;
            }
            E::ConnectionClosed { connection, .. } => {
                let j = *connections
                    .get(connection)
                    .ok_or("unknown closed connection")?;
                active[j] = active[j]
                    .checked_sub(1)
                    .ok_or("connection count underflow")?;
                mark = Some(marker(
                    at,
                    "connection_closed",
                    Some(m.brokers[j].id),
                    Some(*connection),
                    json!({}),
                ));
            }
            E::ClientRequestDispatched {
                connection,
                correlation,
                api,
                request_id,
                tokens: ids,
                batches: bs,
                ..
            } => {
                let j = *connections
                    .get(connection)
                    .ok_or("dispatch without connection")?;
                if requests
                    .insert(
                        *request_id,
                        Request {
                            connection: *connection,
                            broker: j,
                            correlation: *correlation,
                            at,
                            write: None,
                            responded: false,
                            finished: false,
                        },
                    )
                    .is_some()
                    || pairs
                        .insert((*connection, *correlation), *request_id)
                        .is_some()
                {
                    return Err("duplicate request identity".into());
                }
                global.add("client_requests", b, 1);
                brokers[j].add("client_requests", b, 1);
                depth[j] += 1;
                max_depth[j][b] = max_depth[j][b].max(depth[j]);
                if *api == 0 {
                    global.add("client_produce_requests", b, 1);
                    produce_count[b] += 1;
                    dispatch_records[b] += ids.len() as u64;
                    let mut retry = 0;
                    for token in ids {
                        let id = tokens.get(token).ok_or("dispatch without accepted token")?;
                        let row = rows.get_mut(id).unwrap();
                        retry += u64::from(row.dispatches > 0);
                        row.dispatches += 1;
                        row.broker = Some(m.brokers[j].id);
                        if row.first_dispatch.is_none() {
                            row.first_dispatch = Some(at);
                            first[b].push(at - row.accept.unwrap());
                        }
                    }
                    if retry > 0 {
                        global.add("client_retry_requests", b, 1);
                        global.add("client_retry_records", b, retry);
                    }
                    for batch in bs {
                        if batches.insert(batch.batch_id) {
                            batch_raw += batch.raw_bytes;
                            batch_wire += batch.wire_bytes;
                        }
                    }
                }
            }
            E::ClientRequestWriteCompleted {
                request_id,
                connection,
                correlation,
            } => {
                let req = requests
                    .get_mut(request_id)
                    .ok_or("write completion without dispatch")?;
                if req.connection != *connection
                    || req.correlation != *correlation
                    || req.write.replace(at).is_some()
                {
                    return Err("invalid request write completion".into());
                }
            }
            E::ClientRequestFinished {
                request_id,
                connection,
                correlation,
                ..
            } => {
                let req = requests
                    .get_mut(request_id)
                    .ok_or("finish without dispatch")?;
                if req.finished || req.connection != *connection || req.correlation != *correlation
                {
                    return Err("invalid request finish".into());
                }
                req.finished = true;
                depth[req.broker] = depth[req.broker]
                    .checked_sub(1)
                    .ok_or("request depth underflow")?;
            }
            E::BrokerRequest { connection, .. } => {
                let j = *connections
                    .get(connection)
                    .ok_or("broker request without connection")?;
                global.add("broker_requests", b, 1);
                brokers[j].add("broker_requests", b, 1);
            }
            E::ResponseRead {
                connection,
                correlation,
            } => {
                let id = pairs
                    .get(&(*connection, *correlation))
                    .ok_or("response without dispatch")?;
                let req = requests.get_mut(id).unwrap();
                if req.responded {
                    return Err("duplicate response".into());
                }
                req.responded = true;
                let latency = at.checked_sub(req.at).ok_or("negative dispatch RTT")?;
                dispatch_rtt.push(latency);
                dr[req.broker][b].push(latency);
                if let Some(written) = req.write.filter(|w| *w <= at) {
                    let latency = at - written;
                    write_rtt.push(latency);
                    wr[req.broker][b].push(latency);
                }
                global.add("responses", b, 1);
                brokers[req.broker].add("responses", b, 1);
            }
            E::WriteAdmitted {
                operation,
                connection,
                ..
            } => {
                operations.insert(*operation, *connection);
            }
            E::WriteCompleted {
                operation, bytes, ..
            } => {
                let connection = operations
                    .remove(operation)
                    .ok_or("write operation completion without admission")?;
                let j = *connections
                    .get(&connection)
                    .ok_or("write without connection")?;
                global.add("bytes_wire", b, *bytes as u64);
                brokers[j].add("bytes_wire", b, *bytes as u64);
            }
            E::BrokerCommit {
                batches, records, ..
            } => {
                global.add("commit_batches", b, u64::from(*batches));
                global.add("commit_records", b, u64::from(*records));
            }
            E::Credits { held: next, .. } => {
                if next.len() != held.len() {
                    return Err("credit dimension".into());
                }
                held.clone_from(next);
                for j in 0..held.len() {
                    held_max[j][b] = held_max[j][b].max(held[j]);
                }
            }
            E::OffersStopped {
                planned: p,
                cancelled: c,
                ..
            } => {
                planned = *p;
                cancelled = *c;
            }
            E::ScheduledControl { action, .. } => {
                let kind = match action {
                    TimedControl::MoveLeader {
                        topic,
                        partition,
                        broker,
                    } => {
                        let id = topic_ids[*topic as usize];
                        if let Some(&i) = partitions.get(&(id, *partition)) {
                            leaders[i] = Some(*broker);
                        }
                        "leader_move"
                    }
                    TimedControl::RecreateTopic { topic, new_id } => {
                        topic_ids[*topic as usize] = *new_id;
                        for (p, broker) in m.topics[*topic as usize].leaders.iter().enumerate() {
                            if let Some(&i) = partitions.get(&(*new_id, p as i32)) {
                                leaders[i] = Some(*broker);
                            }
                        }
                        "topic_recreate"
                    }
                    TimedControl::AddPartitions {
                        topic,
                        additional_leaders,
                    } => {
                        let id = topic_ids[*topic as usize];
                        let current = leaders
                            .iter()
                            .enumerate()
                            .filter(|(i, l)| {
                                l.is_some()
                                    && partitions
                                        .iter()
                                        .any(|((t, _), index)| *index == *i && *t == id)
                            })
                            .count();
                        for (p, broker) in additional_leaders.iter().enumerate() {
                            if let Some(&i) = partitions.get(&(id, (current + p) as i32)) {
                                leaders[i] = Some(*broker);
                            }
                        }
                        "add_partitions"
                    }
                    TimedControl::DeleteTopic { .. } => "topic_delete",
                    TimedControl::Close { .. } => {
                        close_at = Some(at);
                        "close"
                    }
                    TimedControl::Flush => "flush",
                    _ => "workload_step",
                };
                mark = Some(marker(at, kind, None, None, json!(action)));
            }
            E::BrokerFrameAbandoned {
                connection,
                correlation,
                broker,
                window,
            } => {
                mark = Some(marker(
                    at,
                    "workload_step",
                    Some(*broker),
                    Some(*connection),
                    json!({"action":"broker frame abandoned by crash","correlation":correlation,"window":window}),
                ));
            }
            E::FaultDecision(d) => {
                let j = *broker_index.get(&d.hook.broker).ok_or("fault broker")?;
                use kr_kafka_sim::faults::Outcome;
                let kind = match d.effects.outcome {
                    Outcome::Disconnect => Some("disconnect"),
                    Outcome::Drop => Some("drop"),
                    Outcome::SetupFailure => Some("setup_failure"),
                    _ => None,
                };
                if d.effects.delay_ns > 0 {
                    brokers[j].add("delayed_hooks", b, 1);
                }
                if let Some(kind) = kind {
                    brokers[j].add(
                        match kind {
                            "disconnect" => "disconnects",
                            "drop" => "drops",
                            _ => "setup_failures",
                        },
                        b,
                        1,
                    );
                    mark = Some(marker(
                        at,
                        kind,
                        Some(d.hook.broker),
                        Some(d.hook.connection),
                        json!({"api":d.hook.api,"phase":d.hook.phase,"effects":d.effects}),
                    ));
                }
            }
            E::FlushDone { token } => {
                mark = Some(marker(
                    at,
                    "flush_done",
                    None,
                    None,
                    json!({"token":token.to_string()}),
                ))
            }
            E::TopicReady { handle, id } => {
                mark = Some(marker(
                    at,
                    "topic_ready",
                    None,
                    None,
                    json!({"handle":handle,"id":hex(id)}),
                ))
            }
            E::TopicFailed { handle, code } => {
                mark = Some(marker(
                    at,
                    "topic_failed",
                    None,
                    None,
                    json!({"handle":handle,"code":code}),
                ))
            }
            E::Fatal { code } => mark = Some(marker(at, "fatal", None, None, json!({"code":code}))),
            E::Closed { unknown } => {
                mark = Some(marker(at, "closed", None, None, json!({"unknown":unknown})))
            }
            _ => {}
        }
        if let Some(mark) = mark {
            if markers.len() < 4096 {
                markers.push(mark)
            } else {
                markers_truncated += 1;
            }
        }
    }
    if pending != 0
        || depth.iter().any(|n| *n != 0)
        || active.iter().any(|n| *n != 0)
        || requests.values().any(|r| !r.finished)
    {
        return Err("report has live obligations".into());
    }
    for b in cursor..n {
        for j in 0..nb {
            end_depth[j][b] = depth[j];
            active_series[j][b] = active[j];
        }
        for j in 0..held.len() {
            held_last[j][b] = held[j];
        }
        for p in 0..np {
            pl[p][b] = leaders[p];
        }
        outstanding[b] = pending;
    }
    let mut gj = global.json();
    gj.insert(
        "latency".into(),
        bucket_quantiles(&mut lat, &["p50", "p90", "p99", "max"]),
    );
    gj.insert(
        "first_dispatch".into(),
        bucket_quantiles(&mut first, &["p50", "p99"]),
    );
    gj.insert("outstanding".into(), json!(outstanding));
    gj.insert(
        "records_per_request_mean".into(),
        json!(
            dispatch_records
                .iter()
                .zip(&produce_count)
                .map(|(records, count)| if *count == 0 {
                    None
                } else {
                    Some(*records as f64 / *count as f64)
                })
                .collect::<Vec<_>>()
        ),
    );
    gj.insert("credits".into(),json!({"pools":Resource::ALL.iter().map(|r|format!("{r:?}")).collect::<Vec<_>>(),"capacity":capacity,"held_observed_max":held_max,"held_last_observed":held_last}));
    let bj: Vec<_> = brokers
        .iter()
        .enumerate()
        .map(|(j, series)| {
            let mut value = series.json();
            value.insert("broker".into(), json!(m.brokers[j].id));
            value.insert("client_inflight_max".into(), json!(max_depth[j]));
            value.insert("client_inflight_end".into(), json!(end_depth[j]));
            value.insert("active_connections".into(), json!(active_series[j]));
            value.insert(
                "dispatch_rtt".into(),
                bucket_quantiles(&mut dr[j], &["p50", "p99"]),
            );
            value.insert(
                "full_write_rtt".into(),
                bucket_quantiles(&mut wr[j], &["p50", "p99"]),
            );
            Value::Object(value)
        })
        .collect();
    let latency_acked = stats(&mut acked_lat);
    let latency_all = stats(&mut all_lat);
    let dispatch_stats = stats(&mut dispatch_rtt);
    let write_stats = stats(&mut write_rtt);
    let mut attempts = BTreeMap::<u32, u64>::new();
    let mut outcomes = BTreeMap::<(u32, String), u64>::new();
    for row in rows.values() {
        if row.accept.is_some() {
            *attempts.entry(row.attempts).or_default() += 1;
        }
        *outcomes
            .entry((row.outcome, row.reason.clone()))
            .or_default() += 1;
    }
    let points: Vec<_> = (0..acked_lat.len().min(1000))
        .map(|i| {
            let rank = ((i + 1) * acked_lat.len()).div_ceil(acked_lat.len().min(1000));
            json!({"latency":acked_lat[rank-1],"cumulative_count":rank})
        })
        .collect();
    let bands = environment_bands(r, duration);
    let recovery: Vec<_> = bands
        .iter()
        .enumerate()
        .map(|(i, band)| {
            let end = band["end"].as_u64().unwrap();
            let ack = rows
                .values()
                .filter(|row| {
                    row.outcome == 0
                        && band["broker"]
                            .as_i64()
                            .is_none_or(|b| row.broker == Some(b as i32))
                })
                .filter_map(|row| row.deliver.filter(|at| *at >= end))
                .min();
            json!({"band":i,"first_ack_after_end":ack,"delay":ack.map(|at|at-end)})
        })
        .collect();
    let mut ack_times: Vec<_> = rows
        .values()
        .filter(|r| r.outcome == 0)
        .filter_map(|r| r.deliver)
        .collect();
    ack_times.sort_unstable();
    let max_stall = ack_times.windows(2).map(|w| w[1] - w[0]).max();
    let e = m
        .experiment
        .as_ref()
        .ok_or("report needs experiment workload")?;
    let mut report = ExperimentReport {
        schema: SCHEMA.into(),
        meta: json!({"scenario":{"id":s.id,"title":s.title,"category":s.category,"description":s.description,"what_to_look_for":s.what_to_look_for},"variant":{"name":v.name,"deltas":v.params,"summary":v.summary},"seed":m.seed.to_string(),"origin_ns":origin.to_string(),"end_ns":r.checkpoint.now_ns.to_string(),"duration":duration,"size":size,"workload":{"active_intervals":e.loads.iter().map(|l|json!({"start":l.shape.start_ns(),"end":l.shape.end_ns(),"shape":l.shape,"first_id":l.template.first_id.to_string(),"topic":l.template.topic,"partitioning":l.template.partitioning,"value_bytes":l.template.value_bytes,"key_bytes":l.template.key_bytes,"lane":l.template.lane,"native":l.template.native,"value_pattern":format!("{:?}",l.template.value_pattern)})).collect::<Vec<_>>(),"test_adjustments":super::adjustments::describe(s,v,size,m),"required_phases":phases.iter().map(|p|&p.phase).collect::<Vec<_>>(),"planned_offers":planned,"cancelled_unoffered":cancelled},"replay_verified":replay_verified,"source":m.versions,"generated_by":"kr-kafka-experiments","measurements":{"latency":"event consumption minus accepted time, nanoseconds","bytes_wire":"successful local write-completion bytes, including retries and control traffic","dispatch_rtt":"complete response visibility minus first client submission","full_write_rtt":"response visibility minus full write confirmation, absent for early responses","producer_attempts":"producer batch ledger WriteAdmitted count","client_dispatches":"observed Produce request attempts containing this token","credits":"observed maxima and last observation, not continuous occupancy"}}),
        topology: json!({"brokers":m.brokers,"topics":m.topics.iter().enumerate().map(|(i,t)|json!({"index":i,"name":t.name,"id_hex":hex(&t.id),"initial_leaders":t.leaders})).collect::<Vec<_>>(),"partitions":partitions.keys().map(|(id,p)|json!({"topic_id":hex(id),"partition":p})).collect::<Vec<_>>()}),
        config: json!({"batch_target_mode":format!("{:?}",m.producer.batch_target_mode),"descriptor_admission_policy":format!("{:?}",m.producer.descriptor_admission_policy),"linger_max":m.producer.linger_max.as_nanos(),"batch_target_bytes":m.producer.batch_target_bytes,"batch_hard_bytes":m.producer.batch_hard_bytes,"max_in_flight_per_connection":m.producer.max_in_flight_per_connection,"lanes":m.producer.lanes,"connection_wire_window_bytes":m.producer.connection_wire_window_bytes,"request_timeout":m.producer.request_timeout.as_nanos(),"delivery_timeout":m.producer.delivery_timeout.as_nanos(),"retry_backoff":{"min":m.producer.retry_backoff_min.as_nanos(),"max":m.producer.retry_backoff_max.as_nanos()},"metadata_max_age":m.producer.metadata_max_age.as_nanos(),"compression":format!("{:?}",m.producer.compression),"driver":{"transport_model":"bounded-directional-propagation/v1","crash_on_isolation":m.faults.crash_on_isolation,"propagation_links":m.faults.links,"local_completion_latency":m.driver.link_latency_ns,"jitter":m.driver.jitter_ns,"service_delay":m.driver.service_delay_ns,"chunk_bytes":m.driver.chunk_bytes,"pipe_bytes":m.driver.pipe_bytes,"encode_bytes_per_poll":m.driver.encode_bytes,"encode_cost_per_poll":m.driver.encode_cost_ns}}),
        environment: json!({"bands":bands,"markers":markers,"markers_truncated":markers_truncated}),
        buckets: json!({"bucket_ns":width,"count":n,"global":gj,"brokers":bj}),
        partitions: json!({"acked":pa,"not_written":pn,"unrouted_not_written":unrouted_not_written,"leader":pl}),
        records: record_columns(&rows, 65_536),
        summary: json!({"records":{"offered":global.sum("offered_actual"),"accepted":global.sum("accepted"),"refused":global.sum("refused"),"acked":global.sum("acked"),"not_written":global.sum("not_written"),"unknown":global.sum("unknown")},"latency_acked":latency_acked,"latency_all_deliveries":latency_all,"dispatch_rtt":dispatch_stats,"full_write_rtt":write_stats,"client_requests":global.sum("client_requests"),"broker_requests":global.sum("broker_requests"),"responses":global.sum("responses"),"client_retry_requests":global.sum("client_retry_requests"),"client_retry_records":global.sum("client_retry_records"),"bytes_wire":global.sum("bytes_wire"),"duplicates":r.coverage.duplicate_sequences,"throttles":r.coverage.throttles,"peak_refusals_per_bucket":global.values["refused"].iter().max(),"peak_outstanding":peak_outstanding,"first_dispatch_batch_bytes":{"count":batches.len(),"sum":batch_wire,"raw_sum":batch_raw,"mean":if batches.is_empty(){None}else{Some(batch_wire as f64/batches.len() as f64)}},"peak_inflight":m.brokers.iter().enumerate().map(|(j,b)|json!({"broker":b.id,"value":max_depth[j].iter().max()})).collect::<Vec<_>>(),"max_stall":max_stall,"recovery":recovery,"close_at":close_at,"coverage":r.coverage,"fault_stats":r.fault_stats}),
        phase_evidence: phases.to_vec(),
        distributions: json!({"latency_ecdf":{"population_count":acked_lat.len(),"points":points,"reduced":acked_lat.len()>1000},"attempts_histogram":attempts.iter().map(|(a,c)|json!({"producer_attempts":a,"count":c})).collect::<Vec<_>>(),"outcomes_by_reason":outcomes.iter().map(|((o,reason),count)|json!({"outcome":o,"reason":reason,"count":count})).collect::<Vec<_>>()}),
        hdr: hdr(r, origin)?,
    };
    let mut limit = 65_536;
    while serde_json::to_vec(&report)
        .map_err(|e| e.to_string())?
        .len()
        > MAX_REPORT_BYTES
    {
        limit /= 2;
        if limit == 0 {
            return Err("aggregate report exceeds byte cap".into());
        }
        report.records = record_columns(&rows, limit);
    }
    Ok(report)
}
fn environment_bands(r: &RunReport, duration: u64) -> Vec<Value> {
    let m = &r.manifest;
    let mut bands = vec![];
    let mut add = |kind: &str,
                   broker: Option<i32>,
                   start: u64,
                   end: u64,
                   direction: Value,
                   mode: Value,
                   value: Value| {
        bands.push(json!({"kind":kind,"broker":broker,"start":start,"end":end,"realized_start":if start<=duration{Some(start)}else{None},"realized_end":if end<=duration{Some(end)}else{None},"direction":direction,"mode":mode,"value":value,"label":kind.replace('_'," ")}));
    };
    for w in &m.faults.isolations {
        add(
            "isolation",
            Some(w.broker),
            w.start_ns,
            w.end_ns,
            Value::Null,
            Value::Null,
            Value::Null,
        );
    }
    for w in &m.faults.link_outages {
        add(
            "link_outage",
            Some(w.broker),
            w.start_ns,
            w.end_ns,
            json!(w.direction),
            json!(w.mode),
            Value::Null,
        );
    }
    for w in &m.faults.environment {
        let kind = if w.effects.reject_error.is_some() {
            "reject"
        } else if w.effects.throttle_ms > 0 {
            "throttle"
        } else if w.effects.delay_ns > 0 || w.ramp.is_some() {
            "service_delay"
        } else {
            "loss"
        };
        add(
            kind,
            w.broker,
            w.start_ns,
            w.end_ns,
            Value::Null,
            Value::Null,
            json!({"phase":w.phase,"api":w.api,"probability_ppm":w.probability_ppm,"effects":w.effects,"ramp":w.ramp}),
        );
    }
    if let Some(e) = &m.experiment {
        for w in &e.polling_pauses {
            add(
                "stop_polling",
                None,
                w.start_ns,
                w.end_ns,
                Value::Null,
                Value::Null,
                Value::Null,
            );
        }
    }
    bands.sort_by_key(|b| (b["start"].as_u64().unwrap(), b["end"].as_u64().unwrap()));
    bands
}
fn hdr(r: &RunReport, origin: u64) -> Result<Value, String> {
    if r.metrics_samples.is_empty() {
        return Ok(Value::Null);
    }
    let mut scopes = vec![];
    for s in &r.metrics_samples {
        for scope in &s.scopes {
            let v = match scope.scope {
                MetricScope::Global => json!({"kind":"global"}),
                MetricScope::Broker(b) => json!({"kind":"broker","broker":b}),
                MetricScope::Partition {
                    topic_id,
                    partition,
                } => json!({"kind":"partition","topic_id":hex(&topic_id),"partition":partition}),
            };
            if !scopes.contains(&v) {
                scopes.push(v);
            }
        }
    }
    let offset = |n: u64| {
        n.checked_sub(origin)
            .ok_or_else(|| "HDR time before origin".to_string())
    };
    let mut series = vec![];
    for (i, scope) in scopes.iter().enumerate() {
        for metric in Metric::ALL {
            let samples: Vec<_> = r
                .metrics_samples
                .iter()
                .map(|s| {
                    s.scopes
                        .iter()
                        .find(|candidate| match candidate.scope {
                            MetricScope::Global => scope["kind"] == "global",
                            MetricScope::Broker(b) => {
                                scope["kind"] == "broker" && scope["broker"] == b
                            }
                            MetricScope::Partition {
                                topic_id,
                                partition,
                            } => {
                                scope["kind"] == "partition"
                                    && scope["topic_id"] == hex(&topic_id)
                                    && scope["partition"] == partition
                            }
                        })
                        .and_then(|s| s.distributions.iter().find(|d| d.metric == metric as u8))
                })
                .collect();
            series.push(json!({"scope":i,"metric":metric as u8,"count":samples.iter().map(|d|d.map_or(0,|d|d.count)).collect::<Vec<_>>(),"p50_range":samples.iter().map(|d|d.and_then(|d|d.p50)).collect::<Vec<_>>(),"p90_range":samples.iter().map(|d|d.and_then(|d|d.p90)).collect::<Vec<_>>(),"p99_range":samples.iter().map(|d|d.and_then(|d|d.p99)).collect::<Vec<_>>(),"p999_range":samples.iter().map(|d|d.and_then(|d|d.p999)).collect::<Vec<_>>(),"exact_max":samples.iter().map(|d|d.and_then(|d|d.exact_max)).collect::<Vec<_>>(),"out_of_range":samples.iter().map(|d|d.map_or(0,|d|d.out_of_range)).collect::<Vec<_>>(),"count_overflow":samples.iter().map(|d|d.map_or(0,|d|d.count_overflow)).collect::<Vec<_>>(),"diagnostic_overflow":samples.iter().map(|d|d.is_some_and(|d|d.diagnostic_overflow)).collect::<Vec<_>>(),"significant_digits":samples.iter().flatten().next().map(|d|d.significant_digits),"highest_trackable":samples.iter().flatten().next().map(|d|d.highest_trackable)}));
        }
    }
    Ok(
        json!({"metric_names":Metric::ALL.iter().map(|m|format!("{m:?}")).collect::<Vec<_>>(),"units":Metric::ALL.iter().map(|m|format!("{:?}",m.unit())).collect::<Vec<_>>(),"scopes":scopes,"intervals":{"count":r.metrics_samples.len(),"epoch":r.metrics_samples.iter().map(|s|s.epoch.to_string()).collect::<Vec<_>>(),"requested":r.metrics_samples.iter().map(|s|s.requested_ns.map(offset).transpose()).collect::<Result<Vec<_>,_>>()?,"taken":r.metrics_samples.iter().map(|s|offset(s.taken_ns)).collect::<Result<Vec<_>,_>>()?,"start":r.metrics_samples.iter().map(|s|s.start_ns.map(offset).transpose()).collect::<Result<Vec<_>,_>>()?,"end":r.metrics_samples.iter().map(|s|s.end_ns.map(offset).transpose()).collect::<Result<Vec<_>,_>>()?},"series":series,"config":{"significant_digits":r.manifest.producer.metrics.significant_digits,"highest_duration_nanos":r.manifest.producer.metrics.highest_duration_nanos,"highest_bytes":r.manifest.producer.metrics.highest_bytes,"highest_count":r.manifest.producer.metrics.highest_count},"diagnostics":r.metrics_samples.iter().map(|s|json!({"omitted_scope_samples":s.omitted_scope_samples,"scope_capacity_rejections":s.scope_capacity_rejections,"invalid_scope_samples":s.invalid_scope_samples,"invalid_time_samples":s.invalid_time_samples,"missing_time_samples":s.missing_time_samples,"invalid_depth_samples":s.invalid_depth_samples,"diagnostic_overflow":s.diagnostic_overflow})).collect::<Vec<_>>(),"missed_requests":r.missed_metrics_requests.iter().map(|m|Ok(json!({"scheduled":offset(m.scheduled_ns)?,"attempted":offset(m.attempted_ns)?,"reason":m.reason}))).collect::<Result<Vec<_>,String>>()?}),
    )
}
