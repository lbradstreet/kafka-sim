use super::*;
use std::collections::BTreeSet;
const SAFE: u64 = 9_007_199_254_740_991;
fn require(ok: bool, why: &str) -> Result<(), String> {
    if ok { Ok(()) } else { Err(why.into()) }
}
fn number(v: &Value) -> Result<u64, String> {
    v.as_u64()
        .filter(|n| *n <= SAFE)
        .ok_or_else(|| "expected bounded unsigned number".into())
}
fn array(v: &Value, max: usize) -> Result<&Vec<Value>, String> {
    v.as_array()
        .filter(|a| a.len() <= max)
        .ok_or_else(|| "array type/capacity".into())
}
fn text(v: &Value) -> Result<&str, String> {
    v.as_str()
        .filter(|s| s.len() <= 4096)
        .ok_or_else(|| "string type/capacity".into())
}
fn decimal(v: &Value) -> Result<u64, String> {
    let s = text(v)?;
    let n = s.parse::<u64>().map_err(|_| "u64 decimal")?;
    require(s == n.to_string(), "noncanonical u64")?;
    Ok(n)
}
fn signed(v: &Value) -> Result<i64, String> {
    let s = text(v)?;
    let n = s.parse::<i64>().map_err(|_| "i64 decimal")?;
    require(s == n.to_string(), "noncanonical i64")?;
    Ok(n)
}
fn boolean(v: &Value) -> Result<bool, String> {
    v.as_bool().ok_or_else(|| "expected boolean".into())
}
fn time(v: &Value, duration: u64) -> Result<u64, String> {
    let n = number(v)?;
    require(n <= duration, "time outside duration")?;
    Ok(n)
}
fn nullable_time(v: &Value, duration: u64) -> Result<Option<u64>, String> {
    if v.is_null() {
        Ok(None)
    } else {
        time(v, duration).map(Some)
    }
}
fn uuid(v: &Value) -> Result<&str, String> {
    let s = text(v)?;
    require(
        s.len() == 32
            && s.bytes()
                .all(|c| c.is_ascii_digit() || (b'a'..=b'f').contains(&c)),
        "UUID hex",
    )?;
    Ok(s)
}
fn series(v: &Value, n: usize) -> Result<&Vec<Value>, String> {
    let a = array(v, n)?;
    require(a.len() == n, "series length")?;
    Ok(a)
}
fn sum(v: &Value, n: usize) -> Result<u64, String> {
    series(v, n)?.iter().try_fold(0u64, |sum, v| {
        sum.checked_add(number(v)?)
            .filter(|n| *n <= SAFE)
            .ok_or_else(|| "series sum overflow".into())
    })
}
fn quantiles(v: &Value, count: Option<u64>, maximum: u64) -> Result<(), String> {
    let object = v.as_object().ok_or("quantile object")?;
    let mut previous = None;
    for name in ["p50", "p90", "p99", "p999", "max"] {
        if let Some(value) = object.get(name) {
            let n = nullable_time(value, maximum)?;
            if let Some(count) = count {
                require(n.is_some() == (count > 0), "empty quantile semantics")?;
            }
            if let (Some(p), Some(n)) = (previous, n) {
                require(p <= n, "quantile ordering")?;
            }
            previous = n;
        }
    }
    if let Some(mean) = object.get("mean") {
        require(
            mean.is_null()
                || mean
                    .as_f64()
                    .is_some_and(|m| m.is_finite() && m >= 0.0 && m <= maximum as f64),
            "mean range",
        )?;
        if let Some(c) = count {
            require(mean.is_null() == (c == 0), "empty mean")?;
        }
    }
    Ok(())
}
fn quantile_columns(v: &Value, n: usize, duration: u64, names: &[&str]) -> Result<(), String> {
    for name in names {
        series(&v[*name], n)?;
    }
    for i in 0..n {
        let q = Value::Object(
            names
                .iter()
                .map(|name| (name.to_string(), v[*name][i].clone()))
                .collect(),
        );
        quantiles(&q, None, duration)?;
        let empty = v[names[0]][i].is_null();
        for name in names {
            require(v[*name][i].is_null() == empty, "quantile columns emptiness")?;
        }
    }
    Ok(())
}
fn safe_tree(v: &Value, depth: usize, work: &mut usize) -> Result<(), String> {
    *work += 1;
    require(
        depth <= 32 && *work <= 8_000_000,
        "aggregate shape capacity",
    )?;
    match v {
        Value::String(s) => require(s.len() <= 4096, "string capacity"),
        Value::Array(a) => {
            for v in a {
                safe_tree(v, depth + 1, work)?;
            }
            Ok(())
        }
        Value::Object(o) => {
            for (k, v) in o {
                require(k.len() <= 128, "key capacity")?;
                safe_tree(v, depth + 1, work)?;
            }
            Ok(())
        }
        Value::Number(n) => require(
            n.as_f64()
                .is_some_and(|n| n.is_finite() && n.abs() <= SAFE as f64),
            "unsafe number",
        ),
        _ => Ok(()),
    }
}
pub(super) fn report(r: &ExperimentReport) -> Result<(), String> {
    require(r.schema == SCHEMA, "run schema")?;
    safe_tree(
        &serde_json::to_value(r).map_err(|e| e.to_string())?,
        0,
        &mut 0,
    )?;
    let meta = &r.meta;
    let origin = decimal(&meta["origin_ns"])?;
    let end = decimal(&meta["end_ns"])?;
    let duration = time(&meta["duration"], 300_000_000_000)?;
    require(
        end.checked_sub(origin) == Some(duration),
        "absolute time difference",
    )?;
    decimal(&meta["seed"])?;
    boolean(&meta["replay_verified"])?;
    require(
        ["test", "full"].contains(&text(&meta["size"])?),
        "fixture size",
    )?;
    let id = text(&meta["scenario"]["id"])?;
    require(
        !id.is_empty()
            && id.len() <= 128
            && id
                .bytes()
                .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == b'.' || c == b'-'),
        "scenario ID",
    )?;
    for name in ["title", "description", "what_to_look_for"] {
        text(&meta["scenario"][name])?;
    }
    require(
        ["baseline", "hard", "soft", "topology", "resources"]
            .contains(&text(&meta["scenario"]["category"])?),
        "category",
    )?;
    let variant = text(&meta["variant"]["name"])?;
    require(
        !variant.is_empty()
            && variant.len() <= 128
            && variant
                .bytes()
                .all(|c| c.is_ascii_alphanumeric() || c == b'-' || c == b'_'),
        "variant name",
    )?;
    for name in ["manifest", "history", "scenario", "model", "driver", "rng"] {
        require(number(&meta["source"][name])? > 0, "source version")?;
    }
    for name in [
        "package",
        "source_sha256",
        "kafka_schema_sha256",
        "kafka_revision",
    ] {
        text(&meta["source"][name])?;
    }
    let brokers = array(&r.topology["brokers"], 16)?;
    require(!brokers.is_empty(), "empty topology")?;
    let mut broker_ids = BTreeSet::new();
    for b in brokers {
        let id = number(&b["id"])?;
        require(
            id <= i32::MAX as u64 && broker_ids.insert(id),
            "broker identity",
        )?;
        text(&b["host"])?;
        require((1..=65535).contains(&number(&b["port"])?), "port")?;
    }
    config(&r.config, &broker_ids)?;
    let topics = array(&r.topology["topics"], 64)?;
    let mut topic_ids = BTreeSet::new();
    for (i, t) in topics.iter().enumerate() {
        require(
            number(&t["index"])? == i as u64 && topic_ids.insert(uuid(&t["id_hex"])?),
            "topic identity",
        )?;
        text(&t["name"])?;
        let leaders = array(&t["initial_leaders"], 1024)?;
        require(!leaders.is_empty(), "empty topic")?;
        for leader in leaders {
            require(broker_ids.contains(&number(leader)?), "initial leader")?;
        }
    }
    let partitions = array(&r.topology["partitions"], 1024)?;
    let np = partitions.len();
    let mut partition_ids = BTreeSet::new();
    for p in partitions {
        require(
            partition_ids.insert((uuid(&p["topic_id"])?, number(&p["partition"])?)),
            "partition identity",
        )?;
        require(number(&p["partition"])? < 1024, "partition index")?;
    }
    let n = number(&r.buckets["count"])? as usize;
    require((1..=4096).contains(&n), "bucket count")?;
    let width = number(&r.buckets["bucket_ns"])?;
    require(
        [1, 2, 5, 10, 20, 50, 100, 200, 500]
            .map(|v| v * 1_000_000)
            .contains(&width)
            && duration / width + 1 == n as u64,
        "bucket coverage",
    )?;
    let global = &r.buckets["global"];
    let totals = &r.summary["records"];
    for name in [
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
    ] {
        let total = sum(&global[name], n)?;
        let summary = match name {
            "offered_due" | "offered_actual" => Some(&totals["offered"]),
            "accepted" | "refused" | "acked" | "not_written" | "unknown" => Some(&totals[name]),
            "client_requests"
            | "client_retry_requests"
            | "client_retry_records"
            | "broker_requests"
            | "responses"
            | "bytes_wire" => Some(&r.summary[name]),
            _ => None,
        };
        if let Some(summary) = summary {
            require(total == number(summary)?, "bucket sum differs from summary")?;
        }
    }
    let offered = number(&totals["offered"])?;
    let accepted = number(&totals["accepted"])?;
    let acked = number(&totals["acked"])?;
    let refused = number(&totals["refused"])?;
    require(
        offered <= 1_000_000
            && offered == accepted + refused
            && accepted == acked + number(&totals["not_written"])? + number(&totals["unknown"])?,
        "population accounting",
    )?;
    require(
        number(&meta["workload"]["planned_offers"])?
            == offered + number(&meta["workload"]["cancelled_unoffered"])?,
        "cancelled offer accounting",
    )?;
    quantile_columns(
        &global["latency"],
        n,
        duration,
        &["p50", "p90", "p99", "max"],
    )?;
    quantile_columns(&global["first_dispatch"], n, duration, &["p50", "p99"])?;
    for value in series(&global["records_per_request_mean"], n)? {
        require(
            value.is_null()
                || value
                    .as_f64()
                    .is_some_and(|v| (0.0..=1_000_000.0).contains(&v)),
            "request record mean",
        )?;
    }
    let mut pending = 0u64;
    for i in 0..n {
        pending = pending
            .checked_add(number(&global["accepted"][i])?)
            .and_then(|p| {
                p.checked_sub(
                    number(&global["acked"][i]).ok()?
                        + number(&global["not_written"][i]).ok()?
                        + number(&global["unknown"][i]).ok()?,
                )
            })
            .ok_or("outstanding bucket accounting")?;
        require(
            number(&series(&global["outstanding"], n)?[i])? == pending,
            "outstanding series",
        )?;
    }
    let credits = &global["credits"];
    let pools = array(&credits["pools"], 32)?;
    let capacities = series(&credits["capacity"], pools.len())?;
    let mut pool_names = BTreeSet::new();
    for pool in pools {
        require(pool_names.insert(text(pool)?), "duplicate pool")?;
    }
    for field in ["held_observed_max", "held_last_observed"] {
        let values = series(&credits[field], pools.len())?;
        for (i, row) in values.iter().enumerate() {
            for v in series(row, n)? {
                require(
                    number(v)? <= number(&capacities[i])?,
                    "credit held exceeds capacity",
                )?;
            }
        }
    }
    for p in 0..pools.len() {
        for i in 0..n {
            require(
                number(&credits["held_last_observed"][p][i])?
                    <= number(&credits["held_observed_max"][p][i])?,
                "last credit exceeds observed maximum",
            )?;
        }
    }
    let per_broker = series(&r.buckets["brokers"], brokers.len())?;
    let mut seen = BTreeSet::new();
    for b in per_broker {
        let id = number(&b["broker"])?;
        require(
            broker_ids.contains(&id) && seen.insert(id),
            "broker series identity",
        )?;
        for name in [
            "client_requests",
            "broker_requests",
            "responses",
            "bytes_wire",
            "disconnects",
            "setup_failures",
            "drops",
            "delayed_hooks",
            "acked",
            "client_inflight_max",
            "client_inflight_end",
            "active_connections",
        ] {
            sum(&b[name], n)?;
        }
        quantile_columns(&b["dispatch_rtt"], n, duration, &["p50", "p99"])?;
        quantile_columns(&b["full_write_rtt"], n, duration, &["p50", "p99"])?;
        for i in 0..n {
            require(
                number(&b["client_inflight_end"][i])? <= number(&b["client_inflight_max"][i])?,
                "inflight maximum",
            )?;
        }
        require(
            number(&b["client_inflight_end"][n - 1])? == 0
                && number(&b["active_connections"][n - 1])? == 0,
            "live terminal connection/request",
        )?;
    }
    for name in [
        "client_requests",
        "broker_requests",
        "responses",
        "bytes_wire",
    ] {
        for i in 0..n {
            let total = per_broker
                .iter()
                .try_fold(0, |a, b| Ok::<_, String>(a + number(&b[name][i])?))?;
            require(
                total == number(&global[name][i])?,
                "broker sum differs from global",
            )?;
        }
    }
    for name in ["acked", "not_written", "leader"] {
        let matrix = series(&r.partitions[name], np)?;
        for row in matrix {
            for v in series(row, n)? {
                if name == "leader" {
                    require(
                        v.is_null() || broker_ids.contains(&number(v)?),
                        "partition leader",
                    )?;
                } else {
                    number(v)?;
                }
            }
        }
        if name != "leader" {
            for b in 0..n {
                let total = matrix
                    .iter()
                    .try_fold(0, |sum, row| Ok::<_, String>(sum + number(&row[b])?))?;
                let unrouted = if name == "not_written" {
                    number(&series(&r.partitions["unrouted_not_written"], n)?[b])?
                } else {
                    0
                };
                require(
                    total + unrouted == number(&global[name][b])?,
                    "partition count sum",
                )?;
            }
        }
    }
    records(r, offered, accepted, acked, np, &broker_ids, duration)?;
    for (name, count) in [
        ("latency_acked", acked),
        ("latency_all_deliveries", accepted),
        ("dispatch_rtt", number(&r.summary["responses"])?),
    ] {
        require(
            number(&r.summary[name]["count"])? == count,
            "distribution population count",
        )?;
        quantiles(&r.summary[name], Some(count), duration)?;
    }
    let written_count = number(&r.summary["full_write_rtt"]["count"])?;
    require(
        written_count <= number(&r.summary["responses"])?,
        "full-write population",
    )?;
    quantiles(&r.summary["full_write_rtt"], Some(written_count), duration)?;
    let ecdf = &r.distributions["latency_ecdf"];
    require(
        number(&ecdf["population_count"])? == acked,
        "ECDF population",
    )?;
    let points = array(&ecdf["points"], 1000)?;
    require(points.is_empty() == (acked == 0), "ECDF emptiness")?;
    let (mut last_time, mut rank) = (0, 0);
    for p in points {
        let at = time(&p["latency"], duration)?;
        let next = number(&p["cumulative_count"])?;
        require(
            at >= last_time && next > rank && next <= acked,
            "ECDF order/rank",
        )?;
        last_time = at;
        rank = next;
    }
    require(rank == acked, "ECDF final rank")?;
    require(
        boolean(&ecdf["reduced"])? == (acked > points.len() as u64),
        "ECDF reduction label",
    )?;
    let histogram = array(&r.distributions["attempts_histogram"], 1024)?;
    let mut seen = BTreeSet::new();
    let mut total = 0;
    for row in histogram {
        require(
            seen.insert(number(&row["producer_attempts"])?),
            "attempt histogram duplicate",
        )?;
        total += number(&row["count"])?;
    }
    require(total == accepted, "attempt histogram population")?;
    let reasons = array(&r.distributions["outcomes_by_reason"], 4096)?;
    let mut outcomes = [0u64; 4];
    let mut keys = BTreeSet::new();
    for row in reasons {
        let code = number(&row["outcome"])? as usize;
        require(
            code < 4 && keys.insert((code, text(&row["reason"])?)),
            "outcome reason table",
        )?;
        outcomes[code] += number(&row["count"])?;
    }
    require(
        outcomes
            == [
                acked,
                number(&totals["not_written"])?,
                number(&totals["unknown"])?,
                refused,
            ],
        "reason table population",
    )?;
    let bands = array(&r.environment["bands"], 1024)?;
    let mut last = 0;
    for band in bands {
        let start = time(&band["start"], 300_000_000_000)?;
        let end = time(&band["end"], 300_000_000_000)?;
        require(start >= last && start < end, "band order/window")?;
        last = start;
        require(
            [
                "isolation",
                "link_outage",
                "service_delay",
                "reject",
                "throttle",
                "loss",
                "stop_polling",
            ]
            .contains(&text(&band["kind"])?),
            "band kind",
        )?;
        require(
            band["broker"].is_null() || broker_ids.contains(&number(&band["broker"])?),
            "band broker",
        )?;
        text(&band["label"])?;
        for (name, planned) in [("realized_start", start), ("realized_end", end)] {
            require(
                nullable_time(&band[name], duration)?
                    == if planned <= duration {
                        Some(planned)
                    } else {
                        None
                    },
                "band realization",
            )?;
        }
        if band["kind"] == "link_outage" {
            require(
                ["ToBroker", "FromBroker", "Both"].contains(&text(&band["direction"])?),
                "link direction",
            )?;
            require(
                ["BlackHole", "FailFast"].contains(&text(&band["mode"])?),
                "link mode",
            )?;
        }
    }
    last = 0;
    for marker in array(&r.environment["markers"], 4096)? {
        let at = time(&marker["at"], duration)?;
        require(at >= last, "marker ordering")?;
        last = at;
        require(
            [
                "leader_move",
                "add_partitions",
                "topic_delete",
                "topic_recreate",
                "flush",
                "flush_done",
                "close",
                "closed",
                "setup_failure",
                "disconnect",
                "drop",
                "fatal",
                "topic_ready",
                "topic_failed",
                "workload_step",
                "connection_closed",
            ]
            .contains(&text(&marker["kind"])?),
            "marker kind",
        )?;
        if !marker["connection"].is_null() {
            decimal(&marker["connection"])?;
        }
        text(&marker["label"])?;
    }
    number(&r.environment["markers_truncated"])?;
    require(r.phase_evidence.len() <= 1024, "phase count")?;
    let mut phases = BTreeSet::new();
    for p in &r.phase_evidence {
        require(
            phases.insert(&p.phase)
                && p.start < p.end
                && p.end <= 300_000_000_000
                && p.witnesses.len() <= 32
                && p.exact_counts.len() <= 64,
            "phase contract",
        )?;
        for ids in p.witnesses.values() {
            require(ids.len() <= 16, "phase witness cap")?;
            for id in ids {
                decimal(&Value::String(id.clone()))?;
            }
        }
        for check in &p.check_results {
            expectation(check)?;
        }
    }
    hdr(&r.hdr, duration, &broker_ids, &partition_ids)?;
    require(
        serde_json::to_vec(r).map_err(|e| e.to_string())?.len() <= MAX_REPORT_BYTES,
        "report byte cap",
    )
}
fn config(c: &Value, brokers: &BTreeSet<u64>) -> Result<(), String> {
    if let Some(mode) = c.get("batch_target_mode") {
        require(
            matches!(mode.as_str(), Some("Raw" | "EstimatedWire")),
            "batch target mode",
        )?;
    }
    if let Some(policy) = c.get("descriptor_admission_policy") {
        require(
            matches!(policy.as_str(), Some("Shared" | "PartitionPressure")),
            "descriptor admission policy",
        )?;
    }
    for key in [
        "linger_max",
        "batch_target_bytes",
        "batch_hard_bytes",
        "max_in_flight_per_connection",
        "lanes",
        "connection_wire_window_bytes",
        "request_timeout",
        "delivery_timeout",
        "metadata_max_age",
    ] {
        number(&c[key])?;
    }
    require(
        (1..=255).contains(&number(&c["lanes"])?)
            && (1..=255).contains(&number(&c["max_in_flight_per_connection"])?)
            && number(&c["batch_target_bytes"])? <= number(&c["batch_hard_bytes"])?,
        "config bounds",
    )?;
    require(
        number(&c["retry_backoff"]["min"])? <= number(&c["retry_backoff"]["max"])?,
        "retry backoff",
    )?;
    let compression = text(&c["compression"])?;
    require(
        compression == "None"
            || compression
                .strip_prefix("Zstd { level: ")
                .and_then(|s| s.strip_suffix(" }"))
                .is_some_and(|s| !s.is_empty() && s.bytes().all(|b| b.is_ascii_digit())),
        "compression",
    )?;
    let d = &c["driver"];
    require(
        [
            "bounded-directional-propagation/v1",
            "sim-network-local-completion/v1",
        ]
        .contains(&text(&d["transport_model"])?),
        "transport model",
    )?;
    for key in [
        "chunk_bytes",
        "encode_bytes_per_poll",
        "encode_cost_per_poll",
        "jitter",
        "local_completion_latency",
        "pipe_bytes",
        "service_delay",
    ] {
        number(&d[key])?;
    }
    boolean(&d["crash_on_isolation"])?;
    let mut seen = BTreeSet::new();
    for link in array(&d["propagation_links"], 16)? {
        let b = number(&link["broker"])?;
        require(
            brokers.contains(&b) && seen.insert(b) && number(&link["chunk_bytes"])? > 0,
            "propagation link identity/chunk",
        )?;
        number(&link["to_broker_latency_ns"])?;
        number(&link["from_broker_latency_ns"])?;
    }
    Ok(())
}
fn records(
    r: &ExperimentReport,
    population: u64,
    accepted: u64,
    acked: u64,
    np: usize,
    brokers: &BTreeSet<u64>,
    duration: u64,
) -> Result<(), String> {
    let rows = &r.records;
    let count = number(&rows["count"])? as usize;
    require(
        count <= 65_536
            && count as u64 <= population
            && number(&rows["population_count"])? == population,
        "record population/sample cap",
    )?;
    let complete = boolean(&rows["complete"])?;
    require(
        complete == (count as u64 == population),
        "sample completeness",
    )?;
    require(
        rows["outcome_names"] == serde_json::json!(["acked", "not_written", "unknown", "refused"]),
        "outcome table",
    )?;
    require(
        rows["sampling"]["method"] == "evenly-spaced-rank-in-record-id-order",
        "sampling method",
    )?;
    let reasons = array(&rows["reason_names"], 4096)?;
    let mut names = BTreeSet::new();
    for reason in reasons {
        require(names.insert(text(reason)?), "reason names unique")?;
    }
    for name in [
        "record_id",
        "due",
        "offer",
        "accept",
        "deliver",
        "outcome",
        "reason",
        "producer_attempts",
        "client_dispatches",
        "partition",
        "broker",
        "offset",
    ] {
        series(&rows[name], count)?;
    }
    let mut ids = BTreeSet::new();
    let mut previous = None;
    let mut outcomes = [0u64; 4];
    let n = number(&r.buckets["count"])? as usize;
    let width = number(&r.buckets["bucket_ns"])?;
    let mut observed = vec![vec![0u64; n]; 4];
    let mut latency = Vec::new();
    for i in 0..count {
        let id = decimal(&rows["record_id"][i])?;
        require(
            ids.insert(id) && previous.is_none_or(|p| id > p),
            "record ID uniqueness/order",
        )?;
        previous = Some(id);
        let due = time(&rows["due"][i], duration)?;
        let offer = time(&rows["offer"][i], duration)?;
        require(due <= offer, "offer precedes due")?;
        let accept = nullable_time(&rows["accept"][i], duration)?;
        let deliver = nullable_time(&rows["deliver"][i], duration)?;
        let outcome = number(&rows["outcome"][i])? as usize;
        require(outcome < 4, "record outcome")?;
        outcomes[outcome] += 1;
        require(
            number(&rows["reason"][i])? < (reasons.len() as u64),
            "record reason",
        )?;
        let dispatches = number(&rows["client_dispatches"][i])?;
        if outcome == 3 {
            require(
                accept.is_none()
                    && deliver.is_none()
                    && rows["partition"][i].is_null()
                    && rows["broker"][i].is_null()
                    && rows["offset"][i].is_null()
                    && rows["producer_attempts"][i].is_null()
                    && dispatches == 0,
                "refusal nullable semantics",
            )?;
        } else {
            let a = accept.ok_or("accepted row missing accept")?;
            let d = deliver.ok_or("accepted row missing delivery")?;
            require(offer <= a && a <= d, "record time order")?;
            observed[0][(a / width) as usize] += 1;
            observed[outcome + 1][(d / width) as usize] += 1;
            if outcome == 0 {
                latency.push(d - a);
            }
            require(
                (outcome != 0 && rows["partition"][i].is_null())
                    || number(&rows["partition"][i])? < np as u64,
                "record partition",
            )?;
            number(&rows["producer_attempts"][i])?;
            if !rows["broker"][i].is_null() {
                require(
                    brokers.contains(&number(&rows["broker"][i])?),
                    "record broker",
                )?;
            }
            if !rows["offset"][i].is_null() {
                require(
                    outcome == 0 && signed(&rows["offset"][i])? >= 0,
                    "record offset",
                )?;
            }
            if outcome == 0 {
                require(
                    !rows["offset"][i].is_null() && dispatches > 0,
                    "acked record proof",
                )?;
            }
        }
    }
    for (j, name) in ["accepted", "acked", "not_written", "unknown"]
        .iter()
        .enumerate()
    {
        for (i, observed_count) in observed[j].iter().enumerate() {
            let exact = number(&r.buckets["global"][*name][i])?;
            require(
                if complete {
                    *observed_count == exact
                } else {
                    *observed_count <= exact
                },
                "record bucket population",
            )?;
        }
    }
    latency.sort_unstable();
    if complete && !latency.is_empty() {
        for (name, ppm) in [
            ("p50", 500_000),
            ("p90", 900_000),
            ("p99", 990_000),
            ("p999", 999_000),
        ] {
            let rank = (latency.len() as u64 * ppm).div_ceil(1_000_000) as usize - 1;
            require(
                number(&r.summary["latency_acked"][name])? == latency[rank],
                "complete record quantile",
            )?;
        }
    }
    let totals = [
        acked,
        number(&r.summary["records"]["not_written"])?,
        number(&r.summary["records"]["unknown"])?,
        population - accepted,
    ];
    for i in 0..4 {
        require(
            if complete {
                outcomes[i] == totals[i]
            } else {
                outcomes[i] <= totals[i]
            },
            "sample outcome counts",
        )?;
    }
    Ok(())
}
fn hdr(
    h: &Value,
    duration: u64,
    brokers: &BTreeSet<u64>,
    partitions: &BTreeSet<(&str, u64)>,
) -> Result<(), String> {
    if h.is_null() {
        return Ok(());
    }
    for (i, metric) in kr_kafka_producer::telemetry::metrics::Metric::ALL
        .iter()
        .enumerate()
    {
        require(
            series(&h["metric_names"], 11)?[i] == format!("{metric:?}")
                && series(&h["units"], 11)?[i] == format!("{:?}", metric.unit()),
            "HDR metric/unit enums",
        )?;
    }
    let scopes = array(&h["scopes"], 273)?;
    let mut seen = BTreeSet::new();
    for scope in scopes {
        require(seen.insert(scope.to_string()), "HDR scope duplicate")?;
        match text(&scope["kind"])? {
            "global" => {}
            "broker" => require(brokers.contains(&number(&scope["broker"])?), "HDR broker")?,
            "partition" => require(
                partitions.contains(&(uuid(&scope["topic_id"])?, number(&scope["partition"])?)),
                "HDR partition",
            )?,
            _ => return Err("HDR scope kind".into()),
        }
    }
    let intervals = &h["intervals"];
    let n = number(&intervals["count"])? as usize;
    require((1..=1024).contains(&n), "HDR interval cap")?;
    for name in ["epoch", "requested", "taken", "start", "end"] {
        series(&intervals[name], n)?;
    }
    let (mut epoch, mut taken) = (0, 0);
    for i in 0..n {
        let next = decimal(&intervals["epoch"][i])?;
        let now = time(&intervals["taken"][i], duration)?;
        require(next > epoch && now >= taken, "HDR epoch/take order")?;
        epoch = next;
        taken = now;
        let request = nullable_time(&intervals["requested"][i], duration)?;
        let start = nullable_time(&intervals["start"][i], duration)?;
        let end = nullable_time(&intervals["end"][i], duration)?;
        require(
            request.is_none_or(|r| r <= now)
                && start.is_none() == end.is_none()
                && start.zip(end).is_none_or(|(a, b)| a <= b && b <= now),
            "HDR actual interval bounds",
        )?;
    }
    let series_rows = series(&h["series"], scopes.len() * 11)?;
    let mut keys = BTreeSet::new();
    for row in series_rows {
        let scope = number(&row["scope"])?;
        let metric = number(&row["metric"])?;
        require(
            scope < scopes.len() as u64 && metric < 11 && keys.insert((scope, metric)),
            "HDR series identity",
        )?;
        let high = number(&row["highest_trackable"])?;
        require(
            (1..=5).contains(&number(&row["significant_digits"])?),
            "HDR precision",
        )?;
        for name in [
            "count",
            "p50_range",
            "p90_range",
            "p99_range",
            "p999_range",
            "exact_max",
            "out_of_range",
            "count_overflow",
            "diagnostic_overflow",
        ] {
            series(&row[name], n)?;
        }
        for i in 0..n {
            let count = number(&row["count"][i])?;
            let max = nullable_time(&row["exact_max"][i], high)?;
            require(max.is_some() == (count > 0), "HDR empty maximum")?;
            let mut previous = None;
            for name in ["p50_range", "p90_range", "p99_range", "p999_range"] {
                let q = &row[name][i];
                if count == 0 {
                    require(q.is_null(), "HDR empty quantile")?;
                } else {
                    let a = series(q, 2)?;
                    let low = number(&a[0])?;
                    let high = number(&a[1])?;
                    require(
                        low <= high && previous.is_none_or(|(pl, ph)| low >= pl && high >= ph),
                        "HDR equivalent range ordering",
                    )?;
                    previous = Some((low, high));
                }
            }
            number(&row["out_of_range"][i])?;
            number(&row["count_overflow"][i])?;
            boolean(&row["diagnostic_overflow"][i])?;
        }
    }
    for d in series(&h["diagnostics"], n)? {
        for field in [
            "omitted_scope_samples",
            "scope_capacity_rejections",
            "invalid_scope_samples",
            "invalid_time_samples",
            "missing_time_samples",
            "invalid_depth_samples",
        ] {
            number(&d[field])?;
        }
        boolean(&d["diagnostic_overflow"])?;
    }
    for missed in array(&h["missed_requests"], 1024)? {
        require(
            time(&missed["scheduled"], duration)? <= time(&missed["attempted"], duration)?,
            "missed HDR request order",
        )?;
        text(&missed["reason"])?;
    }
    Ok(())
}
fn expectation(e: &ExpectationResult) -> Result<(), String> {
    require(
        ["passed", "failed", "observation", "not-applicable"].contains(&e.status.as_str())
            && e.name.len() <= 256
            && e.detail.len() <= 4096,
        "expectation result",
    )
}
pub(super) fn bundle(b: &ExperimentBundle) -> Result<(), String> {
    require(
        b.schema == BUNDLE_SCHEMA && !b.runs.is_empty() && b.runs.len() <= 32,
        "bundle schema/run cap",
    )?;
    require(
        b.variants.len() <= 4096 && b.seeds.len() <= 4096,
        "bundle metadata cap",
    )?;
    let mut keys = BTreeSet::new();
    let mut names = BTreeSet::new();
    for variant in &b.variants {
        require(
            names.insert(text(&variant["name"])?),
            "bundle variant duplicate",
        )?;
    }
    let mut seeds = BTreeSet::new();
    for seed in &b.seeds {
        decimal(&Value::String(seed.clone()))?;
        require(seeds.insert(seed.as_str()), "bundle seed duplicate")?;
    }
    for r in &b.runs {
        r.validate()?;
        require(
            r.meta["scenario"] == b.scenario
                && names.contains(text(&r.meta["variant"]["name"])?)
                && seeds.contains(text(&r.meta["seed"])?)
                && keys.insert((text(&r.meta["variant"]["name"])?, text(&r.meta["seed"])?)),
            "bundle run identity",
        )?;
    }
    for e in &b.comparisons {
        expectation(e)?;
    }
    require(
        number(&b.page["count"])? > number(&b.page["index"])?
            && number(&b.page["total_runs"])? >= b.runs.len() as u64,
        "bundle page",
    )?;
    require(
        serde_json::to_vec(b).map_err(|e| e.to_string())?.len() <= MAX_BUNDLE_BYTES,
        "bundle byte cap",
    )
}
