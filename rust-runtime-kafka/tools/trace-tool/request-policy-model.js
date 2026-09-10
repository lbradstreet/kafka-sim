"use strict";
(function (root) {
  const C = root.TRACE_VIEWER_CORE;
  const fields = ["offered", "accepted", "refused", "acked", "failed", "ack_p99_ns", "ack_max_ns", "produce_requests", "wire_bytes", "partition_pending_gap_ns", "first_observation_wait_max_ns", "broker_attempts_max", "last_terminal_ns"];
  const nullable = new Set(["ack_p99_ns", "ack_max_ns", "first_observation_wait_max_ns", "broker_attempts_max", "last_terminal_ns"]);
  function require(ok, why) { if (!ok) throw new Error(why); }
  function text(v) { require(typeof v === "string" && v.length <= 8192, "bounded text"); return v; }
  function list(v, max) { require(Array.isArray(v) && v.length <= max, "bounded list"); return v; }
  function decimal(v) { require(typeof v === "string", "decimal string required"); return C.exactUnsigned(v, "metric"); }
  function metrics(v) {
    C.record(v, "metrics");
    const out = {};
    fields.forEach(k => { out[k] = v[k] === null && nullable.has(k) ? null : decimal(v[k]); });
    require(out.offered === out.accepted + out.refused, "offer accounting");
    require(out.accepted === out.acked + out.failed, "terminal accounting");
    require((out.acked === 0n) === (out.ack_p99_ns === null), "latency population");
    require((out.acked === 0n) === (out.ack_max_ns === null), "maximum latency population");
    require(out.ack_p99_ns === null || out.ack_max_ns >= out.ack_p99_ns, "latency order");
    return out;
  }
  function validate(data) {
    C.record(data, "artifact");
    require(data.schema === "kr-request-policy-dashboard/v1", "unsupported schema");
    const provenance = list(data.provenance, 8).map(p => {
      text(p.name); require(/^[a-f0-9]{64}$/.test(p.sha256), "provenance hash"); return p;
    });
    require(provenance.length >= 3, "missing provenance");
    const seen = new Set();
    const cases = list(data.cases, 1000).map(row => {
      const key = list(row.case, 4).map(text);
      require(key.length === 4 && ["common", "original"].includes(key[2]), "case identity");
      decimal(key[3]);
      const id = key.join(" / ");
      require(!seen.has(id), "duplicate case"); seen.add(id);
      require(typeof row.same_offered_ids === "boolean", "population flag");
      const runs = { sealed: metrics(row.runs.sealed), "broker-ready": metrics(row.runs["broker-ready"]) };
      if (row.runs.java !== null) { require(key[3] === "0", "Java seed reference"); runs.java = metrics(row.runs.java); }
      else runs.java = null;
      return { ...row, id, runs, description: list(row.description, 128).map(text) };
    });
    require(cases.length > 0, "empty catalogue");
    return { provenance, cases };
  }
  // Keep comparisons exact, including differences above Number.MAX_SAFE_INTEGER.
  function change(row, field) {
    const a = row.runs.sealed[field], b = row.runs["broker-ready"][field];
    return a === null || b === null ? null : b - a;
  }
  function compare(a, b) { return a === b ? 0 : a === null ? 1 : b === null ? -1 : a < b ? -1 : 1; }
  function filterSort(cases, query, profile, field) {
    const rows = cases.filter(r => r.id.toLowerCase().includes(query.toLowerCase()) && (!profile || r.case[2] === profile));
    return rows.sort((a, b) => (field ? compare(change(a, field), change(b, field)) : 0) || a.id.localeCompare(b.id));
  }
  root.REQUEST_POLICY_MODEL = Object.freeze({ fields, validate, change, filterSort });
})(globalThis);
