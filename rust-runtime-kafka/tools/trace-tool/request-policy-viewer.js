"use strict";
(function () {
  const M = globalThis.REQUEST_POLICY_MODEL, C = globalThis.TRACE_VIEWER_CORE;
  const $ = id => document.getElementById(id);
  const element = (tag, text) => { const e = document.createElement(tag); e.textContent = text; return e; };
  const number = n => n === null ? "—" : n.toLocaleString("en-US");
  const nanos = n => n === null ? "—" : C.formatExactNanos(n);
  const signed = (n, time = false) => n === null ? "—" : `${n > 0n ? "+" : n < 0n ? "−" : ""}${time ? nanos(n < 0n ? -n : n) : number(n < 0n ? -n : n)}`;
  try {
    const data = M.validate(globalThis.REQUEST_POLICY_DATA);
    $("status").textContent = `${data.cases.length} native policy pairs · all executions replayed and audited · Java reference available at seed 0`;
    data.provenance.forEach(p => $("provenance").append(element("li", `${p.name}: ${p.sha256}`)));
    let rows = [];
    function detail() {
      const row = rows.find(r => r.id === $("case").value);
      $("detail").tBodies[0].replaceChildren(); $("description").replaceChildren();
      $("case-title").textContent = row ? row.id : "No matching case";
      $("intent").textContent = row ? globalThis.PRODUCER_EXPERIMENT_MODEL.derive.scenarioIntent(row.case[0]) : "";
      $("population").textContent = row ? `${row.same_offered_ids ? "Both native runs offered the same source populations." : "Native source populations differ because progress changes subsequent offers."} ${row.runs.java ? "Java is a reused seed-0 reference, not a new execution." : "No Java reference is included for this follow-up seed."}` : "";
      if (!row) return;
      row.description.forEach(t => $("description").append(element("li", t)));
      const labels = { offered: "Offered", accepted: "Accepted", refused: "Refused", acked: "Acknowledged", failed: "Failed deliveries", ack_p99_ns: "Successful p99", ack_max_ns: "Successful maximum", produce_requests: "Broker-observed Produce requests", wire_bytes: "All-API completed wire bytes", partition_pending_gap_ns: "Longest partition pending gap", first_observation_wait_max_ns: "Maximum wait to first broker observation", broker_attempts_max: "Maximum attempts observed at broker", last_terminal_ns: "Last terminal event" };
      M.fields.forEach(k => {
        const tr = document.createElement("tr"); tr.append(element("th", labels[k]));
        ["sealed", "broker-ready", "java"].forEach(mode => tr.append(element("td", row.runs[mode] ? (k.endsWith("_ns") ? nanos : number)(row.runs[mode][k]) : "—")));
        $("detail").tBodies[0].append(tr);
      });
    }
    function refresh() {
      const selected = $("case").value;
      rows = M.filterSort(data.cases, $("query").value, $("profile").value, $("sort").value);
      $("case").replaceChildren(); $("catalogue").tBodies[0].replaceChildren();
      rows.forEach(row => {
        const option = element("option", row.id); option.value = row.id; $("case").append(option);
        const tr = document.createElement("tr"); tr.append(element("td", row.id));
        ["produce_requests", "ack_p99_ns", "acked", "failed"].forEach(k => tr.append(element("td", signed(M.change(row, k), k.endsWith("_ns")))));
        $("catalogue").tBodies[0].append(tr);
      });
      if (rows.some(r => r.id === selected)) $("case").value = selected;
      $("catalogue-title").textContent = `Filtered comparisons (${rows.length})`;
      detail();
    }
    ["query", "profile", "sort"].forEach(id => $(id).addEventListener("input", refresh));
    $("case").addEventListener("change", detail);
    refresh();
  } catch (error) { $("error").hidden = false; $("error").textContent = `Cannot display evidence: ${error.message}`; $("status").textContent = "Evidence validation failed"; }
})();
