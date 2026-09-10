"use strict";
const test = require("node:test");
const assert = require("node:assert/strict");
const fs = require("node:fs");
require("./trace-viewer-core.js");
require("./request-policy-model.js");
const M = globalThis.REQUEST_POLICY_MODEL;
function fixture() {
  const metrics = Object.fromEntries(M.fields.map(k => [k, "1"]));
  Object.assign(metrics, { offered: "2", refused: "1", accepted: "1", acked: "1", failed: "0" });
  return { schema: "kr-request-policy-dashboard/v1", provenance: ["full", "seeds", "java"].map(name => ({name, sha256: "0".repeat(64)})),
    cases: [{case: ["baseline.test", "v", "common", "0"], same_offered_ids: true, description: ["<script>inert</script>"],
      runs: {sealed: structuredClone(metrics), "broker-ready": structuredClone(metrics), java: structuredClone(metrics)}}] };
}
test("full-width times and byte totals remain exact", () => {
  const f = fixture();
  f.cases[0].runs.sealed.wire_bytes = "18446744073709551614";
  f.cases[0].runs["broker-ready"].wire_bytes = "18446744073709551615";
  f.cases[0].runs.sealed.ack_max_ns = "9007199254740993";
  const d = M.validate(f);
  assert.equal(M.change(d.cases[0], "wire_bytes"), 1n);
  assert.equal(d.cases[0].runs.sealed.ack_max_ns, 9007199254740993n);
});
test("rejects schema, accounting, unsafe integers, bad hashes and duplicate cases", () => {
  const edits = [f => f.schema = "v2", f => f.cases[0].runs.sealed.acked = "2", f => f.cases[0].runs.sealed.wire_bytes = 9007199254740992,
    f => f.cases[0].runs.sealed.wire_bytes = "18446744073709551616", f => f.cases[0].case[3] = "01", f => f.provenance[0].sha256 = "x",
    f => f.cases.push(structuredClone(f.cases[0])), f => f.cases[0].runs.sealed.ack_max_ns = "0", f => f.cases[0].runs.sealed.ack_p99_ns = null,
    f => { f.cases[0].runs.sealed.ack_p99_ns = "0"; f.cases[0].runs.sealed.ack_max_ns = null; }];
  edits.forEach(edit => { const f = fixture(); edit(f); assert.throws(() => M.validate(f)); });
});
test("filters, exact difference ordering and missing ACK populations", () => {
  const f = fixture(), b = structuredClone(f.cases[0]); b.case[1] = "other"; b.runs["broker-ready"].produce_requests = "0"; f.cases.push(b);
  const d = M.validate(f);
  assert.equal(M.filterSort(d.cases, "baseline", "common", "produce_requests")[0].case[1], "other");
  assert.equal(M.filterSort(d.cases, "absent", "", "").length, 0);
  for (const run of Object.values(f.cases[0].runs)) Object.assign(run, { accepted: "0", acked: "0", refused: "2", ack_p99_ns: null, ack_max_ns: null });
  assert.equal(M.change(M.validate(f).cases[0], "ack_p99_ns"), null);
  f.cases[0].runs.sealed.ack_max_ns = "0";
  assert.throws(() => M.validate(f));
});
test("HTML wiring uses native controls and safe text rendering", () => {
  const html = fs.readFileSync(__dirname + "/request-policy.html", "utf8");
  const js = fs.readFileSync(__dirname + "/request-policy-viewer.js", "utf8");
  for (const id of ["query", "profile", "sort", "case", "detail", "catalogue", "error", "status"]) assert.ok(html.includes(`id="${id}"`));
  assert.ok(html.indexOf("request-policy-model.js") < html.indexOf("request-policy-data.js"));
  assert.ok(html.indexOf("request-policy-data.js") < html.indexOf("request-policy-viewer.js"));
  assert.doesNotMatch(js, /innerHTML|eval\(|new Function/);
});
