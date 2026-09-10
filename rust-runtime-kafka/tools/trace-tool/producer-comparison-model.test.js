import "./trace-viewer-core.js";
import "./producer-comparison-model.js";
import "./producer-experiment-model.js";
const M = globalThis.PRODUCER_EXPERIMENT_MODEL;
const source = await Deno.readTextFile(
  new URL("./producer-comparison-data.js", import.meta.url),
);
const raw = M.parseArtifactText(source);
function assert(ok, why = "assertion failed") {
  if (!ok) throw new Error(why);
}
function rejects(fn) {
  let rejected = false;
  try {
    fn();
  } catch {
    rejected = true;
  }
  assert(rejected, "corruption accepted");
}
Deno.test("paired recovery sample preserves complete populations and exact wrappers", () => {
  const pair = M.parseArtifactText(JSON.stringify(raw)).pairs[0];
  assert(pair.runs.classic.summary.acked === 96);
  assert(pair.runs.native.summary.acked === 96);
  assert(pair.fault_exposure_comparable);
  assert(pair.environment.bands[0].start === 10_000_000_000);
  rejects(() => M.parseArtifactText(source + "alert(1)"));
  rejects(() => M.parseArtifactText("alert(1);" + source));
  rejects(() =>
    M.parseArtifactText(
      source.replace("classic_visualization", "unknown_generator"),
    )
  );
});
Deno.test("full-width origins and seeds survive comparison parsing", () => {
  const bundle = structuredClone(raw), pair = bundle.pairs[0];
  pair.seed = "18446744073709551615";
  pair.origin_ns = String((1n << 64n) - 1n - BigInt(pair.duration_ns));
  const result = M.parseArtifactText(JSON.stringify(bundle)).pairs[0];
  assert(result.seed === "18446744073709551615");
  assert(
    BigInt(result.origin_ns) + BigInt(result.duration_ns) === (1n << 64n) - 1n,
  );
});
for (
  const [name, mutate] of [
    ["schema", (b) => b.schema = "unknown"],
    ["extra top-level field", (b) => b.extra = 1],
    ["empty pairs", (b) => b.pairs = []],
    ["17 pairs", (b) => b.pairs = Array(17).fill(b.pairs[0])],
    ["duplicate pair", (b) => b.pairs.push(b.pairs[0])],
    ["unsafe absolute number", (b) => b.pairs[0].origin_ns = 9007199254740992],
    ["noncanonical seed", (b) => b.pairs[0].seed = "01"],
    ["seed overflow", (b) => b.pairs[0].seed = "18446744073709551616"],
    [
      "absolute clock overflow",
      (b) => b.pairs[0].origin_ns = "18446744073709551615",
    ],
    ["profile", (b) => b.pairs[0].profile = "same"],
    ["replay", (b) => b.pairs[0].replay_verified = false],
    ["manifest hash", (b) => b.pairs[0].manifest_sha256 = "x"],
    ["bucket geometry", (b) => b.pairs[0].bucket_ns++],
    ["oversized time", (b) => b.pairs[0].duration_ns = 300_000_000_001],
    ["unknown band", (b) => b.pairs[0].environment.bands[0].kind = "unknown"],
    ["inverted band", (b) => b.pairs[0].environment.bands[0].end = 0],
    ["summary total", (b) => b.pairs[0].runs.classic.summary.acked++],
    ["series total", (b) => b.pairs[0].runs.classic.buckets.acked[0]++],
    ["series dimensions", (b) => b.pairs[0].runs.classic.buckets.acked.pop()],
    [
      "outstanding continuity",
      (b) => b.pairs[0].runs.classic.buckets.outstanding[0]++,
    ],
    [
      "empty quantile semantics",
      (b) => b.pairs[0].runs.classic.buckets.p99[0] = 1,
    ],
    [
      "quantile order",
      (b) => b.pairs[0].runs.classic.summary.p50 = b.pairs[0].duration_ns,
    ],
    ["duplicate partition", (b) =>
      b.pairs[0].runs.classic.partitions.push(
        b.pairs[0].runs.classic.partitions[0],
      )],
    [
      "partition bound",
      (b) => b.pairs[0].runs.classic.partitions[0].partition = 1024,
    ],
    [
      "partition population",
      (b) => b.pairs[0].runs.classic.partitions[0].acked[0]++,
    ],
    [
      "broker population",
      (b) => b.pairs[0].runs.classic.brokers[0].wire_bytes[0]++,
    ],
    ["ECDF count", (b) => b.pairs[0].runs.classic.ecdf.at(-1).count++],
    ["ECDF quantile", (b) => {
      const r = b.pairs[0].runs.classic;
      r.ecdf.find((p) => p.count >= Math.ceil(r.summary.acked / 2)).latency++;
    }],
    ["reasons population", (b) =>
      b.pairs[0].runs.classic.failure_reasons.push({
        label: "Java failure",
        count: 1,
      })],
    [
      "unreported exposure gap",
      (b) =>
        b.pairs[0].runs.classic.coverage_gaps.push("rule has no opportunity"),
    ],
    ["duplicate source", (b) => b.pairs[0].sources[1] = b.pairs[0].sources[0]],
  ]
) {
  Deno.test(`comparison rejects ${name}`, () => {
    const b = structuredClone(raw);
    mutate(b);
    rejects(() => M.validateData(b));
  });
}
Deno.test("unmatched exposure remains an explicitly labeled usable observation", () => {
  const b = structuredClone(raw);
  b.pairs[0].runs.classic.coverage_gaps.push("rule has no opportunity");
  b.pairs[0].fault_exposure_comparable = false;
  assert(!M.validateData(b).pairs[0].fault_exposure_comparable);
});
