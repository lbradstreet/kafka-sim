import "./trace-viewer-core.js";
import "./network-trace-model.js";
import "./network-trace-data.js";

const { parseArtifactText, validateData } = globalThis.NETWORK_TRACE_MODEL;
const bundledFixture = globalThis.NETWORK_TRACE_DATA;

function assert(condition, message = "assertion failed") {
  if (!condition) throw new Error(message);
}

function assertEquals(actual, expected, message = "values differ") {
  if (!Object.is(actual, expected)) {
    throw new Error(
      `${message}: expected ${String(expected)}, got ${String(actual)}`,
    );
  }
}

function assertDeepEquals(actual, expected, message = "values differ") {
  const actualJson = JSON.stringify(actual);
  const expectedJson = JSON.stringify(expected);
  if (actualJson !== expectedJson) {
    throw new Error(`${message}: expected ${expectedJson}, got ${actualJson}`);
  }
}

function assertThrows(action, message = "expected operation to throw") {
  try {
    action();
  } catch (error) {
    assert(error instanceof Error, "operation threw a non-Error value");
    return error;
  }
  throw new Error(message);
}

function cloneFixture() {
  return structuredClone(bundledFixture);
}

Deno.test("validateData accepts and enriches the bundled network trace", () => {
  const fixture = cloneFixture();
  const validated = validateData(fixture);

  assertEquals(
    validated.raw,
    fixture,
    "validation should retain input artifact",
  );
  assertEquals(validated.steps.length, 12);
  assertEquals(validated.capacity, 4);
  assertEquals(validated.endpoints.left, 1n);
  assertEquals(validated.endpoints.right, 2n);
  for (const step of validated.steps) {
    assert(typeof step._startedAt === "bigint", "start must remain exact");
    assert(
      typeof step._completedAt === "bigint",
      "completion must remain exact",
    );
    assert(typeof step._duration === "bigint", "duration must remain exact");
  }
});

Deno.test("runtime lifetime steps may exceed one driving call budget", () => {
  const fixture = cloneFixture();
  fixture.runtime.max_steps_per_run = "3";
  fixture.runtime.total_steps = "12";
  const validated = validateData(fixture);
  assertEquals(validated.steps.length, fixture.steps.length);
});

Deno.test("bundled trace captures partition, backpressure, half-close, and EOF", () => {
  const { steps } = validateData(cloneFixture());
  const rejected = steps.find((step) => step.outcome === "rejected");
  assert(rejected, "partition rejection is missing");
  assertEquals(rejected.certainty, "not_applied");
  assertDeepEquals(rejected._flowBefore, rejected._flowAfter);

  const pending = steps.find((step) =>
    step.outcome === "pending_then_completed"
  );
  assert(pending, "capacity-pending transition is missing");
  assertDeepEquals(pending._flowBefore.leftToRight.bytes, [16, 17, 18, 19]);
  assertDeepEquals(pending._flowAfter.leftToRight.bytes, [17, 18, 19, 32]);
  assertEquals(pending.fields.status_while_pending.inflight_operations, 1);

  const halfClosed = steps.find((step) => step.outcome === "half_closed");
  assert(halfClosed, "half-close transition is missing");
  assert(halfClosed._flowBefore.leftToRight.senderOpen);
  assert(!halfClosed._flowAfter.leftToRight.senderOpen);
  assertDeepEquals(
    halfClosed._flowBefore.leftToRight.bytes,
    halfClosed._flowAfter.leftToRight.bytes,
  );

  const eof = steps.find((step) => step.outcome === "eof");
  assert(eof, "EOF transition is missing");
  assertEquals(eof.fields.read.end_of_stream, true);
  assertDeepEquals(eof._flowAfter.leftToRight.bytes, []);
  assert(!eof._flowAfter.leftToRight.senderOpen);
});

Deno.test("parseArtifactText accepts raw JSON and the exact generated wrapper", async () => {
  const modified = cloneFixture();
  modified.scenario = "raw_network_round_trip";
  assertDeepEquals(parseArtifactText(JSON.stringify(modified)), modified);

  const generated = await Deno.readTextFile(
    new URL("./network-trace-data.js", import.meta.url),
  );
  const originalEval = globalThis.eval;
  globalThis.eval = () => {
    throw new Error("parseArtifactText must not evaluate artifact text");
  };
  try {
    assertDeepEquals(parseArtifactText(generated), bundledFixture);
  } finally {
    globalThis.eval = originalEval;
  }
});

Deno.test("parseArtifactText rejects executable and trailing JavaScript", () => {
  const json = JSON.stringify(bundledFixture);
  const attackKey = "__networkTraceParserExecuted";
  delete globalThis[attackKey];
  assertThrows(() =>
    parseArtifactText(
      `globalThis.${attackKey} = true;\nglobalThis.NETWORK_TRACE_DATA = ${json};`,
    )
  );
  assertEquals(globalThis[attackKey], undefined);
  assertThrows(() =>
    parseArtifactText(
      `globalThis.NETWORK_TRACE_DATA = ${json};\nglobalThis.${attackKey} = true;`,
    )
  );
  assertEquals(globalThis[attackKey], undefined);
});

Deno.test("validateData rejects corrupt timing, sequence, and byte bounds", () => {
  const unsupported = cloneFixture();
  unsupported.schema += 1;
  assertThrows(() => validateData(unsupported));

  const unordered = cloneFixture();
  unordered.steps[1].sequence = unordered.steps[0].sequence;
  assertThrows(() => validateData(unordered));

  const duration = cloneFixture();
  duration.steps[0].duration_ns = "99";
  assertThrows(() => validateData(duration));

  const byteOverflow = cloneFixture();
  byteOverflow.steps[0].flow_after.left_to_right.bytes[0] = 256;
  assertThrows(() => validateData(byteOverflow));

  const capacityOverflow = cloneFixture();
  capacityOverflow.steps[3].flow_after.left_to_right.bytes.push(99);
  assertThrows(() => validateData(capacityOverflow));

  const stepOverflow = cloneFixture();
  stepOverflow.steps = Array.from(
    { length: 4_097 },
    () => structuredClone(stepOverflow.steps[0]),
  );
  assertThrows(() => validateData(stepOverflow));
});

Deno.test("validateData rejects discontinuous flow and provider snapshots", () => {
  const flow = cloneFixture();
  flow.steps[1].flow_before.left_to_right.bytes = [99];
  assertThrows(() => validateData(flow));

  const provider = cloneFixture();
  provider.steps[1].provider_before.connections = 0;
  assertThrows(() => validateData(provider));

  const terminal = cloneFixture();
  terminal.provider_completed.connections = 0;
  assertThrows(() => validateData(terminal));
});

Deno.test("validateData rejects incomplete metadata and contradictory outcomes", () => {
  const runtime = cloneFixture();
  delete runtime.runtime.rng_version;
  assertThrows(() => validateData(runtime));

  const config = cloneFixture();
  delete config.config.max_listener_backlog;
  assertThrows(() => validateData(config));

  const unknown = cloneFixture();
  unknown.steps[0].outcome = "unknown";
  assertThrows(() => validateData(unknown));

  const misplacedCertainty = cloneFixture();
  misplacedCertainty.steps[0].certainty = "applied";
  assertThrows(() => validateData(misplacedCertainty));

  const appliedMutation = cloneFixture();
  const rejected = appliedMutation.steps.find((step) =>
    step.outcome === "rejected"
  );
  rejected.flow_after.left_to_right.sender_open = false;
  assertThrows(() => validateData(appliedMutation));
});

Deno.test("network page composes shared and domain-specific components", async () => {
  const html = await Deno.readTextFile(
    new URL("./network-trace.html", import.meta.url),
  );
  const app = await Deno.readTextFile(
    new URL("./network-trace-viewer.js", import.meta.url),
  );
  const scripts = [
    "trace-viewer-core.js",
    "network-trace-model.js",
    "network-trace-data.js",
    "trace-viewer-ui.js",
    "network-trace-viewer.js",
  ].map((file) => html.indexOf(`<script src="./${file}"></script>`));
  assert(
    scripts.every((index) => index >= 0),
    "page is missing a required script",
  );
  assert(
    scripts.every((index, position) =>
      position === 0 || index > scripts[position - 1]
    ),
    "page script dependency order is invalid",
  );
  for (const stylesheet of ["trace-viewer.css", "network-trace.css"]) {
    assert(
      html.includes(`<link rel="stylesheet" href="./${stylesheet}">`),
      `page is missing ${stylesheet}`,
    );
  }
  for (
    const id of [
      "timeline",
      "flow-before",
      "flow-after",
      "occupancy-chart",
      "detail-fields",
    ]
  ) {
    assert(html.includes(`id="${id}"`), `page is missing #${id}`);
  }
  for (
    const integration of [
      "renderOperationTimeline({",
      "createStepNavigator({",
      "createArtifactLoader({",
      "renderJsonDump(elements.detailFields, step.fields)",
      "renderFlowSnapshot(elements.flowBefore",
      "renderOccupancyChart()",
    ]
  ) {
    assert(app.includes(integration), `network app is missing ${integration}`);
  }
  assert(!app.includes("innerHTML"), "network app must not inject trace HTML");
  assert(
    html.includes('<svg id="timeline" role="group"'),
    "interactive timeline must expose its descendant controls",
  );
  assert(
    html.includes('class="detail-context"'),
    "variable detail text must render inside the stable shared context",
  );
  assert(
    app.includes("MAX_VISIBLE_BUFFER_SLOTS"),
    "network buffer rendering must remain bounded",
  );
});
