import "./trace-viewer-core.js";
import "./ring-trace-model.js";
import "./ring-trace-data.js";

const { parseArtifactText, validateData } = globalThis.RING_TRACE_MODEL;
const bundledFixture = globalThis.RING_TRACE_DATA;

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

function firstStatus(fixture) {
  return fixture.steps.find((step) => step.status !== null)?.status;
}

function firstPhysical(fixture) {
  return fixture.steps.find((step) => step.status?.physical)?.status.physical;
}

Deno.test("validateData accepts the bundled ring trace", () => {
  const fixture = cloneFixture();
  const validated = validateData(fixture);

  assertEquals(
    validated.raw,
    fixture,
    "validation should retain the input artifact",
  );
  assertEquals(
    validated.steps.length,
    fixture.steps.length,
    "validated step count",
  );
  assertEquals(
    validated.steps[0].sequence,
    fixture.steps[0].sequence,
    "first sequence",
  );
  assert(
    typeof validated.steps[0]._startedAt === "bigint",
    "start time should be exact",
  );
  assert(
    typeof validated.steps[0]._completedAt === "bigint",
    "completion time should be exact",
  );
  assert(
    typeof validated.steps[0]._duration === "bigint",
    "duration should be exact",
  );

  const forwardFallback = cloneFixture();
  forwardFallback.steps[0].outcome = "unknown";
  forwardFallback.steps[0].certainty = "unknown";
  const validatedFallback = validateData(forwardFallback);
  assertEquals(
    validatedFallback.steps[0]._outcome,
    "unknown",
    "unknown outcomes must retain their distinct presentation semantics",
  );
});

Deno.test("validateData bounds presentation steps", () => {
  const fixture = cloneFixture();
  fixture.steps = Array.from(
    { length: 4_097 },
    () => structuredClone(fixture.steps[0]),
  );
  const error = assertThrows(() => validateData(fixture));
  assert(error.message.includes("4096-step viewer limit"));
});

Deno.test("parseArtifactText accepts raw JSON and the exact generated JavaScript", async () => {
  const modifiedFixture = cloneFixture();
  modifiedFixture.scenario = "raw_json_round_trip";
  const parsedJson = parseArtifactText(JSON.stringify(modifiedFixture));
  assertDeepEquals(parsedJson, modifiedFixture, "raw JSON artifact");

  const generatedText = await Deno.readTextFile(
    new URL("./ring-trace-data.js", import.meta.url),
  );
  const originalEval = globalThis.eval;
  globalThis.eval = () => {
    throw new Error("parseArtifactText must not evaluate artifact text");
  };
  try {
    const parsedGenerated = parseArtifactText(generatedText);
    assertDeepEquals(
      parsedGenerated,
      bundledFixture,
      "generated JavaScript artifact",
    );
  } finally {
    globalThis.eval = originalEval;
  }
});

Deno.test("parseArtifactText rejects executable and trailing JavaScript without running it", () => {
  const json = JSON.stringify(bundledFixture);
  const attackKey = "__ringTraceParserExecuted";
  delete globalThis[attackKey];

  assertThrows(() =>
    parseArtifactText(
      `globalThis.${attackKey} = true;\nglobalThis.RING_TRACE_DATA = ${json};`,
    )
  );
  assertEquals(
    globalThis[attackKey],
    undefined,
    "leading JavaScript must not execute",
  );

  assertThrows(() =>
    parseArtifactText(
      `globalThis.RING_TRACE_DATA = ${json};\nglobalThis.${attackKey} = true;`,
    )
  );
  assertEquals(
    globalThis[attackKey],
    undefined,
    "trailing JavaScript must not execute",
  );
});

Deno.test("ring trace page wires shared components before its application", async () => {
  const html = await Deno.readTextFile(
    new URL("./ring-trace.html", import.meta.url),
  );
  const coreScript = html.indexOf(
    '<script src="./trace-viewer-core.js"></script>',
  );
  const modelScript = html.indexOf(
    '<script src="./ring-trace-model.js"></script>',
  );
  const sampleScript = html.indexOf(
    '<script src="./ring-trace-data.js"></script>',
  );
  const uiScript = html.indexOf(
    '<script src="./trace-viewer-ui.js"></script>',
  );
  const appScript = html.indexOf(
    '<script src="./ring-trace-viewer.js"></script>',
  );
  const inlineScriptMatches = [
    ...html.matchAll(/<script(?: [^>]*)?>([\s\S]*?)<\/script>/g),
  ].filter((match) => match[1]);
  const ui = await Deno.readTextFile(
    new URL("./trace-viewer-ui.js", import.meta.url),
  );
  const app = await Deno.readTextFile(
    new URL("./ring-trace-viewer.js", import.meta.url),
  );
  const styles = await Deno.readTextFile(
    new URL("./trace-viewer.css", import.meta.url),
  );

  assert(coreScript >= 0, "page must load the shared core");
  assert(modelScript > coreScript, "page must load the core before its model");
  assert(
    sampleScript > modelScript,
    "page must load the model before its sample",
  );
  assert(uiScript > sampleScript, "page must load the sample before shared UI");
  assert(
    appScript > uiScript,
    "page must load shared UI before its application",
  );
  assertEquals(
    inlineScriptMatches.length,
    0,
    "page should keep application code in checked external files",
  );
  for (const stylesheet of ["trace-viewer.css", "ring-trace.css"]) {
    assert(
      html.includes(`<link rel="stylesheet" href="./${stylesheet}">`),
      `page is missing ${stylesheet}`,
    );
  }
  for (const id of ["trace-file", "bundled-sample", "trace-source", "error"]) {
    assert(html.includes(`id="${id}"`), `page is missing #${id}`);
  }
  for (const shortcut of ["ArrowLeft", "ArrowRight"]) {
    assert(
      html.includes(`aria-keyshortcuts="${shortcut}"`),
      `page does not advertise ${shortcut}`,
    );
  }
  for (
    const syntaxClass of [
      "json-key",
      "json-string",
      "json-number",
      "json-boolean",
      "json-null",
    ]
  ) {
    assert(
      styles.includes(`.${syntaxClass}`),
      `shared styles are missing syntax color ${syntaxClass}`,
    );
  }
  for (
    const wiring of [
      "arrowKeyBelongsToControl(event.target)",
      "event.isComposing",
      "event.preventDefault()",
      "selectAdjacent(event.key)",
      "document.createTextNode(token.text)",
      "span.textContent = token.text",
      "element.replaceChildren(fragment)",
      "renderOperationTimeline",
      'tabindex: index === selectedIndex ? "0" : "-1"',
      "selectAndRestoreFocus(nextIndex, true)",
      "createArtifactLoader",
      "createStepNavigator",
    ]
  ) {
    assert(
      ui.includes(wiring),
      `shared UI is missing required wiring: ${wiring}`,
    );
  }
  for (
    const integration of [
      "renderOperationTimeline({",
      "renderJsonDump(elements.detailFields, step.fields)",
      "navigator.setItems(state.steps.length, initialIndex)",
      "loader.installBundled()",
    ]
  ) {
    assert(app.includes(integration), `ring app is missing ${integration}`);
  }
  assert(
    !ui.includes("innerHTML") && !app.includes("innerHTML"),
    "page must not inject highlighted fields as HTML",
  );
  assert(
    html.includes('<svg id="timeline" role="group"'),
    "interactive timeline must expose its descendant controls",
  );
  assert(
    html.includes('class="detail-context"'),
    "variable detail text must render inside the stable shared context",
  );
});

Deno.test("validateData rejects unsupported schemas and non-increasing sequences", () => {
  const unsupported = cloneFixture();
  unsupported.schema += 1;
  assertThrows(
    () => validateData(unsupported),
    "unsupported schema was accepted",
  );

  const unordered = cloneFixture();
  unordered.steps[1].sequence = unordered.steps[0].sequence;
  assertThrows(
    () => validateData(unordered),
    "non-increasing sequence was accepted",
  );
});

Deno.test("validateData rejects inconsistent duration and invalid decimals", () => {
  const inconsistentDuration = cloneFixture();
  inconsistentDuration.steps[0].duration_ns = "99";
  assertThrows(
    () => validateData(inconsistentDuration),
    "duration differing from completion minus start was accepted",
  );

  const invalidDecimal = cloneFixture();
  invalidDecimal.runtime.seed = "08244241983492743523";
  assertThrows(
    () => validateData(invalidDecimal),
    "non-canonical decimal was accepted",
  );

  const overflowingDecimal = cloneFixture();
  overflowingDecimal.runtime.seed = "18446744073709551616";
  assertThrows(
    () => validateData(overflowingDecimal),
    "uint64 overflow was accepted",
  );
});

Deno.test("validateData rejects non-sequential or post-runtime step timing", () => {
  const overlapping = cloneFixture();
  overlapping.steps[1].started_at_ns = "99";
  overlapping.steps[1].duration_ns = "51";
  assertThrows(
    () => validateData(overlapping),
    "step starting before the prior completion was accepted",
  );

  const afterRuntime = cloneFixture();
  const finalStep = afterRuntime.steps.at(-1);
  const completed = BigInt(afterRuntime.runtime.now_ns) + 1n;
  finalStep.completed_at_ns = completed.toString();
  finalStep.duration_ns = (completed - BigInt(finalStep.started_at_ns))
    .toString();
  assertThrows(
    () => validateData(afterRuntime),
    "step completing after runtime.now_ns was accepted",
  );
});

Deno.test("validateData rejects corrupt cursor relationships", () => {
  const accepted = cloneFixture();
  firstStatus(accepted).accepted_head = "1";
  assertThrows(
    () => validateData(accepted),
    "accepted head beyond accepted tail was accepted",
  );

  const durableInterval = cloneFixture();
  firstStatus(durableInterval).durable_head = "1";
  assertThrows(
    () => validateData(durableInterval),
    "durable head beyond durable tail was accepted",
  );

  const durableHead = cloneFixture();
  durableHead.steps[2].status.durable_head = "1";
  assertThrows(
    () => validateData(durableHead),
    "durable head beyond accepted head was accepted",
  );

  const durableTail = cloneFixture();
  firstStatus(durableTail).durable_tail = "1";
  assertThrows(
    () => validateData(durableTail),
    "durable tail beyond accepted tail was accepted",
  );

  const trimBeyondDurable = cloneFixture();
  const trimStatus = firstStatus(trimBeyondDurable);
  trimStatus.accepted_head = "1";
  trimStatus.accepted_tail = "1";
  trimStatus.pending_reclaim_records = 1;
  trimStatus.retained_records = 1;
  assertThrows(
    () => validateData(trimBeyondDurable),
    "accepted head beyond durable tail was accepted",
  );
});

Deno.test("validateData rejects corrupt physical byte accounting", () => {
  const fixture = cloneFixture();
  const physical = firstPhysical(fixture);
  physical.free_bytes = "127";

  assertThrows(
    () => validateData(fixture),
    "protected plus free bytes differing from capacity was accepted",
  );
});

Deno.test("validateData rejects physical capacity mismatch and out-of-range offsets", () => {
  const mismatchedCapacity = cloneFixture();
  const mismatchedPhysical = firstPhysical(mismatchedCapacity);
  mismatchedPhysical.data_capacity_bytes = "256";
  mismatchedPhysical.free_bytes = "256";
  assertThrows(
    () => validateData(mismatchedCapacity),
    "physical capacity differing from artifact configuration was accepted",
  );

  const invalidOffset = cloneFixture();
  const physical = firstPhysical(invalidOffset);
  physical.accepted_tail_offset = physical.data_capacity_bytes;
  assertThrows(
    () => validateData(invalidOffset),
    "physical offset equal to capacity was accepted",
  );

  const inconsistentEndpoint = cloneFixture();
  firstPhysical(inconsistentEndpoint).accepted_tail_offset = "1";
  assertThrows(
    () => validateData(inconsistentEndpoint),
    "accepted tail offset outside the protected interval was accepted",
  );

  const zeroGeneration = cloneFixture();
  firstPhysical(zeroGeneration).metadata_generation = "0";
  assertThrows(
    () => validateData(zeroGeneration),
    "zero metadata generation was accepted",
  );

  const nonCanonicalEmpty = cloneFixture();
  const emptyPhysical = firstPhysical(nonCanonicalEmpty);
  emptyPhysical.durable_head_offset = "1";
  emptyPhysical.durable_tail_offset = "1";
  emptyPhysical.accepted_tail_offset = "1";
  assertThrows(
    () => validateData(nonCanonicalEmpty),
    "empty durable interval with non-zero offsets was accepted",
  );
});

Deno.test("validateData rejects corrupt record and payload accounting", () => {
  const invalidLiveCursorSpan = cloneFixture();
  firstStatus(invalidLiveCursorSpan).accepted_tail = "1";
  assertThrows(
    () => validateData(invalidLiveCursorSpan),
    "accepted live count differing from its cursor interval was accepted",
  );

  const invalidPendingCursorSpan = cloneFixture();
  const pendingStatus = firstStatus(invalidPendingCursorSpan);
  pendingStatus.accepted_head = "1";
  pendingStatus.accepted_tail = "1";
  pendingStatus.durable_tail = "1";
  assertThrows(
    () => validateData(invalidPendingCursorSpan),
    "pending reclaim count differing from its cursor interval was accepted",
  );

  const invalidRecords = cloneFixture();
  const recordStatus = firstStatus(invalidRecords);
  recordStatus.retained_records = 1;
  assertThrows(
    () => validateData(invalidRecords),
    "retained record accounting mismatch was accepted",
  );

  const invalidPayload = cloneFixture();
  const payloadStatus = firstStatus(invalidPayload);
  payloadStatus.retained_payload_bytes = 1;
  assertThrows(
    () => validateData(invalidPayload),
    "retained payload accounting mismatch was accepted",
  );

  const retainedRecordLimit = cloneFixture();
  const retainedRecordStatus = firstStatus(retainedRecordLimit);
  retainedRecordStatus.accepted_head = "2";
  retainedRecordStatus.accepted_tail = "4";
  retainedRecordStatus.durable_tail = "4";
  retainedRecordStatus.accepted_live_records = 2;
  retainedRecordStatus.pending_reclaim_records = 2;
  retainedRecordStatus.retained_records = 4;
  assertThrows(
    () => validateData(retainedRecordLimit),
    "retained record count beyond the configured limit was accepted",
  );

  const retainedPayloadLimit = cloneFixture();
  const retainedPayloadStatus = retainedPayloadLimit.steps[4].status;
  retainedPayloadStatus.pending_reclaim_payload_bytes = 61;
  retainedPayloadStatus.retained_payload_bytes = 81;
  assertThrows(
    () => validateData(retainedPayloadLimit),
    "retained payload bytes beyond the configured limit were accepted",
  );

  const mismatchedStatusLimit = cloneFixture();
  firstStatus(mismatchedStatusLimit).max_live_records += 1;
  assertThrows(
    () => validateData(mismatchedStatusLimit),
    "status limit differing from artifact configuration was accepted",
  );
});
