import "./sbe-ir.js";
import "./storage-trace-sbe-ir.js";
import "./storage-trace-model.js";
import "./storage-trace-data.js";

const MODEL = globalThis.STORAGE_TRACE_MODEL;
const BYTES = globalThis.SbeIr.decodeBase64(globalThis.STORAGE_TRACE_DATA);
const SCHEMA = globalThis.SbeIr.parse(
  globalThis.SbeIr.decodeBase64(globalThis.StorageTraceSbeIr.base64),
);
const MAX_U64 = (1n << 64n) - 1n;

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

function assertArrayEquals(actual, expected, message = "arrays differ") {
  assert(Array.isArray(actual), `${message}: actual value is not an array`);
  assertEquals(actual.length, expected.length, `${message}: lengths differ`);
  actual.forEach((value, index) =>
    assertEquals(value, expected[index], `${message} at ${index}`)
  );
}

function assertThrows(action, fragment = null) {
  let thrown = null;
  try {
    action();
  } catch (error) {
    thrown = error;
  }
  assert(thrown instanceof Error, "expected operation to throw an Error");
  if (fragment !== null) {
    assert(
      thrown.message.includes(fragment),
      `expected error containing ${JSON.stringify(fragment)}, got ${
        JSON.stringify(thrown.message)
      }`,
    );
  }
  return thrown;
}

function decodedFixture() {
  return structuredClone(
    globalThis.SbeIr.decodeMessage(SCHEMA, BYTES, {
      requireExactLength: true,
    }).value,
  );
}

function changed(edit) {
  const bytes = BYTES.slice();
  edit(bytes);
  return bytes;
}

function view(bytes) {
  return new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
}

function setU16(bytes, offset, value) {
  view(bytes).setUint16(offset, value, true);
}

function setU32(bytes, offset, value) {
  view(bytes).setUint32(offset, value, true);
}

function setU64(bytes, offset, value) {
  view(bytes).setBigUint64(offset, value, true);
}

function binaryLayout(bytes) {
  const dataView = view(bytes);
  const groupOffset = 8 + 169;
  const blockLength = dataView.getUint16(groupOffset, true);
  const count = dataView.getUint16(groupOffset + 2, true);
  let cursor = groupOffset + 4;
  const steps = [];
  for (let index = 0; index < count; index += 1) {
    const fixed = cursor;
    cursor += blockLength;
    const variable = [];
    for (let field = 0; field < 9; field += 1) {
      const lengthOffset = cursor;
      const length = dataView.getUint16(lengthOffset, true);
      variable.push({ lengthOffset, dataOffset: lengthOffset + 2, length });
      cursor += 2 + length;
    }
    steps.push({ fixed, variable });
  }
  const provenance = [];
  for (let field = 0; field < 4; field += 1) {
    const lengthOffset = cursor;
    const length = dataView.getUint16(lengthOffset, true);
    provenance.push({ lengthOffset, dataOffset: lengthOffset + 2, length });
    cursor += 2 + length;
  }
  assertEquals(cursor, bytes.length, "binary layout must consume bundled data");
  return { groupOffset, blockLength, count, steps, provenance };
}

function enumValue(name, value) {
  return { name, value };
}

const LAYOUT = binaryLayout(BYTES);

Deno.test("bundled storage SBE trace decodes with exact normalized state", () => {
  const trace = MODEL.decodeBase64Artifact(globalThis.STORAGE_TRACE_DATA);
  assertEquals(trace.steps.length, 11);
  assertEquals(MODEL.initialSelectionIndex(trace), 2);
  assertEquals(trace.steps[2].operation, "sync");
  assertEquals(trace.steps[2].outcome, "failed");
  assertEquals(trace.steps[2].certainty, "may_have_applied");
  assertEquals(trace.steps[2]._outcome, "uncertain");
  assertEquals(trace.steps[3]._outcome, "crash");
  assertEquals(trace.steps[4]._outcome, "recovered");
  assertEquals(typeof trace.startedAtNs, "string");
  assertEquals(typeof trace.steps[0].before.acceptedLen, "string");
  assertEquals(typeof trace.steps[0]._before.acceptedLen, "bigint");
  assertEquals(typeof trace.steps[0]._before.inFlight, "number");
  assertEquals(typeof trace.steps[0]._before.faultHits, "bigint");
  assert(Array.isArray(trace.steps[0]._before.acceptedBytes));
  assertArrayEquals(trace._initial.acceptedBytes, [66, 65, 83, 69]);
  assertArrayEquals(
    trace._terminal.acceptedBytes,
    [66, 65, 83, 69, 45, 107, 101, 112, 116],
  );
  assertArrayEquals(
    trace._terminal.acceptedBytes,
    trace._terminal.durableBytes,
  );
  assert(trace.runtime.stopped);
  assertEquals(trace.runtime.nowNs, trace.completedAtNs);
});

Deno.test("validateData preserves full-width decoded uint64 values exactly", () => {
  const fixture = decodedFixture();
  const shift = (1n << 63n) + 17n;
  fixture.startedAtNs += shift;
  fixture.completedAtNs += shift;
  fixture.runtime.nowNs += shift;
  fixture.runtime.seed = MAX_U64 - 1n;
  for (const step of fixture.steps) {
    step.startedAtNs += shift;
    step.completedAtNs += shift;
  }
  const trace = MODEL.validateData(fixture);
  assertEquals(trace.runtime.seed, (MAX_U64 - 1n).toString(10));
  assertEquals(trace.startedAtNs, shift.toString(10));
  assertEquals(trace._startedAt, shift);
  assertEquals(trace._completedAt, shift + 32n);
});

Deno.test("decoder rejects required integer null sentinels", () => {
  // Offsets are fixed by StorageTraceArtifact block length 169 and the
  // RuntimeMetadata/StorageStatusSnapshot composites in schema version 0.
  assertThrows(
    () => MODEL.decodeArtifact(changed((bytes) => setU64(bytes, 105, MAX_U64))),
    "non-null uint64",
  );
  assertThrows(
    () =>
      MODEL.decodeArtifact(changed((bytes) => setU32(bytes, 92, 0xffff_ffff))),
  );
  assertThrows(
    () =>
      MODEL.decodeArtifact(
        changed((bytes) =>
          setU32(bytes, LAYOUT.steps[0].fixed + 64, 0xffff_ffff)
        ),
      ),
  );
});

Deno.test("base64 parsing is bounded and does not evaluate input", () => {
  const originalEval = globalThis.eval;
  globalThis.eval = () => {
    throw new Error("storage decoder must not evaluate artifact input");
  };
  try {
    assertEquals(
      MODEL.decodeBase64Artifact(globalThis.STORAGE_TRACE_DATA).steps.length,
      11,
    );
  } finally {
    globalThis.eval = originalEval;
  }
  assertThrows(() => MODEL.decodeBase64Artifact("not base64"), "base64");
  assertThrows(
    () => MODEL.decodeBase64Artifact("AAAA".repeat(MODEL.maxMessageBytes)),
    "bound",
  );
});

Deno.test("decoder rejects wrong SBE headers, truncation, and trailing bytes", () => {
  assertThrows(
    () => MODEL.decodeArtifact(changed((bytes) => setU16(bytes, 0, 168))),
    "block length",
  );
  assertThrows(
    () => MODEL.decodeArtifact(changed((bytes) => setU16(bytes, 2, 2))),
    "template ID",
  );
  assertThrows(
    () => MODEL.decodeArtifact(changed((bytes) => setU16(bytes, 4, 3))),
    "schema ID",
  );
  assertThrows(
    () => MODEL.decodeArtifact(changed((bytes) => setU16(bytes, 6, 1))),
    "schema version",
  );
  for (const cut of [0, 1, 7, 8, 176, BYTES.length - 1]) {
    assertThrows(() => MODEL.decodeArtifact(BYTES.slice(0, cut)));
  }
  const trailing = new Uint8Array(BYTES.length + 1);
  trailing.set(BYTES);
  trailing[BYTES.length] = 0xa5;
  assertThrows(() => MODEL.decodeArtifact(trailing), "trailing");
});

Deno.test("decoder rejects enum, group, and variable-data corruption", () => {
  assertThrows(
    () =>
      MODEL.decodeArtifact(changed((bytes) => {
        bytes[LAYOUT.steps[0].fixed + 20] = 0xfe;
      })),
    "enum",
  );
  assertThrows(
    () =>
      MODEL.decodeArtifact(changed((bytes) => {
        setU16(bytes, LAYOUT.groupOffset + 2, 65);
      })),
    "64",
  );
  assertThrows(
    () =>
      MODEL.decodeArtifact(changed((bytes) => {
        setU16(bytes, LAYOUT.groupOffset, LAYOUT.blockLength - 1);
      })),
  );
  assertThrows(
    () =>
      MODEL.decodeArtifact(changed((bytes) => {
        setU16(bytes, LAYOUT.steps[0].variable[3].lengthOffset, 4_097);
      })),
    "4096",
  );
});

Deno.test("model rejects corrupt artifact and provenance metadata", () => {
  assertThrows(
    () => MODEL.decodeArtifact(changed((bytes) => setU32(bytes, 8, 2))),
    "artifact schema",
  );
  for (
    const edit of [
      (fixture) => fixture.scenario = "",
      (fixture) => fixture.sourceTest = "x".repeat(513),
      (fixture) => fixture.provider = "OtherStorage",
      (fixture) => fixture.generator = "",
    ]
  ) {
    const fixture = decodedFixture();
    edit(fixture);
    assertThrows(() => MODEL.validateData(fixture));
  }
});

Deno.test("model rejects inconsistent config and terminal runtime metadata", () => {
  for (
    const edit of [
      (fixture) =>
        fixture.config.maxReadBytes = fixture.config.maxFileBytes + 1n,
      (fixture) => fixture.config.maxInFlight = 0n,
      (fixture) => fixture.runtime.reproductionSchema = 0,
      (fixture) => fixture.runtime.stopped = 0,
      (fixture) => fixture.runtime.nowNs += 1n,
      (fixture) => fixture.runtime.liveTasks = 1n,
      (fixture) => fixture.runtime.totalSteps = 1n,
    ]
  ) {
    const fixture = decodedFixture();
    edit(fixture);
    assertThrows(() => MODEL.validateData(fixture));
  }
});

Deno.test("model rejects corrupt sequence and timing", () => {
  for (
    const edit of [
      (fixture) => fixture.steps[1].sequence = 0,
      (fixture) => fixture.steps[2].startedAtNs += 1n,
      (fixture) =>
        fixture.steps[2].completedAtNs = fixture.steps[2].startedAtNs - 1n,
      (fixture) => fixture.completedAtNs += 1n,
      (fixture) => fixture.steps = [],
    ]
  ) {
    const fixture = decodedFixture();
    edit(fixture);
    assertThrows(() => MODEL.validateData(fixture));
  }
});

Deno.test("model rejects byte, boolean, session, and status-count corruption", () => {
  for (
    const edit of [
      (fixture) => fixture.steps[0].requestBytes = new Uint8Array(4_097),
      (fixture) => fixture.steps[0].before.acceptedLen += 1n,
      (fixture) => fixture.steps[0].before.hasFsyncGatedData = 2,
      (fixture) => fixture.steps[0].before.session = enumValue("Closed", 2),
      (fixture) => fixture.steps[0].before.inFlight = 1n,
      (fixture) =>
        fixture.steps[0].before.inFlightLimit =
          BigInt(Number.MAX_SAFE_INTEGER) + 1n,
      (fixture) =>
        fixture.steps[0].before.pendingFaults =
          fixture.config.maxScriptedFaults + 1n,
    ]
  ) {
    const fixture = decodedFixture();
    edit(fixture);
    assertThrows(() => MODEL.validateData(fixture));
  }
});

Deno.test("model rejects state discontinuity and inconsistent terminal state", () => {
  const discontinuous = decodedFixture();
  discontinuous.steps[1].acceptedBytesBefore[0] ^= 0xff;
  assertThrows(() => MODEL.validateData(discontinuous), "discontinuous");

  const terminal = decodedFixture();
  terminal.steps.at(-1).after.session = enumValue("Closed", 2);
  terminal.steps.at(-1).after.closed = 1;
  assertThrows(() => MODEL.validateData(terminal));
});

Deno.test("model rejects contradictory operation, outcome, and certainty", () => {
  for (
    const edit of [
      (fixture) => fixture.steps[0].operation = enumValue("ReadAt", 3),
      (fixture) => fixture.steps[0].outcome = enumValue("Failed", 2),
      (fixture) => fixture.steps[2].certainty = enumValue("NotApplied", 1),
      (fixture) => fixture.steps[3].outcome = enumValue("Success", 0),
      (fixture) => fixture.steps[4].certainty = enumValue("Applied", 2),
    ]
  ) {
    const fixture = decodedFixture();
    edit(fixture);
    assertThrows(() => MODEL.validateData(fixture));
  }
});

Deno.test("model independently rejects every invalid storage transition", () => {
  const cases = [
    (fixture) => fixture.steps[0].acceptedBytesAfter[4] ^= 0xff,
    (fixture) => fixture.steps[1].after.pendingFaults = 0n,
    (fixture) =>
      fixture.steps[2].durableBytesAfter = new Uint8Array([
        ...fixture.steps[2].durableBytesAfter,
        0,
      ]),
    (fixture) =>
      fixture.steps[3].acceptedBytesAfter =
        fixture.steps[3].acceptedBytesBefore,
    (fixture) => fixture.steps[4].after.faultHits = 1n,
    (fixture) => fixture.steps[6].durableBytesAfter[0] ^= 0xff,
    (fixture) => fixture.steps[10].resultBytes[0] ^= 0xff,
  ];
  for (const edit of cases) {
    const fixture = decodedFixture();
    edit(fixture);
    assertThrows(() => MODEL.validateData(fixture));
  }
});

Deno.test("model enforces full read and write request admission bounds", () => {
  const oversizedWrite = decodedFixture();
  oversizedWrite.config.maxWriteBytes = 6n;
  oversizedWrite.config.maxWriteChunk = 6n;
  oversizedWrite.steps[0].requestBytes = new Uint8Array([
    ...oversizedWrite.steps[0].requestBytes,
    0x78,
  ]);
  oversizedWrite.steps[0].resultBytes = oversizedWrite.steps[0].requestBytes
    .slice();
  oversizedWrite.steps[0].resultLength = 7n;
  assertThrows(() => MODEL.validateData(oversizedWrite), "request");

  const outOfRangeWrite = decodedFixture();
  outOfRangeWrite.config.maxWriteBytes = 32n;
  const suffix = new Uint8Array(23).fill(0x78);
  outOfRangeWrite.steps[0].requestBytes = new Uint8Array([
    ...outOfRangeWrite.steps[0].requestBytes,
    ...suffix,
  ]);
  outOfRangeWrite.steps[0].resultBytes = outOfRangeWrite.steps[0].requestBytes
    .slice();
  outOfRangeWrite.steps[0].resultLength = 29n;
  assertThrows(() => MODEL.validateData(outOfRangeWrite), "full request range");

  const oversizedRead = decodedFixture();
  oversizedRead.config.maxReadBytes = 8n;
  oversizedRead.config.maxReadChunk = 8n;
  const read = oversizedRead.steps.at(-1);
  read.transferLength = 8n;
  read.resultLength = 9n;
  read.resultBytes = new Uint8Array([
    ...read.acceptedBytesBefore.slice(0, 8),
    read.requestBytes[8],
  ]);
  assertThrows(() => MODEL.validateData(oversizedRead), "request");
});

Deno.test("model accepts a partial read with its full owned buffer", () => {
  const fixture = decodedFixture();
  fixture.config.maxReadChunk = 8n;
  const read = fixture.steps.at(-1);
  read.transferLength = 8n;
  read.resultLength = 9n;
  read.resultBytes = new Uint8Array([
    ...read.acceptedBytesBefore.slice(0, 8),
    read.requestBytes[8],
  ]);

  const trace = MODEL.validateData(fixture);
  assertEquals(trace.steps.at(-1).transferLength, "8");
  assertEquals(trace.steps.at(-1).resultLength, "9");
  assertArrayEquals(
    trace.steps.at(-1).resultBytes,
    Array.from(read.resultBytes),
  );
});

Deno.test("storage page composes generic SBE, shared UI, and bounded domain rendering", async () => {
  const [html, app, styles] = await Promise.all([
    Deno.readTextFile(new URL("./storage-trace.html", import.meta.url)),
    Deno.readTextFile(new URL("./storage-trace-viewer.js", import.meta.url)),
    Deno.readTextFile(new URL("./storage-trace.css", import.meta.url)),
  ]);
  const scripts = [
    "trace-viewer-core.js",
    "sbe-ir.js",
    "storage-trace-sbe-ir.js",
    "storage-trace-model.js",
    "storage-trace-data.js",
    "trace-viewer-ui.js",
    "storage-trace-viewer.js",
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
  for (const stylesheet of ["trace-viewer.css", "storage-trace.css"]) {
    assert(
      html.includes(`<link rel="stylesheet" href="./${stylesheet}">`),
      `page is missing ${stylesheet}`,
    );
  }
  for (
    const id of [
      "timeline",
      "storage-before",
      "storage-after",
      "length-chart",
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
      "renderJsonDump(elements.detailFields, rawFields(step))",
      "parseText: decodeArtifact",
      "readFile: (file) => file.arrayBuffer()",
      "maxFileBytes: maxMessageBytes",
      "MAX_VISIBLE_BYTE_SLOTS = 16",
    ]
  ) {
    assert(app.includes(integration), `storage app is missing ${integration}`);
  }
  assert(
    /<svg\s+id="timeline"\s+role="group"/.test(html),
    "interactive timeline must expose its descendant controls",
  );
  assert(
    html.includes('class="detail-context"'),
    "variable detail text must render inside the stable shared context",
  );
  assert(
    html.includes('accept=".sbe,application/octet-stream"'),
    "storage loader must advertise binary SBE artifacts",
  );
  assert(
    styles.includes("overflow-x: auto") &&
      styles.includes("scrollbar-gutter: stable"),
    "bounded byte grids must not widen the page",
  );
  assert(
    styles.indexOf(".byte-cell.dirty") >
      styles.indexOf(".byte-cell.changed"),
    "dirty bytes must retain their durability warning over change styling",
  );
  assert(
    !app.includes("innerHTML") && !app.includes("insertAdjacentHTML"),
    "storage app must not inject trace HTML",
  );
});

Deno.test("storage model source contains no executable parsing or unsafe DOM sinks", async () => {
  const source = await Deno.readTextFile(
    new URL("./storage-trace-model.js", import.meta.url),
  );
  for (
    const forbidden of [
      /\beval\s*\(/,
      /\bFunction\s*\(/,
      /innerHTML\s*=/,
      /insertAdjacentHTML\s*\(/,
      /document\./,
    ]
  ) {
    assert(!forbidden.test(source), `model contains forbidden ${forbidden}`);
  }
});
