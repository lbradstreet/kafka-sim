import "./sbe-ir.js";
import "./dst-trace-sbe-ir.js";
import "./sbe-decoder.js";
import "./dst-trace-sample.js";

const SBE_URL = new URL(
  "./testdata/browser-sbe-c1-s1v1-a8-t5.sbe",
  import.meta.url,
);
const GOLDEN = await Deno.readFile(SBE_URL);
const DECODER = globalThis.DstTraceSbe;
const EMBEDDED_SAMPLE = globalThis.SbeIr.decodeBase64(
  globalThis.DstTraceSample.base64,
);
const IR_SCHEMA = globalThis.SbeIr.parse(
  globalThis.SbeIr.decodeBase64(globalThis.DstTraceSbeIr.base64),
);
const HEADER_OFFSETS = Object.freeze({
  artifactSchema: 0,
  maxTimers: 36,
  maxTimePresent: 52,
  maxTimeNs: 53,
  eventCount: 61,
  retentionMode: 69,
  capacityUnit: 70,
  prefixCapacity: 71,
  tailCapacity: 79,
  retainedBytes: 87,
  samplingMode: 95,
  samplingAlgorithm: 96,
  samplingPeriod: 100,
  samplingPhase: 108,
  fingerprintScope: 116,
  lastSequencePresent: 125,
  orderingPresent: 134,
  previousSequence: 135,
  rejectedSequence: 143,
  traceSchema: 12,
  readyTasks: 199,
  liveTimers: 207,
  liveTasks: 215,
  stopped: 223,
  nowNs: 159,
  randomCount: 224,
  taskCount: 228,
  startTimeNs: 232,
});
const EXPECTED_TEMPLATES = Object.freeze([
  [2, 24, "random_stream_state"],
  [4, 240, "artifact_header"],
  [100, 24, "runtime_started"],
  [5, 9, "task_snapshot"],
  [102, 32, "task_enqueued"],
  [103, 24, "task_poll_started"],
  [104, 24, "task_pending"],
  [105, 24, "task_completed"],
  [106, 25, "task_cancelled"],
  [107, 25, "task_panicked"],
  [108, 25, "task_drop_panicked"],
  [109, 25, "waker_panicked"],
  [110, 40, "timer_scheduled"],
  [111, 32, "timer_fired"],
  [112, 32, "timer_cancelled"],
  [113, 32, "time_advanced"],
  [114, 24, "runtime_stalled"],
  [115, 24, "budget_exhausted"],
  [116, 16, "runtime_stopped"],
  [117, 48, "random_choice"],
  [118, 56, "random_choice"],
  [119, 64, "random_choice"],
  [120, 33, "task_spawned"],
]);
const EXPECTED_EVENT_TYPES = Object.freeze([
  "budget_exhausted",
  "random_choice",
  "runtime_stalled",
  "runtime_started",
  "runtime_stopped",
  "task_cancelled",
  "task_completed",
  "task_drop_panicked",
  "task_enqueued",
  "task_panicked",
  "task_pending",
  "task_poll_started",
  "task_spawned",
  "time_advanced",
  "timer_cancelled",
  "timer_fired",
  "timer_scheduled",
  "waker_panicked",
]);

function fail(message) {
  throw new Error(message);
}

function assert(condition, message) {
  if (!condition) fail(message);
}

function canonical(value) {
  if (Array.isArray(value)) return value.map(canonical);
  if (value !== null && typeof value === "object") {
    return Object.fromEntries(
      Object.keys(value).sort().map((key) => [key, canonical(value[key])]),
    );
  }
  return value;
}

function assertEquals(actual, expected, message) {
  const actualText = JSON.stringify(canonical(actual));
  const expectedText = JSON.stringify(canonical(expected));
  if (actualText !== expectedText) {
    fail(`${message}\nexpected: ${expectedText}\nactual:   ${actualText}`);
  }
}

function assertNoBigInt(value, path = "root") {
  if (typeof value === "bigint") {
    fail(`BigInt escaped normalization at ${path}`);
  }
  if (Array.isArray(value)) {
    value.forEach((entry, index) => assertNoBigInt(entry, `${path}[${index}]`));
  } else if (value !== null && typeof value === "object") {
    for (const [key, entry] of Object.entries(value)) {
      assertNoBigInt(entry, `${path}.${key}`);
    }
  }
}

function view(bytes) {
  return new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
}

function readU16(bytes, offset) {
  return view(bytes).getUint16(offset, true);
}

function readU32(bytes, offset) {
  return view(bytes).getUint32(offset, true);
}

function readU64(bytes, offset) {
  return view(bytes).getBigUint64(offset, true);
}

function setU8(bytes, offset, expected, replacement, label) {
  assert(
    bytes[offset] === expected,
    `${label}: expected original uint8 ${expected}`,
  );
  bytes[offset] = replacement;
}

function setU16(bytes, offset, expected, replacement, label) {
  assert(
    readU16(bytes, offset) === expected,
    `${label}: expected original uint16 ${expected}`,
  );
  view(bytes).setUint16(offset, replacement, true);
}

function setU32(bytes, offset, expected, replacement, label) {
  assert(
    readU32(bytes, offset) === expected,
    `${label}: expected original uint32 ${expected}`,
  );
  view(bytes).setUint32(offset, replacement, true);
}

function setU64(bytes, offset, expected, replacement, label) {
  assert(
    readU64(bytes, offset) === expected,
    `${label}: expected original uint64 ${expected}`,
  );
  view(bytes).setBigUint64(offset, replacement, true);
}

function parseFrames(bytes) {
  const frames = [];
  let offset = 16;
  while (offset < bytes.length) {
    const length = readU32(bytes, offset);
    assert(
      length >= 12,
      `golden frame at ${offset} is shorter than its headers`,
    );
    assert(
      offset + length <= bytes.length,
      `golden frame at ${offset} exceeds the file`,
    );
    frames.push(Object.freeze({
      offset,
      length,
      end: offset + length,
      blockLength: readU16(bytes, offset + 4),
      templateId: readU16(bytes, offset + 6),
      body: offset + 12,
    }));
    offset += length;
  }
  assert(offset === bytes.length, "golden frames consume the exact file");
  return Object.freeze(frames);
}

const FRAMES = parseFrames(GOLDEN);
const HEADER_FRAME = FRAMES[0];
const RANDOM_FRAMES = Object.freeze(FRAMES.slice(1, 6));
const TASK_FRAMES = Object.freeze(FRAMES.slice(6, 9));
const EVENT_FRAMES = Object.freeze(FRAMES.slice(9));

function frameForTemplate(templateId, occurrence = 0) {
  const matches = FRAMES.filter((frame) => frame.templateId === templateId);
  assert(
    matches.length > occurrence,
    `golden contains template ${templateId} occurrence ${occurrence}`,
  );
  return matches[occurrence];
}

function changed(edit) {
  const bytes = GOLDEN.slice();
  edit(bytes);
  return bytes;
}

function withInternalTrailingByte(frame) {
  const bytes = new Uint8Array(GOLDEN.length + 1);
  bytes.set(GOLDEN.subarray(0, frame.end));
  bytes[frame.end] = 0xa5;
  bytes.set(GOLDEN.subarray(frame.end), frame.end + 1);
  view(bytes).setUint32(frame.offset, frame.length + 1, true);
  return bytes;
}

function expectRejected(bytes, label, fragment = null) {
  let thrown = null;
  try {
    DECODER.decodeArtifact(bytes);
  } catch (error) {
    thrown = error;
  }
  assert(thrown instanceof Error, `${label}: corrupted artifact was accepted`);
  if (fragment !== null) {
    assert(
      thrown.message.includes(fragment),
      `${label}: expected error containing ${JSON.stringify(fragment)}, got ${
        JSON.stringify(thrown.message)
      }`,
    );
  }
}

function expectLimitRejected(limits, label, fragment) {
  let thrown = null;
  try {
    DECODER.decodeArtifact(GOLDEN, limits);
  } catch (error) {
    thrown = error;
  }
  assert(thrown instanceof Error, `${label}: bounded decode was accepted`);
  assert(
    thrown.message.includes(fragment),
    `${label}: expected error containing ${JSON.stringify(fragment)}, got ${
      JSON.stringify(thrown.message)
    }`,
  );
}

Deno.test("generated embedded sample is a coherent Rust runtime trace", () => {
  const decoded = DECODER.decodeArtifact(EMBEDDED_SAMPLE);
  assertNoBigInt(decoded);
  assertEquals(decoded.notices, [], "embedded sample has decoder notices");
  assertEquals(decoded.header.random.map((entry) => entry.stream).sort(), [
    "debug",
    "fault",
    "scenario",
    "schedule",
    "workload",
  ], "embedded sample does not contain all runtime random streams");
  assertEquals(
    Object.fromEntries(
      decoded.header.random.map((entry) => [entry.stream, entry.draws]),
    ),
    {
      debug: "0",
      fault: "0",
      scenario: "0",
      schedule: "0",
      workload: "1",
    },
    "embedded sample random checkpoints differ from the runtime execution",
  );
  const choices = decoded.events.filter((event) =>
    event.type === "random_choice"
  );
  assertEquals(choices.map((event) => ({
    stream: event.fields.stream,
    choice: event.fields.choice,
    draws_before: event.fields.draws_before,
    draws_after: event.fields.draws_after,
  })), [{
    stream: "workload",
    choice: "below",
    draws_before: "0",
    draws_after: "1",
  }], "embedded sample random-choice history is inconsistent");
});

Deno.test("browser decoder matches the exhaustive Rust golden", () => {
  const decoded = DECODER.decodeArtifact(GOLDEN);
  const coordinateStem =
    `browser-sbe-c${DECODER.schema.containerVersion}-s${DECODER.schema.schemaId}v${DECODER.schema.schemaVersion}-a${DECODER.schema.artifactSchema}-t${DECODER.schema.traceSchema}`;
  assert(
    SBE_URL.pathname.endsWith(`/${coordinateStem}.sbe`),
    "SBE golden filename does not match live schema coordinates",
  );
  assertEquals(decoded.notices, [], "valid golden has no decoder notices");
  assertNoBigInt(decoded);

  assertEquals(
    DECODER.schema.templates.map((
      { id, blockLength, kind },
    ) => [id, blockLength, kind]),
    EXPECTED_TEMPLATES,
    "public browser schema registry differs",
  );
  assertEquals(
    DECODER.schema.templates.map(({ id, blockLength }) => [id, blockLength]),
    IR_SCHEMA.messages.map(({ id, blockLength }) => [id, blockLength]),
    "trace adapter template coordinates do not come from the generated IR",
  );
  assertEquals(
    [DECODER.schema.schemaId, DECODER.schema.schemaVersion],
    [IR_SCHEMA.id, IR_SCHEMA.version],
    "trace adapter schema coordinates do not come from the generated IR",
  );
  assertEquals(
    [...new Set(EVENT_FRAMES.map((frame) => frame.templateId))].sort((
      left,
      right,
    ) => left - right),
    [100, ...Array.from({ length: 19 }, (_unused, index) => 102 + index)],
    "raw fixture does not cover every event template",
  );
  assertEquals(
    [...new Set(decoded.events.map((event) => event.type))].sort(),
    EXPECTED_EVENT_TYPES,
    "normalized fixture does not cover every event type",
  );
  assertEquals(
    decoded.events.filter((event) => event.type === "task_cancelled").map(
      (event) => event.fields.reason,
    ).sort(),
    ["block_on_failure", "explicit_abort", "runtime_stopped"],
    "fixture cancellation reasons differ",
  );
  assertEquals(
    decoded.events.filter((event) => event.type === "random_choice").map(
      (event) => event.fields.choice,
    ).sort(),
    ["below", "bool_ratio", "u64"],
    "fixture random-choice shapes differ",
  );
  assertEquals(decoded.header.random.map((entry) => entry.stream).sort(), [
    "debug",
    "fault",
    "scenario",
    "schedule",
    "workload",
  ], "fixture random streams differ");
  assertEquals(decoded.header.tasks.map((task) => task.state).sort(), [
    "ready",
    "running",
    "waiting",
  ], "fixture task states differ");

  const maximum = "18446744073709551615";
  assert(decoded.header.seed === maximum, "header seed lost uint64 precision");
  assert(
    decoded.header.last_sequence === maximum,
    "last sequence lost uint64 precision",
  );
  assert(
    decoded.events.at(-1).sequence === maximum,
    "event sequence lost uint64 precision",
  );
  assert(
    decoded.events[0].sequence === "18446744073709551593",
    "first high sequence changed",
  );
  assert(
    decoded.header.driver.startsWith("\u{feff}"),
    "driver BOM was stripped",
  );
  assert(
    decoded.header.outcome.startsWith("\u{feff}"),
    "outcome BOM was stripped",
  );
  const panic = decoded.events.find((event) => event.type === "task_panicked");
  assert(
    panic.fields.message.startsWith("\u{feff}"),
    "panic-message BOM was stripped",
  );
  assert(
    decoded.header.capacity_unit === "bytes",
    "fixture must cover byte retention",
  );
  assert(
    decoded.header.retention.mode === "prefix_and_tail",
    "fixture must cover prefix-plus-tail retention",
  );
  assert(
    decoded.header.sampling?.period === "1",
    "fixture must cover sampling metadata",
  );
  assert(
    decoded.header.ordering_violation?.rejected_sequence === maximum,
    "fixture must cover ordering metadata",
  );
});

Deno.test("browser decoder applies caller bounds before materializing records", () => {
  expectLimitRejected(
    { maxTaskSnapshots: TASK_FRAMES.length - 1 },
    "task presentation bound",
    "task count 3 exceeds decode limit 2",
  );
  expectLimitRejected(
    { maxEvents: EVENT_FRAMES.length - 1 },
    "event presentation bound",
    `event count ${EVENT_FRAMES.length} exceeds decode limit ${
      EVENT_FRAMES.length - 1
    }`,
  );

  const decoded = DECODER.decodeArtifact(GOLDEN, {
    maxTaskSnapshots: TASK_FRAMES.length,
    maxEvents: EVENT_FRAMES.length,
  });
  assertEquals(
    decoded.header.tasks.length,
    TASK_FRAMES.length,
    "task bound is inclusive",
  );
  assertEquals(
    decoded.events.length,
    EVENT_FRAMES.length,
    "event bound is inclusive",
  );
});

Deno.test("browser decoder rejects every truncation and trailing file data", () => {
  for (let end = 0; end < GOLDEN.length; end += 1) {
    expectRejected(GOLDEN.subarray(0, end), `truncation at byte ${end}`);
  }
  const trailing = new Uint8Array(GOLDEN.length + 1);
  trailing.set(GOLDEN);
  trailing[GOLDEN.length] = 0xa5;
  expectRejected(trailing, "appended trailing byte", "trailing data");
});

Deno.test("browser decoder rejects corrupt framing and schema coordinates", () => {
  const cases = [
    [
      "bad magic",
      changed((bytes) => setU8(bytes, 0, 0x44, 0x58, "magic")),
      "bad container magic",
    ],
    [
      "container version",
      changed((bytes) => setU16(bytes, 8, 1, 2, "container version")),
      "container version",
    ],
    [
      "container flags",
      changed((bytes) => setU16(bytes, 10, 0, 1, "container flags")),
      "container flags",
    ],
    [
      "preamble length",
      changed((bytes) => setU32(bytes, 12, 16, 15, "preamble length")),
      "container header length",
    ],
    [
      "short frame",
      changed((bytes) =>
        setU32(
          bytes,
          HEADER_FRAME.offset,
          HEADER_FRAME.length,
          11,
          "short header frame",
        )
      ),
      "outside",
    ],
    [
      "shortened header frame",
      changed((bytes) =>
        setU32(
          bytes,
          HEADER_FRAME.offset,
          HEADER_FRAME.length,
          HEADER_FRAME.length - 1,
          "shortened header frame",
        )
      ),
      null,
    ],
    [
      "oversized frame",
      changed((bytes) =>
        setU32(
          bytes,
          HEADER_FRAME.offset,
          HEADER_FRAME.length,
          16 * 1024 * 1024 + 1,
          "oversized frame",
        )
      ),
      "outside",
    ],
    [
      "wrong header template",
      changed((bytes) =>
        setU16(bytes, HEADER_FRAME.offset + 6, 4, 2, "header template")
      ),
      "template ID",
    ],
    [
      "wrong random template",
      changed((bytes) =>
        setU16(bytes, RANDOM_FRAMES[0].offset + 6, 2, 3, "random template")
      ),
      "template ID",
    ],
    [
      "wrong task template",
      changed((bytes) =>
        setU16(bytes, TASK_FRAMES[0].offset + 6, 5, 2, "task template")
      ),
      "template ID",
    ],
    [
      "unknown low event template",
      changed((bytes) =>
        setU16(bytes, EVENT_FRAMES[0].offset + 6, 100, 99, "low event template")
      ),
      "not a known event template",
    ],
    [
      "unknown high event template",
      changed((bytes) =>
        setU16(
          bytes,
          EVENT_FRAMES[0].offset + 6,
          100,
          121,
          "high event template",
        )
      ),
      "not a known event template",
    ],
    [
      "unsupported artifact schema",
      changed((bytes) =>
        setU32(
          bytes,
          HEADER_FRAME.body + HEADER_OFFSETS.artifactSchema,
          8,
          7,
          "artifact schema",
        )
      ),
      "artifact schema",
    ],
    [
      "unsupported trace schema",
      changed((bytes) =>
        setU32(
          bytes,
          HEADER_FRAME.body + HEADER_OFFSETS.traceSchema,
          5,
          6,
          "trace schema",
        )
      ),
      "trace schema",
    ],
    [
      "header internal trailing byte",
      withInternalTrailingByte(HEADER_FRAME),
      "trailing bytes inside artifact header",
    ],
    [
      "task internal trailing byte",
      withInternalTrailingByte(TASK_FRAMES[0]),
      "trailing bytes inside task snapshot",
    ],
    [
      "panic internal trailing byte",
      withInternalTrailingByte(frameForTemplate(107)),
      "trailing bytes inside panic event",
    ],
    [
      "fixed event internal trailing byte",
      withInternalTrailingByte(frameForTemplate(100)),
      "unexpected message length",
    ],
  ];
  for (const frame of FRAMES) {
    cases.push([
      `template ${frame.templateId} block length`,
      changed((bytes) =>
        setU16(
          bytes,
          frame.offset + 4,
          frame.blockLength,
          frame.blockLength + 1,
          `template ${frame.templateId} block length`,
        )
      ),
      "block length",
    ]);
    cases.push([
      `template ${frame.templateId} schema ID`,
      changed((bytes) =>
        setU16(
          bytes,
          frame.offset + 8,
          1,
          2,
          `template ${frame.templateId} schema ID`,
        )
      ),
      "schema ID",
    ]);
    cases.push([
      `template ${frame.templateId} schema version`,
      changed((bytes) =>
        setU16(
          bytes,
          frame.offset + 10,
          1,
          2,
          `template ${frame.templateId} schema version`,
        )
      ),
      "schema version",
    ]);
  }
  for (const [label, bytes, fragment] of cases) {
    expectRejected(bytes, label, fragment);
  }
});

Deno.test("browser decoder rejects invalid UTF-8, enum tags, flags, and options", () => {
  const task = TASK_FRAMES[0];
  const spawned = frameForTemplate(120);
  const spawnedWithoutParent = frameForTemplate(120, 1);
  const cancelled = frameForTemplate(106);
  const panicked = frameForTemplate(107);
  const randomChoice = frameForTemplate(117);
  const headerVariable = HEADER_FRAME.body + HEADER_FRAME.blockLength;
  const driverData = headerVariable + 4;
  const panicLength = panicked.body + panicked.blockLength;
  const panicData = panicLength + 4;
  const cases = [
    [
      "driver UTF-8",
      changed((bytes) => setU8(bytes, driverData, 0xef, 0xff, "driver UTF-8")),
      "not valid UTF-8",
    ],
    [
      "panic UTF-8",
      changed((bytes) => setU8(bytes, panicData, 0xef, 0xff, "panic UTF-8")),
      "not valid UTF-8",
    ],
    [
      "panic-message bound",
      changed((bytes) => setU32(bytes, panicLength, 14, 4097, "panic length")),
      "panic message exceeds",
    ],
    [
      "retention mode",
      changed((bytes) =>
        setU8(
          bytes,
          HEADER_FRAME.body + HEADER_OFFSETS.retentionMode,
          2,
          3,
          "retention mode",
        )
      ),
      "retention mode",
    ],
    [
      "capacity unit",
      changed((bytes) =>
        setU8(
          bytes,
          HEADER_FRAME.body + HEADER_OFFSETS.capacityUnit,
          1,
          2,
          "capacity unit",
        )
      ),
      "capacity unit",
    ],
    [
      "sampling mode",
      changed((bytes) =>
        setU8(
          bytes,
          HEADER_FRAME.body + HEADER_OFFSETS.samplingMode,
          1,
          2,
          "sampling mode",
        )
      ),
      "sampling mode",
    ],
    [
      "noncanonical disabled sampling",
      changed((bytes) =>
        setU8(
          bytes,
          HEADER_FRAME.body + HEADER_OFFSETS.samplingMode,
          1,
          0,
          "disabled sampling",
        )
      ),
      "disabled sampling metadata",
    ],
    [
      "sampling algorithm",
      changed((bytes) =>
        setU32(
          bytes,
          HEADER_FRAME.body + HEADER_OFFSETS.samplingAlgorithm,
          1,
          2,
          "sampling algorithm",
        )
      ),
      "sampling metadata",
    ],
    [
      "zero sampling period",
      changed((bytes) =>
        setU64(
          bytes,
          HEADER_FRAME.body + HEADER_OFFSETS.samplingPeriod,
          1n,
          0n,
          "sampling period",
        )
      ),
      "sampling metadata",
    ],
    [
      "sampling phase",
      changed((bytes) =>
        setU64(
          bytes,
          HEADER_FRAME.body + HEADER_OFFSETS.samplingPhase,
          0n,
          1n,
          "sampling phase",
        )
      ),
      "sampling metadata",
    ],
    [
      "start time exceeds the terminal instant",
      changed((bytes) => {
        setU64(
          bytes,
          HEADER_FRAME.body + HEADER_OFFSETS.nowNs,
          0xffff_ffff_ffff_ffffn,
          0n,
          "terminal instant",
        );
        setU64(
          bytes,
          HEADER_FRAME.body + HEADER_OFFSETS.startTimeNs,
          0n,
          1n,
          "start time",
        );
      }),
      "start time",
    ],
    [
      "fingerprint scope",
      changed((bytes) =>
        setU8(
          bytes,
          HEADER_FRAME.body + HEADER_OFFSETS.fingerprintScope,
          1,
          2,
          "fingerprint scope",
        )
      ),
      "sampling metadata",
    ],
    [
      "max-time presence",
      changed((bytes) =>
        setU8(
          bytes,
          HEADER_FRAME.body + HEADER_OFFSETS.maxTimePresent,
          1,
          2,
          "max-time presence",
        )
      ),
      "presence",
    ],
    [
      "absent max time carries data",
      changed((bytes) =>
        setU8(
          bytes,
          HEADER_FRAME.body + HEADER_OFFSETS.maxTimePresent,
          1,
          0,
          "absent max time",
        )
      ),
      "absent max time",
    ],
    [
      "last-sequence presence",
      changed((bytes) =>
        setU8(
          bytes,
          HEADER_FRAME.body + HEADER_OFFSETS.lastSequencePresent,
          1,
          2,
          "last-sequence presence",
        )
      ),
      "presence",
    ],
    [
      "absent last sequence carries data",
      changed((bytes) =>
        setU8(
          bytes,
          HEADER_FRAME.body + HEADER_OFFSETS.lastSequencePresent,
          1,
          0,
          "absent last sequence",
        )
      ),
      "absent last sequence",
    ],
    [
      "ordering presence",
      changed((bytes) =>
        setU8(
          bytes,
          HEADER_FRAME.body + HEADER_OFFSETS.orderingPresent,
          1,
          2,
          "ordering presence",
        )
      ),
      "presence",
    ],
    [
      "absent ordering carries sequences",
      changed((bytes) =>
        setU8(
          bytes,
          HEADER_FRAME.body + HEADER_OFFSETS.orderingPresent,
          1,
          0,
          "absent ordering",
        )
      ),
      "absent ordering violation",
    ],
    [
      "stopped flag",
      changed((bytes) =>
        setU8(
          bytes,
          HEADER_FRAME.body + HEADER_OFFSETS.stopped,
          0,
          2,
          "stopped flag",
        )
      ),
      "stopped",
    ],
    [
      "task state",
      changed((bytes) => setU8(bytes, task.body + 8, 0, 3, "task state")),
      "task state",
    ],
    [
      "parent presence",
      changed((bytes) =>
        setU8(bytes, spawned.body + 24, 1, 2, "parent presence")
      ),
      "parent presence",
    ],
    [
      "absent parent carries bytes",
      changed((bytes) =>
        setU8(bytes, spawnedWithoutParent.body + 25, 0, 1, "absent parent")
      ),
      "absent task identifier parent",
    ],
    [
      "cancellation reason",
      changed((bytes) =>
        setU8(bytes, cancelled.body + 24, 0, 3, "cancellation reason")
      ),
      "cancellation reason",
    ],
    [
      "panic truncation",
      changed((bytes) =>
        setU8(bytes, panicked.body + 24, 0, 2, "panic truncation")
      ),
      "message truncation",
    ],
    [
      "metadata stream tag",
      changed((bytes) =>
        setU64(
          bytes,
          RANDOM_FRAMES[0].body,
          0x5343_4845_4455_4c45n,
          0n,
          "metadata stream tag",
        )
      ),
      "random-stream tag",
    ],
    [
      "event stream tag",
      changed((bytes) =>
        setU64(
          bytes,
          randomChoice.body + 16,
          0x574f_524b_4c4f_4144n,
          0n,
          "event stream tag",
        )
      ),
      "random-stream tag",
    ],
  ];
  for (const [label, bytes, fragment] of cases) {
    expectRejected(bytes, label, fragment);
  }
});

Deno.test("browser decoder rejects inconsistent counts, bounds, duplicates, and event order", () => {
  const firstSequence = readU64(GOLDEN, EVENT_FRAMES[0].body);
  const retainedBytes = 1034n;
  const cases = [
    [
      "event outside sampled sequence class",
      changed((bytes) =>
        setU64(
          bytes,
          HEADER_FRAME.body + HEADER_OFFSETS.samplingPeriod,
          1n,
          2n,
          "sampling period",
        )
      ),
      "violates periodic sampling metadata",
    ],
    [
      "event count plus one",
      changed((bytes) =>
        setU64(
          bytes,
          HEADER_FRAME.body + HEADER_OFFSETS.eventCount,
          23n,
          24n,
          "event count plus one",
        )
      ),
      null,
    ],
    [
      "event count minus one",
      changed((bytes) =>
        setU64(
          bytes,
          HEADER_FRAME.body + HEADER_OFFSETS.eventCount,
          23n,
          22n,
          "event count minus one",
        )
      ),
      "disagree",
    ],
    [
      "inexact event count",
      changed((bytes) =>
        setU64(
          bytes,
          HEADER_FRAME.body + HEADER_OFFSETS.eventCount,
          23n,
          9_007_199_254_740_992n,
          "inexact event count",
        )
      ),
      "exact count range",
    ],
    [
      "missing random stream",
      changed((bytes) =>
        setU32(
          bytes,
          HEADER_FRAME.body + HEADER_OFFSETS.randomCount,
          5,
          4,
          "random count",
        )
      ),
      "complete runtime stream set",
    ],
    [
      "too many random streams",
      changed((bytes) =>
        setU32(
          bytes,
          HEADER_FRAME.body + HEADER_OFFSETS.randomCount,
          5,
          6,
          "random count",
        )
      ),
      "complete runtime stream set",
    ],
    [
      "too many tasks",
      changed((bytes) =>
        setU32(
          bytes,
          HEADER_FRAME.body + HEADER_OFFSETS.taskCount,
          3,
          1_000_001,
          "task count",
        )
      ),
      "one-million-task bound",
    ],
    [
      "task/live mismatch",
      changed((bytes) =>
        setU64(
          bytes,
          HEADER_FRAME.body + HEADER_OFFSETS.liveTasks,
          3n,
          2n,
          "live tasks",
        )
      ),
      "declared task frames",
    ],
    [
      "ready exceeds live",
      changed((bytes) =>
        setU64(
          bytes,
          HEADER_FRAME.body + HEADER_OFFSETS.readyTasks,
          1n,
          4n,
          "ready tasks",
        )
      ),
      "ready task count",
    ],
    [
      "ready disagrees with task states",
      changed((bytes) =>
        setU64(
          bytes,
          HEADER_FRAME.body + HEADER_OFFSETS.readyTasks,
          1n,
          2n,
          "ready tasks",
        )
      ),
      "ready task count disagrees with task snapshot states",
    ],
    [
      "timers exceed maximum",
      changed((bytes) =>
        setU64(
          bytes,
          HEADER_FRAME.body + HEADER_OFFSETS.liveTimers,
          0n,
          5n,
          "live timers",
        )
      ),
      "configured maximum",
    ],
    [
      "retained bytes mismatch",
      changed((bytes) =>
        setU64(
          bytes,
          HEADER_FRAME.body + HEADER_OFFSETS.retainedBytes,
          retainedBytes,
          retainedBytes + 1n,
          "retained bytes",
        )
      ),
      "disagree",
    ],
    [
      "event capacity carries retained bytes",
      changed((bytes) =>
        setU8(
          bytes,
          HEADER_FRAME.body + HEADER_OFFSETS.capacityUnit,
          1,
          0,
          "event capacity unit",
        )
      ),
      "event-count artifact",
    ],
    [
      "retained bytes exceed capacity",
      changed((bytes) =>
        setU64(
          bytes,
          HEADER_FRAME.body + HEADER_OFFSETS.retainedBytes,
          retainedBytes,
          4353n,
          "retained bytes capacity",
        )
      ),
      "retention capacity",
    ],
    [
      "prefix mode carries tail capacity",
      changed((bytes) =>
        setU8(
          bytes,
          HEADER_FRAME.body + HEADER_OFFSETS.retentionMode,
          2,
          0,
          "prefix retention",
        )
      ),
      "disagrees with its capacities",
    ],
    [
      "tail mode carries prefix capacity",
      changed((bytes) =>
        setU8(
          bytes,
          HEADER_FRAME.body + HEADER_OFFSETS.retentionMode,
          2,
          1,
          "tail retention",
        )
      ),
      "disagrees with its capacities",
    ],
    [
      "capacity overflow",
      changed((bytes) => {
        setU64(
          bytes,
          HEADER_FRAME.body + HEADER_OFFSETS.prefixCapacity,
          256n,
          18_446_744_073_709_551_615n,
          "prefix capacity",
        );
        setU64(
          bytes,
          HEADER_FRAME.body + HEADER_OFFSETS.tailCapacity,
          4096n,
          1n,
          "tail capacity",
        );
      }),
      "capacity overflowed",
    ],
    [
      "duplicate random stream",
      changed((bytes) => {
        const tag = GOLDEN.subarray(
          RANDOM_FRAMES[0].body,
          RANDOM_FRAMES[0].body + 8,
        );
        assert(
          readU64(bytes, RANDOM_FRAMES[1].body) !==
            readU64(bytes, RANDOM_FRAMES[0].body),
          "random tags start distinct",
        );
        bytes.set(tag, RANDOM_FRAMES[1].body);
      }),
      "duplicate random-stream",
    ],
    [
      "duplicate task snapshot",
      changed((bytes) => {
        const id = GOLDEN.subarray(
          TASK_FRAMES[0].body,
          TASK_FRAMES[0].body + 8,
        );
        assert(
          readU64(bytes, TASK_FRAMES[1].body) !==
            readU64(bytes, TASK_FRAMES[0].body),
          "task IDs start distinct",
        );
        bytes.set(id, TASK_FRAMES[1].body);
      }),
      "duplicate task snapshot",
    ],
    [
      "duplicate event sequence",
      changed((bytes) =>
        setU64(
          bytes,
          EVENT_FRAMES[1].body,
          firstSequence + 1n,
          firstSequence,
          "duplicate event sequence",
        )
      ),
      "increase strictly",
    ],
    [
      "decreasing event sequence",
      changed((bytes) =>
        setU64(
          bytes,
          EVENT_FRAMES[1].body,
          firstSequence + 1n,
          firstSequence - 1n,
          "decreasing event sequence",
        )
      ),
      "increase strictly",
    ],
  ];
  for (const [label, bytes, fragment] of cases) {
    expectRejected(bytes, label, fragment);
  }
});
