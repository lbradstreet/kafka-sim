import "./trace-viewer-core.js";
import "./sbe-ir.js";
import "./dst-trace-sbe-ir.js";
import "./sbe-decoder.js";
import "./dst-trace-sample.js";
import "./runtime-trace-time.js";

const { formatExactNanos } = globalThis.TRACE_VIEWER_CORE;
const {
  ABSOLUTE,
  RELATIVE,
  boundedTraceTime,
  formatTraceTime,
  timeTickLayout,
  traceTimeRange,
} = globalThis.RUNTIME_TRACE_TIME;

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

function assertThrows(action, fragment) {
  try {
    action();
  } catch (error) {
    assert(error instanceof Error, "operation threw a non-Error value");
    assert(
      error.message.includes(fragment),
      `${JSON.stringify(error.message)} does not include ${
        JSON.stringify(fragment)
      }`,
    );
    return;
  }
  throw new Error(`expected operation to throw ${JSON.stringify(fragment)}`);
}

Deno.test("large virtual epochs retain nanosecond precision in both time bases", () => {
  const cases = [
    {
      start: "3868693840289130663",
      absoluteStart: "3868693840.289130663 s",
      absoluteEnd: "3868693840.289140663 s",
    },
    {
      start: "1234623339628287327",
      absoluteStart: "1234623339.628287327 s",
      absoluteEnd: "1234623339.628297327 s",
    },
  ];

  for (const testCase of cases) {
    const end = (BigInt(testCase.start) + 10_000n).toString();
    const range = traceTimeRange(testCase.start, end);
    assertEquals(formatTraceTime(range.start, range, RELATIVE), "+0 ns");
    assertEquals(formatTraceTime(range.end, range, RELATIVE), "+10 µs");
    assertEquals(
      formatTraceTime(range.start, range, ABSOLUTE),
      testCase.absoluteStart,
    );
    assertEquals(
      formatTraceTime(range.end, range, ABSOLUTE),
      testCase.absoluteEnd,
    );
    assert(
      formatTraceTime(range.start, range, ABSOLUTE) !==
        formatTraceTime(range.end, range, ABSOLUTE),
      "absolute labels collapsed a 10 microsecond trace",
    );
  }
});

Deno.test("time formatting preserves exact nanos across scaled unit boundaries", () => {
  assertEquals(formatExactNanos("999"), "999 ns");
  assertEquals(formatExactNanos("1251"), "1.251 µs");
  assertEquals(formatExactNanos("2000001"), "2.000001 ms");
  assertEquals(formatExactNanos("1500000001"), "1.500000001 s");
  assertEquals(
    formatExactNanos("18446744073709551615"),
    "18446744073.709551615 s",
  );
});

Deno.test("trace time ranges are inclusive and fail closed", () => {
  const range = traceTimeRange("9007199254740993", "9007199254741003");
  assertEquals(range.duration, 10n);
  assertEquals(boundedTraceTime(range.start, range, "event time"), range.start);
  assertEquals(boundedTraceTime(range.end, range, "event time"), range.end);
  assertThrows(
    () => boundedTraceTime(range.start - 1n, range, "event time"),
    "outside the trace time range",
  );
  assertThrows(
    () => boundedTraceTime(range.end + 1n, range, "event time"),
    "outside the trace time range",
  );
  assertThrows(
    () => traceTimeRange("10", "9"),
    "start time must not exceed",
  );
  assertThrows(
    () => formatTraceTime(range.start, range, "wall-clock"),
    "unsupported virtual-time basis",
  );
});

Deno.test("zero-duration traces have one stable relative instant", () => {
  const range = traceTimeRange("18446744073709551615", "18446744073709551615");
  assertEquals(range.duration, 0n);
  assertEquals(formatTraceTime(range.start, range, RELATIVE), "+0 ns");
  assertEquals(
    formatTraceTime(range.start, range, ABSOLUTE),
    "18446744073.709551615 s",
  );
});

Deno.test("time tick layout bounds density and staggers long narrow labels", () => {
  const range = traceTimeRange("3868693840289130663", "3868693840289140663");
  assertEquals(
    JSON.stringify(timeTickLayout(range, 5, 1_200, () => 170)),
    JSON.stringify({ count: 5, staggerEndpoints: false }),
  );
  assertEquals(
    JSON.stringify(timeTickLayout(range, 5, 800, () => 170)),
    JSON.stringify({ count: 3, staggerEndpoints: false }),
  );
  assertEquals(
    JSON.stringify(timeTickLayout(range, 3, 176, () => 170)),
    JSON.stringify({ count: 2, staggerEndpoints: true }),
  );

  const oneSecond = traceTimeRange("0", "1000000000");
  assertEquals(
    JSON.stringify(timeTickLayout(oneSecond, 5, 400, (value) => {
      return value === oneSecond.start || value === oneSecond.end ? 40 : 120;
    })),
    JSON.stringify({ count: 3, staggerEndpoints: false }),
    "long interior labels were not included in density selection",
  );

  const twoNanos = traceTimeRange("10", "12");
  assertEquals(
    JSON.stringify(timeTickLayout(twoNanos, 5, 800, () => 40)),
    JSON.stringify({ count: 3, staggerEndpoints: false }),
  );
  const oneInstant = traceTimeRange("10", "10");
  assertEquals(
    JSON.stringify(timeTickLayout(oneInstant, 5, 1, () => 500)),
    JSON.stringify({ count: 1, staggerEndpoints: false }),
  );
});

Deno.test("bundled runtime trace and page use the decoder's current schema", async () => {
  const decoder = globalThis.DstTraceSbe;
  const decoded = decoder.decodeArtifact(
    globalThis.SbeIr.decodeBase64(globalThis.DstTraceSample.base64),
  );
  assertEquals(
    decoded.header.artifact_schema,
    decoder.schema.artifactSchema,
    "bundled sample drifted from the decoder schema",
  );
  const range = traceTimeRange(
    decoded.header.start_time_ns,
    decoded.header.now_ns,
  );
  assertEquals(range.start, 0n);
  assertEquals(range.end, 5_000n);
  for (const [index, event] of decoded.events.entries()) {
    boundedTraceTime(event.at_ns, range, `event ${index}.at_ns`);
  }

  const page = await Deno.readTextFile(
    new URL("./index.html", import.meta.url),
  );
  const coreScript = page.indexOf(
    '<script src="trace-viewer-core.js"></script>',
  );
  const decoderScript = page.indexOf('<script src="sbe-decoder.js"></script>');
  const timeScript = page.indexOf(
    '<script src="runtime-trace-time.js"></script>',
  );
  const sampleScript = page.indexOf(
    '<script src="dst-trace-sample.js"></script>',
  );
  assert(coreScript >= 0, "runtime page does not load the shared viewer core");
  assert(
    coreScript < decoderScript && decoderScript < timeScript &&
      timeScript < sampleScript,
    "runtime page scripts are not in dependency order",
  );
  assert(
    page.includes('id="time-basis-select"'),
    "runtime page is missing the time-basis control",
  );
  assert(
    page.includes("elements.timeBasis.value = TRACE_TIME.RELATIVE"),
    "runtime page does not default and reset to relative time",
  );
  assert(
    page.includes("globalThis.DstTraceSbe.schema.artifactSchema"),
    "runtime page does not derive the sample schema label from the decoder",
  );
  assert(
    !page.includes("const ARTIFACT_SCHEMA_VERSION"),
    "runtime page duplicates the decoder's artifact schema version",
  );
  assert(
    !page.includes("function formatDuration"),
    "runtime page retains its lossy inline time formatter",
  );
});
