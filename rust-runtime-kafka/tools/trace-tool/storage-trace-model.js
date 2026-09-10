"use strict";

(function installStorageTraceModel(root) {
  const ARTIFACT_SCHEMA = 1;
  const SBE_SCHEMA_ID = 2;
  const SBE_SCHEMA_VERSION = 0;
  const TEMPLATE_ID = 1;
  const TEMPLATE_BLOCK_LENGTH = 169;
  const MAX_STEPS = 64;
  const MAX_PROVENANCE_BYTES = 512;
  const MAX_PHASE_BYTES = 64;
  const MAX_DESCRIPTION_BYTES = 512;
  const MAX_SUMMARY_BYTES = 1_024;
  const MAX_BYTE_DATA_BYTES = 4_096;
  const MAX_CONFIG_FILE_BYTES = 64 * 1_024 * 1_024;
  const MAX_SAFE_COUNT = 1_000_000;
  const MAX_U64 = (1n << 64n) - 1n;
  const MAX_REQUIRED_U64 = MAX_U64 - 1n;
  const MAX_REQUIRED_U32 = 0xffff_fffe;
  const MAX_SAFE_BIGINT = BigInt(Number.MAX_SAFE_INTEGER);
  const UTF8 = new TextEncoder();

  // Header + message block + group dimensions + the maximum encoded group +
  // four maximum provenance fields. This is an input-allocation bound, not a
  // promise that every semantically valid trace reaches the maximum.
  const MAX_MESSAGE_BYTES = 8 + TEMPLATE_BLOCK_LENGTH + 4 + MAX_STEPS * (
        157 +
        2 + MAX_PHASE_BYTES +
        2 + MAX_DESCRIPTION_BYTES +
        2 + MAX_SUMMARY_BYTES +
        6 * (2 + MAX_BYTE_DATA_BYTES)
      ) +
    4 * (2 + MAX_PROVENANCE_BYTES);

  const OPERATION = new Map([
    ["Open", [0, "open"]],
    ["ScriptFault", [1, "script_fault"]],
    ["WriteAt", [2, "write_at"]],
    ["ReadAt", [3, "read_at"]],
    ["SetLen", [4, "set_len"]],
    ["Len", [5, "len"]],
    ["Sync", [6, "sync"]],
    ["Crash", [7, "crash"]],
    ["Reopen", [8, "reopen"]],
    ["Close", [9, "close"]],
  ]);
  const OUTCOME = new Map([
    ["Success", [0, "success"]],
    ["Rejected", [1, "rejected"]],
    ["Failed", [2, "failed"]],
    ["Crash", [3, "crash"]],
    ["Recovered", [4, "recovered"]],
  ]);
  const CERTAINTY = new Map([
    ["NotApplicable", [0, "not_applicable"]],
    ["NotApplied", [1, "not_applied"]],
    ["Applied", [2, "applied"]],
    ["MayHaveApplied", [3, "may_have_applied"]],
  ]);
  const SESSION = new Map([
    ["Absent", [0, "absent"]],
    ["Open", [1, "open"]],
    ["Closed", [2, "closed"]],
  ]);

  if (root.SbeIr === undefined || root.StorageTraceSbeIr === undefined) {
    throw new Error(
      "sbe-ir.js and storage-trace-sbe-ir.js must load before storage-trace-model.js",
    );
  }

  const GENERIC_SBE = root.SbeIr;
  const SBE_SCHEMA = GENERIC_SBE.parse(
    GENERIC_SBE.decodeBase64(root.StorageTraceSbeIr.base64),
  );
  const TEMPLATE = SBE_SCHEMA.message(TEMPLATE_ID);
  if (
    SBE_SCHEMA.id !== SBE_SCHEMA_ID ||
    SBE_SCHEMA.version !== SBE_SCHEMA_VERSION ||
    SBE_SCHEMA.headerLength !== 8 ||
    TEMPLATE === null ||
    TEMPLATE.name !== "StorageTraceArtifact" ||
    TEMPLATE.blockLength !== TEMPLATE_BLOCK_LENGTH ||
    TEMPLATE.version !== SBE_SCHEMA_VERSION
  ) {
    throw new Error(
      "generated storage trace SBE IR has unexpected coordinates",
    );
  }

  function fail(message) {
    throw new Error(`Invalid storage trace artifact: ${message}`);
  }

  function record(value, name) {
    if (
      value === null || typeof value !== "object" || Array.isArray(value) ||
      ArrayBuffer.isView(value)
    ) {
      fail(`${name} must be an object`);
    }
    return value;
  }

  function unsignedNumber(value, name, maximum = Number.MAX_SAFE_INTEGER) {
    if (!Number.isSafeInteger(value) || value < 0 || value > maximum) {
      fail(`${name} must be an unsigned integer no greater than ${maximum}`);
    }
    return value;
  }

  function exactU64(value, name) {
    if (
      typeof value !== "bigint" || value < 0n || value > MAX_REQUIRED_U64
    ) {
      fail(`${name} must be a decoded non-null uint64`);
    }
    return value;
  }

  function optionalU64(value, name) {
    return value === null ? null : exactU64(value, name);
  }

  function safeU64(value, name, maximum = Number.MAX_SAFE_INTEGER) {
    const exact = exactU64(value, name);
    if (exact > BigInt(maximum)) {
      fail(`${name} exceeds ${maximum}`);
    }
    return Number(exact);
  }

  function booleanByte(value, name) {
    if (value === 0) return false;
    if (value === 1) return true;
    fail(`${name} must be encoded as 0 or 1`);
  }

  function boundedText(value, maximum, name, allowEmpty = false) {
    if (typeof value !== "string") fail(`${name} must be text`);
    const length = UTF8.encode(value).length;
    if ((!allowEmpty && length === 0) || length > maximum) {
      fail(
        `${name} must contain ${
          allowEmpty ? "at most" : "1.."
        }${maximum} UTF-8 bytes`,
      );
    }
    return value;
  }

  function boundedBytes(value, maximum, name) {
    if (!(value instanceof Uint8Array)) {
      fail(`${name} must be decoded byte data`);
    }
    if (value.length > maximum) {
      fail(`${name} exceeds ${maximum} bytes`);
    }
    return Array.from(value);
  }

  function normalizedEnum(value, values, name) {
    const candidate = record(value, name);
    if (typeof candidate.name !== "string") {
      fail(`${name} has an unknown enum value`);
    }
    const expected = values.get(candidate.name);
    if (
      expected === undefined || !Number.isSafeInteger(candidate.value) ||
      candidate.value !== expected[0]
    ) {
      fail(`${name} has an unsupported enum name/value pair`);
    }
    return expected[1];
  }

  function sameBytes(left, right) {
    return left.length === right.length &&
      left.every((byte, index) => byte === right[index]);
  }

  function requireSameBytes(left, right, message) {
    if (!sameBytes(left, right)) fail(message);
  }

  function normalizeConfig(value) {
    const candidate = record(value, "config");
    const exact = {
      maxFileBytes: exactU64(candidate.maxFileBytes, "config.maxFileBytes"),
      maxReadBytes: exactU64(candidate.maxReadBytes, "config.maxReadBytes"),
      maxWriteBytes: exactU64(candidate.maxWriteBytes, "config.maxWriteBytes"),
      maxReadChunk: exactU64(candidate.maxReadChunk, "config.maxReadChunk"),
      maxWriteChunk: exactU64(candidate.maxWriteChunk, "config.maxWriteChunk"),
      maxInFlight: exactU64(candidate.maxInFlight, "config.maxInFlight"),
      maxScriptedFaults: exactU64(
        candidate.maxScriptedFaults,
        "config.maxScriptedFaults",
      ),
      defaultLatencyNs: exactU64(
        candidate.defaultLatencyNs,
        "config.defaultLatencyNs",
      ),
    };
    const safe = {
      maxFileBytes: safeU64(
        exact.maxFileBytes,
        "config.maxFileBytes",
        MAX_CONFIG_FILE_BYTES,
      ),
      maxReadBytes: safeU64(
        exact.maxReadBytes,
        "config.maxReadBytes",
        MAX_CONFIG_FILE_BYTES,
      ),
      maxWriteBytes: safeU64(
        exact.maxWriteBytes,
        "config.maxWriteBytes",
        MAX_CONFIG_FILE_BYTES,
      ),
      maxReadChunk: safeU64(
        exact.maxReadChunk,
        "config.maxReadChunk",
        MAX_CONFIG_FILE_BYTES,
      ),
      maxWriteChunk: safeU64(
        exact.maxWriteChunk,
        "config.maxWriteChunk",
        MAX_CONFIG_FILE_BYTES,
      ),
      maxInFlight: safeU64(
        exact.maxInFlight,
        "config.maxInFlight",
        MAX_SAFE_COUNT,
      ),
      maxScriptedFaults: safeU64(
        exact.maxScriptedFaults,
        "config.maxScriptedFaults",
        MAX_SAFE_COUNT,
      ),
    };
    if (
      safe.maxFileBytes === 0 || safe.maxReadBytes > safe.maxFileBytes ||
      safe.maxWriteBytes > safe.maxFileBytes || safe.maxReadChunk === 0 ||
      safe.maxReadChunk > safe.maxReadBytes || safe.maxWriteChunk === 0 ||
      safe.maxWriteChunk > safe.maxWriteBytes || safe.maxInFlight === 0 ||
      safe.maxScriptedFaults === 0
    ) {
      fail("storage configuration bounds are inconsistent");
    }
    return {
      raw: Object.fromEntries(
        Object.entries(exact).map(([key, entry]) => [key, entry.toString(10)]),
      ),
      exact,
      safe,
    };
  }

  function normalizeRuntime(value, completedAt) {
    const candidate = record(value, "runtime");
    const reproductionSchema = unsignedNumber(
      candidate.reproductionSchema,
      "runtime.reproductionSchema",
      MAX_REQUIRED_U32,
    );
    const checkpointSchema = unsignedNumber(
      candidate.checkpointSchema,
      "runtime.checkpointSchema",
      MAX_REQUIRED_U32,
    );
    const rngVersion = unsignedNumber(
      candidate.rngVersion,
      "runtime.rngVersion",
      MAX_REQUIRED_U32,
    );
    if (
      reproductionSchema === 0 || checkpointSchema === 0 || rngVersion === 0
    ) {
      fail("runtime schema and RNG versions must be positive");
    }
    const stopped = booleanByte(candidate.stopped, "runtime.stopped");
    if (!stopped) fail("terminal runtime metadata must be stopped");
    const seed = exactU64(candidate.seed, "runtime.seed");
    const now = exactU64(candidate.nowNs, "runtime.nowNs");
    const totalSteps = exactU64(candidate.totalSteps, "runtime.totalSteps");
    const nextEnqueue = exactU64(
      candidate.nextEnqueueSequence,
      "runtime.nextEnqueueSequence",
    );
    const nextTimerSequence = exactU64(
      candidate.nextTimerSequence,
      "runtime.nextTimerSequence",
    );
    const nextTimerId = exactU64(candidate.nextTimerId, "runtime.nextTimerId");
    const readyTasks = safeU64(
      candidate.readyTasks,
      "runtime.readyTasks",
      MAX_SAFE_COUNT,
    );
    const liveTimers = safeU64(
      candidate.liveTimers,
      "runtime.liveTimers",
      MAX_SAFE_COUNT,
    );
    const liveTasks = safeU64(
      candidate.liveTasks,
      "runtime.liveTasks",
      MAX_SAFE_COUNT,
    );
    if (now !== completedAt) {
      fail("runtime.nowNs differs from completedAtNs");
    }
    if (readyTasks !== 0 || liveTimers !== 0 || liveTasks !== 0) {
      fail("stopped runtime metadata retains live work");
    }
    return {
      reproductionSchema,
      checkpointSchema,
      rngVersion,
      stopped,
      seed: seed.toString(10),
      nowNs: now.toString(10),
      totalSteps: totalSteps.toString(10),
      nextEnqueueSequence: nextEnqueue.toString(10),
      nextTimerSequence: nextTimerSequence.toString(10),
      nextTimerId: nextTimerId.toString(10),
      readyTasks: candidate.readyTasks.toString(10),
      liveTimers: candidate.liveTimers.toString(10),
      liveTasks: candidate.liveTasks.toString(10),
      _seed: seed,
      _now: now,
      _totalSteps: totalSteps,
      _readyTasks: readyTasks,
      _liveTimers: liveTimers,
      _liveTasks: liveTasks,
    };
  }

  function normalizeStatus(value, acceptedBytes, durableBytes, config, name) {
    const candidate = record(value, name);
    const session = normalizedEnum(
      candidate.session,
      SESSION,
      `${name}.session`,
    );
    if (session === "absent") {
      fail(`${name}.session is absent in an operation trace`);
    }
    const acceptedLen = exactU64(candidate.acceptedLen, `${name}.acceptedLen`);
    const durableLen = exactU64(candidate.durableLen, `${name}.durableLen`);
    const fsyncGateVersion = unsignedNumber(
      candidate.fsyncGateVersion,
      `${name}.fsyncGateVersion`,
      MAX_REQUIRED_U32,
    );
    if (fsyncGateVersion === 0) {
      fail(`${name}.fsyncGateVersion must be positive`);
    }
    const hasFsyncGatedData = booleanByte(
      candidate.hasFsyncGatedData,
      `${name}.hasFsyncGatedData`,
    );
    const inFlight = safeU64(
      candidate.inFlight,
      `${name}.inFlight`,
      MAX_SAFE_COUNT,
    );
    const inFlightLimit = safeU64(
      candidate.inFlightLimit,
      `${name}.inFlightLimit`,
      MAX_SAFE_COUNT,
    );
    const pendingFaults = safeU64(
      candidate.pendingFaults,
      `${name}.pendingFaults`,
      MAX_SAFE_COUNT,
    );
    const faultHits = exactU64(candidate.faultHits, `${name}.faultHits`);
    const closed = booleanByte(candidate.closed, `${name}.closed`);
    if ((session === "closed") !== closed) {
      fail(`${name}.session contradicts its closed flag`);
    }
    if (acceptedLen !== BigInt(acceptedBytes.length)) {
      fail(`${name}.acceptedLen differs from accepted bytes`);
    }
    if (durableLen !== BigInt(durableBytes.length)) {
      fail(`${name}.durableLen differs from durable bytes`);
    }
    if (
      acceptedLen > config.exact.maxFileBytes ||
      durableLen > config.exact.maxFileBytes
    ) {
      fail(`${name} byte image exceeds configured file capacity`);
    }
    if (
      inFlight !== 0 || inFlightLimit !== config.safe.maxInFlight ||
      pendingFaults > config.safe.maxScriptedFaults
    ) {
      fail(`${name} provider counts exceed their scenario bounds`);
    }
    const raw = {
      session,
      acceptedLen: acceptedLen.toString(10),
      durableLen: durableLen.toString(10),
      fsyncGateVersion,
      hasFsyncGatedData,
      inFlight: candidate.inFlight.toString(10),
      inFlightLimit: candidate.inFlightLimit.toString(10),
      pendingFaults: candidate.pendingFaults.toString(10),
      faultHits: faultHits.toString(10),
      closed,
    };
    const derived = {
      session,
      acceptedBytes,
      durableBytes,
      acceptedLen,
      durableLen,
      fsyncGateVersion,
      hasFsyncGatedData,
      inFlight,
      inFlightLimit,
      pendingFaults,
      faultHits,
      closed,
    };
    return { raw, derived };
  }

  const STATUS_FIELDS = [
    "session",
    "acceptedLen",
    "durableLen",
    "fsyncGateVersion",
    "hasFsyncGatedData",
    "inFlight",
    "inFlightLimit",
    "pendingFaults",
    "faultHits",
    "closed",
  ];

  function statusValue(state, field) {
    return state[field];
  }

  function sameStatus(left, right) {
    return STATUS_FIELDS.every((field) =>
      statusValue(left, field) === statusValue(right, field)
    );
  }

  function requireStatusChangesOnly(before, after, allowed, description) {
    for (const field of STATUS_FIELDS) {
      if (
        !allowed.has(field) &&
        statusValue(before, field) !== statusValue(after, field)
      ) {
        fail(`${description} unexpectedly changes ${field}`);
      }
    }
  }

  function sameState(left, right) {
    return sameStatus(left, right) &&
      sameBytes(left.acceptedBytes, right.acceptedBytes) &&
      sameBytes(left.durableBytes, right.durableBytes);
  }

  function requireOpen(state, description) {
    if (state.session !== "open" || state.closed) {
      fail(`${description} requires an open session`);
    }
  }

  function exactOptionalRaw(value) {
    return value === null ? null : value.toString(10);
  }

  function requireNoPayload(step, description) {
    if (step.requestBytes.length !== 0 || step.resultBytes.length !== 0) {
      fail(`${description} carries unexpected request or result bytes`);
    }
  }

  function requireNoCoordinates(step, description) {
    if (step._offset !== null || step._transferLength !== null) {
      fail(`${description} carries unexpected offset or transfer length`);
    }
  }

  function validateWrite(step, config) {
    if (step.outcome !== "success" || step.certainty !== "not_applicable") {
      fail("write_at must be a successful operation without failure certainty");
    }
    requireOpen(step._before, "write_at");
    requireOpen(step._after, "write_at");
    if (step._offset === null || step._transferLength === null) {
      fail("write_at requires offset and transferLength");
    }
    if (step.requestBytes.length > config.safe.maxWriteBytes) {
      fail("write_at request exceeds configured maxWriteBytes");
    }
    if (
      step._offset > MAX_SAFE_BIGINT ||
      step._transferLength > MAX_SAFE_BIGINT ||
      step._transferLength > BigInt(step.requestBytes.length) ||
      step._transferLength > config.exact.maxWriteChunk
    ) {
      fail("write_at coordinates exceed their exact bounds");
    }
    const requestEnd = step._offset + BigInt(step.requestBytes.length);
    if (requestEnd > config.exact.maxFileBytes) {
      fail("write_at full request range exceeds configured file capacity");
    }
    const offset = Number(step._offset);
    const transfer = Number(step._transferLength);
    const end = offset + transfer;
    if (
      !Number.isSafeInteger(end) || end > MAX_BYTE_DATA_BYTES ||
      end > config.safe.maxFileBytes
    ) {
      fail("write_at range exceeds the viewer byte-image bound");
    }
    requireSameBytes(
      step.resultBytes,
      step.requestBytes,
      "write_at did not return its owned request bytes",
    );
    if (step._resultLength !== BigInt(step.requestBytes.length)) {
      fail("write_at resultLength differs from its returned request buffer");
    }
    const expected = step._before.acceptedBytes.slice();
    while (expected.length < end) expected.push(0);
    expected.splice(offset, transfer, ...step.requestBytes.slice(0, transfer));
    requireSameBytes(
      expected,
      step._after.acceptedBytes,
      "write_at bytes do not match its request",
    );
    requireSameBytes(
      step._before.durableBytes,
      step._after.durableBytes,
      "write_at changed durable bytes without a sync",
    );
    requireStatusChangesOnly(
      step._before,
      step._after,
      new Set(["acceptedLen", "hasFsyncGatedData"]),
      "write_at",
    );
  }

  function validateFaultInjection(step) {
    if (step.outcome !== "success" || step.certainty !== "not_applicable") {
      fail("script_fault must succeed without completion certainty");
    }
    requireOpen(step._before, "script_fault");
    requireOpen(step._after, "script_fault");
    requireNoPayload(step, "script_fault");
    requireNoCoordinates(step, "script_fault");
    if (step._resultLength !== null) {
      fail("script_fault carries a resultLength");
    }
    requireSameBytes(
      step._before.acceptedBytes,
      step._after.acceptedBytes,
      "script_fault changed accepted bytes",
    );
    requireSameBytes(
      step._before.durableBytes,
      step._after.durableBytes,
      "script_fault changed durable bytes",
    );
    if (step._after.pendingFaults !== step._before.pendingFaults + 1) {
      fail("script_fault did not add exactly one pending fault");
    }
    requireStatusChangesOnly(
      step._before,
      step._after,
      new Set(["pendingFaults"]),
      "script_fault",
    );
  }

  function validateSync(step) {
    requireOpen(step._before, "sync");
    requireOpen(step._after, "sync");
    requireNoPayload(step, "sync");
    requireNoCoordinates(step, "sync");
    requireSameBytes(
      step._before.acceptedBytes,
      step._after.acceptedBytes,
      "sync changed accepted bytes",
    );
    if (step.outcome === "failed") {
      if (step.certainty !== "may_have_applied") {
        fail("failed sync must retain MayHaveApplied certainty");
      }
      if (step._resultLength !== null) {
        fail("failed sync carries a resultLength");
      }
      requireSameBytes(
        step._before.durableBytes,
        step._after.durableBytes,
        "ambiguous pre-effect sync changed durable bytes",
      );
      if (
        step._before.pendingFaults === 0 ||
        step._after.pendingFaults + 1 !== step._before.pendingFaults ||
        step._after.faultHits !== step._before.faultHits + 1n
      ) {
        fail("ambiguous sync did not consume one fault and record one hit");
      }
      requireStatusChangesOnly(
        step._before,
        step._after,
        new Set(["pendingFaults", "faultHits"]),
        "ambiguous sync",
      );
      return;
    }
    if (step.outcome !== "success" || step.certainty !== "not_applicable") {
      fail("successful sync has contradictory outcome or certainty");
    }
    requireSameBytes(
      step._before.acceptedBytes,
      step._after.durableBytes,
      "successful sync did not copy accepted bytes to durable bytes",
    );
    if (
      step._resultLength !== null &&
      step._resultLength !== BigInt(step._after.durableBytes.length)
    ) {
      fail("successful sync resultLength differs from durable length");
    }
    if (step._after.hasFsyncGatedData) {
      fail("successful sync leaves fsync-gated data");
    }
    requireStatusChangesOnly(
      step._before,
      step._after,
      new Set(["durableLen", "hasFsyncGatedData"]),
      "successful sync",
    );
  }

  function validateCrash(step) {
    if (step.outcome !== "crash" || step.certainty !== "not_applicable") {
      fail("crash step has contradictory outcome or certainty");
    }
    requireOpen(step._before, "crash");
    if (step._after.session !== "closed" || !step._after.closed) {
      fail("crash did not close the session");
    }
    requireNoPayload(step, "crash");
    requireNoCoordinates(step, "crash");
    if (step._resultLength !== null) fail("crash carries a resultLength");
    requireSameBytes(
      step._before.durableBytes,
      step._after.durableBytes,
      "clean crash changed durable bytes",
    );
    requireSameBytes(
      step._before.durableBytes,
      step._after.acceptedBytes,
      "clean crash did not roll accepted bytes back to durable bytes",
    );
    if (step._after.hasFsyncGatedData) {
      fail("clean crash leaves fsync-gated data");
    }
    requireStatusChangesOnly(
      step._before,
      step._after,
      new Set(["session", "acceptedLen", "hasFsyncGatedData", "closed"]),
      "crash",
    );
  }

  function validateReopen(step) {
    if (step.outcome !== "recovered" || step.certainty !== "not_applicable") {
      fail("reopen step has contradictory outcome or certainty");
    }
    if (step._before.session !== "closed" || !step._before.closed) {
      fail("reopen requires a closed session");
    }
    requireOpen(step._after, "reopen");
    requireNoPayload(step, "reopen");
    requireNoCoordinates(step, "reopen");
    if (step._resultLength !== null) fail("reopen carries a resultLength");
    for (
      const bytes of [
        step._before.acceptedBytes,
        step._before.durableBytes,
        step._after.acceptedBytes,
        step._after.durableBytes,
      ]
    ) {
      requireSameBytes(
        bytes,
        step._before.durableBytes,
        "reopen did not preserve the durable image",
      );
    }
    if (
      step._after.pendingFaults !== 0 || step._after.faultHits !== 0n ||
      step._after.hasFsyncGatedData
    ) {
      fail("reopen did not reset session diagnostics");
    }
    requireStatusChangesOnly(
      step._before,
      step._after,
      new Set(["session", "pendingFaults", "faultHits", "closed"]),
      "reopen",
    );
  }

  function validateRead(step, config) {
    if (step.outcome !== "success" || step.certainty !== "not_applicable") {
      fail("read_at must be a successful operation without failure certainty");
    }
    requireOpen(step._before, "read_at");
    requireOpen(step._after, "read_at");
    if (step._offset === null) fail("read_at requires an offset");
    if (step._offset > MAX_SAFE_BIGINT) {
      fail("read_at offset exceeds the viewer bound");
    }
    if (
      step.requestBytes.length > config.safe.maxReadBytes ||
      step._transferLength === null ||
      step._transferLength > MAX_SAFE_BIGINT
    ) {
      fail("read_at request or transfer length exceeds its configured bound");
    }
    if (step._resultLength !== BigInt(step.requestBytes.length)) {
      fail("read_at resultLength differs from its returned request buffer");
    }
    const offset = Number(step._offset);
    const expectedLength = Math.min(
      step.requestBytes.length,
      config.safe.maxReadChunk,
      Math.max(0, step._before.acceptedBytes.length - offset),
    );
    if (step._transferLength !== BigInt(expectedLength)) {
      fail("read_at length does not match the requested accepted slice");
    }
    const expectedResult = step.requestBytes.slice();
    expectedResult.splice(
      0,
      expectedLength,
      ...step._before.acceptedBytes.slice(offset, offset + expectedLength),
    );
    requireSameBytes(
      step.resultBytes,
      expectedResult,
      "read_at result does not match its updated request buffer",
    );
    if (!sameState(step._before, step._after)) {
      fail("read_at changed storage state");
    }
  }

  function validateTransition(step, config) {
    switch (step.operation) {
      case "write_at":
        validateWrite(step, config);
        break;
      case "script_fault":
        validateFaultInjection(step);
        break;
      case "sync":
        validateSync(step);
        break;
      case "crash":
        validateCrash(step);
        break;
      case "reopen":
        validateReopen(step);
        break;
      case "read_at":
        validateRead(step, config);
        break;
      default:
        fail(
          `operation ${step.operation} is outside the focused storage scenario`,
        );
    }
  }

  function sharedOutcome(outcome, certainty) {
    if (outcome === "success") return "success";
    if (outcome === "failed" && certainty === "may_have_applied") {
      return "uncertain";
    }
    if (outcome === "crash") return "crash";
    if (outcome === "recovered") return "recovered";
    fail(`outcome ${outcome}/${certainty} has no focused viewer meaning`);
  }

  function normalizeStep(candidate, index, config) {
    const value = record(candidate, `steps[${index}]`);
    const sequence = unsignedNumber(
      value.sequence,
      `steps[${index}].sequence`,
      MAX_STEPS - 1,
    );
    if (sequence !== index) {
      fail(`steps[${index}].sequence must equal ${index}`);
    }
    const startedAt = exactU64(
      value.startedAtNs,
      `steps[${index}].startedAtNs`,
    );
    const completedAt = exactU64(
      value.completedAtNs,
      `steps[${index}].completedAtNs`,
    );
    if (completedAt < startedAt) {
      fail(`step ${index} completes before it starts`);
    }
    const operation = normalizedEnum(
      value.operation,
      OPERATION,
      `steps[${index}].operation`,
    );
    const outcome = normalizedEnum(
      value.outcome,
      OUTCOME,
      `steps[${index}].outcome`,
    );
    const certainty = normalizedEnum(
      value.certainty,
      CERTAINTY,
      `steps[${index}].certainty`,
    );
    const offset = optionalU64(value.offset, `steps[${index}].offset`);
    const transferLength = optionalU64(
      value.transferLength,
      `steps[${index}].transferLength`,
    );
    const resultLength = optionalU64(
      value.resultLength,
      `steps[${index}].resultLength`,
    );
    const phase = boundedText(
      value.phase,
      MAX_PHASE_BYTES,
      `steps[${index}].phase`,
    );
    const description = boundedText(
      value.description,
      MAX_DESCRIPTION_BYTES,
      `steps[${index}].description`,
    );
    const summary = boundedText(
      value.summary,
      MAX_SUMMARY_BYTES,
      `steps[${index}].summary`,
    );
    const requestBytes = boundedBytes(
      value.requestBytes,
      MAX_BYTE_DATA_BYTES,
      `steps[${index}].requestBytes`,
    );
    const resultBytes = boundedBytes(
      value.resultBytes,
      MAX_BYTE_DATA_BYTES,
      `steps[${index}].resultBytes`,
    );
    const acceptedBytesBefore = boundedBytes(
      value.acceptedBytesBefore,
      MAX_BYTE_DATA_BYTES,
      `steps[${index}].acceptedBytesBefore`,
    );
    const durableBytesBefore = boundedBytes(
      value.durableBytesBefore,
      MAX_BYTE_DATA_BYTES,
      `steps[${index}].durableBytesBefore`,
    );
    const acceptedBytesAfter = boundedBytes(
      value.acceptedBytesAfter,
      MAX_BYTE_DATA_BYTES,
      `steps[${index}].acceptedBytesAfter`,
    );
    const durableBytesAfter = boundedBytes(
      value.durableBytesAfter,
      MAX_BYTE_DATA_BYTES,
      `steps[${index}].durableBytesAfter`,
    );
    const before = normalizeStatus(
      value.before,
      acceptedBytesBefore,
      durableBytesBefore,
      config,
      `steps[${index}].before`,
    );
    const after = normalizeStatus(
      value.after,
      acceptedBytesAfter,
      durableBytesAfter,
      config,
      `steps[${index}].after`,
    );
    const raw = {
      sequence,
      startedAtNs: startedAt.toString(10),
      completedAtNs: completedAt.toString(10),
      operation,
      outcome,
      certainty,
      offset: exactOptionalRaw(offset),
      transferLength: exactOptionalRaw(transferLength),
      resultLength: exactOptionalRaw(resultLength),
      before: before.raw,
      after: after.raw,
      phase,
      description,
      summary,
      requestBytes,
      resultBytes,
      acceptedBytesBefore,
      durableBytesBefore,
      acceptedBytesAfter,
      durableBytesAfter,
    };
    return {
      raw,
      enriched: {
        ...raw,
        _startedAt: startedAt,
        _completedAt: completedAt,
        _duration: completedAt - startedAt,
        _outcome: sharedOutcome(outcome, certainty),
        _offset: offset,
        _transferLength: transferLength,
        _resultLength: resultLength,
        _before: before.derived,
        _after: after.derived,
      },
    };
  }

  function validateData(input) {
    const artifact = record(input, "storage trace");
    const artifactSchemaVersion = unsignedNumber(
      artifact.artifactSchemaVersion,
      "artifactSchemaVersion",
      MAX_REQUIRED_U32,
    );
    if (artifactSchemaVersion !== ARTIFACT_SCHEMA) {
      fail(
        `unsupported artifact schema ${artifactSchemaVersion}; expected ${ARTIFACT_SCHEMA}`,
      );
    }
    const startedAt = exactU64(artifact.startedAtNs, "startedAtNs");
    const completedAt = exactU64(artifact.completedAtNs, "completedAtNs");
    if (completedAt < startedAt) fail("trace completes before it starts");
    const config = normalizeConfig(artifact.config);
    const runtime = normalizeRuntime(artifact.runtime, completedAt);
    if (!Array.isArray(artifact.steps) || artifact.steps.length === 0) {
      fail("storage trace contains no steps");
    }
    if (artifact.steps.length > MAX_STEPS) {
      fail(`storage trace exceeds the ${MAX_STEPS}-step bound`);
    }
    if (BigInt(artifact.steps.length) > runtime._totalSteps) {
      fail("presentation step count exceeds runtime.totalSteps");
    }

    const normalized = artifact.steps.map((step, index) =>
      normalizeStep(step, index, config)
    );
    const steps = normalized.map((step) => step.enriched);
    let previousCompletion = startedAt;
    let previousState = null;
    for (const step of steps) {
      if (step._startedAt !== previousCompletion) {
        fail(`step ${step.sequence} timing is not contiguous`);
      }
      if (previousState !== null && !sameState(step._before, previousState)) {
        fail(`step ${step.sequence} starts from discontinuous storage state`);
      }
      validateTransition(step, config);
      previousCompletion = step._completedAt;
      previousState = step._after;
    }
    if (previousCompletion !== completedAt) {
      fail("final step completion differs from completedAtNs");
    }
    const terminal = steps.at(-1)._after;
    if (
      terminal.session !== "open" || terminal.closed ||
      !sameBytes(terminal.acceptedBytes, terminal.durableBytes)
    ) {
      fail("terminal storage state is not open on one durable image");
    }

    const scenario = boundedText(
      artifact.scenario,
      MAX_PROVENANCE_BYTES,
      "scenario",
    );
    const sourceTest = boundedText(
      artifact.sourceTest,
      MAX_PROVENANCE_BYTES,
      "sourceTest",
    );
    const provider = boundedText(
      artifact.provider,
      MAX_PROVENANCE_BYTES,
      "provider",
    );
    const generator = boundedText(
      artifact.generator,
      MAX_PROVENANCE_BYTES,
      "generator",
    );
    if (provider !== "SimStorage") {
      fail("provider must identify SimStorage");
    }

    const raw = {
      artifactSchemaVersion,
      startedAtNs: startedAt.toString(10),
      completedAtNs: completedAt.toString(10),
      config: config.raw,
      runtime: Object.fromEntries(
        Object.entries(runtime).filter(([key]) => !key.startsWith("_")),
      ),
      steps: normalized.map((step) => step.raw),
      scenario,
      sourceTest,
      provider,
      generator,
    };
    return {
      ...raw,
      raw,
      steps,
      _startedAt: startedAt,
      _completedAt: completedAt,
      _duration: completedAt - startedAt,
      _initial: steps[0]._before,
      _terminal: terminal,
      _runtime: runtime,
      _config: config.safe,
    };
  }

  function asBytes(input) {
    if (input instanceof Uint8Array) {
      return new Uint8Array(input.buffer, input.byteOffset, input.byteLength);
    }
    if (ArrayBuffer.isView(input)) {
      return new Uint8Array(input.buffer, input.byteOffset, input.byteLength);
    }
    if (input instanceof ArrayBuffer) return new Uint8Array(input);
    throw new TypeError("storage trace SBE input must be bytes");
  }

  function decodeArtifact(input) {
    const bytes = asBytes(input);
    if (bytes.length > MAX_MESSAGE_BYTES) {
      fail(`message exceeds the ${MAX_MESSAGE_BYTES}-byte bound`);
    }
    try {
      const header = GENERIC_SBE.decodeHeader(SBE_SCHEMA, bytes).header;
      if (header.schemaId !== SBE_SCHEMA_ID) {
        fail(`unsupported SBE schema ID ${header.schemaId}`);
      }
      if (header.version !== SBE_SCHEMA_VERSION) {
        fail(`unsupported SBE schema version ${header.version}`);
      }
      if (header.templateId !== TEMPLATE_ID) {
        fail(`unexpected template ID ${header.templateId}`);
      }
      if (header.blockLength !== TEMPLATE_BLOCK_LENGTH) {
        fail(`unexpected block length ${header.blockLength}`);
      }
      const decoded = GENERIC_SBE.decodeMessage(SBE_SCHEMA, bytes, {
        requireExactLength: true,
        limits: {
          maxMessageBytes: MAX_MESSAGE_BYTES,
          maxGroupEntries: MAX_STEPS,
          maxVarDataBytes: MAX_BYTE_DATA_BYTES,
          maxNestingDepth: 8,
        },
      });
      if (
        decoded.bytesRead !== bytes.length || decoded.endOffset !== bytes.length
      ) {
        fail("SBE message was not consumed exactly");
      }
      return validateData(decoded.value);
    } catch (error) {
      if (
        error instanceof Error &&
        error.message.startsWith("Invalid storage trace artifact:")
      ) {
        throw error;
      }
      if (
        error instanceof GENERIC_SBE.SbeDecodeError ||
        error instanceof GENERIC_SBE.SbeIrError
      ) {
        fail(error.message.replace(/^Invalid SBE (?:message|IR): /, ""));
      }
      throw error;
    }
  }

  function decodeBase64Artifact(source) {
    if (typeof source !== "string") {
      throw new TypeError("storage trace base64 input must be a string");
    }
    const maximumEncodedLength = 4 * Math.ceil(MAX_MESSAGE_BYTES / 3);
    if (source.length > maximumEncodedLength) {
      fail(`base64 message exceeds the ${MAX_MESSAGE_BYTES}-byte bound`);
    }
    let bytes;
    try {
      bytes = GENERIC_SBE.decodeBase64(source, { maxBytes: MAX_MESSAGE_BYTES });
    } catch (error) {
      if (
        error instanceof GENERIC_SBE.SbeIrError ||
        error instanceof GENERIC_SBE.SbeDecodeError
      ) {
        fail(error.message.replace(/^Invalid SBE (?:message|IR): /, ""));
      }
      throw error;
    }
    return decodeArtifact(bytes);
  }

  function initialSelectionIndex(trace) {
    const candidate = record(trace, "validated storage trace");
    if (!Array.isArray(candidate.steps) || candidate.steps.length === 0) {
      fail("validated storage trace has no steps");
    }
    const index = candidate.steps.findIndex((step) =>
      step.operation === "sync" && step.outcome === "failed" &&
      step.certainty === "may_have_applied"
    );
    return index < 0 ? 0 : index;
  }

  root.STORAGE_TRACE_MODEL = Object.freeze({
    artifactSchema: ARTIFACT_SCHEMA,
    sbeSchemaId: SBE_SCHEMA_ID,
    sbeSchemaVersion: SBE_SCHEMA_VERSION,
    templateId: TEMPLATE_ID,
    blockLength: TEMPLATE_BLOCK_LENGTH,
    maxMessageBytes: MAX_MESSAGE_BYTES,
    maxSteps: MAX_STEPS,
    decodeArtifact,
    decodeBase64Artifact,
    validateData,
    initialSelectionIndex,
  });
})(globalThis);
