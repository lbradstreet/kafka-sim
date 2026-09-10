(function (root) {
  "use strict";

  const RENDERING_DEFINITIONS = Object.freeze([
    { id: 4, name: "ArtifactHeader", kind: "artifact_header" },
    { id: 2, name: "RandomStreamState", kind: "random_stream_state" },
    { id: 5, name: "TaskSnapshot", kind: "task_snapshot" },
    {
      id: 100,
      name: "RuntimeStarted",
      kind: "runtime_started",
      family: "runtime",
      severity: 10,
      decode: decodeRuntimeStarted,
    },
    {
      id: 120,
      name: "TaskSpawned",
      kind: "task_spawned",
      family: "task",
      severity: 10,
      decode: decodeTaskSpawned,
    },
    {
      id: 102,
      name: "TaskEnqueued",
      kind: "task_enqueued",
      family: "task",
      severity: 5,
      decode: decodeTaskEnqueued,
    },
    {
      id: 103,
      name: "TaskPollStarted",
      kind: "task_poll_started",
      family: "task",
      severity: 5,
      decode: decodeTaskOnly,
    },
    {
      id: 104,
      name: "TaskPending",
      kind: "task_pending",
      family: "task",
      severity: 5,
      decode: decodeTaskOnly,
    },
    {
      id: 105,
      name: "TaskCompleted",
      kind: "task_completed",
      family: "task",
      severity: 10,
      decode: decodeTaskOnly,
    },
    {
      id: 106,
      name: "TaskCancelled",
      kind: "task_cancelled",
      family: "task",
      severity: 30,
      decode: decodeTaskCancelled,
    },
    {
      id: 107,
      name: "TaskPanicked",
      kind: "task_panicked",
      family: "task",
      severity: 40,
      decode: decodeTaskPanicked,
    },
    {
      id: 108,
      name: "TaskDropPanicked",
      kind: "task_drop_panicked",
      family: "task",
      severity: 40,
      decode: decodeTaskPanicked,
    },
    {
      id: 109,
      name: "WakerPanicked",
      kind: "waker_panicked",
      family: "task",
      severity: 40,
      decode: decodeTaskPanicked,
    },
    {
      id: 110,
      name: "TimerScheduled",
      kind: "timer_scheduled",
      family: "timer",
      severity: 10,
      decode: decodeTimerScheduled,
    },
    {
      id: 111,
      name: "TimerFired",
      kind: "timer_fired",
      family: "timer",
      severity: 10,
      decode: decodeTimer,
    },
    {
      id: 112,
      name: "TimerCancelled",
      kind: "timer_cancelled",
      family: "timer",
      severity: 10,
      decode: decodeTimer,
    },
    {
      id: 113,
      name: "TimeAdvanced",
      kind: "time_advanced",
      family: "runtime",
      severity: 10,
      decode: decodeTimeAdvanced,
    },
    {
      id: 114,
      name: "RuntimeStalled",
      kind: "runtime_stalled",
      family: "runtime",
      severity: 30,
      decode: decodeRuntimeStalled,
    },
    {
      id: 115,
      name: "BudgetExhausted",
      kind: "budget_exhausted",
      family: "runtime",
      severity: 30,
      decode: decodeBudgetExhausted,
    },
    {
      id: 116,
      name: "RuntimeStopped",
      kind: "runtime_stopped",
      family: "runtime",
      severity: 10,
      decode: decodeRuntimeStopped,
    },
    {
      id: 117,
      name: "RandomChoiceU64",
      kind: "random_choice",
      family: "random",
      severity: 5,
      choice: "u64",
      decode: decodeRandomChoice,
    },
    {
      id: 118,
      name: "RandomChoiceBelow",
      kind: "random_choice",
      family: "random",
      severity: 5,
      choice: "below",
      decode: decodeRandomChoice,
    },
    {
      id: 119,
      name: "RandomChoiceBoolRatio",
      kind: "random_choice",
      family: "random",
      severity: 5,
      choice: "bool_ratio",
      decode: decodeRandomChoice,
    },
  ]);

  if (root.SbeIr === undefined || root.DstTraceSbeIr === undefined) {
    throw new Error(
      "sbe-ir.js and dst-trace-sbe-ir.js must load before sbe-decoder.js",
    );
  }
  const GENERIC_SBE = root.SbeIr;
  const SBE_SCHEMA = GENERIC_SBE.parse(
    GENERIC_SBE.decodeBase64(root.DstTraceSbeIr.base64),
  );
  const RENDERING_BY_ID = new Map(
    RENDERING_DEFINITIONS.map((rendering) => [rendering.id, rendering]),
  );
  if (RENDERING_BY_ID.size !== RENDERING_DEFINITIONS.length) {
    throw new Error("DST trace rendering registry contains duplicate template IDs");
  }
  const TEMPLATE_DEFINITIONS = Object.freeze(
    SBE_SCHEMA.messages.map((template) => {
      const rendering = RENDERING_BY_ID.get(template.id);
      if (rendering === undefined || template.name !== rendering.name) {
        throw new Error(
          `DST trace template ${template.id}/${template.name} does not match the explicit rendering registry`,
        );
      }
      return Object.freeze({ ...rendering, blockLength: template.blockLength });
    }),
  );
  if (TEMPLATE_DEFINITIONS.length !== RENDERING_DEFINITIONS.length) {
    throw new Error("DST trace rendering registry does not cover every SBE template");
  }

  const TEMPLATE_BY_KIND = new Map(
    TEMPLATE_DEFINITIONS.map((definition) => [definition.kind, definition]),
  );
  const EVENT_TEMPLATE_BY_ID = new Map(
    TEMPLATE_DEFINITIONS
      .filter((definition) => typeof definition.decode === "function")
      .map((definition) => [definition.id, definition]),
  );
  const PANIC_EVENT_TEMPLATE_IDS = new Set([107, 108, 109]);
  const MAGIC = Object.freeze([0x44, 0x53, 0x54, 0x52, 0x53, 0x42, 0x45, 0x00]);
  const CONTAINER_VERSION = 1;
  const CONTAINER_FLAGS = 0;
  const PREAMBLE_LENGTH = 16;
  const SBE_SCHEMA_ID = SBE_SCHEMA.id;
  const SBE_SCHEMA_VERSION = SBE_SCHEMA.version;
  const ARTIFACT_SCHEMA_VERSION = 8;
  const TRACE_SCHEMA_VERSION = 5;
  const MESSAGE_HEADER_LENGTH = SBE_SCHEMA.headerLength;
  const FRAME_PREFIX_LENGTH = 4;
  const MIN_FRAME_LENGTH = FRAME_PREFIX_LENGTH + MESSAGE_HEADER_LENGTH;
  const MIN_EVENT_FRAME_LENGTH = 28;
  const MAX_FRAME_LENGTH = 16 * 1024 * 1024;
  const MAX_FILE_LENGTH = 64 * 1024 * 1024;
  const MAX_RANDOM_STREAMS = 5;
  const MAX_TASK_SNAPSHOTS = 1_000_000;
  const MAX_PANIC_MESSAGE_BYTES = 4 * 1024;
  const MAX_SAFE_BIGINT = BigInt(Number.MAX_SAFE_INTEGER);
  const MAX_U64 = (1n << 64n) - 1n;

  const RANDOM_STREAMS = new Map([
    [0x5343_4845_4455_4c45n, "schedule"],
    [0x5343_454e_4152_494fn, "scenario"],
    [0x574f_524b_4c4f_4144n, "workload"],
    [0x4641_554c_5400_0000n, "fault"],
    [0x4445_4255_4700_0000n, "debug"],
  ]);

  class BinaryReader {
    constructor(bytes) {
      this.bytes = bytes;
      this.view = new DataView(
        bytes.buffer,
        bytes.byteOffset,
        bytes.byteLength,
      );
      this.offset = 0;
    }

    require(offset, length, description) {
      if (
        !Number.isSafeInteger(offset) || !Number.isSafeInteger(length) ||
        offset < 0 || length < 0 || offset + length > this.bytes.length
      ) {
        fail(`truncated ${description}`);
      }
    }

    u16(offset, description) {
      this.require(offset, 2, description);
      return this.view.getUint16(offset, true);
    }

    u32(offset, description) {
      this.require(offset, 4, description);
      return this.view.getUint32(offset, true);
    }
  }

  function fail(message) {
    throw new Error(`Invalid SBE trace artifact: ${message}`);
  }

  function asBytes(input) {
    if (input instanceof Uint8Array) {
      return new Uint8Array(input.buffer, input.byteOffset, input.byteLength);
    }
    if (ArrayBuffer.isView(input)) {
      return new Uint8Array(input.buffer, input.byteOffset, input.byteLength);
    }
    if (input instanceof ArrayBuffer) return new Uint8Array(input);
    throw new TypeError("SBE input must be an ArrayBuffer or typed-array view");
  }

  function decodeLimits(limits) {
    if (limits === undefined) {
      return { maxTaskSnapshots: null, maxEvents: null };
    }
    if (
      limits === null || typeof limits !== "object" || Array.isArray(limits)
    ) {
      throw new TypeError("SBE decode limits must be an object");
    }
    const read = (field) => {
      const value = limits[field];
      if (value === undefined) return null;
      if (!Number.isSafeInteger(value) || value < 0) {
        throw new RangeError(`${field} must be a non-negative safe integer`);
      }
      return value;
    };
    return {
      maxTaskSnapshots: read("maxTaskSnapshots"),
      maxEvents: read("maxEvents"),
    };
  }

  function hasMagic(input) {
    const bytes = asBytes(input);
    if (bytes.length < MAGIC.length) return false;
    return MAGIC.every((value, index) => bytes[index] === value);
  }

  function bool(value, description) {
    if (value === 0) return false;
    if (value === 1) return true;
    fail(`invalid ${description} flag ${value}`);
  }

  function optionalU64(present, value, description) {
    if (bool(present, `${description} presence`)) return value;
    if (value !== 0n) fail(`absent ${description} has a nonzero value`);
    return null;
  }

  function safeCount(value, description, maximum = null) {
    if (value > MAX_SAFE_BIGINT) {
      fail(`${description} exceeds JavaScript's exact count range`);
    }
    const count = Number(value);
    if (maximum !== null && count > maximum) {
      fail(`${description} exceeds ${maximum}`);
    }
    return count;
  }

  function decimal(value) {
    return value.toString(10);
  }

  function taskId(value) {
    return `${value.slot}:${value.generation}`;
  }

  function streamName(tag) {
    const name = RANDOM_STREAMS.get(tag);
    if (name === undefined) {
      fail(`unknown random-stream tag 0x${tag.toString(16).padStart(16, "0")}`);
    }
    return name;
  }

  function readFrame(reader, description, minimum = MIN_FRAME_LENGTH) {
    reader.require(reader.offset, FRAME_PREFIX_LENGTH, `${description} length`);
    const start = reader.offset;
    const totalLength = reader.u32(start, `${description} length`);
    if (totalLength < minimum || totalLength > MAX_FRAME_LENGTH) {
      fail(
        `${description} length ${totalLength} is outside ${minimum}..=${MAX_FRAME_LENGTH}`,
      );
    }
    reader.require(start, totalLength, description);
    reader.offset = start + totalLength;
    return {
      start,
      totalLength,
      payloadStart: start + FRAME_PREFIX_LENGTH,
      end: start + totalLength,
    };
  }

  function genericFailure(error) {
    if (error instanceof GENERIC_SBE.SbeDecodeError) {
      fail(error.message.replace(/^Invalid SBE message: /, ""));
    }
    throw error;
  }

  function readMessageHeader(reader, frame) {
    try {
      return GENERIC_SBE.decodeHeader(SBE_SCHEMA, reader.bytes, {
        offset: frame.payloadStart,
        end: frame.end,
      }).header;
    } catch (error) {
      genericFailure(error);
    }
  }

  function validateMessage(
    reader,
    frame,
    definition,
    description,
    variableLimit = null,
  ) {
    const header = readMessageHeader(reader, frame);
    if (header.templateId !== definition.id) {
      fail(`unexpected ${description} template ID ${header.templateId}`);
    }
    if (header.schemaId !== SBE_SCHEMA_ID) {
      fail(`unsupported ${description} SBE schema ID ${header.schemaId}`);
    }
    if (header.version !== SBE_SCHEMA_VERSION) {
      fail(`unsupported ${description} SBE schema version ${header.version}`);
    }
    if (header.blockLength !== definition.blockLength) {
      fail(`unexpected ${description} block length ${header.blockLength}`);
    }
    try {
      return GENERIC_SBE.decodeMessage(SBE_SCHEMA, reader.bytes, {
        offset: frame.payloadStart,
        end: frame.end,
        requireExactLength: false,
        limits: variableLimit === null
          ? undefined
          : { maxVarDataBytes: variableLimit.maximum },
      });
    } catch (error) {
      if (
        variableLimit !== null &&
        error instanceof GENERIC_SBE.SbeDecodeError &&
        error.message.includes(`length exceeds ${variableLimit.maximum}`)
      ) {
        fail(
          `${variableLimit.description} exceeds ${variableLimit.maximum} bytes`,
        );
      }
      genericFailure(error);
    }
  }

  function validatePreamble(reader) {
    reader.require(0, PREAMBLE_LENGTH, "container preamble");
    if (!hasMagic(reader.bytes)) fail("bad container magic");
    if (reader.u16(8, "container version") !== CONTAINER_VERSION) {
      fail("unsupported container version");
    }
    if (reader.u16(10, "container flags") !== CONTAINER_FLAGS) {
      fail("unsupported container flags");
    }
    if (reader.u32(12, "container header length") !== PREAMBLE_LENGTH) {
      fail("unsupported container header length");
    }
    reader.offset = PREAMBLE_LENGTH;
  }

  function decodeArtifactHeader(reader, frame) {
    const definition = TEMPLATE_BY_KIND.get("artifact_header");
    const message = validateMessage(
      reader,
      frame,
      definition,
      "artifact header",
    );
    if (message.endOffset !== frame.end) {
      fail("trailing bytes inside artifact header frame");
    }
    const value = message.value;

    const artifactSchema = value.artifactSchema;
    const traceSchema = value.traceSchema;
    if (artifactSchema !== ARTIFACT_SCHEMA_VERSION) {
      fail(`unsupported artifact schema ${artifactSchema}`);
    }
    if (traceSchema !== TRACE_SCHEMA_VERSION) {
      fail(`unsupported trace schema ${traceSchema}`);
    }

    const prefixCapacity = value.prefixCapacity;
    const tailCapacity = value.tailCapacity;
    const capacity = prefixCapacity + tailCapacity;
    if (capacity > MAX_U64) fail("combined retention capacity overflowed");
    const eventCount = safeCount(
      value.retainedEventCount,
      "retained event count",
    );
    const retainedBytesValue = value.retainedBytes;
    let capacityUnit;
    let retainedBytes;
    switch (value.capacityUnit) {
      case 0:
        if (retainedBytesValue !== 0n) {
          fail("event-count artifact has a nonzero retained byte count");
        }
        capacityUnit = "events";
        retainedBytes = null;
        break;
      case 1:
        capacityUnit = "bytes";
        retainedBytes = retainedBytesValue;
        break;
      default:
        fail(`unknown trace capacity unit ${value.capacityUnit}`);
    }

    let retentionMode;
    switch (value.retentionMode) {
      case 0:
        if (tailCapacity !== 0n) {
          fail("retention mode disagrees with its capacities");
        }
        retentionMode = "prefix";
        break;
      case 1:
        if (prefixCapacity !== 0n) {
          fail("retention mode disagrees with its capacities");
        }
        retentionMode = "tail";
        break;
      case 2:
        retentionMode = "prefix_and_tail";
        break;
      default:
        fail(`unknown retention mode ${value.retentionMode}`);
    }
    if (capacityUnit === "events" && BigInt(eventCount) > capacity) {
      fail("retained event count exceeds retention capacity");
    }
    if (retainedBytes !== null && retainedBytes > capacity) {
      fail("retained bytes exceed byte retention capacity");
    }

    let sampling = null;
    const samplingMode = value.samplingMode;
    const samplingAlgorithm = value.samplingAlgorithmVersion;
    const samplingPeriod = value.samplingPeriod;
    const samplingPhase = value.samplingPhase;
    const fingerprintScope = value.fingerprintScope;
    if (samplingMode === 0) {
      if (
        samplingAlgorithm !== 0 || samplingPeriod !== 0n ||
        samplingPhase !== 0n || fingerprintScope !== 0
      ) {
        fail("noncanonical disabled sampling metadata");
      }
    } else if (samplingMode === 1) {
      if (
        samplingAlgorithm !== 1 || samplingPeriod === 0n ||
        samplingPhase >= samplingPeriod || fingerprintScope !== 1
      ) {
        fail("invalid periodic sampling metadata");
      }
      sampling = {
        mode: "periodic",
        algorithm_version: samplingAlgorithm,
        period: decimal(samplingPeriod),
        phase: decimal(samplingPhase),
        fingerprint_scope: "sampled_events",
      };
    } else {
      fail(`unknown sampling mode ${samplingMode}`);
    }

    const maxTime = optionalU64(
      value.maxTimePresent,
      value.maxTimeNs,
      "max time",
    );
    const lastSequence = optionalU64(
      value.lastSequencePresent,
      value.lastSequence,
      "last sequence",
    );
    const orderingPresent = bool(
      value.orderingViolationPresent,
      "ordering-violation presence",
    );
    const previousSequence = value.previousSequence;
    const rejectedSequence = value.rejectedSequence;
    let orderingViolation = null;
    if (orderingPresent) {
      orderingViolation = {
        previous_sequence: decimal(previousSequence),
        rejected_sequence: decimal(rejectedSequence),
      };
    } else if (previousSequence !== 0n || rejectedSequence !== 0n) {
      fail("absent ordering violation has nonzero sequence values");
    }

    const maxTasks = value.maxTasks;
    const maxTimers = value.maxTimers;
    const liveTasksValue = value.liveTasks;
    const liveTimersValue = value.liveTimers;
    const readyTasksValue = value.readyTasks;
    const randomStreamCount = value.randomStreamCount;
    const taskCount = value.taskCount;
    if (randomStreamCount !== MAX_RANDOM_STREAMS) {
      fail("random-stream count must equal the complete runtime stream set");
    }
    if (taskCount > MAX_TASK_SNAPSHOTS) {
      fail("task count exceeds the one-million-task bound");
    }
    if (BigInt(taskCount) !== liveTasksValue || liveTasksValue > maxTasks) {
      fail("declared task frames disagree with the bounded live-task count");
    }
    if (readyTasksValue > liveTasksValue) {
      fail("ready task count exceeds live task count");
    }
    if (liveTimersValue > maxTimers) {
      fail("live timer count exceeds configured maximum");
    }
    const readyTasks = safeCount(readyTasksValue, "ready task count");
    const liveTimers = safeCount(liveTimersValue, "live timer count");
    const liveTasks = safeCount(liveTasksValue, "live task count");
    const stopped = bool(value.stopped, "stopped");
    if (value.startTimeNs > value.nowNs) {
      fail("start time exceeds the terminal instant");
    }

    const fingerprint = value.traceFingerprint;
    return {
      randomStreamCount,
      taskCount,
      retainedBytesValue: retainedBytes,
      samplingPeriod: sampling === null ? null : samplingPeriod,
      samplingPhase,
      record: {
        record: "header",
        format: "dst-trace",
        artifact_schema: artifactSchema,
        runtime_reproduction_schema: value.runtimeReproductionSchema,
        determinism_checkpoint_schema: value.determinismCheckpointSchema,
        trace_schema: traceSchema,
        rng_version: value.rngVersion,
        seed: decimal(value.seed),
        max_tasks: decimal(maxTasks),
        max_timers: decimal(maxTimers),
        max_steps_per_run: decimal(value.maxStepsPerRun),
        max_time_ns: maxTime === null ? null : decimal(maxTime),
        start_time_ns: decimal(value.startTimeNs),
        driver: value.driver,
        outcome: value.outcome,
        event_count: eventCount,
        capacity_unit: capacityUnit,
        capacity: decimal(capacity),
        retention: {
          mode: retentionMode,
          prefix_capacity: decimal(prefixCapacity),
          tail_capacity: decimal(tailCapacity),
        },
        retained_bytes: retainedBytes === null ? null : decimal(retainedBytes),
        sampling,
        dropped_events: decimal(value.droppedEvents),
        last_sequence: lastSequence === null ? null : decimal(lastSequence),
        ordering_violation: orderingViolation,
        trace_fingerprint: `0x${fingerprint.toString(16).padStart(16, "0")}`,
        now_ns: decimal(value.nowNs),
        total_steps: decimal(value.totalSteps),
        next_enqueue_sequence: decimal(value.nextEnqueueSequence),
        next_timer_sequence: decimal(value.nextTimerSequence),
        next_timer_id: decimal(value.nextTimerId),
        ready_tasks: readyTasks,
        live_timers: liveTimers,
        live_tasks: liveTasks,
        stopped,
        random: [],
        tasks: [],
      },
    };
  }

  function decodeRandomStreamState(reader, frame) {
    const definition = TEMPLATE_BY_KIND.get("random_stream_state");
    const message = validateMessage(
      reader,
      frame,
      definition,
      "random-stream state",
    );
    if (message.endOffset !== frame.end) {
      fail("random-stream state frame has trailing bytes");
    }
    const value = message.value;
    const tag = value.streamTag;
    return {
      stream: streamName(tag),
      stream_tag: decimal(tag),
      state: decimal(value.state),
      draws: decimal(value.draws),
    };
  }

  function decodeTaskSnapshot(reader, frame) {
    const definition = TEMPLATE_BY_KIND.get("task_snapshot");
    const message = validateMessage(
      reader,
      frame,
      definition,
      "task snapshot",
    );
    if (message.endOffset !== frame.end) {
      fail("trailing bytes inside task snapshot frame");
    }
    const value = message.value;
    const id = taskId(value.task);
    const stateTag = value.state;
    const state = ["waiting", "ready", "running"][stateTag];
    if (state === undefined) fail(`unknown task state ${stateTag}`);
    return { id, state };
  }

  function eventRecord(definition, sequence, atNs, task, fields) {
    return {
      record: "event",
      sequence: decimal(sequence),
      at_ns: decimal(atNs),
      type: definition.kind,
      family: definition.family,
      severity: definition.severity,
      task,
      fields,
    };
  }

  function decodeEvent(reader, frame) {
    const templateId = readMessageHeader(reader, frame).templateId;
    const definition = EVENT_TEMPLATE_BY_ID.get(templateId);
    if (definition === undefined) {
      fail(`SBE template id ${templateId} is not a known event template`);
    }
    const hasVariableMessage = PANIC_EVENT_TEMPLATE_IDS.has(templateId);
    const message = validateMessage(
      reader,
      frame,
      definition,
      "event",
      hasVariableMessage
        ? { maximum: MAX_PANIC_MESSAGE_BYTES, description: "panic message" }
        : null,
    );
    if (message.endOffset !== frame.end) {
      if (hasVariableMessage) fail("trailing bytes inside panic event frame");
      fail(`event template ${templateId} has an unexpected message length`);
    }
    const value = message.value;
    return {
      sequence: value.sequence,
      record: definition.decode(definition, value, value.sequence, value.atNs),
    };
  }

  function decodeRuntimeStarted(definition, value, sequence, atNs) {
    return eventRecord(definition, sequence, atNs, null, {
      seed: decimal(value.seed),
    });
  }

  function decodeTaskSpawned(definition, value, sequence, atNs) {
    const task = taskId(value.task);
    const parentPresent = bool(value.parentPresent, "parent presence");
    if (
      !parentPresent &&
      (value.parent.slot !== 0 || value.parent.generation !== 0)
    ) {
      fail("absent task identifier parent has nonzero bytes");
    }
    const parent = taskId(value.parent);
    return eventRecord(definition, sequence, atNs, task, {
      parent: parentPresent ? parent : null,
    });
  }

  function decodeTaskEnqueued(definition, value, sequence, atNs) {
    const task = taskId(value.task);
    return eventRecord(definition, sequence, atNs, task, {
      ready_sequence: decimal(value.readySequence),
    });
  }

  function decodeTaskOnly(definition, value, sequence, atNs) {
    return eventRecord(definition, sequence, atNs, taskId(value.task), {});
  }

  function decodeTaskCancelled(definition, value, sequence, atNs) {
    const reason =
      ["explicit_abort", "block_on_failure", "runtime_stopped"][value.reason];
    if (reason === undefined) {
      fail(`unknown task cancellation reason ${value.reason}`);
    }
    return eventRecord(definition, sequence, atNs, taskId(value.task), {
      reason,
    });
  }

  function decodeTaskPanicked(definition, value, sequence, atNs) {
    return eventRecord(definition, sequence, atNs, taskId(value.task), {
      message: value.message,
      message_truncated: bool(value.messageTruncated, "message truncation"),
    });
  }

  function decodeTimerScheduled(definition, value, sequence, atNs) {
    return eventRecord(definition, sequence, atNs, taskId(value.task), {
      timer: decimal(value.timer),
      deadline_ns: decimal(value.deadlineNs),
    });
  }

  function decodeTimer(definition, value, sequence, atNs) {
    return eventRecord(definition, sequence, atNs, taskId(value.task), {
      timer: decimal(value.timer),
    });
  }

  function decodeTimeAdvanced(definition, value, sequence, atNs) {
    const from = value.fromNs;
    const to = value.toNs;
    const fields = { from_ns: decimal(from), to_ns: decimal(to) };
    if (to >= from) fields.delta_ns = decimal(to - from);
    return eventRecord(definition, sequence, atNs, null, fields);
  }

  function decodeRuntimeStalled(definition, value, sequence, atNs) {
    return eventRecord(definition, sequence, atNs, null, {
      live_tasks: decimal(value.liveTasks),
    });
  }

  function decodeBudgetExhausted(definition, value, sequence, atNs) {
    return eventRecord(definition, sequence, atNs, null, {
      steps: decimal(value.steps),
    });
  }

  function decodeRuntimeStopped(definition, _value, sequence, atNs) {
    return eventRecord(definition, sequence, atNs, null, {});
  }

  function decodeRandomChoice(definition, value, sequence, atNs) {
    const tag = value.streamTag;
    const fields = {
      stream: streamName(tag),
      stream_tag: decimal(tag),
      choice: definition.choice,
      draws_before: decimal(value.drawsBefore),
      draws_after: decimal(value.drawsAfter),
      value: decimal(value.value),
    };
    if (definition.choice === "below") {
      fields.upper_exclusive = decimal(value.upperExclusive);
    } else if (definition.choice === "bool_ratio") {
      fields.numerator = decimal(value.numerator);
      fields.denominator = decimal(value.denominator);
    }
    return eventRecord(definition, sequence, atNs, null, fields);
  }

  function decodeArtifact(input, limits) {
    const bytes = asBytes(input);
    const { maxTaskSnapshots, maxEvents } = decodeLimits(limits);
    if (bytes.length > MAX_FILE_LENGTH) {
      fail(`file exceeds the ${MAX_FILE_LENGTH}-byte viewer limit`);
    }
    const reader = new BinaryReader(bytes);
    validatePreamble(reader);

    const headerFrame = readFrame(reader, "artifact header");
    const decodedHeader = decodeArtifactHeader(reader, headerFrame);
    if (
      maxTaskSnapshots !== null &&
      decodedHeader.taskCount > maxTaskSnapshots
    ) {
      fail(
        `task count ${decodedHeader.taskCount} exceeds decode limit ${maxTaskSnapshots}`,
      );
    }
    if (
      maxEvents !== null && decodedHeader.record.event_count > maxEvents
    ) {
      fail(
        `event count ${decodedHeader.record.event_count} exceeds decode limit ${maxEvents}`,
      );
    }
    const minimumTaskSnapshotLength = FRAME_PREFIX_LENGTH +
      MESSAGE_HEADER_LENGTH + TEMPLATE_BY_KIND.get("task_snapshot").blockLength;
    const minimumRemaining = decodedHeader.randomStreamCount * 36 +
      decodedHeader.taskCount * minimumTaskSnapshotLength +
      decodedHeader.record.event_count * MIN_EVENT_FRAME_LENGTH;
    if (minimumRemaining > bytes.length - reader.offset) {
      fail("declared record counts cannot fit in the remaining file");
    }

    const randomTags = new Set();
    for (let index = 0; index < decodedHeader.randomStreamCount; index += 1) {
      const record = decodeRandomStreamState(
        reader,
        readFrame(reader, "random-stream state"),
      );
      if (randomTags.has(record.stream_tag)) {
        fail("duplicate random-stream state");
      }
      randomTags.add(record.stream_tag);
      decodedHeader.record.random.push(record);
    }

    const taskIds = new Set();
    for (let index = 0; index < decodedHeader.taskCount; index += 1) {
      const record = decodeTaskSnapshot(
        reader,
        readFrame(reader, "task snapshot"),
      );
      if (taskIds.has(record.id)) fail("duplicate task snapshot");
      taskIds.add(record.id);
      decodedHeader.record.tasks.push(record);
    }
    const readyTasks =
      decodedHeader.record.tasks.filter((task) => task.state === "ready")
        .length;
    if (readyTasks !== decodedHeader.record.ready_tasks) {
      fail("ready task count disagrees with task snapshot states");
    }

    const events = [];
    let previousSequence = null;
    let retainedBytes = 0n;
    for (let index = 0; index < decodedHeader.record.event_count; index += 1) {
      const frame = readFrame(reader, "event", MIN_EVENT_FRAME_LENGTH);
      retainedBytes += BigInt(frame.totalLength);
      const event = decodeEvent(reader, frame);
      if (previousSequence !== null && event.sequence <= previousSequence) {
        fail(
          `event sequences must increase strictly; ${event.sequence} follows ${previousSequence}`,
        );
      }
      if (
        decodedHeader.samplingPeriod !== null &&
        event.sequence % decodedHeader.samplingPeriod !==
          decodedHeader.samplingPhase
      ) {
        fail(
          `event sequence ${event.sequence} violates periodic sampling metadata`,
        );
      }
      previousSequence = event.sequence;
      events.push(event.record);
    }

    if (
      decodedHeader.retainedBytesValue !== null &&
      decodedHeader.retainedBytesValue !== retainedBytes
    ) {
      fail(
        `declared retained bytes ${decodedHeader.retainedBytesValue} disagree with ${retainedBytes} encoded event bytes`,
      );
    }
    if (reader.offset !== bytes.length) {
      fail("trailing data after declared event frames");
    }
    return { header: decodedHeader.record, events, notices: [] };
  }

  const publicSchema = Object.freeze({
    containerVersion: CONTAINER_VERSION,
    schemaId: SBE_SCHEMA_ID,
    schemaVersion: SBE_SCHEMA_VERSION,
    artifactSchema: ARTIFACT_SCHEMA_VERSION,
    traceSchema: TRACE_SCHEMA_VERSION,
    templates: Object.freeze(
      TEMPLATE_DEFINITIONS.map(({ id, blockLength, kind }) =>
        Object.freeze({ id, blockLength, kind })
      ),
    ),
  });

  root.DstTraceSbe = Object.freeze({
    decodeArtifact,
    hasMagic,
    schema: publicSchema,
  });
})(globalThis);
