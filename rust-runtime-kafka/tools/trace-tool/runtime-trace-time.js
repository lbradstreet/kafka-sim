"use strict";

(function installRuntimeTraceTime(root) {
  const core = root.TRACE_VIEWER_CORE;
  if (!core) throw new Error("trace-viewer-core.js must load first");

  const ABSOLUTE = "absolute";
  const RELATIVE = "relative";
  const TIME_BASES = Object.freeze([RELATIVE, ABSOLUTE]);

  function traceTimeRange(startValue, endValue) {
    const start = core.exactUnsigned(startValue, "trace start time");
    const end = core.exactUnsigned(endValue, "trace end time");
    if (end < start) {
      throw new Error("trace start time must not exceed trace end time");
    }
    return Object.freeze({ start, end, duration: end - start });
  }

  function validateTraceTimeRange(range) {
    if (
      range === null ||
      typeof range !== "object" ||
      typeof range.start !== "bigint" ||
      typeof range.end !== "bigint" ||
      typeof range.duration !== "bigint" ||
      range.end < range.start ||
      range.duration !== range.end - range.start
    ) {
      throw new Error("trace time range is invalid");
    }
    core.exactUnsigned(range.start, "trace start time");
    core.exactUnsigned(range.end, "trace end time");
  }

  function boundedTraceTime(value, range, name = "virtual time") {
    validateTraceTimeRange(range);
    const exact = core.exactUnsigned(value, name);
    if (exact < range.start || exact > range.end) {
      throw new Error(`${name} is outside the trace time range`);
    }
    return exact;
  }

  function formatTraceTime(value, range, basis) {
    const exact = boundedTraceTime(value, range);
    if (basis === RELATIVE) {
      return `+${core.formatExactNanos(exact - range.start)}`;
    }
    if (basis === ABSOLUTE) return core.formatExactNanos(exact);
    throw new Error(`unsupported virtual-time basis ${String(basis)}`);
  }

  function timeTickLayout(
    range,
    preferredCount,
    plotWidth,
    measureLabelWidth,
  ) {
    boundedTraceTime(range.start, range, "trace start time");
    if (!Number.isSafeInteger(preferredCount) || preferredCount < 2) {
      throw new Error(
        "preferred tick count must be a safe integer of at least two",
      );
    }
    if (!Number.isFinite(plotWidth) || plotWidth < 0) {
      throw new Error("plot width must be a finite nonnegative number");
    }
    if (typeof measureLabelWidth !== "function") {
      throw new Error("time tick layout requires a label-width function");
    }
    if (range.duration === 0n) {
      return Object.freeze({ count: 1, staggerEndpoints: false });
    }

    let count = preferredCount;
    if (range.duration < BigInt(count - 1)) {
      count = Number(range.duration) + 1;
    }

    for (; count >= 2; count -= 1) {
      const interval = plotWidth / (count - 1);
      let previousRight = Number.NEGATIVE_INFINITY;
      let labelsFit = true;
      for (let index = 0; index < count; index += 1) {
        const value = range.start +
          (range.duration * BigInt(index)) / BigInt(count - 1);
        const labelWidth = measureLabelWidth(value);
        if (!Number.isFinite(labelWidth) || labelWidth < 0) {
          throw new Error(
            "time tick label width must be a finite nonnegative number",
          );
        }
        const x = interval * index;
        const left = index === 0
          ? x
          : (index === count - 1 ? x - labelWidth : x - labelWidth / 2);
        const right = index === 0
          ? x + labelWidth
          : (index === count - 1 ? x : x + labelWidth / 2);
        if (left - previousRight < 16) labelsFit = false;
        previousRight = right;
      }
      if (labelsFit) {
        return Object.freeze({ count, staggerEndpoints: false });
      }
    }
    return Object.freeze({ count: 2, staggerEndpoints: true });
  }

  root.RUNTIME_TRACE_TIME = Object.freeze({
    ABSOLUTE,
    RELATIVE,
    TIME_BASES,
    boundedTraceTime,
    formatTraceTime,
    timeTickLayout,
    traceTimeRange,
  });
})(globalThis);
