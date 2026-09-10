"use strict";

(function installTraceViewerCore(root) {
  const MAX_U64 = 18_446_744_073_709_551_615n;
  const MAX_U64_DECIMAL = "18446744073709551615";
  const JSON_TOKEN =
    /"(?:\\(?:["\\/bfnrt]|u[0-9a-fA-F]{4})|[^"\\])*"|-?(?:0|[1-9][0-9]*)(?:\.[0-9]+)?(?:[eE][+-]?[0-9]+)?|\b(?:true|false|null)\b/g;

  function record(value, name) {
    if (value === null || typeof value !== "object" || Array.isArray(value)) {
      throw new Error(`${name} must be an object`);
    }
    return value;
  }

  function string(value, name) {
    if (typeof value !== "string") throw new Error(`${name} must be a string`);
    return value;
  }

  function exactUnsigned(value, name) {
    if (typeof value === "bigint") {
      if (value < 0n || value > MAX_U64) {
        throw new Error(`${name} must fit an unsigned 64-bit integer`);
      }
      return value;
    }
    if (typeof value === "number") {
      if (!Number.isSafeInteger(value) || value < 0) {
        throw new Error(
          `${name} must be a safe unsigned integer or decimal string`,
        );
      }
      return BigInt(value);
    }
    if (typeof value !== "string" || !/^(?:0|[1-9][0-9]*)$/.test(value)) {
      throw new Error(`${name} must be an unsigned decimal string`);
    }
    if (
      value.length > MAX_U64_DECIMAL.length ||
      (value.length === MAX_U64_DECIMAL.length && value > MAX_U64_DECIMAL)
    ) {
      throw new Error(`${name} must fit an unsigned 64-bit integer`);
    }
    return BigInt(value);
  }

  function safeUnsigned(value, name) {
    if (!Number.isSafeInteger(value) || value < 0) {
      throw new Error(`${name} must be a safe unsigned integer`);
    }
    return value;
  }

  function positiveVersion(value, name) {
    safeUnsigned(value, name);
    if (value === 0) throw new Error(`${name} must be positive`);
    return value;
  }

  function adjacentSelectionIndex(key, selectedIndex, selectionCount) {
    if (!Number.isSafeInteger(selectedIndex)) {
      throw new Error("selected index must be a safe integer");
    }
    safeUnsigned(selectionCount, "selection count");
    if (selectionCount === 0) return -1;

    const current = Math.max(
      0,
      Math.min(selectionCount - 1, selectedIndex),
    );
    if (key === "ArrowLeft") return Math.max(0, current - 1);
    if (key === "ArrowRight") {
      return Math.min(selectionCount - 1, current + 1);
    }
    return current;
  }

  function tokenizeJson(text) {
    if (typeof text !== "string") {
      throw new Error("JSON syntax highlighting input must be text");
    }

    const tokens = [];
    let cursor = 0;
    for (const match of text.matchAll(JSON_TOKEN)) {
      const index = match.index;
      if (index > cursor) {
        tokens.push({ kind: "plain", text: text.slice(cursor, index) });
      }

      const token = match[0];
      let kind;
      if (token.startsWith('"')) {
        kind = /^\s*:/.test(text.slice(index + token.length))
          ? "key"
          : "string";
      } else if (token === "true" || token === "false") {
        kind = "boolean";
      } else if (token === "null") {
        kind = "null";
      } else {
        kind = "number";
      }
      tokens.push({ kind, text: token });
      cursor = index + token.length;
    }
    if (cursor < text.length) {
      tokens.push({ kind: "plain", text: text.slice(cursor) });
    }
    return tokens;
  }

  function parseJsonArtifact(
    text,
    { assignment, generatedComment, description = "trace artifact" },
  ) {
    if (typeof text !== "string") {
      throw new Error(`${description} file must contain text`);
    }
    if (typeof assignment !== "string" || assignment.length === 0) {
      throw new Error("generated artifact assignment must be nonempty");
    }

    let candidate = text.replace(/^\uFEFF/, "").trim();
    if (candidate.startsWith("//")) {
      const newline = candidate.indexOf("\n");
      const comment = newline < 0
        ? candidate
        : candidate.slice(0, newline).trimEnd();
      if (comment !== generatedComment) {
        throw new Error(`${description} has an unsupported leading comment`);
      }
      candidate = newline < 0 ? "" : candidate.slice(newline + 1).trim();
    }

    if (candidate.startsWith(assignment)) {
      candidate = candidate.slice(assignment.length).trim();
      if (!candidate.endsWith(";")) {
        throw new Error(
          `generated ${description} assignment must end with a semicolon`,
        );
      }
      candidate = candidate.slice(0, -1).trim();
    } else if (!candidate.startsWith("{")) {
      throw new Error(`select raw JSON or the generated ${description} file`);
    }

    try {
      return JSON.parse(candidate);
    } catch (error) {
      const detail = error instanceof Error ? error.message : String(error);
      throw new Error(`invalid ${description} JSON: ${detail}`);
    }
  }

  function ratioBigInt(value, minimum, maximum) {
    if (maximum <= minimum) return 0.5;
    const scale = 1_000_000n;
    return Number(((value - minimum) * scale) / (maximum - minimum)) /
      Number(scale);
  }

  function formatScaled(value, divisor, suffix) {
    const whole = value / divisor;
    const remainder = value % divisor;
    if (remainder === 0n) return `${whole} ${suffix}`;
    const thousandths = (remainder * 1_000n) / divisor;
    const fraction = thousandths.toString().padStart(3, "0").replace(/0+$/, "");
    if (fraction.length === 0) return `${whole} ${suffix}`;
    return `${whole}.${fraction} ${suffix}`;
  }

  function formatExactScaled(value, divisor, suffix) {
    const whole = value / divisor;
    const remainder = value % divisor;
    if (remainder === 0n) return `${whole} ${suffix}`;
    const fractionalDigits = divisor.toString().length - 1;
    const fraction = remainder.toString()
      .padStart(fractionalDigits, "0")
      .replace(/0+$/, "");
    return `${whole}.${fraction} ${suffix}`;
  }

  function formatNanos(value) {
    const nanos = exactUnsigned(value, "nanoseconds");
    if (nanos >= 1_000_000_000n) {
      return formatScaled(nanos, 1_000_000_000n, "s");
    }
    if (nanos >= 1_000_000n) return formatScaled(nanos, 1_000_000n, "ms");
    if (nanos >= 1_000n) return formatScaled(nanos, 1_000n, "µs");
    return `${nanos} ns`;
  }

  function formatExactNanos(value) {
    const nanos = exactUnsigned(value, "nanoseconds");
    if (nanos >= 1_000_000_000n) {
      return formatExactScaled(nanos, 1_000_000_000n, "s");
    }
    if (nanos >= 1_000_000n) {
      return formatExactScaled(nanos, 1_000_000n, "ms");
    }
    if (nanos >= 1_000n) {
      return formatExactScaled(nanos, 1_000n, "µs");
    }
    return `${nanos} ns`;
  }

  function displayLabel(value) {
    return String(value ?? "unknown").replace(/[_-]+/g, " ");
  }

  function displayTitle(value) {
    const words = displayLabel(value);
    return words.charAt(0).toUpperCase() + words.slice(1);
  }

  function safeJson(value) {
    try {
      return JSON.stringify(value ?? {}, null, 2);
    } catch (_error) {
      return "[fields could not be represented]";
    }
  }

  root.TRACE_VIEWER_CORE = Object.freeze({
    adjacentSelectionIndex,
    displayLabel,
    displayTitle,
    exactUnsigned,
    formatExactNanos,
    formatNanos,
    parseJsonArtifact,
    positiveVersion,
    ratioBigInt,
    record,
    safeJson,
    safeUnsigned,
    string,
    tokenizeJson,
  });
})(globalThis);
