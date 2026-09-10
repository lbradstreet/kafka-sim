import "./trace-viewer-core.js";
import "./trace-viewer-ui.js";

const {
  adjacentSelectionIndex,
  exactUnsigned,
  formatExactNanos,
  formatNanos,
  parseJsonArtifact,
  tokenizeJson,
} = globalThis.TRACE_VIEWER_CORE;
const { createArtifactLoader, renderOperationTimeline } =
  globalThis.TRACE_VIEWER_UI;

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

function fakeControl(properties = {}) {
  const listeners = new Map();
  return Object.assign(properties, {
    addEventListener(type, listener) {
      listeners.set(type, listener);
    },
    dispatch(type) {
      const listener = listeners.get(type);
      if (!listener) throw new Error(`no ${type} listener is installed`);
      listener({ target: this, type });
    },
  });
}

function deferred() {
  let resolve;
  let reject;
  const promise = new Promise((resolvePromise, rejectPromise) => {
    resolve = resolvePromise;
    reject = rejectPromise;
  });
  return { promise, reject, resolve };
}

function loaderElements() {
  return {
    errorElement: { hidden: true, textContent: "" },
    fileInput: fakeControl({ files: [], value: "" }),
    resetButton: fakeControl({ disabled: true }),
    sourceElement: { textContent: "" },
  };
}

function fakeSvgNode(name) {
  const listeners = new Map();
  return {
    attributes: new Map(),
    children: [],
    focusOptions: null,
    id: "",
    name,
    parentElement: null,
    textContent: "",
    addEventListener(type, listener) {
      listeners.set(type, listener);
    },
    append(...children) {
      this.children.push(...children);
    },
    dispatch(type, event) {
      const listener = listeners.get(type);
      if (!listener) throw new Error(`no ${type} listener is installed`);
      listener({ ...event, target: this });
    },
    focus(options) {
      this.focusOptions = options;
    },
    getBoundingClientRect() {
      return { width: 720 };
    },
    querySelector(selector) {
      const match = /^\[data-trace-step-index="(\d+)"\]$/.exec(selector);
      if (!match) return null;
      const expected = match[1];
      const visit = (node) => {
        if (node.attributes?.get("data-trace-step-index") === expected) {
          return node;
        }
        for (const child of node.children ?? []) {
          const found = visit(child);
          if (found) return found;
        }
        return null;
      };
      return visit(this);
    },
    replaceChildren(...children) {
      this.children = [...children];
    },
    setAttribute(key, value) {
      this.attributes.set(key, String(value));
    },
  };
}

Deno.test("step navigation moves and stops at trace boundaries", () => {
  assertEquals(adjacentSelectionIndex("ArrowLeft", 2, 5), 1);
  assertEquals(adjacentSelectionIndex("ArrowRight", 2, 5), 3);
  assertEquals(adjacentSelectionIndex("ArrowLeft", 0, 5), 0);
  assertEquals(adjacentSelectionIndex("ArrowRight", 4, 5), 4);
  assertEquals(adjacentSelectionIndex("ArrowLeft", 0, 1), 0);
  assertEquals(adjacentSelectionIndex("ArrowRight", 0, 0), -1);
  assertEquals(adjacentSelectionIndex("Home", 2, 5), 2);
});

Deno.test("exact unsigned values preserve the complete u64 domain", () => {
  assertEquals(exactUnsigned(7, "value"), 7n);
  assertEquals(exactUnsigned("18446744073709551615", "value"), 2n ** 64n - 1n);
  assertThrows(() => exactUnsigned(Number.MAX_SAFE_INTEGER + 1, "value"));
  assertThrows(() => exactUnsigned("18446744073709551616", "value"));
  assertThrows(() => exactUnsigned("07", "value"));
});

Deno.test("nanosecond formatting stays compact across unit boundaries", () => {
  assertEquals(formatNanos("999"), "999 ns");
  assertEquals(formatNanos("1250"), "1.25 µs");
  assertEquals(formatNanos("2000000"), "2 ms");
  assertEquals(formatNanos("1500000000"), "1.5 s");
  assertEquals(formatNanos("1000000001"), "1 s");
});

Deno.test("exact nanosecond formatting preserves every scaled digit", () => {
  assertEquals(formatExactNanos("1251"), "1.251 µs");
  assertEquals(formatExactNanos("2000001"), "2.000001 ms");
  assertEquals(formatExactNanos("1500000001"), "1.500000001 s");
  assertEquals(
    formatExactNanos("18446744073709551615"),
    "18446744073.709551615 s",
  );
});

Deno.test("JSON syntax tokens preserve exact text and classify scalar values", () => {
  const json = JSON.stringify(
    {
      message: 'escaped "quote" and true',
      markup: "</span><script>alert(false)</script>",
      count: -12.5e2,
      scale: 1e21,
      ready: true,
      blocked: false,
      missing: null,
    },
    null,
    2,
  );
  const tokens = tokenizeJson(json);

  assertEquals(
    tokens.map((token) => token.text).join(""),
    json,
    "tokenization must preserve copyable JSON text",
  );
  assertDeepEquals(
    tokens.filter((token) => token.kind !== "plain"),
    [
      { kind: "key", text: '"message"' },
      { kind: "string", text: '"escaped \\"quote\\" and true"' },
      { kind: "key", text: '"markup"' },
      {
        kind: "string",
        text: '"</span><script>alert(false)</script>"',
      },
      { kind: "key", text: '"count"' },
      { kind: "number", text: "-1250" },
      { kind: "key", text: '"scale"' },
      { kind: "number", text: "1e+21" },
      { kind: "key", text: '"ready"' },
      { kind: "boolean", text: "true" },
      { kind: "key", text: '"blocked"' },
      { kind: "boolean", text: "false" },
      { kind: "key", text: '"missing"' },
      { kind: "null", text: "null" },
    ],
  );
  assertDeepEquals(tokenizeJson("[fields could not be represented]"), [
    { kind: "plain", text: "[fields could not be represented]" },
  ]);
  assertThrows(() => tokenizeJson({}), "non-text syntax input was accepted");
});

Deno.test("artifact parser accepts its declared wrapper without evaluating code", () => {
  const options = {
    assignment: "globalThis.EXAMPLE_TRACE =",
    generatedComment: "// generated example",
    description: "example trace",
  };
  const artifact = { schema: 1, value: "safe" };
  assertDeepEquals(
    parseJsonArtifact(JSON.stringify(artifact), options),
    artifact,
    "raw JSON",
  );
  assertDeepEquals(
    parseJsonArtifact(
      `// generated example\nglobalThis.EXAMPLE_TRACE = ${
        JSON.stringify(artifact)
      };`,
      options,
    ),
    artifact,
    "generated wrapper",
  );

  const attackKey = "__traceViewerCoreExecuted";
  delete globalThis[attackKey];
  assertThrows(() =>
    parseJsonArtifact(
      `globalThis.${attackKey} = true;\nglobalThis.EXAMPLE_TRACE = ${
        JSON.stringify(artifact)
      };`,
      options,
    )
  );
  assertEquals(
    globalThis[attackKey],
    undefined,
    "parser executed leading code",
  );
  assertThrows(() =>
    parseJsonArtifact(
      `globalThis.EXAMPLE_TRACE = ${
        JSON.stringify(artifact)
      };\nglobalThis.${attackKey} = true;`,
      options,
    )
  );
  assertEquals(
    globalThis[attackKey],
    undefined,
    "parser executed trailing code",
  );
});

Deno.test("artifact loader defaults to text reads and delegates parsing", async () => {
  const elements = loaderElements();
  const installed = deferred();
  let textReads = 0;
  const file = {
    name: "trace.json",
    size: 18,
    text() {
      textReads += 1;
      return Promise.resolve('{"schema":1}');
    },
  };
  elements.fileInput.files = [file];

  createArtifactLoader({
    ...elements,
    bundledData: { schema: 1 },
    description: "example trace",
    installData(value) {
      installed.resolve(value);
    },
    parseText(contents) {
      assertEquals(contents, '{"schema":1}');
      return JSON.parse(contents);
    },
  });

  elements.fileInput.dispatch("change");
  assertDeepEquals(await installed.promise, { schema: 1 });
  await Promise.resolve();
  assertEquals(textReads, 1);
  assertEquals(elements.sourceElement.textContent, "trace.json");
  assertEquals(elements.resetButton.disabled, false);
  assertEquals(elements.errorElement.hidden, true);
  assertEquals(elements.fileInput.value, "");
});

Deno.test("artifact loader supports binary reads and ignores stale results", async () => {
  const elements = loaderElements();
  const staleRead = deferred();
  const staleFailure = deferred();
  const currentRead = deferred();
  const installed = deferred();
  const files = [
    { name: "stale.sbe", size: 3 },
    { name: "stale-error.sbe", size: 3 },
    { name: "current.sbe", size: 4 },
  ];
  const reads = new Map([
    [files[0], staleRead.promise],
    [files[1], staleFailure.promise],
    [files[2], currentRead.promise],
  ]);
  const parsed = [];
  const installedValues = [];

  createArtifactLoader({
    ...elements,
    bundledData: new Uint8Array(),
    description: "binary trace",
    installData(value) {
      installedValues.push(value);
      installed.resolve(value);
    },
    parseText(contents) {
      parsed.push(contents);
      return contents;
    },
    readFile(file) {
      return reads.get(file);
    },
  });

  elements.fileInput.files = [files[0]];
  elements.fileInput.dispatch("change");
  elements.fileInput.files = [files[1]];
  elements.fileInput.dispatch("change");
  elements.fileInput.files = [files[2]];
  elements.fileInput.dispatch("change");

  const currentBytes = new Uint8Array([0x44, 0x53, 0x54, 0x52]);
  currentRead.resolve(currentBytes.buffer);
  assertEquals(await installed.promise, currentBytes.buffer);
  staleRead.resolve(new Uint8Array([0, 1, 2]).buffer);
  staleFailure.reject(new Error("stale read failed"));
  await Promise.resolve();
  await Promise.resolve();

  assertDeepEquals(parsed, [currentBytes.buffer]);
  assertDeepEquals(installedValues, [currentBytes.buffer]);
  assertEquals(elements.sourceElement.textContent, "current.sbe");
  assertEquals(elements.errorElement.hidden, true);
});

Deno.test("timeline keyboard focus restoration does not scroll the page", () => {
  const originalDocument = Object.getOwnPropertyDescriptor(
    globalThis,
    "document",
  );
  Object.defineProperty(globalThis, "document", {
    configurable: true,
    value: {
      activeElement: null,
      createElementNS(_namespace, name) {
        return fakeSvgNode(name);
      },
    },
  });

  try {
    const svg = fakeSvgNode("svg");
    svg.id = "timeline";
    const steps = [0, 1].map((sequence) => ({
      sequence,
      operation: "read",
      _completedAt: BigInt(sequence + 1),
      _duration: 1n,
      _outcome: "success",
      _startedAt: BigInt(sequence),
    }));
    let selectedIndex = 0;
    renderOperationTimeline({
      description: "Focus behavior",
      onSelect(index) {
        selectedIndex = index;
      },
      selectedIndex,
      steps,
      svg,
      title: "Focus behavior",
    });

    const first = svg.querySelector('[data-trace-step-index="0"]');
    const second = svg.querySelector('[data-trace-step-index="1"]');
    let prevented = false;
    first.dispatch("keydown", {
      key: "ArrowRight",
      preventDefault() {
        prevented = true;
      },
    });

    assert(prevented, "timeline arrow key was not handled");
    assertEquals(selectedIndex, 1);
    assertDeepEquals(second.focusOptions, { preventScroll: true });
  } finally {
    if (originalDocument) {
      Object.defineProperty(globalThis, "document", originalDocument);
    } else {
      Reflect.deleteProperty(globalThis, "document");
    }
  }
});
