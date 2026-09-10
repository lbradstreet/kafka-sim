function assert(condition, message = "assertion failed") {
  if (!condition) throw new Error(message);
}

Deno.test("shared JSON details reserve a stable responsive scroll block", async () => {
  const styles = await Deno.readTextFile(
    new URL("./trace-viewer.css", import.meta.url),
  );
  const block = styles.match(/\.json-dump \{([^}]+)\}/)?.[1] ?? "";
  for (
    const declaration of [
      "box-sizing: border-box;",
      "inline-size: 100%;",
      "block-size: clamp(12rem, 32vh, 22rem);",
      "block-size: clamp(12rem, 32svh, 22rem);",
      "overflow: auto;",
      "scrollbar-gutter: stable;",
    ]
  ) {
    assert(
      block.includes(declaration),
      `shared JSON dump is missing ${declaration}`,
    );
  }
  assert(
    styles.includes("block-size: clamp(10rem, 38vh, 18rem);") &&
      styles.includes("block-size: clamp(10rem, 38svh, 18rem);"),
    "narrow JSON dump does not use its smaller responsive bounds",
  );

  const context = styles.match(/\.detail-context \{([^}]+)\}/)?.[1] ?? "";
  for (
    const declaration of [
      "block-size: clamp(8rem, 20vh, 12rem);",
      "block-size: clamp(8rem, 20svh, 12rem);",
      "overflow: auto;",
      "scrollbar-gutter: stable;",
    ]
  ) {
    assert(
      context.includes(declaration),
      `shared detail context is missing ${declaration}`,
    );
  }
  assert(
    styles.includes("block-size: clamp(9rem, 24vh, 13rem);") &&
      styles.includes("block-size: clamp(9rem, 24svh, 13rem);"),
    "narrow detail context does not reserve stable wrapped-text space",
  );
});

Deno.test("shared timeline gives uncertain completion its own color and shape", async () => {
  const [styles, source] = await Promise.all([
    Deno.readTextFile(new URL("./trace-viewer.css", import.meta.url)),
    Deno.readTextFile(new URL("./trace-viewer-ui.js", import.meta.url)),
  ]);
  for (
    const contract of [
      "--uncertain:",
      ".legend-item.uncertain",
      ".trace-step.uncertain .completion-mark",
      ".outcome.uncertain",
    ]
  ) {
    assert(styles.includes(contract), `shared styles are missing ${contract}`);
  }
  assert(
    source.includes('step._outcome === "uncertain"'),
    "shared timeline is missing its uncertain marker shape",
  );
});

Deno.test("shared timeline gives unknown outcomes their own color and shape", async () => {
  const [styles, source] = await Promise.all([
    Deno.readTextFile(new URL("./trace-viewer.css", import.meta.url)),
    Deno.readTextFile(new URL("./trace-viewer-ui.js", import.meta.url)),
  ]);
  for (
    const selector of [
      "--unknown:",
      ".legend-item.unknown",
      ".trace-step.unknown .completion-mark",
      ".outcome.unknown",
    ]
  ) {
    assert(styles.includes(selector), `shared styles are missing ${selector}`);
  }
  assert(
    source.includes('step._outcome === "unknown"'),
    "shared timeline is missing its unknown marker shape",
  );
  assert(
    source.includes('"stroke-dasharray": "2 2"'),
    "unknown marker should remain distinct without color",
  );
});

Deno.test("operation timeline accepts exact external domains and clips crossing steps", async () => {
  await import("./trace-viewer-core.js");
  await import("./trace-viewer-ui.js");
  class Node {
    constructor(name) {
      this.name = name;
      this.attributes = {};
      this.children = [];
      this.id = "test";
      this.listeners = {};
    }
    setAttribute(k, v) {
      this.attributes[k] = v;
    }
    append(...nodes) {
      this.children.push(...nodes);
    }
    replaceChildren(...nodes) {
      this.children = nodes;
    }
    getBoundingClientRect() {
      return { width: 720 };
    }
    addEventListener(kind, listener) {
      this.listeners[kind] = listener;
    }
    querySelector() {
      return null;
    }
  }
  const previous = globalThis.document;
  globalThis.document = { createElementNS: (_, name) => new Node(name) };
  try {
    const svg = new Node("svg"),
      base = 1n << 63n,
      steps = [
        {
          sequence: 1,
          operation: "write",
          _startedAt: base,
          _completedAt: base + 100n,
          _duration: 100n,
          _outcome: "success",
        },
        {
          sequence: 2,
          operation: "read",
          _startedAt: base + 200n,
          _completedAt: base + 200n,
          _duration: 0n,
          _outcome: "success",
        },
      ];
    let selected = null;
    const render = (domain) =>
      globalThis.TRACE_VIEWER_UI.renderOperationTimeline({
        svg,
        steps,
        domain,
        selectedIndex: 0,
        title: "t",
        description: "d",
        onSelect: (index) => {
          selected = index;
        },
      });
    const natural = render(undefined);
    assert(
      natural.domain.minimum === base && natural.domain.maximum === base + 200n,
    );
    const clipped = render({ minimum: base + 25n, maximum: base + 75n });
    assert(clipped.domain.minimum === base + 25n);
    const groups = svg.children.filter((n) => n.name === "g");
    assert(groups.length === 1, "out-of-domain marker rendered");
    for (
      const flag of [
        "defaultPrevented",
        "isComposing",
        "altKey",
        "ctrlKey",
        "metaKey",
        "shiftKey",
      ]
    ) {
      groups[0].listeners.keydown({
        key: "ArrowRight",
        [flag]: true,
        preventDefault() {
          throw new Error("intercepted modified/composing key");
        },
      });
      assert(selected === null);
    }
    groups[0].listeners.keydown({ key: "ArrowRight", preventDefault() {} });
    assert(selected === 1, "ordinary marker navigation was disabled");
    const line = groups[0].children.find((n) =>
      n.attributes.class === "duration-line"
    );
    assert(
      line.attributes.x1 === "131" && line.attributes.x2 === "707",
      "crossing duration not clipped to plot",
    );
    for (
      const domain of [{ minimum: 0, maximum: 1 }, { minimum: 2n, maximum: 1n }]
    ) {
      let threw = false;
      try {
        render(domain);
      } catch {
        threw = true;
      }
      assert(threw, "bad domain accepted");
    }
  } finally {
    globalThis.document = previous;
  }
});
