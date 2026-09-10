import "./trace-viewer-core.js";
import "./producer-comparison-model.js";
import "./producer-experiment-model.js";
import "./trace-viewer-ui.js";
import "./producer-comparison-viewer.js";
function assert(ok, why = "assertion failed") {
  if (!ok) throw new Error(why);
}
// A deliberately small DOM/canvas contract double. This checks execution and
// native-control wiring; it is not a browser layout or accessibility audit.
class Element {
  constructor(tag, doc) {
    this.tagName = tag;
    this.doc = doc;
    this.children = [];
    this.attributes = {};
    this.style = {};
    this.dataset = {};
    this.listeners = new Map();
    this.value = "";
    this.hidden = false;
    this.className = "";
    this.id = "";
    this.parentElement = null;
    this._text = "";
  }
  set textContent(v) {
    this._text = String(v);
    this.children = [];
  }
  get textContent() {
    return this._text + this.children.map((c) => c.textContent).join("");
  }
  get classList() {
    return { contains: (c) => this.className.split(" ").includes(c) };
  }
  get lastChild() {
    return this.children.at(-1);
  }
  get nextElementSibling() {
    if (!this.parentElement) return null;
    return this.parentElement
      .children[this.parentElement.children.indexOf(this) + 1];
  }
  get isConnected() {
    return this.parentElement !== null;
  }
  setAttribute(k, v) {
    this.attributes[k] = String(v);
    if (k === "id") this.id = String(v);
    if (k === "class") this.className = String(v);
    if (k.startsWith("data-")) {
      this.dataset[k.slice(5).replace(/-([a-z])/g, (_, c) => c.toUpperCase())] =
        String(v);
    }
  }
  append(...nodes) {
    for (const n of nodes) {
      n.parentElement = this;
      this.children.push(n);
    }
  }
  replaceChildren(...nodes) {
    this.children.forEach((n) => n.parentElement = null);
    this.children = [];
    this._text = "";
    this.append(...nodes);
  }
  after(n) {
    const p = this.parentElement;
    n.parentElement = p;
    p.children.splice(p.children.indexOf(this) + 1, 0, n);
  }
  remove() {
    if (this.parentElement) {
      const p = this.parentElement;
      p.children.splice(p.children.indexOf(this), 1);
      this.parentElement = null;
    }
  }
  addEventListener(k, fn) {
    if (!this.listeners.has(k)) this.listeners.set(k, []);
    this.listeners.get(k).push(fn);
  }
  removeEventListener() {}
  emit(k, props = {}) {
    const e = {
      target: this,
      key: "",
      button: 0,
      pointerId: 1,
      preventDefault() {
        this.defaultPrevented = true;
      },
      ...props,
    };
    for (const fn of this.listeners.get(k) ?? []) fn(e);
    return e;
  }
  getBoundingClientRect() {
    return {
      width: this.doc.width,
      height: Number(this.attributes.height) || 190,
      left: 0,
      top: 0,
    };
  }
  querySelector(selector) {
    return this.all().find((n) => n.matches(selector)) ?? null;
  }
  matches(s) {
    if (s.startsWith("#")) return this.id === s.slice(1);
    const attr = s.match(/^\[([^=\]]+)(?:="([^"]*)")?\]$/);
    if (attr) {
      const v = attr[1].startsWith("data-")
        ? this.dataset[
          attr[1].slice(5).replace(/-([a-z])/g, (_, c) => c.toUpperCase())
        ]
        : this.attributes[attr[1]];
      return attr[2] === undefined ? v !== undefined : v === attr[2];
    }
    return s.split(",").includes(this.tagName);
  }
  closest(s) {
    if (this.matches(s)) return this;
    return this.parentElement?.closest(s) ?? null;
  }
  *walk() {
    yield this;
    for (const c of this.children) yield* c.walk();
  }
  all() {
    return [...this.walk()];
  }
  focus() {
    this.doc.activeElement = this;
  }
  setPointerCapture() {}
  getContext() {
    this.canvasTexts = [];
    return {
      scale() {},
      fillRect() {},
      fillText: (text) => this.canvasTexts.push(text),
    };
  }
}
function documentFrom(html, width) {
  const doc = {
    width,
    activeElement: null,
    createElement(tag) {
      return new Element(tag, doc);
    },
    createElementNS(_, tag) {
      return new Element(tag, doc);
    },
    createTextNode(t) {
      const n = new Element("#text", doc);
      n.textContent = t;
      return n;
    },
    createDocumentFragment() {
      return new Element("fragment", doc);
    },
  };
  const root = new Element("document", doc), stack = [root];
  doc.root = root;
  doc.getElementById = (id) => root.all().find((n) => n.id === id) ?? null;
  doc.querySelector = (s) => root.querySelector(s);
  doc.listeners = new Map();
  doc.addEventListener = (k, fn) => {
    if (!doc.listeners.has(k)) doc.listeners.set(k, []);
    doc.listeners.get(k).push(fn);
  };
  for (const token of html.matchAll(/<\/?[a-z][^>]*>|[^<]+/gi)) {
    const t = token[0];
    if (t.startsWith("</")) {
      if (stack.length > 1) stack.pop();
      continue;
    }
    if (t.startsWith("<")) {
      const tag = t.match(/^<([a-z0-9-]+)/i)[1], n = doc.createElement(tag);
      for (const m of t.matchAll(/([a-z-]+)="([^"]*)"/g)) {
        n.setAttribute(m[1], m[2]);
      }
      stack.at(-1).append(n);
      if (!["meta", "link", "input", "br"].includes(tag)) stack.push(n);
    } else stack.at(-1).append(doc.createTextNode(t));
  }
  return doc;
}
Deno.test("viewer executes at desktop/narrow widths and native controls preserve exact data", async () => {
  const html = await Deno.readTextFile(
    new URL("./producer-experiment.html", import.meta.url),
  );
  const text = await Deno.readTextFile(
    new URL("./producer-experiment-data.js", import.meta.url),
  );
  const parsed = globalThis.TRACE_VIEWER_CORE.parseJsonArtifact(text, {
    assignment: "globalThis.PRODUCER_EXPERIMENT_DATA =",
    generatedComment:
      "// Generated by generate_producer_experiment; do not edit.",
  });
  const hostile =
    '</script><img src=x onerror="globalThis.hostile=true"> & " \u2028 \u2029';
  parsed.scenario.description = hostile;
  parsed.runs[0].meta.scenario.description = hostile;
  const second = structuredClone(parsed.runs[0]);
  second.meta.seed = "1";
  second.config.max_in_flight_per_connection = 5;
  const topic = second.topology.topics[0];
  for (let p = second.topology.partitions.length; p < 1024; p++) {
    second.topology.partitions.push({ topic_id: topic.id_hex, partition: p });
    topic.initial_leaders.push(1);
    second.partitions.acked.push(Array(second.buckets.count).fill(0));
    second.partitions.not_written.push(Array(second.buckets.count).fill(0));
    second.partitions.leader.push(Array(second.buckets.count).fill(1));
  }
  parsed.runs.push(second);
  parsed.seeds.push("1");
  parsed.page.total_runs = 2;
  globalThis.PRODUCER_EXPERIMENT_DATA = parsed;
  const doc = documentFrom(html, 1120);
  globalThis.document = doc;
  globalThis.innerWidth = 1120;
  globalThis.innerHeight = 900;
  globalThis.devicePixelRatio = 2;
  globalThis.getComputedStyle = () => ({ color: "rgb(20,100,160)" });
  globalThis.ResizeObserver = class {
    observe() {}
    disconnect() {}
  };
  globalThis.matchMedia = () => ({ addEventListener() {} });
  globalThis.requestAnimationFrame = (fn) => fn();
  await import("./producer-experiment-viewer.js");
  const $ = doc.getElementById;
  assert($("error").hidden, $("error").textContent);
  assert($("description").textContent === hostile);
  assert($("experiment-intent").textContent.length > 50);
  assert($("experiment-traffic").textContent.includes("records"));
  assert(
    $("topology-heading").textContent === "Partition topology at cursor time",
  );
  const radios = () => $("runs").all().filter((n) => n.type === "radio");
  radios()[1].emit("change");
  assert($("experiment-primary").textContent.includes("seed 1"));
  assert(
    $("experiment-settings").textContent.includes("5 requests per connection"),
  );
  assert($("heatmap-page").children.length === 16);
  $("heatmap-page").value = "15";
  $("heatmap-page").emit("change");
  assert($("heatmap").style.height === "1182px");
  assert($("heatmap").canvasTexts.some((s) => s.endsWith("/ 1023")));
  $("experiment").emit("pointermove", {
    target: $("heatmap"),
    clientX: 200,
    clientY: 22 + 63 * 18 + 1,
  });
  assert($("heat-readout").textContent.includes("partition 1023"));
  radios()[0].emit("change");
  assert($("heatmap-page").value === "0");
  assert($("experiment-primary").textContent.includes("seed 0"));
  assert(
    !doc.root.all().some((n) => n.tagName === "img"),
    "untrusted text became markup",
  );
  assert($("summary").textContent.includes("48"));
  assert($("outcomes").children.length > 0);
  $("view-start").value = "50";
  $("view-end").value = "80";
  $("apply-view").emit("click");
  assert($("view-caption").textContent.startsWith("50 ms–80 ms"));
  $("scatter-section").open = true;
  $("scatter-section").emit("toggle");
  assert($("sample-note").textContent.includes("16 sampled offers"));
  $("record-index").value = "48";
  $("record-index").emit("change");
  assert($("record-detail").textContent.includes('"48"'));
  $("next").emit("click");
  assert($("step-output").textContent.startsWith("2 /"));
  doc.width = 360;
  globalThis.innerWidth = 360;
  $("reset-view").emit("click");
  assert($("view-start").value === "0");
  $("latency-log").checked = true;
  $("latency-log").emit("change");
  for (const n of doc.root.all()) {
    for (const [k, v] of Object.entries(n.attributes)) {
      assert(!/NaN|Infinity/.test(v), `invalid ${k}=${v}`);
      if (k === "d") {
        assert(!/^\s*Z/.test(v), "empty invalid SVG path");
      }
    }
  }
  for (const id of ["heatmap", "scatter"]) {
    const c = $(id);
    assert(c.width * c.height <= 16 * 1024 * 1024, "canvas pixel cap");
  }
  const before = $("view-caption").textContent;
  for (const listener of doc.listeners.get("keydown") ?? []) {
    listener({
      key: "-",
      target: $("view-start"),
      preventDefault() {
        throw new Error("intercepted control");
      },
    });
  }
  assert($("view-caption").textContent === before);
  // Load the paired artifact through the ordinary native file-loader path.
  const paired = await Deno.readTextFile(
    new URL("./producer-comparison-data.js", import.meta.url),
  );
  $("trace-file").files = [{
    name: "paired.js",
    size: paired.length,
    text: () => Promise.resolve(paired),
  }];
  $("trace-file").emit("change");
  await Promise.resolve();
  await Promise.resolve();
  assert($("error").hidden, $("error").textContent);
  assert(!$("comparison-plots").hidden);
  assert($("outcomes-heading").closest("section").hidden);
  assert($("scenario").textContent.includes("Classic Java vs native"));
  assert($("comparison-summary").textContent.includes("96"));
  assert(
    $("comparison-status").textContent.includes("complete replay verified"),
  );
  assert($("comparison-acks").children.length > 0);
  assert(
    $("comparison-latency").all().some((n) => n.tagName === "circle"),
    "isolated latency buckets must remain visible",
  );
  const allTime = $("view-caption").textContent;
  $("comparison-fault-window").emit("click");
  assert($("view-caption").textContent !== allTime);
  $("comparison-quantile").value = "p50";
  $("comparison-quantile").emit("change");
  $("comparison-log").checked = true;
  $("comparison-log").emit("change");
  for (const n of $("comparison-plots").all()) {
    for (const [k, v] of Object.entries(n.attributes)) {
      assert(!/NaN|Infinity/.test(v), `${k}=${v}`);
    }
  }
  $("bundled-sample").emit("click");
  assert($("comparison-plots").hidden);
  assert(!$("outcomes-heading").closest("section").hidden);
});
Deno.test("static loader order, text-only rendering and bounded canvas contract", async () => {
  const [html, js] = await Promise.all([
    Deno.readTextFile(new URL("./producer-experiment.html", import.meta.url)),
    Deno.readTextFile(
      new URL("./producer-experiment-viewer.js", import.meta.url),
    ),
  ]);
  let prior = -1;
  for (
    const file of [
      "trace-viewer-core.js",
      "producer-experiment-model.js",
      "producer-experiment-data.js",
      "trace-viewer-ui.js",
      "producer-experiment-viewer.js",
    ]
  ) {
    const at = html.indexOf(`src="./${file}"`);
    assert(at > prior, `script order ${file}`);
    prior = at;
  }
  assert(!js.includes("innerHTML") && !js.includes("eval("));
  assert(js.includes("maxFileBytes: 48 * 1024 * 1024"));
  assert(js.includes("16 * 1024 * 1024"));
});
