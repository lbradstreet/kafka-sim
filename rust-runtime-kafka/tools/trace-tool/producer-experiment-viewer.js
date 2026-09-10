"use strict";
(function installProducerExperimentViewer(root) {
  const M = root.PRODUCER_EXPERIMENT_MODEL, U = root.TRACE_VIEWER_UI;
  const $ = (id) => document.getElementById(id);
  const state = {
    bundle: null,
    primary: 0,
    overlay: new Set(),
    view: [0, 1],
    hover: 0,
    selectedMarker: 0,
    pools: new Set(),
    charts: new Map(),
    steps: [],
    reasonRows: [],
  };
  const color = (i) => `var(--variant-${i % 6 + 1})`;
  const pattern = (i) => ["", "8 3", "2 3", "10 3 2 3", "5 2", "1 2"][i % 6];
  const fmt = (n) =>
    n === null || !Number.isFinite(n)
      ? "—"
      : n.toLocaleString(undefined, { maximumFractionDigits: 2 });
  const time = (n) =>
    n === null || !Number.isFinite(n)
      ? "—"
      : n < 1e6
      ? `${fmt(n / 1e3)} µs`
      : n < 1e9
      ? `${fmt(n / 1e6)} ms`
      : `${fmt(n / 1e9)} s`;
  const run = () => state.bundle.runs[state.primary];
  const selected = () =>
    [state.primary, ...state.overlay].filter((v, i, a) => a.indexOf(v) === i)
      .slice(0, 6);
  const el = (tag, text = null, cls = null) => {
    const n = document.createElement(tag);
    if (text !== null) n.textContent = text;
    if (cls) n.className = cls;
    return n;
  };
  function option(select, value, label) {
    const n = el("option", label);
    n.value = String(value);
    select.append(n);
  }
  function table(target, headers, rows) {
    const t = el("table"), head = el("thead"), tr = el("tr");
    headers.forEach((v) => tr.append(el("th", v)));
    head.append(tr);
    t.append(head);
    const body = el("tbody");
    rows.forEach((row) => {
      const tr = el("tr");
      row.forEach((v) => tr.append(el("td", String(v))));
      body.append(tr);
    });
    t.append(body);
    target.replaceChildren(t);
  }
  function points(r, values) {
    return Array.from(
      values,
      (y, i) => ({
        x: Math.min(r.meta.duration, (i + .5) * r.buckets.bucket_ns),
        y,
        i,
        left: i * r.buckets.bucket_ns,
        right: (i + 1) * r.buckets.bucket_ns,
      }),
    );
  }
  function line(r, key, label, c = 0, options = {}) {
    return {
      label,
      color: color(c),
      dash: pattern(c),
      points: points(r, key),
      bucketWidth: r.buckets.bucket_ns,
      duration: r.meta.duration,
      ...options,
    };
  }
  function chart(
    id,
    specs,
    {
      unit = "count / bucket",
      log = false,
      domain = state.view,
      timeAxis = true,
      xFormat = null,
      max = null,
      height = 190,
      stack = false,
    } = {},
  ) {
    const svg = $(id);
    svg.replaceChildren();
    const width = U.chartWidth(svg),
      left = 68,
      right = width - 16,
      top = 26,
      bottom = height - 32,
      pw = right - left;
    svg.setAttribute("viewBox", `0 0 ${width} ${height}`);
    svg.setAttribute("height", String(height));
    const lo = domain[0], hi = Math.max(lo + 1, domain[1]);
    const transform = (v) =>
      log ? Math.log10(1 + Math.max(0, v)) : Math.max(0, v);
    let highest = max ?? 0;
    for (const s of specs) {
      for (const p of s.points) {
        if (p.x >= lo && p.x <= hi && p.y !== null && Number.isFinite(p.y)) {
          highest = Math.max(
            highest,
            p.y,
            p.high ?? 0,
            p.base === undefined ? 0 : p.base + p.y,
          );
        }
      }
    }
    highest = Math.max(1, highest);
    const y = (v) =>
        bottom - transform(v) / transform(highest) * (bottom - top),
      x = (v) => left + (v - lo) / (hi - lo) * pw;
    const defs = U.svgElement("defs"),
      clip = U.svgElement("clipPath", { id: `clip-${id}` });
    clip.append(
      U.svgElement("rect", {
        x: left,
        y: top,
        width: pw,
        height: bottom - top,
      }),
    );
    defs.append(clip);
    svg.append(defs);
    svg.append(
      U.svgElement("text", { x: left, y: 15, class: "plot-label" }, unit),
    );
    for (let i = 0; i < 4; i++) {
      const f = i / 3,
        v = log ? 10 ** (transform(highest) * f) - 1 : highest * f,
        yy = bottom - f * (bottom - top);
      svg.append(
        U.svgElement("line", {
          x1: left,
          x2: right,
          y1: yy,
          y2: yy,
          class: "chart-grid",
        }),
        U.svgElement(
          "text",
          { x: left - 7, y: yy + 4, "text-anchor": "end" },
          unit.startsWith("latency") || unit.includes("RTT") ||
            unit.includes("duration")
            ? time(v)
            : fmt(v),
        ),
      );
    }
    for (let i = 0; i < (width < 500 ? 3 : 5); i++) {
      const v = lo + (hi - lo) * i / (width < 500 ? 2 : 4);
      svg.append(
        U.svgElement("text", {
          x: x(v),
          y: height - 10,
          "text-anchor": "middle",
        }, xFormat ? xFormat(v) : timeAxis ? time(v) : fmt(v)),
      );
    }
    const plot = U.svgElement("g", { "clip-path": `url(#clip-${id})` });
    if (timeAxis) {
      for (const band of run().environment.bands) {
        if (band.end < lo || band.start > hi) continue;
        plot.append(
          U.svgElement("rect", {
            x: x(Math.max(lo, band.start)),
            y: top,
            width: Math.max(
              0,
              x(Math.min(hi, band.end)) - x(Math.max(lo, band.start)),
            ),
            height: bottom - top,
            fill: "var(--muted)",
            opacity: .65,
          }),
        );
      }
    }
    if (timeAxis && state.bundle.schema === "kr-producer-comparison/v1") {
      for (const marker of run().environment.markers) {
        if (marker.at < lo || marker.at > hi) continue;
        const tick = U.svgElement("line", {
          x1: x(marker.at),
          x2: x(marker.at),
          y1: top,
          y2: bottom,
          stroke: "var(--foreground)",
          "stroke-dasharray": "2 4",
          opacity: .5,
        });
        tick.append(
          U.svgElement("title", {}, `${time(marker.at)}: ${marker.label}`),
        );
        plot.append(tick);
      }
    }
    specs.forEach((s, si) => {
      const visible = s.points.filter((p) =>
        (s.kind === "bar" && p.left !== undefined
          ? p.right >= lo && p.left <= hi
          : p.x >= lo && p.x <= hi) && p.y !== null && Number.isFinite(p.y)
      );
      const bw = s.bucketWidth
        ? Math.max(.3, pw * s.bucketWidth / (hi - lo))
        : Math.max(2, pw / Math.max(1, s.points.length * specs.length) * .75);
      if (s.band) {
        const poly = visible.filter((p) =>
          p.high !== null && Number.isFinite(p.high)
        );
        if (poly.length) {
          plot.append(
            U.svgElement("path", {
              d: poly.map((p, i) => `${i ? "L" : "M"}${x(p.x)},${y(p.high)}`)
                .join(" ") +
                poly.slice().reverse().map((p) => ` L${x(p.x)},${y(p.y)}`).join(
                  "",
                ) + " Z",
              fill: s.color,
              opacity: .16,
            }),
          );
        }
      }
      if (s.kind === "bar") {
        visible.forEach((p) => {
          const base = stack ? p.base ?? 0 : 0;
          plot.append(
            U.svgElement("rect", {
              x: (p.left !== undefined ? x(p.left) : x(p.x) - bw / 2) +
                (s.grouped ? si * bw / specs.length : 0),
              y: y(base + p.y),
              width: s.grouped ? bw / specs.length : bw,
              height: Math.max(0, y(base) - y(base + p.y)),
              fill: s.color,
              opacity: .8,
            }),
          );
        });
      } else if (s.kind === "dots" || s.kind === "ticks") {
        visible.forEach((p) => {
          if (s.kind === "ticks" && p.y === 0) return;
          plot.append(
            U.svgElement(
              s.kind === "dots" ? "circle" : "line",
              s.kind === "dots"
                ? { cx: x(p.x), cy: y(p.y), r: 2, fill: s.color }
                : {
                  x1: x(p.x),
                  x2: x(p.x),
                  y1: bottom - 8 - si * 5,
                  y2: bottom - si * 5,
                  stroke: s.color,
                  "stroke-width": 2,
                },
            ),
          );
        });
      } else {
        let path = "", pen = false;
        for (const p of s.points) {
          if (p.x < lo || p.x > hi || p.y === null || !Number.isFinite(p.y)) {
            pen = false;
            continue;
          }
          path += `${pen ? " L" : " M"}${x(p.x)},${y(p.y)}`;
          pen = true;
        }
        if (s.area && visible.length) {
          plot.append(
            U.svgElement("path", {
              d: path +
                ` L${x(visible.at(-1).x)},${bottom} L${
                  x(visible[0].x)
                },${bottom} Z`,
              fill: s.color,
              opacity: .15,
            }),
          );
        }
        plot.append(
          U.svgElement("path", {
            d: path,
            fill: "none",
            stroke: s.color,
            "stroke-width": 1.6,
            "stroke-dasharray": s.dash ?? "",
          }),
        );
        if (s.markers) {
          for (const p of visible) {
            plot.append(
              U.svgElement("circle", {
                cx: x(p.x),
                cy: y(p.y),
                r: 2,
                fill: s.color,
              }),
            );
          }
        }
      }
    });
    svg.append(plot);
    const guide = U.svgElement("line", {
      class: "crosshair",
      y1: top,
      y2: bottom,
      x1: left,
      x2: left,
      visibility: "hidden",
    });
    svg.append(guide);
    // Legends are text outside the plotting rectangle and wrap with native layout.
    let legend = svg.nextElementSibling;
    if (!legend?.classList.contains("plot-legend")) {
      legend = el("div", null, "caption plot-legend controls");
      svg.after(legend);
    }
    legend.replaceChildren();
    specs.forEach((s) => {
      const item = el("span", s.label);
      item.style.color = s.color;
      item.style.borderBottom = `2px ${s.dash ? "dashed" : "solid"} ${s.color}`;
      legend.append(item);
    });
    if (timeAxis) {
      svg.dataset.timeChart = id;
      state.charts.set(id, {
        svg,
        x,
        y,
        left,
        right,
        top,
        bottom,
        width,
        guide,
        specs,
        unit,
      });
    }
  }
  function renderBands() {
    const svg = $("bands"),
      width = U.chartWidth(svg),
      left = 68,
      right = width - 16,
      h = 100;
    svg.replaceChildren();
    svg.setAttribute("viewBox", `0 0 ${width} ${h}`);
    svg.setAttribute("height", String(h));
    const [a, z] = state.view,
      x = (t) => left + (t - a) / Math.max(1, z - a) * (right - left);
    const kinds = [...new Set(run().environment.bands.map((b) => b.kind))];
    const defs = U.svgElement("defs");
    kinds.forEach((_k, i) => {
      const p = U.svgElement("pattern", {
        id: `hatch-${i}`,
        width: 6 + i,
        height: 6 + i,
        patternUnits: "userSpaceOnUse",
        patternTransform: `rotate(${i % 2 ? 45 : -45})`,
      });
      p.append(
        U.svgElement("line", {
          x1: 0,
          x2: 0,
          y1: 0,
          y2: 10,
          stroke: color(i),
          "stroke-width": 2,
        }),
      );
      defs.append(p);
    });
    svg.append(defs);
    run().environment.bands.forEach((b) => {
      if (b.end < a || b.start > z) return;
      const idx = kinds.indexOf(b.kind),
        r = U.svgElement("rect", {
          x: x(Math.max(a, b.start)),
          y: 10 + idx % 3 * 15,
          width: Math.max(1, x(Math.min(z, b.end)) - x(Math.max(a, b.start))),
          height: 36,
          fill: `url(#hatch-${idx})`,
          stroke: color(idx),
        });
      r.append(
        U.svgElement(
          "title",
          {},
          `${b.label}: ${time(b.start)}–${time(b.end)}`,
        ),
      );
      svg.append(r);
    });
    for (let i = 0; i < 5; i++) {
      const t = a + (z - a) * i / 4;
      svg.append(
        U.svgElement(
          "text",
          { x: x(t), y: 92, "text-anchor": "middle" },
          time(t),
        ),
      );
    }
    const selection = U.svgElement("rect", {
      id: "brush-selection",
      class: "brush-selection",
      x: left,
      y: 8,
      width: 0,
      height: 64,
    });
    svg.append(selection);
    const guide = U.svgElement("line", {
      class: "crosshair",
      y1: 8,
      y2: 73,
      visibility: "hidden",
    });
    svg.append(guide);
    svg.dataset.timeChart = "bands";
    state.charts.set("bands", {
      svg,
      x,
      left,
      right,
      top: 8,
      bottom: 73,
      width,
      specs: [],
      guide,
    });
    $("band-legend").textContent =
      run().environment.bands.map((b) =>
        `${b.label}${
          b.direction
            ? ` ${{ ToBroker: "▸", FromBroker: "◂", Both: "◂▸" }[b.direction]}`
            : ""
        } (${time(b.start)}–${time(b.end)})`
      ).join(" · ") || "No environment fault windows.";
  }
  function renderTopology() {
    const r = run(),
      svg = $("topology"),
      width = U.chartWidth(svg),
      bs = r.topology.brokers,
      page = Number($("topology-page").value) || 0,
      start = page * 32,
      ps = r.topology.partitions.slice(start, start + 32),
      height = 90 + ps.length * 20;
    svg.replaceChildren();
    svg.setAttribute("viewBox", `0 0 ${width} ${height}`);
    svg.setAttribute("height", String(height));
    const bx = (b) =>
      110 + (bs.findIndex((v) => v.id === b) + .5) * (width - 130) / bs.length;
    const bucket = Math.min(
      r.buckets.count - 1,
      Math.floor(state.hover / r.buckets.bucket_ns),
    );
    bs.forEach((b) => {
      const isolated = r.environment.bands.some((x) =>
        x.broker === b.id && x.kind === "isolation" && state.hover >= x.start &&
        state.hover < x.end
      );
      svg.append(
        U.svgElement("rect", {
          x: bx(b.id) - 25,
          y: 10,
          width: 50,
          height: 32,
          rx: 5,
          fill: isolated ? "var(--selection-fill)" : "var(--muted)",
          "stroke-dasharray": isolated ? "3 2" : "",
        }),
        U.svgElement("text", {
          x: bx(b.id),
          y: 30,
          "text-anchor": "middle",
          class: "plot-label",
        }, `B${b.id}${isolated ? " ×" : ""}`),
      );
    });
    ps.forEach((p, j) => {
      const y = 70 + j * 20,
        leader = r.partitions.leader[start + j][bucket],
        label = `${p.topic_id.slice(0, 6)}… / ${p.partition}`;
      svg.append(U.svgElement("text", { x: 8, y: y + 4 }, label));
      if (leader !== null) {
        svg.append(
          U.svgElement("line", {
            x1: 102,
            x2: bx(leader),
            y1: y,
            y2: y,
            class: "partition-link",
            stroke: color(bs.findIndex((b) => b.id === leader)),
            "stroke-width": 1.4,
          }),
        );
        svg.append(
          U.svgElement("path", {
            d: `M${bx(leader) - 5},${y - 4} L${bx(leader)},${y} L${
              bx(leader) - 5
            },${y + 4}`,
            fill: "none",
            stroke: color(bs.findIndex((b) => b.id === leader)),
          }),
        );
      }
      const title = U.svgElement(
        "title",
        {},
        `Topic ${p.topic_id}, partition ${p.partition}, leader ${
          leader ?? "unavailable"
        }`,
      );
      svg.lastChild?.append(title);
    });
    $("topology-note").textContent = `${
      time(state.hover)
    } · bucket-resolved leader state · ${r.topology.brokers.length} brokers · ${r.topology.partitions.length} immutable partition identities · lanes ${
      start + 1
    }–${start + ps.length}`;
  }
  function renderEcdf() {
    const rs = selected(),
      max = Math.max(
        1,
        ...rs.map((i) => state.bundle.runs[i].summary.latency_acked.max ?? 0),
      );
    chart(
      "ecdf",
      rs.map((i) => {
        const r = state.bundle.runs[i];
        return {
          label: r.meta.variant.name,
          color: color(i),
          dash: pattern(i),
          points: r.distributions.latency_ecdf.points.map((p) => ({
            x: Math.log10(1 + p.latency),
            y: 100 * p.cumulative_count /
              Math.max(1, r.distributions.latency_ecdf.population_count),
          })),
        };
      }),
      {
        timeAxis: false,
        domain: [0, Math.log10(1 + max)],
        max: 100,
        unit: "cumulative acknowledgments (%) · logarithmic latency axis",
        xFormat: (v) => time(10 ** v - 1),
      },
    );
    $("ecdf-note").textContent = rs.map((i) => {
      const r = state.bundle.runs[i], e = r.distributions.latency_ecdf;
      return `${r.meta.variant.name}: ${
        fmt(e.population_count)
      } acknowledgments, ${e.points.length} exact-rank points${
        e.reduced ? " (curve reduced)" : ""
      }`;
    }).join(" · ");
  }
  function renderBrokers() {
    const parent = $("brokers");
    parent.replaceChildren();
    run().buckets.brokers.forEach((b, i) => {
      parent.append(el("h3", `Broker ${b.broker}`, "broker-heading"));
      for (const suffix of ["inflight", "rtt"]) {
        const svg = U.svgElement("svg", {
          id: `broker-${b.broker}-${suffix}`,
          role: "img",
          "aria-label": `Broker ${b.broker} ${suffix}`,
        });
        parent.append(svg);
        const readout = el("p", null, "chart-readout");
        readout.dataset.readout = svg.id;
        parent.append(readout);
      }
      chart(`broker-${b.broker}-inflight`, [
        line(run(), b.client_inflight_max, "in-flight max", i, { area: true }),
        line(run(), b.disconnects, "disconnect ticks", 1, { kind: "ticks" }),
        line(run(), b.setup_failures, "setup-failure ticks", 3, {
          kind: "ticks",
        }),
        line(run(), b.drops, "drop ticks", 4, { kind: "ticks" }),
      ], { unit: "requests / event counts", height: 140 });
      chart(`broker-${b.broker}-rtt`, [
        line(run(), b.dispatch_rtt.p99, "dispatch RTT p99", i),
        line(run(), b.full_write_rtt.p99, "full-write RTT p99", (i + 1) % 6),
      ], { unit: "RTT", height: 140 });
    });
  }
  function canvas(id, height) {
    const c = $(id),
      width = Math.min(
        8192,
        Math.max(260, c.parentElement.getBoundingClientRect().width),
      ),
      h = Math.min(8192, height);
    const ratio = Math.min(
      2,
      Math.max(1, root.devicePixelRatio || 1),
      Math.sqrt(16 * 1024 * 1024 / (width * h)),
    );
    c.width = Math.floor(width * ratio);
    c.height = Math.floor(h * ratio);
    c.style.height = `${h}px`;
    const ctx = c.getContext("2d");
    if (!ctx) throw new Error("Canvas rendering is unavailable");
    ctx.scale(ratio, ratio);
    return { c, ctx, width, height: h };
  }
  function canvasGuide(id) {
    let guide = $(`${id}-cursor`);
    if (!guide) {
      guide = el("div", null, "canvas-crosshair");
      guide.id = `${id}-cursor`;
      guide.setAttribute("aria-hidden", "true");
      $(id).parentElement.append(guide);
    }
    return guide;
  }
  function cssColor(name) {
    // Computed color resolves light-dark() and custom properties for canvas.
    const probe = el("span");
    probe.style.color = `var(${name})`;
    probe.hidden = true;
    $("experiment").append(probe);
    const value = getComputedStyle(probe).color;
    probe.remove();
    return value;
  }
  function renderHeatmap() {
    const r = run(),
      start = (Number($("heatmap-page").value) || 0) * 64,
      ps = r.topology.partitions.slice(start, start + 64),
      { ctx, width, height } = canvas(
        "heatmap",
        Math.max(100, 30 + ps.length * 18),
      ),
      left = 100,
      pw = width - left - 16,
      [a, z] = state.view,
      x = (t) => left + (t - a) / Math.max(1, z - a) * pw;
    const slice = M.derive.bucketSlice(r, state.view),
      max = Math.max(
        1,
        ...r.partitions.acked.map((row) =>
          Math.max(...row.slice(slice.start, slice.end))
        ),
      ),
      ink = cssColor("--foreground"),
      fill = cssColor("--variant-1");
    ctx.font = "11px system-ui";
    ctx.fillStyle = ink;
    ps.forEach((p, row) => {
      const j = start + row, y = 22 + row * 18;
      ctx.fillStyle = ink;
      ctx.globalAlpha = 1;
      ctx.fillText(`${p.topic_id.slice(0, 6)}… / ${p.partition}`, 2, y + 11);
      for (let i = slice.start; i < slice.end; i++) {
        const start = Math.max(a, i * r.buckets.bucket_ns),
          end = Math.min(z, (i + 1) * r.buckets.bucket_ns);
        if (end < start) continue;
        ctx.fillStyle = fill;
        ctx.globalAlpha = .06 + .94 * r.partitions.acked[j][i] / max;
        ctx.fillRect(x(start), y, Math.max(.4, x(end) - x(start)), 15);
        if (
          i > 0 && r.partitions.leader[j][i] !== r.partitions.leader[j][i - 1]
        ) {
          ctx.globalAlpha = 1;
          ctx.fillStyle = ink;
          ctx.fillRect(x(start), y, 1, 15);
        }
      }
    });
    ctx.globalAlpha = 1;
    state.charts.set("heatmap", {
      svg: $("heatmap"),
      left,
      right: width - 16,
      width,
      top: 22,
      bottom: height,
      partitionStart: start,
      partitionEnd: start + ps.length,
      heat: true,
      guide: canvasGuide("heatmap"),
    });
    $("heat-readout").textContent = "";
    $("heatmap").dataset.timeChart = "heatmap";
  }
  function renderPressure() {
    const c = run().buckets.global.credits;
    chart(
      "credits",
      [...state.pools].map((p, i) =>
        line(
          run(),
          c.held_observed_max[p].map((v) =>
            c.capacity[p] ? 100 * v / c.capacity[p] : 0
          ),
          c.pools[p],
          i,
        )
      ),
      { unit: "observed pool utilization (%)", max: 100 },
    );
    chart("pressure", [
      line(
        run(),
        run().buckets.global.outstanding,
        "outstanding at bucket end",
        0,
      ),
      line(run(), run().buckets.global.refused, "refusals", 1, {
        kind: "bar",
        bucketWidth: run().buckets.bucket_ns,
      }),
    ]);
  }
  function renderAttempts() {
    const rs = selected(),
      max = Math.max(
        1,
        ...rs.flatMap((i) =>
          state.bundle.runs[i].distributions.attempts_histogram.map((h) =>
            h.producer_attempts
          )
        ),
      );
    chart(
      "attempts",
      rs.map((i) => ({
        label: state.bundle.runs[i].meta.variant.name,
        color: color(i),
        points: state.bundle.runs[i].distributions.attempts_histogram.map(
          (h) => ({ x: h.producer_attempts, y: h.count }),
        ),
        kind: "bar",
        grouped: true,
      })),
      {
        timeAxis: false,
        domain: [-.5, max + .5],
        log: true,
        unit: "accepted records · x = producer attempts",
      },
    );
  }
  function renderReasons() {
    state.reasonRows = selected().flatMap((i) =>
      state.bundle.runs[i].distributions.outcomes_by_reason.map((h) => ({
        run: i,
        ...h,
      }))
    );
    const select = $("reason-page"),
      old = Number(select.value) || 0,
      pages = Math.ceil(state.reasonRows.length / 32);
    select.replaceChildren();
    for (let i = 0; i < Math.max(1, pages); i++) {
      option(select, i, `${i + 1} / ${Math.max(1, pages)}`);
    }
    select.value = String(Math.min(old, Math.max(0, pages - 1)));
    const rows = state.reasonRows.slice(
        Number(select.value) * 32,
        Number(select.value) * 32 + 32,
      ),
      svg = $("reasons"),
      width = U.chartWidth(svg),
      left = Math.min(280, width * .55),
      max = Math.max(1, ...rows.map((r) => r.count));
    svg.replaceChildren();
    svg.setAttribute("viewBox", `0 0 ${width} ${rows.length * 24 + 20}`);
    svg.setAttribute("height", String(rows.length * 24 + 20));
    rows.forEach((r, i) => {
      const label = `${state.bundle.runs[r.run].meta.variant.name} · ${
        run().records.outcome_names[r.outcome]
      } · ${r.reason}`;
      const text = U.svgElement(
        "text",
        { x: 5, y: 17 + i * 24 },
        label.length > 36 ? label.slice(0, 33) + "…" : label,
      );
      text.append(U.svgElement("title", {}, label));
      svg.append(
        text,
        U.svgElement("rect", {
          x: left,
          y: 5 + i * 24,
          width: (width - left - 60) * r.count / max,
          height: 16,
          fill: color(r.run),
        }),
        U.svgElement("text", {
          x: width - 5,
          y: 17 + i * 24,
          "text-anchor": "end",
        }, fmt(r.count)),
      );
    });
    table(
      $("reason-table"),
      ["Run", "Outcome", "Reason", "Records"],
      rows.map(
        (r) => [
          state.bundle.runs[r.run].meta.variant.name,
          run().records.outcome_names[r.outcome],
          r.reason,
          fmt(r.count),
        ],
      ),
    );
  }
  function renderBatching() {
    const r = run(), g = r.buckets.global;
    chart("batch-records", [
      line(r, g.records_per_request_mean, "mean records / request", 0),
      line(r, g.commit_records, "committed records / bucket", 2),
      line(r, g.commit_batches, "committed batches / bucket", 3),
    ]);
    chart("batch-bytes", [
      line(r, g.bytes_wire, "completed wire bytes", 0, {
        kind: "bar",
        bucketWidth: r.buckets.bucket_ns,
      }),
    ], { unit: "bytes / bucket" });
    const target = $("hdr-charts");
    target.replaceChildren();
    if (!r.hdr) {
      target.append(el("p", "No HDR intervals were recorded.", "caption"));
      return;
    }
    const h = r.hdr, scope = h.scopes.findIndex((s) => s.kind === "global");
    for (const metric of ["BatchFillNanos", "QueueWaitNanos"]) {
      const mi = h.metric_names.indexOf(metric),
        s = h.series.find((s) => s.scope === scope && s.metric === mi);
      if (!s) continue;
      target.append(
        el("h3", `${metric} · global owner intervals`, "broker-heading"),
      );
      const id = `hdr-${mi}`;
      target.append(
        U.svgElement("svg", {
          id,
          role: "img",
          "aria-label": `${metric} HDR p99 equivalent ranges`,
        }),
      );
      const readout = el("p", null, "chart-readout");
      readout.dataset.readout = id;
      target.append(readout);
      chart(id, [{
        label: "p99 equivalent-value range",
        color: color(2),
        band: true,
        points: s.p99_range.map((q, i) => ({
          x: h.intervals.end[i] ?? h.intervals.taken[i],
          y: q?.[0] ?? null,
          high: q?.[1] ?? null,
          count: s.count[i],
          interval: i,
        })),
      }], { unit: "duration (HDR equivalent range)" });
    }
    target.append(
      el(
        "p",
        `${h.intervals.count} actual intervals · ${h.missed_requests.length} missed snapshot requests · ${h.config.significant_digits} significant digits. Scope omissions and overflow diagnostics are in the configuration details.`,
        "caption",
      ),
    );
  }
  function renderEvents() {
    U.renderOperationTimeline({
      svg: $("events"),
      steps: state.steps,
      selectedIndex: state.selectedMarker,
      domain: {
        minimum: BigInt(state.view[0]),
        maximum: BigInt(state.view[1]),
      },
      title: "Producer environment and control events",
      description: "Select a marker or use the event navigation controls.",
      onSelect: (i) => navigator.select(i),
    });
    const m = run().environment.markers[state.selectedMarker];
    U.renderJsonDump(
      $("event-detail"),
      m ?? { message: "No events in this run" },
    );
    $("events-note").textContent =
      `${state.steps.length} retained markers · ${run().environment.markers_truncated} omitted by the report cap. Markers outside the selected window are hidden.`;
  }
  function renderSummary() {
    const rows = state.bundle.runs.map((r) => {
      const s = r.summary;
      return [
        r.meta.variant.name,
        r.meta.seed,
        r.meta.size,
        r.meta.replay_verified ? "verified" : "unverified",
        ...["offered", "accepted", "refused", "acked", "not_written", "unknown"]
          .map((k) => fmt(s.records[k])),
        time(s.latency_acked.p50),
        time(s.latency_acked.p99),
        time(s.latency_acked.max),
        fmt(s.client_requests),
        fmt(s.client_retry_requests),
        fmt(s.bytes_wire),
        fmt(s.peak_outstanding),
      ];
    });
    table($("summary"), [
      "Variant",
      "Seed",
      "Size",
      "Replay",
      "Offered",
      "Accepted",
      "Refused",
      "Acked",
      "Not written",
      "Unknown",
      "Ack p50",
      "Ack p99",
      "Ack max",
      "Client requests",
      "Retry requests",
      "Wire bytes",
      "Peak outstanding",
    ], rows);
  }
  function inspectRecord() {
    const r = run().records,
      i = Math.max(
        0,
        Math.min(r.count - 1, Number($("record-index").value) - 1),
      );
    if (!r.count) {
      U.renderJsonDump($("record-detail"), { message: "No record rows" });
      return;
    }
    $("record-index").value = String(i + 1);
    const row = {};
    for (
      const k of [
        "record_id",
        "due",
        "offer",
        "accept",
        "deliver",
        "producer_attempts",
        "client_dispatches",
        "partition",
        "broker",
        "offset",
      ]
    ) row[k] = r[k][i];
    row.outcome = r.outcome_names[r.outcome[i]];
    row.reason = r.reason_names[r.reason[i]];
    U.renderJsonDump($("record-detail"), row);
  }
  function renderScatter() {
    if (!$("scatter-section").open) return;
    const r = run(),
      s = M.derive.sampledRecordSlice(r, state.view),
      rows = r.records,
      { ctx, width } = canvas("scatter", 280),
      left = 68,
      right = width - 16,
      bottom = 248,
      [a, z] = state.view;
    let max = 1;
    for (const i of s.indices) {
      if (rows.deliver[i] !== null) {
        max = Math.max(max, rows.deliver[i] - rows.accept[i]);
      }
    }
    const colors = ["--variant-1", "--variant-2", "--variant-4"].map(cssColor);
    ctx.globalAlpha = .45;
    for (const i of s.indices) {
      if (rows.deliver[i] !== null) {
        const x = left +
            (rows.offer[i] - a) / Math.max(1, z - a) * (right - left),
          y = bottom - (rows.deliver[i] - rows.accept[i]) / max * 220;
        ctx.fillStyle = colors[rows.outcome[i]];
        ctx.fillRect(x - 1, y - 1, 2, 2);
      }
    }
    ctx.globalAlpha = 1;
    ctx.fillStyle = cssColor("--foreground");
    ctx.font = "11px system-ui";
    ctx.fillText(time(max), 2, 22);
    ctx.fillText("0", 24, bottom);
    ctx.fillText(time(a), left, 272);
    ctx.textAlign = "right";
    ctx.fillText(time(z), right, 272);
    $("sample-note").textContent = `${fmt(rows.count)} retained rows / ${
      fmt(rows.population_count)
    } offered records (${
      rows.complete ? "complete" : "evenly spaced record-ID rank sample"
    }). Window contains ${fmt(s.indices.length)} sampled offers; ${
      s.counts.map((v, i) => `${rows.outcome_names[i]} ${fmt(v)}`).join(", ")
    }. Sample mean delivery latency: ${
      time(s.meanDeliveryLatency)
    }. X = actual offer time, Y = acceptance-to-delivery latency. Refusals have no delivery latency and are excluded from dots.`;
    $("record-index").max = String(rows.count);
    inspectRecord();
    $("scatter").dataset.timeChart = "scatter";
    state.charts.set("scatter", {
      svg: $("scatter"),
      left,
      right,
      width,
      top: 20,
      bottom,
      scatter: true,
      guide: canvasGuide("scatter"),
    });
  }
  function render() {
    if (!state.bundle) return;
    if (state.bundle.schema === "kr-producer-comparison/v1") {
      comparison.render();
      return;
    }
    state.charts.clear();
    const r = run(), g = r.buckets.global, w = r.buckets.bucket_ns;
    $("view-start").value = String(state.view[0] / 1e6);
    $("view-end").value = String(state.view[1] / 1e6);
    const slice = M.derive.bucketSlice(r, state.view);
    $("view-caption").textContent = `${time(state.view[0])}–${
      time(state.view[1])
    } · bucket-selected counts include ${time(slice.startNs)}–${
      time(slice.endNs)
    } at ${time(w)} resolution. Fault assertions use exact timestamps in Rust.`;
    renderBands();
    renderTopology();
    const stacks = ["acked", "not_written", "unknown"].map((k, j) =>
      line(r, g[k], k, [0, 1, 3][j], {
        kind: "bar",
        bucketWidth: w,
        points: points(r, g[k]).map((p, i) => ({
          ...p,
          base: j === 0 ? 0 : g.acked[i] + (j === 2 ? g.not_written[i] : 0),
        })),
      })
    );
    chart("outcomes", [
      ...stacks,
      ...selected().filter((i) => i !== state.primary).map((i) =>
        line(
          state.bundle.runs[i],
          state.bundle.runs[i].buckets.global.acked,
          `${state.bundle.runs[i].meta.variant.name} acked`,
          i,
        )
      ),
    ], { stack: true });
    chart("admission", [
      line(r, g.refused, "refused", 1, { kind: "bar", bucketWidth: w }),
      line(r, g.offered_due, "offered due", 0),
      line(r, g.offered_actual, "offered actual", 3),
      line(r, g.accepted, "accepted", 2),
    ]);
    chart("latency", [
      line(r, g.latency.p50, "p50–p99", 0, {
        band: true,
        points: points(r, g.latency.p50).map((p, i) => ({
          ...p,
          high: g.latency.p99[i],
        })),
      }),
      line(r, g.latency.p90, "p90", 2),
      line(r, g.latency.max, "max", 1, { kind: "dots" }),
    ], { unit: "latency", log: $("latency-log").checked });
    renderEcdf();
    renderBrokers();
    renderHeatmap();
    renderPressure();
    renderAttempts();
    renderReasons();
    renderBatching();
    renderEvents();
    renderSummary();
    renderScatter();
    updateHover();
  }
  function nearest(points, t) {
    let best = null, dist = Infinity;
    for (const p of points) {
      const d = Math.abs(p.x - t);
      if (d < dist) {
        dist = d;
        best = p;
      }
    }
    return best;
  }
  function updateHover(event = null, chartId = null) {
    const t = state.hover, lines = [`Time ${time(t)}`];
    for (const [id, c] of state.charts) {
      if (c.heat || c.scatter) {
        const f = (t - state.view[0]) /
          Math.max(1, state.view[1] - state.view[0]);
        c.guide.style.left = `${c.left + f * (c.right - c.left)}px`;
        c.guide.style.top = `${(c.svg.offsetTop ?? 0) + c.top}px`;
        c.guide.style.height = `${c.bottom - c.top}px`;
        c.guide.hidden = f < 0 || f > 1;
        continue;
      }
      if (c.guide?.isConnected) {
        const x = c.x(t);
        c.guide.setAttribute("x1", String(x));
        c.guide.setAttribute("x2", String(x));
        c.guide.setAttribute(
          "visibility",
          t >= state.view[0] && t <= state.view[1] ? "visible" : "hidden",
        );
      }
      if (!c.specs) continue;
      const details = c.specs.map((s) => {
        const p = s.bucketWidth
          ? (t <= s.duration
            ? s
              .points[
                Math.min(s.points.length - 1, Math.floor(t / s.bucketWidth))
              ]
            : null)
          : nearest(s.points, t);
        return `${s.label}: ${
          p
            ? ((c.unit ?? "").startsWith("latency") ||
                (c.unit ?? "").includes("RTT") ||
                (c.unit ?? "").includes("duration")
              ? time(p.y)
              : fmt(p.y))
            : "—"
        }${p?.high !== undefined ? `–${time(p.high)}` : ""}${
          p?.count !== undefined ? ` (n=${fmt(p.count)})` : ""
        }`;
      });
      const out = document.querySelector(`[data-readout="${id}"]`);
      if (out) out.textContent = `${time(t)} · ${details.join(" · ")}`;
      if (id === chartId) lines.push(...details);
    }
    const b = Math.min(
      run().buckets.count - 1,
      Math.floor(t / run().buckets.bucket_ns),
    );
    if (chartId === "credits") {
      for (const p of state.pools) {
        const c = run().buckets.global.credits;
        lines.push(
          `${c.pools[p]}: max ${fmt(c.held_observed_max[p][b])}, last ${
            fmt(c.held_last_observed[p][b])
          }, capacity ${fmt(c.capacity[p])}`,
        );
      }
    }
    if (chartId === "bands") {
      lines.push(
        ...run().environment.bands.filter((b) => t >= b.start && t < b.end).map(
          (b) => b.label,
        ),
      );
    }
    if (chartId === "heatmap" && event) {
      const c = state.charts.get(chartId),
        rect = c.svg.getBoundingClientRect(),
        row = Math.floor((event.clientY - rect.top - 22) / 18),
        p = c.partitionStart + row;
      if (row >= 0 && p < c.partitionEnd) {
        const part = run().topology.partitions[p];
        lines.push(
          `Topic ${part.topic_id} / partition ${part.partition}`,
          `Acked ${fmt(run().partitions.acked[p][b])}, leader ${
            run().partitions.leader[p][b] ?? "—"
          }`,
        );
        $("heat-readout").textContent = lines.join(" · ");
      }
    }
    if (chartId === "scatter") {
      const rows = run().records;
      let best = 0, dist = Infinity;
      for (let i = 0; i < rows.count; i++) {
        const d = Math.abs(rows.offer[i] - t);
        if (d < dist) {
          best = i;
          dist = d;
        }
      }
      lines.push(`Nearest retained row: ${rows.record_id[best] ?? "—"}`);
      $("scatter-readout").textContent = lines.join(" · ");
    }
    if (state.bundle.schema !== "kr-producer-comparison/v1") renderTopology();
    if (event) {
      const tip = $("tooltip");
      tip.textContent = lines.join("\n");
      tip.hidden = false;
      const rect = tip.getBoundingClientRect();
      tip.style.left = `${
        Math.max(
          12,
          Math.min(root.innerWidth - rect.width - 12, event.clientX + 16),
        )
      }px`;
      tip.style.top = `${
        Math.max(
          12,
          Math.min(root.innerHeight - rect.height - 12, event.clientY + 16),
        )
      }px`;
    }
  }
  function setView(range) {
    state.view = M.derive.viewRange(run(), range);
    if (state.view[0] === state.view[1]) {
      state.view = [
        Math.max(0, state.view[0] - 1),
        Math.min(run().meta.duration, state.view[1] + 1),
      ];
    }
    state.hover = Math.max(state.view[0], Math.min(state.view[1], state.hover));
    render();
  }
  function shiftWindow(delta, scale = 1) {
    const [a, z] = state.view,
      d = run().meta.duration,
      span = Math.min(d, Math.max(1, (z - a) * scale)),
      center = (a + z) / 2 + delta * (z - a),
      start = Math.max(0, Math.min(d - span, center - span / 2));
    setView([Math.floor(start), Math.ceil(start + span)]);
  }
  function selectPrimary(i) {
    state.primary = i;
    state.overlay.delete(i);
    state.view = [0, run().meta.duration];
    state.hover = 0;
    state.selectedMarker = 0;
    state.steps = M.derive.markersAsSteps(run());
    state.pools = new Set(
      [0, 1].filter((p) => p < run().buckets.global.credits.pools.length),
    );
    for (const [id, size] of [["topology-page", 32], ["heatmap-page", 64]]) {
      $(id).replaceChildren();
      for (
        let i = 0;
        i < Math.max(1, Math.ceil(run().topology.partitions.length / size));
        i++
      ) {
        option(
          $(id),
          i,
          `${i * size + 1}–${
            Math.min((i + 1) * size, run().topology.partitions.length)
          }`,
        );
      }
      $(id).value = "0";
    }
    $("pools").replaceChildren();
    run().buckets.global.credits.pools.forEach((p, i) => {
      const label = el("label"), input = el("input");
      input.type = "checkbox";
      input.checked = state.pools.has(i);
      input.addEventListener("change", () => {
        if (input.checked) {
          if (state.pools.size >= 6) {
            input.checked = false;
            return;
          }
          state.pools.add(i);
        } else state.pools.delete(i);
        render();
      });
      label.append(input, document.createTextNode(p));
      $("pools").append(label);
    });
    const r = run();
    const overview = M.derive.experimentOverview(r, state.bundle);
    $("experiment-intent").textContent = overview.intent;
    $("experiment-comparison").textContent = overview.comparison;
    $("experiment-primary").textContent =
      `Setup shown for primary run ${r.meta.variant.name}, seed ${r.meta.seed} (${r.meta.size}). Change the primary run below to update these values.`;
    for (const key of ["settings", "transport", "capacity"]) {
      $(`experiment-${key}`).textContent = overview[key];
    }
    for (const key of ["traffic", "faults", "reading"]) {
      $(`experiment-${key}`).replaceChildren(
        ...overview[key].map((text) =>
          el(key === "reading" ? "p" : "li", text)
        ),
      );
    }
    $("runtime").textContent = `Seed ${r.meta.seed} · ${r.meta.size} · ${
      time(r.meta.duration)
    } virtual time · replay ${
      r.meta.replay_verified ? "verified" : "not verified"
    } · source ${r.meta.source.source_sha256.slice(0, 12)} · page ${
      state.bundle.page.index + 1
    }/${state.bundle.page.count}`;
    U.renderJsonDump($("provenance"), {
      meta: r.meta,
      config: r.config,
      phase_evidence: r.phase_evidence,
      comparisons: state.bundle.comparisons,
      hdr: r.hdr
        ? {
          config: r.hdr.config,
          diagnostics: r.hdr.diagnostics,
          missed_requests: r.hdr.missed_requests,
        }
        : null,
    });
    renderRunControls();
    render();
    navigator.setItems(state.steps.length);
  }
  function renderRunControls() {
    const target = $("runs");
    target.replaceChildren();
    state.bundle.runs.forEach((r, i) => {
      const row = el("div", null, "run-row"),
        radio = el("input"),
        check = el("input"),
        p = el("label"),
        o = el("label");
      radio.type = "radio";
      radio.name = "primary";
      radio.checked = i === state.primary;
      radio.addEventListener("change", () => selectPrimary(i));
      check.type = "checkbox";
      check.checked = state.overlay.has(i);
      check.disabled = i === state.primary;
      check.addEventListener("change", () => {
        if (check.checked) {
          if (state.overlay.size >= 5) {
            check.checked = false;
            return;
          }
          state.overlay.add(i);
        } else state.overlay.delete(i);
        render();
      });
      p.append(
        radio,
        document.createTextNode(
          `Primary: ${r.meta.variant.name} / seed ${r.meta.seed}`,
        ),
      );
      o.append(check, document.createTextNode("Overlay"));
      const swatch = U.svgElement("svg", {
        class: "run-swatch",
        viewBox: "0 0 36 10",
        "aria-hidden": "true",
      });
      swatch.append(
        U.svgElement("line", {
          x1: 0,
          x2: 36,
          y1: 5,
          y2: 5,
          stroke: color(i),
          "stroke-width": 3,
          "stroke-dasharray": pattern(i),
        }),
      );
      row.append(
        swatch,
        p,
        o,
        el("code", JSON.stringify(r.meta.variant.deltas)),
      );
      target.append(row);
    });
  }
  const navigator = U.createStepNavigator({
    previousButton: $("previous"),
    nextButton: $("next"),
    rangeInput: $("step-range"),
    rangeOutput: $("step-output"),
    keyboardTarget: document,
    describe: (i) =>
      `${i + 1} / ${state.steps.length} · ${state.steps[i]?.operation ?? ""}`,
    onSelect: (i) => {
      state.selectedMarker = i;
      const t = Number(state.steps[i]._startedAt);
      state.hover = t;
      if (t < state.view[0] || t > state.view[1]) {
        const span = state.view[1] - state.view[0];
        setView([
          Math.max(0, t - span / 2),
          Math.min(run().meta.duration, t + span / 2),
        ]);
      } else {
        renderEvents();
        updateHover();
      }
    },
  });
  const comparison = root.PRODUCER_COMPARISON_VIEWER.create({
    state,
    $,
    el,
    option,
    table,
    chart,
    line,
    time,
    fmt,
    renderBands,
    updateHover,
    setView,
    navigator,
  });
  function install(value) {
    const bundle = value.runs?.[0]?.exact ? value : M.validateData(value);
    if (bundle.schema === "kr-producer-comparison/v1") {
      comparison.install(bundle);
      return;
    }
    comparison.mode(false);
    state.bundle = bundle;
    state.overlay.clear();
    $("scenario").textContent = bundle.scenario.title;
    $("category").textContent = bundle.scenario.category;
    $("description").textContent = bundle.scenario.description;
    $("look-for").textContent = bundle.scenario.what_to_look_for;
    selectPrimary(0);
  }
  const loader = U.createArtifactLoader({
    fileInput: $("trace-file"),
    resetButton: $("bundled-sample"),
    sourceElement: $("trace-source"),
    errorElement: $("error"),
    bundledData: root.PRODUCER_EXPERIMENT_DATA,
    parseText: M.parseArtifactText,
    installData: install,
    description: "Producer experiment",
    maxFileBytes: 48 * 1024 * 1024,
  });
  let scheduled = false, pendingPointer = null;
  $("experiment").addEventListener("pointermove", (e) => {
    const target = e.target.closest?.("[data-time-chart]");
    if (!target) {
      $("tooltip").hidden = true;
      return;
    }
    pendingPointer = {
      x: e.clientX,
      y: e.clientY,
      id: target.dataset.timeChart,
    };
    if (scheduled) return;
    scheduled = true;
    root.requestAnimationFrame(() => {
      scheduled = false;
      if (!state.bundle || !pendingPointer) return;
      const p = pendingPointer, c = state.charts.get(p.id);
      if (!c) return;
      const rect = c.svg.getBoundingClientRect(),
        px = (p.x - rect.left) * c.width / rect.width,
        f = Math.max(0, Math.min(1, (px - c.left) / (c.right - c.left)));
      state.hover = Math.round(
        state.view[0] + f * (state.view[1] - state.view[0]),
      );
      updateHover({ clientX: p.x, clientY: p.y }, p.id);
    });
  });
  $("experiment").addEventListener("pointerleave", () => {
    $("tooltip").hidden = true;
  });
  let brush = null;
  function pointerTime(e) {
    const c = state.charts.get("bands"),
      rect = c.svg.getBoundingClientRect(),
      px = (e.clientX - rect.left) * c.width / rect.width;
    return Math.round(
      state.view[0] +
        Math.max(0, Math.min(1, (px - c.left) / (c.right - c.left))) *
          (state.view[1] - state.view[0]),
    );
  }
  $("bands").addEventListener("pointerdown", (e) => {
    if (e.button !== 0) return;
    brush = pointerTime(e);
    $("bands").setPointerCapture(e.pointerId);
  });
  $("bands").addEventListener("pointermove", (e) => {
    if (brush === null) return;
    const t = pointerTime(e),
      c = state.charts.get("bands"),
      box = $("brush-selection");
    box.setAttribute("x", String(c.x(Math.min(brush, t))));
    box.setAttribute("width", String(Math.abs(c.x(t) - c.x(brush))));
  });
  $("bands").addEventListener("pointerup", (e) => {
    if (brush === null) return;
    const start = brush;
    brush = null;
    const end = pointerTime(e);
    if (Math.abs(end - start) > 1000) {
      setView([Math.min(start, end), Math.max(start, end)]);
    }
  });
  $("bands").addEventListener("pointercancel", () => {
    brush = null;
    $("brush-selection")?.setAttribute("width", "0");
  });
  $("apply-view").addEventListener("click", () => {
    try {
      setView([
        Number($("view-start").value) * 1e6,
        Number($("view-end").value) * 1e6,
      ]);
    } catch (e) {
      loader.showError(e);
    }
  });
  $("reset-view").addEventListener(
    "click",
    () => setView([0, run().meta.duration]),
  );
  $("latency-log").addEventListener("change", render);
  $("topology-page").addEventListener("change", renderTopology);
  $("heatmap-page").addEventListener("change", renderHeatmap);
  $("reason-page").addEventListener("change", renderReasons);
  $("scatter-section").addEventListener("toggle", renderScatter);
  $("record-index").addEventListener("change", inspectRecord);
  document.addEventListener("keydown", (e) => {
    if (
      e.defaultPrevented || e.isComposing || e.altKey || e.ctrlKey ||
      e.metaKey || e.shiftKey ||
      e.target.closest?.(
        "input,select,textarea,button,[contenteditable]:not([contenteditable='false'])",
      )
    ) return;
    if (e.key === "[") shiftWindow(-.25);
    else if (e.key === "]") shiftWindow(.25);
    else if (e.key === "-") shiftWindow(0, 1.5);
    else if (e.key === "=") shiftWindow(0, .67);
    else if (e.key === "0") setView([0, run().meta.duration]);
    else return;
    e.preventDefault();
  });
  U.observeResize($("experiment"), render);
  root.matchMedia("(prefers-color-scheme: dark)").addEventListener(
    "change",
    render,
  );
  try {
    loader.installBundled();
  } catch (e) {
    loader.showError(e);
  }
})(globalThis);
