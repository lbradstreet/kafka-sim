"use strict";
(function installProducerComparisonViewer(root) {
  root.PRODUCER_COMPARISON_VIEWER = Object.freeze({
    create(
      {
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
      },
    ) {
      const M = root.PRODUCER_EXPERIMENT_MODEL, U = root.TRACE_VIEWER_UI;
      const hiddenHeadings = [
        "runs",
        "topology",
        "outcomes",
        "admission",
        "latency",
        "ecdf",
        "brokers",
        "heat",
        "pressure",
        "attempts",
        "reasons",
        "batching",
        "events",
        "summary",
      ];
      let bundle = null, pair = null;
      const names = { classic: "Classic Java", native: "Native / Panama" };
      function mode(on) {
        $("experiment-setup").open = !on;
        for (const id of hiddenHeadings) {
          $(`${id}-heading`).closest("section").hidden = on;
        }
        $("scatter-section").hidden = on;
        $("comparison-selection").hidden = !on;
        $("comparison-plots").hidden = !on;
        $("time-help").textContent = on
          ? "Drag across the fault strip to select a shared window. [ / ] pan · − / = zoom · 0 reset. Hover a time chart to compare both producers at the same instant."
          : "Drag across the fault strip to select a window. [ / ] pan · − / = zoom · 0 reset · ← / → select an event. Hover any time chart to align the cursor.";
      }
      function list(id, values) {
        $(id).replaceChildren(...values.map((value) => el("li", value)));
      }
      function select(index) {
        pair = bundle.pairs[index];
        $("comparison-pair").value = String(index);
        state.bundle = {
          ...bundle,
          runs: Object.entries(pair.runs).map(([adapter, r]) => ({
            ...r,
            adapter,
            meta: { duration: pair.duration_ns },
            environment: pair.environment,
            buckets: {
              bucket_ns: pair.bucket_ns,
              count: pair.bucket_count,
              global: r.buckets,
            },
          })),
        };
        state.primary = 0;
        state.pools.clear();
        state.view = [0, pair.duration_ns];
        state.hover = 0;
        state.steps = [];
        navigator.setItems(0);
        $("scenario").textContent =
          `Classic Java vs native · ${bundle.scenario}`;
        $("category").textContent = `${pair.size} · ${pair.profile}`;
        $("description").textContent =
          "Both producers run the same effective scenario through one Java workload driver and the shared Rust broker, byte streams and fault engine.";
        $("experiment-intent").textContent = M.derive.scenarioIntent(
          bundle.scenario,
        );
        $("look-for").textContent =
          "Compare the colored lines at the same time, especially through shaded faults and recovery. Admission refusals and accepted-record failures are separate populations.";
        $("runtime").textContent =
          `Seed ${pair.seed} · complete replay verified · ${pair.bucket_count} shared buckets · exact origin ${pair.origin_ns} ns`;
        $("experiment-comparison").textContent =
          `Matched variant ${pair.variant}; ${pair.profile} profile; ${pair.size} workload. Classic Java is blue and solid; native / Panama is orange and dashed.`;
        $("comparison-setup").textContent = `${
          pair.setup.settings[0]
        } ${pair.setup.loads.length} source phase(s); ${pair.environment.bands.length} fault/pause window(s). Expand Experiment setup for their exact schedules.`;
        $("experiment-primary").textContent =
          "Both implementations are always overlaid. Counts and percentiles use the entire population; bucket quantiles are calculated before presentation.";
        list("experiment-traffic", pair.setup.loads);
        $("experiment-settings").textContent = pair.setup.settings[0];
        $("experiment-transport").textContent = pair.setup.settings.slice(1)
          .join(" ");
        $("experiment-capacity").textContent = pair.profile === "common"
          ? `Applied common-profile adjustments: ${
            pair.setup.adjustments.join("; ")
          }.`
          : "Original manifest retained. Native-only controls remain visible in the comparison limits below.";
        list("experiment-faults", [
          ...pair.environment.bands.map((b) =>
            `${time(b.start)}–${time(b.end)}: ${b.label}`
          ),
          ...pair.environment.markers.map((m) => `${time(m.at)}: ${m.label}`),
          ...(pair.environment.bands.length || pair.environment.markers.length
            ? []
            : ["No scheduled fault or topology change."]),
        ]);
        $("experiment-reading").replaceChildren(...[
          "Open-loop arrivals use the same due-time schedule. Closed-loop counts may differ because consumed deliveries release source credit. Test loads preserve fault phases but are smaller than Full loads; empty intervals may be scheduled source silence.",
          "Latency is acceptance to application consumption for acknowledged records only. Java callback time is retained in the source evidence. Failures here mean accepted records with terminal failures; Java exceptions do not expose native delivery certainty.",
          "Request curves count complete Produce frames observed by the shared broker, including retries. Frames lost before broker observation are excluded. Wire bytes count successful local client write completions, including control traffic and retries.",
          "Native descriptor/event/wire pool occupancy, internal attempt counters and HDR owner metrics are not available on an equal basis for Java; these paired plots use shared observations. This native adapter exercises ProducerClient through Panama, not the production Java facade.",
        ].map((text) => el("p", text)));
        const limits = [
          ...pair.limits,
          ...Object.entries(pair.runs).flatMap(([adapter, r]) =>
            r.coverage_gaps.map((g) => `${names[adapter]}: ${g}`)
          ),
        ];
        $("comparison-status").textContent = pair.fault_exposure_comparable
          ? "Matching manifest · complete replay verified · required fault opportunities observed"
          : "Fault exposure differs: this pair is not an equivalent fault-effect trial";
        $("comparison-status").className = pair.fault_exposure_comparable
          ? "comparison-status"
          : "comparison-status comparison-gap";
        list(
          "comparison-limits",
          limits.length ? limits : [
            "No additional scenario-specific compatibility limit. Millisecond Java timers, batching, retries and recovery behavior still differ.",
          ],
        );
        U.renderJsonDump($("comparison-provenance"), {
          manifest_sha256: pair.manifest_sha256,
          sources: pair.sources,
          setup: pair.setup,
          fault_phases: Object.fromEntries(
            Object.entries(pair.runs).map(([k, r]) => [k, r.fault_phases]),
          ),
        });
        const routes = [
          ...new Set(
            Object.values(pair.runs).flatMap((r) =>
              r.partitions.map((p) => `${p.topic_id}/${p.partition}`)
            ),
          ),
        ].sort();
        $("comparison-partition").replaceChildren();
        for (const route of routes) {
          const [topic, partition] = route.split("/");
          option(
            $("comparison-partition"),
            route,
            topic
              ? `${topic.slice(-8)} / partition ${partition}`
              : "Unresolved route",
          );
        }
        $("comparison-partition").value = routes[0] ?? "";
        render();
      }
      function install(value) {
        bundle = value;
        mode(true);
        $("comparison-pair").replaceChildren();
        bundle.pairs.forEach((p, i) =>
          option(
            $("comparison-pair"),
            i,
            `${p.variant} · ${p.profile} · ${p.size} · seed ${p.seed}`,
          )
        );
        const requested = /^#pair=([0-9]+)$/.exec(root.location?.hash ?? "");
        select(
          Math.min(
            bundle.pairs.length - 1,
            requested ? Number(requested[1]) : 0,
          ),
        );
      }
      function lines(key) {
        return state.bundle.runs.map((r, i) =>
          line(r, r.buckets.global[key], names[r.adapter], i, {
            duration: r.duration_ns,
            points: r.buckets.global[key].map((value, j) => ({
              x: Math.min(pair.duration_ns, (j + .5) * pair.bucket_ns),
              y: j * pair.bucket_ns <= r.duration_ns ? value : null,
            })),
          })
        );
      }
      function render() {
        if (!pair || state.bundle?.schema !== "kr-producer-comparison/v1") {
          return;
        }
        state.charts.clear();
        $("view-start").value = String(state.view[0] / 1e6);
        $("view-end").value = String(state.view[1] / 1e6);
        $("view-caption").textContent = `${time(state.view[0])}–${
          time(state.view[1])
        } · shared bucket width ${
          time(pair.bucket_ns)
        }. Whole-run totals below do not change when zooming. Fault schedules retain exact nanosecond boundaries.`;
        renderBands();
        const bandChart = state.charts.get("bands");
        for (const marker of pair.environment.markers) {
          if (marker.at < state.view[0] || marker.at > state.view[1]) continue;
          const tick = U.svgElement("line", {
            x1: bandChart.x(marker.at),
            x2: bandChart.x(marker.at),
            y1: 8,
            y2: 73,
            stroke: "var(--foreground)",
            "stroke-dasharray": "2 4",
          });
          tick.append(
            U.svgElement("title", {}, `${time(marker.at)}: ${marker.label}`),
          );
          $("bands").append(tick);
        }
        if (pair.environment.markers.length) {
          $("band-legend").textContent += " · Dotted ticks: " +
            pair.environment.markers.map((m) => `${time(m.at)} ${m.label}`)
              .join("; ");
        }
        const c = pair.runs.classic.summary, n = pair.runs.native.summary;
        const delta = (a, b) =>
          a === null || b === null
            ? "—"
            : `${b - a > 0 ? "+" : ""}${fmt(b - a)}`;
        table($("comparison-summary"), [
          "Whole-run metric",
          "Classic Java",
          "Native / Panama",
          "Native − Java",
        ], [
          ...["offered", "accepted", "refused", "acked", "failed"].map(
            (k) => [k, fmt(c[k]), fmt(n[k]), delta(c[k], n[k])],
          ),
          ...["p50", "p90", "p99", "max"].map(
            (k) => [
              `Ack latency ${k}`,
              time(c[k]),
              time(n[k]),
              c[k] === null || n[k] === null
                ? "—"
                : `${n[k] >= c[k] ? "+" : "−"}${time(Math.abs(n[k] - c[k]))}`,
            ],
          ),
          [
            "Run duration",
            time(pair.runs.classic.duration_ns),
            time(pair.runs.native.duration_ns),
            "source feedback + settlement",
          ],
          [
            "Maximum source lag",
            time(pair.runs.classic.max_source_lag_ns),
            time(pair.runs.native.max_source_lag_ns),
            "actual offer − due time",
          ],
        ]);
        for (
          const [id, key] of [
            ["comparison-acks", "acked"],
            ["comparison-offers", "offered"],
            ["comparison-admitted", "accepted"],
            ["comparison-refused", "refused"],
            ["comparison-failed", "failed"],
            ["comparison-requests", "produce_requests"],
            ["comparison-committed", "committed"],
          ]
        ) chart(id, lines(key));
        chart("comparison-outstanding", lines("outstanding"), {
          unit: "outstanding at bucket end",
        });
        chart("comparison-wire", lines("wire_bytes"), {
          unit: "bytes / bucket",
        });
        chart(
          "comparison-latency",
          lines($("comparison-quantile").value || "p99").map((s) => ({
            ...s,
            markers: true,
          })),
          { unit: "latency", log: $("comparison-log").checked },
        );
        const maxLatency = Math.max(c.max ?? 1, n.max ?? 1, 1);
        chart(
          "comparison-ecdf",
          state.bundle.runs.map((r, i) => ({
            label: names[r.adapter],
            color: `var(--variant-${i + 1})`,
            dash: i ? "8 3" : "",
            points: r.ecdf.map((p) => ({
              x: Math.log10(1 + p.latency),
              y: p.count / r.summary.acked * 100,
            })),
          })),
          {
            unit: "acknowledged population (%)",
            timeAxis: false,
            domain: [0, Math.log10(1 + maxLatency)],
            max: 100,
            xFormat: (x) => time(10 ** x - 1),
          },
        );
        $("comparison-ecdf-note").textContent =
          "The distribution uses every acknowledged record; up to 512 exact cumulative-rank points are drawn. The x-axis is logarithmic. Refused and failed records are excluded; inspect their counts above.";
        const route = $("comparison-partition").value;
        chart(
          "comparison-partition-chart",
          state.bundle.runs.map((r, i) => {
            const row = r.partitions.find((p) =>
              `${p.topic_id}/${p.partition}` === route
            );
            return line(
              r,
              row?.acked ?? Array(pair.bucket_count).fill(0),
              names[r.adapter],
              i,
            );
          }),
        );
        const brokerRows = pair.runs.classic.brokers.map((b) => {
          const n = pair.runs.native.brokers.find((n) => n.id === b.id);
          const total = (r, key) => r[key].reduce((a, b) => a + b, 0);
          return [
            `Broker ${b.id}`,
            fmt(total(b, "committed")),
            fmt(total(n, "committed")),
            fmt(total(b, "produce_requests")),
            fmt(total(n, "produce_requests")),
            fmt(total(b, "wire_bytes")),
            fmt(total(n, "wire_bytes")),
          ];
        });
        table($("comparison-brokers"), [
          "Broker",
          "Java commits",
          "Native commits",
          "Java requests",
          "Native requests",
          "Java bytes",
          "Native bytes",
        ], brokerRows);
        const failureRows = Object.entries(pair.runs).flatMap(([adapter, r]) =>
          r.failure_reasons.map(
            (reason) => [names[adapter], reason.label, fmt(reason.count)],
          )
        );
        table(
          $("comparison-reasons"),
          ["Producer", "Terminal failure", "Count"],
          failureRows.length
            ? failureRows
            : [["Both", "No accepted-record failures", "0"]],
        );
        updateHover();
      }
      $("comparison-pair").addEventListener(
        "change",
        () => select(Number($("comparison-pair").value)),
      );
      for (
        const id of [
          "comparison-quantile",
          "comparison-log",
          "comparison-partition",
        ]
      ) $(id).addEventListener("change", render);
      $("comparison-fault-window").addEventListener("click", () => {
        const bands = pair.environment.bands.filter((b) =>
          b.start < pair.duration_ns
        );
        if (!bands.length) return setView([0, pair.duration_ns]);
        const first = Math.min(...bands.map((b) => b.start)),
          last = Math.max(...bands.map((b) => b.end));
        const margin = Math.max(pair.bucket_ns * 2, (last - first) * .1);
        setView([
          Math.max(0, first - margin),
          Math.min(pair.duration_ns, last + margin),
        ]);
      });
      return { install, render, mode };
    },
  });
})(globalThis);
