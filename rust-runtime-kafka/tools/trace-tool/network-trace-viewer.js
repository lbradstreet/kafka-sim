"use strict";

(function installNetworkTraceViewer(root) {
  const MAX_VISIBLE_BUFFER_SLOTS = 16;
  const { displayLabel, displayTitle, formatNanos, ratioBigInt } =
    root.TRACE_VIEWER_CORE;
  const {
    chartWidth,
    createArtifactLoader,
    createStepNavigator,
    observeResize,
    renderJsonDump,
    renderOperationTimeline,
    svgElement,
  } = root.TRACE_VIEWER_UI;
  const { parseArtifactText, validateData } = root.NETWORK_TRACE_MODEL;
  const bundledData = root.NETWORK_TRACE_DATA;
  const elements = {
    scenario: document.getElementById("scenario"),
    sourceTest: document.getElementById("source-test"),
    provider: document.getElementById("provider"),
    seed: document.getElementById("seed"),
    runtimeTime: document.getElementById("runtime-time"),
    runtimeSteps: document.getElementById("runtime-steps"),
    bufferCapacity: document.getElementById("buffer-capacity"),
    traceFile: document.getElementById("trace-file"),
    bundledSample: document.getElementById("bundled-sample"),
    traceSource: document.getElementById("trace-source"),
    error: document.getElementById("error"),
    timeline: document.getElementById("timeline"),
    timelineCaption: document.getElementById("timeline-caption"),
    previous: document.getElementById("previous"),
    next: document.getElementById("next"),
    range: document.getElementById("step-range"),
    rangeOutput: document.getElementById("step-output"),
    duplexSummary: document.getElementById("duplex-summary"),
    flowBefore: document.getElementById("flow-before"),
    flowAfter: document.getElementById("flow-after"),
    occupancyChart: document.getElementById("occupancy-chart"),
    providerSummary: document.getElementById("provider-summary"),
    detailHeading: document.getElementById("detail-heading"),
    detailOutcome: document.getElementById("detail-outcome"),
    detailDescription: document.getElementById("detail-description"),
    detailSummary: document.getElementById("detail-summary"),
    detailMeta: document.getElementById("detail-meta"),
    detailFields: document.getElementById("detail-fields"),
  };
  const state = {
    steps: [],
    capacity: 0,
    endpoints: { left: 0n, right: 0n },
    selectedIndex: 0,
  };

  function installData(raw) {
    const validated = validateData(raw);
    state.steps = validated.steps;
    state.capacity = validated.capacity;
    state.endpoints = validated.endpoints;

    elements.scenario.textContent = displayTitle(
      raw.scenario ?? "Network trace",
    );
    elements.sourceTest.textContent = String(
      raw.source_test ?? "unknown source test",
    );
    elements.provider.textContent = String(raw.provider ?? "unknown provider");
    elements.seed.textContent = String(raw.runtime.seed);
    elements.runtimeTime.textContent = formatNanos(raw.completed_at_ns);
    elements.runtimeSteps.textContent =
      `${state.steps.length} diagnostic · ${raw.runtime.total_steps} runtime`;
    elements.bufferCapacity.textContent =
      `${state.capacity} bytes per direction`;

    const pressureStep = state.steps.findIndex((step) =>
      step.outcome === "pending_then_completed"
    );
    navigator.setItems(
      state.steps.length,
      pressureStep >= 0 ? pressureStep : 0,
    );
  }

  function renderTimeline() {
    const result = renderOperationTimeline({
      svg: elements.timeline,
      steps: state.steps,
      selectedIndex: state.selectedIndex,
      preferredLanes: [
        "write",
        "read",
        "partition",
        "heal",
        "shutdown-write",
      ],
      title: "Network operations aligned by virtual time",
      description:
        "Select a mark to inspect that operation and its directional buffers.",
      onSelect: (index) => navigator.select(index),
    });
    if (result === null) {
      elements.timelineCaption.textContent = "No diagnostic steps.";
      return;
    }
    elements.timelineCaption.textContent =
      `${state.steps.length} assertion-checked operations across ${result.lanes.length} lanes · ${
        formatNanos(result.domain.minimum)
      } to ${formatNanos(result.domain.maximum)}.`;
  }

  function byteLabel(byte) {
    return byte.toString(16).toUpperCase().padStart(2, "0");
  }

  function changedAt(direction, comparison, index) {
    if (!comparison) return false;
    return direction.bytes[index] !== comparison.bytes[index];
  }

  function appendFlowRow(
    container,
    key,
    direction,
    comparison,
    from,
    to,
    label,
  ) {
    const row = document.createElement("article");
    row.className = `flow-row ${key}`;

    const header = document.createElement("div");
    header.className = "flow-header";
    const directionLabel = document.createElement("span");
    directionLabel.className = `flow-direction ${key}`;
    directionLabel.textContent = label;
    const linkState = document.createElement("span");
    linkState.className = `link-state ${direction.linkState}`;
    linkState.textContent = direction.linkState;
    header.append(directionLabel, linkState);

    const track = document.createElement("div");
    track.className = "flow-track";
    const fromNode = document.createElement("span");
    fromNode.className = "node";
    fromNode.textContent = `Node ${from}`;
    const buffer = document.createElement("div");
    buffer.className = "buffer";
    const visibleSlots = Math.min(state.capacity, MAX_VISIBLE_BUFFER_SLOTS);
    buffer.style.setProperty("--visible-buffer-slots", String(visibleSlots));
    buffer.setAttribute("role", "img");
    buffer.setAttribute(
      "aria-label",
      `${label} buffer: ${direction.bytes.length} of ${state.capacity} bytes`,
    );
    for (let index = 0; index < visibleSlots; index += 1) {
      const cell = document.createElement("span");
      const occupied = index < direction.bytes.length;
      cell.className = `byte-cell${occupied ? " occupied" : ""}${
        changedAt(direction, comparison, index) ? " changed" : ""
      }`;
      cell.textContent = occupied ? byteLabel(direction.bytes[index]) : "·";
      buffer.append(cell);
    }
    if (visibleSlots < state.capacity) {
      const omitted = document.createElement("span");
      omitted.className = "buffer-omitted";
      omitted.textContent = `+${state.capacity - visibleSlots} slots not shown`;
      buffer.append(omitted);
    }
    const toNode = document.createElement("span");
    toNode.className = "node";
    toNode.textContent = `Node ${to}`;
    track.append(fromNode, buffer, toNode);

    const footer = document.createElement("p");
    footer.className = "flow-footer";
    const occupancy = document.createElement("span");
    occupancy.textContent =
      `${direction.bytes.length}/${state.capacity} bytes queued`;
    const sender = document.createElement("span");
    sender.className = `sender-state${direction.senderOpen ? "" : " closed"}`;
    sender.textContent = direction.senderOpen
      ? "sender open"
      : "sender half-closed";
    footer.append(occupancy, sender);

    row.append(header, track, footer);
    container.append(row);
  }

  function renderFlowSnapshot(container, snapshot, comparison) {
    container.replaceChildren();
    appendFlowRow(
      container,
      "left-to-right",
      snapshot.leftToRight,
      comparison?.leftToRight,
      state.endpoints.left,
      state.endpoints.right,
      "Left → Right",
    );
    appendFlowRow(
      container,
      "right-to-left",
      snapshot.rightToLeft,
      comparison?.rightToLeft,
      state.endpoints.right,
      state.endpoints.left,
      "Right → Left",
    );
  }

  function renderDuplexTransition() {
    const step = state.steps[state.selectedIndex];
    renderFlowSnapshot(elements.flowBefore, step._flowBefore, step._flowAfter);
    renderFlowSnapshot(elements.flowAfter, step._flowAfter, step._flowBefore);
    const beforeBytes = step._flowBefore.leftToRight.bytes.length +
      step._flowBefore.rightToLeft.bytes.length;
    const afterBytes = step._flowAfter.leftToRight.bytes.length +
      step._flowAfter.rightToLeft.bytes.length;
    const delta = afterBytes - beforeBytes;
    const deltaLabel = delta === 0
      ? "no net byte change"
      : `${delta > 0 ? "+" : ""}${delta} queued byte${
        Math.abs(delta) === 1 ? "" : "s"
      }`;
    elements.duplexSummary.textContent = `${
      displayLabel(step.operation)
    } · ${deltaLabel}`;
  }

  function renderOccupancyChart() {
    const svg = elements.occupancyChart;
    svg.replaceChildren();
    svg.append(
      svgElement(
        "title",
        { id: "occupancy-title" },
        "Buffered bytes after each network operation",
      ),
      svgElement(
        "desc",
        { id: "occupancy-desc" },
        "Stepped lines show directional buffer occupancy with the selected step highlighted.",
      ),
    );
    if (state.steps.length === 0) return;

    const width = chartWidth(svg);
    const height = 205;
    const left = width < 480 ? 36 : 46;
    const right = 14;
    const top = 12;
    const bottom = 38;
    const plotWidth = width - left - right;
    const plotHeight = height - top - bottom;
    const firstSequence = state.steps[0].sequence;
    const lastSequence = state.steps.at(-1).sequence;
    const xFor = (sequence) =>
      left +
      (lastSequence === firstSequence
          ? 0.5
          : (sequence - firstSequence) / (lastSequence - firstSequence)) *
        plotWidth;
    const yFor = (value) =>
      top + (1 - ratioBigInt(BigInt(value), 0n, BigInt(state.capacity))) *
        plotHeight;
    svg.setAttribute("viewBox", `0 0 ${width} ${height}`);
    svg.setAttribute("height", String(height));

    const yTicks = Math.min(state.capacity, 4) + 1;
    for (let index = 0; index < yTicks; index += 1) {
      const value = Math.round(
        (state.capacity * index) / Math.max(1, yTicks - 1),
      );
      const y = yFor(value);
      svg.append(
        svgElement("line", {
          class: "chart-grid",
          x1: left,
          x2: width - right,
          y1: y,
          y2: y,
        }),
        svgElement(
          "text",
          {
            class: "axis-label",
            x: left - 7,
            y: y + 4,
            "text-anchor": "end",
          },
          value,
        ),
      );
    }

    const xTicks = width < 460 ? 3 : 5;
    for (let index = 0; index < xTicks; index += 1) {
      const sequence = Math.round(
        firstSequence +
          ((lastSequence - firstSequence) * index) / (xTicks - 1),
      );
      const x = xFor(sequence);
      svg.append(
        svgElement("line", {
          class: "chart-grid",
          x1: x,
          x2: x,
          y1: top,
          y2: top + plotHeight,
        }),
        svgElement(
          "text",
          {
            class: "axis-label",
            x,
            y: height - 12,
            "text-anchor": "middle",
          },
          `#${sequence}`,
        ),
      );
    }

    const selectedSequence = state.steps[state.selectedIndex].sequence;
    const selectedX = xFor(selectedSequence);
    svg.append(
      svgElement("rect", {
        class: "selection-band",
        x: selectedX - 2,
        y: top,
        width: 4,
        height: plotHeight,
      }),
    );

    for (
      const definition of [
        {
          key: "leftToRight",
          className: "left-to-right",
          color: "var(--left-flow)",
        },
        {
          key: "rightToLeft",
          className: "right-to-left",
          color: "var(--right-flow)",
        },
      ]
    ) {
      const values = state.steps.map((step) =>
        step._flowAfter[definition.key].bytes.length
      );
      let path = `M ${xFor(firstSequence)} ${yFor(values[0])}`;
      for (let index = 1; index < state.steps.length; index += 1) {
        path += ` H ${xFor(state.steps[index].sequence)} V ${
          yFor(values[index])
        }`;
      }
      svg.append(
        svgElement("path", {
          class: `occupancy-path ${definition.className}`,
          d: path,
        }),
        svgElement("circle", {
          class: "occupancy-point",
          cx: selectedX,
          cy: yFor(values[state.selectedIndex]),
          r: 4,
          fill: definition.color,
        }),
      );
    }

    const status = state.steps[state.selectedIndex]._providerAfter;
    elements.providerSummary.textContent =
      `${status.connections} connection · ${status.inflightOperations} in flight · ${status.pendingFaults} pending faults · ${status.faultHits} fault hits`;
  }

  function renderDetail() {
    const step = state.steps[state.selectedIndex];
    elements.detailHeading.textContent = `#${step.sequence} · ${
      displayLabel(step.operation)
    }`;
    elements.detailOutcome.textContent = displayLabel(step.outcome);
    elements.detailOutcome.className = `outcome ${step._outcome}`;
    elements.detailDescription.textContent = String(
      step.description ?? "No description recorded.",
    );
    elements.detailSummary.textContent = String(step.summary ?? "");
    elements.detailMeta.replaceChildren();
    const values = [
      `phase ${step.phase}`,
      `${formatNanos(step._startedAt)} → ${formatNanos(step._completedAt)}`,
      `duration ${formatNanos(step._duration)}`,
    ];
    if (step.certainty !== null) values.push(`certainty ${step.certainty}`);
    for (const value of values) {
      const span = document.createElement("span");
      span.textContent = value;
      elements.detailMeta.append(span);
    }
    renderJsonDump(elements.detailFields, step.fields);
  }

  function renderSelection(index) {
    if (state.steps.length === 0) return;
    state.selectedIndex = index;
    renderTimeline();
    renderDuplexTransition();
    renderOccupancyChart();
    renderDetail();
  }

  const navigator = createStepNavigator({
    previousButton: elements.previous,
    nextButton: elements.next,
    rangeInput: elements.range,
    rangeOutput: elements.rangeOutput,
    keyboardTarget: document,
    describe: (index) => {
      const step = state.steps[index];
      return `#${step.sequence} · ${displayLabel(step.operation)}`;
    },
    onSelect: renderSelection,
  });
  const loader = createArtifactLoader({
    fileInput: elements.traceFile,
    resetButton: elements.bundledSample,
    sourceElement: elements.traceSource,
    errorElement: elements.error,
    bundledData,
    parseText: parseArtifactText,
    installData,
    description: "network trace",
  });

  observeResize(document.querySelector("main"), () => {
    if (state.steps.length === 0) return;
    renderTimeline();
    renderOccupancyChart();
  });

  try {
    loader.installBundled();
  } catch (error) {
    loader.showError(error);
  }
})(globalThis);
