"use strict";

(function installRingTraceViewer(root) {
  const {
    displayLabel,
    displayTitle,
    exactUnsigned,
    formatNanos,
    ratioBigInt,
  } = root.TRACE_VIEWER_CORE;
  const {
    chartWidth,
    createArtifactLoader,
    createStepNavigator,
    observeResize,
    renderJsonDump,
    renderOperationTimeline,
    svgElement,
  } = root.TRACE_VIEWER_UI;
  const { parseArtifactText, validateData } = root.RING_TRACE_MODEL;
  const bundledData = root.RING_TRACE_DATA;
  const elements = {
    scenario: document.getElementById("scenario"),
    sourceTest: document.getElementById("source-test"),
    provider: document.getElementById("provider"),
    seed: document.getElementById("seed"),
    runtimeTime: document.getElementById("runtime-time"),
    runtimeSteps: document.getElementById("runtime-steps"),
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
    cursorChart: document.getElementById("cursor-chart"),
    cursorValues: document.getElementById("cursor-values"),
    physicalHeading: document.getElementById("physical-heading"),
    physicalCapacity: document.getElementById("physical-capacity"),
    physicalValues: document.getElementById("physical-values"),
    physicalStrip: document.getElementById("physical-strip"),
    fullCircleNote: document.getElementById("full-circle-note"),
    recoveryNote: document.getElementById("recovery-note"),
    physicalObservation: document.getElementById("physical-observation"),
    detailHeading: document.getElementById("detail-heading"),
    detailOutcome: document.getElementById("detail-outcome"),
    detailDescription: document.getElementById("detail-description"),
    detailSummary: document.getElementById("detail-summary"),
    detailMeta: document.getElementById("detail-meta"),
    detailFields: document.getElementById("detail-fields"),
  };
  const state = {
    data: null,
    steps: [],
    selectedIndex: 0,
  };

  function installData(raw) {
    const validated = validateData(raw);
    state.data = validated.raw;
    state.steps = validated.steps;
    const fullCircleSync = state.steps.findIndex((step) =>
      step.phase === "wrap" && step.operation === "sync"
    );
    const initialIndex = fullCircleSync >= 0
      ? fullCircleSync
      : state.steps.length - 1;

    elements.scenario.textContent = displayTitle(raw.scenario ?? "Ring trace");
    elements.sourceTest.textContent = String(
      raw.source_test ?? "unknown source test",
    );
    elements.provider.textContent = String(raw.provider ?? "unknown provider");
    elements.seed.textContent = String(raw.runtime.seed);
    elements.runtimeTime.textContent = formatNanos(raw.runtime.now_ns);
    elements.runtimeSteps.textContent = String(raw.runtime.total_steps);
    navigator.setItems(state.steps.length, initialIndex);
  }

  function renderTimeline() {
    const result = renderOperationTimeline({
      svg: elements.timeline,
      steps: state.steps,
      selectedIndex: state.selectedIndex,
      preferredLanes: [
        "create",
        "open",
        "status",
        "append",
        "read",
        "trim",
        "sync",
        "crash",
        "recover",
        "reopen",
      ],
      title: "Ring operations aligned by virtual time",
      description:
        "Select a mark to inspect that operation. Press Left Arrow or Right Arrow to inspect adjacent diagnostic steps.",
      onSelect: (index) => navigator.select(index),
    });
    if (result === null) {
      elements.timelineCaption.textContent = "No diagnostic steps.";
      return;
    }
    elements.timelineCaption.textContent =
      `${state.steps.length} diagnostic steps across ${result.lanes.length} operation lanes · ${
        formatNanos(result.domain.minimum)
      } to ${formatNanos(result.domain.maximum)} virtual time.`;
  }

  function statusAt(index) {
    for (let cursor = index; cursor >= 0; cursor -= 1) {
      if (state.steps[cursor].status) {
        return { status: state.steps[cursor].status, observedIndex: cursor };
      }
    }
    return null;
  }

  function cursorSamples() {
    const samples = [];
    let current = null;
    for (const step of state.steps) {
      if (step.status) current = step.status;
      if (!current) continue;
      samples.push({
        sequence: step.sequence,
        acceptedHead: exactUnsigned(
          current.accepted_head,
          `status at ${step.sequence}.accepted_head`,
        ),
        acceptedTail: exactUnsigned(
          current.accepted_tail,
          `status at ${step.sequence}.accepted_tail`,
        ),
        durableHead: exactUnsigned(
          current.durable_head,
          `status at ${step.sequence}.durable_head`,
        ),
        durableTail: exactUnsigned(
          current.durable_tail,
          `status at ${step.sequence}.durable_tail`,
        ),
      });
    }
    return samples;
  }

  function renderCursorChart() {
    const svg = elements.cursorChart;
    svg.replaceChildren();
    svg.append(
      svgElement(
        "title",
        { id: "cursor-title" },
        "Accepted and durable ring cursors by diagnostic sequence",
      ),
      svgElement(
        "desc",
        { id: "cursor-desc" },
        "Stepped lines distinguish accepted state from state made visible by a durability fence.",
      ),
    );
    const samples = cursorSamples();
    if (samples.length === 0) {
      svg.setAttribute("viewBox", "0 0 600 80");
      svg.setAttribute("height", "80");
      svg.append(
        svgElement(
          "text",
          { class: "chart-label", x: 300, y: 42, "text-anchor": "middle" },
          "No logical status observed yet",
        ),
      );
      elements.cursorValues.textContent = "No status at or before this step";
      return;
    }

    const width = chartWidth(svg);
    const height = width < 480 ? 210 : 225;
    const left = width < 480 ? 38 : 48;
    const right = 14;
    const top = 12;
    const bottom = 38;
    const plotWidth = width - left - right;
    const plotHeight = height - top - bottom;
    const firstSequence = samples[0].sequence;
    const lastSequence = state.steps[state.steps.length - 1].sequence;
    let maximum = 1n;
    for (const sample of samples) {
      maximum = [
        sample.acceptedHead,
        sample.acceptedTail,
        sample.durableHead,
        sample.durableTail,
      ].reduce(
        (current, value) => value > current ? value : current,
        maximum,
      );
    }
    const xFor = (sequence) =>
      left +
      (lastSequence === firstSequence
          ? 0.5
          : (sequence - firstSequence) / (lastSequence - firstSequence)) *
        plotWidth;
    const yFor = (value) =>
      top + (1 - ratioBigInt(value, 0n, maximum)) * plotHeight;
    svg.setAttribute("viewBox", `0 0 ${width} ${height}`);
    svg.setAttribute("height", String(height));

    const yTicks = 4;
    for (let index = 0; index < yTicks; index += 1) {
      const value = (maximum * BigInt(index)) / BigInt(yTicks - 1);
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
    const selectedX = xFor(Math.max(firstSequence, selectedSequence));
    svg.append(
      svgElement("rect", {
        class: "selection-band",
        x: selectedX - 2,
        y: top,
        width: 4,
        height: plotHeight,
      }),
    );

    const series = [
      { key: "acceptedHead", className: "accepted head" },
      { key: "acceptedTail", className: "accepted tail" },
      { key: "durableHead", className: "durable head" },
      { key: "durableTail", className: "durable tail" },
    ];
    for (const definition of series) {
      let path = `M ${xFor(samples[0].sequence)} ${
        yFor(samples[0][definition.key])
      }`;
      for (let index = 1; index < samples.length; index += 1) {
        const sample = samples[index];
        path += ` H ${xFor(sample.sequence)} V ${yFor(sample[definition.key])}`;
      }
      svg.append(
        svgElement("path", {
          class: `cursor-path ${definition.className}`,
          d: path,
        }),
      );
    }

    const selectedStatus = statusAt(state.selectedIndex);
    if (selectedStatus) {
      const values = [
        [
          "accepted",
          exactUnsigned(selectedStatus.status.accepted_head, "accepted_head"),
        ],
        [
          "accepted",
          exactUnsigned(selectedStatus.status.accepted_tail, "accepted_tail"),
        ],
        [
          "durable",
          exactUnsigned(selectedStatus.status.durable_head, "durable_head"),
        ],
        [
          "durable",
          exactUnsigned(selectedStatus.status.durable_tail, "durable_tail"),
        ],
      ];
      for (const [kind, value] of values) {
        svg.append(
          svgElement("circle", {
            class: "cursor-point",
            cx: selectedX,
            cy: yFor(value),
            r: 4,
            fill: kind === "accepted" ? "var(--accepted)" : "var(--durable)",
          }),
        );
      }
      const status = selectedStatus.status;
      elements.cursorValues.textContent =
        `AH ${status.accepted_head} · AT ${status.accepted_tail} · DH ${status.durable_head} · DT ${status.durable_tail}`;
    } else {
      elements.cursorValues.textContent = "No status at or before this step";
    }
  }

  function appendPhysicalValue(className, label, value) {
    const wrapper = document.createElement("span");
    wrapper.className = className;
    const marker = document.createElement("i");
    marker.setAttribute("aria-hidden", "true");
    wrapper.append(marker, document.createTextNode(`${label} ${value}`));
    elements.physicalValues.append(wrapper);
  }

  function markerLine(svg, className, x, top, bottom, shape) {
    svg.append(
      svgElement("line", {
        class: `strip-marker ${className}`,
        x1: x,
        x2: x,
        y1: top,
        y2: bottom,
      }),
    );
    if (shape === "triangle") {
      svg.append(
        svgElement("path", {
          class: `strip-marker ${className}`,
          d: `M ${x} ${bottom} L ${x - 5} ${bottom - 7} L ${x + 5} ${
            bottom - 7
          } Z`,
        }),
      );
    } else if (shape === "square") {
      svg.append(
        svgElement("rect", {
          class: `strip-marker ${className}`,
          x: x - 4,
          y: top - 4,
          width: 8,
          height: 8,
        }),
      );
    } else {
      svg.append(
        svgElement("circle", {
          class: `strip-marker ${className}`,
          cx: x,
          cy: top,
          r: 4,
        }),
      );
    }
  }

  function renderPhysicalStrip() {
    const svg = elements.physicalStrip;
    svg.replaceChildren();
    svg.append(
      svgElement(
        "title",
        { id: "physical-title" },
        "Physical byte allocation",
      ),
      svgElement(
        "desc",
        { id: "physical-desc" },
        "Protected bytes may wrap around the end of the fixed allocation. Equal head and tail offsets can mean full or empty; the protected-byte count disambiguates them.",
      ),
    );
    elements.physicalValues.replaceChildren();
    elements.fullCircleNote.hidden = true;
    elements.recoveryNote.hidden = true;

    const observation = statusAt(state.selectedIndex);
    const physical = observation?.status?.physical ?? null;
    if (!physical) {
      svg.setAttribute("viewBox", "0 0 600 80");
      svg.setAttribute("height", "80");
      svg.append(
        svgElement(
          "text",
          { class: "chart-label", x: 300, y: 42, "text-anchor": "middle" },
          "No physical allocation snapshot observed yet",
        ),
      );
      elements.physicalHeading.textContent = "Physical ring";
      elements.physicalCapacity.textContent = "No provider snapshot";
      elements.physicalObservation.textContent = observation
        ? `Logical status observed at diagnostic step #${
          state.steps[observation.observedIndex].sequence
        }; this provider did not report physical state.`
        : "No status at or before the selected step.";
      return;
    }

    const capacity = exactUnsigned(
      physical.data_capacity_bytes,
      "physical.data_capacity_bytes",
    );
    if (capacity === 0n) {
      throw new Error("physical data capacity must be non-zero");
    }
    const protectedBytes = exactUnsigned(
      physical.protected_bytes,
      "physical.protected_bytes",
    );
    const freeBytes = exactUnsigned(physical.free_bytes, "physical.free_bytes");
    const head = exactUnsigned(
      physical.durable_head_offset,
      "physical.durable_head_offset",
    ) % capacity;
    const durableTail = exactUnsigned(
      physical.durable_tail_offset,
      "physical.durable_tail_offset",
    ) % capacity;
    const acceptedTail = exactUnsigned(
      physical.accepted_tail_offset,
      "physical.accepted_tail_offset",
    ) % capacity;
    const width = chartWidth(svg);
    const height = 105;
    const left = 18;
    const right = 18;
    const stripTop = 38;
    const stripHeight = 24;
    const stripWidth = width - left - right;
    const xFor = (value) =>
      left + ratioBigInt(value, 0n, capacity) * stripWidth;
    svg.setAttribute("viewBox", `0 0 ${width} ${height}`);
    svg.setAttribute("height", String(height));
    svg.append(
      svgElement("rect", {
        class: "strip-base strip-outline",
        x: left,
        y: stripTop,
        width: stripWidth,
        height: stripHeight,
        rx: 2,
      }),
    );

    const shownProtected = protectedBytes > capacity
      ? capacity
      : protectedBytes;
    if (shownProtected >= capacity) {
      svg.append(
        svgElement("rect", {
          class: "strip-protected",
          x: left,
          y: stripTop,
          width: stripWidth,
          height: stripHeight,
        }),
      );
    } else if (shownProtected > 0n) {
      const untilEnd = capacity - head;
      if (shownProtected <= untilEnd) {
        svg.append(
          svgElement("rect", {
            class: "strip-protected",
            x: xFor(head),
            y: stripTop,
            width: xFor(head + shownProtected) - xFor(head),
            height: stripHeight,
          }),
        );
      } else {
        svg.append(
          svgElement("rect", {
            class: "strip-protected",
            x: xFor(head),
            y: stripTop,
            width: xFor(capacity) - xFor(head),
            height: stripHeight,
          }),
          svgElement("rect", {
            class: "strip-protected",
            x: left,
            y: stripTop,
            width: xFor(shownProtected - untilEnd) - left,
            height: stripHeight,
          }),
        );
      }
    }

    svg.append(
      svgElement("rect", {
        class: "strip-outline",
        x: left,
        y: stripTop,
        width: stripWidth,
        height: stripHeight,
        rx: 2,
        fill: "none",
      }),
    );
    const tickCount = width < 460 ? 3 : 5;
    for (let index = 0; index < tickCount; index += 1) {
      const value = (capacity * BigInt(index)) / BigInt(tickCount - 1);
      const x = xFor(value);
      svg.append(
        svgElement("line", {
          class: "chart-axis",
          x1: x,
          x2: x,
          y1: stripTop + stripHeight,
          y2: stripTop + stripHeight + 5,
        }),
        svgElement(
          "text",
          {
            class: "axis-label",
            x,
            y: stripTop + stripHeight + 20,
            "text-anchor": index === 0
              ? "start"
              : index === tickCount - 1
              ? "end"
              : "middle",
          },
          value,
        ),
      );
    }

    markerLine(svg, "head", xFor(head), 12, stripTop, "triangle");
    markerLine(
      svg,
      "durable-tail",
      xFor(durableTail),
      stripTop + stripHeight,
      stripTop + stripHeight + 12,
      "circle",
    );
    markerLine(
      svg,
      "accepted-tail",
      xFor(acceptedTail),
      stripTop - 8,
      stripTop,
      "square",
    );

    elements.physicalHeading.textContent = `Physical ${capacity}-byte ring`;
    elements.physicalCapacity.textContent =
      `${protectedBytes} protected · ${freeBytes} free`;
    appendPhysicalValue("head", "Durable head @", head);
    appendPhysicalValue("durable-tail", "Durable tail @", durableTail);
    appendPhysicalValue("accepted-tail", "Accepted tail @", acceptedTail);

    const equalOffsets = head === acceptedTail;
    const fullCircle = equalOffsets && protectedBytes >= capacity;
    if (fullCircle) {
      elements.fullCircleNote.textContent =
        `Full circle: durable head and accepted tail both map to byte ${head}, but protected_bytes=${protectedBytes} means the ${capacity}-byte allocation is full, not empty.`;
      elements.fullCircleNote.hidden = false;
    } else if (equalOffsets && protectedBytes === 0n) {
      elements.fullCircleNote.textContent =
        "Empty circle: equal head and tail offsets are disambiguated by protected_bytes=0.";
      elements.fullCircleNote.hidden = false;
    }
    if (physical.recovery_required) {
      elements.recoveryNote.textContent =
        "This snapshot requires close and recovery before more ring operations are safe.";
      elements.recoveryNote.hidden = false;
    }
    const observedStep = state.steps[observation.observedIndex];
    elements.physicalObservation.textContent =
      observation.observedIndex === state.selectedIndex
        ? `Snapshot recorded by selected diagnostic step #${observedStep.sequence} · metadata generation ${physical.metadata_generation}.`
        : `Showing the latest prior snapshot from diagnostic step #${observedStep.sequence} · metadata generation ${physical.metadata_generation}.`;
  }

  function renderDetail() {
    const step = state.steps[state.selectedIndex];
    elements.detailHeading.textContent = `#${step.sequence} · ${
      displayLabel(step.operation)
    }`;
    elements.detailOutcome.textContent = step._outcome;
    elements.detailOutcome.className = `outcome ${step._outcome}`;
    elements.detailDescription.textContent = String(
      step.description ?? "No description recorded.",
    );
    elements.detailSummary.textContent = String(step.summary ?? "");
    elements.detailMeta.replaceChildren();
    const values = [
      `phase ${step.phase ?? "—"}`,
      `${formatNanos(step._startedAt)} → ${formatNanos(step._completedAt)}`,
      `duration ${formatNanos(step._duration)}`,
    ];
    if (step.certainty !== null && step.certainty !== undefined) {
      values.push(`certainty ${step.certainty}`);
    }
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
    renderCursorChart();
    renderPhysicalStrip();
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
    description: "ring trace",
  });

  observeResize(document.querySelector("main"), () => {
    if (state.steps.length === 0) return;
    renderTimeline();
    renderCursorChart();
    renderPhysicalStrip();
  });

  try {
    loader.installBundled();
  } catch (error) {
    loader.showError(error);
  }
})(globalThis);
