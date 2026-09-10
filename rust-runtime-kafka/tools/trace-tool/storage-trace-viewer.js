"use strict";

(function installStorageTraceViewer(root) {
  const MAX_VISIBLE_BYTE_SLOTS = 16;
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
  const {
    decodeArtifact,
    decodeBase64Artifact,
    initialSelectionIndex,
    maxMessageBytes,
  } = root.STORAGE_TRACE_MODEL;
  const bundledData = root.STORAGE_TRACE_DATA;
  const elements = {
    scenario: document.getElementById("scenario"),
    sourceTest: document.getElementById("source-test"),
    provider: document.getElementById("provider"),
    seed: document.getElementById("seed"),
    runtimeTime: document.getElementById("runtime-time"),
    runtimeSteps: document.getElementById("runtime-steps"),
    fileLimit: document.getElementById("file-limit"),
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
    imageSummary: document.getElementById("image-summary"),
    storageBefore: document.getElementById("storage-before"),
    storageAfter: document.getElementById("storage-after"),
    lengthChart: document.getElementById("length-chart"),
    providerSummary: document.getElementById("provider-summary"),
    detailHeading: document.getElementById("detail-heading"),
    detailOutcome: document.getElementById("detail-outcome"),
    detailDescription: document.getElementById("detail-description"),
    detailSummary: document.getElementById("detail-summary"),
    detailMeta: document.getElementById("detail-meta"),
    detailFields: document.getElementById("detail-fields"),
  };
  const state = {
    trace: null,
    steps: [],
    selectedIndex: 0,
  };

  function installData(input) {
    const trace = typeof input === "string"
      ? decodeBase64Artifact(input)
      : input;
    state.trace = trace;
    state.steps = trace.steps;

    elements.scenario.textContent = displayTitle(trace.scenario);
    elements.sourceTest.textContent = trace.sourceTest;
    elements.provider.textContent = trace.provider;
    elements.seed.textContent = trace.runtime.seed;
    elements.runtimeTime.textContent = formatNanos(trace._completedAt);
    elements.runtimeSteps.textContent = state.steps.length +
      " diagnostic · " + trace.runtime.totalSteps + " runtime";
    elements.fileLimit.textContent = trace._config.maxFileBytes + " bytes";

    navigator.setItems(
      state.steps.length,
      initialSelectionIndex(trace),
    );
  }

  function renderTimeline() {
    const result = renderOperationTimeline({
      svg: elements.timeline,
      steps: state.steps,
      selectedIndex: state.selectedIndex,
      preferredLanes: [
        "open",
        "script_fault",
        "write_at",
        "read_at",
        "set_len",
        "len",
        "sync",
        "crash",
        "reopen",
        "close",
      ],
      title: "Storage operations aligned by virtual time",
      description:
        "Select a mark to inspect accepted and durable bytes before and after that operation.",
      onSelect: (index) => navigator.select(index),
    });
    if (result === null) {
      elements.timelineCaption.textContent = "No diagnostic steps.";
      return;
    }
    elements.timelineCaption.textContent = state.steps.length +
      " assertion-checked operations across " + result.lanes.length +
      " lanes · " + formatNanos(result.domain.minimum) + " to " +
      formatNanos(result.domain.maximum) + ".";
  }

  function byteLabel(byte) {
    return byte.toString(16).toUpperCase().padStart(2, "0");
  }

  function byteChanged(bytes, comparison, index) {
    const occupied = index < bytes.length;
    const comparisonOccupied = index < comparison.length;
    return occupied !== comparisonOccupied ||
      (occupied && bytes[index] !== comparison[index]);
  }

  function dirtyByte(snapshot, index) {
    return index < snapshot.acceptedBytes.length &&
      (index >= snapshot.durableBytes.length ||
        snapshot.acceptedBytes[index] !== snapshot.durableBytes[index]);
  }

  function dirtyByteCount(snapshot) {
    let count = 0;
    for (let index = 0; index < snapshot.acceptedBytes.length; index += 1) {
      if (dirtyByte(snapshot, index)) count += 1;
    }
    return count;
  }

  function appendImageRow(
    container,
    kind,
    bytes,
    comparisonBytes,
    snapshot,
    visibleSlots,
  ) {
    const row = document.createElement("div");
    row.className = "image-row";

    const header = document.createElement("div");
    header.className = "image-header";
    const label = document.createElement("span");
    label.className = "image-label " + kind;
    label.textContent = displayTitle(kind);
    const encoding = document.createElement("span");
    encoding.textContent = "hex bytes";
    header.append(label, encoding);

    const grid = document.createElement("div");
    grid.className = "byte-grid";
    grid.style.setProperty("--visible-byte-slots", String(visibleSlots));
    grid.setAttribute("role", "img");
    grid.setAttribute(
      "aria-label",
      displayTitle(kind) + " image: " + bytes.length + " bytes; showing " +
        Math.min(bytes.length, visibleSlots),
    );
    for (let index = 0; index < visibleSlots; index += 1) {
      const occupied = index < bytes.length;
      const isDirty = kind === "accepted" && dirtyByte(snapshot, index);
      const changed = byteChanged(bytes, comparisonBytes, index);
      const cell = document.createElement("span");
      cell.className = "byte-cell" +
        (occupied ? " occupied " + kind : "") +
        (isDirty ? " dirty" : "") +
        (changed ? " changed" : "");
      cell.textContent = occupied ? byteLabel(bytes[index]) : "·";
      grid.append(cell);
    }
    if (bytes.length > visibleSlots) {
      const omitted = document.createElement("span");
      omitted.className = "bytes-omitted";
      omitted.textContent = "+" + (bytes.length - visibleSlots) +
        " bytes not shown";
      grid.append(omitted);
    }

    const footer = document.createElement("div");
    footer.className = "image-footer";
    const length = document.createElement("span");
    length.textContent = bytes.length + " byte" +
      (bytes.length === 1 ? "" : "s");
    const meaning = document.createElement("span");
    if (kind === "accepted") {
      const dirty = dirtyByteCount(snapshot);
      meaning.textContent = dirty === 0
        ? "fully durable"
        : dirty + " byte" + (dirty === 1 ? "" : "s") + " not durable";
    } else {
      meaning.textContent = "crash-safe image";
    }
    footer.append(length, meaning);

    row.append(header, grid, footer);
    container.append(row);
  }

  function renderStorageSnapshot(
    container,
    snapshot,
    comparison,
    visibleSlots,
  ) {
    container.replaceChildren();

    const status = document.createElement("div");
    status.className = "storage-state";
    const session = document.createElement("span");
    session.className = "session-state" +
      (snapshot.closed ? " closed" : "");
    session.textContent = displayTitle(snapshot.session);
    const provider = document.createElement("span");
    provider.textContent = snapshot.inFlight + "/" + snapshot.inFlightLimit +
      " in flight · " + snapshot.pendingFaults + " pending faults · " +
      snapshot.faultHits + " fault hits";
    const gate = document.createElement("span");
    gate.textContent = "fsync gate v" + snapshot.fsyncGateVersion + " · " +
      (snapshot.hasFsyncGatedData ? "gated data" : "no gated data");
    status.append(session, provider, gate);

    container.append(status);
    appendImageRow(
      container,
      "accepted",
      snapshot.acceptedBytes,
      comparison.acceptedBytes,
      snapshot,
      visibleSlots,
    );
    appendImageRow(
      container,
      "durable",
      snapshot.durableBytes,
      comparison.durableBytes,
      snapshot,
      visibleSlots,
    );
  }

  function byteDelta(before, after) {
    const delta = after - before;
    if (delta === 0n) return "unchanged";
    const magnitude = delta < 0n ? -delta : delta;
    return (delta > 0n ? "+" : "") + delta + " byte" +
      (magnitude === 1n ? "" : "s");
  }

  function renderStorageTransition() {
    const step = state.steps[state.selectedIndex];
    const longestImage = Math.max(
      step._before.acceptedBytes.length,
      step._before.durableBytes.length,
      step._after.acceptedBytes.length,
      step._after.durableBytes.length,
    );
    const visibleSlots = Math.max(
      1,
      Math.min(longestImage, MAX_VISIBLE_BYTE_SLOTS),
    );
    renderStorageSnapshot(
      elements.storageBefore,
      step._before,
      step._after,
      visibleSlots,
    );
    renderStorageSnapshot(
      elements.storageAfter,
      step._after,
      step._before,
      visibleSlots,
    );
    elements.imageSummary.textContent = displayLabel(step.operation) +
      " · accepted " +
      byteDelta(step._before.acceptedLen, step._after.acceptedLen) +
      " · durable " +
      byteDelta(step._before.durableLen, step._after.durableLen);
  }

  function renderLengthChart() {
    const svg = elements.lengthChart;
    svg.replaceChildren();
    svg.append(
      svgElement(
        "title",
        { id: "length-title" },
        "Accepted and durable byte lengths",
      ),
      svgElement(
        "desc",
        { id: "length-desc" },
        "Stepped lines show both image lengths with the selected step highlighted.",
      ),
    );
    if (state.steps.length === 0) return;

    const width = chartWidth(svg);
    const height = 205;
    const left = width < 480 ? 40 : 50;
    const right = 14;
    const top = 12;
    const bottom = 38;
    const plotWidth = width - left - right;
    const plotHeight = height - top - bottom;
    const lastIndex = state.steps.length - 1;
    const maximumLength = state.steps.reduce(
      (maximum, step) =>
        [step._after.acceptedLen, step._after.durableLen].reduce(
          (candidate, value) => value > candidate ? value : candidate,
          maximum,
        ),
      1n,
    );
    const xFor = (index) =>
      left + (lastIndex === 0 ? 0.5 : index / lastIndex) * plotWidth;
    const yFor = (value) =>
      top + (1 - ratioBigInt(value, 0n, maximumLength)) * plotHeight;
    svg.setAttribute("viewBox", "0 0 " + width + " " + height);
    svg.setAttribute("height", String(height));

    const yIntervals = Number(maximumLength < 4n ? maximumLength : 4n);
    for (let index = 0; index <= yIntervals; index += 1) {
      const value = yIntervals === 0
        ? 0n
        : (maximumLength * BigInt(index)) / BigInt(yIntervals);
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

    const xTickCount = Math.min(
      width < 460 ? 3 : 5,
      state.steps.length,
    );
    const tickIndexes = new Set();
    for (let tick = 0; tick < xTickCount; tick += 1) {
      const index = xTickCount === 1
        ? 0
        : Math.round((lastIndex * tick) / (xTickCount - 1));
      if (tickIndexes.has(index)) continue;
      tickIndexes.add(index);
      const x = xFor(index);
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
          "#" + state.steps[index].sequence,
        ),
      );
    }

    const selectedX = xFor(state.selectedIndex);
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
          key: "acceptedLen",
          className: "accepted",
          color: "var(--accepted)",
        },
        {
          key: "durableLen",
          className: "durable",
          color: "var(--durable)",
        },
      ]
    ) {
      const values = state.steps.map((step) => step._after[definition.key]);
      let path = "M " + xFor(0) + " " + yFor(values[0]);
      for (let index = 1; index < state.steps.length; index += 1) {
        path += " H " + xFor(index) + " V " + yFor(values[index]);
      }
      svg.append(
        svgElement("path", {
          class: "length-path " + definition.className,
          d: path,
        }),
        svgElement("circle", {
          class: "length-point",
          cx: selectedX,
          cy: yFor(values[state.selectedIndex]),
          r: 4,
          fill: definition.color,
        }),
      );
    }

    const status = state.steps[state.selectedIndex]._after;
    elements.providerSummary.textContent = displayTitle(status.session) +
      " · " + status.inFlight + "/" + status.inFlightLimit +
      " in flight · " + status.pendingFaults + " pending faults · " +
      status.faultHits + " fault hits";
  }

  function appendDetailMeta(value) {
    const span = document.createElement("span");
    span.textContent = value;
    elements.detailMeta.append(span);
  }

  function rawFields(step) {
    return Object.fromEntries(
      Object.entries(step).filter(([key]) => !key.startsWith("_")),
    );
  }

  function renderDetail() {
    const step = state.steps[state.selectedIndex];
    elements.detailHeading.textContent = "#" + step.sequence + " · " +
      displayLabel(step.operation);
    elements.detailOutcome.textContent = step._outcome === "uncertain"
      ? displayLabel(step.outcome) + " · " + displayLabel(step.certainty)
      : displayLabel(step.outcome);
    elements.detailOutcome.className = "outcome " + step._outcome;
    elements.detailDescription.textContent = step.description;
    elements.detailSummary.textContent = step.summary;
    elements.detailMeta.replaceChildren();
    appendDetailMeta("phase " + displayLabel(step.phase));
    appendDetailMeta(
      formatNanos(step._startedAt) + " → " +
        formatNanos(step._completedAt),
    );
    appendDetailMeta("duration " + formatNanos(step._duration));
    appendDetailMeta(
      "session " + step._before.session + " → " + step._after.session,
    );
    appendDetailMeta(
      "accepted " + step._before.acceptedLen + " → " +
        step._after.acceptedLen,
    );
    appendDetailMeta(
      "durable " + step._before.durableLen + " → " +
        step._after.durableLen,
    );
    if (step.certainty !== "not_applicable") {
      appendDetailMeta("certainty " + displayLabel(step.certainty));
    }
    if (step._offset !== null) {
      appendDetailMeta("offset " + step._offset);
    }
    if (step._transferLength !== null) {
      appendDetailMeta("transfer " + step._transferLength + " bytes");
    }
    if (step._resultLength !== null) {
      appendDetailMeta("result " + step._resultLength + " bytes");
    }
    renderJsonDump(elements.detailFields, rawFields(step));
  }

  function renderSelection(index) {
    if (state.steps.length === 0) return;
    state.selectedIndex = index;
    renderTimeline();
    renderStorageTransition();
    renderLengthChart();
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
      return "#" + step.sequence + " · " + displayLabel(step.operation);
    },
    onSelect: renderSelection,
  });
  const loader = createArtifactLoader({
    fileInput: elements.traceFile,
    resetButton: elements.bundledSample,
    sourceElement: elements.traceSource,
    errorElement: elements.error,
    bundledData,
    parseText: decodeArtifact,
    installData,
    description: "storage trace SBE",
    maxFileBytes: maxMessageBytes,
    readFile: (file) => file.arrayBuffer(),
  });

  observeResize(document.querySelector("main"), () => {
    if (state.steps.length === 0) return;
    renderTimeline();
    renderLengthChart();
  });

  try {
    loader.installBundled();
  } catch (error) {
    loader.showError(error);
  }
})(globalThis);
