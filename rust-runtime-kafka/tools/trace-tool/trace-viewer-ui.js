"use strict";

(function installTraceViewerUi(root) {
  const SVG_NS = "http://www.w3.org/2000/svg";
  const {
    adjacentSelectionIndex,
    displayLabel,
    formatNanos,
    ratioBigInt,
    safeJson,
    tokenizeJson,
  } = root.TRACE_VIEWER_CORE;

  function svgElement(name, attributes = {}, text = null) {
    const node = document.createElementNS(SVG_NS, name);
    for (const [key, value] of Object.entries(attributes)) {
      node.setAttribute(key, String(value));
    }
    if (text !== null) node.textContent = String(text);
    return node;
  }

  function chartWidth(svg) {
    const ownWidth = svg.getBoundingClientRect().width;
    const parentWidth = svg.parentElement?.getBoundingClientRect().width ?? 0;
    return Math.max(260, Math.floor(ownWidth || parentWidth || 720));
  }

  function renderJsonDump(element, value) {
    const json = safeJson(value);
    const fragment = document.createDocumentFragment();
    for (const token of tokenizeJson(json)) {
      if (token.kind === "plain") {
        fragment.append(document.createTextNode(token.text));
        continue;
      }
      const span = document.createElement("span");
      span.className = `json-${token.kind}`;
      span.textContent = token.text;
      fragment.append(span);
    }
    element.replaceChildren(fragment);
  }

  function operationLanes(steps, preferred = []) {
    const present = [
      ...new Set(steps.map((step) => String(step.operation ?? "unknown"))),
    ];
    return present.sort((left, right) => {
      const leftRank = preferred.indexOf(left.toLowerCase());
      const rightRank = preferred.indexOf(right.toLowerCase());
      if (leftRank >= 0 || rightRank >= 0) {
        if (leftRank < 0) return 1;
        if (rightRank < 0) return -1;
        if (leftRank !== rightRank) return leftRank - rightRank;
      }
      return left.localeCompare(right);
    });
  }

  function timeDomain(steps) {
    let minimum = steps[0]._startedAt;
    let maximum = steps[0]._completedAt;
    for (const step of steps) {
      if (step._startedAt < minimum) minimum = step._startedAt;
      if (step._completedAt > maximum) maximum = step._completedAt;
    }
    return { minimum, maximum };
  }

  function markerFor(step, x, y) {
    const common = {
      class: "completion-mark",
      stroke: "currentColor",
      "stroke-width": 1.5,
    };
    if (step._outcome === "rejected") {
      return svgElement("path", {
        ...common,
        d: `M ${x} ${y - 6} L ${x + 6} ${y + 5} L ${x - 6} ${y + 5} Z`,
      });
    }
    if (step._outcome === "uncertain") {
      return svgElement("path", {
        ...common,
        d: `M ${x - 6} ${y} L ${x - 3} ${y - 5} L ${x + 3} ${y - 5} L ${
          x + 6
        } ${y} L ${x + 3} ${y + 5} L ${x - 3} ${y + 5} Z`,
      });
    }
    if (step._outcome === "unknown") {
      return svgElement("circle", {
        ...common,
        cx: x,
        cy: y,
        r: 6,
        fill: "none",
        "stroke-dasharray": "2 2",
      });
    }
    if (step._outcome === "crash") {
      return svgElement("rect", {
        ...common,
        x: x - 5,
        y: y - 5,
        width: 10,
        height: 10,
      });
    }
    if (step._outcome === "recovered") {
      return svgElement("path", {
        ...common,
        d: `M ${x} ${y - 6} L ${x + 6} ${y} L ${x} ${y + 6} L ${x - 6} ${y} Z`,
      });
    }
    return svgElement("circle", { ...common, cx: x, cy: y, r: 5 });
  }

  function renderOperationTimeline({
    svg,
    steps,
    selectedIndex,
    preferredLanes = [],
    title,
    description,
    onSelect,
    domain: externalDomain = null,
  }) {
    if (
      externalDomain !== null &&
      (typeof externalDomain.minimum !== "bigint" ||
        typeof externalDomain.maximum !== "bigint" ||
        externalDomain.minimum > externalDomain.maximum)
    ) throw new Error("timeline domain must be ordered BigInt bounds");
    svg.replaceChildren();
    svg.append(
      svgElement("title", { id: `${svg.id}-title` }, title),
      svgElement("desc", { id: `${svg.id}-desc` }, description),
    );
    if (steps.length === 0) return null;

    const width = chartWidth(svg);
    const lanes = operationLanes(steps, preferredLanes);
    const labelWidth = width < 480 ? 76 : 122;
    const plotLeft = labelWidth + 9;
    const plotRight = width - 13;
    const plotWidth = Math.max(1, plotRight - plotLeft);
    const top = 20;
    const laneHeight = 35;
    const bottom = 42;
    const height = top + lanes.length * laneHeight + bottom;
    const domain = externalDomain ?? timeDomain(steps);
    const laneIndex = new Map(lanes.map((lane, index) => [lane, index]));
    const xFor = (value) =>
      plotLeft + ratioBigInt(value, domain.minimum, domain.maximum) * plotWidth;

    svg.setAttribute("viewBox", `0 0 ${width} ${height}`);
    svg.setAttribute("height", String(height));

    const tickCount = width < 460 ? 3 : 5;
    for (let index = 0; index < tickCount; index += 1) {
      const value = tickCount === 1 ? domain.minimum : domain.minimum +
        ((domain.maximum - domain.minimum) * BigInt(index)) /
          BigInt(tickCount - 1);
      const x = xFor(value);
      svg.append(
        svgElement("line", {
          class: "chart-grid",
          x1: x,
          x2: x,
          y1: top - 5,
          y2: height - bottom + 7,
        }),
        svgElement(
          "text",
          {
            class: "axis-label",
            x,
            y: height - bottom + 24,
            "text-anchor": "middle",
          },
          formatNanos(value),
        ),
      );
    }

    lanes.forEach((lane, index) => {
      const y = top + index * laneHeight + laneHeight / 2;
      svg.append(
        svgElement("line", {
          class: "lane-line",
          x1: plotLeft,
          x2: plotRight,
          y1: y,
          y2: y,
        }),
        svgElement(
          "text",
          {
            class: "lane-label",
            x: labelWidth,
            y: y + 4,
            "text-anchor": "end",
          },
          displayLabel(lane),
        ),
      );
    });

    const collisionTotals = new Map();
    for (const step of steps) {
      const key = `${step.operation}:${step._startedAt}:${step._completedAt}`;
      collisionTotals.set(key, (collisionTotals.get(key) ?? 0) + 1);
    }
    const collisions = new Map();
    steps.forEach((step, index) => {
      if (
        step._completedAt < domain.minimum ||
        step._startedAt > domain.maximum
      ) return;
      const key = `${step.operation}:${step._startedAt}:${step._completedAt}`;
      const collision = collisions.get(key) ?? 0;
      collisions.set(key, collision + 1);
      const collisionTotal = collisionTotals.get(key) ?? 1;
      const lane = laneIndex.get(String(step.operation ?? "unknown")) ?? 0;
      const y = top + lane * laneHeight + laneHeight / 2 +
        (collision - (collisionTotal - 1) / 2) * 10;
      const x1 = xFor(
        step._startedAt < domain.minimum ? domain.minimum : step._startedAt,
      );
      const rawX2 = xFor(
        step._completedAt > domain.maximum ? domain.maximum : step._completedAt,
      );
      const x2 = rawX2 === x1
        ? x1
        : externalDomain === null
        ? Math.max(x1 + 2, rawX2)
        : Math.min(plotRight, Math.max(x1 + 2, rawX2));
      const label = `Diagnostic step ${step.sequence}: ${
        displayLabel(step.operation)
      }, ${step._outcome}`;
      const group = svgElement("g", {
        class: `trace-step ${step._outcome}${
          index === selectedIndex ? " selected" : ""
        }`,
        role: "button",
        tabindex: index === selectedIndex ? "0" : "-1",
        "data-trace-step-index": index,
        "aria-pressed": index === selectedIndex ? "true" : "false",
        "aria-label": label,
      });
      group.append(
        svgElement(
          "title",
          {},
          `#${step.sequence} ${
            displayLabel(step.operation)
          } · ${step._outcome} · ${formatNanos(step._duration)}`,
        ),
        svgElement("line", {
          class: "duration-line",
          x1,
          x2,
          y1: y,
          y2: y,
        }),
        markerFor(step, x2, y),
        svgElement("circle", {
          class: "selection-halo",
          cx: x2,
          cy: y,
          r: 10,
        }),
      );

      function selectAndRestoreFocus(nextIndex, restoreFocus) {
        onSelect(nextIndex);
        if (!restoreFocus) return;
        svg.querySelector(
          `[data-trace-step-index="${nextIndex}"]`,
        )?.focus({ preventScroll: true });
      }

      group.addEventListener("click", () => {
        selectAndRestoreFocus(index, document.activeElement === group);
      });
      group.addEventListener("keydown", (event) => {
        if (
          event.defaultPrevented || event.isComposing || event.altKey ||
          event.ctrlKey || event.metaKey || event.shiftKey
        ) return;
        let nextIndex = index;
        if (event.key === "ArrowLeft") nextIndex = Math.max(0, index - 1);
        else if (event.key === "ArrowRight") {
          nextIndex = Math.min(steps.length - 1, index + 1);
        } else if (event.key !== "Enter" && event.key !== " ") return;
        event.preventDefault();
        selectAndRestoreFocus(nextIndex, true);
      });
      svg.append(group);
    });

    return { domain, lanes };
  }

  function arrowKeyBelongsToControl(target) {
    return target !== null && typeof target === "object" &&
      typeof target.closest === "function" &&
      target.closest(
          "input, select, textarea, button, [contenteditable]:not([contenteditable='false'])",
        ) !== null;
  }

  function createStepNavigator({
    previousButton,
    nextButton,
    rangeInput,
    rangeOutput,
    keyboardTarget,
    describe,
    onSelect,
  }) {
    let count = 0;
    let selectedIndex = -1;

    function updateControls() {
      rangeInput.min = "0";
      rangeInput.max = String(Math.max(0, count - 1));
      rangeInput.value = String(Math.max(0, selectedIndex));
      rangeInput.disabled = count === 0;
      rangeOutput.textContent = count === 0 ? "—" : describe(selectedIndex);
      previousButton.disabled = selectedIndex <= 0;
      nextButton.disabled = selectedIndex < 0 || selectedIndex >= count - 1;
    }

    function select(index, force = false) {
      if (count === 0 || !Number.isInteger(index)) return;
      const nextIndex = Math.max(0, Math.min(count - 1, index));
      const changed = nextIndex !== selectedIndex;
      selectedIndex = nextIndex;
      updateControls();
      if (changed || force) onSelect(selectedIndex);
    }

    function selectAdjacent(key) {
      if (count === 0) return;
      const index = adjacentSelectionIndex(key, selectedIndex, count);
      if (index !== selectedIndex) select(index);
    }

    function setItems(nextCount, initialIndex = 0) {
      if (!Number.isSafeInteger(nextCount) || nextCount < 0) {
        throw new Error("step navigator count must be a safe unsigned integer");
      }
      count = nextCount;
      selectedIndex = -1;
      if (count === 0) {
        updateControls();
        return;
      }
      select(initialIndex, true);
    }

    const previousListener = () => selectAdjacent("ArrowLeft");
    const nextListener = () => selectAdjacent("ArrowRight");
    const rangeListener = () => select(Number(rangeInput.value));
    const keyboardListener = (event) => {
      if (event.key !== "ArrowLeft" && event.key !== "ArrowRight") return;
      if (
        event.defaultPrevented ||
        event.isComposing ||
        event.altKey ||
        event.ctrlKey ||
        event.metaKey ||
        event.shiftKey ||
        count === 0 ||
        arrowKeyBelongsToControl(event.target)
      ) return;

      event.preventDefault();
      selectAdjacent(event.key);
    };

    previousButton.addEventListener("click", previousListener);
    nextButton.addEventListener("click", nextListener);
    rangeInput.addEventListener("input", rangeListener);
    keyboardTarget.addEventListener("keydown", keyboardListener);
    updateControls();

    return Object.freeze({
      destroy() {
        previousButton.removeEventListener("click", previousListener);
        nextButton.removeEventListener("click", nextListener);
        rangeInput.removeEventListener("input", rangeListener);
        keyboardTarget.removeEventListener("keydown", keyboardListener);
      },
      index: () => selectedIndex,
      select,
      selectAdjacent,
      setItems,
    });
  }

  function createArtifactLoader({
    fileInput,
    resetButton,
    sourceElement,
    errorElement,
    bundledData,
    parseText,
    installData,
    description,
    maxFileBytes = 4 * 1024 * 1024,
    readFile = (file) => file.text(),
  }) {
    let loadToken = 0;

    function clearError() {
      errorElement.textContent = "";
      errorElement.hidden = true;
    }

    function showError(error) {
      errorElement.textContent = error instanceof Error
        ? error.message
        : String(error);
      errorElement.hidden = false;
      resetButton.disabled = false;
    }

    function install(raw, sourceLabel, isBundled) {
      installData(raw);
      sourceElement.textContent = sourceLabel;
      resetButton.disabled = isBundled;
      clearError();
    }

    async function loadFile(file, token) {
      if (file.size > maxFileBytes) {
        const limitMiB = maxFileBytes / (1024 * 1024);
        throw new Error(
          `${description} file exceeds the ${limitMiB} MiB viewer limit`,
        );
      }
      const contents = await readFile(file);
      if (token !== loadToken) return;
      install(parseText(contents), file.name, false);
    }

    fileInput.addEventListener("change", () => {
      const file = fileInput.files?.[0];
      if (!file) return;
      const token = ++loadToken;
      loadFile(file, token)
        .catch((error) => {
          if (token === loadToken) showError(error);
        })
        .finally(() => {
          fileInput.value = "";
        });
    });
    resetButton.addEventListener("click", () => {
      loadToken += 1;
      try {
        install(bundledData, "Bundled sample", true);
      } catch (error) {
        showError(error);
      }
    });

    return Object.freeze({
      installBundled() {
        install(bundledData, "Bundled sample", true);
      },
      showError,
    });
  }

  function observeResize(element, render, delay = 60) {
    let timer = null;
    const observer = new ResizeObserver(() => {
      globalThis.clearTimeout(timer);
      timer = globalThis.setTimeout(render, delay);
    });
    observer.observe(element);
    return () => {
      globalThis.clearTimeout(timer);
      observer.disconnect();
    };
  }

  root.TRACE_VIEWER_UI = Object.freeze({
    chartWidth,
    createArtifactLoader,
    createStepNavigator,
    observeResize,
    renderJsonDump,
    renderOperationTimeline,
    svgElement,
  });
})(globalThis);
