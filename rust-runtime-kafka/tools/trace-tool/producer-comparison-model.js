"use strict";
(function installProducerComparisonModel(root) {
  const SCHEMA = "kr-producer-comparison/v1", MAX_TIME = 300_000_000_000;
  const COUNTS = ["offered", "accepted", "refused", "acked", "failed"];
  const SERIES = [
    ...COUNTS,
    "outstanding",
    "produce_requests",
    "produce_records",
    "committed",
    "wire_bytes",
  ];
  const Q = ["p50", "p90", "p99", "max"];
  function require(ok, why) {
    if (!ok) throw new Error(`Producer comparison: ${why}`);
  }
  function num(v, max = Number.MAX_SAFE_INTEGER) {
    require(Number.isSafeInteger(v) && v >= 0 && v <= max, "bounded integer");
    return v;
  }
  function text(v) {
    require(typeof v === "string" && v.length <= 4096, "bounded text");
    return v;
  }
  function arr(v, max) {
    require(Array.isArray(v) && v.length <= max, "array capacity");
    return v;
  }
  function keys(v, names) {
    require(v && typeof v === "object" && !Array.isArray(v), "object");
    require(
      Object.keys(v).sort().join() === names.slice().sort().join(),
      "field inventory",
    );
  }
  function digest(v) {
    require(/^[0-9a-f]{64}$/.test(text(v)), "source digest");
  }
  function decimal(v) {
    require(
      typeof v === "string" && /^(0|[1-9][0-9]*)$/.test(v),
      "canonical decimal",
    );
    return root.TRACE_VIEWER_CORE.exactUnsigned(v);
  }
  function values(v, count) {
    require(arr(v, count).length === count, "series dimension");
    return v;
  }
  function sum(v) {
    return num(v.reduce((a, b) => a + num(b), 0));
  }
  function quantiles(q, count, max) {
    let previous = 0;
    for (const name of Q) {
      const value = q[name];
      require((value === null) === (count === 0), "empty quantile semantics");
      if (value !== null) {
        require(num(value, max) >= previous, "quantile order");
        previous = value;
      }
    }
  }
  function run(r, pair) {
    keys(r, [
      "duration_ns",
      "summary",
      "buckets",
      "partitions",
      "brokers",
      "ecdf",
      "ecdf_complete",
      "failure_reasons",
      "sender_errors",
      "max_source_lag_ns",
      "fault_phases",
      "coverage_gaps",
    ]);
    const duration = num(r.duration_ns, pair.duration_ns),
      n = pair.bucket_count;
    require(duration > 0, "positive run duration");
    keys(r.summary, [...COUNTS, ...Q]);
    for (const key of COUNTS) num(r.summary[key], 1_000_000);
    require(
      r.summary.offered === r.summary.accepted + r.summary.refused,
      "admission population",
    );
    require(
      r.summary.accepted === r.summary.acked + r.summary.failed,
      "terminal population",
    );
    quantiles(r.summary, r.summary.acked, duration);
    keys(r.buckets, [...SERIES, ...Q]);
    for (const key of SERIES) {
      values(r.buckets[key], n).forEach((v) => num(v));
      if (COUNTS.includes(key)) {
        require(sum(r.buckets[key]) === r.summary[key], "bucket population");
      }
    }
    for (const key of Q) values(r.buckets[key], n);
    let outstanding = 0;
    for (let i = 0; i < n; i++) {
      outstanding += r.buckets.accepted[i] - r.buckets.acked[i] -
        r.buckets.failed[i];
      require(
        outstanding >= 0 && outstanding === r.buckets.outstanding[i],
        "outstanding continuity",
      );
      quantiles(
        Object.fromEntries(Q.map((k) => [k, r.buckets[k][i]])),
        r.buckets.acked[i],
        duration,
      );
      if (i * pair.bucket_ns > duration) {
        require(
          SERIES.every((key) => r.buckets[key][i] === 0),
          "activity after run end",
        );
      }
    }
    require(outstanding === 0, "terminal outstanding work");
    const identities = new Set();
    for (const p of arr(r.partitions, 1024)) {
      keys(p, ["topic_id", "partition", "acked", "failed"]);
      require(
        p.topic_id === ""
          ? p.partition === -1
          : /^[0-9a-f]{32}$/.test(p.topic_id) &&
            Number.isSafeInteger(p.partition) && p.partition >= 0 &&
            p.partition < 1024,
        "partition identity",
      );
      const identity = `${p.topic_id}/${p.partition}`;
      require(!identities.has(identity), "duplicate partition");
      identities.add(identity);
      for (const key of ["acked", "failed"]) {
        values(p[key], n).forEach((v) => num(v, 1_000_000));
      }
    }
    for (const key of ["acked", "failed"]) {
      require(
        sum(r.partitions.map((p) => sum(p[key]))) === r.summary[key],
        "partition population",
      );
    }
    const brokerIds = new Set();
    for (const b of arr(r.brokers, 64)) {
      keys(b, ["id", "committed", "produce_requests", "wire_bytes"]);
      num(b.id, 2147483647);
      require(!brokerIds.has(b.id), "duplicate broker");
      brokerIds.add(b.id);
      for (const key of ["committed", "produce_requests", "wire_bytes"]) {
        values(b[key], n).forEach((v) => num(v));
      }
    }
    for (const key of ["committed", "produce_requests", "wire_bytes"]) {
      for (let i = 0; i < n; i++) {
        require(
          sum(r.brokers.map((b) => b[key][i])) === r.buckets[key][i],
          "broker population",
        );
      }
    }
    require(typeof r.ecdf_complete === "boolean", "ECDF completeness");
    let lastLatency = -1, lastCount = 0;
    for (const point of arr(r.ecdf, 512)) {
      keys(point, ["latency", "count"]);
      require(
        num(point.latency, duration) > lastLatency &&
          num(point.count, r.summary.acked) > lastCount,
        "ECDF order",
      );
      lastLatency = point.latency;
      lastCount = point.count;
    }
    require(
      lastCount === r.summary.acked &&
        (lastCount === 0 || lastLatency === r.summary.max),
      "ECDF population",
    );
    if (r.ecdf_complete && r.summary.acked) {
      for (const [key, rank] of [["p50", 50], ["p90", 90], ["p99", 99]]) {
        require(
          r.ecdf.find((p) => p.count >= Math.ceil(r.summary.acked * rank / 100))
            .latency === r.summary[key],
          "ECDF quantile",
        );
      }
    }
    for (const reason of arr(r.failure_reasons, 256)) {
      keys(reason, ["label", "count"]);
      text(reason.label);
      num(reason.count, r.summary.failed);
    }
    require(
      sum(r.failure_reasons.map((r) => r.count)) === r.summary.failed,
      "failure reason population",
    );
    num(r.sender_errors);
    num(r.max_source_lag_ns, duration);
    arr(r.fault_phases, 512);
    arr(r.coverage_gaps, 512).forEach(text);
  }
  function validate(value) {
    // Bound recursive work and reject unsafe numeric values before rendering.
    let work = 0;
    function tree(v, depth = 0) {
      require(++work <= 8_000_000 && depth <= 24, "aggregate shape bound");
      if (typeof v === "number") {
        require(Number.isSafeInteger(v), "unsafe number");
      } else if (typeof v === "string") text(v);
      else if (Array.isArray(v)) v.forEach((x) => tree(x, depth + 1));
      else if (v && typeof v === "object") {
        for (const [key, child] of Object.entries(v)) {
          require(
            !["__proto__", "constructor", "prototype"].includes(key),
            "unsafe key",
          );
          tree(child, depth + 1);
        }
      } else require(v === null || typeof v === "boolean", "JSON value");
    }
    tree(value);
    require(
      new TextEncoder().encode(JSON.stringify(value)).length <=
        48 * 1024 * 1024,
      "artifact byte bound",
    );
    keys(value, ["schema", "scenario", "pairs"]);
    require(value.schema === SCHEMA, "schema");
    require(/^[a-z0-9.-]{1,128}$/.test(value.scenario), "scenario identity");
    const identities = new Set();
    require(arr(value.pairs, 16).length > 0, "empty pair set");
    for (const p of value.pairs) {
      keys(p, [
        "variant",
        "profile",
        "size",
        "seed",
        "origin_ns",
        "duration_ns",
        "bucket_ns",
        "bucket_count",
        "replay_verified",
        "manifest_sha256",
        "environment",
        "limits",
        "setup",
        "sources",
        "runs",
        "fault_exposure_comparable",
      ]);
      text(p.variant);
      require(
        ["original", "common"].includes(p.profile) &&
          ["test", "full"].includes(p.size),
        "profile/size",
      );
      decimal(p.seed);
      require(
        decimal(p.origin_ns) + BigInt(num(p.duration_ns, MAX_TIME)) < 1n << 64n,
        "absolute clock overflow",
      );
      require(
        p.duration_ns > 0 && num(p.bucket_ns, MAX_TIME) > 0,
        "positive time geometry",
      );
      require(
        num(p.bucket_count, 320) === Math.ceil(p.duration_ns / p.bucket_ns),
        "bucket geometry",
      );
      require(
        p.bucket_ns === Math.max(1, Math.ceil(p.duration_ns / 320)),
        "canonical shared bucket width",
      );
      require(p.replay_verified === true, "replay verification required");
      digest(p.manifest_sha256);
      const identity = [p.variant, p.profile, p.size, p.seed].join("/");
      require(!identities.has(identity), "duplicate pair");
      identities.add(identity);
      keys(p.environment, ["bands", "markers"]);
      for (const band of arr(p.environment.bands, 512)) {
        keys(band, ["kind", "start", "end", "label"]);
        require(
          ["isolation", "link", "rule", "polling pause"].includes(band.kind),
          "fault band kind",
        );
        require(
          num(band.start, MAX_TIME) < num(band.end, MAX_TIME),
          "fault band interval",
        );
        text(band.label);
      }
      let previous = -1;
      for (const marker of arr(p.environment.markers, 512)) {
        keys(marker, ["at", "label"]);
        require(num(marker.at, p.duration_ns) >= previous, "control order");
        previous = marker.at;
        text(marker.label);
      }
      arr(p.limits, 64).forEach(text);
      keys(p.setup, [
        "loads",
        "settings",
        "adjustments",
        "original_manifest_sha256",
        "native_config",
        "java_config",
      ]);
      digest(p.setup.original_manifest_sha256);
      for (const key of ["loads", "settings", "adjustments"]) {
        arr(p.setup[key], 512).forEach(text);
      }
      for (
        const [key, field] of [["native_config", "native"], [
          "java_config",
          "java",
        ]]
      ) {
        for (const config of arr(p.setup[key], 256)) {
          keys(config, ["name", field]);
          text(config.name);
          text(config[field]);
        }
      }
      require(
        arr(p.sources, 2).length === 2 &&
          new Set(p.sources.map((s) => s.adapter)).size === 2,
        "paired sources",
      );
      for (const source of p.sources) {
        keys(source, ["adapter", "file", "sha256"]);
        require(
          ["classic", "native"].includes(source.adapter),
          "source adapter",
        );
        text(source.file);
        digest(source.sha256);
      }
      keys(p.runs, ["classic", "native"]);
      for (const r of Object.values(p.runs)) run(r, p);
      require(
        p.runs.classic.brokers.map((b) => b.id).join() ===
          p.runs.native.brokers.map((b) => b.id).join(),
        "shared broker inventory",
      );
      require(
        p.duration_ns === Math.max(
          ...Object.values(p.runs).map((r) => r.duration_ns),
        ),
        "pair duration",
      );
      require(
        p.fault_exposure_comparable ===
          Object.values(p.runs).every((r) => r.coverage_gaps.length === 0),
        "fault exposure claim",
      );
    }
    return value;
  }
  root.PRODUCER_COMPARISON_MODEL = Object.freeze({ SCHEMA, validate });
})(globalThis);
