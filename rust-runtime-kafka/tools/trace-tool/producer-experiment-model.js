"use strict";

(function installProducerExperimentModel(root) {
  const RUN = "kr-kafka-experiment/v1";
  const BUNDLE = "kr-kafka-experiment-bundle/v1";
  const MAX_TIME = 300_000_000_000;
  const encoder = new TextEncoder();
  const OUTCOMES = ["acked", "not_written", "unknown", "refused"];
  const METRICS = [
    "ProduceRttNanos",
    "BatchFillNanos",
    "BatchRawBytes",
    "BatchWireBytes",
    "RecordsPerBatch",
    "QueueWaitNanos",
    "DeliveryAckedNanos",
    "DeliveryNotWrittenNanos",
    "DeliveryUnknownNanos",
    "InFlightRequests",
    "InFlightWireBytes",
  ];
  const UNITS = [
    "Nanoseconds",
    "Nanoseconds",
    "Bytes",
    "Bytes",
    "Count",
    "Nanoseconds",
    "Nanoseconds",
    "Nanoseconds",
    "Nanoseconds",
    "Count",
    "Bytes",
  ];
  const require = (ok, why) => {
    if (!ok) throw new Error(why);
  };
  function number(v, max = Number.MAX_SAFE_INTEGER) {
    require(
      Number.isSafeInteger(v) && v >= 0 && v <= max,
      "bounded unsigned number",
    );
    return v;
  }
  function text(v) {
    require(
      typeof v === "string" && encoder.encode(v).length <= 4096,
      "bounded text",
    );
    return v;
  }
  function bool(v) {
    require(typeof v === "boolean", "boolean");
    return v;
  }
  function arr(v, max) {
    require(Array.isArray(v) && v.length <= max, "array capacity");
    return v;
  }
  function series(v, n) {
    require(arr(v, n).length === n, "series length");
    return v;
  }
  function object(v) {
    require(v !== null && typeof v === "object" && !Array.isArray(v), "object");
    return v;
  }
  function dec(v, signed = false) {
    text(v);
    require(
      (signed ? /^(0|-?[1-9][0-9]*)$/ : /^(0|[1-9][0-9]*)$/).test(v),
      "canonical decimal",
    );
    const b = BigInt(v);
    require(
      b >= (signed ? -(1n << 63n) : 0n) && b < (1n << (signed ? 63n : 64n)),
      "decimal range",
    );
    return b;
  }
  function uuid(v) {
    require(/^[0-9a-f]{32}$/.test(text(v)), "UUID");
    return v;
  }
  function unique(v, why) {
    require(new Set(v).size === v.length, why);
    return new Set(v);
  }
  function nullable(v, max) {
    return v === null ? null : number(v, max);
  }
  function sum(v, n) {
    return number(series(v, n).reduce((a, x) => a + number(x), 0));
  }
  function stable(v) {
    if (Array.isArray(v)) return `[${v.map(stable).join(",")}]`;
    if (v && typeof v === "object") {
      return `{${
        Object.keys(v).sort().map((k) => `${JSON.stringify(k)}:${stable(v[k])}`)
          .join(",")
      }}`;
    }
    return JSON.stringify(v);
  }
  function tree(v, maxWork) {
    let work = 0;
    function walk(x, depth) {
      require(++work <= maxWork && depth <= 32, "aggregate shape capacity");
      if (typeof x === "number") {
        require(
          Number.isFinite(x) && Math.abs(x) <= Number.MAX_SAFE_INTEGER,
          "unsafe number",
        );
      } else if (typeof x === "string") text(x);
      else if (Array.isArray(x)) x.forEach((a) => walk(a, depth + 1));
      else if (x !== null && typeof x === "object") {
        for (const [k, a] of Object.entries(x)) {
          require(
            encoder.encode(k).length <= 128 &&
              !["__proto__", "constructor", "prototype"].includes(k),
            "unsafe key",
          );
          walk(a, depth + 1);
        }
      } else require(x === null || typeof x === "boolean", "JSON value");
    }
    walk(v, 0);
  }
  function bytes(v, cap) {
    // The 48 MiB bundle may contain more nodes than one 5 MiB run. JSON
    // needs at least two bytes per value on average, apart from the root.
    // Keep the run's existing work limit and bound bundle work by its byte cap.
    tree(v, Math.max(8_000_000, Math.floor(cap / 2) + 1));
    require(
      encoder.encode(JSON.stringify(v)).length <= cap,
      "serialized byte cap",
    );
  }
  function quantiles(
    q,
    count,
    max,
    names = ["p50", "p90", "p99", "p999", "max"],
  ) {
    object(q);
    let previous = null, empty;
    for (const k of names) {
      const x = nullable(q[k], max);
      if (count !== undefined) {
        require((x !== null) === (count > 0), "empty quantile semantics");
      }
      if (empty !== undefined) {
        require(empty === (x === null), "quantile emptiness");
      }
      empty = x === null;
      if (previous !== null && x !== null) {
        require(previous <= x, "quantile ordering");
      }
      previous = x;
    }
    if (Object.hasOwn(q, "mean")) {
      require(
        q.mean === null ||
          (Number.isFinite(q.mean) && q.mean >= 0 && q.mean <= max),
        "mean range",
      );
      if (count !== undefined) {
        require((q.mean === null) === (count === 0), "empty mean");
      }
    }
  }
  function quantileColumns(q, n, max, names) {
    names.forEach((k) => series(q[k], n));
    for (let i = 0; i < n; i++) {
      quantiles(
        Object.fromEntries(names.map((k) => [k, q[k][i]])),
        undefined,
        max,
        names,
      );
    }
  }
  function expectation(e) {
    require(
      ["passed", "failed", "observation", "not-applicable"].includes(e.status),
      "check status",
    );
    require(text(e.name).length <= 256, "check name");
    text(e.detail);
  }
  function scenario(s) {
    require(/^[a-z0-9.-]{1,128}$/.test(text(s.id)), "scenario ID");
    require(
      ["baseline", "hard", "soft", "topology", "resources"].includes(
        s.category,
      ),
      "category",
    );
    ["title", "description", "what_to_look_for"].forEach((k) => text(s[k]));
  }
  function variant(v) {
    require(/^[a-zA-Z0-9_-]{1,128}$/.test(text(v.name)), "variant name");
    object(v.deltas);
  }
  function validateConfig(c, brokers) {
    if (c.batch_target_mode !== undefined) {
      require(
        ["Raw", "EstimatedWire"].includes(c.batch_target_mode),
        "batch target mode",
      );
    }
    if (c.descriptor_admission_policy !== undefined) {
      require(
        ["Shared", "PartitionPressure"].includes(c.descriptor_admission_policy),
        "descriptor admission policy",
      );
    }
    for (
      const k of [
        "linger_max",
        "batch_target_bytes",
        "batch_hard_bytes",
        "max_in_flight_per_connection",
        "lanes",
        "connection_wire_window_bytes",
        "request_timeout",
        "delivery_timeout",
        "metadata_max_age",
      ]
    ) number(c[k]);
    require(
      c.lanes > 0 && c.lanes <= 255 && c.max_in_flight_per_connection > 0 &&
        c.max_in_flight_per_connection <= 255 &&
        c.batch_target_bytes <= c.batch_hard_bytes,
      "config bounds",
    );
    require(
      number(c.retry_backoff.min) <= number(c.retry_backoff.max),
      "retry backoff",
    );
    require(
      /^(None|Zstd \{ level: [0-9]+ \})$/.test(text(c.compression)),
      "compression",
    );
    const d = c.driver;
    require(
      ["bounded-directional-propagation/v1", "sim-network-local-completion/v1"]
        .includes(d.transport_model),
      "transport model",
    );
    for (
      const k of [
        "chunk_bytes",
        "encode_bytes_per_poll",
        "encode_cost_per_poll",
        "jitter",
        "local_completion_latency",
        "pipe_bytes",
        "service_delay",
      ]
    ) number(d[k]);
    bool(d.crash_on_isolation);
    unique(
      arr(d.propagation_links, 16).map((l) => {
        require(brokers.has(l.broker), "link broker");
        number(l.to_broker_latency_ns);
        number(l.from_broker_latency_ns);
        require(number(l.chunk_bytes) > 0, "link chunk");
        return l.broker;
      }),
      "duplicate link",
    );
  }
  function validateRecords(r, brokers) {
    const rows = r.records,
      totals = r.summary.records,
      d = r.meta.duration,
      n = r.buckets.count,
      w = r.buckets.bucket_ns;
    const count = number(rows.count, 65536),
      pop = number(rows.population_count, 1_000_000);
    require(
      count <= pop && pop === totals.offered &&
        bool(rows.complete) === (count === pop),
      "record population/completeness",
    );
    require(stable(rows.outcome_names) === stable(OUTCOMES), "outcome table");
    require(
      rows.sampling.method === "evenly-spaced-rank-in-record-id-order",
      "sampling method",
    );
    unique(arr(rows.reason_names, 4096).map(text), "reason names");
    const cols = [
      "record_id",
      "due",
      "offer",
      "accept",
      "deliver",
      "outcome",
      "reason",
      "producer_attempts",
      "client_dispatches",
      "partition",
      "broker",
      "offset",
    ];
    cols.forEach((k) => series(rows[k], count));
    const observed = Array.from({ length: 7 }, () => new Uint32Array(n));
    const outcomeCounts = [0, 0, 0, 0], latencies = [], ids = [], offsets = [];
    let prior = -1n;
    for (let i = 0; i < count; i++) {
      const id = dec(rows.record_id[i]);
      require(id > prior, "record ID order/uniqueness");
      prior = id;
      ids.push(id);
      const due = number(rows.due[i], d),
        offer = number(rows.offer[i], d),
        a = nullable(rows.accept[i], d),
        end = nullable(rows.deliver[i], d);
      require(due <= offer, "due/offer order");
      const o = number(rows.outcome[i], 3);
      outcomeCounts[o]++;
      number(rows.reason[i], rows.reason_names.length - 1);
      const dispatch = number(rows.client_dispatches[i]);
      observed[0][Math.floor(due / w)]++;
      observed[1][Math.floor(offer / w)]++;
      const offset = rows.offset[i] === null ? null : dec(rows.offset[i], true);
      offsets.push(offset);
      if (o === 3) {
        require(
          a === null && end === null && rows.partition[i] === null &&
            rows.broker[i] === null && offset === null &&
            rows.producer_attempts[i] === null && dispatch === 0,
          "refusal nullables",
        );
        observed[3][Math.floor(offer / w)]++;
      } else {
        require(
          a !== null && end !== null && offer <= a && a <= end,
          "delivery times",
        );
        number(rows.producer_attempts[i]);
        observed[2][Math.floor(a / w)]++;
        observed[4 + o][Math.floor(end / w)]++;
        require(
          rows.partition[i] === null
            ? o !== 0
            : number(rows.partition[i], r.topology.partitions.length - 1) >= 0,
          "record partition",
        );
        require(
          rows.broker[i] === null || brokers.has(rows.broker[i]),
          "record broker",
        );
        require(offset === null || (o === 0 && offset >= 0n), "record offset");
        if (o === 0) {
          require(offset !== null && dispatch > 0, "acked proof");
          latencies.push(end - a);
        }
      }
    }
    [
      "offered_due",
      "offered_actual",
      "accepted",
      "refused",
      "acked",
      "not_written",
      "unknown",
    ].forEach((k, j) => {
      observed[j].forEach((v, i) =>
        require(
          rows.complete
            ? v === r.buckets.global[k][i]
            : v <= r.buckets.global[k][i],
          "record bucket population",
        )
      );
    });
    OUTCOMES.forEach((k, i) =>
      require(
        rows.complete
          ? outcomeCounts[i] === totals[k]
          : outcomeCounts[i] <= totals[k],
        "sample outcome population",
      )
    );
    if (rows.complete && latencies.length) {
      latencies.sort((a, b) => a - b);
      [["p50", .5], ["p90", .9], ["p99", .99], ["p999", .999], ["max", 1]]
        .forEach(([k, p]) =>
          require(
            r.summary.latency_acked[k] ===
              latencies[Math.ceil(p * latencies.length) - 1],
            "complete record quantile",
          )
        );
    }
    return { ids, offsets };
  }
  function validateHdr(h, duration, brokers, partitions) {
    if (h === null) return;
    require(
      stable(h.metric_names) === stable(METRICS) &&
        stable(h.units) === stable(UNITS),
      "HDR metric/unit enums",
    );
    const scopes = arr(h.scopes, 273);
    unique(
      scopes.map((s) => {
        if (s.kind === "broker") require(brokers.has(s.broker), "HDR broker");
        else if (s.kind === "partition") {
          require(
            partitions.has(`${uuid(s.topic_id)}:${number(s.partition)}`),
            "HDR partition",
          );
        } else require(s.kind === "global", "HDR scope");
        return s.kind === "global"
          ? "global"
          : s.kind === "broker"
          ? `b${s.broker}`
          : `p${s.topic_id}:${s.partition}`;
      }),
      "duplicate HDR scope",
    );
    const t = h.intervals, n = number(t.count, 1024);
    require(n > 0, "empty HDR intervals");
    ["epoch", "requested", "taken", "start", "end"].forEach((k) =>
      series(t[k], n)
    );
    let epoch = 0n, taken = 0;
    for (let i = 0; i < n; i++) {
      const e = dec(t.epoch[i]),
        at = number(t.taken[i], duration),
        req = nullable(t.requested[i], duration),
        start = nullable(t.start[i], duration),
        end = nullable(t.end[i], duration);
      require(
        e > epoch && at >= taken && (req === null || req <= at) &&
          (start === null) === (end === null) &&
          (start === null || (start <= end && end <= at)),
        "HDR interval ordering",
      );
      epoch = e;
      taken = at;
    }
    unique(
      series(h.series, scopes.length * 11).map((row) => {
        number(row.scope, scopes.length - 1);
        number(row.metric, 10);
        require(number(row.significant_digits, 5) > 0, "HDR precision");
        const high = number(row.highest_trackable);
        [
          "count",
          "p50_range",
          "p90_range",
          "p99_range",
          "p999_range",
          "exact_max",
          "out_of_range",
          "count_overflow",
          "diagnostic_overflow",
        ].forEach((k) => series(row[k], n));
        for (let i = 0; i < n; i++) {
          const c = number(row.count[i]),
            max = nullable(row.exact_max[i], high);
          require((max !== null) === (c > 0), "HDR empty maximum");
          let previous = [0, 0];
          for (
            const k of ["p50_range", "p90_range", "p99_range", "p999_range"]
          ) {
            const q = row[k][i];
            if (c === 0) require(q === null, "HDR empty quantile");
            else {
              series(q, 2);
              const lo = number(q[0]), hi = number(q[1]);
              require(
                lo <= hi && lo >= previous[0] && hi >= previous[1],
                "HDR range order",
              );
              previous = q;
            }
          }
          number(row.out_of_range[i]);
          number(row.count_overflow[i]);
          bool(row.diagnostic_overflow[i]);
        }
        return `${row.scope}:${row.metric}`;
      }),
      "duplicate HDR series",
    );
    series(h.diagnostics, n).forEach((d) => {
      [
        "omitted_scope_samples",
        "scope_capacity_rejections",
        "invalid_scope_samples",
        "invalid_time_samples",
        "missing_time_samples",
        "invalid_depth_samples",
      ].forEach((k) => number(d[k]));
      bool(d.diagnostic_overflow);
    });
    arr(h.missed_requests, 1024).forEach((m) => {
      require(
        number(m.scheduled, duration) <= number(m.attempted, duration),
        "missed request time",
      );
      text(m.reason);
    });
    require(number(h.config.significant_digits, 5) > 0, "HDR config precision");
    ["highest_bytes", "highest_count", "highest_duration_nanos"].forEach((k) =>
      number(h.config[k])
    );
  }
  function typed(v) {
    if (Array.isArray(v)) {
      if (v.every((x) => x === null || typeof x === "number")) {
        return Float64Array.from(v, (x) => x === null ? NaN : x);
      }
      return v.map(typed);
    }
    if (v && typeof v === "object") {
      return Object.fromEntries(
        Object.entries(v).map(([k, x]) => [k, typed(x)]),
      );
    }
    return v;
  }
  function schemaRun(r) {
    bytes(r, 5 * 1024 * 1024);
    require(r.schema === RUN, "run schema");
    const keys = [
      "schema",
      "meta",
      "topology",
      "config",
      "environment",
      "buckets",
      "partitions",
      "records",
      "summary",
      "phase_evidence",
      "distributions",
      "hdr",
    ];
    require(
      Object.keys(r).length === keys.length &&
        keys.every((k) => Object.hasOwn(r, k)),
      "run fields",
    );
    const m = r.meta,
      duration = number(m.duration, MAX_TIME),
      origin = dec(m.origin_ns),
      end = dec(m.end_ns),
      seed = dec(m.seed);
    require(end - origin === BigInt(duration), "absolute time difference");
    bool(m.replay_verified);
    require(["test", "full"].includes(m.size), "fixture size");
    scenario(m.scenario);
    variant(m.variant);
    ["manifest", "history", "scenario", "model", "driver", "rng"].forEach((k) =>
      require(number(m.source[k]) > 0, "source version")
    );
    ["package", "source_sha256", "kafka_schema_sha256", "kafka_revision"]
      .forEach((k) => text(m.source[k]));
    text(m.generated_by);
    const bs = arr(r.topology.brokers, 16);
    require(bs.length > 0, "empty brokers");
    const brokers = unique(
      bs.map((b) => {
        text(b.host);
        require(number(b.port, 65535) > 0, "port");
        return number(b.id, 2147483647);
      }),
      "broker identity",
    );
    unique(
      arr(r.topology.topics, 64).map((t, i) => {
        require(t.index === i, "topic index");
        text(t.name);
        require(arr(t.initial_leaders, 1024).length > 0, "empty topic");
        t.initial_leaders.forEach((b) =>
          require(brokers.has(b), "initial leader")
        );
        return uuid(t.id_hex);
      }),
      "topic identity",
    );
    const ps = arr(r.topology.partitions, 1024);
    const partitions = unique(
      ps.map((p) => `${uuid(p.topic_id)}:${number(p.partition, 1023)}`),
      "partition identity",
    );
    validateConfig(r.config, brokers);
    const b = r.buckets, n = number(b.count, 4096), w = number(b.bucket_ns);
    require(
      n > 0 &&
        [1, 2, 5, 10, 20, 50, 100, 200, 500].some((x) => x * 1e6 === w) &&
        Math.floor(duration / w) + 1 === n,
      "bucket coverage",
    );
    const g = b.global, totals = r.summary.records;
    ["offered", "accepted", ...OUTCOMES].forEach((k) =>
      number(totals[k], 1_000_000)
    );
    require(
      totals.offered === totals.accepted + totals.refused &&
        totals.accepted === totals.acked + totals.not_written + totals.unknown,
      "population accounting",
    );
    require(
      number(m.workload.planned_offers, 1_000_000) ===
        totals.offered + number(m.workload.cancelled_unoffered, 1_000_000),
      "cancelled accounting",
    );
    text(m.workload.test_adjustments);
    arr(m.workload.active_intervals, 65536);
    arr(m.workload.required_phases, 1024);
    const sums = {};
    for (
      const k of [
        "offered_due",
        "offered_actual",
        "accepted",
        "refused",
        "acked",
        "not_written",
        "unknown",
        "client_requests",
        "client_produce_requests",
        "client_retry_requests",
        "client_retry_records",
        "broker_requests",
        "responses",
        "bytes_wire",
        "commit_batches",
        "commit_records",
      ]
    ) {
      sums[k] = sum(g[k], n);
      const expected = k.startsWith("offered_")
        ? totals.offered
        : totals[k] ?? r.summary[k];
      if (expected !== undefined) {
        require(sums[k] === number(expected), "bucket summary sum");
      }
    }
    quantileColumns(g.latency, n, duration, ["p50", "p90", "p99", "max"]);
    quantileColumns(g.first_dispatch, n, duration, ["p50", "p99"]);
    series(g.records_per_request_mean, n).forEach((v) =>
      require(
        v === null || (Number.isFinite(v) && v >= 0 && v <= 1_000_000),
        "records/request mean",
      )
    );
    let pending = 0;
    series(g.outstanding, n).forEach((v, i) => {
      pending += g.accepted[i] - g.acked[i] - g.not_written[i] - g.unknown[i];
      require(number(v) === pending, "outstanding accounting");
    });
    const c = g.credits, pools = arr(c.pools, 32);
    unique(pools.map(text), "duplicate pool");
    series(c.capacity, pools.length).forEach((v) => number(v));
    ["held_observed_max", "held_last_observed"].forEach((k) =>
      series(c[k], pools.length).forEach((row, p) =>
        series(row, n).forEach((v) => number(v, c.capacity[p]))
      )
    );
    pools.forEach((_, p) =>
      c.held_last_observed[p].forEach((v, i) =>
        require(v <= c.held_observed_max[p][i], "last credit above max")
      )
    );
    unique(
      series(b.brokers, brokers.size).map((row) => {
        require(brokers.has(row.broker), "broker series identity");
        [
          "client_requests",
          "broker_requests",
          "responses",
          "bytes_wire",
          "disconnects",
          "setup_failures",
          "drops",
          "delayed_hooks",
          "acked",
          "client_inflight_max",
          "client_inflight_end",
          "active_connections",
        ].forEach((k) => sum(row[k], n));
        ["dispatch_rtt", "full_write_rtt"].forEach((k) =>
          quantileColumns(row[k], n, duration, ["p50", "p99"])
        );
        row.client_inflight_end.forEach((v, i) =>
          require(v <= row.client_inflight_max[i], "inflight max")
        );
        require(
          row.client_inflight_end[n - 1] === 0 &&
            row.active_connections[n - 1] === 0,
          "live terminal connection",
        );
        return row.broker;
      }),
      "duplicate broker series",
    );
    ["client_requests", "broker_requests", "responses", "bytes_wire"].forEach(
      (k) =>
        g[k].forEach((v, i) =>
          require(
            b.brokers.reduce((s, row) => s + row[k][i], 0) === v,
            "broker sum",
          )
        ),
    );
    series(r.partitions.unrouted_not_written, n).forEach((v) => number(v));
    for (const k of ["acked", "not_written", "leader"]) {
      series(r.partitions[k], ps.length).forEach((row) =>
        series(row, n).forEach((v) =>
          k === "leader"
            ? require(v === null || brokers.has(v), "partition leader")
            : number(v)
        )
      );
      if (k !== "leader") {
        g[k].forEach((v, i) =>
          require(
            r.partitions[k].reduce(
              (s, row) => s + row[i],
              k === "not_written" ? r.partitions.unrouted_not_written[i] : 0,
            ) === v,
            "partition sum",
          )
        );
      }
    }
    for (
      const [k, count] of [
        ["latency_acked", totals.acked],
        ["latency_all_deliveries", totals.accepted],
        ["dispatch_rtt", r.summary.responses],
        ["full_write_rtt", r.summary.full_write_rtt.count],
      ]
    ) {
      require(
        number(r.summary[k].count) === count &&
          (k !== "full_write_rtt" || count <= r.summary.responses),
        "distribution count",
      );
      quantiles(r.summary[k], count, duration);
    }
    const ecdf = r.distributions.latency_ecdf;
    require(ecdf.population_count === totals.acked, "ECDF population");
    const points = arr(ecdf.points, 1000);
    require(
      (points.length === 0) === (totals.acked === 0) &&
        bool(ecdf.reduced) === (totals.acked > points.length),
      "ECDF emptiness/reduction",
    );
    let last = 0, rank = 0;
    points.forEach((p) => {
      const t = number(p.latency, duration),
        next = number(p.cumulative_count, totals.acked);
      require(t >= last && next > rank, "ECDF ordering");
      last = t;
      rank = next;
    });
    require(rank === totals.acked, "ECDF final rank");
    let count = 0;
    unique(
      arr(r.distributions.attempts_histogram, 1024).map((h) => {
        count += number(h.count);
        return number(h.producer_attempts);
      }),
      "duplicate attempts",
    );
    require(count === totals.accepted, "attempt population");
    const outcomes = [0, 0, 0, 0];
    unique(
      arr(r.distributions.outcomes_by_reason, 4096).map((h) => {
        const o = number(h.outcome, 3);
        outcomes[o] += number(h.count);
        return `${o}:${text(h.reason)}`;
      }),
      "duplicate reason",
    );
    OUTCOMES.forEach((k, i) =>
      require(outcomes[i] === totals[k], "reason population")
    );
    last = 0;
    arr(r.environment.bands, 1024).forEach((band) => {
      const a = number(band.start, MAX_TIME), z = number(band.end, MAX_TIME);
      require(a >= last && a < z, "band ordering");
      last = a;
      require(
        [
          "isolation",
          "link_outage",
          "service_delay",
          "reject",
          "throttle",
          "loss",
          "stop_polling",
        ].includes(band.kind),
        "band kind",
      );
      require(band.broker === null || brokers.has(band.broker), "band broker");
      text(band.label);
      require(
        band.realized_start === (a <= duration ? a : null) &&
          band.realized_end === (z <= duration ? z : null),
        "band realization",
      );
      if (band.kind === "link_outage") {
        require(
          ["ToBroker", "FromBroker", "Both"].includes(band.direction),
          "direction",
        );
        require(["BlackHole", "FailFast"].includes(band.mode), "outage mode");
      }
    });
    last = 0;
    arr(r.environment.markers, 4096).forEach((marker) => {
      const at = number(marker.at, duration);
      require(at >= last, "marker ordering");
      last = at;
      require(
        [
          "leader_move",
          "add_partitions",
          "topic_delete",
          "topic_recreate",
          "flush",
          "flush_done",
          "close",
          "closed",
          "setup_failure",
          "disconnect",
          "drop",
          "fatal",
          "topic_ready",
          "topic_failed",
          "workload_step",
          "connection_closed",
        ].includes(marker.kind),
        "marker kind",
      );
      if (marker.connection !== null) dec(marker.connection);
      require(
        marker.broker === null || brokers.has(marker.broker),
        "marker broker",
      );
      text(marker.label);
    });
    number(r.environment.markers_truncated);
    unique(
      arr(r.phase_evidence, 1024).map((p) => {
        text(p.phase);
        require(
          number(p.start, MAX_TIME) < number(p.end, MAX_TIME),
          "phase bounds",
        );
        require(
          Object.keys(object(p.exact_counts)).length <= 64 &&
            Object.keys(object(p.witnesses)).length <= 32,
          "phase capacity",
        );
        Object.values(p.exact_counts).forEach((v) => number(v));
        Object.values(p.witnesses).forEach((ids) =>
          arr(ids, 16).forEach((v) => dec(v))
        );
        arr(p.check_results, 1024).forEach(expectation);
        return p.phase;
      }),
      "phase duplicate",
    );
    validateHdr(r.hdr, duration, brokers, partitions);
    const exact = validateRecords(r, brokers);
    return {
      ...r,
      exact: {
        origin,
        end,
        seed,
        recordIds: exact.ids,
        offsets: exact.offsets,
      },
      numeric: { buckets: typed(b), partitions: typed(r.partitions) },
    };
  }
  function schemaBundle(b) {
    bytes(b, 48 * 1024 * 1024);
    require(b.schema === BUNDLE, "bundle schema");
    scenario(b.scenario);
    const runs = arr(b.runs, 32);
    require(runs.length > 0, "empty bundle");
    const names = unique(
      arr(b.variants, 4096).map((v) => {
        variant(v);
        number(v.order);
        return v.name;
      }),
      "duplicate variant",
    );
    const seeds = unique(
      arr(b.seeds, 4096).map((v) => {
        dec(v);
        return v;
      }),
      "duplicate seed",
    );
    unique(
      runs.map((r) => {
        require(
          stable(r.meta.scenario) === stable(b.scenario) &&
            names.has(r.meta.variant.name) && seeds.has(r.meta.seed),
          "bundle run identity",
        );
        return `${r.meta.variant.name}:${r.meta.seed}`;
      }),
      "duplicate run",
    );
    arr(b.comparisons, 1024).forEach(expectation);
    require(
      number(b.page.count) > number(b.page.index) &&
        number(b.page.total_runs) >= runs.length,
      "bundle page",
    );
    return { ...b, runs: runs.map(schemaRun) };
  }
  function validateData(v) {
    if (v.schema === "kr-producer-comparison/v1") {
      return root.PRODUCER_COMPARISON_MODEL.validate(v);
    }
    if (v.schema === RUN) {
      return schemaBundle({
        schema: BUNDLE,
        scenario: v.meta.scenario,
        variants: [{ ...v.meta.variant, order: 0 }],
        seeds: [v.meta.seed],
        runs: [v],
        comparisons: [],
        page: { index: 0, count: 1, total_runs: 1 },
      });
    }
    return schemaBundle(v);
  }
  function parseArtifactText(text) {
    require(encoder.encode(text).length <= 48 * 1024 * 1024, "file byte cap");
    return validateData(
      root.TRACE_VIEWER_CORE.parseJsonArtifact(text, {
        assignment: "globalThis.PRODUCER_EXPERIMENT_DATA =",
        generatedComment:
          text.startsWith("// Generated by classic_visualization; do not edit.")
            ? "// Generated by classic_visualization; do not edit."
            : "// Generated by generate_producer_experiment; do not edit.",
        description: "producer experiment",
      }),
    );
  }
  function viewRange(run, range = [0, run.meta.duration]) {
    const d = run.meta.duration;
    require(
      Array.isArray(range) && range.length === 2 &&
        range.every(Number.isFinite),
      "view range",
    );
    const a = Math.max(0, Math.min(d, Math.floor(range[0]))),
      b = Math.max(0, Math.min(d, Math.ceil(range[1])));
    require(a <= b, "reversed view");
    return [a, b];
  }
  function bucketSlice(run, range) {
    const [a, b] = viewRange(run, range), w = run.buckets.bucket_ns;
    const start = Math.min(run.buckets.count - 1, Math.floor(a / w)),
      end = Math.min(run.buckets.count, Math.floor(b / w) + 1);
    return {
      start,
      end,
      startNs: start * w,
      endNs: Math.min(run.meta.duration, end * w),
      resolutionNs: w,
    };
  }
  function markersAsSteps(run) {
    return run.environment.markers.map((m, i) => ({
      ...m,
      sequence: i + 1,
      operation: m.kind,
      _startedAt: BigInt(m.at),
      _completedAt: BigInt(m.at),
      _duration: 0n,
      _outcome: ["fatal", "topic_failed", "setup_failure"].includes(m.kind)
        ? "rejected"
        : "success",
    }));
  }
  function sampledRecordSlice(run, range) {
    const [a, b] = viewRange(run, range),
      rows = run.records,
      indices = [],
      counts = [0, 0, 0, 0];
    let latencySum = 0, deliveries = 0;
    for (let i = 0; i < rows.count; i++) {
      if (rows.offer[i] >= a && rows.offer[i] <= b) {
        indices.push(i);
        counts[rows.outcome[i]]++;
        if (rows.deliver[i] !== null) {
          latencySum += rows.deliver[i] - rows.accept[i];
          deliveries++;
        }
      }
    }
    return {
      indices: Uint32Array.from(indices),
      counts,
      meanDeliveryLatency: deliveries ? latencySum / deliveries : null,
      sampleCount: rows.count,
      populationCount: rows.population_count,
      complete: rows.complete,
      basis: "record-row sample selected by actual offer time",
    };
  }
  // Questions are catalogue-specific; all quantities below come from the loaded
  // run, so Test fixtures, overrides and partial bundles describe their own data.
  const INTENT = Object.freeze({
    "hard.partition-admission-isolation":
      "Does pressure-based descriptor admission preserve service to healthy partitions while a broker is unavailable? Six independent clock-driven sources keep offering throughout the same outage. Shared and partition-pressure policies use identical offered demand and total capacity. Inspect per-partition refusals and acknowledgments during the outage, then recovery; fast accepted records alone do not establish availability.",
    "baseline.partition-admission-skew":
      "How much useful capacity does partition-pressure admission leave available to skewed traffic? After warmup, independent sources send either all traffic to one partition or 90% to a hot partition. The sparse topology has 1,024 configured partitions but only six active destinations. Compare acknowledgments, refusals and pool utilization at matched rates; idle partitions do not receive fixed descriptor reservations.",
    "baseline.closed-loop-inflight":
      "How much work must the application keep outstanding to use the producer efficiently? This experiment varies both the number of accepted records waiting for delivery and the number of requests allowed on each connection. Follow how batching, throughput and queueing change as those two limits increase; a request limit cannot help when the application supplies too little work.",
    "baseline.open-loop-rate":
      "At what offered rate does admission become the limiting factor? A clock-driven source keeps offering records even when delivery slows. Compare the offered and accepted rates, permanent refusals and the latency of the records that get in. A low latency percentile can coexist with many refused offers.",
    "baseline.linger-sweep":
      "How much latency buys useful batching? These runs vary the maximum linger wait and whether sparse traffic can skip it. Compare batch fill and seal reasons with delivered throughput and tail latency. Linger is one contributor to waiting; zero linger still leaves routing, encoding and network work.",
    "baseline.partition-fanout-skew":
      "How does spreading traffic over more partitions interact with a hot key? The runs change partition count and the chance of choosing a preferred key from a deterministic key corpus. Compare actual partition shares and progress under pending demand; hash collisions mean a key's share is not the same as its partition's share.",
    "baseline.bursty-onoff":
      "Can the producer absorb a burst and drain it before the next one? Each run repeats finite bursts separated by scheduled quiet periods, with burst size as the comparison. Look for admission pressure and queueing within a burst. Blank intervals after it drains are intentional idle time.",
    "baseline.asymmetric-wan-broker":
      "Does one distant broker slow partitions served by nearby brokers? Only one broker receives the longer propagation delay. The runs vary request depth and the wire-byte window. Compare that broker's RTT and partition progress with the others; a larger byte window helps only if the smaller one actually fills.",
    "baseline.compression":
      "When does compression reduce the bytes needed to deliver a workload? Repeated values and deterministic random values are tested with no compression and different Zstd levels. Compare unique first-dispatched batch bytes within the same corpus. Simulated encoder timing is not a measurement of codec CPU cost on hardware.",
    "hard.crash-restart-closed":
      "What happens to an application with a bounded outstanding workload when one broker crashes and returns? The runs vary that outstanding limit and retry backoff. Track the affected partitions through the outage and recovery, while checking independent healthy traffic. A shared outstanding limit can stop the main source from feeding healthy partitions too.",
    "hard.crash-restart-open":
      "Can one unavailable broker consume admission capacity needed by healthy destinations? Offers continue on a fixed clock across the crash, at several rates. Compare refusals and admitted progress by destination during the outage, then inspect the backlog drain after restart. Fast admitted records alone do not establish healthy admission availability.",
    "hard.leader-failover-during-outage":
      "Can pending records escape an unavailable broker when leadership moves elsewhere? The leader move is scheduled before the old broker returns; runs vary the move timing and metadata refresh age. Look for acknowledgments of the old cohort through its new leader, and measure recovery from the move rather than from the eventual restart.",
    "hard.bootstrap-down-at-start":
      "Can initial topic discovery use another endpoint when the first bootstrap broker is down? Runs change endpoint order or leave only one endpoint, and offer another burst after recovery. Separate discovery failure from partitions whose actual leader is still down; a failed topic handle may need reopening even after connectivity returns.",
    "hard.rolling-restart":
      "Do partitions continue making progress while brokers restart one after another? The same outage duration is tested with separated or overlapping restart windows. Follow the leader of each partition at each time: an auxiliary destination that is healthy for one window can be affected by another overlapping restart.",
    "hard.short-vs-long-outage":
      "How does an outage shorter or longer than the delivery deadline change the result? Runs vary outage duration and request timeout and include both already-written and queued records. Distinguish acknowledged, NotWritten and Unknown outcomes, and check whether ambiguous sequence expiry leaves the whole producer failed after the broker returns.",
    "hard.flapping-broker":
      "How well does retry policy use the healthy intervals between repeated outages? A clock-driven source crosses a sequence of short broker flaps, with different outage duty cycles and backoff ranges. Compare connection attempts, affected-partition recovery and progress on other brokers; longer backoff can extend silence beyond each flap.",
    "hard.close-during-outage":
      "What does shutdown guarantee while one broker is unavailable? Close stops new offers partway through the outage; the variants allow a short or long drain deadline. Compare successful draining with NotWritten and Unknown settlement, and use the Closed event to measure shutdown rather than the end of all scheduled simulation tasks.",
    "soft.slow-broker-window":
      "Can a slow but reachable broker cause queueing or retry amplification? Its Produce service is delayed temporarily, while runs vary lanes and requests per connection. Compare affected and healthy partitions, acceptance-to-dispatch wait and connection retirements. A deeper pipeline can make several requests share the consequences of one deadline.",
    "soft.degrading-broker-ramp":
      "Where does gradual slowness turn into a loss of progress? A broker's added service delay increases during the run, and the variants use different request timeouts. Find when service plus queueing outgrows the timeout, then inspect retries, source feedback and recovery. Aggregate p99 can hide a small partition cohort waiting for seconds.",
    "soft.blackhole-vs-failfast":
      "Does immediate transport failure recover differently from silently held traffic? The broker-bound path is interrupted in either mode, with different request deadlines. Compare when requests retire, when reconnection succeeds and when pending records acknowledge; the first detected error and end-to-end recovery are different milestones.",
    "soft.one-way-loss-responses":
      "What if a broker can append records but its replies cannot reach the client? The return path is held for different durations while request depth varies. Follow committed requests, lost response visibility and retries. Check eventual acknowledgment and duplicate protection separately from the amount of retry work.",
    "soft.sustained-random-loss":
      "How do occasional request or response losses combine with retry backoff? Independent drop opportunities run throughout the fault window at different probabilities. Compare clustered partition gaps, retry attempts and healthy progress. The probability applies to each eligible hook, and one seed does not define a worst-case latency bound.",
    "soft.throttle-window":
      "How does a broker's requested cooldown affect its backlog? Produce replies impose throttling during a fixed window, with different request depths. Look for pauses across that broker's lanes and check other brokers independently. A record can wait through several cooldowns while its partition still makes intermittent progress.",
    "soft.retriable-error-storm":
      "How do retries differ when a broker rejects writes temporarily versus when leadership moves? Runs use different retriable errors and backoffs; the leader-error case also changes the destination. Follow which broker handles the retry and how long progress takes to resume. The error-code variants do not all impose the same outage.",
    "soft.disconnect-storm":
      "How much extra work does a connection failure create when several requests are in flight? A broker sometimes disconnects just before its response, possibly after append. Compare request depths, connection retirements, duplicate handling and partition gaps. Request structure changes the realized random opportunities, so use the trace alongside aggregate comparisons.",
    "soft.slow-setup":
      "Can a short interruption become a long outage because reconnecting is slow? After forcing reconnection, this experiment delays connection setup and compares request timeouts. Check whether setup can finish within its deadline and whether healthy connections keep progressing. The setup-delay window matters as well as the shorter isolation window.",
    "soft.high-jitter":
      "How does variation in I/O completion time affect a bounded workload? Propagation is fixed while local completions receive zero or bounded extra jitter, with different request depths. Compare startup, queueing and steady progress. Jitter applies to modeled I/O operations, so several delays can accumulate within one record's lifetime.",
    "soft.tiny-chunk-transport":
      "Do small chunks and bounded pipes limit progress before the producer's wire-byte window does? The transport splits frames into tiny pieces; runs vary byte credit and request depth. Compare write completion, request queueing and partition progress. Identical window variants can mean the pipe or chunk schedule is the active limit.",
    "soft.metadata-loss-during-move":
      "Can the producer discover a new leader when one endpoint loses metadata requests? Leadership moves while that endpoint drops control traffic, and runs vary metadata refresh age. Look for fallback through other endpoints and prompt delivery through the new leader; waiting for periodic refresh is only one discovery path.",
    "topology.leader-rebalance-churn":
      "Does repeated leadership movement leave accumulating stalls between changes? A sustained workload crosses a series of leader moves, with different request depths. Follow each moved partition's retry and new destination, then check that ordinary progress resumes before the next change.",
    "topology.partition-expansion":
      "How quickly does keyed traffic discover and use newly added partitions? A finite load and pre-change probe are followed by expansion and a clock-driven second phase. Compare metadata ages using the first traffic on new partitions. The scheduled quiet interval before expansion is not producer downtime.",
    "topology.delete-recreate":
      "What happens when a familiar topic name refers to a new immutable topic identity? After an initial workload, the topic is replaced and another burst is offered. Compare keeping the old handle with explicitly closing and reopening it. Refusal or NotWritten through the stale handle must not be mistaken for successful delivery to the replacement topic.",
    "topology.multi-topic-isolation":
      "Does delaying one topic's broker affect an independently supplied topic on another broker? Separate bounded sources target one partition each, and runs vary lane count. Compare the two topics' progress under demand. Because their brokers differ, this does not establish isolation for topics sharing a connection.",
    "resources.memory-bounded-overload":
      "Which shared admission resource fills first under heavy offered load? After metadata warm-up, a deliberately slow encoder faces a fixed high offer rate; payload size changes descriptor versus input-byte pressure. Compare refusals, accepted queueing and the recovery probe. Metadata refresh is deferred here, so this fixture does not cover startup or refresh allocation under saturation.",
    "resources.wire-window-vs-latency":
      "Does outstanding byte credit restrict throughput on a high-latency network? A bounded source and fixed request depth run with several wire-window sizes. Compare actual byte-credit saturation and steady delivery after startup. A flat comparison means these runs did not make that window the limiting resource.",
    "resources.delivery-timeout-tuning":
      "How does the delivery deadline trade waiting for broker recovery against terminal outcomes? Different deadlines cross the same isolation window with written, queued and post-outage probes. Inspect Unknown, NotWritten and fatal state as well as latency. Some variants deliberately stop their source earlier, so compare equivalent phases before comparing throughput.",
    "resources.stop-polling-backpressure":
      "What happens when the application stops consuming delivery events but keeps offering records? A polling pause tests different event capacities. Compare admission pressure and draining when polling resumes. Delivery latency ends at event consumption, so the apparent acknowledgment gap can include time after a successful broker response was already visible.",
  });
  const readableNumber = (n) =>
    typeof n === "number" && Number.isSafeInteger(n) && n >= 0
      ? n.toLocaleString("en-US")
      : "unspecified";
  const readableTime = (n) =>
    typeof n === "number" && Number.isSafeInteger(n) && n >= 0
      ? n === 0
        ? "0 s"
        : n < 1e6
        ? `${n / 1e3} µs`
        : n < 1e9
        ? `${n / 1e6} ms`
        : `${n / 1e9} s`
      : "unspecified time";
  const readableBytes = (n) =>
    typeof n === "number" && Number.isSafeInteger(n) && n >= 0
      ? n >= 1024 && n % 1024 === 0
        ? `${readableNumber(n / 1024)} KiB`
        : `${readableNumber(n)} bytes`
      : "unspecified bytes";
  function workloadText(load, run) {
    const shape = load?.shape;
    const topic = run.topology.topics.find((t) => t.index === load?.topic);
    const destination = topic
      ? `topic “${topic.name}”`
      : "an unspecified topic";
    const route = load?.partitioning;
    let routing = "routing described in the run configuration";
    if (route === "RoundRobin") routing = "round-robin partition selection";
    else if (route?.Fixed) {
      routing = `fixed partition ${readableNumber(route.Fixed.partition)}`;
    } else if (route?.Keyed) {
      routing = `${readableNumber(route.Keyed.keys)} hashed keys${
        route.Keyed.skew_ppm
          ? `, with a ${
            readableNumber(route.Keyed.skew_ppm / 10000)
          }% preferred-key branch`
          : ""
      }`;
    }
    const pattern = load?.value_pattern === "Compressible"
      ? "repeated values"
      : load?.value_pattern?.Incompressible
      ? "deterministic random values"
      : "the recorded value corpus";
    const payload = `${readableBytes(load?.value_bytes)} per value and ${
      readableBytes(load?.key_bytes)
    } per key, ${pattern}; ${destination}, ${routing}`;
    if (shape?.OpenLoop) {
      const s = shape.OpenLoop;
      return `Clock-driven offers: ${
        readableNumber(s.rate_per_s)
      } records/s from ${readableTime(s.start_ns)} to ${
        readableTime(s.end_ns)
      }; ${payload}.`;
    }
    if (shape?.ClosedLoopUntil) {
      const s = shape.ClosedLoopUntil;
      return `Closed-loop source from ${readableTime(s.start_ns)} to ${
        readableTime(s.end_ns)
      }: up to ${
        readableNumber(s.outstanding)
      } accepted records awaiting delivery, with a safety budget of ${
        readableNumber(s.max_offers)
      } offers; ${payload}. The budget is a ceiling, not a fixed offered count.`;
    }
    if (shape?.ClosedLoop) {
      const s = shape.ClosedLoop;
      return `Finite source starting at ${readableTime(s.start_ns)}: ${
        readableNumber(s.count)
      } records, up to ${
        readableNumber(s.outstanding)
      } accepted records awaiting delivery; ${payload}.`;
    }
    return "This load's traffic shape is not described by this viewer; inspect the exact workload in Configuration, source and exact phase checks.";
  }
  function faultText(band) {
    const where = band.broker === null
      ? "application"
      : `broker ${band.broker}`;
    const when = `${readableTime(band.start)}–${readableTime(band.end)}`;
    const value = band.value, e = value?.effects;
    if (band.kind === "isolation") return `${when}: ${where} isolated.`;
    if (band.kind === "stop_polling") {
      return `${when}: the application pauses event consumption while sources may continue offering.`;
    }
    if (band.kind === "link_outage") {
      return `${when}: ${where}'s ${
        band.direction === "ToBroker"
          ? "outgoing request"
          : band.direction === "FromBroker"
          ? "return response"
          : "bidirectional"
      } path ${
        band.mode === "BlackHole"
          ? "holds bytes without delivering them"
          : "fails immediately"
      }.`;
    }
    const phase = {
      BeforeAppend: "before append",
      AfterAppend: "after append",
      BeforeResponse: "before response",
      Setup: "during connection setup",
    }[value?.phase] ?? "at the recorded hook";
    const api = value?.api === 3
      ? "Metadata"
      : value?.api === 0
      ? "Produce"
      : "eligible operations";
    const effect = value?.ramp
      ? `added delay ramps from ${readableTime(e?.delay_ns)} to ${
        readableTime(value.ramp.end_delay_ns)
      }`
      : band.kind === "service_delay"
      ? `adds ${readableTime(e?.delay_ns)}`
      : band.kind === "throttle"
      ? `returns a ${readableNumber(e?.throttle_ms)} ms throttle`
      : band.kind === "reject"
      ? `rejects with error ${readableNumber(e?.reject_error)}`
      : e?.outcome === "Disconnect"
      ? "disconnects"
      : "drops traffic";
    const probability = typeof value?.probability_ppm === "number"
      ? `${value.probability_ppm / 10000}% of eligible opportunities`
      : "the recorded opportunities";
    return `${when}: ${where} ${effect} ${phase} for ${api}, at ${probability}.`;
  }
  function markerText(m) {
    const d = m.detail;
    if (m.kind === "leader_move" && d?.MoveLeader) {
      return `${readableTime(m.at)}: partition ${
        readableNumber(d.MoveLeader.partition)
      } of topic ${readableNumber(d.MoveLeader.topic)} moves to broker ${
        readableNumber(d.MoveLeader.broker)
      }.`;
    }
    if (
      m.kind === "add_partitions" &&
      Array.isArray(d?.AddPartitions?.additional_leaders)
    ) {
      return `${
        readableTime(m.at)
      }: add ${d.AddPartitions.additional_leaders.length} partitions to topic ${
        readableNumber(d.AddPartitions.topic)
      }.`;
    }
    if (m.kind === "topic_recreate") {
      return `${
        readableTime(m.at)
      }: replace the topic with a new immutable identity.`;
    }
    if (d?.Close) {
      return `${readableTime(m.at)}: stop offers and request Close with a ${
        readableTime(d.Close.deadline_ns)
      } drain deadline.`;
    }
    if (d?.CloseTopic) {
      return `${readableTime(m.at)}: close the current topic handle.`;
    }
    if (d?.OpenTopic) return `${readableTime(m.at)}: open a new topic handle.`;
    return null;
  }
  function experimentOverview(run, bundle) {
    const c = run.config, d = c.driver;
    const firstLoad = (r) => r.meta.workload.active_intervals[0];
    const quantities = [
      [
        "outstanding record limit",
        (r) =>
          firstLoad(r)?.shape?.ClosedLoop?.outstanding ??
            firstLoad(r)?.shape?.ClosedLoopUntil?.outstanding,
        readableNumber,
      ],
      [
        "requests per connection",
        (r) => r.config.max_in_flight_per_connection,
        readableNumber,
      ],
      ["lanes per broker", (r) => r.config.lanes, readableNumber],
      [
        "descriptor admission",
        (r) => r.config.descriptor_admission_policy ?? "Shared",
        String,
      ],
      [
        "offered records/s",
        (r) =>
          r.meta.workload.active_intervals.find((l) => l?.shape?.OpenLoop)
            ?.shape.OpenLoop.rate_per_s,
        readableNumber,
      ],
      ["linger", (r) => r.config.linger_max, readableTime],
      ["request timeout", (r) => r.config.request_timeout, readableTime],
      ["delivery deadline", (r) => r.config.delivery_timeout, readableTime],
      ["metadata age", (r) => r.config.metadata_max_age, readableTime],
      [
        "retry backoff",
        (r) =>
          `${readableTime(r.config.retry_backoff.min)}–${
            readableTime(r.config.retry_backoff.max)
          }`,
        String,
      ],
      [
        "wire-byte window",
        (r) => r.config.connection_wire_window_bytes,
        readableBytes,
      ],
      ["batch target", (r) => r.config.batch_target_bytes, readableBytes],
      ["value size", (r) => firstLoad(r)?.value_bytes, readableBytes],
      ["compression", (r) => r.config.compression, String],
    ];
    const extras = {
      skip: [
        "skip linger for sparse traffic",
        (v) => v ? "enabled" : "disabled",
      ],
      partitions: ["partitions", readableNumber],
      skew: ["preferred-key branch", (v) => `${v / 10000}%`],
      random: ["value corpus", (v) => v ? "deterministic random" : "repeated"],
      burst: ["records per burst", readableNumber],
      move_seconds: ["leader move after outage begins", (v) => `${v} s`],
      order: [
        "bootstrap endpoints",
        (v) =>
          [
            "broker 1 then broker 2",
            "broker 2 then broker 1",
            "broker 1 only",
          ][v],
      ],
      overlap: ["restart window overlap", (v) => v ? "1 s" : "none"],
      duration: ["outage duration", (v) => `${v} s`],
      duty: ["outage duty cycle", (v) => `${v}%`],
      close_seconds: ["close drain deadline", (v) => `${v} s`],
      mode: ["transport outage", (v) => v ? "immediate failure" : "held bytes"],
      duration_ms: ["return-path outage", (v) => `${v} ms`],
      loss_percent: ["loss per eligible hook", (v) => `${v}%`],
      code: ["broker error", readableNumber],
      jitter_ms: ["maximum added completion jitter", (v) => `${v} ms`],
      reopen: ["reopen topic handle", (v) => v ? "yes" : "no"],
      events: ["delivery event capacity", readableNumber],
    };
    const effectiveExtra = (r, key) => {
      // Variant labels retain the requested sweep even when Test fixtures
      // shrink a workload. Prefer the actual run wherever it is recorded.
      if (r.meta.variant.deltas.extra?.[key] === undefined) return null;
      if (key === "burst") return firstLoad(r)?.shape?.ClosedLoop?.count;
      if (key === "partitions") {
        return r.topology.topics[0]?.initial_leaders.length;
      }
      if (key === "skew") return firstLoad(r)?.partitioning?.Keyed?.skew_ppm;
      if (key === "random") {
        return firstLoad(r)?.value_pattern === "Compressible" ? 0 : 1;
      }
      if (key === "jitter_ms") return r.config.driver.jitter / 1e6;
      if (key === "events") {
        return r.buckets.global.credits
          .capacity[r.buckets.global.credits.pools.indexOf("DeliveryEvents")];
      }
      return r.meta.variant.deltas.extra[key];
    };
    for (const [key, [label, format]] of Object.entries(extras)) {
      quantities.push([label, (r) => {
        const v = effectiveExtra(r, key);
        return Number.isSafeInteger(v) && v >= 0 ? v : null;
      }, format]);
    }
    const varied = quantities.flatMap(([label, get, format]) => {
      const values = [
        ...new Set(
          bundle.runs.map(get).filter((v) => v !== null && v !== undefined),
        ),
      ];
      return values.length > 1
        ? [`${label}: ${values.map(format).join(" / ")}`]
        : [];
    });
    const comparison = `This page contains ${bundle.runs.length} run${
      bundle.runs.length === 1 ? "" : "s"
    }${
      bundle.page.count > 1
        ? ` of ${bundle.page.total_runs} across ${bundle.page.count} pages`
        : ""
    }. ${
      varied.length
        ? `Parameters compared on this page — ${varied.join("; ")}.`
        : "No parameter variation is present on this page; other runs or seeds may be loaded separately."
    }`;
    // Bound presentation independently of the artifact's much larger load cap.
    const loads = run.meta.workload.active_intervals;
    const burstKey = (l) =>
      l?.shape?.ClosedLoop
        ? stable({
          count: l.shape.ClosedLoop.count,
          outstanding: l.shape.ClosedLoop.outstanding,
          topic: l.topic,
          partitioning: l.partitioning,
          lane: l.lane,
          value_bytes: l.value_bytes,
          key_bytes: l.key_bytes,
          value_pattern: l.value_pattern,
        })
        : null;
    const repeated = loads.length > 1 && burstKey(loads[0]) !== null &&
      loads.every((l) => burstKey(l) === burstKey(loads[0]));
    const traffic = repeated
      ? [
        `${loads.length} matching finite bursts; first at ${
          readableTime(loads[0].start)
        }, last at ${readableTime(loads.at(-1).start)}. ${
          workloadText(loads[0], run)
        } Each burst has the same record count and outstanding limit; exact start times are recorded in the workload configuration.`,
      ]
      : loads.slice(0, 6).map((l) => workloadText(l, run));
    if (!repeated && loads.length > 6) {
      traffic.push(
        `${
          loads.length - 6
        } further load phases are recorded; the final phase: ${
          workloadText(loads.at(-1), run)
        } Full timing is in the exact workload configuration.`,
      );
    }
    const bands = run.environment.bands;
    const faults = bands.slice(0, 6).map(faultText);
    if (bands.length > 6) {
      faults.push(
        `${bands.length - 6} further fault windows are recorded; the last: ${
          faultText(bands.at(-1))
        } Inspect the fault strip for every window.`,
      );
    }
    const changes = run.environment.markers.map(markerText).filter(Boolean);
    faults.push(...changes.slice(0, 6));
    if (changes.length > 6) {
      faults.push(
        `${
          changes.length - 6
        } further topology or lifecycle changes are recorded; the last: ${
          changes.at(-1)
        }`,
      );
    }
    if (!faults.length) {
      faults.push(
        "No timed fault or topology/lifecycle change is recorded. Network and service settings still contribute to latency.",
      );
    }
    const links = d.propagation_links.map((l) =>
      `broker ${l.broker}: ${readableTime(l.to_broker_latency_ns)} outbound / ${
        readableTime(l.from_broker_latency_ns)
      } return`
    ).join("; ");
    const selectedExtras = Object.entries(extras).flatMap(
      ([key, [label, format]]) => {
        const value = effectiveExtra(run, key);
        return Number.isSafeInteger(value) && value >= 0
          ? [`${label}: ${format(value)}`]
          : [];
      },
    );
    const settings = `${run.topology.brokers.length} brokers; ${
      run.topology.topics.reduce((sum, t) => sum + t.initial_leaders.length, 0)
    } initial partitions across ${run.topology.topics.length} topic${
      run.topology.topics.length === 1 ? "" : "s"
    }. ${c.lanes} lanes per broker, ${c.max_in_flight_per_connection} requests per connection, ${
      readableBytes(c.connection_wire_window_bytes)
    } wire-byte window. Batches target ${readableBytes(c.batch_target_bytes)} ${
      c.batch_target_mode === "EstimatedWire"
        ? "estimated wire bytes (including the batch header)"
        : "raw record bytes"
    }, with up to ${
      readableTime(c.linger_max)
    } linger; compression: ${c.compression}. Request timeout ${
      readableTime(c.request_timeout)
    }, delivery deadline ${readableTime(c.delivery_timeout)}, retry backoff ${
      readableTime(c.retry_backoff.min)
    }–${readableTime(c.retry_backoff.max)}, metadata age ${
      readableTime(c.metadata_max_age)
    }.${
      selectedExtras.length
        ? ` Additional settings: ${selectedExtras.join("; ")}.`
        : ""
    }`;
    const transport = `Base broker service ${
      readableTime(d.service_delay)
    }; propagation ${
      links || "uses the recorded transport model"
    }. Local completion latency ${
      readableTime(d.local_completion_latency)
    }, with up to ${
      readableTime(d.jitter)
    } extra jitter per modeled I/O completion. Transport chunks ${
      readableBytes(d.chunk_bytes)
    }, pipe capacity ${readableBytes(d.pipe_bytes)}; encoder progress ${
      readableBytes(d.encode_bytes_per_poll)
    } per ${readableTime(d.encode_cost_per_poll)} poll.`;
    const pools = run.buckets.global.credits;
    const capacityNames = {
      Descriptors: "record descriptors",
      InputBytes: "input storage",
      DeliveryEvents: "delivery events",
      Mailbox: "mailbox entries",
    };
    const capacity = `Shared admission capacities: ${
      pools.pools.flatMap((name, i) =>
        capacityNames[name]
          ? [`${
            name === "InputBytes"
              ? readableBytes(pools.capacity[i])
              : readableNumber(pools.capacity[i])
          } ${capacityNames[name]}`]
          : []
      ).join("; ")
    }.`;
    const reading = [
      "Delivery latency runs from acceptance to application consumption. Refused offers are excluded from that latency population. Compare partition progress with pending demand; an empty heatmap cell alone does not establish unavailability.",
    ];
    if (loads.some((l) => l?.shape?.ClosedLoop || l?.shape?.ClosedLoopUntil)) {
      reading.push(
        "Each closed-loop source shares its outstanding limit across its destinations and waits for deliveries to free slots. A slow destination can therefore stop that source offering to healthy partitions. Separately listed sources have independent limits; their progress can mask a stalled main source in aggregate charts.",
      );
    }
    if (loads.some((l) => l?.shape?.OpenLoop)) {
      reading.push(
        "Clock-driven sources keep their scheduled offer rate during pressure. A refused offer is recorded permanently rather than retried as part of the same offer.",
      );
    }
    if (run.meta.size === "test") {
      reading.push(
        `This is the smaller Test fixture: ${run.meta.workload.test_adjustments}`,
      );
    }
    return {
      intent: INTENT[run.meta.scenario.id] ?? run.meta.scenario.description,
      comparison,
      settings,
      transport,
      capacity,
      traffic,
      faults,
      reading,
    };
  }

  root.PRODUCER_EXPERIMENT_MODEL = Object.freeze({
    schemaRun,
    schemaBundle,
    parseArtifactText,
    validateData,
    derive: Object.freeze({
      viewRange,
      bucketSlice,
      markersAsSteps,
      sampledRecordSlice,
      experimentOverview,
      scenarioIntent: (id) =>
        INTENT[id] ??
          "Inspect delivery, admission and recovery under the selected scenario's exact workload and fault schedule.",
    }),
  });
})(globalThis);
