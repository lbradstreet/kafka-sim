# Producer distributions

`ProducerClient::status()` preserves its fixed counters and maxima. Distribution
readers use `client.metrics()` independently of the admission mutex. HDR
recording is passive: supplied runtime/model timestamps, no allocation, no clock
reads, no RNG draws, no tasks, and no wakes merely because it is enabled.

The default enables eleven global distributions with three significant digits.
The highest values are 600 seconds in nanoseconds, 1 GiB for bytes, and 1,048,576
for counts. Out-of-range samples are rejected and counted, never clamped. Counter
exhaustion is also explicit. `exact_max()` covers successful samples only;
rejected values do not contribute. Quantiles return inclusive equivalent-value
ranges at the configured precision, rather than claiming lossless nanoseconds.

| Metric | Sampling edge |
| --- | --- |
| Produce RTT | First fully confirmed write to validated matching response, once per request attempt |
| Batch fill | First record acceptance to the first seal transition |
| Batch raw bytes and records | Once at the first seal transition, including batches that later fail encoding |
| Batch wire bytes | Once when encoded output first exists, including the record-batch header; retries do not resample |
| Queue wait | Each record's acceptance to successful batch insertion |
| Delivery latency | Acceptance to terminal event publication, separately for Acked, NotWritten and Unknown |
| In-flight requests and wire bytes | After request-plan allocation/release; counts include queued requests using the engine's window |

Depth distributions are **event-weighted**, not time-weighted. Global depth is
the total across brokers; a broker's sample uses only that broker's depth.
Response-less attempts have no fabricated RTT sample. Missing timestamps and
backward durations are counted explicitly. Untimed passive callers can supply a
context with `engine.observe_metrics_time(now)` or use `fail_producer_at`.

Broker and partition scopes are opt-in because each scope adds eleven HDRs in
each bank. Set `max_broker_scopes` / `max_partition_scopes` and increase the checked
`max_storage_bytes` if necessary. Zero means intentionally omitted scoped samples,
with explicit omission counts; global samples remain present. Registration uses
the first configured number of identities and never evicts or reuses them.
Partitions are labeled by UUID and partition index, so a recreated topic cannot
merge with its predecessor. Creating broker or partition state registers a token
through a preallocated AVL index with logarithmic lookup. Nodes keep their token
positions across rotations; subsequent recording indexes fixed storage. The
memory subtotal includes the scope index's node backing.

`config.validate().memory.fixed_metadata.metrics` reports histogram counts and
requested metadata backing for all three banks. Allocation-free preflight uses
the pinned hdrhistogram 7.6.0 layout and is checked against the dependency in tests.
Permitted reservation slack, Arc control-block overhead and platform mutex
backing remain unaccounted
producer-core terms in [the memory report](MEMORY_ACCOUNTING.md). Allocator
rounding that is not exposed as capacity is a separate scope exclusion. Neither
the histogram subtotal nor the encompassing report is a process-memory bound.

## Snapshot ownership

Keep a `MetricsReader` from `client.metrics()`. Call
`client.request_metrics_snapshot()` for an explicit diagnostic request and owner
notification, then use `reader.try_take_snapshot()` to obtain the interval at a
later owner safe point. Direct passive engine users request through the reader
and call `engine.publish_metrics(now)` themselves. A reader request alone does not
wake any runtime. Only one request or untaken published interval is admitted at
a time. If both spare banks are retained, another request returns `NoSpareBank`;
recording continues in the active bank.

The owner performs one nonblocking exchange-lock attempt and an O(1) bank swap.
Platform mutex backing is initialized during construction; a poll without a
snapshot request skips the exchange lock entirely. Reader critical sections move bank ownership only. Histogram iteration, quantile
calculation, reset and cumulative merging never run under that lock or in the
owner poll. A snapshot is immutable, has a monotonic epoch and schema version,
and may be inspected on another thread. Dropping it resets its bank on the
reader's thread and returns it for reuse. Do not drop a snapshot inside an actor
poll. A retained reader or snapshot owns only telemetry storage, not the producer,
its buffers, credits, runtime, or providers.

Intervals have explicit supplied start/end times. The next interval starts at the
previous handoff boundary. Bounds stay absent if no runtime time was supplied;
exporters must not substitute a host clock. On owner destruction, the final
interval remains available after any older published interval. No snapshot is
silently overwritten. Exporters may merge interval bucket counts outside the
producer to form cumulative views. OpenTelemetry and FFI are external sinks;
neither defines the core histogram representation.
