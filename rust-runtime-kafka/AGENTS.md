# Feature log

Features wanted over time, not yet scheduled. Add to this list as ideas come
up; move an entry out (or mark it done) when it lands.

## Producer

1. **Partially done: configurable request batching levels.** Batching policy should be a
   knob, not a fixed strategy: by byte size, by record count, by linger, up
   to the strictest form where a produce request carries a single partition.
   The current engine batches per broker; the strict single-partition mode
   is needed for clients that want per-partition request isolation.
   Define whether byte targets mean raw or estimated compressed bytes and
   whether the final record may cross the target. Use the matched Full Java
   comparison to measure packing differences under equal numeric settings;
   see `kafka/kr-kafka-experiments/CLASSIC_COMPARISON.md`.
   Implemented request grouping policies: SinglePartition, Sealed (default),
   and bounded BrokerReady gathering, independently of batch target mode.
   Rust/C/Java contracts are in `kafka/kr-kafka-producer/REQUEST_BATCHING.md`.
   Use `scripts/run-batching-policy-matrix.py --policy request-batching` to
   measure request packing and delivery tails. BrokerReady remains opt-in.
   Separate request record-count and linger policies remain open.
2. **Configurable key partition hash.** Today `routing.rs` hard-codes
   Java-compatible murmur2. librdkafka defaults to crc32 (`consistent_random`)
   and also offers murmur2 and fnv1a. Expose a partitioner choice so a
   producer can be key-compatible with either the Java client or librdkafka,
   with golden tests pinning each hash against known vectors.
3. **Expose a produce-batch primitive to wrapping clients.** A client layered
   above the producer (e.g. via `kr-kafka-ffi`) should be able to assemble a
   batch itself and hand it in as a unit, rather than only pushing records
   through the engine's internal batcher. Needs a design for how an
   externally built batch interacts with sequence numbers, admission bounds,
   and the partition-per-batch restriction above.

4. **Accept an unknown offset on a retried idempotent produce.** A broker
   that has evicted its producer-state cache can still pass the sequence
   check for a retry but no longer knows the record's offset, and answers
   success with base offset -1. Today `control.rs` rejects that combination
   as a malformed response. Make it a configurable success: the retry is
   acknowledged as delivered (the sequence check is the duplicate guard) and
   the delivery result carries no offset, surfaced as a distinct
   `OffsetUnknown` outcome rather than a fake value.

5. **Re-partition unassigned records on non-idempotent retry.** When a
   non-idempotent produce is retried, records whose partition was not pinned
   by the caller or the key (unkeyed, no explicit partition id) may
   optionally be re-routed to a different partition instead of retried
   against the same one. This lets an offline or leaderless partition be
   handled by moving the traffic rather than waiting it out. Records with an
   explicit partition or a hashed key always stay put. Needs a retry policy
   knob, and the routing decision must record whether a partition was
   assigned or chosen so the retry path can tell them apart.

# Requirements to verify

Non-feature work: properties the current design should already have, but
that need an explicit audit and tests before we trust them.

1. **Backpressure must flow through to request and batch creation.** Trace
   backpressure end to end: connection send-side pressure and the max
   in-flight limit should stop the engine from cutting new requests, so that
   records keep accumulating into larger batches while a broker is slow,
   rather than the engine sealing small batches that then sit in a queue.
   Confirm where the flush decision is made, that it observes both signals,
   and add a sim test that shows batch size growing under sustained
   backpressure instead of staying flat.
2. **Metrics well beyond what the Java client provides.** The current
   `telemetry.rs` is a small set of cumulative counters and maxima. Latency
   and size distributions (produce round trip, batch fill time, batch bytes,
   records per batch, queue wait, in-flight depth) should be recorded with
   `hdrhistogram` so percentiles are exact rather than averaged away, with
   per-broker and per-partition breakdowns where cheap. Recording must stay
   passive: no clock reads or allocation on the sim path, no effect on the
   determinism checkpoint.
3. **OpenTelemetry export.** At some point the metrics above (and possibly
   spans for a produce request's lifetime) need an OTel exporter. Keep the
   metrics model exporter-agnostic so OTel is one sink beside snapshot
   readers and the FFI, not the core representation.
4. **Measure host ingress lock and allocation overhead.** Compare an atomic
   empty-queue hint and reusable drain storage against the current host wake
   benchmarks. A plain `mem::take` moves allocation to the next enqueue rather
   than eliminating it. Any owner-local wake path must preserve ingress bounds
   and define its ordering against foreign wakes, aborts and send-spawns.

5. **Audit metadata publication under input-pool saturation.** The experiment
   harness reproduced bootstrap failure when 32k offers/s of 2 KiB records fill
   a 256 KiB InputBytes pool before the initial metadata response arrives:
   metadata snapshot retention fails, producing Fatal(12), TopicFailed(14), and
   early Closed. Verify whether metadata needs reserved headroom or retryable
   publication, including refresh with retained old snapshots. The steady-state
   admission-overload fixture warms up first and defers refresh beyond its load;
   that fixture does not establish bootstrap safety under saturation.

6. **Partially done: audit isolation of admission and terminal failure between partitions.**
   Measure whether one unavailable broker exhausts the shared descriptor pool
   and refuses healthy-destination offers. The opt-in `PartitionPressure`
   descriptor policy has independent continuous-demand, recovery and skew
   trials in `scripts/rerun-admission-trial.py`. Shared remains the default.
   Input-byte isolation, owner-selected/unresolved routing isolation, and
   partition-local recovery remain open. Ambiguous expiry recovers through
   a quiescent local epoch bump, with Java-compatible fresh-ID rollover at epoch
   exhaustion. The global pause and ordering limits are recorded in
   `kafka/kr-kafka-producer/RECOVERY.md`. Use the matched classic comparison to
   check healthy admission and post-expiry probes for both clients.
7. **Bound retry amplification and pre-dispatch waiting.** Audit request-wide
   retirement, retry ordering, batch sealing and fairness with exact blocker
   evidence before deciding which waits are necessary. Include slow-broker,
   rising-delay and repeated-throttle scenarios across multiple seeds.
   The runner in `kafka/kr-kafka-experiments/CLASSIC_COMPARISON.md` compares
   both clients across the full catalogue. Its request witnesses distinguish
   dispatch from broker observation; time before first broker observation can
   include many earlier dispatches.

## Bindings

1. **Done: Java producer client over FFM, copy profile.** JDK 25
   `Producer<K,V>` implementation with pinned jextract bindings, bounded
   admission, callbacks and metadata, plus ABI 2 snapshots, topic retirement
   and owner status. Usage and verification are in
   `kafka/kr-kafka-java/README.md`. Native-buffer/foreign input, blocking event
   waits and metrics export remain deferred in `kafka/JAVA_CLIENT_DESIGN.md`.

2. **Fold `UNKNOWN` deliveries into ordinary Kafka exceptions.** Today a native
   `UNKNOWN` outcome becomes `DeliveryUnknownException` (a new public type
   carrying reason and attempt count), which is the one place the fail-closed
   certainty discipline leaks into an otherwise drop-in `Producer` surface. An
   application written against `KafkaProducer` sees an exception type it cannot
   name. Map `UNKNOWN` onto the Kafka exception its reason implies (as the
   `NOT_WRITTEN` path already does) and keep the certainty detail as the
   attached `NativeDeliveryException`, so the visible contract is exactly
   Kafka's. Touches `KrKafkaProducer.outcome`, the missing-outcome path in
   `destroyAndFinish`, `OutcomeTest`, `kafka/integration/VerifyRecords.java`
   and the mapping table in `kafka/JAVA_CLIENT_DESIGN.md`.
3. **Clean-sheet producer interface that surfaces certainty.** The information
   dropped by (2) is real and worth exposing — just not on a type claiming to
   be Kafka's `Producer`. Design a separate, non-Kafka Java interface where
   `Applied` / `NotApplied` / `MayHaveApplied` is a first-class part of the
   delivery result rather than an exception subtype, so callers that want to
   reason about replay safety can, and the compatibility facade stays
   compatible. `DeliveryUnknownException` moves there.
4. **Support unbounded producer retries.** Kafka's `retries` defaults to
   `Integer.MAX_VALUE` and is bounded in practice by `delivery.timeout.ms`;
   ours defaults to 254 and rejects anything outside 1..254, because
   `ProducerConfig::max_attempts` is a `u8` and the ABI event `attempts` field
   is validated to 0..255. The cap was presumably chosen so an attempt counter
   fits a byte and terminal reporting stays exact — establish whether that
   still matters before widening it. Wanted: accept the Kafka default and any
   value up to `Integer.MAX_VALUE`, letting `delivery.timeout.ms` be the real
   bound. Needs a widened native attempt counter (or a saturating counter with
   an explicit "attempts exceeded the reportable range" encoding), an ABI
   revision for the event field, and tests that a record whose deadline passes
   mid-retry still expires deterministically.

## Compression

1. **zstd levels above 3.** `ZstdConfig::validate` in
   `kafka/kr-kafka-record/src/codec.rs` accepts only levels 1..3 (and
   `window_log` 10..23), and the Java binding mirrors that in
   `compression.zstd.level`. Nothing in the format or the wire protocol
   requires it: the limit exists because `CodecPool` warms a fixed number of
   contexts at construction and charges their `sizeof()` against a declared
   memory budget, and higher levels raise both workspace bytes and per-batch
   CPU time on the owner thread. Raising the ceiling means measuring workspace
   growth per level, deciding whether the codec budget scales with the
   configured level, and confirming the compression fairness accounting (SC02)
   still holds when a single batch costs several times more CPU.

2. **Partially done: compression-aware batching with bounded retained memory.** Both Java and
   Rust already stream records into zstd. Make estimated-wire batch targets
   the default, retaining raw targets as an explicit option. Keep raw-work and
   hard-output limits independent. Evaluate bounded output tail compaction,
   context scheduling and growth under backpressure.
   Keep finish capacity reserved and immutable retry payloads; emitted bytes
   alone do not predict final size. Source findings, codec probes and the
   proposed evaluation are in
   `kafka/kr-kafka-experiments/COMPRESSION_BATCHING_DESIGN.md`.

   Implemented: estimated-wire targets are the default, raw is an
   explicit Rust/C/Java option, dispatch pressure defers soft sealing, and a
   bounded final-tail copy releases oversized backing before publication.
   Per-partition estimates use completed frames and retain existing hard bounds.
   Use the Full classic matrix to measure packing, bounded memory and delivery
   latency, then `scripts/run-batching-policy-matrix.py --policy batch-target`
   for a same-code Raw/EstimatedWire comparison and additional-seed witnesses.
   Uncompressed linger/close packing and latency costs, request coalescing and
   long pre-dispatch waits remain tuning work.
   Topic fallback, size buckets, incremental output reservation and host CPU/
   allocation comparisons remain open; see the implementation status in the design.
