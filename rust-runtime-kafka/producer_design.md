# Kafka producer design on kr-runtime

This document specifies a **bounded, batch-oriented Kafka producer with explicit ownership**, built on `kr-runtime`. The schema compiler, protocol codecs, producer, record-batch encoder, TLS/SASL integration and C ABI are implemented and undergoing validation. See the [producer guide](kafka/README.md) for build and validation commands.

The central design decision is:

> **An immutable, compressed partition batch is the unit of transmission and retry. Everything before it exists to construct that batch efficiently; everything after it moves references to it and tracks delivery.**

That gives the schema compiler, FFI interface, compression layer, scheduler, and networking backends a common representation to optimize around.

Here, “Kafka schema compiler” means **Apache Kafka’s protocol schemas → generated Rust wire codecs**. Application payload serializers can plug into the same length-aware encoding machinery, but Avro/Protobuf/Schema Registry support belongs in a separate layer.

The v0 target is an idempotent, nontransactional producer with zstd, TLS/SASL, Linux `io_uring` and readiness-based networking, and a bulk C ABI. Transactions are out of scope.

Part I (sections 1–12) gives the constraints and the reasoning. Part II (sections 13–31) fixes the v0 decisions and specifies the crates, types, credit ledger, state machines, wire plan, I/O extension, FFI, host wiring, and simulation oracle.

### Reuse the runtime; build the Kafka layer above it

`kr-runtime` already has two concrete single-owner executors, `HostRuntime` and `SimRuntime`. They share a task/timer kernel and the application-facing `RuntimeHandle` enum; there is no executor `Runtime` trait to implement. I/O providers are independent of the executor and expose owned completion futures. See the [runtime boundary guide](PRODUCTION-RUNTIME.md) and [shared I/O contract](io/kr-runtime-io/README.md).

| Concern | Existing foundation | Producer work |
|---|---|---|
| Execution | `HostRuntime`, `SimRuntime`, `RuntimeHandle`, local `!Send` tasks | Passive producer engine and a bounded owner actor |
| Time and randomness | `RuntimeInstant`, `RuntimeDuration`, sleeps, seeded streams | Linger/deadline policy, retry jitter, routing decisions |
| Stream I/O | `NetworkProviderSubmit`, `ByteStreamSubmit`, `ColdNetwork`, `ColdStream` | Kafka framing, connection state, bounded RX assembly |
| Reference and fault models | `MemoryNetwork`, `SimNetwork`, shared provider conformance tests | In-memory Kafka broker model and an independent delivery oracle |
| Linux networking | `UringNetPool`, `PooledUringStream`; also per-stream providers | Pool sizing, host wiring, provider-failure recovery |
| Blocking work | `HostBlocking`; `UringEnv::on_runtime` for providers that use that environment | Bounded DNS/authentication jobs and explicit completion reporting |
| Diagnostics | Runtime checkpoints and bounded binary SBE traces | Versioned producer history, replay manifest, protocol invariants |
| Missing transport pieces | Current stream writes own one `Vec<u8>` | Owned scatter/gather extension, readiness provider, TLS adapter |

Keep Kafka metadata, batching, sequencing, quotas, and delivery policy out of the runtime kernel. Extend the shared I/O contract only for reusable transport capabilities. In particular, the current API cannot transmit a generated scatter/gather plan directly: that requires a separately tested owned-buffer extension, described in section 9.

## 1. Start with the constraints that actually determine the architecture

### There are four different kinds of batching

They should share information, but they should not share a single timer or size threshold.

| Layer | What is batched | Main benefit |
|---|---|---|
| Language boundary | Many record descriptors in one FFI call | Amortizes language transitions and synchronization |
| Partition batch | Records for one topic-partition | Compression, sequence numbering, batch overhead |
| Produce request | Partition batches for one broker connection | Amortizes request processing and network overhead |
| Transport submission | Writes and other operations across sockets | Amortizes system calls and reactor work |

Two Kafka constraints make this distinction particularly important.

First, **Produce API v3+ requires exactly one magic-2 record batch per included partition**. This includes the zstd/idempotent versions targeted here. You cannot seal many small batches for a hot partition and later combine them into a large request for that same partition. See Kafka's [Produce request validation](https://raw.githubusercontent.com/apache/kafka/3.7/clients/src/main/java/org/apache/kafka/common/requests/ProduceRequest.java).

Second, **client request pipelining is not parallel request execution within one broker connection**. Kafka documents ordered processing and responses, with one broker-side request in progress per connection; additional client requests can already be buffered for subsequent processing. See the [Kafka network protocol contract](https://kafka.apache.org/41/design/protocol/#network).

Consequently:

- Partition batch size directly affects the maximum throughput of a hot partition.
- Request aggregation helps across partitions, but cannot repair premature sealing within one partition.
- A deeper pipeline can hide network delay, but cannot multiply the processing capacity of a serialized broker connection.

### Choose the delivery contract before optimizing

For v0, require `acks=all`, retries, and idempotency. Adopt Kafka's documented idempotent-client limit of at most five outstanding requests per connection, which reflects the broker's bounded batch history. This is a client-side safety constraint, not a broker admission rule rejecting request six. See [producer configuration](https://kafka.apache.org/41/configuration/producer-configs/#max.in.flight.requests.per.connection).

Internally, distinguish:

```text
accepted
    → assigned to partition
    → encoded/compressed
    → assigned sequence
    → possibly transmitted
    → acknowledged or terminally failed
    → result observed by application
```

Input-buffer release is a separate event, often much earlier than acknowledgement.

That distinction is foundational for both memory efficiency and FFI safety.

## 2. Derive the batching and pipelining model

The objective should be something like:

\[
\min \left(
\text{CPU per acknowledged record},
\text{retained byte-time}
\right)
\]

subject to a latency objective, bounded memory, ordering, and delivery semantics.

“Maximize batch size” and “keep five requests outstanding” are not sufficient objectives.

### 2.1 Pipeline capacity

For one connection, define:

- \(Q\): average encoded Produce-request size, in wire bytes.
- \(R(Q,k)\): average time from committing a request to transmission until its response is processed.
- \(k\): permitted outstanding requests.
- \(S_b(Q)\): effective serialized broker service time per request.
- \(C\): sustainable wire throughput of the remaining bottlenecks.

Little’s Law relates average outstanding work, throughput, and residence time. Applying it to this request window gives the necessary bound:

\[
X \leq \frac{kQ}{R(Q,k)}
\]

The broker’s serialized service imposes another bound:

\[
X \leq \frac{Q}{S_b(Q)}
\]

Thus a useful planning model is:

\[
\boxed{
X \lesssim
\min\left(
C,\;
\frac{kQ}{R(Q,k)},\;
\frac{Q}{S_b(Q)}
\right)
}
\]

These are capacity constraints, not a complete queueing model. In particular, \(R\) changes with request size and congestion; it is not simply network ping latency. Little’s Law itself concerns averages, not a p99 guarantee.

**Example.** Suppose the target is 250 MiB/s, average request residence time is 4 ms, and \(k=5\). The request-window constraint requires:

\[
Q \geq \frac{250 \times 0.004}{5}
= 0.2\ \text{MiB}
\approx 205\ \text{KiB}
\]

But if serialized broker service takes 2 ms per request, a 205 KiB request supports only about 100 MiB/s through that service stage. Reaching 250 MiB/s would require approximately 512 KiB requests, lower service time, or parallelism across independent connections.

For a single hot partition, those larger requests require larger **partition batches**.

### 2.2 Batch formation has a quantifiable latency cost

For fixed-size batches of \(n\) records and Poisson arrivals at rate \(\lambda_p\) to a partition, the mean gathering delay is:

\[
E[W_{\text{gather}}]
=
\frac{n-1}{2\lambda_p}
\]

This follows by averaging the remaining arrivals needed by each position in the batch. It excludes compression, scheduling, network, and broker time.

Suppose 100,000 records/s are distributed uniformly across 100 partitions. Each partition receives about 1,000 records/s. A 100-record batch then has approximately:

\[
\frac{99}{2 \times 1000}
=49.5\text{ ms}
\]

of mean gathering delay.

Concentrating an unkeyed run on one partition at the full arrival rate reduces the corresponding delay to approximately 0.495 ms.

That is why batching-aware partitioning matters so much. It changes the arrival process seen by the accumulator; it is not merely a hashing optimization.

### 2.3 Queueing near saturation defeats latency objectives

A useful single-bottleneck approximation is Kingman’s:

\[
E[W_q]
\approx
\frac{\rho}{1-\rho}
\frac{c_a^2+c_s^2}{2}
E[S]
\]

where \(c_a\) and \(c_s\) describe arrival and service-time variability. It is not an exact model of a replicated Kafka cluster, but it captures the important interaction between utilization and variability.

The utilization multiplier is 4 at 80% utilization and 19 at 95%.

The practical conclusion is not “always operate at 80%.” It is:

> **Find the measured throughput/latency knee, and reject excess work rather than turning it into an ever-growing producer queue.**

A larger buffer is useful for an explicitly budgeted burst. It is not a solution to sustained overload.

### 2.4 Request aggregation also couples latency

Kafka’s delayed-produce handling waits for the involved partitions to become satisfied or encounter a terminal condition before completing the response. A slow partition can therefore delay results for other partitions in the same request.

A simplified model is:

\[
R_{\text{request}}
\approx
R_{\text{transport}}
+
\max_{p \in \text{request}} R_p
\]

Therefore, “aggregate every eligible partition into the largest possible request” is not always desirable.

I would group by compatible latency class and cap request size and partition count. However, splitting requests on the **same connection** does not eliminate connection-level head-of-line blocking. Strict isolation requires separate connection lanes or separate producer instances.

## 3. The scheduling policy I would actually ship

I would use **size-or-age batching, acknowledgement-driven dispatch, and explicit byte budgets**. I would not start with several interacting adaptive controllers.

### Partition batch formation

Each open logical batch tracks:

```text
first_record_accepted_at
oldest_delivery_deadline
uncompressed_encoded_bytes
record_count
target_bytes
hard_byte_limit
compression_state
seal_reason
```

Its gathering deadline is approximately:

\[
t_{\text{seal}}
=
\min\left(
t_{\text{first}}+\text{linger}_{\max},
D_{\text{oldest}}-\widehat{T}_{\text{remaining}}
\right)
\]

Here, \(\widehat{T}_{\text{remaining}}\) includes compression and downstream service allowance. It is an estimate, not a promise that remote delivery will meet the deadline.

Seal on the earliest applicable condition: target size, gathering deadline, flush watermark, required compression-context reclamation, or a hard limit before appending the next record.

**Never restart linger when another record arrives.** Otherwise, a busy partition can keep extending the oldest record’s wait.

Drain a bounded amount of already-available ingress before deciding to send. This obtains batching without an additional timer delay. Similarly, when broker credits are unavailable, let records accumulate naturally rather than sealing every tiny arrival into an immutable batch.

For very sparse traffic, skip intentional linger when another arrival during the permitted interval is unlikely. Under a Poisson approximation, that probability is \(1-e^{-\lambda_p L}\). At 100 records/s and 250 μs, it is only about 2.5%.

### Produce-request construction

Build a request **just in time**, when dispatch credits are available.

Select eligible partition heads, preserve per-partition FIFO order, and gather their existing compressed chunks. Do not add a second request-level linger timer and do not wait for every partition to have data.

I would permit at most one partially transmitted request per connection, rather than accumulating a deep queue of fully constructed unsent frames.

Use both:

```text
outstanding_request_count <= configured_limit <= 5
outstanding_wire_bytes   <= connection_byte_budget
```

The byte budget matters because five 8 KiB requests and five 1 MiB requests are very different queues.

An initial byte target can be based on desired sustainable throughput times a low-congestion response-time estimate. **Do not grow it automatically because congested response time increased**; that creates positive feedback:

```text
more queueing → larger measured latency → larger window → more queueing
```

Soft window targets must still allow one legal request to make progress. An oversized relative-to-target batch must not become permanently undispatchable.

### Fairness must exist before and after compression

I would use separate accounting at different bottlenecks:

- Admission: record slots and retained input bytes.
- Compression: uncompressed bytes, with bounded execution quanta.
- Transmission: encoded wire bytes.
- Delivery: reserved completion slots.

Deficit round robin is a reasonable v0 scheduler for active lanes and partitions, with bounded age-based priority boosts. Charge compression work in raw bytes and transmission work in wire bytes; one byte metric cannot represent both costs.

Fair dispatch alone is insufficient. A hot producer lane must not consume every input and completion slot before a cold lane gets admitted. Use per-lane allowances with a shared borrowable pool.

Under sustained overload, return backpressure. Do not silently drop, indefinitely extend queues, or claim a latency guarantee the broker cannot satisfy.

### Connection parallelism

I would default to one data connection per broker, but make the connection key:

```text
(broker_id, lane_id)
```

rather than baking one connection per broker into the architecture.

A fixed, small number of **partition-affine lanes** can be enabled for throughput or isolation. Each partition belongs to exactly one lane at a time. All lanes share the same broker-level budgets and quota handling.

Do not stripe successive batches of one partition across sockets as an easy throughput trick. That complicates ordering, recovery, and the unresolved sequence window.

This is useful parallelism across independent partitions—not a way around a single partition’s ordering requirements.

## 4. Make partitioning explicitly batch-aware

A partitioner should not merely implement:

```rust
fn partition(record: &Record) -> PartitionId;
```

That API hides the information needed for good batching and encourages a callback for every record.

### Default keyed behavior

Use Java-compatible keyed partitioning by default:

```text
toPositive(murmur2(serialized_key)) % partition_count
```

That is the algorithm used by Kafka's [built-in partitioner](https://raw.githubusercontent.com/apache/kafka/trunk/clients/src/main/java/org/apache/kafka/clients/producer/internals/BuiltInPartitioner.java). An empty key is still a key; it must not be treated as an absent key.

Do not reroute keyed records to faster partitions when their destination is overloaded. Backpressure is preferable to silently changing key placement.

Also document that changing partition count can change key placement; per-key ordering across such a change requires application-level care.

### Default unkeyed behavior

Use **byte-quota sticky routing**:

```text
Choose a partition.
Route an admitted run of approximately B encoded raw bytes to it.
Rotate after consuming that quota.
```

Rotation should be driven by admitted bytes—not acknowledgement timing or incidental batch creation.

Kafka’s KIP-794 identifies the feedback problem with older sticky behavior: slower destinations can accumulate larger batches and consequently receive disproportionate traffic. Its byte-based approach separates routing rotation from draining behavior.

I would expose two explicit policies:

**Uniform-byte sticky:** approximately balances admitted bytes across partitions over time.

**Adaptive sticky:** biases new unkeyed runs toward destinations with better estimated drain capacity.

These are different semantics. Strict uniformity and maximizing throughput over heterogeneous destinations are not simultaneously achievable in general.

### Plugin contract

Provide a bulk interface and an optional routing-lease interface:

```text
choose_partitions(record_metadata[], snapshot) → partition_ids[]

choose_run(snapshot, hints) →
    { partition_id, byte_quota }
```

The snapshot can expose bounded, read-only information such as queued bytes, oldest age, open-batch fill, broker throttling, and estimated drain rate.

A native policy may execute on the engine thread under a strict nonblocking contract. Managed-language policies should run on their own language thread, using a snapshot and returning bulk decisions. The engine must not enter Python, Java, or JavaScript once per record.

The engine always validates decisions and retains authority over memory admission, ordering, and delivery. A plugin cannot repartition a record during retry.

## 5. Runtime architecture and ownership

### Passive engine, portable actor, concrete host

Use the same layering as the [Quarry actor](dogfood/quarry/README.md): a passive engine holds protocol state, and a thin actor drives it with `RuntimeHandle` and injected I/O providers. The engine consumes explicit admission commands, times, routing choices, and operation results; it emits bounded work and delivery events. It does not read host time, spawn threads, or drive an executor.

For v0, use one owner actor per logical producer, with bounded, thread-safe bulk submission handles for application threads. Production creates and drives a `HostRuntime` on that owner thread. Embedded Rust callers can run the actor on an existing owner-local runtime. Simulation runs the same actor on `SimRuntime`, with modeled network and worker completions.

The intended composition is:

```text
application / C ABI -> bounded producer mailbox
                              |
                      producer actor (RuntimeHandle)
                              |
                 passive engine + bounded operation slots
                              |
                   ColdNetwork / ColdStream
                    /                    \
        MemoryNetwork / SimNetwork     UringNetPool / readiness provider
                SimRuntime                   HostRuntime
```

The readiness provider and producer layers in this diagram are proposed. A small number of long-lived actor/connection futures and one future per admitted I/O operation fit the runtime; individual records need no task or future.

The owner holds:

```text
metadata and topic handles
partition accumulators
producer identity and sequence ledgers
connection state
request slots
timers
buffer pools
completion state
```

Local state can use ordinary owner-local data structures and `!Send` tasks. Transfer only owned work and synchronized completion messages across actual thread boundaries. `HostSendHandle` can bootstrap `Send + 'static` tasks on the owner; it is not the record submission queue and must not spawn one task per submission or record.

### A bounded actor poll

```text
poll bounded ready I/O and worker completions; apply responses
process bounded expired deadlines
drain bounded ingress
advance bounded compression work
schedule eligible requests
poll new cold I/O futures into admission; retain pending operations
publish bounded host notifications
yield if immediate work remains; otherwise return Pending with wakes armed
```

Process acknowledgements early because they release resources. Register ingress, operation, and timer wakes before returning `Pending`, including the mailbox check/register/recheck needed to avoid a lost wake. If a work quota expires while immediate work remains, arrange another poll or use `yield_now`. Let the runtime park the host thread and advance timers; the producer does not implement a second reactor or call host sleep.

Use active queues and a producer deadline heap, with one armed sleep for the earliest deadline, rather than scanning every partition or allocating a runtime timer per record. Use `RuntimeHandle::now()` and `sleep_until()` with `RuntimeInstant`/`RuntimeDuration` for linger, retry, metadata, and delivery deadlines. These are monotonic runtime-relative coordinates, not Kafka record timestamps: callers supply record timestamps, or a separate injected timestamp source supplies them.

Route sticky choices and retry jitter through runtime workload randomness; the passive engine receives the resulting decisions. Simulation fault and completion-timing choices use the separate `Fault` and `Schedule` streams. A simulation poll consumes zero virtual time, and `yield_now` alone does not advance time. Tests that model compression cost or broker service must schedule delayed modeled completions; a continually self-waking producer would otherwise starve virtual deadlines.

`HostConfig` task, timer, and scheduler-ingress limits supplement producer limits. They do not bound producer payload bytes or completion obligations. Reserve actor/operation capacity at startup and treat runtime spawn failures as explicit admission/startup failures. Exhaustion of the runtime's infallible wake ingress is a runtime failure, not ordinary producer backpressure.

### Compression execution

Support inline compression and a fixed, bounded compression-worker configuration behind the same job interface.

Inline compression is the reference configuration. Worker execution becomes useful when compression would otherwise monopolize the owner. Jobs contain a batch or a bounded run of record descriptors. Define that adapter's job, byte, output, and terminal-result reservations independently of runtime task limits.

A live zstd stream belongs to one execution context until it is sealed. Do not submit successive mutations of that stream to arbitrary workers. Whole deferred-batch jobs may use `HostBlocking`; progressive worker compression requires an explicitly provisioned fixed worker lane with context affinity.

`HostRuntime::blocking()` / `HostHandle::blocking()` supplies `HostBlocking::submit`, a host-only fire-and-forget closure facility. Its queue is unbounded and it has no typed result or cancellation future. A producer adapter must acquire all job credits before submission and guard terminal completion even when a job panics. Hold credits until the job actually terminates, including when its waiter is dropped. Keep long compression work from starving DNS/authentication or provider lifecycle work; use a separately budgeted worker fleet if fixed shared workers cannot provide that isolation.

Bound inline work by input bytes and measure its host duration when choosing the quota; a zstd call cannot be preempted by a runtime poll budget. Simulation uses fixed reproducible work quotas and the same encoder with owner-thread, modeled job completion; it never reads host timing or sends real worker-thread wakes into `SimRuntime`.

### Ownership representation

Use stable pool handles:

```text
BufferKey { slot, generation }
BatchKey
RequestKey
ConnectionKey
InputLeaseKey
```

Queues move handles and ranges, not payload copies. The owner can maintain ordinary local reference counts; buffers handed to provider threads require owned, thread-safe leases. Runtime task generations and producer pool generations are separate identities.

A generation protects against stale handles. It does not substitute for correct kernel and application buffer lifetimes.

Reserve control-plane resources separately. Metadata refresh, authentication, reconnect, cancellation, and completion publication must still make progress when the data budget is exhausted.

## 6. The FFI should expose ownership, not Rust implementation details

### Three input paths

| Path | Contract | Intended use |
|---|---|---|
| Copy submission | Producer copies accepted payloads before returning | Safe default for managed runtimes |
| Native buffer acquisition | Host serializes into producer-owned memory, then commits it | Preferred high-throughput path |
| Registered foreign lease | Host guarantees immutable, stable storage until release | Native callers and carefully implemented bindings |

The copy path should make **one intentional boundary copy** into pooled storage. It should not then copy into an uncompressed batch and again into a request buffer.

The native-buffer path avoids that boundary copy: a language serializer writes directly into a buffer acquired from the client.

The foreign-lease path must not pretend arbitrary garbage-collected memory can safely be retained. For example, Go explicitly constrains which pointers C may retain and how their backing memory must remain pinned.

### Bulk API shape

The C ABI should provide operations corresponding to:

```text
producer_create
topic_open
buffer_acquire / buffer_commit / buffer_release

submitv_copy
submitv_leased

poll_events
flush
close
destroy
```

Use opaque handles, fixed-width integers, explicit pointer-and-length spans, and versioned structures with `struct_size`. Represent null and empty distinctly.

A submission returns an accepted prefix count. Only accepted records acquire producer obligations; the remainder remains unaccepted. Reserve the mailbox space, input bytes, descriptors, and completion capacity atomically before reporting acceptance. Registered lease handles make partial submissions manageable without ambiguous ownership transfer of an entire input vector.

Delivery is observed through event polling. Once admission commits, a concurrent
poller may observe an event before the submitting thread returns. Callers that
require return-before-observation ordering must serialize submission and event
polling. The sequential simulation oracle below uses that ordering.

Do not expose Rust `Vec`, `String`, trait objects, futures, or Rust-layout enums.

### Input release and delivery are different events

For a record using a foreign input lease:

```text
INPUT_RELEASED(lease_id)
DELIVERY(record_token, outcome, metadata)
```

The first permits the binding to unpin or reclaim source memory. The second reports the Kafka result.

With streaming compression, source input can often be released once consumed, while the compressed representation remains available for retry.

Allocate input slabs with lifetime in mind. A giant lease spanning thousands of partitions may remain pinned because of one slow record. Avoiding a copy is not worthwhile when it pins an unnecessarily large allocation.

### Completion capacity is reserved at admission

Every accepted record must already have capacity for its eventual delivery result, and every accepted lease must have capacity for its release notification.

If the application stops polling, new submissions eventually receive backpressure. The engine must not:

```text
allocate an unbounded completion queue
drop completion events
block its I/O thread in a language callback
```

The binding drains events in bulk and invokes language callbacks on the language’s own thread or executor.

`poll_events` drains producer events; it does not drive `HostRuntime` from a foreign thread. Thread-safe FFI handles contain mailbox/event capabilities, never an owner-local `HostHandle` or runtime controller. The FFI and any raw-pointer lease implementation belong in a separate audited adapter crate; the runtime kernel keeps its `forbid(unsafe_code)` contract.

Prevent unwinding across the C boundary; catch supported Rust panics at the ABI boundary and transition the affected engine to a defined failed state. Foreign callbacks must not unwind into Rust. Rust’s FFI documentation distinguishes these unwind contracts explicitly.

## 7. Compile Kafka schemas into wire programs, not object graphs

This is where I would invest substantial v0 effort.

Apache Kafka’s JSON schemas encode version ranges, nullability, flexible encodings, tagged fields, defaults, and other generation rules. Some versions intentionally share layouts while differing in behavior, so schema generation alone cannot replace semantic capability handling.

### Compiler pipeline

```text
Pinned Kafka JSON schemas
    → validated protocol IR
    → layout-equivalence classes
    → generated request planners
    → generated response views/visitors
    → generated compatibility and boundary tests
```

Pin the schema revision. Generate offline and review generated diffs. Do not fetch a moving Kafka branch from `build.rs`.

Reject unknown schema constructs rather than guessing their meaning.

### Generate one version dispatch, not field-by-field branching

Group versions with identical wire layout into one implementation.

At request planning time, dispatch once to the appropriate layout. Avoid repeated checks such as `if version >= 9` inside every field and nested array iteration.

Also avoid excessive specialization across every API version × transport backend × compression implementation. The generated codec should produce a backend-independent send plan.

### Make external payload spans a first-class operation

The generated writer should support operations equivalent to:

```text
write_scalar
write_varint
write_compact_length
write_inline_metadata
emit_external_span
```

A Produce request becomes a plan containing small metadata fragments and references to sealed batch chunks.

```text
request header and topic metadata
    → partition metadata and batch header
    → compressed chunk references
    → following partition metadata
    → compressed chunk references
```

The target send plan needs no request-sized flattening buffer. The initial implementation over today's contiguous `WriteRequest` needs bounded staging; eliminating that copy depends on the owned scatter/gather extension in section 9.

Lengths should come from scalar metadata, array counts, and cached batch lengths. Planning may walk descriptors, but it should not scan opaque key/value bytes a second time.

Keep the small metadata in a compact arena. Copying a few integers and names together is usually preferable to producing hundreds of tiny `iovec`s. Cap scatter/gather count and deliberately coalesce small fragments.

### Generate borrowed response views

A Produce response parser should operate over the receive frame using bounded views and iterators, rather than allocating an owned tree of vectors and strings.

Validate frame lengths, array counts, tags, integer arithmetic, and correlation before applying state changes. Long-lived metadata should be copied into the metadata cache intentionally, rather than retaining an entire receive buffer for one broker name.

Use safe byte decoding, not casts from wire bytes to Rust structs.

### Important compatibility cases

These need dedicated tests, not generic assumptions:

- Flexible request headers still encode `ClientId` using its classic string representation.
- `ApiVersions` has a response-header exception even when the response body is flexible; see its [response schema](https://raw.githubusercontent.com/apache/kafka/trunk/clients/src/main/resources/common/message/ApiVersionsResponse.json).
- Compact nullable lengths and record-format signed varints are different encodings.

The [Produce schema](https://raw.githubusercontent.com/apache/kafka/trunk/clients/src/main/resources/common/message/ProduceRequest.json) records zstd support beginning at version 7, flexible encoding at version 9, and replacement of topic names with topic IDs at version 13. Negotiate the highest version whose **semantics the implementation has tested**, not merely the highest version the generator recognizes. These upstream links explain the design; generated artifacts must record an immutable schema revision.

### Keep record encoding specialized

The record-batch encoder deserves a separate optimized component.

Before feeding a record into compression, compute its encoded length from key/value/header lengths and timestamp/offset deltas. Emit scalar prefixes from small scratch storage and feed large payload spans directly.

A payload serializer can participate through:

```text
encoded_len(value)
encode_into(value, sink)
```

But an unknown-length serialization cannot magically bypass Kafka’s length prefix. It needs a size pass or bounded staging. Avoid a foreign-language callback for every serialized field.

## 8. Streaming zstd: minimize retained input, not pretend the whole path is zero-copy

### A batch must be finalized before transmission

Kafka’s magic-2 record batch has an uncompressed fixed header. The records are compressed, and CRC32C covers the attributes-through-end portion of the batch, including the compressed records.

Consequently, this design streams compression **into owned memory**, not directly into the TCP socket.

The final lengths and checksum must be available before the batch can be transmitted. Given the objective of releasing raw input early and retrying cheaply, the compressed payload becomes the canonical replay representation.

### Encoding path

I would implement:

```text
record descriptors and input leases
    → specialized record encoder
    → small optional coalescing scratch
    → zstd stream
    → pooled compressed chunks
```

Use one zstd frame per record batch, no external dictionary, and no nested zstd worker pool.

Feed with `ZSTD_e_continue`; at sealing, drive `ZSTD_e_end` until it reports completion. When progressive compression starts before total size is known, use the documented unknown-content-size mode rather than pledging the eventual maximum as though it were the actual size.

Do not flush zstd after each record. Kafka batching and zstd flushing are separate decisions.

A small coalescing buffer is useful for tiny records and headers. Kafka’s own Java zstd implementation also buffers small writes before the compressor, so streaming compression alone should not be presented as a novel advantage over existing producers.

Compression sealing freezes the compressed record payload and releases the context. Wire finalization happens after producer identity and sequence assignment: fill the remaining header fields, then calculate CRC32C across the header suffix and compressed chunks. Only then is the entire batch immutable for ordinary retries. For v0, use a straightforward checksum pass; it reads the payload without copying it.

### Do not allocate one compressor per partition

A producer with thousands of intermittently active partitions must not create thousands of full zstd contexts.

Use a bounded pool of \(K\) live contexts. Compression workspace is a separate memory cost, governed by codec parameters and stream state—not just the amount of compressed output.

I would use two states of the same logical batch machinery:

**Progressive:** a batch has a context and consumes arriving input incrementally.

**Deferred:** it holds bounded descriptors/input leases until it acquires a context, often at sealing time.

This is a deliberate tradeoff. Progressive compression releases raw input earlier; deferred compression avoids monopolizing contexts for cold partitions.

Do not keep a compression context while waiting for a broker acknowledgement. Sealed batches contain output bytes, not live compressor state.

### Make output capacity a real resource

A subtle trap: zstd’s source explicitly warns that `ZSTD_compressBound()` is a one-pass bound, not a general bound for arbitrary streaming patterns with frequent flushes.

For v0:

1. Fix and validate the codec configuration and stream pattern.
2. Enforce independent raw-batch and compressed-batch hard limits.
3. Reserve sufficient output capacity before activating a compressor.
4. Treat exceeding the hard output limit as a pre-transmission failure—not permission to exceed the memory budget.

Admission reserves input, descriptors, and completion obligations. Activation additionally reserves its output envelope. Keep transform/control reserves separate so a full input queue cannot prevent accepted work from being compressed and drained.

Do not rely on expected compression ratio for safety.

Also avoid promising cheap automatic splitting of an already-compressed oversized batch after its input has been released. Splitting may require decompression and re-encoding. I would leave that recovery path out of v0 and make size-limit errors explicit.

### Account for allocation capacity and fragmentation

The memory budget should include:

\[
\begin{aligned}
M ={}&
M_{\text{input}}
+M_{\text{compressed}}
+K M_{\text{codec}}\\
&+M_{\text{TLS/RX}}
+M_{\text{descriptors}}
+M_{\text{completions}}
+M_{\text{allocator slack}}
\end{aligned}
\]

Count each physical allocation once, using its retained capacity rather than only its used length.

A 500-byte batch should not automatically retain a 64 KiB output slab. Use small size classes for sparse batches, larger chunks for growing streams, and optionally compact a tiny sealed payload when doing so saves substantial retained memory.

**An intentional small copy can be the correct memory optimization.**

Separately account for kernel socket buffers and foreign-runtime memory. A client-owned memory bound is not a promise about the entire host process’s RSS.

## 9. Network backends should share a buffer-lifetime contract

### Use the existing owned stream boundary

Use [`kr-runtime-io`](io/kr-runtime-io/README.md), whose application surface is `ColdNetwork<P>` and `ColdStream<S>` over the `NetworkProviderSubmit` and `ByteStreamSubmit` provider traits. The current stream operations are partial read/write, write-half shutdown, and close. Provider-native addresses stay in host/model wiring; the producer resolves broker endpoints through an injected resolver.

There are two precise admission points:

```text
cold operation method -> construct future; no admission yet
first poll            -> attempt admission once
warm submit_* method  -> attempt admission during the call
admitted operation    -> owned by provider until terminal completion
```

Use cold operations by default, and an explicit warm submission only where systems code needs that synchronous admission point. Retain the actual operation futures in bounded connection slots across producer polls. There is no current public stage/commit/wait reactor API or general per-operation cancel token to wire into the producer. Submission batching is a provider responsibility.

A never-polled cold future has not started. After admission, dropping the future abandons observation, not the operation: a dropped read can still consume subsequent bytes. A timer winning a race must not cause the driver to drop and recreate an admitted read. Keep it until completion, or close the connection and let the provider finish teardown.

The protocol engine receives normalized results and progress, independent of readiness events or CQEs. A successful partial write advances a cursor through the request; only its unsent suffix is scheduled next. On uncertain progress, retire the connection and reconcile the original batch through the Kafka retry ledger rather than guessing where to resume the byte stream.

### Contiguous baseline and owned scatter/gather extension

The [current request types](io/kr-runtime-io/src/network.rs) own a single `Vec<u8>`: `ReadRequest { buffer, max_bytes }` and `WriteRequest { buffer }`. A write success returns the original buffer with `bytes_written`; `NetworkFailure` carries returned ownership and `bytes_transferred` where applicable. There is no shared immutable span, write offset, or vectored request in this API yet.

First make the producer correct with a bounded staging buffer over that contract. Keep immutable compressed retry chunks separately, copy the next bounded span into staging, and retain/repack the unwritten suffix after partial writes. Charge that capacity and every copy to the budget and benchmarks. Never allocate an unbounded request-sized buffer as an implicit fallback.

For the intended optimized path, extend the shared I/O boundary with an owned vectored-write capability. Define it in memory first, then simulation, Linux, and readiness implementations with the same conformance suite. Its contract must specify:

- Owned immutable backing allocations plus validated ranges; no temporarily borrowed `IoSlice` may outlive its owner.
- Hard bounds on segment count, total payload bytes, and retained allocation capacity, with checked length arithmetic.
- A partial-progress cursor across segment boundaries and error progress/certainty semantics.
- Ownership returned on rejection, retention through admitted work, and terminal release even when the response waiter is abandoned.
- Thread-safe shared ownership for buffers reaching provider threads, while the producer ledger retains the retry reference.

This is a reusable I/O extension, not a Kafka-specific provider interface. Preserve the cold/warm admission distinction. Keep read and write resource reserves separate: a blocked send must not consume the capacity needed to receive acknowledgements. Provider credits and producer credits may cover the same allocation at different boundaries; the aggregate physical-memory account counts it once.

### Readiness backend

Add a readiness implementation of the same owned stream contract, using nonblocking sockets plus `epoll` for the initial Linux target. It is a v0 requirement, not an existing `HostRuntime` facility. A non-Linux backend requires an explicit platform implementation and conformance run. The current Tokio facade's host reexports do not supply this native provider.

A pending write retains its send plan across partial writes and `EAGAIN`. Readiness causes the backend to attempt progress and publish the same logical events as the `io_uring` backend.

Enable write readiness only while blocked on a pending write. Its provider host completes owned futures through ordinary wakers accepted by `HostRuntime`; it must not feed real host wakes into `SimRuntime`.

### io_uring backend

Reuse [`UringNetPool` and `PooledUringStream`](io/kr-runtime-io-uring/src/pooled_network.rs) for broker connections. The pool already owns one ring/reactor thread and one coordinator thread, with connect/listen/accept on the pool and no additional threads per registered stream. The original `UringNetwork` / `UringByteStream` providers remain available, but their per-stream resources are a different deployment cost.

The current pool reserves sustained ring capacity for both directions of each registered stream, runs at most one active operation per direction, and bounds queued commands. Align `UringNetPoolConfig` stream, queue, ring-entry, and operation-byte limits with the producer's connection and memory budgets. The pool has its own provider threads; `HostRuntime` owns the protocol actor and receives normal cross-thread wakes. It does not own or poll the ring.

Feature-probe at startup. Add explicit `required` and `auto` producer configuration so policy either reports ring unavailability or selects the readiness provider. Until readiness is implemented and tested, `auto` cannot promise a fallback. `UringEnv::on_runtime(runtime.blocking()?)` is the existing wiring for providers that accept an environment; `UringNetPool::new` currently provisions its own pool and does not accept that environment.

Keep one send operation and one receive operation outstanding per TCP socket in the producer driver as well. These directions can proceed independently, and the producer need not fill an extra queue behind the provider's active operations.

**One outstanding kernel send does not mean one outstanding Kafka request.** Once a request’s bytes have been handed off, the next send can begin while earlier Kafka responses remain outstanding.

Retain both payload and operation metadata until terminal completion. The current provider close path interrupts the socket and drains terminal CQEs before releasing buffers; a requested close or an expired producer deadline is not itself proof of release. A pool failure may affect every connection on it, so recovery must terminalize/reconcile each affected ledger without attributing the failure to just one broker.

Defer `SEND_ZC`. Any later implementation must distinguish the initial send result from the notification that releases the payload; zero-copy send is not a v0 correctness dependency. See the [liburing send-zc completion contract](https://man7.org/linux/man-pages/man3/io_uring_prep_send_zc.3.html).

### TLS

Add TLS as a producer-side transform above the owned stream boundary, with bounded plaintext/ciphertext and handshake storage. Evaluate [rustls' unbuffered state-machine API](https://docs.rs/rustls/latest/rustls/unbuffered/index.html) for the adapter; it is not wired into the current I/O crates. Test real TLS framing and parsing over the modeled stream where possible, with explicit test credentials and injected clock/entropy dependencies when needed for reproducibility.

TLS is a transform boundary. Do not promise plaintext scatter/gather maps directly into zero-copy encrypted transmission.

Keep compressed retry bytes immutable, encrypt into reusable bounded storage, and generate fresh TLS records when retrying Kafka data over a connection. DNS and potentially expensive authentication setup use separately budgeted host jobs, with modeled implementations in simulation. Runtime-seeded workload randomness is for policy and tests; production TLS/SASL cryptography uses the security library's appropriate entropy source. Pin the supported SASL mechanism set before the security release gate.

## 10. Idempotency and recovery need their own explicit state machine

The sequence ledger is not an incidental field on the connection.

For each partition, track:

```text
producer identity / epoch
next sequence
ordered unresolved batch ledger
acknowledged progress
retry state
metadata / leader state
connection-lane assignment
```

Reserve sequence numbers only after the batch is sealed and the resources needed to dispatch it are secured. Otherwise, local compression or allocation failure can create avoidable sequence holes.

Here, sealed means the compressed payload is complete. Assign identity/sequence fields and finalize the batch header/CRC before admitting its first write. From that point onward, retain the entire immutable batch through its unresolved lifetime.

### Ordinary retries

For an ordinary retry, preserve:

```text
partition
producer ID and epoch
base sequence and record count
compressed record payload
```

The enclosing request, correlation ID, and destination broker may change.

Process Produce responses per partition. A request can contain successful and failed partition results; do not retry the successful ones merely because another partition failed. The response schema explicitly carries partition-level results.

Keep the unresolved batch limit across reconnects and leader changes. Creating a new socket must not reset the logical sequence window.

### Ambiguous outcomes

Expose at least three delivery classes:

| Outcome | Meaning |
|---|---|
| `ACKED` | A successful Kafka acknowledgement was observed |
| `NOT_WRITTEN` | The client can establish that the operation was not written |
| `UNKNOWN` | It may have been written, but a definitive outcome was not established |

A timeout after possible transmission is not evidence of non-delivery.

`kr_runtime::CompletionCertainty::{NotApplied, Applied, MayHaveApplied}` describes the effect of one provider operation, not Kafka delivery. A successful socket write or an `Applied` transport failure is not `ACKED`. A later `NotApplied` write failure does not establish `NOT_WRITTEN` if an earlier fragment or attempt may have reached the broker. Classify delivery from the full attempt history and parsed Kafka partition responses.

Similarly, losing producer state is not solved by obtaining a new producer identity and blindly resending ambiguous records. KIP-360 discusses why producer-state loss and unsafe sequence resets can compromise correctness.

Do not map every out-of-order-sequence response directly to “fatal,” either. Later batches can encounter sequence errors because an earlier batch remains unresolved. Kafka’s producer implementation maintains explicit unresolved-sequence and recovery state for these situations.

An ambiguous terminal outcome does not permanently fail the producer. Fence new assignments, settle all old possibly transmitted batches and request plans, then increment the epoch under the same producer ID. Re-finalize only never-transmitted retained batches, restarting each partition at sequence zero. Terminal `Unknown` records stay terminal and are never replayed under the new identity. Capacity reclamation uses the same quiescent identity change.

At the maximum epoch (32,767), follow the nontransactional Java producer: retire old connections, request a fresh producer ID with `InitProducerId`, and install the actual returned identity. Epochs never wrap. A fresh producer ID cannot fence remote writes carrying the old ID, so cross-ID ordering of those ambiguous records is not guaranteed. Ordinary same-ID epoch fencing takes effect per partition when the broker sees the newer epoch. Genuine fatal failures remain sticky. The [implemented recovery contract](kafka/kr-kafka-producer/RECOVERY.md) records these boundaries and the remaining producer-wide pause.

This remains idempotent production within the supported producer lifecycle—not end-to-end exactly-once processing across arbitrary application restarts and application-level resubmissions.

### Flush, cancellation, and close

`flush()` captures admission watermarks and waits for those accepted records to reach terminal outcomes. It does not continuously absorb records submitted afterward.

Dropping a language future stops waiting; it does not withdraw a produce operation.

Local cancellation can be supported before sequence assignment. After that point, cancellation must respect the sequence ledger, and after possible transmission it cannot promise non-delivery.

Closing stops admission and drives a defined drain/failure procedure. `destroy()` cannot free buffers still owned by the kernel merely because a shutdown deadline expired.

The owned-host shutdown order is:

1. Stop producer admission and capture the close watermark.
2. Drive accepted work to terminal delivery outcomes; after the close deadline, classify unresolved work conservatively. Once delivery draining finishes or its deadline expires, stop new data reads/writes/connects and explicitly close every producer-owned connection, including idle connections with a pending receive. Immediately close any stream returned by a late connect completion.
3. Drain admitted transport/worker work to actual terminal release and publish all reserved delivery/input-release events. A close response alone is not a substitute for observing release of the outstanding operations. Event storage survives until consumed or explicitly discarded by destruction.
4. Release provider and blocking-capability handles, then call `HostRuntime::finish()` and report checked teardown failures.

Keep the runtime alive while normal close awaits provider/worker completions. `HostControl::request_stop()` is an emergency executor stop, not a producer flush: it can drop the owner actor before the protocol drains. A host wrapper must retain enough shared obligation state to publish failure outcomes and preserve provider-owned memory after runtime failure. Dropping a runtime join handle detaches its task; explicit abort only drops the future at a scheduler boundary and does not roll back admitted I/O. For an embedded actor, close releases only that producer's resources and leaves the caller's runtime under caller control.

Worker and provider teardown can outlive a delivery deadline. `HostBlocking` workers live while any capability clone remains, and their closures cannot be forcibly cancelled. If `destroy()` has to wait for safe release, document that behavior rather than promising a hard shutdown-time bound.

## 11. Release requirements and validation

The first release should contain the architectural properties that are painful to retrofit:

| Area | v0 requirement |
|---|---|
| Runtime integration | One bounded owner actor on `RuntimeHandle`, shared by host and simulation |
| Ownership | Bulk submission, input leases, separate release/delivery events |
| Memory | Explicit input/output/workspace/completion budgets |
| Protocol | Generated version-aware planning and parsing; owned scatter/gather transport extension |
| Compression | Bounded zstd streams; no full uncompressed batch image |
| Scheduling | Size-or-age batching, count-and-byte windows, admission fairness |
| Routing | Keyed compatibility and byte-sticky unkeyed routing |
| Networking | Production-quality readiness and io_uring backends with identical semantics |
| Correctness | Idempotent retry ledger, ambiguous outcomes, deterministic fault tests |
| Security | TLS and a defined SASL mechanism set; no blocking control work on the reactor |

I would defer transactions, external compression dictionaries, dynamic thread autoscaling, sophisticated adaptive compression, zero-copy receive, and transparent oversized-batch reconstruction.

### Starting parameters—not claimed universal optima

For an initial benchmark profile, I would explore raw batch targets around 128 KiB, a configurable hard cap, 250 μs–1 ms maximum intentional gathering delay, zstd level 1 versus 3, and a small fixed context pool.

Request targets and byte windows should then be chosen from the measured service curve and latency objective. A fixed “five requests, 1 MiB each” default is not a substitute for that measurement.

### Validation must cover the whole pipeline

Use differential wire tests against Kafka’s implementations, including compact encodings, null versus empty, tags, header exceptions, CRCs, and independently decompressed record batches.

Build a bounded in-memory Kafka broker model above the real `MemoryNetwork`/`SimNetwork` byte-stream providers. It decodes actual producer requests, stores partition logs and producer sequence history, and encodes actual responses. Keep an independent delivery/order oracle so the broker and producer cannot validate each other by sharing the same recovery implementation. The broker model, leader changes, commit-before-response loss, and Kafka quota/identity behavior are new test infrastructure; `SimNetwork` supplies transport faults, not Kafka semantics.

Run the same producer actor and contract scenarios over the reference stack, simulated faults, and real Kafka integration tests with each production backend. Extend the [shared provider conformance suite](io/kr-runtime-io/src/conformance.rs) for vectored writes and run it against every implementation before treating that path as interchangeable.

Use deterministic simulation for partial writes, delayed completions, disconnect-after-commit, lost responses, leader changes, throttling, cancellation races, stale metadata, exhausted budgets, and host-event polling that stops. Include delayed worker results, owner shutdown with admitted reads/writes still live, all deliveries acknowledged while an idle broker leaves a read pending, and a connect completing after close begins. Assert:

- One terminal delivery event per accepted record, no delivery obligation for a rejected suffix, and one release event per accepted lease obligation.
- No `ACKED` without a matching successful Kafka partition response, and no `NOT_WRITTEN` after an unresolved possibly transmitted attempt.
- Per-partition sequence/order safety and unchanged compressed payload/identity/sequence on ordinary retries.
- Conservation of input, output, workspace, request, worker, and completion credits, including failure and abandonment paths.
- No buffer reclamation before provider/worker release, no duplicate admission from repeated cold-future polls, and no lost reads when a deadline wins a race.
- Progress for control work and cold lanes under hot-lane overload, and safe reclamation after cooperative teardown.

The simulation scheduler is deterministic FIFO; changing a seed does not automatically randomize its ready queue. Use explicit `Schedule` choices through `SimNetwork::new_with_schedule_random` and a versioned jitter model, plus separate workload/fault plans. Bound scheduler actions, virtual duration, task/timer capacity, operations, and process-watchdog time. Include nonzero runtime start times. `SimNetwork` latency delays local completion while bytes may become peer-visible earlier, so model broker processing delay explicitly and do not interpret it as a full network propagation simulator.

Keep ordinary campaigns untraced. On failure, rerun with bounded `SbeRecordingTrace` capture and retain a versioned producer-domain history alongside the runtime's [binary trace artifact](tools/trace-tool/README.md). Kafka events belong in that domain history rather than the runtime's `EventKind`. Persist the code/schema/model/driver versions, configurations, initial state, workload, realized fault plan, RNG inputs, and all driving/stop budgets. `RuntimeReproduction` is only the kernel fragment of this manifest; a seed or trace fingerprint alone is insufficient for replay.

Compare the typed harness result and independent oracle with the terminal `DeterminismCheckpoint`. Checkpoint equality is a rerun canary, not proof of correct Kafka delivery. After the root completes, explicitly drain and distinguish `Idle`, `Stalled`, and `Stopped`, then call `SimRuntime::finish()` so teardown failures are observed. A stalled run with accepted obligations is a liveness failure unless the scenario deliberately leaves an external dependency unavailable.

Benchmark with identical acknowledgement, idempotency, compression, and TLS settings against established clients. Include sparse traffic, one hot partition, many partitions, skewed keys, incompressible data, slow brokers, and actual language bindings.

Measure:

```text
CPU per acknowledged record
raw and wire throughput
latency by pipeline stage
batch fill and sealing reasons
outstanding request bytes and counts
retained input / compressed / codec memory
copies and allocation counts
actor work per poll and runtime/provider queue pressure
host completion-drain delay
retries and ambiguous outcomes
```

Use open-loop offered load and include admission delay and rejection rates. A benchmark that slows its own offered load whenever the client stalls can conceal the queueing behavior this design is intended to control. Follow the workspace's [benchmarking guidance](BENCHMARKING.md); record backend, provider topology, all byte budgets, runtime configuration, and whether the path still uses contiguous staging. Simulation validates semantics; host measurements establish CPU cost and throughput.

## 12. Implementation sequence

Each stage leaves a reviewable contract and executable reference path before adding another production dependency.

1. **Producer contract and passive engine.** Define bulk admission, lease/delivery ownership, budgets, flush watermarks, close, and the idempotent ledger. Implement the simplest bounded in-memory engine and independent model tests. Fix the error/outcome semantics before optimizing representation.
2. **Wire codecs and broker model.** Pin Kafka schemas; implement the required negotiation, metadata, identity, Produce, and error-response subset with differential tests. Drive actual frames through the in-memory broker and current contiguous stream API.
3. **Runtime actor and simulation.** Wire `RuntimeHandle`, the bounded mailbox, retained operation futures, one deadline sleep, and modeled worker completions. Run fault, conservation, cancellation, and replay campaigns on `SimRuntime`; run the same actor on `HostRuntime` with `MemoryNetwork` for host boundary tests.
4. **Compression and owned vectored I/O.** Add bounded zstd and pool ownership, then the shared transport extension with reference/simulation conformance first. Preserve the staging path as a correctness baseline and measure the copies removed by the new path.
5. **Production transport and security.** Wire `UringNetPool`, implement readiness parity, and add bounded DNS, TLS, and the declared SASL mechanisms. Run real-broker recovery and shutdown tests on each supported host configuration; ring fallback is enabled only after readiness passes.
6. **C ABI and performance gate.** Put raw foreign-memory handling in its adapter crate, test partial bulk admission and bindings that stop polling, and benchmark the full release semantics under open-loop load. Admit worker compression or extra connection lanes only with measured benefit and unchanged correctness checks.

---

# Part II — Concrete contracts

Part I fixes the principles. This part turns them into the crates, types,
state machines, budgets, and test obligations an implementer can build and
review against. Where Part I left a choice open, this part makes it and says
why; a change to one of these decisions is a design change, not a detail.

## 13. Decisions fixed for v0

| Decision | Choice | Why |
|---|---|---|
| Topic identity | Kafka topic ID (KIP-516 UUID) is the in-memory key; the name is an attribute | A recreated topic is a different topic; the producer must not silently route old records to it |
| Minimum broker capability | Produce v13 and Metadata v12 | Strict topic-ID identity requires IDs in both metadata refreshes and Produce requests. Reject missing capabilities at ApiVersions; the former Kafka 3.1 floor is superseded. |
| Produce version | v13 required; v9–v12 producer fallback deferred | User-selected strict identity: a same-name successor cannot receive an old-ID request. Older generated codecs remain compatibility fixtures, not enabled producer modes. |
| Other APIs | `ApiVersions` v3, `Metadata` v12, `InitProducerId` v4, `SaslHandshake` v1, `SaslAuthenticate` v2 | Smallest tested set that covers negotiation, ID-keyed routing, identity, and auth |
| Compression | `zstd` and `none` | `none` is the benchmark baseline and the incompressible-data escape hatch |
| SASL | `PLAIN`, `SCRAM-SHA-256`, `SCRAM-SHA-512`; TLS required for `PLAIN` | Bounded, well-specified; `OAUTHBEARER` deferred |
| Acks | `all` only | Idempotence requires it; `acks=1`/`0` would need separate outcome semantics |
| Connections | one per `(broker_id, lane_id)`, `lanes ∈ 1..=4`, default 1 | Section 3 |
| In-flight | `max_in_flight ∈ 1..=5`, default 5, plus a wire-byte window | Section 2.1 |
| Sequence assignment | at dispatch, after seal and credit acquisition | Section 10 |
| Epoch recovery | same-ID local bump after global quiescence; fresh broker ID at epoch exhaustion; never replay terminal Unknown records | Section 10 and [recovery contract](kafka/kr-kafka-producer/RECOVERY.md) |
| Staging path | ships in v0 and stays as the correctness baseline | Section 9 |
| Vectored write | shared I/O extension, memory → sim → uring → readiness | Section 9 |
| Readiness provider | `epoll` over `rustix`; joins the unsafe-permitted set only if `rustix` cannot express something | Keeps `forbid(unsafe_code)` where possible |
| FFI | one adapter crate, the only other `unsafe` site | Section 6 |

## 14. Crate layout

```text
kafka/kr-kafka-protocol      generated codecs, planner, response views      no runtime dependency
kafka/kr-kafka-record        record-batch encoder, CRC32C, zstd stream, chunk pool
kafka/kr-kafka-client        shared control codecs, connection driver, connector and security config
kafka/kr-kafka-producer      engine, ledger, scheduler, actor, mailbox, events
kafka/kr-kafka-broker-model  in-memory broker over ByteStreamSubmit + independent oracle   test-support
kafka/kr-kafka-host          request-independent UringNetPool/readiness, TLS, DNS, SASL  Linux
kafka/kr-kafka-producer-host producer owner thread, host composition and codec calibration
kafka/kr-kafka-ffi           C ABI, raw-pointer leases                                   unsafe allowed
io/kr-runtime-io             + vectored write extension (ByteStreamVectoredSubmit, SharedBytes)
io/kr-runtime-io-readiness   epoll provider implementing the owned stream contract        Linux
```

Dependency direction is strictly downward: `producer` depends on `client`,
`record`, `protocol`, `kr-runtime`, and `kr-runtime-io`. The shared `client` and
`host` crates do not depend on producer policy or record encoding.
`producer-host` composes `producer` with `host`; a future consumer can reuse
`client` and `host` without depending on the producer. `broker-model` depends on `protocol` and
`record` so it decodes the real wire format, but it must not depend on
`producer`: the oracle is only independent if it cannot import the ledger.

`kr-kafka-protocol` and `kr-kafka-record` are `#![forbid(unsafe_code)]`,
`no_std + alloc`-compatible, and have no I/O. Their tests are differential
against fixtures captured from the Java client.

## 15. Identity and keys

Every pooled object has a generation-tagged key. Generations are per pool and
unrelated to runtime task generations.

```rust
pub struct Slot<T> { index: u32, generation: u32, _tag: PhantomData<T> }

pub type InputLeaseKey  = Slot<InputLease>;    // one accepted lease (copy slab or foreign pin)
pub type RecordToken    = u64;                 // dense per producer, returned in DELIVERY
pub type BatchKey       = Slot<Batch>;
pub type RequestKey     = Slot<InFlightRequest>;
pub type ConnectionKey  = Slot<Connection>;    // (broker_id, lane_id) resolves to at most one live key
pub type ChunkKey       = Slot<Chunk>;         // compressed output chunk
pub type CodecKey       = Slot<CodecContext>;  // live zstd context

pub struct TopicId([u8; 16]);                                  // Kafka topic UUID, the only topic identity
pub type  TopicSlot     = Slot<Topic>;                         // dense local index for hot paths; maps 1:1 to a TopicId
pub struct TopicPartition { topic: TopicSlot, partition: i32 }
pub struct Sequence(i32);                                      // wraps at i32::MAX per Kafka
pub struct ProducerIdentity { producer_id: i64, epoch: i16 }
```

`RecordToken` is assigned at admission from a monotonic counter; it and the
opaque topic handle are the only identities that cross the FFI.

### 15.1 Topics are keyed by ID

```rust
pub struct Topic {
    id: TopicId,
    name: String,                       // attribute for initial resolution and diagnostics; Produce v13 uses the ID
    partitions: Vec<PartitionMeta>,     // leader, leader_epoch, lane, accumulator slot
    state: TopicState,                  // Resolving | Ready | Deleted
    generation: u32,                    // bumped on every metadata change that alters partition count or leaders
}
```

The cache is `HashMap<TopicId, TopicSlot>` plus `Vec<Topic>` indexed by slot;
`HashMap<String, TopicId>` exists only to serve `topic_open(name)`. The
v0 producer requires Produce v13; name-based Produce fallback is deferred. Metadata requests are issued by ID
(`Metadata` v12 `Topics[].TopicId`), so a topic whose name was reused after
deletion resolves to `UNKNOWN_TOPIC_ID` for the old ID and is marked
`Deleted` rather than being rebound to the new ID. Records already admitted
under the old ID fail `NotWritten { reason: TopicDeleted }`; the application
must reopen the name to obtain the new ID.

A `Metadata` response that returns a different ID for a name the producer
holds open is not an error for the open handle: the handle keeps its ID, and
the name map is updated to the new ID for future opens. The partition count
of a topic can only grow; a response that shrinks it or changes an existing
partition's topic ID is `INCONSISTENT_TOPIC_ID`-class and fails the refresh
closed.
 A stale `Slot` lookup returns `None` and is a
bug in the engine, so the engine treats it as an invariant violation
(`EngineError::StaleKey`) and fails closed rather than ignoring it.

## 16. Configuration

```rust
pub struct ProducerConfig {
    // Delivery contract
    pub delivery_timeout: RuntimeDuration,         // default 120 s; per-record override allowed, never longer
    pub request_timeout:  RuntimeDuration,         // default 30 s; one attempt on one connection
    pub max_in_flight_per_connection: u8,          // 1..=5, default 5
    pub connection_wire_window_bytes: u32,         // default 4 MiB; see 2.1
    pub lanes: u8,                                 // 1..=4, default 1

    // Batching
    pub batch_target_bytes: u32,                   // default 128 KiB raw
    pub batch_hard_bytes: u32,                     // default 1 MiB raw; also caps compressed output
    pub linger_max: RuntimeDuration,               // default 500 µs; never restarted
    pub linger_skip_below_rate: Option<u32>,       // records/s below which linger is skipped; default 200
    pub request_target_bytes: u32,                 // default 512 KiB wire
    pub request_hard_bytes: u32,                   // default 1 MiB wire; must be <= broker socket.request.max.bytes
    pub request_max_partitions: u16,               // default 64

    // Memory budgets (client-owned bytes; each counts retained capacity once)
    pub input_bytes: usize,                        // default 64 MiB
    pub record_descriptors: u32,                   // default 1 M
    pub compressed_bytes: usize,                   // default 64 MiB
    pub codec_contexts: u8,                        // default 4
    pub staging_bytes_per_connection: u32,         // default 256 KiB (contiguous path only)
    pub rx_bytes_per_connection: u32,              // default 1 MiB; must hold one max response frame
    pub control_reserve_bytes: usize,              // default 2 MiB; metadata/auth/reconnect only

    // Completion capacity
    pub delivery_event_capacity: u32,              // >= record_descriptors; default = record_descriptors
    pub release_event_capacity: u32,               // >= max live leases
    pub mailbox_capacity: u32,                     // bulk submissions queued to the actor; default 1024

    // Routing
    pub unkeyed_policy: UnkeyedPolicy,             // UniformBytes { run_bytes: 64 KiB } | Adaptive { .. }
    pub partitioner: PartitionerConfig,            // Builtin | Native(Box<dyn NativePartitioner>) | External

    // Protocol
    pub client_id: String,
    pub compression: Compression,                  // Zstd { level: 1..=3 } | None
    pub bootstrap: Vec<BrokerEndpoint>,
    pub metadata_max_age: RuntimeDuration,         // default 5 min
    pub topic_resolve_timeout: RuntimeDuration,    // default 60 s; name → ID resolution for a fresh handle
    pub max_open_topics: u32,                      // default 1024; bounds the ID-keyed cache and pending queues
    pub pending_records_per_topic: u32,            // default 65536; records held in `Accepted` while a topic resolves
    pub security: SecurityConfig,                  // Plaintext | Tls { .. } | SaslTls { mechanism, .. }
    pub transport: TransportPolicy,                // Uring | Readiness | Auto
}
```

Validation is total and happens in `ProducerConfig::validate()`, which returns
a `#[non_exhaustive] ConfigError` naming the field. The invariants the engine
relies on and therefore validates up front:

- `batch_target_bytes <= batch_hard_bytes <= request_hard_bytes`.
- `request_target_bytes <= request_hard_bytes`.
- `rx_bytes_per_connection` must hold the largest frame the cluster sends.
  Produce responses are small, but a Metadata response is not; the first
  Metadata frame length is checked against it and a too-small value is a
  startup failure, not a silent truncation.
- `delivery_event_capacity >= record_descriptors`: every admitted record can
  always publish its outcome.
- `input_bytes + compressed_bytes + codec_contexts * codec_workspace_bytes +
  lanes * brokers_max * (staging + rx) + control_reserve` is reported as the
  configured client-owned bound; it is a number the operator can read back,
  not a promise about RSS.

## 17. Credit ledger

Every bounded resource is a named credit pool. A credit is acquired at exactly
one point, held by exactly one owner at a time, and released at exactly one
point. The table is the conservation contract the simulation asserts.

| Pool | Unit | Acquired at | Held by | Released at |
|---|---|---|---|---|
| `mailbox` | submission | client `submit_bulk` | mailbox slot | actor drains the slot |
| `descriptors` | record | admission | record → batch | batch terminal |
| `input_bytes` | byte | admission (copy or lease) | lease | lease consumed by encoder, or batch terminal if never consumed |
| `release_events` | event | admission of a lease | lease | application drains the event |
| `delivery_events` | event | admission of a record | record | application drains the event |
| `codec_contexts` | context | batch activation | batch (progressive or deferred-at-seal) | seal complete |
| `compressed_bytes` | byte | batch activation (output envelope) | batch chunks | batch terminal |
| `staging_bytes` | byte | request dispatch (contiguous path) | connection | write terminal |
| `request_slots` | request | request dispatch | connection | response processed or connection retired |
| `wire_window` | byte | request dispatch | connection | response processed or connection retired |
| `rx_bytes` | byte | connection open | connection | connection released |
| `control_reserve` | byte | control operation | metadata/auth/reconnect job | job terminal |
| `worker_jobs` | job | compression/DNS/SASL job submission | adapter | job terminal, including panic |

Rules:

- **Admission is atomic across pools.** `submit_bulk` computes the longest
  prefix for which `mailbox`, `descriptors`, `input_bytes`, `delivery_events`,
  and (if leased) `release_events` can all be reserved, reserves them, and
  returns that count. It never reserves for a record it does not accept.
- **Activation is separate from admission.** `codec_contexts` and
  `compressed_bytes` are reserved when a batch is activated, and a batch that
  cannot be activated stays deferred with its input retained. Activation
  reserves the full output envelope `bound(batch_hard_bytes)`; unused envelope
  is returned at seal.
- **Control never borrows from data.** `control_reserve` is disjoint from
  every data pool, and control work never waits on a data pool.
- **Per-lane allowances with a borrowable pool.** `descriptors`,
  `input_bytes`, and `delivery_events` are split into a guaranteed per-lane
  allowance plus a shared pool. A lane reserves from its allowance first, then
  borrows. Borrowed credits are returned to the shared pool before the
  allowance is refilled, so a hot lane cannot hold a cold lane's guarantee.
- **Terminal means terminal.** A credit held by an admitted I/O or worker
  operation is released only on the operation's terminal completion, never on
  future drop, deadline expiry, or close request.

The engine keeps a `CreditLedger` with `reserve(pool, n) -> Result<Credit,
ResourceExhausted { resource, limit }>` and `Credit` is a non-`Copy` token
whose `Drop` in debug builds panics if it was not explicitly released, so a
leaked credit is a test failure rather than a slow leak.

## 18. State machines

### 18.1 Record

```text
Accepted ──(topic Ready, partition assigned)──▶ Queued ──(encoded into batch)──▶ InBatch
InBatch  ──(batch terminal)──▶ Delivered(outcome) ──(event drained)──▶ [released]
Accepted ──(topic Deleted / resolution deadline)──▶ Delivered(NotWritten)
Queued   ──(local cancel / partition failed)──▶ Delivered(NotWritten)
```

A record never has state of its own beyond a token, its descriptor, and the
batch it joined. `InBatch` records share their batch's fate; there is no
per-record retry. `Accepted` is the only state in which a record has no
partition: it is waiting in the topic's bounded pending queue for the topic
to leave `Resolving`. Pending records hold their admission credits like any
other, so an unresolvable topic exerts backpressure instead of growing.

### 18.2 Batch

```text
Open(Deferred)   ── activate ──▶ Open(Progressive)
Open(*)          ── seal ──▶ Sealing ── zstd end / CRC-less ──▶ Sealed
Sealed           ── sequence assigned + header/CRC finalized ──▶ Ready
Ready            ── placed in request ──▶ InFlight(attempt n)
InFlight         ── partition response OK / DUPLICATE_SEQUENCE ──▶ Acked
InFlight         ── retriable error ──▶ Ready (retry, same identity/sequence)
InFlight         ── request lost / connection retired ──▶ Ready { transmitted: true }
InFlight         ── definitive not-written error ──▶ Failed(NotWritten)
InFlight         ── fatal error ──▶ Failed(NotWritten)
Ready/InFlight   ── delivery deadline ──▶ Failed(if transmitted { Unknown } else { NotWritten })
Sealed/Ready     ── partition fail-closed ──▶ Failed(NotWritten)
```

`transmitted` is sticky: it becomes true the first time any byte of a
request containing the batch is admitted to a write whose result is `Applied`
or `MayHaveApplied` (including a partial write), and never returns to false.
It is the only input to the `Unknown` vs `NotWritten` distinction for
deadline and connection-loss failures.

A `Ready` batch with `transmitted == true` is a retry candidate and keeps its
sequence. A `Ready` batch with `transmitted == false` may be re-finalized
under a new epoch (18.4); a `transmitted` batch never is.

### 18.3 Request

```text
Building ── plan complete ──▶ Sending(cursor) ── write terminal, cursor == len ──▶ AwaitingResponse
Sending  ── write Applied/MayHaveApplied failure ──▶ Retired { transmitted: true }
Sending  ── write NotApplied failure, cursor == 0 ──▶ Retired { transmitted: false }
Sending  ── write NotApplied failure, cursor > 0 ──▶ Retired { transmitted: true }
AwaitingResponse ── response frame parsed ──▶ Resolved (per-partition results applied)
AwaitingResponse ── request_timeout / connection retired ──▶ Retired { transmitted: true }
```

Requests are resolved strictly in FIFO order per connection. A response whose
correlation ID is not the FIFO head is a protocol violation: the connection is
retired with `transmitted = true` for every in-flight request. Because
idempotent retries are safe, "retire and re-send everything with the same
sequences" is always a correct recovery, only a slow one.

### 18.4 Partition ledger

```text
Idle ── first batch Ready ──▶ Active { unresolved: [batches in sequence order] }
Active ── OOOS/UNKNOWN_PRODUCER_ID on non-head batch ──▶ Active (hold; head decides)
Active ── terminal sequence rejection, assigned expiry/cancellation, or capacity reclaim ──▶ NeedsIdentity
NeedsIdentity ── all old possibly transmitted batches and request plans settled ──▶ RefreshingIdentity
RefreshingIdentity ── epoch < 32767 ──▶ install (same PID, epoch+1) locally
RefreshingIdentity ── epoch == 32767 ──▶ retire old connections, InitProducerId(null) for fresh PID
RefreshingIdentity ── bounded rewrite of never-transmitted batches complete ──▶ Active (sequences from 0)
Any state ── genuine fatal ──▶ FailedClosed
FailedClosed: preserve parsed outcomes and cumulative certainty; no recovery
```

The pinned Kafka `TransactionCoordinator.scala` implementation (lines 124–132)
returns a fresh producer ID and epoch zero for a nontransactional
`InitProducerId` request, even when prior PID/epoch fields are supplied. The
engine uses this path only at epoch exhaustion and installs the returned
identity. Ordinary recovery increments the epoch locally, without a broker
identity request or retiring idle connections.

Reissuing a terminal ambiguous batch under a new identity could duplicate an
earlier commit. The application therefore continues to see `Unknown` for that
record, and the engine never reissues it. The identity change allows later
records to progress without asserting a new outcome for the old record. A
same-ID higher epoch fences old writes after the partition observes it; a fresh
PID at exhaustion does not fence old-PID stragglers. See the explicit ordering
boundary in section 10 and the implemented recovery contract.

`NeedsIdentity` is producer-wide in this implementation: the bump waits for
every partition's old transmitted work to settle. Records can still be admitted
within the configured bounds, and their original deadlines keep running.
Existing old-identity retries may dispatch during settlement, but new assignments
wait for installation to complete. Java's partition-local migration remains
follow-up work; a later deadline on another unavailable partition extends the
global pause.

### 18.5 Connection

```text
Resolving ── DNS job ──▶ Connecting ── connect ──▶ Handshaking(TLS) ── ──▶ Negotiating(ApiVersions)
Negotiating ── ──▶ Authenticating(SASL) ── ──▶ Active
Active ── retire(reason) ──▶ Draining { close future retained } ── close terminal + all ops terminal ──▶ Released
* ── control_reserve exhausted / fatal ──▶ Released (after draining admitted ops)
```

Invariants:

- At most one admitted read and one admitted write per connection. The read is
  re-armed immediately on completion while `Active`; the write is armed only
  while a request is `Sending`.
- The read future is never dropped while admitted. A deadline or retire
  decision sets a flag; the driver keeps polling the read until terminal, then
  transitions. Same for the write.
- `Draining` holds the stream's `close` future and every still-admitted
  operation future; `Released` returns `rx_bytes`, `staging_bytes`, and the
  connection slot only after all are terminal.
- Reconnect backoff is `min(cap, base * 2^n) + jitter`, jitter drawn from the
  `Workload` stream via `RuntimeHandle::random_below`.

### 18.6 Topic

```text
Opened(name) ── Metadata by name ──▶ Resolving(id) ── Metadata by id, partitions known ──▶ Ready
Ready ── UNKNOWN_TOPIC_ID on refresh by id ──▶ Deleted (pending + Queued records → NotWritten { TopicDeleted }; in-flight batches keep their fate)
Ready ── metadata_max_age / NOT_LEADER / UNKNOWN_TOPIC_ID on Produce ──▶ Ready (refresh by id; generation++ on change)
Resolving ── name unknown for `topic_resolve_timeout` ──▶ Failed(UnknownTopic) (pending records → NotWritten)
```

The first `Metadata` for a freshly opened handle is by name (the only time
a name reaches the wire for routing); every subsequent one is by ID. A
topic handle never changes ID. Deleted and failed topics keep their slot until
the application closes the handle, so a stale handle is rejected with
`TopicClosed`, never reused.

## 19. Engine interface

The engine is a passive `struct ProducerEngine` with no async, no I/O, and no
clock. The actor is the only caller.

```rust
impl ProducerEngine {
    pub fn new(config: ProducerConfig, identity: Option<ProducerIdentity>) -> Result<Self, ConfigError>;

    // Ingress — returns accepted prefix; reservations already made by the client side
    pub fn admit(&mut self, now: RuntimeInstant, batch: SubmissionBatch, route: &[PartitionChoice]) -> Admitted;

    // Time
    pub fn next_deadline(&self) -> Option<RuntimeInstant>;
    pub fn on_deadline(&mut self, now: RuntimeInstant, budget: WorkBudget) -> Progress;

    // Work (all bounded by `budget`)
    pub fn encode(&mut self, now: RuntimeInstant, budget: WorkBudget) -> Progress;       // inline compression quanta
    pub fn schedule(&mut self, now: RuntimeInstant) -> Vec<DispatchOrder>;              // requests to build and send

    // Results from the world
    pub fn on_write(&mut self, key: ConnectionKey, result: WriteOutcome);
    pub fn on_frame(&mut self, key: ConnectionKey, frame: &[u8]) -> Result<(), ProtocolViolation>;
    pub fn on_connection(&mut self, key: ConnectionKey, event: ConnectionEvent);
    pub fn on_job(&mut self, job: JobKey, result: JobResult);                            // compression/DNS/SASL
    pub fn on_metadata(&mut self, now: RuntimeInstant, view: MetadataResponseView<'_>);

    // Egress
    pub fn drain_events(&mut self, out: &mut EventSink) -> usize;
    pub fn drain_orders(&mut self) -> Vec<Order>;   // Connect, Retire, SubmitJob, SendFrame(control), ArmSleep

    // Lifecycle
    pub fn flush(&mut self, now: RuntimeInstant) -> FlushToken;
    pub fn close(&mut self, now: RuntimeInstant, deadline: RuntimeInstant);
    pub fn is_quiescent(&self) -> bool;
}
```

`WorkBudget { bytes: u32, items: u32 }` bounds one call. `Progress { done,
remaining_immediate: bool }` tells the actor whether to yield and re-poll or
park. Every mutation goes through the engine so the simulation can drive it
directly (no actor) for model tests, exactly as quarry drives `InMemoryQueue`.

`SubmissionBatch` is what the mailbox carries: one owned `Vec<RecordDescriptor>`
plus the input storage they reference (a copy slab or a lease set) and the
credits already reserved for them. Routing (`PartitionChoice`) is computed by
the actor from the partitioner before `admit`, so the engine never calls a
plugin.

## 20. Actor poll

The actor is one `RuntimeHandle` task. Its future's `poll` is:

```text
1. drain completions:
     for each connection: poll retained read/write/close futures; feed on_write / on_frame / on_connection
     for each job slot:    poll retained job future;                feed on_job
   (bounded: at most `max_completions_per_poll`)
2. now = handle.now(); engine.on_deadline(now, budget)
3. mailbox: check → register waker → recheck; drain up to `max_submissions_per_poll` into engine.admit
4. engine.encode(now, encode_budget)
5. for order in engine.schedule(now): build plan, place into connection slot, create cold write future
6. for order in engine.drain_orders(): connect / retire / submit job / arm control frame
7. engine.drain_events(&mut event_ring); if ring transitioned empty→nonempty, wake application side
8. poll every newly created cold future once (admission)
9. if any Progress.remaining_immediate: yield_now and return Poll::Pending with self-wake
   else: arm one Sleep at engine.next_deadline(); return Poll::Pending
```

Step 1 precedes 3 because acknowledgements release credits that step 3 wants.
Step 8 is last so every future created in this poll is admitted in a known
order (first-poll order defines write ordering on the cold side).

Lost-wake protocol for the mailbox: the mailbox stores an `AtomicWaker`-style
slot; the actor checks `len`, registers its waker, and checks again. A
submitter pushes, then wakes. In simulation the submitter is an owner-thread
task, so the wake is ordinary; on the host it is a foreign wake entering
`HostRuntime` ingress. The event ring uses the mirror-image protocol toward
the application.

Compression budget on the host: `encode_budget.bytes` is derived at startup by
timing zstd over a fixed 1 MiB sample and choosing the byte count that fits
`target_poll_ms` (default 2 ms). Simulation uses a fixed
`sim_encode_bytes_per_poll` and models cost as a `Schedule`-stream delayed job
completion, so virtual deadlines are not starved by an always-ready encoder.

## 21. Partition accumulator and sealing

```text
on admit(record, partition p, now):
    acc = accumulators[p]
    if acc.open.is_none():
        acc.open = Batch::deferred(first_accepted_at = now)
        deadline_heap.push(p, now + linger_max)      // never moved later
    b = acc.open
    if b.raw_bytes + encoded_len(record) > batch_hard_bytes: seal(b, HardLimit); goto open new batch
    b.append(record)                                  // descriptor only if Deferred; encode+feed if Progressive
    b.oldest_deadline = min(b.oldest_deadline, record.delivery_deadline)
    if b.raw_bytes >= batch_target_bytes: seal(b, Target)
    if b.is_deferred() and contexts_available() and b.raw_bytes >= progressive_threshold: activate(b)

on deadline(p, now):
    if acc.open and acc.open.first_accepted_at + linger_max <= now: seal(open, Linger)
    // t_seal also honours oldest_deadline - T_remaining_estimate, where
    // T_remaining_estimate = seal_cost_estimate(raw_bytes) + request_rtt_estimate; both are EWMAs

seal(b, reason):
    if Deferred: activate(b)     // acquires context + output envelope; if unavailable → b.state = Sealing(WaitingContext), retained
    drive ZSTD_e_end to completion within encode budget (may span polls)
    release context; trim output envelope to retained chunk capacity
    b.state = Sealed; record reason in metrics
```

Sealing on `flush` and on `close` uses reason `Flush`. When broker credits are
unavailable for `p`'s connection, `Target` sealing still happens (an oversized
batch is worse than an idle one), but `Linger` sealing is deferred until credit
returns, up to `oldest_deadline - T_remaining` — this is the "let records
accumulate naturally" rule from section 3 made precise.

Linger skip: if the partition's arrival-rate EWMA is below
`linger_skip_below_rate`, the first record seals immediately once a dispatch
credit exists. The EWMA is engine state updated on admit; it is not a timer.

## 22. Dispatch scheduler

```text
schedule(now):
    for conn in connections.active() with request_slots > 0 and wire_window > 0:
        candidates = partitions routed to conn with a Ready head and ledger not blocked
        order candidates by DRR deficit (quantum = request_target_bytes / active_lanes), then by oldest_deadline
        req = Request::new(conn)
        for p in candidates:
            b = ledger[p].ready_head()
            if req.wire_bytes + b.wire_len() > request_hard_bytes and req.partitions > 0: break
            if req.partitions == request_max_partitions: break
            req.add(p, b); ledger[p].mark_in_flight(b, req)
            charge deficit[p] by b.wire_len()
            if req.wire_bytes >= request_target_bytes: break
        if req.partitions > 0:
            assign sequences to any batch in req that lacks one; finalize headers + CRC
            reserve request_slot + wire_window(req.wire_bytes)  // wire_window may go negative by at most one request
            emit DispatchOrder(req)
```

Three properties this guarantees, each asserted by a test:

- One `Ready` head per partition per request (Produce v3+ rule). A partition
  may have up to five batches in flight across successive requests on one
  connection with increasing sequences; ordering holds because the
  connection is FIFO and a retry always re-enters at the ledger head.
- A single oversized `Ready` batch (up to `batch_hard_bytes`) always fits an
  empty request, so nothing is permanently undispatchable.
- Sequence assignment happens after credits are secured, so a rejected
  dispatch leaves no sequence hole.

Throttling: a Produce response's `throttle_time_ms` marks the connection
`throttled_until = now + throttle` and the scheduler skips it; it does not
change the window. Kafka brokers already delay the response by that amount,
so this is client-side politeness, not a second controller.

## 23. Response handling

Per-partition error code → action. `retriable` means the batch returns to
`Ready` with the same identity and sequence; `refresh` means a metadata
refresh by topic ID is ordered and the partition's unsent batches are
re-routed after it. When the response is v10+ and carries KIP-951
`CurrentLeader`/`NodeEndpoints`, the engine applies that leader and epoch
directly (bumping the topic generation) and re-routes without waiting for
the refresh; the refresh still runs to reconcile the rest of the cache.

| Error | Action | Outcome if retries/deadline exhausted |
|---|---|---|
| `NONE` | `Acked { base_offset, log_append_time }` | — |
| `DUPLICATE_SEQUENCE_NUMBER` | `Acked { base_offset: None }` | — |
| `NOT_LEADER_OR_FOLLOWER`, `LEADER_NOT_AVAILABLE`, `FENCED_LEADER_EPOCH`, `UNKNOWN_LEADER_EPOCH`, `UNKNOWN_TOPIC_OR_PARTITION` | retriable + refresh | `Unknown` (a broker answered, so the batch was transmitted) |
| `UNKNOWN_TOPIC_ID` | retriable + refresh by ID; if the refresh also returns `UNKNOWN_TOPIC_ID`, topic → `Deleted` | `NotWritten { TopicDeleted }` after confirmed deletion, else `Unknown` |
| `INCONSISTENT_TOPIC_ID` | topic fatal: `Deleted`-equivalent, refresh fails closed | `NotWritten` for unsent, `Unknown` for in-flight |
| `REQUEST_TIMED_OUT`, `KAFKA_STORAGE_ERROR`, `NOT_ENOUGH_REPLICAS` | retriable | `Unknown` |
| `NOT_ENOUGH_REPLICAS_AFTER_APPEND` | retriable (leader wrote it; retry is safe by idempotence) | `Unknown` |
| `OUT_OF_ORDER_SEQUENCE_NUMBER`, `UNKNOWN_PRODUCER_ID` | ledger rule 18.4 | `NotWritten` for this batch |
| `MESSAGE_TOO_LARGE`, `RECORD_LIST_TOO_LARGE`, `INVALID_RECORD`, `CORRUPT_MESSAGE`, `UNSUPPORTED_COMPRESSION_TYPE`, `UNSUPPORTED_FOR_MESSAGE_FORMAT`, `INVALID_REQUIRED_ACKS`, `TOPIC_AUTHORIZATION_FAILED` | `Failed(NotWritten)` | — |
| `INVALID_PRODUCER_EPOCH`, `PRODUCER_FENCED`, `CLUSTER_AUTHORIZATION_FAILED`, `INVALID_PRODUCER_ID_MAPPING` | producer fatal: every partition `FailedClosed`; unsent → `NotWritten`, in-flight → `Unknown` | — |
| unknown code | treat as fatal; never as success | — |

The table is data (`kr_kafka_protocol::errors::classify(code) ->
ErrorClass`) with a test that every code the pinned schema defines is
classified, so a schema bump cannot introduce an unclassified code.

## 24. Delivery outcome classification

```text
outcome(batch) =
    Acked            if any attempt received NONE or DUPLICATE_SEQUENCE for this partition
    NotWritten       if terminal via a definitive-not-written broker error
                     or (deadline/cancel/fail-closed) and !transmitted
    Unknown          otherwise
```

Corollaries the oracle checks:

- `Acked` requires a parsed successful partition response carrying this
  batch's base sequence. A successful socket write never produces `Acked`.
- `NotWritten` with `transmitted == true` is only possible through a broker
  rejection, never through a timeout.
- A batch retried after an ambiguous attempt resolves to `Acked` (either
  directly or via `DUPLICATE_SEQUENCE`) or to a ledger decision; it never
  resolves to `NotWritten` by timeout while a later attempt is in flight.

`DeliveryEvent { token, outcome, partition, base_offset: Option<i64>,
timestamp: Option<i64>, attempts: u8 }` is fixed-size and `repr(C)`
so the FFI can expose it directly.

## 25. Wire plan and codec

### 25.1 Send plan

```rust
pub struct SendPlan { segments: SmallVec<[Segment; 16]>, total_len: u32 }
pub enum Segment {
    Arena  { range: Range<u32> },                   // request-local metadata arena, one Vec<u8> per request
    Shared { chunk: SharedBytes, range: Range<u32> } // immutable compressed chunk or finalized batch header
}
pub struct Cursor { segment: u16, offset: u32 }
```

`SharedBytes` is `Arc<[u8]>`-backed (thread-safe because provider threads may
hold it). The ledger keeps its own `SharedBytes` clone per batch, so a
provider retaining a segment past a retired connection cannot block a retry.

The planner emits at most `request_max_partitions * 3 + 4` segments and
coalesces any run of `Arena` segments; runs of small `Shared` segments below
`coalesce_below_bytes` (default 512) are copied into the arena instead. The
finalized batch header (61 bytes: base offset through base sequence) is part
of the batch's shared allocation, written in place at finalization, so a
batch is exactly one `Shared` segment per chunk.

### 25.2 Staging path

Over today's `WriteRequest { buffer: Vec<u8> }`, the connection driver walks
the plan from `cursor`, packs up to `staging_bytes_per_connection` into a
pooled staging `Vec`, submits it, and on `WriteResult { bytes_written }`
advances the cursor by `bytes_written` — never by the staged length. A partial
write leaves the unwritten staged suffix in the cursor, not in the buffer; the
next pack starts from the cursor. Every staged byte is a counted copy in the
`copies` metric.

### 25.3 Vectored extension to `kr-runtime-io`

```rust
pub struct WriteSegment { pub bytes: SharedBytes, pub range: Range<u32> }
pub struct VectoredWriteRequest { pub segments: Vec<WriteSegment> }     // validated: non-empty, in-range, sum <= max_operation_bytes
pub struct VectoredWriteResult { pub segments: Vec<WriteSegment>, pub bytes_written: usize }

pub trait ByteStreamVectoredSubmit: ByteStreamSubmit {
    type WriteVectoredResponse: Future<Output = CompletionResult<VectoredWriteResult, VectoredWriteFailure>> + 'static;
    fn max_segments(&self) -> usize;
    fn submit_write_vectored(&self, request: VectoredWriteRequest) -> Self::WriteVectoredResponse;
}
```

`VectoredWriteFailure` mirrors `NetworkFailure`: error, returned segments,
`bytes_transferred`, and certainty. `ColdStream` gains `write_vectored`.
Contract points the conformance suite (`check_vectored_stream_provider` and
its cold twin) must cover:

1. Rejection (`InvalidRequest`, `ResourceExhausted`, segment cap) is
   `NotApplied` and returns every segment.
2. Partial write across a segment boundary reports exact `bytes_written`; the
   provider never reorders or skips segments.
3. Segments are retained through admission and returned on terminal
   completion even when the response future was dropped (observed via
   `SharedBytes::strong_count` in test support).
4. Read reserves are unaffected by a blocked vectored write.
5. `MemoryNetwork` and `SimNetwork` implement it by concatenation into the
   directional pipe under the same fault plan; `UringNetPool` uses `writev`
   with `IoSlice`s built from the retained segments inside the ring thread;
   the readiness provider uses `sendmsg`.

### 25.4 Protocol compiler

Input: the pinned schema directory (`kafka/kr-kafka-protocol/schemas/<git-sha>/`)
with a checksum manifest. Pipeline stages and their outputs:

```text
parse      JSON → Schema { name, api_key, valid_versions, flexible_versions, fields[] }
validate   unknown field types / tag ranges / version syntax → hard error
classify   for each version v: Layout(v) = list of (field, encoding) after applying versions/nullable/flexible
           group versions with identical Layout into LayoutClass
emit       one `plan_<Api>_<class>()` writer and one `<Api>ResponseView<'a>` reader per class
           a `dispatch(version) -> class` table per API
           `SUPPORTED: &[(ApiKey, RangeInclusive<i16>)]` listing only tested versions
tests      one round-trip and one fixture test per (API, version) in SUPPORTED
```

Produce is the worked example of `classify`: request layouts split into
`{9,10,11,12}` (topic name) and `{13}` (topic ID); response layouts split
into `{9}`, `{10,11,12}` (adds KIP-951 `CurrentLeader` and `NodeEndpoints`),
and `{13}` (topic ID). The compiler retains both request classes for compatibility tests, while the
v0 producer dispatches only the v13 ID class. The general codec representation
can expose `TopicWire` (`Name(&str)` or `Id(TopicId)`); the strict producer
accepts only the ID alternative. The response view exposes a
`topic_id: Option<TopicId>` accessor that the `{13}` class fills directly and
the older classes fill by name lookup in the producer, so the ledger only
ever matches responses by ID.

Readers are borrowed views with checked cursors: every length is bounds-
checked against the frame, every array count is capped
(`max_array_elements`), and every tag is either handled or skipped by its
declared size. A view never allocates; the metadata cache copies what it
keeps.

Encoding rules that get dedicated tests (Part I §7): classic `ClientId` in
flexible headers, `ApiVersions` v0 response header, compact nullable
(`len+1`, `0` = null) vs record varint (zigzag), empty vs null strings and
bytes, tagged-field ordering by tag number.

## 26. Record batch encoder and compression

```text
batch allocation (SharedBytes-backed, grown by chunk):
  [ header 61 bytes, filled at finalization ][ compressed records ... ]

encode(record) → varint(length) varint... : uses a 64-byte scratch for the prefix,
                 then feeds key/value/headers spans directly to the zstd stream
```

Record `length` needs `attributes(1) + varint(timestamp_delta) +
varint(offset_delta) + varint(key_len) + key + varint(value_len) + value +
varint(header_count) + headers`; all computable from descriptor lengths, so no
second scan of payload bytes.

Compression context pool: `codec_contexts` live `ZSTD_CCtx`s with pinned
parameters (`level`, `windowLog` capped so workspace is fixed, no
multithreading, `ZSTD_c_contentSizeFlag = 0`). A batch holds a context from
activation to seal only. Deferred batches keep descriptors and input leases;
their input bytes stay charged. `progressive_threshold` (default 16 KiB raw)
decides when a deferred batch is worth a context; below it, compression runs
at seal.

Output envelope reserved at activation:
`min(batch_hard_bytes, zstd_compress_bound(batch_hard_bytes)) + 61`. If the
stream exceeds it (incompressible data plus streaming overhead), the batch
fails `NotWritten { reason: CompressedTooLarge }` before any sequence is
assigned. The reservation, not `compress_bound`, is the memory guarantee.

Finalization writes `base_offset = 0`, `batch_length`, `partition_leader_epoch
= -1`, `magic = 2`, `attributes` (codec bits, no timestamp type override),
`last_offset_delta`, `base/max timestamp`, `producer_id`, `epoch`,
`base_sequence`, `record_count`, then CRC32C over bytes `[attributes ..
end]`. It is idempotent for untransmitted batches (epoch bump path).

## 27. Mailbox and events

```rust
pub struct ProducerClient { inner: Arc<Shared> }          // Send + Sync; what the FFI wraps
impl ProducerClient {
    pub fn open_topic(&self, name: &str) -> Result<TopicHandle, Backpressure>;      // resolves by name once, then ID-only
    pub fn close_topic(&self, topic: TopicHandle);
    pub fn acquire(&self, bytes: u32) -> Result<InputBuffer, Backpressure>;        // native-buffer path
    pub fn submit_copy(&self, records: &[RecordDescriptor<'_>]) -> Submitted;      // accepted prefix count
    pub fn submit_leased(&self, lease: LeaseId, records: &[RecordDescriptor<'static>]) -> Submitted;
    pub fn poll_events(&self, out: &mut [Event]) -> usize;
    pub fn flush(&self) -> Result<FlushToken, Closed>;
    pub fn close(&self, deadline: RuntimeDuration) -> Result<(), Closed>;
}
```

`Shared` holds the bounded mailbox (`Mutex<VecDeque<SubmissionBatch>>` +
waker slot), the event ring (`Mutex<VecDeque<Event>>` + application waker or
FFI callback-free notify), and the atomic credit counters for the admission
pools so `submit_*` can reserve without waking the actor. Only reservation
is done on the caller thread; every state change is on the actor.

`Event` is `#[repr(C)]`:

```rust
pub enum Event { Delivery(DeliveryEvent), InputReleased { lease: u64 }, FlushDone { token: u64 },
                 TopicReady { topic: u32, id: [u8; 16], partitions: i32 }, TopicFailed { topic: u32, code: u32 },
                 Closed { unresolved: u32 }, Fatal { code: u32 } }
```

Control events (`FlushDone`, `TopicReady`, `TopicFailed`, `Closed`, `Fatal`)
draw from a fixed reserve of `16 + max_open_topics` slots so they can never
be starved by delivery events. `TopicReady` is informational: submissions to
a handle are accepted as soon as `open_topic` returns, subject to the pending
queue bound in 18.1.

## 28. C ABI sketch

```c
typedef struct kr_producer kr_producer;                     // opaque
typedef struct { uint32_t struct_size; /* ProducerConfig mirror, fixed-width */ } kr_producer_config;
typedef struct { uint32_t struct_size; uint32_t topic; int32_t partition_hint;
                 const uint8_t *key; uint32_t key_len; uint8_t key_is_null;
                 const uint8_t *value; uint32_t value_len; uint8_t value_is_null;
                 int64_t timestamp_ms; uint64_t user_token; } kr_record;
typedef struct { uint32_t struct_size; uint8_t kind; /* union by kind */ } kr_event;

int32_t  kr_producer_create(const kr_producer_config*, kr_producer**);
int32_t  kr_topic_open(kr_producer*, const char*, uint32_t len, uint32_t *topic_out);   // handle usable immediately; KR_EVENT_TOPIC_READY reports id + partitions
int32_t  kr_topic_id(kr_producer*, uint32_t topic, uint8_t id_out[16]);                 // KR_ERR_NOT_READY until resolved
int32_t  kr_topic_close(kr_producer*, uint32_t topic);
int32_t  kr_buffer_acquire(kr_producer*, uint32_t bytes, uint8_t **ptr, uint64_t *lease);
int32_t  kr_buffer_commit(kr_producer*, uint64_t lease, uint32_t used);
int32_t  kr_buffer_release(kr_producer*, uint64_t lease);
int32_t  kr_lease_register(kr_producer*, const uint8_t*, uint64_t len, uint64_t *lease);
uint32_t kr_submitv_copy(kr_producer*, const kr_record*, uint32_t n);          // accepted prefix
uint32_t kr_submitv_leased(kr_producer*, uint64_t lease, const kr_record*, uint32_t n);
uint32_t kr_poll_events(kr_producer*, kr_event*, uint32_t cap);
int32_t  kr_flush(kr_producer*, uint64_t *token);
int32_t  kr_close(kr_producer*, uint64_t deadline_ms);
void     kr_destroy(kr_producer*);
```

Rules: every entry point is `catch_unwind`-guarded and returns
`KR_ERR_FAILED` after moving the producer to a latched failed state; `struct_size`
mismatches are rejected; `kr_destroy` blocks until provider-owned memory is
released and documents that bound as best-effort after `kr_close`'s deadline.
`kr_lease_register` documents the pinning contract per language (Go:
`runtime.Pinner`; JVM: direct `ByteBuffer` only; Python: buffer protocol with
`PyBUF_SIMPLE` and the exporter kept alive by the binding).

## 29. Host wiring

```text
kr-kafka-producer-host::producer::HostProducer::start(config):
    HostRuntime::new(HostConfig { max_tasks: 64 + 4 * max_connections, max_timers: same, .. })
    transport = match config.transport {
        Uring     => UringNetPool::new(pool_config_from(config))?,           // error if unavailable
        Readiness => ReadinessNet::new(readiness_config_from(config))?,
        Auto      => try Uring, else Readiness                                // only once readiness is conformant
    }
    blocking = runtime.blocking()?                                            // DNS, SCRAM proofs
    actor = ProducerActor::new(handle, engine, transport, blocking_adapter)
    runtime.block_on(actor)  ...  runtime.finish()
```

`pool_config_from` sets `max_streams = brokers_max * lanes + 2`,
`max_operation_bytes = max(staging_bytes, rx_bytes)`, `ring_entries` to twice
`max_streams`, and rejects any producer byte limit the pool cannot carry.

TLS: a `TlsStream<S>` adapter over `ColdStream<S>` using rustls unbuffered
mode with two fixed buffers (`tls_plaintext_bytes`, `tls_ciphertext_bytes`),
exposing the same read/write shape to the connection driver. Its handshake
runs inline (rustls handshakes are sub-millisecond); SCRAM PBKDF2 runs as a
blocking job with a `control_reserve` credit.

## 30. Simulation harness

Inputs, all derived from one seed and recorded in the replay manifest:

| Stream | Draws |
|---|---|
| `Scenario` | broker count, topic IDs, partition count per topic, initial leaders, lane count, budgets |
| `Workload` | record sizes, keys, submission batch sizes, submission timing, flush/close points |
| `Schedule` | broker service delay, compression job delay, `SimNetwork` completion jitter |
| `Fault` | partial writes, disconnects (before/after commit), dropped responses, leader moves, throttles, OOOS injection, topic delete, topic delete-and-recreate under the same name, partition expansion, application stops polling |
| `Debug` | trace ids only |

The broker model (`kr-kafka-broker-model`) is a passive engine driven by a
per-connection actor over `ColdStream<SimStream>`: it parses real frames,
keeps `log[partition] = Vec<(pid, epoch, base_seq, count)>`, enforces the
five-batch producer-state window, emits real responses, and applies the fault
plan at four points: after parse (drop request), before commit (reject),
after commit (drop response), and on leader move. It serves `Metadata` v12
by ID and by name, assigns a fresh ID on recreate, answers `UNKNOWN_TOPIC_ID`
for deleted IDs, and negotiates Produce v13 for successful producer scenarios.
A broker advertising only Produce v9 exercises startup rejection: name-based
Produce fallback is deferred, preserving strict topic-ID identity.

The oracle is a separate structure fed by (a) every `submit_*` return value,
(b) every drained event, and (c) the broker's committed log. It asserts:

```text
C1  ∀ accepted record: exactly one Delivery event; none for a rejected record
C2  ∀ accepted lease: exactly one InputReleased, not before its last record was encoded or failed
C3  Delivery(Acked, r) ⇒ r appears exactly once in the committed log, at that partition
C4  Delivery(NotWritten, r) ⇒ r does not appear in the committed log
C5  Delivery(Unknown, r) ⇒ (no constraint on the log)  — and the count of Unknown is reported
C6  ∀ partition: committed records from one producer are in submission order with no duplicates
C7  ∀ pool: credits_reserved − credits_released == credits_held_by_live_objects, at every quiescent point and at teardown == 0
C8  no SharedBytes referenced by a live provider operation was reused
C9  Flush(token) ⇒ every record accepted before flush() reached Delivery before FlushDone
C10 Closed ⇒ every accepted record reached Delivery and every lease reached InputReleased
C11 Delivery(Acked, r) ⇒ r is in the log of the topic *ID* r was admitted under, never in a same-name successor
C12 topic recreated ⇒ every record admitted under the old ID is Acked (in the old log), NotWritten { TopicDeleted }, or Unknown
```

Meta-tests mutate the broker log (drop, duplicate, reorder one record) and
the event stream (drop one event, flip one outcome) and assert the oracle
rejects each mutation.

Coverage gates per seed: at least one partial write, one retry, one
`DUPLICATE_SEQUENCE`, one leader move, one topic recreate, one Produce-v9-only
capability rejection and one successful v13 scenario, one linger seal, one target seal, and one
backpressure rejection across the sweep; per-seed, at least one `Acked`
and a non-empty fault realization unless the scenario is the no-fault
baseline. A campaign where any aggregate gate is zero fails.

Teardown: after the root completes, drive to `Idle`/`Stalled`/`Stopped`,
call `SimRuntime::finish()`, then compare the `DeterminismCheckpoint` of a
rerun. `Stalled` with any credit held is a liveness failure.

## 31. Open risks

- **Broker capability floor.** Produce v13 is required even when an older
  broker supports ID-keyed Metadata v12. This intentionally excludes brokers
  whose Produce requests are name-based. Revisit fallback only with an explicit
  contract for delete/recreate races; generated codec availability does not
  authorize a weaker producer identity mode.
- **Streaming zstd overhead on tiny batches.** If the per-frame cost
  dominates below a few KiB, sparse partitions should seal as `none` and
  let the broker's `compression.type` decide. Measure before adding the
  rule.
- **Readiness parity.** The `epoll` provider must pass the same conformance
  and simulation suites before `Auto` can promise fallback; until then
  `Auto` is documented as `Uring`-or-fail.
- **Epoch-bump wait.** A single slow partition holding an in-flight batch
  delays the producer-wide bump; the alternative (per-partition producer
  IDs) multiplies broker state. v0 accepts the wait and exposes it as a
  metric.
- **Foreign-lease pinning.** Each binding's pinning story must be reviewed
  individually; the FFI crate ships with only the copy and native-buffer
  paths enabled until a binding's lease implementation has a test that
  moves or frees the source early and observes the rejection.
