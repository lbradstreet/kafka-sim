# Java producer client design

A Java implementation of `org.apache.kafka.clients.producer.Producer<K, V>`
backed by `kr-kafka-ffi` through the Java Foreign Function & Memory API
(FFM). Rust owns routing, batching, retries, network progress and delivery
certainty; Java owns serialization, admission waits and callback dispatch.

The copy-path implementation is in [kr-kafka-java](kr-kafka-java/README.md).
This document defines its supported contract and the acceptance checks for
changes to it. Native-buffer and foreign-input paths remain deferred (§6).
The baseline is JDK 25 and `org.apache.kafka:kafka-clients:4.3.0`, matching
[the repository's Java fixtures](../scripts/kafka-java-fixtures.py). Pin the
Gradle wrapper, dependency checksums and generator toolchain. Compile the
complete `Producer` interface against that artifact; upgrades require a
compatibility review rather than an open dependency version range.

The [C header](kr-kafka-ffi/include/kr_kafka.h),
[FFI ownership contract](kr-kafka-ffi/README.md) and
[producer design](../producer_design.md) are the native source of truth.
ABI v2 supplies the bounded metadata snapshots, topic retirement and owner
status that ABI v1 lacked. Native ordered terminal publication supplies the
per-partition callback guarantee. These remain release gates for future changes;
foreign leases remain a separate extension.

## 1. Scope and compatibility

The intended first release supports both `send` overloads, `flush`, both
`close` overloads, serializers and interceptors, idempotent `acks=all`
delivery, none/zstd compression, TLS, and SASL PLAIN/SCRAM over TLS. Native
startup targets Linux x86_64 and aarch64. Explicit readiness is supported;
io_uring must be available when selected. Do not promise automatic transport
fallback before the native conformance gate permits it.

| Surface | Contract |
|---|---|
| `partitionsFor` | Returns current, complete metadata from bounded immutable snapshots (§8). |
| `partitioner.class` | Receives a real Kafka `Cluster` snapshot (§8); explicit partitions take precedence. |
| `metrics()` | Initially an immutable empty map, explicitly documented. Native HDR metrics already exist; export is deferred (§12). |
| Transactional methods | Throw `UnsupportedOperationException`; reject `transactional.id` at construction. Includes `sendOffsetsToTransaction(Map, ConsumerGroupMetadata)`. |
| `clientInstanceId(Duration)` | Throw `UnsupportedOperationException`; no broker telemetry registration. |
| `registerMetricForSubscription` / `unregisterMetricFromSubscription` | Explicit no-ops while broker telemetry is unsupported; do not imply that metrics are exported. |
| Delivery certainty | Preserve native `UNKNOWN` as a distinct exception, including during teardown. |

This implements a supported subset of the Kafka producer API. Document the
native retry cap, batching/routing differences, empty metrics and close-time
limitations beside the constructors. Do not describe it as an unrestricted
replacement for `KafkaProducer`. Unsupported known configurations fail early,
including non-idempotent delivery, other acknowledgements/codecs, transactions,
mutual TLS, SASL_PLAINTEXT, GSSAPI and OAuth until implemented. The
[Kafka 4.3 API reference](https://kafka.apache.org/43/javadoc/org/apache/kafka/clients/producer/Producer.html)
describes the interface; the exact method inventory is verified against the
pinned 4.3.0 jar. Neither establishes native support for all of its features.

## 2. FFM and memory safety

Use `java.lang.foreign`, final since JDK 22
([JEP 454](https://openjdk.org/jeps/454)). JDK 21 preview FFM is unsupported.
Deploy with `--enable-native-access=io.krkafka` for the binding module, or
`--enable-native-access=ALL-UNNAMED` on the classpath. An executable jar may
use `Enable-Native-Access: ALL-UNNAMED`; test both deployment modes. See
[JEP 472](https://openjdk.org/jeps/472) for native-access restrictions.

| Storage | Lifetime and access |
|---|---|
| Constructor config and temporary outputs | Confined arena on the constructing thread; config spans are copied before create returns. |
| Pooled submit scratch | Shared-arena slabs, exclusively checked out to one caller at a time; total bytes and checkouts are bounded. |
| Poller's event array | Confined arena created and closed by the poller. |
| Producer pointer | Opaque native-owned address, guarded by Java lifecycle state; closing an arena does not destroy a producer. |
| Native acquired buffer view | Bounded view of native-owned memory; Java must not attach a deallocator. Invalid after release/commit as specified in §6. |
| Foreign allocation | Only an explicit retention/immutability protocol may extend its lifetime (§6.3). |
| Loaded library | Process-lifetime retention in the supported loader arrangement. |

A confined arena cannot migrate between caller threads. A shared arena can
be used across threads, but retaining its Java object does not prevent another
caller from closing it. Likewise, a read-only segment does not invalidate
writable aliases. FFM checks Java segment access; it does not protect Rust
loads through a retained raw pointer. These are ownership obligations, not
properties that the GC or FFM supplies automatically. See the
[JDK Arena contract](https://docs.oracle.com/en/java/javase/25/docs/api/java.base/java/lang/foreign/Arena.html).

The nested pointers in `kr_record` and `kr_header` require native storage.
A heap `byte[]` cannot be used as the address stored in those structs. No
current downcall uses `Linker.Option.critical`: submit paths can lock,
allocate and copy variable amounts of data. A benchmark win does not prove
eligibility; a future critical entry point must be extremely short in every
case and satisfy the [JDK critical-call restrictions](https://docs.oracle.com/en/java/javase/25/docs/api/java.base/java/lang/foreign/Linker.Option.html#critical(boolean)).
Virtual threads may call the binding, but the poller is a platform thread and
blocking native calls must not be advertised as carrier-free.

## 3. Module, generation and library loading

```text
kafka/kr-kafka-java/                     Gradle project, Java 25, module io.krkafka
  src/main/java/io/krkafka/ffi/          generated jextract bindings, not exported
  src/main/java/io/krkafka/producer/     public producer API and support
  src/main/java/io/krkafka/loader/       library loading and ABI handshake
  build/generated/nativeResources/META-INF/native/{linux-x86_64,linux-aarch64}/libkr_kafka_ffi.so
  src/test/java/...                     ABI, lifetime, concurrency, integration
scripts/kafka-java-bindings.sh           reproducible generation and --check
```

`native` is a Java keyword, so it is not a package component. Keep generated
FFI classes internal; callers must not bypass the lifecycle lock.

Generate from `kr-kafka-ffi/include/kr_kafka.h`. Pin a JDK-25-compatible
jextract build, its libclang/toolchain, target triple and complete command in
a manifest. Use the pinned tool's include dump to produce an explicit symbol
argument file covering all required typedefs, structs, functions and constants.
Do not assume that `--include-function 'kr_*'` expands wildcards, or copy CLI
options from another jextract revision. The documented tool workflow is in
the [jextract guide](https://raw.githubusercontent.com/openjdk/jextract/master/doc/GUIDE.md).

Check generated sources in. `--check` regenerates in a temporary directory
and compares the entire output, in addition to checking input hashes. Java
layout tests compare sizes, alignments and every field offset with
`kr-kafka-ffi/tests/c_header.rs` on both architectures. Initialize every
versioned input and every event output slot's `struct_size`; config init's
size check alone cannot validate the other layouts.

Use one loading strategy consistently:

1. Resolve `-Dkr.kafka.library=<absolute path>`, otherwise extract the matching
   bundled artifact to a private directory. Verify its packaged checksum;
   reject unsupported OS/architecture/libc combinations with an actionable
   error. Pin the supported glibc baseline and native library dependencies.
2. `System.load(canonicalPath)` from the binding's class loader. Generated
   bindings using `SymbolLookup.loaderLookup()` must see that load; a separate
   `libraryLookup` is not automatically used by generated calls.
3. A small bootstrap downcall resolves only `kr_abi_version`. Require equality
   with the expected ABI before initializing generated function holders.
   Report absent symbols and ABI mismatches as construction failures.
4. Initialize generated layouts/downcalls only after the handshake. Verify
   with a deliberately mismatched test library that no constructor is called.

Static initialization runs once per defining class loader, not once per JVM.
Initially support one binding class loader per process and fail clearly on
conflicting native loads. The JVM detects another loader's reuse of the same
canonical library file; separately extracted copies remain unsupported and
unverified across loaders. Avoid unloading the library while any producer or
native thread can use it. Verify packaging on both classpath and module path;
compile against the Kafka jar without relocating its public API classes.

## 4. Threads and lifecycle synchronization

```text
caller threads -> serialize -> bounded admission -> kr_submitv_copy
                                                        |
                                    Rust owner thread runs I/O and retries
                                                        |
poller platform thread <- kr_poll_events <---------------+
        |
        +-> interceptor -> callback -> terminal Future -> release Java slot
```

`send` can wait for metadata and memory up to its remaining `max.block.ms`
budget. It does not perform broker I/O itself. User serialization and
partitioner code run on callers, outside binding locks.

For ABI v2, use one short `ReentrantLock` (`callGate`) for **every call using
the producer handle**, including event drains, plus immediate diagnostics and
publication of newly returned topic/flush/lease identifiers. Guard lifecycle
state and pending-table transitions with the same lock. This baseline
serializes downcalls; multiple callers can still serialize records concurrently.
Do not add a second lifecycle lock with a competing lock order.

This is required because the FFI's `guarded` helper resets producer-global
`last_error` even on polling. A lock taken only by failed submitters cannot
make `submit` plus `kr_last_error` atomic. Read the error immediately under
`callGate`, with no intervening downcall. An operation-local result ABI can
remove this diagnostic coupling later (§12).

Never run callbacks, serializers, plugin lifecycle hooks or condition waits
while holding `callGate`. Drain a bounded event array under the lock, then
dispatch outside it, taking the lock briefly for bookkeeping. Register
correlation state before releasing the lock, so an event cannot beat Java
publication even when native admission finishes before the downcall returns.

Lifecycle states are `OPEN`, `FAILED`, `CLOSING`, `DESTROYING`, `DESTROYED`.
Failure records its cause and fences admission; a failure during closing must
not move lifecycle state backwards. Only explicit teardown advances to
destruction. Recheck state on every admission attempt. Track active send
operations and scratch/native-buffer checkouts, including exception paths.
Close wakes waiters and quiesces writers before releasing acquisitions.
Only the designated teardown owner destroys the handle, exactly once; after
`DESTROYING`, no other ABI call is legal. All concurrent close callers share
one completion. Construction failure unwinds partially created native and
Java resources with the same ownership rules.

## 5. Pending records, futures and callback ordering

Use a fixed pending table bounded by `record_descriptors`. A token contains a
slot index and generation; validate both under `callGate`. Publish an entry
before admission and transition it atomically:

```text
FREE -> RESERVED -> ACCEPTED -> TERMINAL_DISPATCH -> FREE(next generation)
                  \-> rejected: caller releases reservation
```

Each entry holds a private completion object, callback, topic identity/name,
serialized key/value sizes and original CreateTime. It must not retain serialized
key/value payload after the copy downcall. Kafka 4.3's header-aware interceptor
acknowledgement requires a separate deep-copied header context when interceptors
are configured. Bound its total retained key/value bytes, retain it only through
terminal dispatch, and report that budget independently. Pending-slot and header
count limits also bound the context objects. Producers without interceptors do
not retain this context. Publish all fields through the lock; a volatile
generation alone is not a publication protocol. A duplicate,
unknown or stale token fences admission as a binding/native protocol failure.
Retire a slot before generation wrap rather than permitting an ABA collision.
Capture the returned future in a caller-local reference before publication;
never reread a slot after unlocking, since the poller may already have reused it.

Return a read-only `Future<RecordMetadata>` whose `cancel` returns `false`.
Keep mutable completion objects private. `get(timeout)` timing out or a waiting
thread being interrupted does not cancel native delivery or release its slot.
There is no FFI record-cancellation operation to implement cancellation safely.
Do not expose a completable future that lets callers manufacture success or
run arbitrary dependent actions inline on the poller. Kafka's implementation
also uses a [non-cancellable delivery future](https://raw.githubusercontent.com/apache/kafka/trunk/clients/src/main/java/org/apache/kafka/clients/producer/internals/FutureRecordMetadata.java).

For each accepted terminal result, run the header-aware `onAcknowledgement`,
then the user callback, then publish the future result and release the slot in
`finally`.
Isolate plugin/callback exceptions so one callback cannot stop event draining.
Callback code must not wait on its own or another pending delivery. Callback
`send` is allowed only when it can proceed immediately: it must fail locally
if it would need metadata, scratch or native capacity, because parking the
poller prevents progress. This stricter reentrant behavior is documented.

**Per-partition callback order is a release gate.** Native publication FIFO
does not imply send-order completion: a later queued record can expire before
an earlier in-flight record (`engine/deadlines.rs`, `engine/lifecycle.rs`).
The Java wrapper also does not learn an unhinted record's routed partition
until delivery. Therefore implement and test ordered terminal publication in
the native producer before claiming Kafka's callback-order guarantee. The
mechanism must be bounded by outstanding record obligations, cover success,
deadlines, retry failures and fatal shutdown, and preserve flush fences.
Concurrent sends are ordered by native admission within a partition; no
cross-partition or concurrent wall-clock invocation ordering is promised.
The native ordering tests and Java callback tests pin this guarantee together.

## 6. Submission and ownership paths

### 6.1 Copy path: the first implementation

```text
intercept -> header-aware serialization -> validate sizes/nulls
-> copy payload into scratch -> build kr_header[] and kr_record[1]
-> kr_submitv_copy -> read rejection code if needed -> return scratch
```

This performs heap-to-scratch and scratch-to-native copies, with one submit
per `send`. Keep batching in Rust; do not add a Java linger queue to disguise
FFI overhead. Future batch APIs must specify prefix acceptance and ordering
separately from Kafka's single-record API.

Scratch slabs use shared arenas with exclusive checkouts. Bound both checkout
count and total allocated bytes, with bounded oversized slabs up to the legal
record size including header/descriptor overhead. A count limit of 64 buffers
that can each grow to `max.request.size` is not a 4 MiB memory bound.
A slab is reusable immediately after the downcall, regardless of delivery.
Do not hold scratch across a metadata wait. Bound concurrent serialization and
retained serialized bytes as well; user serializers can allocate arbitrarily,
so report that exclusion instead of claiming a hard whole-JVM memory cap.

### 6.2 Native buffer path: measured follow-up

```text
serialize -> kr_buffer_acquire(capacity) -> pack all payload bytes
-> stop writes -> kr_buffer_commit(used)
-> kr_submitv_leased(lease, records, 1) -> kr_buffer_release(lease)
-> later INPUT_RELEASED, independently of DELIVERY
```

All key/value/header key/header value bytes must be inside the same committed
allocation. Only descriptors may remain in scratch. Use the copy path for an
entirely empty payload, or allocate at least one byte. Reinterpret the acquired
address only to the acquired length; attach no Java freeing action.

Track an acquisition before exposing its writable view to copying code. A
failed commit leaves it writable. A `finally` path stops access and releases
the handle after success, rejection, interruption or any Java exception.
Uncommitted release abandons the acquisition; committed release forbids further
submits and eventually produces `INPUT_RELEASED`. Install any release tracking
before that event can be drained. Do not free native-owned memory from Java.

Close must wait for active writers to stop and release acquired handles
**before waiting for `CLOSED`**; native closure itself waits for live inputs.
A concurrent map without this writer protocol is insufficient. This path
removes one copy but adds acquire/commit/release transitions; enable it only
after lifetime tests and matched workload measurements.

### 6.3 Foreign registration: deferred extension

Do not expose the proposed `sendNative(..., MemorySegment key, MemorySegment
value, ..., Arena owner)` API. The ABI registers one contiguous allocation,
requires every leased payload span to fall within it, and retains immutable
storage across native threads. Arbitrary segments may have unrelated owners;
an arena reference cannot prohibit closure, mutation or pool reuse.

Design this later around an ownership-controlled allocation or a
provider-specific retain/freeze protocol. Its contract must include:

- Full initialized allocation capacity, including unused suffix, charged once;
  legal capacity is 1 through `UINT32_MAX` bytes.
- All payload ranges inside that allocation, immutable and live from registration
  until `INPUT_RELEASED` or return from exclusive `kr_destroy`.
- A bounded pin installed before native ownership can escape; rejected
  registration retains nothing. After successful registration, always release
  the lease handle even if no record was admitted, and keep the pin until the
  actual release boundary.
- Separate completion of delivery and input release. Neither success nor an
  exception on the delivery future permits early reclamation.

An arbitrary Netty buffer, Arrow batch or mapping is not automatically safe.
Require concrete lifetime evidence for each adapter and keep this API absent
until its tests pass. No native upcall is needed for release notifications.

### 6.4 Record and plugin conversion

| Java value | Native representation |
|---|---|
| Serialized key/value is `null` | Corresponding `*_is_null = 1`, empty span; metadata size `-1`. |
| Serialized bytes are empty | Null flag `0`, empty span; metadata size `0`. An empty serialized key still uses keyed routing. |
| Explicit partition | Validate nonnegative, pass the hint unchanged; never silently reroute it. |
| No explicit partition | Hint `-1`; builtin native routing, unless the supported Java partitioner supplies a validated hint. |
| No timestamp | Capture epoch milliseconds on the caller; store the same CreateTime in pending metadata. |
| Headers | Preserve order, duplicates, null versus empty values; UTF-8 non-null keys and bounded count/bytes. |
| `lane_hint` | `-1` by default; only a validated native extension may override it. |
| `delivery_timeout_ns` | `0` for native default; configure that default at construction. |

Call serializers with `(topic, headers, data)`, including null data; nullness
comes from their result, not from the original object. Interceptors run before
serialization. Apply the pinned client's header mutability rules and copy the
final bytes before returning from a successful `send`, so subsequent caller
mutation cannot alter the native record. Check all unsigned conversions,
UTF-8 lengths, header totals and framed record limits before pointer creation.

Support serializer constructor arguments and configured serializer classes;
call `configure` with the correct key/value role for instances created from
configuration. Define and test close ownership for supplied instances against
the pinned Kafka client. Configure/close interceptors and partitioners, close
partially initialized plugins on construction failure, and avoid holding the
ABI lock while invoking user code.

## 7. Admission, backpressure and synchronous errors

For one-record submission, accepted count is 0 or 1. Under `callGate`, reserve
and publish the pending entry, submit, immediately capture `kr_last_error` on
0, and either mark it accepted or reclaim the reservation. Never resubmit an
accepted record to obtain a diagnostic. A rejected record has no native token
or future delivery obligation.

| Rejection | Binding behavior |
|---|---|
| `KR_ERR_EXHAUSTED` | Wait for capacity within the remaining admission budget, then retry. |
| `KR_ERR_NOT_READY` | Wait for topic/readiness progress within that same budget. |
| `KR_ERR_CLOSED` | Distinguish producer closure from a closed topic using Java state; stop retrying. |
| `KR_ERR_FAILED` | Fence admission, retain the cause, keep draining accepted results. |
| `KR_ERR_INVALID` | Use proven local validation for specific record errors; otherwise report native invalid-input failure without guessing. |
| Version/unsupported/unexpected code | Fail explicitly; never reinterpret as transient pressure. |

Use one monotonic `max.block.ms` budget for binding-controlled metadata and
capacity waits. Exclude time spent in serializers and partitioners. Include
scratch, pending-table and interceptor-context pressure; do not reset the timeout
on each retry. Keep retries bounded with a deadline even when another caller
consumes each wake.
After acceptance, the native delivery deadline governs terminal delivery; an
admission timeout before acceptance proves that this record was not written.

Condition waits use a predicate/progress sequence under the same lock; waiting
releases the lock. Signal on slot/scratch release, event progress and lifecycle
changes. Use bounded timed rechecks as well: the native actor can free mailbox
or input credits without producing a delivery event. Signalling only after a
nonempty event drain can otherwise strand a caller despite available capacity.
No unbounded Java retry queue is introduced.

Match the pinned client's error surfaces: serialization/argument/closed-state
misuse and interruption are synchronous exceptions where Kafka specifies them;
admission timeout and ordinary API record rejection return a failed future and
invoke the callback on the caller. Notify interceptors on pre-admission failures
as appropriate, without inventing a later native callback. Accepted records
complete only through the terminal dispatch path. Pin these cases in tests
against [KafkaProducer's behavior](https://kafka.apache.org/43/javadoc/org/apache/kafka/clients/producer/KafkaProducer.html),
including `BufferExhaustedException` versus metadata `TimeoutException`.

## 8. Topic handles and metadata

Bound the Java topic table by `max_open_topics`; coalesce concurrent opens of
the same name. Publish the returned handle before event dispatch can observe
it. `TOPIC_READY` contains identity and initial partition count. It is emitted
on resolving-to-ready, not on every metadata refresh, so its count can become
stale after expansion. `kr_topic_id` does not solve this.

`TOPIC_FAILED` settles topic readiness with its actual native reason. It does
not authorize completing all accepted records: those still own delivery
outcomes and may include uncertain writes. Likewise, do not evict a Java map
entry and immediately reopen the same name. Native failed handles remain
registered until asynchronous retirement finishes.
ABI v2 topic status exposes that retirement boundary; the binding retains the
old handle until then before reopening the name. Never accidentally route an
old topic UUID's pending records into a same-name replacement.

A current partition count is insufficient for Kafka's
[`Partitioner`](https://kafka.apache.org/43/javadoc/org/apache/kafka/clients/producer/Partitioner.html),
which consumes a `Cluster` including available partitions and broker nodes.
`partitionsFor` also needs proper `PartitionInfo` data. Require a bounded,
consistent native metadata snapshot with topic identity/generation, partition
IDs, leaders, replicas and ISR, plus broker IDs/endpoints and readiness/error
state. Unknown leaders in a real snapshot are legitimate; fabricating unknown
leaders for every partition is not a compatible metadata implementation.
The native model retains full replica/ISR/offline arrays and broker endpoints;
the ABI pins their memory against a bounded snapshot credit until release.
The initial `Cluster` exposes the requested topic's routing rows and UUID.
Cluster ID, controller, other topics and administrative topic sets are outside
this snapshot's supported scope.

`partitionsFor` waits up to `max.block.ms` for usable current metadata and
returns an immutable list. Refresh snapshots on age/invalidation and metadata
changes, not solely `TOPIC_READY`; bound large-topic output (§12). Custom
partitioners receive an immutable snapshot and execute outside `callGate`.
Explicit record partitions take precedence; validate returned hints and retain
native validation when metadata changes concurrently. Key hashing remains
native Java-compatible murmur2 by default; unkeyed routing uses the native
byte-based policy and does not promise Java's identical partition sequence.

## 9. Event dispatch, flush and shutdown

The single poller initializes every event slot and drains at most
`min(1024, max_completions_per_poll)` per call. A zero drain must be checked for
an FFI error under `callGate` before it is treated as idle. Release the lock
before dispatch. With ABI v2, use a bounded adaptive park after an empty drain
and unpark on Java submission/close; wakeups are hints and polling rechecks the
predicate. A future blocking wait requires lost-wakeup and owner-abort handling
and must not hold `callGate` while sleeping (§12).

| Event | Action |
|---|---|
| `DELIVERY` | Validate token and terminal state, map outcome, dispatch plugins/callback/future once (§5). |
| `INPUT_RELEASED` | Retire the lease pin exactly once; it says nothing about broker acknowledgement. |
| `FLUSH_DONE` | Complete the registered flush fence after all earlier event callbacks have returned. |
| `TOPIC_READY` / `TOPIC_FAILED` | Update topic readiness only (§8). |
| `FATAL` | Fence admission and wake waiters; continue draining authoritative per-record outcomes. |
| `CLOSED` | Finish normal draining; `count` is the number of unknown outcomes, not a count of missing callbacks. Proceed to exclusive destruction. |

Do not mass-complete accepted futures on `FATAL` or `TOPIC_FAILED`, or recycle
their slots before delivery events arrive. That loses actual outcomes and can
turn a possible write into a false definite failure.

### 9.1 Outcome mapping

The header defines `base_offset` as **this record's absolute offset**. Construct
`RecordMetadata` with that offset and batch index zero; use `-1` only when the
presence flag is absent. Use native timestamp when present (it may be broker
LogAppendTime or a batch timestamp on duplicate retry), otherwise the stored
CreateTime. Serialized null sizes remain `-1`.

| Native outcome/reason | Result |
|---|---|
| `ACKED` | Success, preserving optional offset/timestamp fields. |
| `NOT_WRITTEN` + `DEADLINE` | `TimeoutException`. |
| `NOT_WRITTEN` + topic deletion/resolution | Topic failure with retained native reason; only use a specific Kafka subclass when justified. |
| `NOT_WRITTEN` + compressed too large / invalid record | `RecordTooLargeException` / `InvalidRecordException`. |
| `NOT_WRITTEN` + authentication / producer fenced | `AuthenticationException` / `ProducerFencedException`. |
| `NOT_WRITTEN` + transport / resource exhaustion | `NetworkException` / `BufferExhaustedException`. |
| `NOT_WRITTEN` + partition failure, broker rejection, sequence unresolved, protocol/runtime failure, closed/cancelled | Binding `KafkaException` carrying the native reason and attempts; avoid inferring a broker error code that the ABI does not provide. |
| `UNKNOWN` + any reason | `DeliveryUnknownException extends KafkaException`, with reason and attempts. |

Retain native outcome/reason/attempts in structured diagnostics even when
mapping to a Kafka subclass. Unknown enum values are protocol failures, never
success. A retriable-looking Kafka exception does not tell the wrapper to
resubmit: Rust already performed the retries. `UNKNOWN` means the write may
have applied; application replay can duplicate it. This differs from the
planned known-delivery/unknown-offset feature in `AGENTS.md`, which is not yet
implemented and must not be simulated by Java.

### 9.2 Flush

Under `callGate`, call `kr_flush` and register its returned token before the
poller can drain it. Then wait outside the lock. Bound concurrent flush entries;
retain an abandoned waiter's entry until its event is consumed. Retry transient
control-credit pressure without holding the lock and allow interruption/close
to break the wait. A failed submission of a flush command is not a completed
flush.

Native flush is a watermark over previously accepted records. It means those
records reached a terminal outcome, including failures; it does not mean all
were acknowledged. Its ordered event queue places the fence after the covered
delivery events. Java returns only after those callbacks/futures have been
dispatched. Records concurrently admitted after the watermark need not finish.
Callers must inspect delivery results for success. Reject `flush()` from the
poller callback thread to avoid self-deadlock.

### 9.3 Close and abnormal owner termination

`close(Duration)` validates duration and uses one close operation:

1. Mark `CLOSING`, reject new sends and wake all admission/metadata waiters.
   Calls already serializing must recheck state before native admission.
2. Quiesce native buffer writers and release all outstanding acquisitions and
   submission handles. Keep foreign backing pins alive. Do this before waiting
   for `CLOSED`; use `finally` cleanup even on interrupted sends.
3. Submit `kr_close` with the remaining delivery-close budget (milliseconds,
   checked conversion), and keep draining deliveries, releases and fences.
4. On normal `CLOSED`, finish the drained events and check that accepted-record
   and successful-flush obligations are settled. Missing outcomes use the
   invariant-failure handling below; an unmatched flush fails explicitly.
   Exclude every other ABI call and invoke `kr_destroy` once. Only after it
   returns may remaining foreign pins be released. Return checked-out Java
   scratch before closing its arenas.
5. Wait for all active serializer/interceptor/partitioner invocations to exit
   before closing those plugins once. Complete shared teardown state and wake
   concurrent close callers.

Callback-initiated close only marks closure, wakes waiters and requests zero
delivery timeout, then returns. It must not wait for writers, plugins or its
own thread. The poller performs the remaining teardown after callback dispatch
unwinds. Once ordinary close has begun, interruption of a waiting close caller
does not abandon teardown or its retained resources.

`close()` requests graceful delivery completion. The duration bounds the
requested delivery-close phase, not physical I/O retirement. `kr_destroy` may
outlast it; blocking user code can also delay cleanup. Strict elapsed-time
Kafka close compatibility would need native lifecycle changes, not an early
free or abandonment of pins. Document this limitation prominently.

There is a second native terminal path: an aborted owner publishes terminal
record events but deliberately emits no normal `CLOSED`
(`kr-kafka-producer/src/actor.rs`). A `while (!closed)` loop can hang forever.
Require an owner-status/wait ABI before production release, distinguishing
running, normally closed and aborted with its terminal publication complete.
On abort, drain available terminal events, quiesce writers, then destroy
exclusively to join remaining provider retirement. Missing accepted outcomes
are a binding/native invariant failure and complete conservatively as
`DeliveryUnknownException`; never synthesize `NOT_WRITTEN` from a timeout.
Keep all pins until release events or destroy returns. The actual-owner abort
tests must cover teardown without a `CLOSED` event.

## 10. Configuration mapping

Use an explicit validated allowlist. Start with `kr_producer_config_init`, then
translate supported Kafka settings and documented native profile defaults.
Reject unsupported known settings and unknown `kr.*` names; unrelated plugin
settings may be passed to configured plugins and reported as unused. Never
blindly expose every ABI field as `kr.<field_name>`: pointers, sizes, enum values
and coupled resource limits are not a safe configuration surface.

| Kafka key | Native mapping / qualification |
|---|---|
| `bootstrap.servers`, `client.id` | Parse endpoints (including IPv6) and validate UTF-8/limits; copy at create. |
| `acks`, `enable.idempotence` | Require `all`/`-1` and true; no silent idempotence downgrade. |
| `max.in.flight.requests.per.connection` | `max_in_flight_per_connection`, 1 through 5. |
| `linger.ms` | `linger_max_ns`; native policy may seal earlier. |
| `batch.size` | `batch_target_bytes`, not a record cap; native hard caps and framing still apply. Reject unsupported zero-batching semantics until implemented. |
| `max.request.size` | `request_hard_bytes` plus Java-side uncompressed record validation; derive compatible batch/chunk caps rather than changing this field alone. |
| `buffer.memory` | `input_bytes`; not a bound on Java heap, scratch, compressed pools or all native memory. Report those budgets separately. |
| `delivery.timeout.ms`, `request.timeout.ms` | Checked nanosecond conversion; validate delivery timeout against request timeout plus linger. |
| `retry.backoff.ms`, `retry.backoff.max.ms` | Native min/max nanoseconds, with range/order checks; native jitter policy is documented. |
| `retries` | Native counts total attempts and stores a `u8`. Explicit supported values 1..254 map to 2..255 attempts. Reject larger values and zero in this idempotent profile. |
| `metadata.max.age.ms` | `metadata_max_age_ns`; does not replace Java snapshot invalidation (§8). |
| `max.block.ms` | Java admission/metadata budget (§7). |
| `compression.type`, `compression.zstd.level` | none/zstd and supported native zstd levels; reject unrepresentable levels. |
| `partitioner.class` | External hints using the requested topic's complete routing `Cluster` snapshot (§8). Reject unsupported ignore-key/adaptive settings rather than approximating them. |
| `security.protocol`, `sasl.mechanism` | PLAINTEXT/SSL/SASL_SSL; PLAIN/SCRAM-SHA-256/SCRAM-SHA-512. |
| `sasl.jaas.config` | Parse only supported static username/password login-module forms; no arbitrary JAAS execution or callback/login providers. Reject conflicting `kr.sasl.*` credentials. |
| Truststore settings | Parse PEM/JKS/PKCS12 in Java and pass **individual DER certificates** in `tls_roots`. Close password/certificate scratch after create; redact diagnostics. |
| `ssl.endpoint.identification.algorithm` | Require `https`; leave `tls_server_name` empty so Rust verifies each broker endpoint's hostname. Reject disabled verification. |
| Serializers/interceptors | Java plugin lifecycle (§6.4). |

Kafka's default `retries` is much larger than the native limit. The prototype
and initial limited profile must explicitly publish a default of 254 retries
(255 total attempts), not silently truncate Kafka's default. Widen native retry
accounting before claiming that setting's full compatibility. Supported Kafka
settings otherwise use the pinned client's defaults where representable; report
any deviation, including native batching targets, at configuration validation.
See the [Kafka producer configuration contract](https://kafka.apache.org/43/configuration/producer-configs/).

The native TLS configuration stores `roots_der`, and its optional
`tls_server_name` is a global override. Using the bootstrap hostname for that
override breaks verification against other brokers. Select trust roots
explicitly: Java truststore roots and native system roots are different trust
sources; do not silently enable the latter when the caller supplied a restricted
truststore. Do not accept mTLS keystore/private-key settings while native client
certificates are unsupported. Scope the initial security adapter narrowly and
test the documented defaults as well as explicit credentials.

Native extensions should cover intentional resource/transport choices, for
example `kr.input.mode`, `kr.transport`, `kr.max.open.topics`, bounded scratch
budgets and selected native pool limits. Validate combined limits through the
native config validator and expose the resulting memory budget in diagnostics.
No secrets, addresses or implementation-only layout fields belong in logs.

## 11. Verification and implementation sequence

Deliver each step as a separate logical change with its own acceptance checks.
Do not implement optimized ownership paths before the copy-path contracts pass.

1. **Bindings and construction.** Reproducible generator, all layout/offset
   goldens, classpath/module-path loading, missing/wrong ABI, missing native
   access, unsupported platform, malformed config and constructor rollback.
   Compile every pinned `Producer` method; verify production artifacts omit
   `binding-test-hooks` symbols.
2. **Copy-path prototype.** Bounded pending/topic/flush/scratch structures,
   atomic diagnostics, record/null/header conversions, plugin lifecycle and
   explicit unsupported operations. Assert accepted-prefix accounting and no
   retention of payload arrays after copied admission.
3. **Controlled concurrency tests.** Use the actual FFI `binding-test-hooks`
   owner/paused connector plus barriers and observable conditions, not sleeps
   or mocks of the native contract. Force delivery-before-submit-return,
   flush-before-registration, last-error overwrite attempts, send/close races,
   zero admission timeout, interrupted waits, callback reentry, callback throws,
   slow callbacks, duplicate/stale tokens and generation exhaustion. Use a pure
   bounded Java state model for generated histories of these operations.
4. **Native release gates.** Implement/test ordered terminal publication, full
   metadata snapshots, topic retirement/reopen semantics and owner-abort status.
   Drive native deadline/retry/failure paths deterministically. Verify flush
   cannot overtake callbacks, old UUID records cannot hit a recreated topic,
   metadata expansion updates `partitionsFor`, and abort does not require
   a nonexistent `CLOSED` event.
5. **Broker compatibility.** Add a producer-class switch to
   `integration/VerifyRecords.java`. Run the supported subset against both
   `KafkaProducer` and this binding: serialized bytes, metadata, key routing,
   delivery futures/callbacks, flush, concurrency and security. Use broker faults
   for realistic stop/restart, authorization, topic recreation and size limits;
   use deterministic native tests for rare reason codes instead of assuming
   every outcome is reproducible reliably against a live broker.
6. **Native/foreign lifetime harness.** Port `binding_lifetime.c` only when
   enabling each ownership path. Test retention before delivery, release before
   delivery, rejected registration/submission, failed commit, interrupted copy,
   close with an active writer, destruction with a paused connector and owner
   abort. Prove backing storage cannot be freed or reused until the release
   boundary; checking that a Java table contains an arena is insufficient.
   Run memory-protection failure probes in isolated subprocesses.
7. **Stress and benchmark.** Platform and virtual callers against tiny limits;
   assert bounded retained bytes, no lost wakeups, token leaks or duplicate
   callbacks. Add a class switch to `benchmarks/JavaOpenLoop.java` and use
   [BENCHMARKING.md](../BENCHMARKING.md): matched acknowledgement/codec/workload,
   warmup, CPU, heap/native memory, allocation rate, goodput and latency tails.
   Compare copy versus native buffer before enabling an optimization. Keep
   correctness constraints fixed across benchmark variants.

## 12. Native changes and deferred extensions

The required native contracts below are implemented alongside ABI v2. The useful
follow-ups remain deferred. Exported struct layouts require exact sizes and the
binding handshakes an exact ABI version. Decide/version compatibility explicitly:
adding a function does not make an old shared library provide it, and extending
an existing struct is not safe merely because it has `struct_size`. Generate
and ship the matching pair.

**Implemented native release gates:**

- **Ordered terminal delivery** (§5), implemented in the native producer with
  bounded storage. No ABI change is necessary if the event shape is unchanged,
  but the stronger ordering contract needs shared native/Java tests.
- **Metadata snapshot**, superseding a count-only `kr_topic_partitions` idea.
  Define a size-versioned snapshot handle with identity/generation/status and
  bounded caller-owned pages of broker/partition rows. Pages refer to the same
  immutable snapshot; define release, stale handles and resource exhaustion.
  Include metadata refresh/invalidation and topic-retirement completion, so
  Java can safely reopen names after failed/deleted handles retire.
- **Owner lifecycle status**, observable after terminal events have been
  published, to distinguish owner abort from ordinary idle/closed state while
  provider retirement may still be pending. Teardown must remain safe after
  either condition, without treating an idle timeout as proof of termination.

**Useful follow-ups after the copy baseline:**

- **Operation-local submission result**, for example new copy/leased entry
  points with a size-versioned `{accepted, error}` output. Define the rejected
  suffix's reason in the same call. Also provide operation-local drain status:
  zero from legacy `kr_poll_events` still needs an atomic last-error query.
  Keep the gate until every diagnostic-dependent path is migrated; submit
  results alone do not make concurrent legacy polling safe. Identifier
  publication and lifecycle synchronization remain necessary afterwards.
- **`kr_wait_events(producer, timeout_ns)`**, backed by the single application
  waiter: atomically check/register/recheck to avoid lost wakes, allow spurious
  wakeups, distinguish timeout and owner termination, and keep events queued
  for the ordinary drain. It must not overwrite unrelated operation diagnostics.
  Java must guard its handle lifetime without holding `callGate` across the
  wait, and shutdown must wake or bound that wait before exclusive destroy.
- **Metrics configuration/snapshot export** over the existing passive HDR
  `MetricsReader` implementation in `kr-kafka-producer/src/telemetry/metrics.rs`.
  Export bounded snapshots with schema version, units, interval boundaries,
  scopes, overflow diagnostics and HDR precision. Keep histogram scans and
  export on the reader side, preserve simulation determinism, and let Java
  metrics/OTel be sinks rather than changing the native representation.
- **Larger retry range** for Kafka-compatible retry limits; review attempt
  counters and control-plane retry policy as well as the ABI config field.
- **Foreign input and external batch APIs**, only after separate ownership,
  prefix-acceptance, memory-accounting and per-partition ordering designs.
  A flat submit signature alone does not make a heap-access critical downcall
  safe; leave that optimization out until its bounded leaf-call proof exists.
