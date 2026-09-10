# Java producer over FFM

`io.krkafka.producer.KrKafkaProducer<K,V>` implements the complete Kafka 4.3.0
`Producer` interface with a deliberately limited supported profile. It requires
JDK 25 and the matching **ABI 4** `kr-kafka-ffi` library. Rust owns routing,
batching, idempotent retries and network progress. Java owns serialization,
bounded admission, interceptors and callback dispatch.

```java
var config = new java.util.HashMap<String, Object>();
config.put("bootstrap.servers", "localhost:9092");
config.put("key.serializer", org.apache.kafka.common.serialization.StringSerializer.class);
config.put("value.serializer", org.apache.kafka.common.serialization.StringSerializer.class);
try (var producer = new io.krkafka.producer.KrKafkaProducer<String, String>(config)) {
    var delivery = producer.send(new org.apache.kafka.clients.producer.ProducerRecord<>("events", "key", "value"));
    producer.flush();
    System.out.println(delivery.get().offset());
}
```

Launch on the classpath with `--enable-native-access=ALL-UNNAMED`, or on the
module path with `--enable-native-access=io.krkafka`. The module exports only
`io.krkafka.producer`; generated bindings and the loader remain internal.
The Kafka jar is used unchanged as automatic module `kafka.clients`.

Use `-Dkr.kafka.library=/absolute/path/libkr_kafka_ffi.so` for an explicit native
artifact. The override is canonicalized and loaded through `System.load` in
the binding's class loader. ABI equality is checked before generated calls.
Without an override, the loader expects a checksummed artifact under
`META-INF/native/linux-{x86_64,aarch64}`. The packaging target is Linux with
glibc 2.28 or later; musl and other platforms are unsupported for production.
macOS can load the explicit test library for portable ABI/owner tests, but its
production constructor cannot start a network owner. Use one binding class
loader per process. The JVM rejects another loader's attempt to load the same
canonical library file; separately extracted copies are unsupported and have
not been tested across loaders. Libraries remain loaded for the process lifetime.

## Supported contract

Both `send` methods, serializers supplied as constructor arguments or configured
classes, interceptors, custom partitioners, `partitionsFor`, `flush` and both
`close` methods are implemented. Metadata includes broker endpoints, leaders,
replicas, ISR, offline replicas and topic identity from immutable native
snapshots. Retired topic handles may reopen a name without moving old UUID
records into the new topic. `metrics()` returns an immutable empty map.
Custom partitioners receive routing metadata for the requested topic, including
its UUID and complete partition rows; the snapshot does not expose cluster ID,
controller, other topics or administrative topic sets.
Broker telemetry subscriptions are no-ops; `clientInstanceId` and all
transactional methods throw `UnsupportedOperationException`.

The supported delivery profile requires `enable.idempotence=true` and
`acks=all`/`-1`, with none or zstd compression (levels 1..3). Native retries are
bounded to **254 retries / 255 total attempts**, also the binding default.
Explicit `retries` values outside 1..254 fail configuration. Native key routing
uses Java-compatible murmur2; unkeyed routing uses a byte-based policy. Linger
is an upper bound and batching can seal earlier. `batch.size` is a target, not
a record cap. Request framing reduces the available record payload.

The copy path packs native scratch and copies into bounded native input storage
before `send` returns. No serialized Java key/value array is retained for delivery.
Producers with interceptors retain a bounded header snapshot for acknowledgement. Native
buffer and arbitrary foreign-segment sends are not exposed: their lifetime
proofs and matched benchmarks remain separate extensions.

Each accepted record has one non-cancellable, read-only `Future`. A timed-out
`get` or interruption does not cancel delivery. Native `UNKNOWN` becomes
`DeliveryUnknownException` with reason and attempt count: the write may have
applied, so application replay can duplicate it. Mapped Kafka exceptions retain
the native diagnostic as a suppressed structured exception. The wrapper never retries an accepted
record independently of Rust.

Interceptors run before serialization; header-aware serializers are called
even for null values. Serializer output determines nullness. Duplicate ordered
headers and null versus empty values are preserved. Supplied serializers are
closed by the producer but are not configured by it. Configured plugins are
configured and closed once, including constructor rollback.

Acknowledgements use Kafka 4.3's header-aware three-argument interceptor method;
its default implementation also supports legacy two-argument plugins. Accepted
records receive structurally frozen, deep copied headers isolated from later
mutation of the input values. These copies are retained only when interceptors
exist and are charged against `kr.interceptor.header.bytes` until terminal dispatch
finishes. Synchronous rejection borrows structurally frozen input headers for the
invocation and does not retain them after `send` returns. A plugin retaining those
borrowed values must copy them if it needs isolation from later caller mutation.

Terminal dispatch runs interceptor acknowledgement, user callback, then future
completion. Callbacks follow native admission order within a partition, including
failure and expiration paths. Callbacks may send only when admission can proceed
immediately; they must not wait on pending delivery futures. `flush` from a
callback throws. Callback close requests zero delivery timeout and returns so
the poller can complete teardown after dispatch unwinds.

`max.block.ms` is one monotonic budget for metadata, pending slots, interceptor headers and scratch
capacity, excluding user serialization/partitioner code. Admission timeout and
ordinary record rejection produce an inline callback and failed future.
Serialization/argument/closed-state errors and interruption throw synchronously.
`flush` waits for terminal callbacks/futures for the native admission watermark;
inspect the delivery futures to distinguish successful records from failures.

**Close duration bounds delivery shutdown, not elapsed teardown time.** Native
I/O retirement and blocking user plugins/callbacks can extend it. Concurrent
close callers share one teardown; interruption does not abandon native resources.
Aborted owners are detected through ABI status after terminal publication rather
than by an idle timeout or an assumed `CLOSED` event. Missing accepted outcomes
are conservatively unknown after exclusive destruction.

## Configuration and resource bounds

Kafka defaults are used for supported settings where representable, with these
explicit profile choices: default client ID `kr-kafka`, default 254 retries,
native byte-based unkeyed routing, and readiness transport. `kr.transport=uring`
requires io_uring; there is no automatic fallback. Unsupported known Kafka
options and unknown `kr.*` options fail construction. Unrelated plugin options
are passed through. Security option values and credentials are not printed in
validation errors.

| Extension | Default | Bound |
|---|---:|---|
| `kr.input.mode` | `copy` | Only copy is enabled |
| `kr.transport` | `readiness` | `readiness` or `uring` |
| `kr.record.descriptors` | 65,536 | 1..1,048,576 pending records |
| `kr.max.open.topics` | 1,024 | 1..65,536 topic entries |
| `kr.scratch.bytes` | 4 MiB | Total shared-arena slab bytes |
| `kr.scratch.checkouts` | 64 | At most 4,096 concurrent checkouts |
| `kr.interceptor.header.bytes` | 4 MiB | 0..2,147,483,647 retained header bytes for interceptor acknowledgements |
| `kr.serialization.concurrency` | 64 | At most 4,096 active sends/plugin calls |
| `kr.max.flushes` | 64 | At most 4,096 retained fences |
| `kr.max.header.count` | 1,024 | 0..65,536 headers per record |
| `kr.max.completions.per.poll` | 128 | 1..1,024 events per drain |
| `kr.sasl.username`, `kr.sasl.password` | absent | Static credentials, exclusive with JAAS |

`buffer.memory` bounds the native input pool, including native bookkeeping; it
does not bound all native memory or the Java heap. Scratch has its own total byte
and checkout limits. Valid serialized arrays retained by active sends are bounded
by concurrency and request size. Arbitrary allocation within user serializers,
callback objects, Kafka classes and JVM overhead is outside those limits.
The interceptor header budget charges two bytes per key character plus each value
byte. Pending-record and per-record header-count limits separately bound snapshot
object counts. One record exceeding the header byte budget fails with
`RecordTooLargeException`; temporary shared pressure follows `max.block.ms`.
`resourceBudget()` reports this configured cap as `java.interceptor.header.bytes`.
Native compressed pools, connection staging/receive buffers, metadata and codec
workspace have separate configured budgets. The native validator checks combined
resource relationships before startup. `resourceBudget()` exposes an immutable
map of these configured independent budgets; it is not a resident-memory metric
and includes no credentials or native addresses.

Each Java metadata snapshot is limited to 65,536 partitions, 4,096 brokers and
16 MiB of row data. A larger snapshot fails explicitly instead of growing the
binding's retained output without a bound.

Supported security protocols are PLAINTEXT, SSL and SASL_SSL. SASL supports PLAIN,
SCRAM-SHA-256 and SCRAM-SHA-512. JAAS is parsed only as one matching static Kafka
login module with `required` and quoted username/password options; arbitrary
login modules or callback providers are not executed. Set the mechanism explicitly
for SASL_SSL: Kafka's GSSAPI default is unsupported.

PEM, JKS and PKCS12 truststores are parsed in Java and passed as individual DER
certificates. Without an explicit truststore, Java's default trust managers supply
the roots. Restricted truststores never silently add native system roots. Broker
hostname verification requires `https`, using each actual broker endpoint.
Mutual TLS, disabled hostname verification, SASL_PLAINTEXT, GSSAPI, OAuth, other
codecs, transactions and non-idempotent delivery are rejected.

## Build and verification

From the repository root, build the explicit test artifact and run the Java tests:

```sh
RUSTC_WRAPPER= cargo build -p kr-kafka-ffi --features binding-test-hooks
cd kafka/kr-kafka-java
./gradlew test
```

The tests use the actual FFI owner fixture and isolated native-library subprocess
probes. Production artifacts must be rebuilt **without** `binding-test-hooks`.
The production symbol gate must verify no `kr_test_*` exports before packaging.
Builds pin Gradle 9.7.1, Kafka 4.3.0, the dependency lockfile and SHA-256 dependency
verification metadata; dependency upgrades require compatibility review.

Generated sources are checked in. Download the exact platform archive named in
`gradle/jextract-toolchain.json`, then run from the repository root:

```sh
scripts/kafka-java-bindings.sh --archive /path/to/pinned-jextract.tar.gz
scripts/kafka-java-bindings.sh --check --archive /path/to/pinned-jextract.tar.gz
```

The script verifies the archive, extracts a private toolchain, checks bundled JDK
and libclang versions, derives exact names from the pinned tool's include dump,
and compares every output file and all input hashes in check mode. Linux and
macOS choose different C typedef aliases for fixed-width 64-bit fields. The
manifest pins one narrow normalization: after checking both emitted layouts use
canonical `OfLong` layouts, qualified `C_LONG` references become `C_LONG_LONG`. No fields,
padding or function descriptors are reconstructed. Java layout
tests compare every public C size, alignment and field offset against generated
layouts, using the same full field inventory as Rust's `tests/c_header.rs`.
Native Linux architecture tests, packaged binary dependency audits, broker fault
coverage, and matched benchmarks are required before claiming release readiness;
portable test success alone does not establish those results.

### Linux release artifact

On Linux aarch64 or x86_64, with Python 3.12+, rustup and JDK 25 installed:

```sh
python3 scripts/kafka-java-native.py
python3 scripts/kafka-java-native.py --self-test
cd kafka/kr-kafka-java
./gradlew releaseJar
```

The result is `build/libs/kr-kafka-java-0.1.0-linux.jar`, containing both
`linux-aarch64` and `linux-x86_64` libraries. Ordinary `jar` and `build` tasks
produce Java archives without embedding native build outputs. The release task
requires the native build first; it never silently packages missing or stale
libraries. It pins Rust and a SHA-256-verified Zig cross compiler, targets a
baseline CPU and glibc 2.28, and uses a portable `libkr_kafka_ffi.so` SONAME.
The manifest records the compiler-input digest, pinned toolchain digest, ELF
architecture, required glibc version, system dependencies, exact production ABI
exports and artifact checksums. `verifyNativeResources` independently parses
both ELF files and checks all of these properties before `releaseJar`. A changed
Rust, C/header, Cargo or packaging-tool input requires a new native build.
Verification is portable and can run on macOS against transferred artifacts.
The self-test corrupts real ELF headers, architecture, exports, glibc versions,
checksums and source freshness, and checks missing and unexpected resources.

Run the public-facade packaging smoke on each target JDK, with the released jar
and its runtime dependency jars in a separate directory:

```sh
scripts/kafka-java-package-smoke.sh "$JAVA_HOME" \
  "$PWD/kafka/kr-kafka-java/build/libs/kr-kafka-java-0.1.0-linux.jar" \
  /path/to/runtime-dependency-jars "$PWD"
```

This compiles a separate consumer module and creates/closes a producer through
both classpath and module-path launches, without a native-library override.
For the glibc minimum check, execute it within a glibc 2.28 runtime image on each
architecture. ELF version auditing and runtime startup are complementary gates.
`LayoutsTest` must also run with each architecture's JDK and C compiler; it
compares every native struct size, alignment and field offset independently.

Batch targets use estimated compressed wire bytes by default, including the
61-byte batch header. `kr.batch.target.mode=raw` selects the legacy raw-byte
policy. Both modes keep the hard raw/output limits. ABI 3 appends this mode to
the native configuration; rebuild native libraries together with the bindings.

Request grouping is independent: `kr.request.batching.policy` accepts `sealed`
(default), `single-partition`, or `broker-ready`. The latter can finish younger
encoded batches while preparing a broker request. See the
[policy contract](../kr-kafka-producer/REQUEST_BATCHING.md). This field requires
matching ABI 4 bindings and library; ABI 3 configurations are rejected.
