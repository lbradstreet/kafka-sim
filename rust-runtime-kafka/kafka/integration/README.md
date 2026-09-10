# Real Kafka correctness gate

This is a correctness/security/recovery harness, separate from the offered-load
benchmarks. Use the commands below to create fresh execution reports.
It uses an existing Kafka 4.3.0 distribution, Java 21 or newer,
OpenSSL, and the release `producer_check` binary. It installs and downloads
nothing. Native execution results belong to the fixture's `report.json`; local
parser tests or successful compilation do not establish broker correctness.

Build and run a small pilot first:

```sh
RUSTC_WRAPPER= cargo build --release -p kr-kafka-bench --bin producer_check
python3 kafka/integration/real_broker.py prepare /tmp/kr-real-pilot \
  --kafka-home /opt/kafka --nodes 1 --base-port 19092
python3 kafka/integration/real_broker.py run /tmp/kr-real-pilot \
  --producer-binary target/release/producer_check --suite baseline \
  --backends readiness --records 192
```

Every fixture directory must be new. `prepare` writes reviewable broker and
certificate configuration only. `run` creates the private test CA, formats only
the new node data directories, starts loopback-only brokers with 256–512 MiB
heaps, creates isolated per-case topics, and stops its own process groups on exit.
It never deletes or replaces an existing fixture. Public fixture credentials are
`bench` / `fixture-password`; these certificates must never be trusted elsewhere.
Each command has a watchdog; each producer case has a 300-second watchdog. Logs,
failed histories, partial ledgers, command arguments, binary/source hashes and
verification results remain on disk. Fixtures are single-use, including after a
failure. A failed case does not silently disappear from the final report.
SIGTERM and SIGINT unwind through owned command, producer and broker cleanup,
retain the interruption in command/case/report JSON, stop the suite immediately,
and exit 143 or 130. Interrupted outages never restart brokers during cleanup.
Allow 90 seconds between the outer watchdog's SIGTERM and final SIGKILL so owned
process groups can exhaust their bounded termination waits. SIGKILL itself cannot
run Python cleanup.

Suites:

| Suite | Cases |
|---|---|
| `baseline` | Plaintext/none, one case per selected backend |
| `positive` | Both backends × plaintext/TLS/PLAIN/SCRAM256/SCRAM512 × none/zstd1: 20 cases |
| `extended` |36 cases: plaintext/TLS vectored writes, TLS/SCRAM512/plaintext restart, four lanes with leased input, metadata-pending admission, stopped application event polling, topic recreation, unavailable/deadline close, six negative security cases, and response loss or RF3 leader loss |
| `all` |56 cases: positive and extended suites |
| `bindings` | Production C ABI on both backends:64 copy,64 native-lease and64 foreign-lease records per case |

Use `--nodes 3` in a separate fixture for RF3/minISR2 leader loss. Other outages
stop all three nodes when the case requires actual unavailability. A single-node
fixture adds a bounded plaintext framing proxy on `base-port+4`; Kafka advertises
that proxy endpoint, and the real broker listens on `base-port`. SSL/SASL listeners
remain direct. The lost-success case arms the proxy exactly once: it captures a
complete, structurally validated Produce13 response containing only successful
partitions and nonnegative offsets, persists its bytes/hash/correlation/offsets,
then closes the client connection before forwarding any byte of that frame. The
gate requires retry attempts and an independently unique consumed log. Arbitrary
broker kills are not labeled as commit-before-response fault injection.

The RF3 case keeps the failed partition0 leader dead after replacement leadership
and ISR2 are observed. The producer resumes, flushes the full middle record cohort
and publishes a `fault_settled` checkpoint. The Java verifier checks those
acknowledged records in the log **before the orchestrator restores the node**.
The checkpoint, record IDs, topology descriptions and verification remain in the
case directory. Only then does the orchestrator restore the old node, observe
ISR3, and resume the last cohort. A run lacking this proof cannot pass by merely
restoring the broker before records finish.

The producer driver exchanges flushed NDJSON phase messages and bounded `continue`
lines with the orchestrator. Before topic deletion the orchestrator verifies the
old generation's checkpoint. After recreation it verifies the new UUID and rejects
any old-generation ID in the new log. The driver independently checks old-handle
settlement and final delivery/input-release/credit conservation. The log verifier
does not infer producer outcomes from broker record counts.

`VerifyRecords.java` uses Kafka's Java `Admin` and `KafkaConsumer`, never the Rust
protocol or record decoder. It discovers the immutable topic UUID, explicitly
assigns every partition, seeks to zero, captures end offsets, and consumes to
those fixed boundaries without group subscription or offset commits. It checks:

- Unique run/record identity headers and exact key/value bytes, including null
  versus empty values; all ordered headers, including duplicate names/null values.
- Exact CreateTime timestamps, explicit partitions, and per-partition acceptance
  order. There is no cross-partition ordering assertion.
- Each Acked record exists once at its reported absolute offset and topic UUID.
  NotWritten records are absent; Unknown records may be absent or present once.
- Unaccepted, foreign-run and wrong-generation records are absent, no accepted ID
  appears twice, and the log remains unchanged during final verification.

`Delivery.timestamp_ms` is the optional broker response's **batch**
`logAppendTime`, not a second copy of each record's CreateTime. Kafka4.3's
[duplicate-batch path](https://github.com/apache/kafka/blob/a9ce3221537b8653448750697915607dc7936cf3/storage/src/main/java/org/apache/kafka/storage/internals/log/UnifiedLog.java#L1247)
returns cached batch timestamp through that field; its
[producer state](https://github.com/apache/kafka/blob/a9ce3221537b8653448750697915607dc7936cf3/storage/src/main/java/org/apache/kafka/storage/internals/log/ProducerAppendInfo.java#L217)
records `batch.maxTimestamp()`. The
[Java producer](https://github.com/apache/kafka/blob/a9ce3221537b8653448750697915607dc7936cf3/clients/src/main/java/org/apache/kafka/clients/producer/internals/FutureRecordMetadata.java#L109)
also uses a returned batch timestamp for its record metadata. The verifier
therefore preserves this optional response value separately, checks its domain,
and compares **consumed record timestamps against accepted input timestamps**.
An initial oracle incorrectly equated these two different fields; lost-response
deduplication exposed that error. The correction leaves content, timestamp,
partition, ordering, UUID and absolute-offset checks intact.

The fixture uses its plaintext administrative listener for independent reads,
including when the producer test uses TLS/SASL. Security-negative cases require
setup failure before ready, explicit Authentication16 rejection evidence, an
empty committed log, completed close/join and zero observable retained credits. Wrong CA, hostname, an expired leaf on a dedicated TLS
listener, and PLAIN/SCRAM passwords are covered. The expired leaf has fixed
validity January1–2,2000 and a valid fixture CA/signature/SAN.

Positive final verification requires complete producer close/join and a known
nonempty selected topic generation. Pre-deletion checkpoints, expected setup
failure and unresolved metadata have explicit verifier modes. Failed producer
runs still invoke the independent verifier with `--mode failed`; a sound log does
not turn a failed delivery run into a passing case. Raw consumed records are
flushed to `verification.json.records.ndjson` before semantic assertions, so a
failed comparison preserves wire evidence as well as the error.

`api-versions.stdout` records actual broker advertisements, and startup requires
Produce13/Metadata12/InitProducerId4/ApiVersions3/Handshake1/Auth2. The intended
distribution is 4.3.0, not a claimed minimum supported Kafka release. Record the
downloaded archive's official checksum alongside the fixture; the harness also
records the installed script's version output. This suite is not a performance
comparison, transactional producer test, or general Kafka consumer implementation.

Independent primitive gates and direct verifier invocation:

```sh
python3 kafka/integration/real_broker.py self-test
javac -cp '/opt/kafka/libs/*' -d /tmp/kr-verify kafka/integration/VerifyRecords.java
java -cp '/tmp/kr-verify:/opt/kafka/libs/*' VerifyRecords --self-test
java -cp '/tmp/kr-verify:/opt/kafka/libs/*' VerifyRecords \
  --bootstrap localhost:19092 --ledger /tmp/kr-real-pilot/readiness-plaintext-none/producer.json \
  --output /tmp/reverification.json --generation 0 --timeout-ms 30000
```

The JSON parser rejects duplicate keys, unsupported numeric forms, malformed
strings, excessive nesting and oversized ledgers. The consumer bounds partitions,
record count, fetch sizes, duration and output history. The proxy bounds frames,
live connections and per-connection correlation slots; it owns no producer state.

For C ABI coverage, build the production library and use a fresh fixture:

```sh
RUSTC_WRAPPER= cargo build --release -p kr-kafka-ffi
python3 kafka/integration/real_broker.py prepare /tmp/kr-real-bindings \
  --kafka-home /opt/kafka --nodes 1
python3 kafka/integration/real_broker.py run /tmp/kr-real-bindings \
  --suite bindings --ffi-library target/release/libkr_kafka_ffi.so
```

The orchestrator compiles `ffi_check.c` against the production header/library
with strict C warnings, hashes the source/executable/library, and uses the same
watchdog, fresh topics and Java verifier. `--ffi-binary` accepts a precompiled
runner. The runner keeps foreign mappings read-only while retained and makes
them unreadable at InputReleased; destruction must return after all192 deliveries,
128 releases and exactly one Closed event. The ABI exposes no credit-pool
snapshot, so its ledger records that limitation instead of inventing counters.
Both bindings cells are plaintext/zstd1 and use the current default staging
transport; they do not establish TLS or vectored binding performance.

The final driver also checks delivery/input-release completeness at the moment
Closed is observed, matches the actual reported backend to the requested case,
and requires partition-0 records in the leader-dead cohort. The metadata-pending
case submits without waiting for TopicReady; it does not forcibly withhold a
Metadata response or prove that native admission observed pending metadata.

## Java producer comparison

`VerifyRecords --producer-class` executes a bounded workload through either
`org.apache.kafka.clients.producer.KafkaProducer` or
`io.krkafka.producer.KrKafkaProducer`. The normal consumed-log mode stays
independent of the binding: it compiles with Kafka's jar alone, and loads the
chosen producer by its public `Map` constructor only in producer mode.

```sh
python3 kafka/integration/java_producer_compare.py /tmp/kr-java-smoke \
  --kafka-home /opt/kafka --java-home /opt/jdk-25 \
  --binding-classes kafka/kr-kafka-java/build/classes/java/main \
  --ffi-library target/release/libkr_kafka_ffi.so \
  --suite smoke --transport readiness --records 192 --callers 4
```

The directory must be new. The harness copies the binding and native artifact
into the fixture, reuses the owned Kafka4.3 fixture and process watchdogs above,
inspects the copied native artifact for test symbols,
compiles/runs the independent verifier's corruption self-tests, and compares
fresh per-producer topics. `--suite positive` runs both producers across
plaintext, TLS, PLAIN, SCRAM-SHA-256 and SCRAM-SHA-512 with none/zstd1 compression.
Run that suite separately with `--transport readiness` and `--transport uring`;
the selected native backend must work without fallback. The binding classes
argument also accepts a jar. This gate requires JDK25 for the binding.
For a class directory, the harness includes the binding's required symbol
inventory resource. Positive cases also load a real header-aware interceptor:
its added header must reach the log and every three-argument acknowledgement
must receive the exact original and added headers. Interceptor close occurs once.

Records cover null/empty/binary keys and values, explicit partitions, native
versus Java murmur2 keyed routing, ordered duplicate headers with null/empty
values and a UTF-8 key, and explicit CreateTime. Callers serialize concurrent
submissions only within each expected partition; different partitions progress
concurrently. Every callback must occur once and in that partition's submission
order. Flush must return after all callbacks/futures complete. Future partition,
callback/future offset and timestamp agreement and serialized sizes are checked;
an acknowledged timestamp may be the broker's returned deduplication batch time.
The independent consumer separately
checks actual bytes, partitions, timestamps, headers and acknowledged offsets.
`partitionsFor` is compared with independent Admin leader/replica/ISR metadata.

The Java driver writes `kr-kafka-java-producer-check/v1`, with no invented native
tokens or credit counters. The verifier accepts this explicit schema beside the
existing native ledger schema. Reports retain source/library/class hashes,
commands, logs, Java callbacks/futures, raw consumed records and paired content
and routing comparisons. A failed producer or verifier remains a failed case.
The positive comparison does not by itself prove broker fault recovery or
latency/throughput parity.

`--suite recovery` runs both producers with none/zstd1 on plaintext. After the
first third of records is flushed, a bounded phase barrier kills the owned
broker. The same producer must admit the remaining cohort while the broker is
dead, with those callbacks still pending. The controller restarts that broker,
observes restored leaders/ISR, and releases the second barrier. All records must
then flush and pass the independent consumed-log and callback-order checks.
Phase order, stdout size, input lines and total runtime are bounded; retained
evidence includes the killed PID, pending cohort and restored topology.

`--suite limits` sets a broker topic limit of 1024 bytes and submits 4096-byte
values with a larger client limit. Both clients use one caller and one in-flight
request, awaiting each rejected future before the next send to isolate broker
error mapping from pipeline failure handling. Each callback and future must expose
`RecordTooLargeException` for a certain rejection, flush must settle all records, and the independent
consumer must prove the rejected records absent. This checks broker rejection,
separately from the binding's local record-size checks.

`--suite authorization` enables Kafka's authorizer only in its fresh fixture,
allows the public `bench` SASL identity to describe each test topic and denies
writes. Like size mapping, both clients use one in-flight request and sequential
send/future settlement. The Kafka client must return `TopicAuthorizationException`; the native
client must retain its structured `NotWritten`/`BrokerRejected` diagnostic.
Callbacks and futures must settle once and the consumed log must remain empty.
The anonymous fixture administrator remains a superuser for provisioning and
independent log verification.

For size or authorization errors after a prior ambiguous attempt, the native
producer may conservatively report `DeliveryUnknownException`. The ledger
preserves `Unknown`, reason and attempt count; consumer absence is recorded
independently and does not change that reported outcome.
Continued metadata access is required after an entirely definitive rejection
cohort. Pipelined rejection, quarantine and fail-closed sequence recovery are
covered separately by deterministic native actor tests.
The sequential rejection phase has a 120-second aggregate bound for its 192
operations; each send/future retains the same 30-second API budget as other
profiles, and the process watchdog remains 180 seconds.

`--suite recreate` verifies a flushed first cohort against the old topic UUID,
then deletes the topic, observes its absence and creates it with five partitions
instead of four. Admin must observe a different UUID. The same producer must
expose the new partition count before sending the remaining cohort; a native
`TopicDeleted` error may be observed during retirement. The new log must contain
only that new generation. This live case uses a
flush boundary. In-flight old-UUID isolation is covered separately by the native
and real-owner deterministic conformance tests.

For a subsequent benchmark in the same owned fixture, `--hold-ms 600000` emits a
`comparison-complete` JSON phase after a passing matrix and keeps the broker
alive until a bounded `continue` line arrives on stdin. The timeout, EOF,
interrupts and ordinary completion all run owned broker cleanup. Run performance
work only after the comparison phase has finished, without other VM builds.
