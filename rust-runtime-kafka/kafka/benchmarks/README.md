# Kafka host measurement gate

These tools prepare measurements. Follow
[the workspace measurement protocol](../../BENCHMARKING.md) on a quiet Linux
host; simulation and macOS primitive tests are correctness checks.

Build the actual native producer, then run a provisioned topic:

```sh
cargo build --release -p kr-kafka-bench
target/release/kr-kafka-bench kafka/benchmarks/profile.json /tmp/kr-result.json
```

The topic must exist with the profile's partition count. Kafka must negotiate
Produce13, Metadata12, and InitProducerId4. Start with the pinned Kafka 4.3.0
distribution used by the protocol fixtures. The producer currently uses bounded
contiguous staging; the report records that choice. Select `readiness` or `uring`
explicitly so an unavailable provider cannot silently change a comparison.

Every offer has the immutable deadline `floor(index * 1e9 / rate)` relative to the
measurement start. A refused admission is counted and never retried. A delayed
benchmark thread catches up against the original schedule, recording scheduler
lateness; it does not reset the offered clock. Inspect that distribution and the
actual offered interval before attributing saturation to a client. Payload corpus
construction, codec calibration, connection provisioning, and initial topic
metadata resolution are outside the measured interval. Initial producer identity
setup may still be completing when the topic becomes ready, and is included.

Histograms retain 1025 counters each. Quantiles are bucket upper bounds with at
most 6.25% bucket width above 16ns; maximum latency is exact. Reported stages are
scheduled offer to admission call, duration of the admission call, submission to
delivery observation, and scheduled offer to delivery/ack observation. Rust and
Python drain delivery notifications on the offering thread; Java observes them
on its sender callback thread. This difference is part of the binding cost.
The corpus is at most 16MiB; accepted Rust timestamps occupy a bounded map whose
population cannot exceed admitted record capacity. Whole-process RSS includes
that harness memory. Credit pools expose actual lifetime high-water marks plus
sampled interval peaks, hard configured limits, and reservation/release totals.
Mailbox/input occupancy sampling can miss short peaks.

Rust also exports `producer_distributions`, the producer's final global HDR
interval, after owner join. It covers the producer lifetime, including setup and
drain; the harness requests no intermediate snapshots during measurement. These
are producer stage timings and event-weighted depths, separate from the harness
observation latencies above. Quantiles are inclusive equivalent-value ranges at
the reported precision; exact maxima exclude rejected outliers. Empty quantiles
and missing runtime bounds are null. Sample counts, rejected samples and missing
timing diagnostics accompany every report. A missing final interval is reported
as unavailable. Snapshot export and histogram scans are outside measured time.

The process CPU interval includes all producer and harness threads from the first
offer through close/drain, with setup excluded. `/usr/bin/time -v` additionally
records whole-process user/system CPU and peak RSS, including setup. Keep those
different intervals separate. Run `perf stat` around the same unchanged binary to
collect cycles, instructions, branches, cache misses, context switches and
migrations. Rust cumulative telemetry records input copies, codec consumption,
actor polls, confirmed Kafka Produce bytes, and staging/coalescing copies over the interval.
When TLS instrumentation is active, ciphertext bytes and explicit adapter copies
are separate counters covering all connections, including control traffic.
Lifetime maximum poll time and codec quantum include startup. Crypto-library
internal copies require a separate profiler. Final actor status supplies actual
batch seal counts by reason and total raw/target bytes; the fill ratio uses each
batch's effective target. These counters cover the producer lifetime. Request-slot
credit high-water marks report actual peak outstanding requests.

A profile can set `native_diagnostics: true` for an explicitly instrumented run.
`HostProducer::start_with_diagnostics` enables fixed-size counters and a 65-bucket
power-of-two nanosecond histogram from native terminal output publication to
observer consumption or abandoned-cell destruction. Counters include stream I/O
since setup, including TLS and control, and report current/peak pending and
undrained cells. Connect/listen operations are outside this timing scope;
io_uring's immediate rejected stream operations are included, while readiness
pre-admission rejections are excluded. Completion clocks and mutex overhead are
part of this run. Normal `start` and simulated completions read no extra clocks.
Provider/runtime queue lengths are sampled at 1ms and may miss short peaks;
provider commands are coordinator ingress, not kernel SQEs. Weak observers retain
neither providers nor runtime resources. `compare.py --native-diagnostics` adds a
separate Rust diagnostic pass and rejects diagnostic-enabled baseline profiles.

`compare.py --allocations` adds one separate heaptrack pass per selected client.
Install heaptrack explicitly on the measurement host first. Raw compressed traces,
SHA-256 hashes, tool versions, stdout/stderr, parser summaries, and workload
outcomes are retained. Native allocator calls include setup and harness costs;
JVM/Python managed object allocations are outside the intercepted native allocator
scope. The summary parser uses the
[heaptrack 1.5 output contract](https://github.com/KDE/heaptrack/blob/v1.5.0/src/analyze/print/heaptrack_print.cpp),
rejects missing or duplicate totals, and marks human-readable byte totals as
rounded. Profiled throughput/CPU is never included in baseline runs. These extra
passes publish records to the configured topic just like the baseline workload.

## Java and librdkafka comparisons

Use an existing local Kafka Java distribution and Python environment containing
`confluent-kafka`. The runner installs nothing and never creates or changes topics:

```sh
python3 kafka/benchmarks/compare.py kafka/benchmarks/profile.json /tmp/kafka-run-001 \
  --kafka-classpath '/opt/kafka/libs/*' --repetitions 3 \
  --environment /tmp/broker-and-host-provenance.json
```

The output directory must be new. Each implementation runs separately, with
rotating order across repetitions, identical deterministic payloads, offered
schedule, ack-all/idempotence, five requests, compression level, TLS/SASL settings,
target batching, and nominal input budget. A watchdog terminates the whole child
process group when a run exceeds its explicit wall budget. Failures and incomplete
runs remain in the manifest; they are never silently omitted from comparisons.
The result directory retains source/profile hashes, Git state, compiler/runtime
versions, CPU topology, affinity, memory/governor/kernel information, commands,
stdout/stderr, resource reports and operator-supplied broker/network provenance.
Never put credentials in the provenance JSON. SASL profiles name environment
variables; the runner never writes their values into generated properties.

Java uses `max.block.ms=0`. Kafka's pinned `KafkaProducer.doSend` reports
pre-admission `ApiException`s through a callback on the submitting thread; the
Java harness classifies those as rejection. Sender-thread callbacks are delivery
outcomes. Python catches `BufferError` directly from `produce`. Neither path
blocks waiting for a prior delivery. These behaviors follow the primary
[Kafka producer configuration](https://kafka.apache.org/43/configuration/producer-configs/),
[pinned Java source](https://github.com/apache/kafka/blob/7be741d08b3b06f6414ac868e57bf9b958f53a72/clients/src/main/java/org/apache/kafka/clients/producer/KafkaProducer.java),
[librdkafka configuration](https://github.com/confluentinc/librdkafka/blob/master/CONFIGURATION.md)
and [Python binding source](https://github.com/confluentinc/confluent-kafka-python/blob/master/src/confluent_kafka/src/Producer.c).
The actual installed Java/binding/native versions are recorded per run.

The Java driver also supports `producer_class=io.krkafka.producer.KrKafkaProducer`
in its properties file. Compare the copy binding with Kafka's Java producer using
the same bounded profile and explicit in-process warmup:

```sh
python3 kafka/benchmarks/compare.py kafka/benchmarks/profile.json /tmp/java-ffm-run \
  --clients java,ffm --kafka-classpath '/opt/kafka/libs/*' \
  --java-binding-jar kafka/kr-kafka-java/build/libs/kr-kafka-java-0.1.0.jar \
  --ffi-library target/release/libkr_kafka_ffi.so --warmup-records 1000 --repetitions 3
```

This pair explicitly uses 254 retries on both sides. The FFM driver selects the
profile's readiness/uring backend and launches with native access. It records the
binding jar and native library hashes. Warmup sends complete before every measured
interval; measured offers remain open-loop and rejected offers are not retried.
Warmup uses the measured profile's routing and payloads. Its complete serial phase
shares one `delivery_timeout_ms` budget; failure to finish that phase fails the run.
The process watchdog includes this additional budget, and results record warmup
elapsed time separately from the measurement interval.
Each Java result includes heap usage before/after, allocated Java bytes/rate, and
the FFM binding's configured resource budgets. `/usr/bin/time -v` records whole-
process peak RSS, including Rust allocations, alongside these separate counters.
Use `--allocations` for the separate native heaptrack pass; its timing is excluded
from baseline comparisons. Neither heap usage nor configured budgets are labeled
as exact Rust resident memory. The current copy binding has no native-buffer
optimization enabled; this switch does not establish any performance advantage.

Comparisons require whole-millisecond linger (default 1ms) and whole-KiB input
limits. Java's `buffer.memory` and librdkafka's queue budget do not equal the Rust
producer's separate hard input/output/codec/RX/control bounds; retain every
effective configuration and report whole-process memory as well. Java's metadata
path permits topic auto-creation, so the broker must set
`auto.create.topics.enable=false`. Java/librdkafka delivery errors remain
`failed_unclassified`; the harness does not invent Rust's stronger
`NotWritten`/`Unknown` distinctions. `skewed` uses the same big-endian keys with
murmur2-compatible routing; `unkeyed` intentionally measures each client's own
unkeyed policy. Explicit `hot`/`many` hints isolate batching behavior.

Run profiles for sparse load (e.g. 20 records/s), one hot partition, 16/256
partitions, 90% skewed keys, compressible/incompressible values, none/zstd1/zstd3,
250–1000us Rust-only linger sweeps, plaintext/TLS/PLAIN/SCRAM, and both native
providers. Provision separate slow-broker conditions and record them; the runner
does not change host networking or broker throttles. Repeat unchanged runs before
interpreting a regression. The librdkafka Python runner is an actual language binding.

`ffi_open_loop.py` measures our production C ABI through CPython/ctypes using
one record per `kr_submitv_copy` call. It uses the same bounded corpus, fixed offer
schedule, routing, security profiles and delivery observation thread as the
librdkafka Python runner. Immutable Python payload owners remain live during
each GIL-releasing call; only the copy path is used. Rejected offers are counted
once. Duplicate/unaccepted deliveries and Closed before delivery fail the run.
Rust creation copies the configuration before its temporary Python owners are
released. Topic readiness is outside the measured interval. Destroy time is
reported separately; the comparison process-group watchdog also bounds a stuck
destroy, preserving its normal input ownership contract.

```sh
RUSTC_WRAPPER= cargo build --release -p kr-kafka-ffi
python3 kafka/benchmarks/compare.py kafka/benchmarks/profile.json /tmp/kr-ffi-run \
  --clients rust,ffi,librdkafka --ffi-library target/release/libkr_kafka_ffi.so
```

The ABI layout definitions are shared with the C/Python conformance fixtures;
the runner never calls their test hooks. Its report hashes the loaded library
and records the Python/ABI version. CPU and RSS include ctypes, Python objects
and the bounded admission timestamp map. Internal producer telemetry is currently
unavailable through this ABI and is reported as such. These runs measure the
cost of single-record copy calls; bulk/native/foreign paths need separate
measurement before drawing conclusions about those input paths. No native
binding measurement has been executed yet.

Primitive checks require no broker:

```sh
cargo test -p kr-kafka-bench
cargo clippy -p kr-kafka-bench --all-targets -- -D warnings
python3 -m unittest discover -s kafka/benchmarks -p 'test_*.py'
# Add real-library configuration checks (no broker/startup is attempted):
KR_BENCH_FFI_LIBRARY=target/debug/libkr_kafka_ffi.so \
  python3 -m unittest discover -s kafka/benchmarks -p 'test_*.py'
javac -cp '/opt/kafka/libs/*' -d /tmp/kr-bench-java kafka/benchmarks/JavaOpenLoop.java
java -cp '/tmp/kr-bench-java:/opt/kafka/libs/*' JavaOpenLoop --self-test
```

`integration.py` prepares an isolated local KRaft broker, test certificates, and
plaintext/TLS/SASL profiles from a caller-provided distribution. Its `run` command
executes small native-producer smoke cases. It is an integration gate, never a
performance result. Existing directories are rejected and no downloads occur.

```sh
python3 kafka/benchmarks/integration.py prepare /tmp/kr-kafka-gate --kafka-home /opt/kafka
# Review server.properties, certificate.cnf, and the five security profiles first.
python3 kafka/benchmarks/integration.py run /tmp/kr-kafka-gate
```

The run creates a private fixture CA and loopback-only broker, formats only its
new data directory, exercises both backends with none/zstd and all five security
modes, and independently checks the total committed offsets with Kafka's own
tool. It records every result and stops its own broker process group on exit.
The fixture's public test password and two-day certificates are isolated test
credentials. A passing smoke run does not establish multi-broker replication,
leader-failure behavior, or security-negative cases against a real broker; those
remain separate integration gates.
