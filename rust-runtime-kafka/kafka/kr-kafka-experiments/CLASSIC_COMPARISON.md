# Classic Java producer scenario comparison

The classic `KafkaProducer` and the native `ProducerClient` now run every entry
from `kr_kafka_experiments::catalogue()` under one Java workload driver and one
Rust simulation environment. The catalogue currently contains **37 families,
146 variants**, with both Test and Full sizes. Java obtains the catalogue and
builds each manifest through Panama; it has no second list of scenario parameters
or reimplementation of the broker faults.

This tests producer behavior under the same offered workload, topology, transport
and failure schedule. Simulated latency is meaningful; elapsed host execution
time is not a producer performance comparison. JSON downcalls and evidence
recording are test infrastructure costs.

## View Java and native differences

Generate a new matched run before exporting comparisons; historical galleries
and campaign measurements are excluded from this import.

The repository's `producer-experiment.html` explorer now accepts a paired
presentation artifact as well as ordinary native experiment reports. Export the
completed runs without rerunning either producer:

```sh
python3 -B kafka/kr-kafka-experiments/analysis/classic_visualization.py \
  target/classic-scenarios/full-review/baseline.* \
  target/classic-scenarios/full-review/hard.* \
  target/classic-scenarios/full-review/resources.* \
  target/classic-scenarios/full-review/soft.* \
  target/classic-scenarios/full-review/topology.* \
  --out target/classic-scenarios/full-review/analysis/viewer
```

The gallery lists the matched pairs from the selected run.
Choose a row to open both producers on the same chart scales and time axis.
**Classic Java is blue/solid; native through Panama is orange/dashed.** The
variant/profile selector changes a matched pair together. Start with `common`
when it is available. Fault-exposure gaps and native-only controls remain visible
above the plots.

Use **Focus fault windows**, drag the fault strip to zoom, or set the shared
start/end times. Hover a time chart for synchronized values. Select p50, p90,
p99 or maximum acknowledgment latency, and choose an immutable topic/partition
to inspect its delivery progress. Shading marks fault/polling windows; dotted
ticks mark scheduled topology and lifecycle changes. Expand **Experiment setup**
for actual traffic, destinations, timing and configuration.

Pages embed the same reviewed model, renderer, CSS, time controls and chart
primitives used by the existing explorer. They need no server or network assets.
Each page also has a `.json` artifact that can be loaded with the existing
explorer's file control. Raw `comparison.json` remains the analysis summary;
the exporter derives the timeline data from complete per-run evidence.

Paired pages show whole-run counts/deltas, acknowledgments, offers, admissions,
refusals, accepted-record failures, outstanding work, exact bucket latency
quantiles, a population ECDF, Produce frames received by the broker, committed
records, local client wire bytes, and per-partition acknowledgments. Counts use
the full population. Refusals are placed at original offer time, including a
closed-loop candidate cancelled before admission. Quantiles use consumption time;
failed or refused records are excluded from acknowledgment latency.

Java failure does not imply native `NotWritten` or `Unknown` certainty; its
exception class is preserved in the reason table. Native pool occupancy, attempt
counters and HDR owner metrics are omitted from the paired charts because they
are not observed on an equal basis for Java. The original native explorer keeps
those features for ordinary native reports.

The new `kr-producer-comparison/v1` artifact has at most 16 pairs per page and
320 shared buckets per pair. Width is `ceil(max(realized durations) / 320)` ns.
All displayed counts, relative times and dimensions have explicit safe numeric
bounds; full-width seeds and absolute origins remain decimal strings. ECDF
drawing retains at most 512 exact cumulative-rank points; whole-population
quantiles are not sampled. The exporter revalidates full histories and artifact
hashes, and the browser independently rejects inconsistent populations, bucket
geometry, timing, identities and exposure claims.

Future paired runs export an `out/viewer/index.html` gallery automatically.
Use `--no-viewer` on `run-classic-scenarios.py` to skip that step. To rebuild
selected pages, the exporter accepts `--scenario` as a whole-string regex.

The checked recovery sample can be regenerated with:

```sh
python3 -B kafka/kr-kafka-experiments/analysis/classic_visualization.py --sample
RUSTC_WRAPPER= bash scripts/check-producer-experiment-viewer.sh
```

Its complete source evidence is retained in
`tools/trace-tool/fixtures/producer-comparison-source.json.gz`; the generator
rechecks both runs and byte-pins `producer-comparison-data.js`. The visualizer
checks cover exact integers, corruption rejection, standalone HTML escaping,
normal/paired loader switching, native controls, sparse latency markers, and
desktop/narrow DOM rendering. A real-browser visual check remains to be done:
the browser automation URL policy rejected the local-file preview in this session.

## Run

Requirements: the Rust workspace toolchain, JDK 25 on `JAVA_HOME`/`PATH`, Python
3.11+, and this Kafka checkout containing `:clients-dst:classicScenarios`. The
orchestrator uses the existing Gradle dependency cache with `--offline`.

From this workspace:

```sh
python3 -B scripts/run-classic-scenarios.py \
  --out target/classic-scenarios/test
```

The default runs both adapters and both profiles, replays every run, and checks
each classic/native pair. The Java producer and the test-only comparison driver
come from this repository. `--kafka` can select another checkout containing the
same `clients-dst:classicScenarios` task; the selected revision is recorded in
the run provenance.

Select a scenario/variant with Java regular expressions (whole-string matches):

```sh
python3 -B scripts/run-classic-scenarios.py \
  --out target/classic-scenarios/full \
  --size full --profile original \
  --scenario 'baseline.open-loop-rate' --variant 'rate32000' --seed 0
```

`--adapter classic|native|both`, `--profile original|common|both` and unsigned
64-bit `--seed` are supported. `--skip-build` uses an existing release library;
provenance explicitly records that the invocation did not build it. Use a fresh
output directory for a new selection: the analyzer checks every report in that
directory, including previous runs.

## Shared execution contract

| Input or operation | How equality is established |
| --- | --- |
| Scenario, variant, size, seed | Rust builds the original manifest once per execution; paired effective manifests must hash identically. |
| Payload, headers, timestamp, record ID, key distribution | Both adapters call the existing Rust record materializer. Every stored record is checked against a regenerated recipe, including all payload bytes and headers. |
| Open-loop arrivals | One Java driver uses `start + floor(index * 1e9 / rate)` nanoseconds. Admission never blocks the source. The checker reconstructs every due time and ID. |
| Closed-loop arrivals | Same outstanding depth, offer budget, admission retry delay and end time. Delivery consumption releases source credit. Counts can differ because completion feedback is part of the experiment. |
| Broker state and protocol | The existing `BrokerModel`, record decoder, sequence checks and broker service process real Kafka frames from either client. Future ApiVersions probes receive the standard unsupported-version negotiation response. |
| Transport | The same bounded `SimNetwork` byte streams, directional pipes, partial writes, chunk sizes, latency/jitter and link profiles. Classic sends are serialized per connection. |
| Faults and topology changes | The same seeded fault engine and exact nanosecond schedules. Crash fencing and append/response hooks are shared with the ordinary Rust runner. |
| Time and scheduling | One owner thread drives `SimRuntime`. Java sender turns, source deadlines and network completions advance simulated time; no broker sockets or producer background threads run. |
| Native Java interop | A test-only Panama adapter invokes the real native producer actor/client in this same simulator. It exercises Java-to-Rust calls, admission and event polling. |

The native adapter is **not the production `KrKafkaProducer` FFM facade**. That
facade currently creates a host owner. Running its public API in simulation would
need a separate owner/clock injection contract. The new bridge demonstrates the
Java/Panama simulation path without changing the production ABI.

## Profiles and remaining differences

`original` preserves every original manifest field and every scenario ID. Native
parameters with no Java counterpart remain visible. `common` applies these
explicit adjustments to *both* effective manifests:

- One native lane; fixed lane selections map to lane zero.
- Sparse-rate linger bypass disabled and shared descriptor admission selected.
- Native simulated encoding cost reduced to its required minimum of 1 ns per
  encoding quantum. Java encoding has no separately modeled CPU cost.
- Broker-hook `Drop` becomes `Disconnect` at the same hook, probability and time
  window. Dropping an entire response while keeping an ordered TCP connection
  alive can make Java see a later correlation ID where it expects the first.
  Original mode keeps that behavior and records the sender errors.
- For `soft.metadata-loss-during-move`, the common metadata outage covers all
  brokers. Java and native metadata node selection differ; a broker-1-only fault
  can be entirely avoided by Java.

Every report contains the original manifest, effective manifest, adjustments,
and exact Java configuration. Common mode improves comparability of the shared
mechanisms; it cannot make all producer internals equivalent.

| Knob/semantic | Java mapping or comparison limit |
| --- | --- |
| Request/delivery/setup timeout, linger, metadata age, retry/reconnect backoff | Mapped to Java configuration in milliseconds, rounded upward. Fault/control clocks retain nanosecond precision. |
| Attempts | `retries = max_attempts - 1`; retry jitter and sequence recovery algorithms remain implementation behavior. |
| Batch/request bytes, compression | Same target/cap and zstd level. Packing, framing overhead and compression heuristics can differ. |
| Admission | `max.block.ms=0`. Unresolved metadata or buffer pressure is an immediate refusal; closed-loop sources retry pressure with the original delay. Open-loop refusal populations include startup. |
| Memory | Java `buffer.memory` uses the native input-byte capacity numerically, but includes different allocations. It does not model the descriptor pool. |
| PartitionPressure, lane, wire-credit sweeps | Java has no corresponding native policy or pool. All workloads execute; those parameter contrasts are identified in `comparison_limits`. |
| Stopped polling | Both pause application consumption and closed-loop feedback. Java callbacks still run and release producer buffers; native delivery-event capacity can stop admission. |
| Topic retirement/recreation | The Java workload stops offers while explicitly retired and resumes on reopen. Java uses names; it has no native topic-handle UUID fence. |
| Flush/close | Scheduled flush starts each client's flush barrier without blocking the source. Scheduled close uses the same deadline, then Java's real sender forced-close path. Native certainty reasons remain available separately from Java exceptions. |

These differences are results to inspect, not reasons to equalize accepted counts,
batch sizes, request counts, or failure outcomes. In particular, equal seeds do
not mean equal request-triggered random draws between implementations: their
request streams differ. Replay equality is required *within* each implementation.

## Evidence and checks

Every run retains all admissions, callbacks/consumed deliveries, broker records,
fault decisions and scheduled controls. The checker verifies:

- Exact offered/accepted/refused/terminal populations and unique record IDs.
- Stored payloads, acknowledged routes and offsets, no duplicate appends, and
  per-topic-UUID/partition append order relative to admission.
- All active loads, exact open-loop due times, scheduled controls, and no delivery
  consumption inside polling pauses.
- Complete fault-decision history against counters, matching opportunities and
  deterministic rule firings; no new appends through a crashed broker.
- Baseline accepted records all acknowledge; finite closed-loop baselines retry
  admission pressure.
- Exact replay of the complete report/history for Test, or complete artifact
  SHA-256 hashes for streamed Full evidence.
- Zero retained network connections, operations and byte obligations at teardown.

These are portable producer and environment checks. The existing Rust experiment
gates on internal credits, native certainty cohorts and fail-closed topic handles
remain in the original Rust suite; they are not claimed as Java assertions.
`comparison_limits` identifies the corresponding scenario mechanisms.

`comparison.json` contains a pair per scenario/variant/profile/size/seed, exact
manifest hashes, phase counts, fault coverage gaps, admission/delivery counts,
conditional p99 acceptance-to-consumed-ack latency, and maximum source lag.
Delivery timestamps must match the consumption trace exactly. Java's earlier
callback time is retained separately as `callback_ns`, including during pauses.
`fault_exposure_comparable=false` means one client missed a required fault
opportunity. It must not be read as an equivalent fault-effect trial. A true value
only certifies fault exposure; consult `comparison_limits` for semantic limits.

Full workloads above 16,384 reserved records stream external history, environment
history and deliveries into `.first/` and `.replay/` directories. Their report
contains absolute artifact paths and hashes. Keep both directories with the
report; relocating them requires updating the path references. The Python
analyzer verifies hashes then loads one run at a time, so large runs still need
substantial memory. Source populations and evidence are never sampled.

The orchestrator records both checkout SHAs, tracked-diff hashes, untracked file
hashes, native library SHA-256, arguments and Java version in `provenance-*.json`.
These identify the source state at invocation. They do not reconstruct uncommitted
source, so commit code before archiving a long-lived result.

For the complete Full matrix on a bounded disk, use
`scripts/run-classic-matrix.py --out <fresh-directory>`.
It runs all catalogue families under both adapters and both profiles, records
each job in `matrix.json`, and resumes completed jobs without rerunning them.
After a job exits, it compresses streamed first/replay/failure artifacts with
gzip and verifies their decoded SHA-256 before removing the uncompressed copy.
The original streamed report and an `archive-*.json` restoration map are retained.
Updated report references declare `encoding: gzip`; the analysis and viewer
readers verify the original decoded hash. Inline reports stay unchanged.
Simulation-library, Kafka revision/diff and catalogue identities must match on
resume. Nonzero execution results remain visible in the inventory; completing
the matrix does not turn a failed execution into a passing run.
