# Producer experiments

This crate runs generated producer workloads through `kr-kafka-sim`, checks the
complete audited history, and derives bounded presentation artifacts. It is
separate from the finite correctness campaign. Most `Size::Test` fixtures use
explicit load phases and at most 512 offers; the continuous admission trial uses
up to 16,384 to retain its progress checks. `Size::Full` allows up to one million. Full
performance trends are observations until a concrete seed/configuration and
measurement interval have been characterized.

The [classic Java comparison](CLASSIC_COMPARISON.md) runs the complete catalogue
against classic `KafkaProducer` and a Java/Panama native simulation adapter using
the same Rust broker, byte streams, fault engine and record generator. It records
the configuration differences and checks complete replay and fault exposure for
each pair.

`run_scenario` replays the realized manifest before checking invariants and phase
coverage. `derive_checked` also accepts an already executed `RunReport`, for
callers that persist complete replay evidence separately from chart data.

The `kr-kafka-experiment/v1` report contains one run. Chart time coordinates are
bounded nanosecond offsets from `meta.origin_ns`; absolute times, seed, record
IDs, epochs and signed Kafka offsets are canonical decimal strings. Topology
uses immutable UUID/partition identities, including HDR scopes across topic
recreation. No viewer needs the full event log.

Counts, quantiles, request attempts, histograms and the population ECDF use the
complete history. Record columns retain at most 65,536 rows, selected at ranks
`floor(i * population_count / count)` in ascending record-ID order. The row cap
is halved if necessary to keep serialized output within 5 MiB. Aggregates are
never reduced to make the report fit. A bounded ECDF selects at most 1,000 exact
cumulative ranks and declares whether it was reduced. Refused offers have no
acceptance, delivery, route or offset, and are distinct from NotWritten outcomes.

Request dispatch, full-write confirmation, response visibility and retirement
are separate observations. RTTs pair by connection/correlation and explicit
request identity; early responses have no full-write-to-response RTT. Producer
ledger attempts and observed Produce dispatch counts remain distinct. Wire bytes
count successful local write completions, including control traffic and retries;
first-dispatch batch bytes deduplicate immutable batch cohorts. Credit series
are observed maxima and last observations, not continuous occupancy.

Imports validate dimensions, exact totals, complete versus sampled populations,
nullable fields, immutable identities, decimal precision, quantile/ECDF order,
HDR equivalent ranges and diagnostics, and serialized/aggregate size caps.
Bundles are paginated at 32 runs or 48 MiB, whichever is reached first. Source
versions and replay verification remain visible in each run.

Generated HTML pages introduce the purpose of each experiment and explain the
loaded run's workload, parameter sweep, producer/transport settings and timed
faults before the charts. The setup follows primary-run selection, including
smaller Test workloads, and calls out closed-loop source feedback and conditional
delivery percentiles when interpreting quiet partitions.

```sh
RUSTC_WRAPPER= cargo test -p kr-kafka-experiments
RUSTC_WRAPPER= cargo clippy -p kr-kafka-experiments --all-targets -- -D warnings
```

## Baseline fixtures and measurement bounds

The eight baseline families cover outstanding/in-flight depth, offered rate,
linger/sparse sealing, fanout/key skew, compression/corpus, bursts, a distant
broker, and admission under partition skew. Full finite closed loops use 4,096 records. Test closed loops use 128–512
records; Test open loops retain the rate and shorten to 512 offers. Bursts use
three phases at 0/2/4 seconds in Test versus ten in Full; the 800-record Test
variant uses 128-record bursts and 16 descriptors to retain admission pressure.
Every load phase must contain actual offers. All accepted baseline records must
acknowledge; closed loops must not permanently refuse. Keyed fanout checks each
accepted route against its generated key and hash.

The open-loop family deliberately has 64 descriptors in every variant.
Measure each current source/seed combination independently.

Reproduce a pilot (complete replay/checkpoint/chart sidecars are retained):

```sh
RUSTC_WRAPPER= cargo build --release -p kr-kafka-experiments --example profile
/usr/bin/time -l target/release/examples/profile baseline.open-loop-rate rate32000 full --replay
```

Records that fail before topic/partition resolution have a null route. The
partition chart includes `unrouted_not_written` as a separate bucket series;
placeholder UUIDs and the producer's `-1` partition sentinel are not topology
identities. Routed and unrouted counts together reconcile with exact totals.

## Hard-failure fixtures

Eight families provide 26 variants: closed/open crash recovery, leader failover,
bootstrap order, five-broker rolling restart, short/long outages, flapping, and
Close during an outage. They opt into broker crash isolation, so a pre-append
service cannot commit through a crash. Full sustained sources use 4 ms broker
service and reserve 900,000 offer IDs. An independent four-outstanding healthy
source spans each band, preventing a blocked primary source from eliminating all
other-broker traffic. Its separately calculated offer envelope preserves the
aggregate one-million limit. Unused reserved IDs are reported as cancelled.

Test fixtures preserve the Full fault/control timestamps and distribute bounded
finite cohorts before, during and after each window. The open-crash Test fixture
has 32 descriptors and a 256-offer during-band source to prove pressure. Flapping
uses three four-record phases per window, plus warmup/final cohorts, staying below
both 512 offers and 64 sources. Close fixtures reserve a future cohort and prove
that Close cancels it without offering it.

Every window has exact pre/during/post evidence, with selected IDs. Checks require
pre-fault offers/acks, active during-band admission, affected connection/setup
attempts, no isolated-broker appends, progress at another broker, and post-band
traffic. Single-endpoint bootstrap and an early Close explicitly mark recovery
admission as not applicable. A bootstrap fallback's second cohort starts at 5.1 s.
Moved-leader fixtures require the pre-move broker-1 cohort to acknowledge through
broker 2 before broker 1 returns at 25 s.

The long-outage expiry fixture observes the producer's terminal sequence failure.
Its Full primary source runs through 6 s (outage 5–13 s), then bounded probes at
13.01/13.02 s observe NotWritten results after recovery. It does not spin an
unbounded closed loop against the failed producer. The short outage and long
Close deadline require every accepted record to acknowledge. Expiry cases require
a fully written client request crossing the outage to resolve Unknown and a
separate during-band cohort that never dispatched to resolve NotWritten. Every
Unknown must have independent possibly-applied client-attempt evidence; a complete
broker request is not required.


## Soft-failure fixtures

Twelve families provide 40 variants: pre-append delay/ramp, directional link
outages, probabilistic drops/disconnects, throttling, retriable errors, slow
setup, completion jitter, tiny transport chunks, and metadata loss during a
leader move. Full windowed fixtures sustain the primary source until two seconds
after the band, with a separate healthy source. The ramp remains active from
5–45 seconds. Finite jitter/chunk fixtures use 4,096 records in Full and 128 in
Test.

Test window fixtures use six 16-record cohorts around the unchanged Full fault
boundaries. The ramp adds 16-record probes at 15/25/35/44 seconds. Random loss
uses 32 offers/s for ten seconds plus a recovery cohort; disconnects use 64/s
inside the five-second band plus warmup, healthy and recovery cohorts. The
return-link fixture adds eight records one millisecond into the band to retain
append-before-response coverage. None of these tests shortens fault duration.

Exact phase evidence counts matching broker/API/phase opportunities separately
for every rule. Deterministic rules must fire on every opportunity; probabilistic
rates are observations with explicit denominators, and replay validates the
individual draws and firings. Link evidence includes a request already active at
the start boundary, a new during-band dispatch, or a failed setup; unavailable
links need not permit new Produce dispatches. Return-link outages require new
commits while response visibility is held. The throttle check pairs the fault's
connection/correlation with client response consumption, then excludes subsequent
Produce dispatches on that connection until the throttle expires. Requests
already dispatched can still reach the broker during the embargo. Mutation tests
remove hook/commit witnesses and inject a dispatch inside a consumed throttle.


## Topology and resource fixtures

These eight families complete the original catalogue of 35 families and 128 variants.
Topology fixtures cover thirty leader rotations, expansion from six to twelve
partitions, deletion/recreation with immutable IDs, and independent traffic to two
topics. Test rotation uses four records on either side of every unchanged move,
plus warmup/final cohorts; Full remains active through 62 seconds. Every rotation
phase requires acknowledgments from the moved partition through its new leader.
Expansion uses a post-control open source long enough to observe the new partition
count at either metadata age. Recreation explicitly closes the old client handle
before reopening; the second cohort must acknowledge under the new UUID. Without
reopening, it must produce TopicDeleted NotWritten evidence.

The two-topic fixture assigns topic A to broker 1/lane 0 and topic B to broker 2/
lane `1 % lanes`. Both sources remain independent. Test uses four 32-record phases
per topic; Full sustains sixteen outstanding records per topic through 17 seconds.
Healthy-topic progress is required during the 200 ms delay band. The suggested
latency ratios remain observations rather than universal lane-isolation gates.

Admission overload uses 256 descriptors, a 256 KiB input pool, and simulated
encoding of 2 KiB per millisecond. Eight warmup records precede a 32,000/s source
at 200 ms, lasting 15 ms in Test and ten seconds in Full, followed by sixteen
recovery records. A 512-byte value targets descriptor pressure; 2 KiB targets
input bytes. Metadata age is sixty seconds so this steady-topology measurement
does not mix input admission with metadata publication. Bootstrap under input-pool
saturation remains a separate requirement to verify in `../../AGENTS.md`.

Refusal proofs pair each failed synchronous admission with its immediate credit
snapshot. Selected witnesses independently show requested credit exceeding global
availability; additional refusals can reflect lane fairness. Input requests
include copied keys, values, identity headers and retained header metadata, so a
byte-pool refusal need not occur at exact capacity. Recovery must acknowledge
records after the offered load ends.

The polling pause is always 10–12 seconds. Full offers 2,000/s for fifteen seconds
with 64/1,024 delivery-event slots; Test uses 16/128 slots and 448 offers during the
pause plus warmup/recovery cohorts. Descriptors are capped at event capacity where
required by configuration validation. Checks exclude every consumed client event
inside the pause, prove continuing offers and event-credit exhaustion, and require
post-pause drain. The delivery-timeout family constructs separate written and
never-dispatched expiry cohorts for the two-second deadline; six/thirty seconds
must acknowledge all accepted records. Wire-window variants check complete
observed Produce byte obligations per connection through request completion.


Reports list exact generated source settings and scenario-specific Test changes,
including adjusted capacities. Driver configuration includes encode work/cost and
pipe size. Full-only performance comparisons do not apply to Test fixtures.

## Partition descriptor admission trial

The descriptor admission trial adds two families and 18 variants, bringing the
catalogue to 37 families and 146 variants. It compares
`Shared` admission with an opt-in `PartitionPressure` policy during the original
broker outage, under independent demand to all six partitions, and under hot,
90%-skewed and sparse 1,024-partition workloads. The sparse workload has six active
destinations and 64 descriptors. Large-topology charts use fewer time buckets to
retain the 5 MiB artifact bound; gates use complete histories. Heatmaps paginate
at 64 rows so every destination remains reachable within the canvas size limit.

Enable the policy in the Rust producer configuration:

```rust
use kr_kafka_producer::config::{DescriptorAdmissionPolicy, ProducerConfig};

let config = ProducerConfig {
    descriptor_admission_policy: DescriptorAdmissionPolicy::PartitionPressure,
    ..ProducerConfig::default()
};
```

With capacity C, total descriptors held H, and descriptors held by the candidate's
destination P, admission requires `H < floor(3*C/4)` or `P + 1 <= C - H`, in addition
to all existing resource checks. The policy requires at least four descriptors.
Ready explicit destinations and built-in keyed routes have separate accounts;
built-in keys retain their admission-time partition across metadata expansion.
Unresolved, unkeyed and custom owner-selected routes share one unclassified
account until settlement. Idle destinations have no reservation. A single busy
destination can leave 25% unused, and this policy supplies no universal starvation
guarantee or isolation for input bytes and terminal producer failures. `Shared`
remains the default; Java and FFI configuration surfaces are unchanged.

The independent Test fixture retains the entire 10–13 second fault, offers from
9.8–13.2 seconds at a capped total 1,000/s, and uses 32 descriptors. Skew Test
fixtures shorten the steady source to 0.2–0.4 seconds while retaining offered
rates, topology and capacity. Full comparisons use seeds 0–15 for both crash
sources and seed 0 for skew. The report records healthy refusals, every healthy
partition's 100 ms acknowledgment windows, recovery and the fixed 95% skew
throughput gate. Failed gates remain visible.

```sh
python3 scripts/rerun-admission-trial.py
python3 kafka/kr-kafka-experiments/analysis/render_admission_trial.py \
  --baseline-inventory target/experiments/availability-analysis/rendered/availability-runs.csv
```

The rerun script executes all 146 Test and Full variants before the 204 paired
Full measurements, saves fresh replay manifests and checkpoints, and exports
seed-0 pages. To run just the original failure with the policy:

```sh
RUSTC_WRAPPER= cargo run --release -p kr-kafka-experiments --bin kafka-experiments -- \
  --scenario hard.crash-restart-open --size full --descriptor-admission partition-pressure \
  --out target/experiments/admission-pressure
```

## Command line and saved replay

```sh
RUSTC_WRAPPER= cargo run --release -p kr-kafka-experiments --bin kafka-experiments -- --list
RUSTC_WRAPPER= cargo run --release -p kr-kafka-experiments --bin kafka-experiments -- \
  --scenario hard.crash-restart-closed --variant k64-slow0 --size full --out target/experiments/crash
RUSTC_WRAPPER= cargo run --release -p kr-kafka-experiments --bin kafka-experiments -- \
  --all --size test --out target/experiments/test-suite
RUSTC_WRAPPER= cargo run --release -p kr-kafka-experiments --bin kafka-experiments -- \
  --replay-dir target/experiments/crash
```

Generate the complete Full catalogue and replay-verified availability tables:

```sh
cargo run --release -p kr-kafka-experiments --bin kafka-experiments --locked -- \
  --all --size full --seed 0 --out target/experiments/full-suite
cargo run --release -p kr-kafka-experiments --example analyze_availability --locked -- \
  target/experiments/full-suite target/experiments/availability-analysis
python3 -B kafka/kr-kafka-experiments/analysis/render_availability.py \
  target/experiments/availability-analysis
```

The tables and report are written to `availability-analysis/rendered/`. Keep run
artifacts under `target/` and inspect them before adding any to Git.

The default is seed 0 and Test size. `--help` lists independent sweep overrides,
including in-flight, lanes, linger, backoff, request/delivery timeouts, compression,
rate, outstanding count, batch target, wire window, value bytes and metadata age.
Actual parameters are recorded in each report. An override can invalidate a timed
witness or characterized comparison; required checks remain enabled. Full runs
accept `--no-replay`, explicitly recorded in both index and reports.

Each invocation writes an index for its selected runs and retains previously
written sidecars. Run files are `<scenario>/<variant>-seed<N>.json`, with matching
`.replay.json`, `.checkpoint.json` and, on failure, `.failure.json` sidecars.
Scenario bundles are paginated by 32-run and 48 MiB bounds. The index includes
phase/invariant results, versions, summaries and comparison outcomes, and is
updated after every run. Failed runs do not stop the remaining `--all` cases;
the command returns nonzero after persisting available evidence. Buffered,
size-capped writes replace artifacts only after serialization succeeds.

`--replay-dir` accepts an indexed output directory and defaults to writing its
results into `DIR/replayed`. It loads the saved realized manifest and decision
tape under the harness's version/source checks, compares the terminal checkpoint,
and checks the derived report against the saved report. It never rebuilds a
scenario from its seed. A saved unverified Full run can become replay-verified.
Imported paths must remain inside the input directory; index imports are bounded
at 4 MiB and 32 runs per scenario. Complete replay JSON has its separate 1 GiB
bound. CLI tests cover checkpoint/tape corruption, exact saved replay, explicit
unverified runs, failure-history persistence, path escape rejection and atomic
output on serialization failure.

## Bundled diagnostic sample

`RUSTC_WRAPPER= cargo run --release -p kr-kafka-experiments --example generate_producer_experiment`
regenerates `tools/trace-tool/producer-experiment-data.js`. This replayed 48-record
diagnostic covers a 50–80 ms broker crash, concurrent healthy progress, and recovery,
with 10 ms global/broker HDR intervals. It is distinct from the Full catalogue.
Its test pins repeated output, phase evidence, and the 150 KiB presentation cap.

## Browser viewer and self-contained gallery

```sh
open tools/trace-tool/producer-experiment.html
RUSTC_WRAPPER= cargo run --release -p kr-kafka-experiments --bin kafka-experiments -- \
  --export-html target/experiments/html --out target/experiments/full-suite
open target/experiments/html/index.html
```

`--export-html DIR` exports an existing indexed directory selected by `--out`
(default `target/experiments`). It can also follow `--scenario`, `--all`, or
`--replay-dir` to export their results. Each scenario gets `<id>.html`; additional
pages use `<id>-page2.html`, with local previous/next/gallery links. Pages contain
all scripts, styles and validated report data and require no server or network.
The gallery groups scenarios by category with exact per-run counts and delivery
sparklines. Replay sidecars remain separate from presentation pages.

Exports rebuild bundles from the individual reports, preserving exact decimal
round trips for floating-point means. Data uses HTML-safe JSON encoding; titles
and gallery text use HTML escaping. Seven source assets, including the page
template, are SHA-256 pinned in `src/export/asset-hashes.json`. After reviewing an
intentional asset change, refresh its pins with
`python3 scripts/pin-producer-experiment-assets.py`; `--check` only verifies them.
Asset provenance and imported-text escaping are separate checks.

Run `RUSTC_WRAPPER= ./scripts/check-producer-experiment-viewer.sh` for sample,
model, shared UI, DOM execution and export checks. The viewer has aligned time
charts, fault-window brushing, run overlays, topology, broker/partition activity,
credit pressure, HDR intervals, exact summaries and explicitly sampled record
inspection. See `tools/trace-tool/README.md` for controls and measurement origins.
