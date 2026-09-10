# Kafka producer simulation

This crate runs the production `ProducerActor` over `SimNetwork`. A connection actor decodes real Kafka requests and drives the separate `kr-kafka-broker-model`; replies pass back through the production framing, correlation, strict Produce13 negotiation, and response parser. It does not replace the engine, connector handshake, compression, input leases, or byte-stream operations with expected-result mocks.

Run the bounded release campaign with an independent process watchdog:

```sh
RUSTC_WRAPPER= cargo run --release -p kr-kafka-sim --bin kafka-sim --locked --offline -- --campaign --watchdog-ms 900000 --failure-dir /tmp/kafka-sim-failure
```

Run a seed or an exported failure manifest:

```sh
RUSTC_WRAPPER= cargo run -p kr-kafka-sim --bin kafka-sim --locked --offline -- --seed 36 --variant isolation
RUSTC_WRAPPER= cargo run -p kr-kafka-sim --bin kafka-sim --offline -- --replay /tmp/kafka-sim-failure/replay.json
```

`run_replayed` compares complete typed producer histories, coverage, and the runtime `DeterminismCheckpoint` across two ordinary untraced executions. `verify_trace_transparency` repeats with the bounded SBE recorder and checks that recording changes none of those observations. `run_and_retain_failure` reruns a failure with recording and writes `replay.json`, `producer-history.json`, and `runtime.sbe` only after runtime shutdown. Before each case, the CLI atomically writes `active-replay.json` outside simulation. The watchdog exits 124 on its independent wall-clock limit; that active manifest identifies an interrupted case even when a completed runtime trace is unavailable.

The versioned manifest contains every producer configuration field, driver settings, runtime/provider/model capacity limits, nonzero start epoch, initial broker/topic IDs and leaders, immutable record bytes and headers, workload operations, planned and realized faults, five named RNG-domain checkpoints, package/model/scenario/driver versions, the pinned Kafka revision/schema hash, and a SHA256 of participating source plus the workspace lockfile. A different version or source fingerprint is rejected. Parsed JSON is capped at 16 MiB and checked before creating the runtime or allocating configured pools. Replay consumes the persisted workload and realized fault schedule; it does not silently regenerate a newer workload from a seed.

Ordinary `cargo test` runs 12 pinned seed/variant cases, a small retained legacy corpus, and focused lifecycle, timing, fault-cut, replay and pressure regressions. The standalone `--campaign` gate unions those pins with the requested range (default `--seed-start 0 --seed-count 128`) and deduplicates cases. Each range seed runs clean, finite-fault and isolation variants plus the inexpensive legacy scenario. The CLI reports the actual case and run counts; Produce9 rejection adds one separate run. `--seed-count 0` runs only retained pins. CI uses a dedicated release job with a 15-minute process watchdog and a 30-minute job limit.

The new profiles contain 192 records across two topics and six rounds, varying 1–3 brokers, 1–8 partitions per topic, 1–4 lanes, 1–5 requests in flight, batch size, linger, compression, native/copy submission, key routing, short writes and service asymmetry. Independent salts select profile dimensions and command order. Each recovery round has one shared eight-second virtual-time deadline, including admission and both flush/settlement barriers; warmup has two seconds and close has two seconds. The complete run is additionally capped at 60 simulated seconds and two million scheduler steps. Finite variants use 100 attempts and 60-second delivery/resolution timeouts. Generation and replay validation reject profiles outside the conservative recovery envelope, which assumes one-byte positive transport progress rather than treating maximum chunk size as a minimum. Every valid submission must become Acked in these profiles; intentionally negative lifecycle and legacy scenarios use separate outcome contracts.

Fault rules match phase, API, broker, connection and round. Setup, ApiVersions, Metadata, InitProducerId and Produce share scripted skip/take budgets and Fault-RNG drops, delays and disconnects. Each round permits at most eight injected script/random actions. Per-broker service delay and time-windowed isolation are explicit environmental settings. Isolation closes live sockets at its start and refuses new connections until healing. A realized decision tape includes no-effect hooks and consumed Fault draws; replay applies recorded effects and checks hook order, draw accounting and budgets. Actual model log growth is recorded separately from reaching an after-append hook, so a lost committed response gate cannot pass on an error response.

The legacy corpus retains duplicate/sequence-error controls, strict Produce13 and Produce9 rejection, deletion/recreation under the same name, additive partition growth, cancellation, stopped event polling, linger/target seals, backpressure and native releases. Focused actor tests cover delayed topic creation, resolution timeout, closing one topic while another progresses, explicit reopen, UUID recreation, exact partition growth, all three request/append/response drop/disconnect cuts, idle isolation and setup crossing an outage. Broker-arrival timestamps check data and control backoff on virtual time. Flush checks use accepted-prefix fences, including a flush admitted before a later send.

C1–C12 are checked by the separate `DeliveryOracle` against accepted-prefix return values, drained events, parsed response proofs, and the broker log. IDs are stamped in nonempty keys/values and a dedicated header followed by a same-name null header; nullable and empty payloads remain distinct. Copied caller buffers are zeroed after their accepted prefix returns. Producer record tokens remain the event/FIFO identity. Acked delivery offsets, response timestamps and attempts are checked against independent parsed request/response witnesses and committed log positions; record CreateTime is checked separately. Committed records without an accepted token fail even if their bytes appeared in a rejected workload suffix. The harness additionally compares committed key/value/header/timestamp contents, prevents delivery route changes, checks every named credit pool's cumulative reserved/released counters and peak, independently counts outstanding delivery and native release obligations, and compares retained provider spans. Input-release events are the public last-owner witness; lower-level input and completion-guard tests directly check encoder/provider abandonment lifetimes. A stopped runtime retaining any producer or provider credit fails. The broker-model oracle suite mutates logs by dropping, duplicating, reordering, or rebinding records and mutates events by dropping deliveries or flipping outcomes; every mutation must be rejected.

```sh
RUSTC_WRAPPER= cargo test -p kr-kafka-sim -p kr-kafka-broker-model -p kr-kafka-producer --offline
RUSTC_WRAPPER= cargo clippy -p kr-kafka-sim --all-targets --offline -- -D warnings
```

The separate Fetch observation runs after each expanded producer profile closes. It uses the shared `kr-kafka-client` Fetch13 operation and connection driver over `SimNetwork`, with explicit UUID/partition/offset requests. The broker retains original committed wire batches; patching base offset and leader epoch leaves the CRC-covered bytes unchanged. The probe validates complete bounded record sets (none or zstd), compares every record with both immutable workload bytes and the independent decoded log, then advances its cursor. It fetches the final empty end offset too. Transport retries retain the last verified cursor; warmed drivers are retired and drained on errors. A corrupted later batch cannot validate an earlier prefix. The probe has a two-second aggregate deadline, and every expanded case must fetch all 192 records. This is a raw protocol operation and test probe, with no consumer, sessions, groups, offset management, long polling, retention or transaction support.

This is a portable simulation gate. It does not claim Linux epoll/io_uring execution or real-broker interoperability; those gates remain separate.

The experiment extensions retain a compatibility baseline for every `PINNED_CASES`
entry. `tests/fixtures/legacy-pinned.json` pins the complete runtime checkpoint
and SHA-256 of the legacy history, decisions, coverage and report measurements.
The projection removes only explicitly named additive lifecycle diagnostics and
renumbers history ordinals; request identities, timestamps, outcomes and fault
draws remain exact. Mutation tests check that semantic changes remain visible.
To capture full evidence before a deliberate compatibility change:

```sh
RUSTC_WRAPPER= cargo run -p kr-kafka-sim --example capture_legacy -- target/experiments/legacy-baseline
RUSTC_WRAPPER= cargo test -p kr-kafka-sim --test legacy_compatibility
```

The capture directory includes complete run reports and a compact `pinned.json`.
Do not refresh the committed baseline to make an unexplained regression pass.

Manifest v6 / history v4 / driver v4 resolve bootstrap endpoints against the
modeled broker list and reject ambiguous endpoints or conflicting broker IDs.
`ConnectionOpened` identifies the broker and lane when the transport pair is
created, including setup traffic. `ConnectionClosed` records broker-service
registration release exactly once; its reason is an ownership observation, not
an inference about the producer's retry policy. Fault decisions retain the
separate evidence for isolation, disconnect and setup failure.

General experiment validation permits at most one million offered records, eight
million history events, 200 million steps and 300 virtual seconds, with five
brokers, 65,536 workload entries and two million fault decisions. Broker log
ceilings are one million records/batches and 2 GiB of retained bytes; these are
caps, not startup allocation requests. Larger tapes grow on demand. Seed
generation stays at 2–256 records, and the finite profile explicitly preserves
all former execution, history, decision, broker, workload and broker-log limits.

`RecordTemplate` generates records lazily with round-robin, fixed or keyed
partitioning, fixed/partition-derived lanes, and compressible or deterministic
incompressible values. Full-width record IDs live in the existing identity
headers. Templates consume no runtime random draws. `SettleAllAccepted` freezes
the accepted-token prefix and waits only for its terminal deliveries; an empty
prefix completes immediately. `SleepUntil` uses an offset from manifest start.
Both operations are excluded from the finite profile. Native acquisition/commit
capacity failures now follow the normal bounded admission-pressure path, with
their partially acquired buffers released.

`ReplayManifest.experiment` selects the generated driver and requires an empty
legacy `workload`. Up to 64 concurrent load sources reserve disjoint ID ranges.
Open loops retain their absolute rational schedule; capacity refusal ends that
offer. Closed loops count a candidate once and retry its admission until progress,
its declared end, or the hard offer deadline. Exhausting a sustained load's ID
budget before its end fails explicitly. Polling transitions precede scheduled
controls (in manifest order), which precede offers at the same timestamp. A pause
continues load and controls while leaving client events unconsumed. Scheduled
reopens initiate resolution asynchronously. Timed close stops all sources and
resolves a waiting candidate as refused; unused reserved IDs are cancelled planned
traffic. Normal completion settles the accepted prefix and then closes.

`Offered`, `AdmissionAttempt`, `Refused`, `ScheduledControl`, `PollingChanged` and
`OffersStopped` make this accounting and ordering replayable. Offer due times and
actual observation times are distinct. Payload checking materializes one record
from its template range at a time, including full-width IDs, rather than retaining
a generated payload map. Legacy executions do not instantiate this driver.

Experiment `faults.links` configure per-broker directional propagation and
chunking; `faults.link_outages` select BlackHole or FailFast for ToBroker,
FromBroker or Both. These require the experiment timeline and are excluded from
the finite profile. The underlying driver's link latency and jitter remain
local-completion timing, separate from propagation. Queued and in-transit bytes
share the configured pipe capacity. Fixed half-open windows govern the provider
even when a callback runs before the workload driver at the same timestamp;
`LinkStateChanged` is inserted before any same-time history observation.

ApiVersions setup traverses the same delayed stream and races the connector's
actual deadline. Fail-fast setup fails immediately; a black hole can recover or
time out without a synthetic maximum sleep. `SetupFinished.resolved_ns` marks
the negotiation/timeout decision, while its outer timestamp and `elapsed_ns`
include transport retirement. Normal idempotent dedup can return the original
successful offset, so the response-loss test proves a committed token cohort was
redispatched and acknowledged without duplicate appends; `duplicate_sequences`
counts the optional explicit duplicate-sequence error response separately.

`observe_requests` enables passive client request history and is excluded from the
finite profile. `ClientRequestDispatched` identifies first transport submission
for one attempt, including requests that never become a complete `BrokerRequest`.
The connection/correlation pair maps to a unique observation request ID; batch
IDs identify complete immutable topic/partition record cohorts across retries.
`ClientRequestWriteCompleted` requires successful confirmation of the full plan
across all partial operations. `ClientRequestFinished` marks response presentation
or cooperative retirement, with confirmed bytes and completion certainty.

`ResponseRead` still marks when a full response becomes visible to the client
transport, which can precede a delayed full-write notification. Dispatch-to-read
latency and full-write-to-read latency are separate measurements; the latter is
absent for an early response. Producer `Delivery.attempts` comes from its batch
ledger at `WriteAdmitted`; observed per-record dispatch counts come from Produce
wire plans. Reports retain both counters, while global request counts also include
control/setup traffic. Raw write operations remain byte/partial-write diagnostics.
The bounded capture sink retains no input leases or payloads after each callback,
uses an empty-queue hint and reusable drain buffers, and fails the run explicitly
on diagnostic overflow or incomplete request ownership.

`faults.environment` supplies unbudgeted rules selected by broker, API, phase and
half-open time window. Optional ramps interpolate delay with integer arithmetic.
Rules merge after fixed service/isolation/link behavior and before finite scripts
and random rules, in manifest order. Probability one uses no random draws; every
other matching probability uses exactly one Fault draw before the existing random
rule draws. Environment firings, including a ramp's zero-delay endpoint, have
separate indices and counters and never decrement either round budget. Replay
recomputes their effects from the declared rule, hook time and recorded draw.
Conflicting overlapping deterministic rules and unknown broker/time references
are rejected at validation. The finite profile excludes environment rules.

The `sustained_broker_and_send_pressure_grow_batches_before_new_requests`
regression warms one partition, then submits the same 63 additional 128-byte
records at one-millisecond intervals without an intermediate flush. It replays
three cases: an immediately responsive broker, a 50 ms broker with a one-request
window, and delayed 64-byte send completions with a five-request window. The
observed batch record counts are respectively `[5 × 12, 3]`, `[5, 30, 28]`, and
`[5, 14, 30, 14]`; the test asserts fewer requests, at least twice the maximum
batch size, more byte-target seals, and all 64 records acknowledged. Actual
request-credit peaks distinguish the full-window case from the delayed-send
case, which remains below its five-request window.

This follows the production decision path: `Batch::seal_due` receives
`ProducerEngine::has_dispatch_credit`, whose connection check requires no
unresolved write, room in the request FIFO, and room in the shared broker byte
window, with broker throttle respected. Linger waits when those credits are
unavailable, allowing the next open batch to grow. Reaching the byte target or
hard limit, explicit flush/close, and delivery-deadline allowance can still seal
under pressure; those are boundedness or explicit completion requirements.

`metrics_sampling: { interval_ns }` opts into an explicitly scheduled reader task.
Intervals must be at least 1 ms and the declared elapsed horizon must fit at most
1,024 snapshots including shutdown banks. Each tick requests publication; the
reader summarizes and recycles completed HDR banks outside the owner actor.
Samples retain actual start/end bounds, request/take times, immutable topic UUID
scopes, equivalent-value quantile ranges, exact maxima and all recording
diagnostics. Busy/NoSpareBank requests are counted explicitly. Shutdown stops and
joins the sampler, then drains published and terminal banks exactly once, even
when the run ends before its first tick. Global `metrics_counts` sum every bank.
Replay and trace-transparency checks compare samples and missed requests too.
Sampling introduces declared scheduling work; passive recording and request
observation themselves do not. The finite compatibility profile excludes sampling.

Complete experiment replay JSON has a separate 1 GiB byte envelope, retaining
all decision-count, source and version checks. Legacy manifests, including the
finite gate, retain the 16 MiB envelope. The 640,000-offer pilot needs about 66 MiB
for its complete realized tape; chart reports and browser bundles remain subject
to the experiment crate's independent 5 MiB and 48 MiB presentation caps.

For keyed admission, the oracle retains the independently computed expected
partition if routing occurs. A record that terminates before routing may carry
the producer's `-1` unassigned-partition sentinel. This is allowed only for an
admission without an explicit partition, NotWritten, zero attempts, and no
transmission, ambiguity or parsed response. Topic identity remains exact and any
commit still contradicts NotWritten. Explicit partitions never gain this
exception. Presentation reports keep these unresolved routes null.

`faults.crash_on_isolation` is an explicit experiment-only policy. Socket isolation
alone can leave an already received frame's simulated service running. Crash
isolation instead abandons pre-append work whose service interval intersects a
crash window, including work delayed past recovery. Declared half-open windows
are checked before append regardless of timer callback order.
`BrokerFrameAbandoned` identifies the connection, correlation and responsible
window; the independently tracked transmission remains potentially ambiguous.
Legacy socket isolation and pinned finite histories retain their prior semantics.
