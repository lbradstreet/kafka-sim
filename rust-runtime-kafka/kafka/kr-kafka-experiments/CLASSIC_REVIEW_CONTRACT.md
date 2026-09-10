# Full producer comparison: review criteria

The campaign executes all 146 catalogue variants at Full size, seed 0, with
original/common profiles and classic/native adapters: 584 independently replayed
executions. Follow-up seeds may test whether important findings repeat; seed 0
alone does not establish a tail distribution or universal bound. The production
producer implementations stay unchanged during the review.

## Expectations

1. **Correctness:** acknowledged payloads/routes/offsets exist in the broker log;
   idempotent retries do not duplicate appends; partition ordering and terminal
   populations are consistent. These are executable checks, independent of speed.
2. **Bounded admission:** pressure is explicit and never blocks the open-loop
   source. Evaluate rejected demand as well as accepted-record latency. Equal
   byte knobs do not equalize Java buffer memory and native descriptor capacity.
3. **Failure isolation:** when independent demand continues to healthy
   destinations, one unavailable destination should not destroy their admission
   or progress. Global memory sharing and producer-wide fatal semantics can be
   valid documented contracts while still being availability weaknesses.
4. **Recovery:** after faults end or leaders move, inspect the previously pending
   cohort and later probes. A producer that settles everything by failure has not
   recovered. Configured backoff, setup, metadata and request deadlines are part
   of the explanation; the experiment does not prove a minimal scheduler delay.
5. **Deadline behavior:** compare terminal times against delivery/close settings.
   Java callbacks and application consumption are distinct. Paused polling
   prevents a consumption-based delay from proving a producer deadline violation.
6. **Retry and waiting cost:** inspect per-record broker-observed attempts,
   admission-to-first-observed-request wait, and wire bytes. The first broker
   observation includes transport and is not a measurement of client dispatch.
   Deeper pipelining should justify any amplified retries or multi-second waits.
7. **Fairness and useful work:** progress must be conditioned on pending or
   independently offered demand. Scheduled quiet periods are not outages;
   closed-loop source silence and aggregate percentiles can hide unavailable
   partitions. Compression simulation does not measure host codec CPU cost.

## Comparison discipline

Effective manifests must match within each Java/native pair. Review original
inputs and common-profile sensitivity separately. Common disables native lanes,
sparse linger bypass and PartitionPressure, minimizes modeled native encoding
cost, and normalizes some faults. Java has no equivalent descriptor/event/wire
credit pools or immutable topic-handle lifecycle.

Record exact failure reasons and fault opportunities. A missed fault opportunity
invalidates an equivalent-effect claim. Request-triggered fault draws can differ
between producers even with the same seed, because request structure differs.
All claims distinguish observed behavior, inferred causes, configured contract
tradeoffs, and issues requiring a controlled follow-up. No aggregate winner score
combines correctness, refusal rates and conditional latency.

## Reproduce the measurements

```sh
python3 -B scripts/run-classic-matrix.py \
  --kafka "$HOME/code/worktrees/kafka-classic-scenarios" \
  --out target/classic-scenarios/full-review --seed 0
python3 -B kafka/kr-kafka-experiments/analysis/classic_review_pipeline.py \
  target/classic-scenarios/full-review \
  --out target/classic-scenarios/full-review/analysis --watch
```

The second command can run while the matrix is active. It reads only finished,
archived jobs, checks each complete report, writes per-run exact measurements to
`analysis/runs/`, and exports the existing paired viewer to `analysis/viewer/`.
`analysis/runs.csv` and `analysis/inventory.json` cover the successfully audited
pairs. Execution failures and analysis errors remain explicit. Cache entries
include source and analysis-code hashes; changing either requires fresh analysis.

Pending no-ACK intervals start only when accepted work exists, split on successful
application consumption, and end separately on successful progress or final
failure settlement. Per-partition latency remains separate from those intervals.
Source gaps omit scheduled silence between separate loads. Fault-phase admission
counts describe the eventual disposition of offers made inside the phase;
delivery counts describe consumption inside the phase. Refused routes are
deterministic intended destinations. Accepted routes use broker evidence, with
admission-time routing as fallback for failures. Topology changes and overlapping
faults are considered when labeling destinations as targeted or healthy.

Broker-request record lists contain admission tokens, which must be translated
through `Accepted` events to workload IDs. The native request capture initially
decodes workload IDs, but `Audit::drain_requests` translates these to admission
tokens before recording `ClientRequestDispatched`; persisted dispatch events
therefore need the same translation. Dispatch times remain distinct from the
common broker-observation measurement. Successful records must occur in the
translated request population.
The 4,096-record keyed routing distribution is pinned independently, and every
stored record in fixed-topology runs is checked against the route oracle.

The deadline review lists callback/consumption delays exceeding the configured
delivery timeout by more than 5 ms, excluding intervals overlapping paused
polling. This is a conservative investigation signal, not a universal real-time
guarantee or an automatic client correctness verdict. Full source traces remain
available for the cause of each listed wait, retry and terminal failure.
