# Descriptor admission trial

**Healthy admission/progress: passed. Skew throughput gate: failed.** The policy remains opt-in; its 75% shared-descriptor threshold was not tuned after measurement.

204 of 204 planned Full comparisons were analyzed from complete histories, with exact saved-checkpoint verification. Crash comparisons cover seeds 0–15; skew comparisons use seed 0. Every run uses fresh manifests and decision tapes.

## Results

| Gate | Result |
|---|---|
| complete inventory | passed |
| full catalogue regression | passed |
| frozen baseline reproduced | failed/incomplete |
| healthy admission and progress | passed |
| accepted records recover | passed |
| skew throughput within five percent | failed/incomplete |

The 96 analyzed pressure-policy fault runs contain 0 healthy outage refusals and 0 healthy 100 ms windows without an acknowledgment, out of 11,520 checked windows. Across all paired runs, 39,198,023 of 39,198,023 accepted records were acknowledged.

Seed-0 outage refusal counts below use intended destinations, including offers that were never admitted.

| Source | Offers/s | Shared healthy refusals | Pressure healthy refusals |
|---|---:|---:|---:|
| original | 1,000 | 792 | 0 |
| original | 4,000 | 7416 | 0 |
| original | 16,000 | 33872 | 0 |
| independent | 1,000 | 984 | 0 |
| independent | 4,000 | 6984 | 0 |
| independent | 16,000 | 31004 | 0 |

Steady acknowledgment counts use the exact 1.0–10.2 second interval, excluding warmup. The throughput gate requires at least 95% of Shared acknowledgment throughput at the same offered rate. Descriptor peaks are observed maxima, shown as Shared / Pressure out of capacity 64.

| Workload | Offers/s | Shared ACKs | Pressure ACKs | Ratio | Descriptor peaks | Gate |
|---|---:|---:|---:|---:|---:|---|
| hot | 8,000 | 73,596 | 73,596 | 100.000% | 33 / 33 | passed |
| hot | 32,000 | 146,772 | 110,072 | 74.995% | 64 / 48 | failed |
| skew90 | 8,000 | 73,604 | 73,604 | 100.000% | 38 / 38 | passed |
| skew90 | 32,000 | 144,008 | 113,128 | 78.557% | 64 / 54 | failed |
| sparse1024 | 8,000 | 73,604 | 73,604 | 100.000% | 46 / 46 | passed |
| sparse1024 | 32,000 | 133,233 | 107,435 | 80.637% | 64 / 53 | failed |

## Contract and limits

Admission shares the first floor(3C/4) descriptors freely. Above that point, a candidate with P outstanding descriptors needs P + 1 ≤ C − H, where H is total descriptor occupancy before the candidate. Ordinary global, lane, input and event-credit checks still apply. Charges last from acceptance to terminal settlement, including the time after encoding releases input. Delivery-event credits retain their separate lifetime. Records already accepted are never evicted to free capacity.

For example, the original-source 16,000/s seed-0 run refused record 161285 for partition 0 at 10.080250000 seconds. Its decision-time state was H=385, P=250, C=512: the next descriptor would require 251 ≤ 127. This stops the large outstanding class from consuming the remaining descriptors.

Ready explicit partitions and built-in keyed records use immutable topic/partition identities. Keyed records retain the partition selected from admission-time metadata. Unresolved, unkeyed and custom owner-selected routes share a conservative unclassified class until settlement. There is no per-partition guarantee inside that class.

Idle partitions receive no reservations. Accounting has at most C live classes, independent of configured partition count. A lone busy partition can leave 25% unused under pressure. Thousands of simultaneously blocked destinations can still fill a small pool; input-byte isolation, metadata saturation, retry amplification and producer-wide terminal recovery remain deferred.

The independent fixture divides total rate among six sources with quotient/remainder allocation and seed-rotated source order. Broker 1 owns partitions 0 and 3 and is isolated during 10–13 seconds. The sparse skew fixture has 1,024 configured partitions and six active destinations, 90% of traffic on partition 0. It retains 64 descriptors and one lane. Large-topology charts use coarser time buckets to stay within the unchanged 5 MiB report bound; acceptance gates use exact history.

Simulation models service and encoder work; these results do not measure host CPU or establish a universal fairness bound. The frozen availability review remains unchanged.

## Evidence and reproduction

Source fingerprint: `7de7cabd997ba827881d33a5d82c279279d081ca26bef8f83c3493fd6d85a605`. Manifest/history/driver/model versions: 7/5/5/3.

- [All paired run measurements](admission-trial-runs.csv)
- [Machine-readable gates](admission-trial-gates.json)
- [Decision-time pressure witnesses](admission-trial-witnesses.json)
- [Original-source Shared pages](../admission-shared/index.html)
- [Original-source pressure pages](../admission-pressure/index.html)
- [Independent-source pages](../admission-independent/index.html)
- [Skew and sparse-topology pages](../admission-skew/index.html)

The catalogue indexes contain 146 Test runs and 146 Full runs, with 128 and 128 original variants respectively. The catalogue gate requires every run to pass with replay verification and the same source fingerprint.

From the repository root:

```sh
python3 scripts/rerun-admission-trial.py
python3 kafka/kr-kafka-experiments/analysis/render_admission_trial.py --baseline-inventory target/experiments/availability-analysis/rendered/availability-runs.csv
```

The rerun script also executes the entire Test and Full catalogue, including the original 128 variants. Logs and indexes remain in `target/experiments/admission-trial/`; experiments execute sequentially to bound resident memory.
