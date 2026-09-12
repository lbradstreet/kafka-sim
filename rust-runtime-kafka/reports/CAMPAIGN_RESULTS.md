# Fresh producer simulation campaigns

These campaigns were rerun from empty output directories using source revision `b65bafb3fc`. The Java comparisons now use the production producer on this branch through the `clients-dst` test driver. Each comparison execution includes an exact replay. Measurements use a deterministic simulation clock and do not measure host CPU performance.

| Campaign | Executions | Result |
|---|---:|---|
| Native Full catalogue | 146 | passed |
| Classic Java/native Test catalogue | 584 | passed; 1 fault-exposure gap flagged |
| Classic Java/native Full catalogue | 584 | passed; 0 fault-exposure gaps flagged |
| Classic additional-seed comparisons | 8 | passed |
| Raw/EstimatedWire Full and followups | 616 | passed |
| Sealed/BrokerReady Full and followups | 648 | passed |
| Descriptor admission, seeds 0–15 | 204 | executions passed; 2 policy gates failed |
| Correctness campaign | 515 | passed |

The comparison campaigns total 2,440 executions. Admission also passed separate 146-run Test and Full catalogue regressions. Raw/EstimatedWire reproduced 292 of 292 complete default-policy controls; Sealed/BrokerReady reproduced 292 of 292. Additional-seed policy campaigns contain 16 and 32 pairs respectively, each with correlated request witnesses.

## Admission findings

The 96 pressure-policy fault runs had 0 healthy outage refusals and 0 missing acknowledgment windows across 11,520 healthy 100 ms windows. Across the admission trial, 39,198,023 records were accepted and 39,198,023 acknowledged.

| Gate | Result |
|---|---|
| Complete inventory | passed |
| Test and Full catalogue regression | passed |
| Historical baseline reproduced | failed |
| Healthy admission and progress | passed |
| Accepted records recover | passed |
| Skew throughput at least 95% of Shared | failed |

The historical baseline retains its original refusal-count criteria:

| Offers/s | Required healthy refusals | Measured |
|---|---:|---:|
| 1,000 | 792 | 792 |
| 4,000 | 7,416 | 7,416 |
| 16,000 | 33,874 | 33,872 |

Pressure-policy steady acknowledgment throughput as a percentage of Shared:

| Demand | 8,000 offers/s | 32,000 offers/s |
|---|---:|---:|
| hot | 100.000% | 74.995% |
| skew90 | 100.000% | 78.557% |
| sparse1024 | 100.000% | 80.637% |

PartitionPressure remains opt-in. Acceptance criteria were retained. The renderer returns a nonzero exit status when policy gates fail, even when all executions and analyses completed.

## Reports

- [Full Java/native gallery](classic-full/index.html)
- [Classic admission isolation](classic-full/admission-isolation.html)
- [Request-grouping dashboard](request-policy/index.html)
- [Classic comparison tables](summaries/COMPRESSION_BATCHING_REVIEW.md)
- [Raw/EstimatedWire comparison tables](summaries/BATCHING_POLICY_REVIEW.md)
- [Sealed/BrokerReady comparison tables](summaries/REQUEST_GATHER_REVIEW.md)
- [Admission trial](summaries/ADMISSION_TRIAL.md)

## Fault-exposure limits

- Test: `soft.metadata-loss-during-move / metadata1000ms / original / test / 0`. The viewer retains the unequal fault exposure; inspect each adapter's coverage before comparing outcomes.

Published pages and compact summaries were checked for personal paths, contact details, historical calendar dates and credentials. Files use synthetic filesystem creation and modification times. Raw histories, replay sidecars and machine provenance remain in ignored target directories; the HTML viewers do not depend on them.
