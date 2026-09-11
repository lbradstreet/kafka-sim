# Completed producer simulation campaigns

All requested executions completed with replay verification. Measurements use a
deterministic simulation clock; they do not measure host CPU performance.

| Campaign | Executions | Result |
|---|---:|---|
| Native Full catalogue | 146 | passed |
| Classic Java/native Test catalogue | 584 | passed; one fault-exposure gap flagged |
| Classic Java/native Full catalogue | 584 | passed; all 292 pairs have comparable fault exposure |
| Classic additional-seed comparisons | 8 | passed |
| Raw/EstimatedWire Full and followups | 616 | passed |
| Sealed/BrokerReady Full and followups | 648 | passed |
| Descriptor admission, seeds 0–15 | 204 | executions passed; policy gates below |
| Correctness campaign | 515 | passed |

The comparison campaigns total 2,440 executions. Admission also passed separate
146-run Test and Full catalogue regressions. Both native policy matrices reproduced
all 292 default-policy baseline controls exactly. Their additional-seed campaigns
include 16 Raw/EstimatedWire and 32 Sealed/BrokerReady pairs with correlated witnesses.

## Admission findings

Healthy admission, progress and recovery passed. The 96 pressure-policy fault
runs had zero healthy outage refusals and zero missing acknowledgment windows
across 11,520 healthy 100 ms windows. All 39,198,023 accepted records were acknowledged.

Two gates failed:

- The retained historical baseline expects 33,874 healthy refusals for the
  original-source Shared run at 16,000 offers/s, seed 0. The fresh result is
  33,872, a difference of two. The criterion was not changed.
- At 32,000 offers/s, pressure-policy steady acknowledgment throughput was
  74.995%, 78.557%, and 80.637% of Shared for hot, skew90, and sparse1024 demand.
  The gate requires at least 95%. At 8,000 offers/s, all three ratios were 100%.

PartitionPressure remains opt-in. The report renderer returns a nonzero exit
status for these failed gates even though all executions and analyses completed.

## Reports

- [Full Java/native gallery](classic-full/index.html)
- [Classic admission isolation](classic-full/admission-isolation.html)
- [Request-grouping dashboard](request-policy/index.html)
- [Classic comparison tables](summaries/COMPRESSION_BATCHING_REVIEW.md)
- [Raw/EstimatedWire comparison tables](summaries/BATCHING_POLICY_REVIEW.md)
- [Sealed/BrokerReady comparison tables](summaries/REQUEST_GATHER_REVIEW.md)
- [Admission trial](summaries/ADMISSION_TRIAL.md)

The Test-size fault-exposure gap is `soft.metadata-loss-during-move`,
`metadata1000ms`, original profile, seed 0: Java had no opportunities for fault
rule 0 while native had one. The viewer keeps this limitation visible.

All published pages and compact summaries were checked for personal paths,
contact details, historical calendar dates and credentials. Files use synthetic
filesystem creation and modification times. The original imported Git history,
old run evidence and machine-specific raw campaign provenance are not included.
