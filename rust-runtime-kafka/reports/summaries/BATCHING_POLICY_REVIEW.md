# Raw and EstimatedWire batch targets

292 Full case pairs and 16 additional-seed pairs, covering 616 replayed executions. Both arms use the same current native implementation and Java simulation driver; the checked pair contract permits only the selected policy override to differ.

292 of 292 default-policy controls reproduce the complete baseline result. Metrics below count Full case pairs, including both original and common profiles. A lower latency is conditional on successful accepted records; inspect offered, refused and failed populations alongside it. Request and byte counts are observations from the simulated workload, not host CPU measurements.

| Metric | Lower with estimated-wire | Higher | Unchanged | Missing population |
|---|---:|---:|---:|---:|
| offered | 39 | 63 | 190 | 0 |
| accepted | 50 | 86 | 156 | 0 |
| refused | 23 | 11 | 258 | 0 |
| acked | 50 | 86 | 156 | 0 |
| failed | 0 | 0 | 292 | 0 |
| ack_p99_ns | 115 | 36 | 139 | 2 |
| ack_max_ns | 162 | 53 | 75 | 2 |
| produce_requests | 180 | 79 | 33 | 0 |
| wire_bytes | 163 | 100 | 29 | 0 |
| partition_pending_gap_ns | 95 | 62 | 135 | 0 |
| first_observation_wait_max_ns | 162 | 83 | 45 | 2 |
| broker_attempts_max | 5 | 8 | 277 | 2 |
| last_terminal_ns | 128 | 59 | 105 | 0 |

[Every case](BATCHING_POLICY_VARIANTS.md) · [CSV](BATCHING_POLICY_RESULTS.csv) · [Additional-seed CSV](BATCHING_POLICY_FOLLOWUP_RESULTS.csv)
