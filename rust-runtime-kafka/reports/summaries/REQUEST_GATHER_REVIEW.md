# Sealed and BrokerReady request grouping

292 Full case pairs and 32 additional-seed pairs, covering 648 replayed executions. Both arms use the same current native implementation and Java simulation driver; the checked pair contract permits only the selected policy override to differ.

292 of 292 default-policy controls reproduce the complete baseline result. Metrics below count Full case pairs, including both original and common profiles. A lower latency is conditional on successful accepted records; inspect offered, refused and failed populations alongside it. Request and byte counts are observations from the simulated workload, not host CPU measurements.

| Metric | Lower with broker-ready | Higher | Unchanged | Missing population |
|---|---:|---:|---:|---:|
| offered | 11 | 50 | 231 | 0 |
| accepted | 16 | 67 | 209 | 0 |
| refused | 17 | 5 | 270 | 0 |
| acked | 16 | 67 | 209 | 0 |
| failed | 0 | 0 | 292 | 0 |
| ack_p99_ns | 68 | 44 | 178 | 2 |
| ack_max_ns | 64 | 25 | 201 | 2 |
| produce_requests | 110 | 41 | 141 | 0 |
| wire_bytes | 50 | 103 | 139 | 0 |
| partition_pending_gap_ns | 49 | 37 | 206 | 0 |
| first_observation_wait_max_ns | 63 | 35 | 192 | 2 |
| broker_attempts_max | 3 | 3 | 284 | 2 |
| last_terminal_ns | 78 | 38 | 176 | 0 |

[Every case](REQUEST_GATHER_VARIANTS.md) · [CSV](REQUEST_GATHER_RESULTS.csv) · [Additional-seed CSV](REQUEST_GATHER_FOLLOWUP_RESULTS.csv)

[Interactive request-policy comparison](../request-policy/index.html)
