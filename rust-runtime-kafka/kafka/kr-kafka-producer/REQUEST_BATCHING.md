# Request batching policy

`ProducerConfig::request_batching_policy` controls Produce request grouping,
independently of `batch_target_mode`, compression and per-partition batch linger.
The default remains `RequestBatchingPolicy::Sealed`.

| Policy | Request membership | Early sealing |
| --- | --- | --- |
| `SinglePartition` | One partition batch per request, including retries | None |
| `Sealed` | Already sealed, dispatchable heads on one broker/lane | None; existing behavior |
| `BrokerReady` | The same heads, plus opportunistically prepared neighbors | Younger open heads may finish before their own target/linger |

`request_target_bytes` counts completed request wire bytes, including framing.
It is a soft target: the final admitted batch may cross it. The exact
`request_hard_bytes`, `request_max_partitions`, segment, wire-credit and fairness
limits remain authoritative. `SinglePartition` reduces the effective partition
limit to one. Policies never combine different brokers/lanes or reorder records
within a partition. They do not introduce a separate request linger or record
count target.

## Broker-ready preparation

A new, sealed, dispatchable head starts at most one preparation pass. Every
candidate visit is charged to the scheduler's item budget and uses its existing
broker/lane index, including dirty or parked nonempty partitions. No unrelated
partition scan is added. The pass stops at the configured partition limit or
estimated request target; an estimated hard-limit overflow skips the candidate.
Final request admission measures the completed output and rechecks exact limits.
Routes containing only the anchor skip preparation without spending scan work.

A younger head qualifies only if all its current input is already encoded, it
owns its transform reservation, and it has no outstanding encoder resource wait.
Its route, topic lifetime, deadline, backoff, identity and sequence-window fences
are checked before sealing. Preparation neither admits another record nor
acquires a new transform envelope. `RequestGather` is a distinct seal reason.

The anchor parks until one ordinary, budgeted encoder pass. It observes that
pass's modeled completion boundary, then resumes dispatch without another
preparation pass. A neighbor that cannot finish in that pass remains sealed for
a later request; the anchor does not wait for more input, memory, codec contexts
or subsequent encoder passes. The ordinary scheduler still owns request
admission and fairness; preparation is not a reservation of network capacity.
Connection or topology changes may therefore reduce the eventual gathering.
Preparation reports newly queued encoding work to the actor once, so a quiet
producer does not depend on another ingress event or metadata timer to finish it.

This deliberately bounded policy can trade larger requests for more small
partition batches. It does not promise every prepared neighbor joins the anchor,
and it does not solve context reclamation, retry ordering or pre-dispatch waits.
The experiment compares that tradeoff with the unchanged default before any
promotion of `BrokerReady`.

Use the [policy matrix runner](../../scripts/run-batching-policy-matrix.py)
with `--policy request-batching` to compare request savings, additional partition
batches, admission and delivery tails with the default and classic Java.
`BrokerReady` remains opt-in while these tradeoffs are evaluated.

## Bindings and replay

C ABI 4 adds `request_batching_policy`: 0 is Sealed, 1 SinglePartition and
2 BrokerReady. `reserved_request_policy` must be zero and makes the configuration
size distinct from ABI 3. Use the matching generated bindings and library.
The Java facade exposes `kr.request.batching.policy=sealed|single-partition|broker-ready`.
Historical simulation manifests without the field deserialize to Sealed; new
manifests record it explicitly. The simulation harness records an explicit
request-policy override separately from the original catalogue manifest.
