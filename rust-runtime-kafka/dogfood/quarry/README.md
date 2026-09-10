# Quarry

Quarry is a small, bounded leased work queue built to exercise `kr-runtime`
as real application infrastructure. It is deliberately simple enough to have
an independent state-machine oracle, while still putting pressure on task
scheduling, virtual time, cancellation, bounded mailboxes, backpressure, and
deterministic replay.

Quarry currently has four layers:

- `InMemoryQueue` is a passive deterministic engine. Its caller supplies every
  runtime-relative timestamp; the engine reads no clock and uses no randomness,
  I/O, or hidden task.
- `start_broker` runs the engine as an actor on either executor through the
  portable `RuntimeHandle`: a host runtime's `HostHandle` for production and a
  simulation `Handle` for deterministic model campaigns and replay, with no
  separate entry points. Clients use a bounded
  command mailbox and one-shot replies. Full mailboxes return immediate
  `Backpressure`, each broker poll handles at most one command, and orderly
  shutdown is explicit. At most one tracked expiry task exists per leased job,
  and broker shutdown or cancellation aborts all of them.
- `DurableQueue<R>` stores successful submits and acknowledgements directly in
  any owned, completion-based `RingWriter`. It conditionally appends at its
  expected tail, syncs the ring, and installs the in-memory mutation only after
  the matching durable checkpoint is known to exist. There is no second
  Quarry-specific storage trait or backend.
- The shared ring providers supply the storage policy. `MemoryRing` is the
  immediate deterministic reference implementation. `FileRing<F>` is the
  checksummed circular file engine over the provider-neutral `kr_runtime_io::FileIo`
  API, so the physical path can run over `SimStorage`. On Linux,
  [`UringRing`](../../storage/kr-runtime-ring-uring/README.md) hosts that same
  file-ring engine over `UringFile` on a dedicated production thread.

The MVP supports bounded payloads and active-job capacity, delayed submission,
request-ID deduplication, bounded batch claims, expiring and renewable leases,
acknowledgement, delayed negative acknowledgement, and deterministic semantic
snapshots. An accepted broker command remains accepted if its client future is
subsequently cancelled; dropping the response does not roll the command back.
The durable layer adds versioned, checksummed Quarry records, exact
configuration replay, restart incarnations, and explicit completion certainty
for failed ring operations.

## Semantics

Quarry promises **at-least-once delivery**. A lease can expire while its worker
is still performing a side effect, after which another worker can claim the
same job. Applications must therefore make their own effects idempotent when
duplicates matter.

Each claim receives a unique lease token. The current token fences only queue
state: stale tokens cannot renew, acknowledge, or negatively acknowledge a
job. A token does not cancel an old worker and does not fence writes to an
external service.

A producer-supplied request ID deduplicates an identical submit while its job
is active and while its completed record remains in history. Reusing that ID
with different payload or scheduling data is a conflict. Completed request and
acknowledgement records are retained only up to
`QueueConfig::completed_history_capacity`; oldest records are evicted. After
eviction, the old request ID may create a new job and an old acknowledgement is
no longer known. Thus deduplication and acknowledgement retry idempotence are
bounded guarantees, not permanent exactly-once semantics.

Acknowledgement removes a job from active capacity. Lease expiry makes a job
eligible again at its deadline, and `nack` makes it eligible at
`now + retry_after`. Renewing a lease replaces its deadline with
`now + lease_for`. The engine treats the supplied runtime-relative instant as
the full notion of time and expires due leases before each state-changing
operation or snapshot. Direct `InMemoryQueue` callers must supply monotonically
nondecreasing instants.

If the runtime cannot spawn an expiry task because its task limit is already
reached, expiry becomes lazy rather than incorrect: the next broker operation
or snapshot expires every lease due at that instant. Lease tokens are currently
scoped to one broker incarnation. Durable recovery appends a new incarnation
marker before issuing leases, so a token issued before restart cannot equal one
issued afterwards.

## Durability and recovery

`DurableQueue::submit` and `DurableQueue::ack` stage an in-memory candidate,
append one owned record at the expected ring tail, sync the ring, and only then
install the candidate.
Failures report whether the logical mutation was definitely not applied,
definitely applied, or may have applied. An uncertain or abandoned mutation
poisons that live queue: every state operation is rejected until the handle is
discarded and recovered from its ring. Conditional appends fence the
queue's expected tail, so a stale durable writer fails closed.

Recovery requires both ring heads to remain at position zero. It first calls
`sync` to fence any accepted suffix left by an abandoned operation, then
replays the complete durable interval in byte- and record-bounded pages under a
separate total-record budget. The first record pins the exact `QueueConfig`;
changing capacity or history bounds is a typed mismatch rather than a subtly
different replay. Records carry a stable format version and CRC32C. Replay
validates record order, job allocation, incarnation progression, and
acknowledgement-token uniqueness before conditionally appending and syncing the
next incarnation marker.

Claims, renewals, lease expiry, and nacks remain deliberately ephemeral. After
restart every active job is unleased and its availability returns to the
original submitted `not_before`; a later nack delay is not durable. Callers must
supply monotonically nondecreasing runtime-relative instants across operations
and recoveries of one ring.

Quarry requires one live writer/recovery owner per ring. The ring's atomic
expected-tail append detects racing or stale durable mutations, but it is not a
leadership lease: a stale process can still make local ephemeral claims until
it attempts a durable write. Cloning a ring handle therefore does not authorize
multiple live `DurableQueue` owners.

`DurableQueue<MemoryRing>` is the small logical reference stack. `MemoryRing`
separates accepted and durable bounds, supplies explicit sync fences, and its
`crash` helper deterministically drops an unsynced suffix and restores an
unsynced trim's records. It does not model physical persistence.

The normal physical simulation stack is
`DurableQueue<FileRing<SimStorage>>`. `SimStorage` models accepted and durable
file images, partial transfers, bounded admission, latency, crash/reopen, and
before/after/ambiguous failures. This exercises the same circular frames,
CRC32C validation, alternating checkpoint slots, and recovery code used by
production instead of substituting a queue-shaped mock. The file ring can wrap
physically while preserving dense absolute logical positions.

On Linux, `DurableQueue<UringRing>` uses the same `FileRing` engine over
`UringFile`. Kernel completions are driven by the ring host's production thread
and never enter `SimRuntime` as foreign wakes.

This replacement preserves Quarry's inner version-1 record bytes, but it is a
cold break in the outer on-disk container. Files created by the removed
append-only backend are not `FileRing` files; deployments must provision a new
ring or perform an offline migration before opening them with this version.

Quarry currently never calls `RingWriter::trim`. Queue recovery needs every
record from position zero, including records no longer retained in the
in-memory completed-history window. Consequently, physical wrapping alone does
not make space reusable: the configured live-record, live-payload, or physical
ring capacity eventually fills under an unbounded workload. A future
queue-level checkpoint or snapshot must preserve all replay state before it can
advance and sync the ring head.

## Current boundaries

This crate is a simulation test bed, not yet a production queue. Even with the
Linux `UringRing` host, it does not currently provide:

- queue-level checkpoints, compaction, ring trimming, online format migration,
  or raw-device/direct I/O;
- a network protocol, reconnect behavior, or partial-I/O handling;
- a broker actor wired to the durable queue or a virtual process supervisor;
- replication, leader election, sharding, multiple named queues, or consensus;
- authentication, authorization, or payload encryption; or
- exactly-once delivery or exactly-once external side effects.

The next storage layer is a queue snapshot/compaction scheme that can safely
advance the durable ring head. The intended application layers are a framed
simulated network transport and durable broker actor, followed by virtual
process death/restart orchestration. Those layers should reuse the same queue
contract and independent model rather than weakening the in-memory engine into
an I/O-shaped implementation.

## Development

Run the crate's tests and lints from the repository root:

```text
cargo test -p quarry --all-targets
cargo clippy -p quarry --all-targets -- -D warnings
cargo fmt --all -- --check
```

Run the bounded model campaign directly with:

```text
cargo test -p quarry --test campaign model_campaign_matches_broker
```

The ordinary seed sweep runs with runtime tracing disabled. If a seed fails,
the test reruns that seed with a byte-bounded SBE prefix-and-tail trace before
reporting the failure. The outer traced failure atomically publishes a `.sbe`
artifact under the platform temporary directory's `quarry-traces` folder
and prints the exact path. Set `QUARRY_TRACE_ARTIFACT_DIR` to choose another
directory. Shrink candidates do not write artifacts.

Open `tools/trace-tool/index.html` and choose the `.sbe` file to inspect it
directly. Runtime trace artifacts intentionally remain binary SBE for both
browser inspection and tooling interchange.

Run the durable submit/ack fault, crash, recovery, and retry campaign with:

```text
cargo test -p quarry --test durability_campaign
```

Replay one campaign seed with the same checked-in driver and workload while
collecting its diagnostic trace fingerprint:

```text
QUARRY_SEED=17 cargo test -p quarry --test campaign reproduce_seed_from_environment -- --ignored --exact --nocapture
```

The reproduction helper pins the rest of the campaign inputs in source. Future
retained failure manifests should include their exact reproduction command; a seed by
itself is insufficient once driver versions, configurations, workloads, and
budgets can vary.
