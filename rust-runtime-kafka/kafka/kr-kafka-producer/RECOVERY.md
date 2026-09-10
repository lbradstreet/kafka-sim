# Nontransactional idempotent recovery

An ambiguous delivery timeout terminates that record as `Unknown`; it no longer
permanently closes the producer. Future records can succeed after the producer
changes generation. This applies to the nontransactional idempotent producer.

## Epochs and exhaustion

The producer identity is `(producer ID, epoch)`. The epoch is a signed 16-bit
generation number with valid values 0 through 32,767. It changes on recovery,
not on every message. Exhaustion means another generation is needed when the
current epoch is already 32,767; it is unrelated to sequence-number wrap.

Normally the client increments the epoch locally and retains its producer ID.
At exhaustion it requests a fresh producer ID through `InitProducerId`, then
uses the actual broker response (fresh ID, epoch zero in the pinned broker).
It neither wraps to a negative epoch nor fails solely because the epoch ran out.

This matches the rollover policy in the pinned classic Java producer at Kafka
revision `4ce510d8647d69ddbd809accdc432dcd2c06979b`:
`clients/src/main/java/org/apache/kafka/clients/producer/internals/TransactionManager.java`,
`bumpIdempotentProducerEpoch` and `bumpIdempotentEpochAndResetIdIfNeeded`
(lines 673–705). Java's `testProducerIdReset` also starts at `Short.MAX_VALUE`.
Matching this policy does not claim identical partition scheduling.

## Recovery barrier

1. Publish FIFO terminal outcomes using the complete attempt history. Parsed
   successes remain successes even when an earlier batch expires. Unknown
   records retain their original failure reason and are never automatically
   replayed under a new epoch or ID.
2. Request an identity change and fence new assignments. Old transmitted
   batches can still settle or retry under their original identity. Recovery
   waits until the ledger has no active, transmitted or pending old entries
   and the engine has no request plans. Each record keeps its original deadline.
3. If an epoch remains, install the next epoch locally. Idle connections stay
   open; the connection carrying the expired request was already quarantined.
   At exhaustion, incrementally retire every old connection, wait for actual
   provider release, and issue `InitProducerId` without a previous identity.
4. Incrementally rewrite only retained, never-transmitted batches, preserving
   canonical payloads and deadlines. Start each partition at sequence zero;
   empty partition histories reset lazily on their next assignment. New
   dispatch stays fenced through the final maintenance step. Pending Unknown
   partition bookkeeping is cleared one entry per step, within the work quota.

Historical partition-capacity reclamation requests this same identity change.
Simultaneous reclamation and ambiguous expiry therefore share one transition.
Broker fatals and explicit failures remain `FailedClosed`; bounded failure
draining cannot re-enter recovery, and the actual stored fatal reason propagates.

## Ordering and availability boundaries

With the same producer ID, the broker rejects an old-epoch write once that
partition has observed a newer epoch. A delayed old write can arrive before the
first new-epoch write; incrementing local state alone does not establish a
remote fence on every partition.

With a fresh producer ID, there is no fence against remote old-ID writes. A
delayed old record can arrive after a new-ID record. Retiring local connections
cannot prove that a broker has discarded every already-transmitted request.
This is the explicit Java-compatible availability choice at exhaustion;
`Unknown` retains its uncertainty, and no cross-ID ordering guarantee is added.

This implementation still pauses new assignments producer-wide. Another
unavailable partition with a later outstanding deadline extends that pause;
queued healthy records can expire while waiting. Java can migrate partitions
as their own in-flight windows drain. Partition-local recovery remains follow-up
work, and the staggered-deadline test deliberately preserves this distinction.

Repeated recovery also means lifetime Unknown deliveries can exceed the old
32-bit bound. `EngineStatus::unknown` is an exact `u64`; the existing Rust/C ABI
close-event summary saturates at `u32::MAX`. Individual delivery events are
unchanged and continue to carry exact per-record outcomes.

## Executable evidence

- `tests/engine/epoch_recovery.rs` drives real Produce frames through the broker
  model, with both append-before-response-loss and no-append ambiguity, plain
  and zstd payloads, two successive recoveries, and the 32,766 → 32,767 → fresh
  ID boundary. It verifies both destination probes, sequence zero, exact
  terminal populations, no replay of terminated records, late-response rejection,
  and old-epoch rejection versus explicitly allowed old-ID stragglers.
- The staggered-deadline test offers healthy traffic throughout the global hold,
  verifies the first healthy ACK after recovery, and preserves an intervening
  short-deadline failure and FIFO terminal order.
- Ledger/engine tests cover overlapping reclamation and ambiguity, later-window
  outcome proofs, one-item installation budgets, cancellation during rewrite,
  retained idle connections, sticky fatal reasons, and counts beyond `u32::MAX`.
- The [classic comparison runner](../kr-kafka-experiments/CLASSIC_COMPARISON.md)
  can compare both timeout variants and both profiles against classic Java.

These are deterministic simulation and passive engine checks. This change does
not add a new host-network or real Kafka broker execution claim.
