# Compression-aware batching with bounded memory

This document describes the batching design and how to evaluate it.
Historical run reports are excluded; regenerate measurements for this source.

Keep Rust's existing progressive zstd encoder and bounded context pool. Improve
its batch boundaries and retained output capacity together. Make estimated wire
bytes the default soft packing target, with independent hard limits for raw work,
record obligations and output memory. Do not flush the compressor per record to
obtain an exact size, or keep one live compressor for every partition.

## Implemented default

`ProducerConfig::default()` now uses `BatchTargetMode::EstimatedWire`. C ABI 3
exposes `batch_target_mode` (0 estimated wire, 1 legacy raw); the Java facade
exposes `kr.batch.target.mode=estimated-wire|raw`, defaulting to estimated wire.
New simulation manifests write the mode explicitly; historical manifests without
the field retain Raw on replay.

The first implementation uses one fixed-point estimate per existing partition,
starting at 1.0, bounded to 1/64..2.0, with 5% observed-size headroom. Each
completed compressed payload updates it: better compression can at most double
packing for the next batch; worse compression takes effect immediately. Each
batch snapshots the estimate at creation. Exact emitted output is a lower bound.
Topic fallback and size buckets remain future tuning work.

Soft target and linger sealing wait for dispatch credit. Raw/output hard limits,
record obligations, oldest deadline, explicit flush/close and context reclamation
remain independent. Existing progressive encoding and context reuse continue
without intermediate compressor flushes or extra raw staging.

Before publication, the default also attempts one tail copy of at most 4 KiB
when it can at least halve an allocation of at least 1 KiB. Old and new backing
are charged together inside the existing reservation and physical pool. Missing
scratch skips compaction immediately. It preserves bytes, segment count and
retry/provider ownership. The raw mode retains the previous representation.
The full active output-envelope reservation and context scheduler are unchanged;
this change does not establish a new no-overflow compression bound or host CPU
improvement.

## What Java and Rust did before this change

Use the [classic comparison runner](CLASSIC_COMPARISON.md) to measure request
packing under equal numeric settings. These observations are not a controlled
comparison of codec speed or compression ratio for identical raw batches.
Request coalescing, batch membership, linger and scheduling also affect them.

Java source below is the frozen comparison revision
`4ce510d8647d69ddbd809accdc432dcd2c06979b`, using zstd-jni 1.5.6-10.
Rust's record crate pins Zstandard 1.5.7. No claim about all Java versions follows.

| Concern | Classic Java | Previous Rust |
| --- | --- | --- |
| Streaming | Encodes records through a DataOutputStream and 16 KiB BufferedOutputStream into a zstd stream. It does not buffer the entire raw batch first. | Feeds record cursor spans into `compress_stream2` with `continue`, then `end` at seal. Drops each input owner after its last byte is consumed. |
| Batch target | Historical compression ratio times raw bytes, with a 1.05 safety factor. The next record is conservatively charged at its raw size. | Seals immediately when cumulative raw encoded bytes cross the target, even without dispatch credit. |
| Estimator scope | Static topic-name/codec map; improves in steps of 0.005, deteriorates by at least 0.05 or the observed ratio. | No compression-ratio packing estimator. |
| Codec ownership | zstd-jni creates a native context per stream and frees it on close. Kafka recycles the output scratch buffer, not that context. | Fixed-count warmed contexts; session reset and reuse after each batch. |
| Output storage | Compressed scratch is copied into a ByteBufferOutputStream; underestimation can allocate and copy a larger backing buffer. | Direct output into owned chunks, but reserves the full hard output envelope before encoding. Partially used chunks retain their full allocation. |

Java references: [builder and room checks](https://github.com/apache/kafka/blob/4ce510d8647d69ddbd809accdc432dcd2c06979b/clients/src/main/java/org/apache/kafka/common/record/internal/MemoryRecordsBuilder.java),
[ratio estimator](https://github.com/apache/kafka/blob/4ce510d8647d69ddbd809accdc432dcd2c06979b/clients/src/main/java/org/apache/kafka/common/record/internal/CompressionRatioEstimator.java),
[zstd wrapper](https://github.com/apache/kafka/blob/4ce510d8647d69ddbd809accdc432dcd2c06979b/clients/src/main/java/org/apache/kafka/common/compress/ZstdCompression.java),
[growing output buffer](https://github.com/apache/kafka/blob/4ce510d8647d69ddbd809accdc432dcd2c06979b/clients/src/main/java/org/apache/kafka/common/utils/ByteBufferOutputStream.java).
Context lifecycle was checked in the cached 1.5.6-10 sources jar's
`ZstdOutputStreamNoFinalizer`: constructor, `write`, `flush`, and `close`.

Rust references: [batch policy](../kr-kafka-producer/src/accumulator.rs),
[progressive record encoder](../kr-kafka-record/src/batch.rs),
[codec pool](../kr-kafka-record/src/codec.rs),
[output ownership](../kr-kafka-record/src/output.rs),
[memory contract](../kr-kafka-record/README.md).

## What streaming can and cannot tell the batcher

Zstd can consume substantial input before emitting output. The return value
from `compress_stream2` is not the exact size of a frame finalized now. A flush
forces block output; it retains history within the frame, but changes block
boundaries. End finalizes the frame. These distinctions are specified in the
[Zstandard 1.5.7 API](https://github.com/facebook/zstd/blob/v1.5.7/lib/zstd.h).

The [reproducible stream probe](analysis/compression_stream_probe.py) uses
installed libzstd 1.5.7, level 1, windowLog 20, no workers, content size or
checksum. All twelve generated frames round-trip exactly. Inputs are synthetic
repeated text and deterministic SHA-256 blocks, without Kafka framing.

| Input | Output before end, no flush | Final bytes, no flush | Final bytes, flush every 2 KiB |
| --- | ---: | ---: | ---: |
| 64 KiB repeated text | 0 | 71 | 414 |
| 64 KiB SHA-256 blocks | 0 | 65,545 | 65,641 |
| 256 KiB repeated text | 83 | 86 | 1,470 |
| 256 KiB SHA-256 blocks | 262,156 | 262,159 | 262,537 |

Feeding 2 KiB versus 16 KiB spans without flushing produced byte-identical
frames for each input. The 256 KiB cases first emitted at 128 KiB of consumed
input. Peak context size was 1,893,913 bytes for these runs, excluding external
input/output storage; this is an observation for these parameters and inputs,
not a new workspace bound.

Java has a smaller, separate close-path inefficiency. Its buffered wrapper's
close drains the input buffer and flushes the wrapped compressor before ending
the frame. With Temurin 25.0.3 and the pinned zstd-jni jar, the
[close probe](analysis/CompressionCloseProbe.java) produced 73 versus 70 bytes
for 4 KiB repeated input and 74 versus 71 for 64 KiB. At 256 KiB both produced
86 bytes. All six frames round-trip.
Avoiding this extra flush is useful, but does not explain the Full request gap.
Rust already uses direct frame end without that wrapper flush.

Streaming across calls preserves useful history inside a Kafka record batch.
Ordinary Kafka compatibility requires batches to remain independently readable:
do not introduce an external dictionary or history dependency on an earlier
batch or another partition. Reusing a context after reset saves allocation;
it does not carry compression history into the next batch.

## Packing contract and further tuning

Make estimated-wire batching the default, including the 61-byte Kafka batch
header in the target. With compression disabled, use exact encoded wire bytes
without a compression estimator. Keep the legacy raw-target mode available as
an explicit option. Keep request targets separate: multiple partition batches
can share a broker request without sharing a compression stream.

Apply the default consistently through native configuration, simulation and
host construction, and FFI/Java configuration mapping. Frozen comparison
baselines must explicitly select the old raw policy so changing the default
does not silently change the control population.

1. Track cumulative raw encoded bytes, pending input allocation capacity,
   compressed output used/capacity, record obligations and the oldest deadline
   separately. Cumulative raw bytes bound decompression and CPU work even after
   the input buffers have been released.
2. Estimate the size after appending the candidate record, including that
   record's expected compression. Allow one record to cross the soft target.
   Check exact raw and record-count hard limits before moving its ownership.
   An oversized candidate remains intact for the next batch or follows the
   explicit oversized-record rejection policy.
3. Learn from completed batch payload bytes divided by exact encoded raw bytes.
   Use bounded state attached to existing topic UUID/partition state, keyed by
   codec configuration, with a topic fallback for cold partitions. Consider
   batch-size buckets to avoid treating tiny-batch overhead as an intrinsic
   property of the corpus. A fixed-point estimator with a positive floor,
   bounded optimistic growth and fast response to worse compression is a
   candidate; tune it with changing-entropy workloads, not only repeated data.
4. Treat observed output as a lower bound on the final wire size. It can stop
   further growth, but must not justify accepting unlimited input when the
   codec has emitted nothing. Do not infer a current compression ratio by
   dividing emitted bytes by all consumed input, including buffered input.
5. Separate readiness from irreversible sealing. Under the default policy,
   expired linger or a reached soft target marks a batch eligible for
   dispatch. While transport/in-flight capacity is unavailable, it may continue
   growing within the hard limits. Deadlines, explicit flush/close, hard limits
   and bounded resource reclamation still force settlement. Do not reset the
   oldest deadline or linger origin on growth.

For illustration, a 64 KiB wire target, 512 KiB raw cap and a separately
validated larger wire cap permit roughly 512 KiB of highly compressible raw
data to accumulate, while incompressible data approaches the 64 KiB target.
These are example policy values, not recommended defaults or measured optima.
A corpus change must remain safe even if the estimator is completely wrong.

The [dispatch credit](../kr-kafka-producer/src/engine/dispatch.rs) check covers
active connection state, an outstanding write, in-flight depth, retry pressure,
throttle and wire-window use. The implemented default makes soft target and
linger/sparse sealing observe this signal; legacy raw target sealing remains
immediate. End-to-end sim tests exercise both provider send pressure and a full
in-flight window.

## Memory and progress are part of the optimization

The historical comparison config uses a 32 KiB output chunk for a 4 KiB raw
batch target; the producer defaults use 512 KiB chunks. Before compaction, a tiny
compressed batch holds the payload chunk's allocation capacity through retries/
provider views. Larger batches amortize this, but do not fix sparse traffic.

The default now attempts bounded compaction of a substantially underfilled final
payload before publication. Copying a small compressed result once can save much more
memory than avoiding that copy. Charge old and new allocations simultaneously;
if scratch credit is unavailable, keep the existing representation and make
progress. Never wait for compaction while retaining the only capacity needed
to finish other work. A size-class output allocator is another candidate.

Smaller arbitrary chunks are not currently a configuration-only fix:
[config validation](../kr-kafka-producer/src/config.rs) permits the header and
at most two payload chunks per batch to preserve the request segment envelope.
Changing chunk growth must preserve that bound, or explicitly revise the
planner/provider segment accounting and budget any final coalescing copy.

Retain the fixed context pool. The probe illustrates why one compressor per
active partition is expensive. Delay activation for cold/sparse partitions;
service hot work fairly and bound context hold time. The
[current reclamation path](../kr-kafka-producer/src/engine/encoding.rs) seals
the oldest holder when a context waiter cannot progress. Record how often this
fragments batches before replacing it. A paused stream cannot simply release
its context and later resume from an empty one.

Initially retain the full output-envelope reservation for every active encoder.
It is conservative, but avoids turning incremental allocation into a cycle
where every encoder needs more output memory before any can finish. Account for
contexts, input, output, cached capacity, descriptors, events and transport
buffers; reserved capacity and physically allocated capacity are different
metrics. Bound inactive raw accumulators too, and reserve control-plane headroom.

A later incremental-reservation design must reserve enough capacity to finish
all already accepted encoder input before consuming another record. It needs a
proven bound for the exact streaming mode, including buffered input and frame
overhead; an estimated ratio is never that proof. Do not silently use the
single-pass `compressBound` contract as a bound on arbitrary streaming flushes.
Until a stronger no-overflow contract is established, preserve the existing
definitive pre-transmission `CompressedTooLarge` outcome and report its incidence;
the proposed optimization must not increase accepted-record failures.

Finish and freeze payload bytes before ordinary dispatch/retry. Preserve record
obligations after releasing raw input. Ordinary retries share the same compressed
bytes and sequence assignment; retain the existing guarded recovery rules for
header refinalization. Do not depend on recompressing input that has been freed
to undo an optimistic append or split an oversized batch.

## Implementation and evaluation order

1. Instrument seal reason, raw/wire occupancy, output used/allocated/reserved
   bytes, context occupancy/reclamation and records per request. Establish the
   current allocation amplification before changing policy.
2. Implement estimated-wire batching as the default and evaluate bounded tail
   compaction as a separately measured change. Retain explicit raw-policy
   baselines and compare each change alone before the combination. Exercise
   default construction through every entry point as well as explicit policy
   selection. Resolve hard-envelope behavior before expanding admissible raw
   work beyond current limits.
3. Evaluate activation/reclamation and readiness-versus-sealing under pressure.
   Then consider incremental reservations only if full-envelope reservation is
   measured to bind throughput or admission.
4. Separately benchmark host codec work: current span feeding versus bounded
   16/64 KiB staging, context reuse versus construction, and already-sealed small
   batches with a truthful pledged source size. A staging copy can amortize many
   tiny codec calls; it costs memory/copy work and must retain precise input
   ownership. Unknown-size progressive streams cannot pledge a guessed size.

Use the existing Full compression, fanout, linger, bounded-memory overload and
slow-broker fixtures with frozen Java inputs. Add compressible-to-random shifts,
heterogeneous partitions within one topic, thousands of sparse active partitions,
highly compressible large records, output/context starvation, held provider
views, paused completion polling, deadlines during compression and retry/recovery.
Prove bounded memory, exact payloads, ordering, replay and terminal certainty.

Two comparisons answer different questions: identical raw batch memberships
isolate codec/wrapper efficiency; identical offered workloads, retained-memory
budgets and latency objectives assess the full producer policy. Measure useful
deliveries, refusals/failures, wire bytes and requests together with completion
latency and maximum waits. On the host, measure CPU per raw byte, allocations,
copies and actual memory including native contexts. Simulator time cannot
establish CPU savings. A lower request count obtained by excessive waiting or
healthy-partition refusal does not meet the goal.

## Reproduce the codec probes

These probes are intentionally small and independent of the simulator. They do
not rerun or replace the Java/native producer campaign.

```sh
python3 -B kafka/kr-kafka-experiments/analysis/compression_stream_probe.py \
  --library /opt/homebrew/lib/libzstd.dylib
java --enable-native-access=ALL-UNNAMED -cp /path/to/zstd-jni-1.5.6-10.jar \
  kafka/kr-kafka-experiments/analysis/CompressionCloseProbe.java
```
