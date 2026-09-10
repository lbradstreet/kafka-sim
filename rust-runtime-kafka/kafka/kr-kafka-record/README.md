# Kafka record batches

`kr-kafka-record` builds Kafka magic-2 record batches with `none` or streaming
`zstd`. The crate forbids unsafe Rust and uses `no_std` plus `alloc`. Its default
`zstd` feature uses the safe API of pinned `zstd-safe` 7.2.4; the workspace lock
pins its C implementation to Zstandard 1.5.7. Disabling default features removes
that native dependency and retains the complete uncompressed path.

## Owner contract

Create one `OutputPool` for the producer's compressed-output capacity and one
`CodecPool` for its fixed number of contexts. Construct a `RecordBatchBuilder`
with `BatchConfig`, compression, and a clone of the output pool. `push` accepts
`OwnedRecord` descriptors with `SharedBytes` spans and does not copy key/value
payloads. The convenience `OwnedRecord::copy_from` is explicitly a copy path.
The producer must separately charge input allocation capacities, descriptor
slots, and delivery/release obligations before pushing records.

`progress(codecs, EncodeBudget { input_bytes, codec_calls })` is passive and
bounded by both raw input bytes and compressor calls. A record cursor uses one
64-byte prefix scratch and feeds payload spans directly into the stream. The
returned `records_released` counts FIFO descriptors whose final byte was
consumed in this call; their source spans have already been dropped. Input for
a partially encoded record stays retained. `waiting_for_context` and
`waiting_for_output` preserve the builder and tell the actor to schedule other
work. A descriptor smaller than `progressive_threshold` remains deferred until
more records arrive or `request_seal` is called.

After `request_seal`, continue bounded progress until `sealed`. The compressor
returns to its pool immediately. `take_sealed` moves the payload out exactly
once; it has no sequence yet. Only after the producer obtains dispatch credits
and assigns its identity/sequence should it call `SealedBatch::finalize`. This
writes the 61-byte header into its own exact-size allocation and calculates
CRC32C over attributes through the final payload byte. `chunks()[0]` is always
that header; subsequent chunks retain payload allocations. The producer planner
uses `Records::HeaderAndChunks` to place the header into its metadata arena and
share the payload chunks, preserving the configured segment bound.

Ordinary retries retain exactly these immutable chunks and identity. Call
`mark_transmitted` when the transport certainty makes possible transmission
sticky. `refinalize` can change identity/sequence only before transmission and
only after all old request views of the first chunk have been released; it
returns `SharedOutput` if they remain. It rewrites only identity and CRC, without
copying or recompressing the payload. Repeating the current identity is a no-op.

An encoding error latches the builder failed and drops all remaining input,
output, and context ownership. The owner must terminally reject all records in
that batch as not written; `CompressedTooLarge` must never be retried using a
new sequence. A full batch is rejected by `push` before mutation, so its input
can instead be placed in the next batch. Empty batches cannot be sealed.

## Memory bounds

`raw_limit` caps complete encoded record bytes, including signed length prefixes.
`output_limit` independently caps the compressed payload; the activation
reservation is exactly `output_limit + 61`. The default is a 1 MiB raw cap and a
1 MiB output cap. This implements the design's hard envelope without assuming
that the one-shot `ZSTD_compressBound` bounds arbitrary streaming behavior.
Chunks grow within that reservation and the final allocation is truncated at
the envelope boundary. An exact-size standard-library `Arc` collection allocates
each output chunk directly, with no temporary full-chunk copy.

Sealing returns unused *unallocated* envelope bytes. Partially used chunks stay
charged by their allocation size, not their exposed view length. The pool keeps
an ownership record after a batch is dropped if a provider still holds a chunk.
It reclaims a reservation only after all such references retire. Reclaimed
payload allocations are cached and reused only when exclusively owned. Headers
are released rather than retained by cache mirrors, so refinalization does not
scan or untrack payload allocations. Cached plus live allocation capacity never
exceeds pool capacity.

Use `OutputPool::with_limits(bytes, max_batches, max_cached_chunks)` at startup
to preallocate reservation slots, cache slots and a fixed release queue.
`OutputPool::new(bytes)` derives conservative slot counts from the 61-byte
minimum. Release tokens attached to every public chunk survive slices, provider
retention and batch destruction; their final drop publishes a slot index using
atomics without allocating or borrowing the owner. There is no release queue
saturation path. Tokens contain no pool, actor or provider ownership.

`status()` is an O(1) observational snapshot. Reserved and allocated charges can
conservatively lag physical release until explicit maintenance. On each owner
poll, register its scheduling waker with `register_reclaim_waker`, then inspect
`has_reclaim_work`. The waker must not panic or synchronously poll/reenter; it may
run on a provider thread or an FFI release callback. Provider-held reservations
alone are not ready work. Call `reclaim_step(max_work)` under the actor budget:
one item processes at most one retired chunk or one cache eviction. Exact-size
cache lookup uses a size-class index rather than scanning cached chunks. A
mismatched cache can make `progress` return `waiting_for_output`; reaping evicts
one allocation per item until the next allocation fits. Input cursors preserve
partial progress across these waits. During cooperative shutdown,
`request_cache_clear` permanently disables caching and schedules remaining cache
owners for bounded eviction.

`metadata_capacity_bytes` reports actual element backing capacities for pool,
codec, builder, abort and finalized owners. Shared pool storage is reported only
by the pool; builder diagnostics include owned header vectors and visit retained
records. Payload bytes, Rc/Arc control blocks, and the size-class BTreeMap's
private node layout are excluded. `cache_index_entries` reports the latter's
bounded live size classes; a complete allocator budget must account for those
excluded terms rather than treating this diagnostic as a total heap bound.
Input/header metadata already charged by admission must not be counted twice.

The default 512 KiB chunk size yields one header plus at most two payload
chunks for the default hard limit. If the producer changes the output hard limit, it must choose a chunk
size that respects its send-plan segment budget. Smaller chunks are supported
for other bounded planners and boundary tests.

A zstd context is pinned to a pool's level (1–3), window log (10–23), zero workers,
no checksum, and no content-size field. It is warmed once in unknown-size stream
mode at pool creation so `workspace_bytes` measures the retained workspace.
The same settings and session-only resets are used thereafter. Tests run streams
across multiple windows and assert the warmed workspace remains a bound.
`continue` is used for records and `end` only for sealing; per-record flushes,
external dictionaries, pledged maximum sizes, and nested worker pools are absent.
The owner-local pools and builders do not cross threads. Their finalized
`SharedBytes` segments are thread safe for provider operations.

## Verification

```sh
cargo test -p kr-kafka-record
cargo test -p kr-kafka-record --no-default-features
cargo clippy -p kr-kafka-record --all-targets -- -D warnings
python3 kafka/kr-kafka-record/tests/fixtures/verify.py --cache /tmp/kafka-java-fixtures
```

The checked-in Java fixtures were independently emitted by Apache Kafka 4.3.0's
`MemoryRecords.withIdempotentRecords`, with CRC verification by Java. The fixture
helper reuses the schema fixture script's SHA-512-verified artifact pins. Pass
`--write` to deliberately regenerate them. Normal Rust tests require no Java or
network access. Tests vary chunk and work boundaries, decode zstd to the Java
canonical record bytes, compare against a separate seeded record writer, exercise
signed varint/timestamp boundaries and invalid descriptors, preserve retry bytes,
and verify pool conservation and delayed provider release.

Wire definitions were checked against pinned Apache Kafka source commit
[`7be741d08b3b06f6414ac868e57bf9b958f53a72`](https://github.com/apache/kafka/blob/7be741d08b3b06f6414ac868e57bf9b958f53a72/clients/src/main/java/org/apache/kafka/common/record/internal/DefaultRecordBatch.java)
and its [record encoder](https://github.com/apache/kafka/blob/7be741d08b3b06f6414ac868e57bf9b958f53a72/clients/src/main/java/org/apache/kafka/common/record/internal/DefaultRecord.java).
The stream contract follows the safe wrapper's
[`compress_stream2`](https://docs.rs/zstd-safe/7.2.4/zstd_safe/struct.CCtx.html#method.compress_stream2)
and Zstandard's [pinned API](https://github.com/facebook/zstd/blob/v1.5.7/lib/zstd.h).

## Bounded inspection

`inspect_batch(bytes, BatchDecodeLimits)` validates exactly one nontransactional,
idempotent magic-2 producer batch before exposing any records. It checks length,
magic, CRC, identity, attributes, count, contiguous offset deltas, timestamps,
canonical signed varints, nullable fields, UTF-8 header keys and aggregate header
limits. Uncompressed payloads borrow the frame; zstd uses a bounded output
allocation and a separately capped decoder window. Concatenated zstd frames and
batches are rejected. Validated records and headers are then traversed through
allocation-free iterators. Decoder malformed-input tests include every fixture
truncation, boundary mutations, expansion/window limits, and 2048 fixed seeds.

`RecordSetIter::new(bytes, RecordSetLimits)` inspects concatenated committed
batches, with aggregate wire/raw-byte, batch, record and header caps. Each yielded
batch is fully validated; offsets must be nonnegative and nonoverlapping, and the
next offset must fit `i64`. Empty record sets and gaps between batches are valid.
Consume every result or call `finish()` before advancing a Fetch offset. A bad
tail cannot be hidden by observing its error and then calling `finish()`.
Response identities, correlation and flexible tags belong to the protocol codec.
Process and drop each decoded batch before the next to bound retained zstd output
to one batch's allocation; the aggregate raw-byte cap counts logical bytes, not
the spare capacity of multiple decoder outputs retained by a caller.

`enable_tail_compaction()` opts into a best-effort copy of at most 4 KiB at
final seal. A payload tail occupying at most half a backing allocation of at
least 1 KiB can be compacted when the full reservation and physical pool can
cover both allocations. The owner drops the old chunk before releasing its
charge. Exhaustion skips the optimization; it cannot park a finishing batch.
This adds at most 4 KiB of copy work to the finishing encoder call, separately
from its reported raw input bytes. Request segment count does not increase.
