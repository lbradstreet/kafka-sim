# `kr-runtime-ring`

`kr-runtime-ring` defines an owned, asynchronous record-ring contract, a
deterministic in-memory reference implementation, and a crash-safe file engine
over the warm [`kr_runtime_io::FileIoSubmit`] boundary(../../io/kr-runtime-io). The logical ring uses dense absolute
positions; only the bounded physical storage wraps.

The core crate is runtime- and storage-provider neutral. The sibling Linux
adapter, [`kr-runtime-ring-uring`](../kr-runtime-ring-uring), exposes
`UringRing` and hosts `FileRing<UringFile>` on a dedicated `io_uring` thread.

## Contract

[`RingReader`](src/lib.rs) exposes bounded durable reads and status.
[`RingWriter`](src/lib.rs) extends it with append, trim, and sync. Every method
returns an owned `'static` future, and every failure is a
`kr_runtime::CompletionError` carrying completion certainty.

A ring tracks four cursors:

| Cursor | Meaning |
| --- | --- |
| `accepted_head` | Consumer progress accepted by this process |
| `accepted_tail` | End of appends accepted by this process |
| `durable_head` | Consumer progress guaranteed by the last checkpoint |
| `durable_tail` | End of records guaranteed by the last checkpoint |

Reads expose only the half-open interval `[durable_head, durable_tail)`. An
append is therefore invisible until a `sync` checkpoint is applied, either by
success or by an `Applied` failure. Likewise, an accepted trim does not hide
records from durable reads or release their resources until that checkpoint
publishes the new head. A crash discards an unsynced append suffix and reverts
an unsynced trim.

Positions are dense `u64` values and remain stable when the physical file
wraps. Positions assigned only to an unsynced suffix may be reused after
recovery. A cursor identifies a position within one ring incarnation; persist
a separate incarnation identifier if files can be destructively replaced.

### Operations

- `append(AppendRequest)` accepts a non-empty batch atomically: every record or
  none. `expected_accepted_tail` makes the comparison and batch acceptance one
  atomic action. Input buffers are returned on both success and failure.
- `read(ReadRequest)` copies a bounded page of durable records. An expired
  cursor fails; a cursor at or beyond the durable tail returns an empty page
  without being clamped backward. If the first record cannot fit in
  `max_bytes`, the read fails with `ReadBudgetTooSmall` instead of making no
  progress indefinitely.
- `trim(before)` advances the accepted head monotonically and is idempotent for
  older cursors. It cannot advance beyond the durable tail observed at its
  ordered turn.
- `sync()` atomically publishes the accepted head and tail as one durable
  checkpoint, then makes storage below the new durable head reusable.
- `status()` reports logical bounds, retained and pending-reclaim resources,
  and optional provider-specific physical diagnostics.

Clones alias one session. Successfully admitted calls share a total order set
at method invocation (queue insertion for the file provider), not when their
futures are first polled. Dropping a response future abandons the response; it
does **not** cancel an admitted operation. Thus a later `sync` still fences an
earlier append whose future was never polled or was dropped.

### Completion certainty

Failure certainty describes the operation's documented logical effect:

- `NotApplied`: the effect definitely did not occur. A retry does not duplicate
  that operation, although intervening ordered operations may have changed the
  ring.
- `Applied`: the effect occurred despite the error. Append failures carry the
  assigned range and sync failures carry the installed checkpoint.
- `MayHaveApplied`: the caller must reconcile before retrying. For sync, close
  and reopen; recovery selects either the complete prior checkpoint or the
  complete target checkpoint, never mixed head and tail bounds.

A `MayHaveApplied` append may carry its candidate range. An append failure
always returns all caller-owned buffers. The concrete providers can offer
stronger outcomes than the trait permits; callers should program to the trait
contract.

## Bounds and backpressure

`RingLimits` fixes maximum record, live-record, live-payload, read-page, and
append-batch sizes. The file provider adds fixed physical data capacity, a
maximum lower-level I/O request size, and a bounded actor queue. Admission at a
full queue fails `NotApplied` with `RingError::Backpressure`; the ring never
silently overwrites protected records.

Accepted trims remain charged to retained limits until a sync checkpoint is
applied, including through an `Applied` failure. Physical capacity also
includes frame headers, checksums, and wrap padding, so an append can reach
`PhysicalCapacityReached` before its logical payload limit. Creation sets the
exact logical file length but does not promise filesystem block reservation; a
later write can still fail for lack of physical storage.

## Implementations

### `MemoryRing`

`MemoryRing` is the permanent logical oracle. Its operations take effect at
method invocation and return immediately-ready futures. Clones share state
through one mutex, so it is `Send` and cloneable across threads; concurrent
invocation order is the mutex acquisition order and is intentionally
unspecified. `MemoryRing::crash()` deterministically drops the unsynced suffix and
restores the last durable head, which makes it useful for model and simulation
tests. It validates logical semantics, not disk persistence or I/O ordering.

```rust
use kr_runtime_ring::{
    AppendRequest, MemoryRing, ReadRequest, RingCursor, RingLimits, RingReader,
    RingWriter,
};

async fn use_ring() -> Result<(), Box<dyn std::error::Error>> {
    let ring = MemoryRing::new(RingLimits::default())?;

    let appended = ring
        .append(
            AppendRequest::new(vec![b"alpha".to_vec(), b"beta".to_vec()])
                .expecting(RingCursor::START),
        )
        .await?;
    assert_eq!(appended.next_cursor, RingCursor::new(2));

    // Accepted records are not readable until fenced.
    let hidden = ring
        .read(ReadRequest::new(RingCursor::START, 16, 1024))
        .await?;
    assert!(hidden.records.is_empty());

    ring.sync().await?;
    let page = ring
        .read(ReadRequest::new(RingCursor::START, 16, 1024))
        .await?;
    assert_eq!(page.records.len(), 2);

    ring.trim(page.next_cursor).await?;
    ring.sync().await?; // makes the trim durable and reclaims both records
    Ok(())
}
```

### `FileRing<F: FileIoSubmit>`

`FileRingDriver::create` and `open` perform initialization or recovery without
spawning. `FileRingDriver::start` returns a cloneable `FileRing` handle and one
executor-neutral actor future. `FileRing::create` and `open` are convenience
forms that spawn that actor on the single-threaded `kr_runtime::Handle`.
Production hosts such as `UringRing` construct a `FileRingDriver` and drive the
future returned by `start` directly on their dedicated thread.

The actor owns the `FileIoSubmit` session exclusively and serializes a bounded number
of commands. Do not issue positional I/O through a retained clone of that file
while the ring is live. If the actor is terminated, admission closes and every
still-observed admitted response completes with `RecoveryRequired`; an
interrupted sync is `MayHaveApplied` once checkpoint metadata might have
reached the file, while other interrupted operations are `NotApplied` at the
logical ring boundary.

## On-disk format v1

The file has an exact configured length and all integers are little-endian:

```text
0                     4096                  8192
+---------------------+---------------------+---------------------------+
| superblock slot 0   | superblock slot 1   | circular data area        |
| 4 KiB, CRC32C       | 4 KiB, CRC32C       | data and padding frames   |
+---------------------+---------------------+---------------------------+
```

Each superblock records the format version, physical slot, generation, static
geometry, and one atomic logical/physical checkpoint. Generations alternate
between slots. Data frames contain a 32-byte checksummed header, absolute
sequence, payload, and trailing CRC32C. Explicit padding frames—or an implicit
short gap when less than one header remains—consume the rest of the data area
before allocation resumes at offset zero.

The sync protocol is:

1. Sync data frames written by accepted appends.
2. Write generation `n + 1` and the complete target checkpoint to the inactive
   superblock.
3. Sync the new superblock.
4. Publish the checkpoint in memory and reclaim records below its head.

If an inactive-slot write may have modified the slot, or its metadata sync
fails, the engine tries to invalidate that slot and sync the invalidation. A
confirmed invalidation leaves the old checkpoint recoverable and reports
`NotApplied`. If invalidation cannot be confirmed, the live handle enters
`RecoveryRequired` and the sync reports `MayHaveApplied`.

Opening validates the exact file length and configured geometry, independently
checks both superblocks, and selects the highest complete valid generation. A
valid pair must have matching geometry, canonical slot parity, and adjacent
generations. Recovery then scans the checkpointed interval from head to tail,
checking dense sequences, frame kinds, lengths, offsets, byte accounting, and
CRC32C values, and syncs the selected state before exposing it. Uncheckpointed
data is ignored. No valid checkpoint, split-brain metadata, non-canonical
encoding, inconsistent accounting, or committed corruption fails closed rather
than guessing.

The format is explicitly versioned as v1. Unsupported versions are rejected;
there is currently no in-place migration contract.

## Validation

The shared conformance suite is available with the `test-support` feature.
`MemoryRing` and `FileRing` are additionally compared by a deterministic model
campaign, while file-specific tests cover wraparound, corruption, injected I/O
failures, actor teardown, crash/reopen, and superblock write-prefix cuts.
The [interactive wrap/recovery trace](../../tools/trace-tool/README.md#file-ring-wrap-and-recovery-trace)
is generated from the corresponding file-specific test scenario.

```sh
cargo fmt --check
cargo test -p kr-runtime-ring --all-features
cargo clippy -p kr-runtime-ring --all-targets --all-features -- -D warnings
cargo test --workspace --all-targets
```

Comparable Criterion workloads for the simulated file engine and Linux
`io_uring` host live in the sibling
[`kr-runtime-ring-uring` benchmark](../kr-runtime-ring-uring/README.md#criterion-benchmarks).
