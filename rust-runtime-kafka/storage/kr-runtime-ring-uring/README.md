# io_uring disk ring

`kr-runtime-ring-uring` is the Linux production adapter for
`kr-runtime-ring`'s owned `RingReader` / `RingWriter` contract. It is a thin
host for `FileRing<UringFile>`: the circular allocation and crash protocol are
the same code exercised over deterministic storage, while a dedicated thread
drives the ring actor and the file provider owns the kernel ring.

The backend provides:

- bounded FIFO ring and file actors with invocation-time ordering;
- owned `'static` operation futures and typed backpressure;
- an exclusive advisory writer lock held until the final clone closes;
- exact-length, checksummed circular storage with two alternating checkpoints;
- durable-only reads and an explicit `sync` fence for accepted appends/trims;
- conservative commit-unknown handling that requires close and recovery; and
- separate logical, physical-file, actor-queue, ring-entry, request, and SQE
  bounds.

`create` durably initializes an empty file and never truncates a nonempty one.
It creates a missing path and fences the parent directory. `open` requires an
existing exact-length file, selects the newest complete checkpoint, rebuilds
the bounded index, and fences recovery before returning. Both report
`CompletionCertainty`; callers must reconcile `MayHaveApplied` initialization
failures instead of blindly recreating the path.

```rust,no_run
use kr_runtime_ring::{AppendRequest, RingWriter};
use kr_runtime_ring_uring::{UringRing, UringRingConfig};

# async fn example() -> Result<(), Box<dyn std::error::Error>> {
let ring = UringRing::create("events.dstr", UringRingConfig::default())?;
let accepted = ring
    .append(AppendRequest::new(vec![b"record".to_vec()]))
    .await?;
let durable = ring.sync().await?;
assert_eq!(accepted.next_cursor, durable.durable_tail);
ring.close()?;
# Ok(())
# }
```

`UringRing` is cloneable because the shared contract is cloneable. Closing one
clone releases only that handle. The final explicit `close` drains admitted
commands and waits up to `shutdown_timeout` for both actors. A timeout detaches
the host: it keeps the file lock, buffers, and kernel pointer targets alive until
the underlying operation terminalizes. Implicit `Drop` is nonblocking for the
same safety reason and cannot report actor failure. Creation and recovery are
similarly bounded by `startup_timeout`.

The fixed file uses ordinary buffered I/O. `data_capacity_bytes` is a logical
circular allocation bound, not a promise that `set_len` reserved filesystem
blocks; later writes can still report an out-of-space backend failure.

## Criterion benchmarks

The `ring` benchmark runs identical API-level workloads against
`FileRing<SimStorage>` on every platform and `UringRing` on Linux. It reports
user-payload throughput; frame headers, checksums, superblocks, and wrap
padding are deliberately not counted as payload bytes.

| Workload | Timed boundary |
| --- | --- |
| `append_accepted_maintenance_excluded` | Batch append through completed frame writes; checkpoint/reclaim maintenance is excluded |
| `sync_dirty_append_excluded` | Dirty durability fence only; the preceding append and later reclaim are excluded |
| `read_durable_fixed_hot_page` | Repeated owned reads of one preloaded durable page; result destruction is excluded |
| `append_sync_trim_sync` | Complete repeatable append → sync → trim → sync lifecycle |

Each workload uses `r1-p64`, `r1-p4096`, `r16-p1024`, and `r64-p16384`, where
`r` is records per batch/page and `p` is payload bytes per record. Payload
buffers and rings are created outside the timed iterations, and append buffers
are reused from `AppendSuccess`. This first suite is deliberately sequential;
queue-depth, concurrent-producer, cold-cache, and recovery benchmarks can be
layered on after this baseline is stable.

Run the deterministic file-engine baseline on any platform:

```text
cargo bench -p kr-runtime-ring-uring --bench ring -- file_sim_zero_latency
```

Run the production backend on Linux, preferably on an explicitly selected
filesystem rather than a possibly memory-backed `/tmp`:

```text
KR_RUNTIME_RING_URING_BENCH_DIR=/mnt/bench-xfs \
  cargo bench -p kr-runtime-ring-uring --bench ring -- file_uring_buffered
```

Omit the final filter to run both backends on Linux. A fast correctness smoke
run is:

```text
cargo bench -p kr-runtime-ring-uring --bench ring -- --test
```

The simulated result is a deterministic CPU, actor, format, and runtime
baseline—not simulated device latency. `SimStorage::sync` snapshots the entire
configured exact-length file, whereas `UringRing` performs buffered kernel I/O
and real `fsync` fences. Record the filesystem, mount options, device, kernel,
and CPU when comparing production runs.

## Validation

On Linux:

```text
cargo test -p kr-runtime-ring-uring --all-targets
cargo clippy -p kr-runtime-ring-uring --all-targets -- -D warnings
```

Set `KR_RUNTIME_RING_URING_TEST_DIR` to run the integration suite on a particular
mounted filesystem:

```text
KR_RUNTIME_RING_URING_TEST_DIR=/mnt/test-xfs cargo test -p kr-runtime-ring-uring --test ring
```
