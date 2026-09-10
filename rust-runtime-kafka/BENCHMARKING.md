# Overhead Benchmarks

This workspace answers four separate performance questions:

1. What does deterministic scheduling cost relative to directly polling the
   same future work?
2. What do host single-owner spawn, wake-ingress, and park/unpark paths cost?
3. What does the production owned-I/O boundary cost relative to raw io_uring?
4. What do the two executors cost on identical owner-local workloads, and
   what does portable `RuntimeHandle` dispatch add over a concrete handle?

These numbers must not be combined. `SimRuntime` rejects foreign-thread wakes
and does not drive production io_uring completions. `HostRuntime` accepts those
wakes through bounded ingress, but it still does not own an io_uring reactor:
`UringFile` and the other Linux providers keep their dedicated bounded hosts and
ordinary response futures. `SimRuntime::new` and `SimRuntime::default` remain the
fast, untraced simulation path, with no trace sink or event construction.

## When to run what

| Changed area | Run |
| --- | --- |
| scheduler, task, timer, RNG, or handle paths in `src/` | `scheduler_overhead`, `executor_parity` |
| host ingress, wake, or park paths in `src/host.rs` | `host_overhead`, `executor_parity` |
| trace recorders, fingerprinting, or SBE export | `runtime_trace`, `trace_artifact` |
| `kr-runtime-io-uring` providers, actors, or the reactor | `file_overhead` |
| `kr-runtime-io-uring` stream providers or the pooled coordinator | `stream_overhead`, `stream_many_connections` |
| the `ColdFile` façade or cold-future wrappers in `kr-runtime-io` | `cold_file_overhead` |
| ring format, checkpoint, sync, or recovery paths | `ring` (both backends) |

## Comparing runs

Regression detection is a local A/B against a saved Criterion baseline, not a
comparison of numbers collected on different days or machines:

```text
cargo bench -p kr-runtime --features benchmarks --bench scheduler_overhead -- --save-baseline before
# apply the change
cargo bench -p kr-runtime --features benchmarks --bench scheduler_overhead -- --baseline before
```

Trust a delta only when Criterion reports it as a change, it survives a
repeated run, and it exceeds the noise you observe between two unchanged
runs on the same host; on a shared developer machine treat small
single-digit-percent deltas as no-signal. Baselines live in `target/` and
are never committed; a reported result is the run plus the environment
record below, not a stored artifact in this repository.

## Quick smoke checks

Smoke mode validates benchmark setup and result checking. Its measurements are
not performance results.

```text
cargo bench -p kr-runtime --features benchmarks --bench scheduler_overhead -- --test
cargo bench -p kr-runtime --features benchmarks --bench host_overhead -- --test
cargo bench -p kr-runtime --features benchmarks --bench executor_parity -- --test

KR_RUNTIME_IO_URING_BENCH_DIR=/path/on/the/target/filesystem \
  cargo bench -p kr-runtime-io-uring --bench file_overhead -- --test
```

The io_uring benchmark is Linux-only. It creates isolated temporary benchmark
files beneath `KR_RUNTIME_IO_URING_BENCH_DIR`, or beneath the host temporary directory
when that variable is absent.

## Scheduler benchmark

Run the complete optimized suite with:

```text
cargo bench -p kr-runtime --features benchmarks --bench scheduler_overhead
```

The direct cases are lower bounds, not alternative executors. Matched
poll/wake cases execute the same future state machine directly and through an
untraced `SimRuntime`. Spawn/join cases additionally measure task IDs, ready
admission, typed joins, and cancellation state requested by the workload.
Runtime construction and fixture allocation stay outside the measured routine
unless a case is explicitly named as a lifecycle benchmark.

## Host scheduler benchmark

Run the host scheduler suite with:

```text
cargo bench -p kr-runtime --features benchmarks --bench host_overhead
```

The suite measures sequential spawn/join throughput, same-thread wake-to-poll,
foreign-thread wake-to-poll, and a park/unpark round-trip canary. Foreign wake
cases include an `mpsc` handoff to a persistent worker, worker scheduling, the
coalescing marker, ingress mutex, and `Thread::unpark`; they are neither
deterministic nor pure task-poll microbenchmarks. The park/unpark case also
includes a fixed 50-microsecond settle delay so the owner has time to park.
Report host, affinity, load, and latency distribution, and do not subtract these
numbers from direct or simulation cases as if the difference isolated one
abstraction layer.

## Trace benchmarks

`runtime_trace` compares trace policies against the untraced runtime. Its
`disabled` cases are useful simulation fast-path regression canaries, but have
no direct-execution or host-runtime baseline. `trace_artifact` in
`kr-runtime-trace-tool` measures recorder retention and zero-copy artifact export:

```text
cargo bench -p kr-runtime --features benchmarks --bench runtime_trace
cargo bench -p kr-runtime-trace-tool --features benchmarks --bench trace_artifact
```

## Executor parity benchmark

Run the matched-executor suite with:

```text
cargo bench -p kr-runtime --features benchmarks --bench executor_parity
```

Each group runs one workload, written once against the portable
`RuntimeHandle`, on the untraced `SimRuntime` and on `HostRuntime`: yield
churn, sequential spawn/join, an actor request/reply ping-pong over
single-slot mailboxes (one wake and one poll per side per round), timer
registration plus cancellation (no case waits for a deadline), and seeded
workload draws with a raw `DeterministicRng` floor. The executors share the task, timer, and join
kernel, so a case difference measures scheduler policy: virtual time and
deterministic wake admission in simulation versus monotonic clock reads and
per-turn ingress and lifecycle checks on the host. No case crosses a thread,
parks, traces, or performs I/O — foreign-wake costs stay in `host_overhead`,
and direct-execution baselines for the simulation cases stay in
`scheduler_overhead`; do not compare across suites.

The `portable_handle_dispatch` group isolates what the `RuntimeHandle` enum
match adds over the same call on a concrete handle, using the
highest-frequency handle operation. Compare its concrete and portable cases
within one executor only. The random-draw cases double as a parity check:
both executors must reproduce the raw generator's exact checksum, so a
benchmark run also revalidates seeded sim/host equivalence.

## Cold façade benchmark

Run the cold file façade suite with:

```text
cargo bench -p kr-runtime-io --features benchmarks --bench cold_file_overhead
```

The suite answers one question: what the `ColdFile` façade adds over calling
the same eager `FileIo` handle directly. Every case is a matched warm/cold
pair on one provider — immediately ready memory completions, a genuinely
pending simulated write, and cold construct-plus-unpolled-drop against a
request-construction floor — and the run reports warm and cold future sizes
at startup. It is CPU-only and valid for relative developer-machine
comparison. It is not an I/O throughput measurement, and its sim-backed cases
must not be compared against the io_uring `file_overhead` suite.

## File io_uring benchmark

Run the complete optimized suite on Linux with:

```text
KR_RUNTIME_IO_URING_BENCH_DIR=/mnt/benchmark-filesystem \
  cargo bench -p kr-runtime-io-uring --bench file_overhead
```

The file benchmark uses a queue depth of one and separates three implementations:

- `raw_direct`: the caller owns and drives `io_uring::IoUring` directly;
- `raw_actor`: a bounded dedicated actor owns the same raw ring;
- `uring_file`: `UringFile` adds the provider-neutral request, validation,
  completion-certainty, owned-future, and teardown contracts.

Interpret the deltas as follows:

- `raw_actor - raw_direct` estimates dedicated-thread and queue handoff cost;
- `uring_file - raw_actor` estimates the additional owned-I/O provider cost.

The current benchmark is deliberately QD1: each iteration awaits one logical
operation before submitting the next. The `UringFile` actor itself can batch
queued reads and queued non-overlapping writes up to its user-SQE limit. A
multi-inflight benchmark would exercise that batching and measure throughput,
not the matched QD1 overhead baseline, and must use a distinct name.
Registered buffers, fixed files, SQPOLL, direct I/O, and batched submission must
also be reported separately unless every compared backend uses them.

Reads and writes are deliberately separate. Durability benchmarks must compare
the same fence cadence and filesystem semantics; an append followed by `fsync`
is not comparable to an unfenced write. Likewise, a raw append stream without
the ring format, checksums, checkpoint protocol, and recovery
guarantees measures feature cost rather than abstraction overhead.

## File multi-inflight benchmark

```text
KR_RUNTIME_IO_URING_BENCH_DIR=/mnt/benchmark-filesystem \
  cargo bench -p kr-runtime-io-uring --bench file_multi_inflight
```

This suite is the deliberately distinct multi-inflight throughput
measurement: every case submits eight operations before awaiting any, reads
and writes separately, hot-cache and unfenced. It compares `uring_file`
(per-file actor, which may publish the batch as commuting SQEs) against
`pooled_file` (the shared-ring pool, which pipelines a file's commuting
prefix and reorders responses to admission order) in two shapes:

- `one_file_qd8`: eight operations on one file at disjoint offsets — the
  per-file batching shape both backends must keep fully in flight;
- `eight_files_qd1`: one operation on each of eight files — the pool's
  intended shape, one ring and three threads against eight rings and
  sixteen threads.

Its numbers are throughput under concurrency and must not be compared with
`file_overhead`, which is the matched QD1 overhead baseline. Both backends
must keep identical ring depth, chunk limits, payloads, and fixtures.

## Stream echo benchmarks

```text
cargo bench -p kr-runtime-io-uring --bench stream_overhead
cargo bench -p kr-runtime-io-uring --bench stream_many_connections
```

`stream_overhead` is the matched QD1 overhead baseline for connected TCP
streams: one loopback connection, one echo round trip awaited at a time,
against the same std-blocking peer thread, with `TCP_NODELAY` everywhere.
Its three cases separate the layers:

- `std_blocking`: `write_all`/`read_exact` syscalls, the loopback floor;
- `uring_stream`: the per-stream actor provider (three threads and a
  private ring per connection);
- `pooled_stream`: the shared-ring pool (routed sustained SQEs on one
  coordinator).

`stream_many_connections` is the deliberately distinct concurrency
measurement: each iteration submits one 4 KiB write on every connection
before awaiting any, then one read on every connection before awaiting
any, so all connections have work in flight while each stays at QD1.
Both backends run 8 and 64 connections; the pool alone also runs 256,
because beyond 64 the per-stream provider's three-threads-and-three-
descriptors-per-connection cost approaches common host limits — the wall
the pool exists to remove. Its numbers are throughput under concurrency
and must not be compared with `stream_overhead`. Loopback echo keeps both
suites hot-cache and kernel-bound; they measure provider overhead and
coordinator scheduling, not network hardware.

First recorded run (2026-07, Fedora 43 aarch64 VM, kernel 7.1.4, 4 vCPUs,
7.7 GiB, rustc 1.97.1, shared virtualized host — relative comparison
only): at QD1 the pooled and per-stream backends are indistinguishable
(98–101 µs per round trip at both 64 B and 4 KiB, against a ~25 µs
blocking-syscall floor), so pooling three threads and a ring per
connection down to none costs nothing per operation. Under the barrier
shape both backends tie at 8 connections (~150 µs), the pool completes
64-connection barriers 1.6x faster (405 µs, ~158 K round trips/s, versus
644 µs, ~99 K/s, with the per-stream fleet's 192 threads contending for
4 vCPUs), and the pool alone sustains 256 connections at ~185 K round
trips/s. The scale claim held; a dedicated many-core host should widen
the 64-connection gap, not shrink it.

The `ring` benchmark in `kr-runtime-ring-uring` remains the end-to-end ring
measurement, running the same file-ring state machine over the zero-latency
simulated backend and over physical io_uring. Its `file_pooled_buffered`
backend drives the identical engine and fence cadence over the shared-ring
`UringIoPool` provider, so `file_uring_buffered` versus `file_pooled_buffered`
is the ring-level comparison of the two file architectures; the simulated
case remains a CPU baseline and is comparable to neither.

```text
cargo bench -p kr-runtime-ring-uring --bench ring
```

Its simulated-versus-io_uring result must not be described as runtime
overhead: the simulated backend is a CPU/state-machine baseline while the
io_uring cases include buffered kernel I/O and real durability fences, so the
storage and durability mechanisms differ by design.

The `file_uring_odsync` and `file_pooled_odsync` backends open the same
backing file with `O_DSYNC`, putting a durability write-through on every
frame write. They measure per-write durability cost, not write concurrency:
concurrent same-file `O_DSYNC` writes serialize on the inode write lock on
ext4 and XFS, so measured cost stays linear in frame count whether or not
the ring pipelines its append plan (verified 2026-07: serial and pipelined
appends measured identical under `O_DSYNC` on a Fedora 43 aarch64 VM, ~70 µs
per frame write, while both stayed fence-dominated and change-neutral under
the buffered backends).

A tuned write-through measurement is deferred until ring throughput binds
or an aligned-frame format is being evaluated: XFS on a real NVMe device, a
`fallocate`-preallocated backing file so `O_DSYNC` writes never allocate
extents, `noatime`/`lazytime` mount options so timestamp maintenance stays
out of the write path, and the mount and device details retained per the
protocol below. Expect it to shrink the per-write constant, not to lift the
linear-in-frames shape — overlapping durable writes to one file needs the
shared inode locking of aligned `O_DIRECT` writes, which the packed frame
format precludes today.

## Ring at-capacity churn benchmark

```text
KR_RUNTIME_RING_URING_BENCH_DIR=/mnt/benchmark-filesystem \
  cargo bench -p kr-runtime-ring-uring --bench ring_churn
```

A fixed-duration custom driver, not a Criterion suite: one thread appends
single records as fast as admission allows while a second concurrently
maintains the ring with the sync-trim-sync checkpoint cadence, starting from
a prefilled at-capacity ring. It reports absolute append-to-accepted latency
percentiles (p50/p90/p99/p99.9/max, including any wait for reclaim), the
capacity-refusal rate, throughput, and maintenance fence percentiles, for
both the per-file-actor and pooled backends. `KR_RUNTIME_RING_CHURN_SECS` sets the
measured duration per backend (default 10). Its numbers are a saturation
workload and must not be compared with `ring`, which measures isolated
operations far from capacity.

## Measurement protocol

The protocol is tiered by what the suite touches. The CPU-only suites
(`scheduler_overhead`, `executor_parity`, `runtime_trace`, `trace_artifact`,
and the same-thread `host_overhead` cases) are valid for *relative*
before/after comparisons on a developer machine, including macOS; use the
baseline workflow above and hold the machine reasonably quiet. Absolute
claims, cross-machine comparisons, and every I/O-bearing or foreign-thread
suite (`file_overhead`, `ring`, the foreign-wake and park cases) need a
quiet, dedicated Linux host.

For every reported run, retain:

- exact Git commit and dirty-worktree status;
- `rustc -vV` and Cargo versions;
- CPU model, topology, governor, turbo policy, affinity, and NUMA placement;
- memory size;
- benchmark filters, payload sizes, sample settings, and environment variables.

For I/O-bearing suites, additionally retain:

- kernel version and io_uring availability;
- huge-page settings;
- block device, scheduler, filesystem, mount options, free space, and file path;
- whether the workload is hot-cache, cold-cache, real-device, or durability-bound.

Pin the benchmark and actor threads consistently across implementations. Do not
mix tmpfs, page-cache, and physical-device measurements. Keep file creation,
opening, connection setup, and buffer allocation outside timed regions except
in explicit lifecycle cases.

Report absolute values for every implementation, not only ratios:

- completed operations or bytes per second;
- latency distribution, including p50, p99, and p99.9 when individually timed;
- user and system CPU time or CPU-nanoseconds per operation;
- cycles, instructions, branches, cache misses, context switches, and migrations;
- allocations and peak resident memory;
- submitted SQEs and completed CQEs per logical operation;
- admission or queue-full rate under saturation workloads.

Criterion provides stable steady-state estimates. Use a fixed-duration custom
driver or an external histogram when per-operation tail latency matters, and
use `perf stat` or an equivalent profiler around the unchanged benchmark binary
for hardware and scheduler counters.
