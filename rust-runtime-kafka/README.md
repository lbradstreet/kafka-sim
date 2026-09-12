# kr-runtime

The [branch overview](../README.md) connects this runtime to the Java producer
simulation harness, the [Rust Kafka producer](kafka/README.md), and the
[published behavior comparisons](reports/CAMPAIGN_RESULTS.md).

`kr-runtime` provides two concrete, single-owner executors for ordinary Rust
futures. `SimRuntime` owns deterministic ordering and virtual time so a run can
be inspected one scheduler action at a time and reproduced from the same
inputs. `HostRuntime` uses host-monotonic time and accepts cross-thread wakes so
the same style of local application task can await production I/O. They share a
small task and timer kernel, not a `Runtime` trait hierarchy.

The simulation kernel exposes a versioned reproduction fragment and terminal
determinism checkpoint, plus opt-in diagnostic traces, for a later exact replay
harness; that harness is not implemented yet.

The crate provides:

- an owner-thread local task contract that admits `'static` tasks which may be
  `!Send`;
- an exclusive deterministic `SimRuntime` controller;
- a single-owner `HostRuntime` controller, owner-local `HostHandle`,
  cross-thread `HostSendHandle` / `HostSendJoinHandle`, and `HostControl`
  stop/status capability;
- shared `Sleep`, `JoinHandle`, and `AbortHandle` task-facing types across both
  executors;
- a portable `RuntimeHandle` sum type so one application actor can spawn,
  sleep, read time, and draw workload randomness on either executor;
- one simulation FIFO runnable queue that is never randomized by the kernel;
- generation-tagged task IDs and stale-waker protection;
- coalesced wake-ups, including wake-during-poll;
- integer virtual time with stable equal-deadline timer ordering;
- explicit spawn, join, drop-to-detach, and abort semantics applied at scheduler
  boundaries;
- step-wise execution, bounded driving, snapshots, and stalled-run outcomes;
- a pinned, domain-separated deterministic RNG with golden vectors;
- independently versioned reproduction, terminal-checkpoint, and diagnostic
  trace contracts;
- fixed-event-capacity typed and fixed-byte-capacity SBE trace recorders with a
  shared rolling trace fingerprint, with no event construction on the default
  untraced path;
- a versioned binary SBE artifact and local interactive
  [trace explorer](tools/trace-tool/README.md);
- reasoned cancellation/runtime-stop events, bounded panic diagnostics, and
  replay-visible waker failures;
- structured task and destructor-panic failures;
- rejection of unrecorded cross-thread wake-ups inside simulation; and
- bounded host ingress that accepts production wake, abort, and `Send`-task
  spawn traffic and unparks the host owner.

The simulation policy deliberately does not use host sleeps, the host clock, OS
entropy, hidden threads, or unsafe code. Host execution deliberately uses the
host monotonic clock and thread parking, while the crate-wide
`#![forbid(unsafe_code)]` remains in force.

The `Schedule` random stream is reserved for higher-level deterministic
race/select and modeled I/O or network completion timing. All resulting wakes
still enter the kernel's FIFO ready queue. A future poll is one
atomic, zero-virtual-time scheduler action, so CPU-only work must yield or await
a modeled delayed operation before virtual timers can interleave with it.

Structured capture of task, destructor, and waker panics requires builds to use
`panic = "unwind"`. A `panic = "abort"` consumer can use the library, but the
process will terminate before either runtime can return those failures
structurally.

```rust
use kr_runtime::{SimDuration, SimRuntime};

let mut runtime = SimRuntime::default();
let handle = runtime.handle();
let task_handle = handle.clone();

let value = runtime.block_on(async move {
    task_handle
        .sleep(SimDuration::from_millis(10).unwrap())
        .await
        .unwrap();
    42
})?;

assert_eq!(value, 42);
assert_eq!(runtime.snapshot().now.as_nanos(), 10_000_000);
# Ok::<(), kr_runtime::RunError>(())
```

The host controller has the same single-owner shape without simulation driving
or replay state:

```rust,no_run
use kr_runtime::{HostConfig, HostRuntime, RuntimeDuration};

# fn main() -> Result<(), Box<dyn std::error::Error>> {
let mut runtime = HostRuntime::new(HostConfig::default())?;
let handle = runtime.handle();

let value = runtime.block_on(async move {
    handle
        .sleep(RuntimeDuration::from_millis(10).unwrap())
        .await?;
    Ok::<_, kr_runtime::TimeError>(42)
})??;

assert_eq!(value, 42);
runtime.finish()?;
# Ok(())
# }
```

`HostRuntime::handle()` returns the owner-local `HostHandle`, which admits
`!Send` tasks. `send_handle()` returns the `Clone + Send + Sync`
`HostSendHandle` for bounded ingress of `Send + 'static` tasks from other
threads. `control()` returns `HostControl`; its idempotent `request_stop`
bypasses ordinary ingress capacity, and `status()` observes the `HostStatus`
lifecycle without driving the runtime.

`SimRuntime::block_on` stops when its root future completes and leaves other
tasks in the simulation runtime. A harness that requires global quiescence
should then call `run_until_stalled` and inspect whether it returned `Idle`,
`Stalled`, or `Stopped`. The stopped result is distinct from ordinary
empty-runtime quiescence. At the ownership boundary, call `SimRuntime::finish`
so destructor and registered-waker failures discovered during teardown cannot
be silently lost through plain `Drop`.
A replay artifact must also pin the versioned harness driver, stop/drain policy,
and action, virtual-time, and process-watchdog budgets; the seed and runtime
configuration alone do not define a replay.

`SimRuntime::new` and `SimRuntime::default` disable diagnostic tracing before
an `EventKind` or `TraceEvent` is constructed. `SimRuntime::with_trace` opts in
to event construction and delivery. `RuntimeReproduction` is only the
kernel-owned fragment of a complete harness manifest, while
`DeterminismCheckpoint` is a cheap terminal rerun canary that excludes
debug-only randomness and includes total task-enqueue and timer-registration
volume. A checkpoint must be compared together with the typed harness outcome
and application/model-checker result. `RecordingTrace` can retain a bounded
event-count prefix, tail, or prefix plus tail. `SbeRecordingTrace` provides the
same retention modes in bytes: it allocates its final buffers up front and
encodes complete, length-framed SBE events directly into them without
per-event allocation. Both recorders compute the same versioned canonical
fingerprint over every ordering-valid observed diagnostic event, including
events beyond retention capacity; the fingerprint is defined over typed event
content, not Rust layout, retained bytes, or SBE wire bytes. Use one recorder
per runtime; independent runtime sequence spaces cannot be combined into one
meaningful trace. The fingerprint is trace integrity metadata, not reproduction
identity. `SamplingTrace` can wrap either sink with deterministic every-N event
sampling; rejected samples skip virtual-time reads and event construction while
preserving the original sequence positions as gaps. The wrapped sink's
fingerprint covers only the sampled subsequence, so the harness must persist the
sampling period and phase with any artifact.

After a run, `kr-runtime-trace-tool` provides zero-copy event-frame export from an
`SbeRecordingTrace` into a versioned binary artifact: it writes the retained
slices without decoding, allocating, or re-encoding those frames. The browser
viewer validates and decodes that binary artifact locally into bounded
in-memory presentation records, avoiding a JSON-sized intermediate. Runtime
trace artifacts intentionally have one persisted form: binary SBE. This keeps
filesystem I/O and presentation serialization out of the simulation path. See
the [trace explorer guide](tools/trace-tool/README.md)
for runnable capture and visualization examples and the exact compatibility
contract.

Simulation actors run on `Handle`; host local actors run on `HostHandle`. Both
controllers remain exclusive, so an actor cannot recursively drive an event
loop. I/O provider traits remain independent from either executor; their
`Send*` companions prove that handles and every associated operation future can
cross threads when a production host requires it. `CompletionCertainty`,
`CompletionError`, and `CompletionResult` provide the shared `NotApplied` /
`Applied` / `MayHaveApplied` vocabulary for side-effecting drivers. The
[`kr-runtime-io`](io/kr-runtime-io/README.md) layer applies that boundary to storage, byte
streams, and atomic datagrams. The thread-safe `Memory*` providers support
concurrent host tests; deterministic `Sim*` providers add fault control; and
Linux `UringFile`/`UringByteStream` implement the same basic APIs without
leaking io_uring SQEs or CQEs into application or executor contracts. Their
dedicated hosts complete through ordinary wakers, which `HostRuntime` accepts
through bounded ingress while `SimRuntime` rejects as nondeterministic external
input. See the [runtime boundary guide](PRODUCTION-RUNTIME.md) for the full
contract.

The [Quarry dogfood crate](dogfood/quarry/README.md) is a bounded leased work
queue that exercises the runtime through a passive engine, a host broker actor
with a deterministic simulation sibling, and `DurableQueue<R>` over the shared
`RingWriter` contract.
`MemoryRing` provides the logical reference path, the checksummed `FileRing<F>`
is tested over fault-injectable `SimStorage`, and the
[Linux io_uring host](storage/kr-runtime-ring-uring/README.md) exposes that same
ring engine as `UringRing` without feeding nondeterministic kernel wakes into
the simulation runtime.

See [DESIGN.md](DESIGN.md) for the completion-based I/O contracts used by the
simulated and io_uring providers, and the path toward broader
FoundationDB-style process, fault, workload, and checker layers.

See [BENCHMARKING.md](BENCHMARKING.md) for the overhead baseline ladder,
interpretation rules, smoke commands, and host metadata required for reportable
performance runs.

## Development

The default local loop excludes the two Linux-only `io_uring` adapter crates.
It still enables every portable test-support feature and runs the complete
non-benchmark test suite:

```text
just t                         # all portable tests
just tp kr-runtime timer             # one package, filtered by test name
just tt kr-runtime runtime timer     # one integration-test target
just c                         # portable cargo check
just r                         # format, lint, doctest, and test gate
```

`just test-uring` is the explicit Linux-only adapter test path. Extra arguments
to the test recipes are passed to nextest as test-name filters. Use
`cargo nextest run ... -E <filterset>` directly for advanced filtersets.

The equivalent full workspace commands remain:

```text
cargo test --workspace --all-targets --all-features
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo fmt --all -- --check
cargo bench -p kr-runtime --features benchmarks --bench runtime_trace
cargo bench -p kr-runtime --features benchmarks --bench scheduler_overhead
cargo bench -p kr-runtime --features benchmarks --bench host_overhead
```
