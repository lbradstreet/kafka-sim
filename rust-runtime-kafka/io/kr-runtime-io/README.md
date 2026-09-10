# kr-runtime-io

`kr-runtime-io` defines the owned completion boundary shared by deterministic I/O
models and production drivers. Every future is `'static`, and dropping a
future after admission abandons only its response. Side-effecting failures use
`NotApplied`, `Applied`, or `MayHaveApplied` certainty from `kr-runtime`.

Every I/O domain has two layers. Application code uses the cold handles —
`ColdFile`, `ColdNetwork`/`ColdListener`/`ColdStream`, and
`ColdDatagramNetwork`/`ColdDatagramSocket`: calling an operation method
constructs a future and admits nothing, the first poll attempts admission
exactly once, and a never-polled future has not started. Successful `listen`,
`connect`, `accept`, and `bind` operations return cold-wrapped handles.
Providers and systems code such as `kr-runtime-ring` use the warm `*Submit` traits,
where a `submit_*` call attempts admission during the method invocation (the
staged migration is recorded in `COLD-IO-FUTURES-PROPOSAL.md`).

Admission is not implicit cancellation. A dropped pending read can still
consume later bytes, and a dropped pending accept can still consume its FIFO
connection; both retain their bounded in-flight capacity until terminal
completion. The provider owns request buffers until then. Callers that need
cancellation must use an explicit cancellation operation when one is added to
the contract rather than relying on `Future::drop`. Only a *never-polled* cold
future is known not to have started.

The first three contracts are intentionally small:

- `FileIoSubmit` represents one already-open file with positional read/write,
  length, truncate, and sync submission operations, with `ColdFile` as the
  application-facing cold layer over any implementation. Providers own path
  opening, locking, and lifecycle policy.
- `ByteStreamSubmit` represents one connected, ordered byte stream with
  partial read/write, half-close, and close, with `ColdStream` as the
  application-facing cold layer. Provider factories own real or simulated
  address setup. EOF follows TCP: a read returning final buffered bytes is not
  EOF; the next successful nonzero-capacity zero-byte read is. Zero-capacity
  reads never report EOF.
- `DatagramSocketSubmit` represents one bound atomic-message transport that can
  communicate with many peers. Sends never partially succeed, receives return
  source and truncation metadata, and blocking, nonblocking, and absolute
  deadline receive modes share owned buffers. `DatagramProviderSubmit` is the bind
  control plane, with `ColdDatagramSocket` as the application-facing cold
  layer. The shared transport does not assign operation or delivery
  identities; protocols needing end-to-end correlation encode it in their
  payload. Provider-private handles and trace correlation stay behind the
  contract. `MemoryDatagramNetwork` provides a thread-safe deterministic
  implementation, while `SimDatagramNetwork` adds owner-thread virtual latency
  and fault control.

`SimStorage` models accepted and durable bytes separately. Writes and length
changes modify the accepted image; `sync` fences it; `crash` discards the
unsynced image and allows the same `SimDisk` to reopen. Fixed request, file,
queue, transfer, and fault-script bounds are enforced at admission. A
versioned pipeline model selects how admitted operations reach the virtual
device: the serial default completes strictly in admission order, while the
commuting-overlap mode runs reads with reads and non-overlapping writes
concurrently — the classification the Linux file pipelines use — so
commuting completions arrive in latency order and, under a jittered latency
model, explore seed-dependent, replayable reorderings. Fences (`sync`,
`set_len`, `len`) and effect equivalence are identical in both modes.

`SimNetwork` provides connected pairs plus a bounded listen/connect/accept
control plane. Each direction has independent capacity, partial-transfer size,
latency, clog/partition state, half-close state, and scripted faults. It uses
only virtual timers and local runtime tasks—never host sockets, sleeps, time,
or entropy. For this simulator specifically, link latency delays local operation
completion while admitted bytes may become peer-visible earlier;
`ByteStreamSubmit` does not make that timing a cross-provider guarantee.

`connected_pair_with_propagation` opts a pair into independent directional
peer-visibility delays and fixed, absolute outage windows. Bytes in transit
remain in the bounded directional pipe and consume its capacity. Black holes
postpone visibility to the end of their half-open window, including previously
queued bytes due during that window. Already visible bytes remain readable.
Fail-fast windows retire the whole affected pair before same-time arrivals and
reject new pairs until recovery. Closing either endpoint cancels transit bytes,
pending operations and propagation timers without waiting for recovery;
half-close preserves ordered transit before EOF. Setup handshake deadlines are
the connector's responsibility, using the same byte policy.

Propagation uses at most one scheduled wake per direction and an arrival queue
bounded by resident pipe bytes. It consumes no random draws. Fixed profiles make
outage timing independent of timer callback order. Ordinary pairs allocate no
propagation queues or tasks and retain the existing local-completion semantics.
The propagation tests cover directional timing, occupied pipe capacity, partial
and vectored writes, known progress on failure, one-way stalls, setup rejection,
recovery, half-close, and complete ownership reclamation during an outage.

`MemoryFile`, `MemoryNetwork`, and `MemoryDatagramNetwork` are bounded,
host-I/O-free providers whose handles and owned futures implement the `Send*`
companion contracts. They support concurrent host-side contract tests without
introducing provider-owned threads or provider-driven wall-clock progress.
Their concurrent order is mutex-acquisition order; the richer `Sim*` providers
remain the choice for replayable fault and latency plans.

The simulation providers model io_uring-style owned completions rather than the
kernel's SQ/CQ shared-memory ABI. Simulation and Linux use the same owned file and
stream requests, partial completions, admission rules, and completion
certainty; only the provider beneath that boundary changes. Shared file,
connected-stream, network-provider, and datagram-provider conformance functions
run unchanged across compatible implementations without forcing atomic
messages through the stream contract.

Fault and latency plans are explicit inputs. A campaign can generate them from
the runtime's `Schedule` and `Fault` random streams and retain that realized
plan in its replay artifact.

Provider authors complete operations through the shared `completion` module:
`LocalOperation` for owner-thread
providers and `SyncOperation` (obtained via `SyncOperation::channel`) for
providers that complete from another thread. Every provider future exported by
this crate and by `kr-runtime-io-uring` is an alias of these two types, which own the
polling, waker-containment, delivery-gating, and bounded-admission-permit
rules described in the workspace [`DESIGN.md`](../../DESIGN.md).

## Owned vectored stream writes

`ByteStreamVectoredSubmit` extends the warm stream contract with
`submit_write_vectored(VectoredWriteRequest { segments })`; application code
uses `ColdStream::write_vectored` with the same first-poll admission semantics.
Each `WriteSegment` contains a `SharedBytes` view and a `Range<u32>` within
that view. `SharedBytes` comes from the runtime-independent `kr-shared-bytes`
leaf crate and retains its `Arc<[u8]>` allocation across provider threads.

`VectoredWriteRequest::validate(max_segments, max_operation_bytes)` bounds
the segment vector's capacity, checked payload length, and complete retained
allocations. Multiple views of one allocation charge it once per operation;
a tiny view still charges the whole backing allocation. Empty segments,
invalid ranges, and oversized requests fail before effects or fault draws.
Memory and simulation streams accept at most 64 segments and append their
ordered prefixes directly into the existing directional pipe. Vectored and
contiguous writes share invocation order, completion limits, and fault plans.

Success returns the original segment vector and exact `bytes_written` prefix.
Failure returns all segments, known `bytes_transferred`, and certainty in the
ordinary `CompletionError`. Dropping an admitted response abandons observation
while the provider retains storage through terminal completion. The simulator
charges shared storage to its existing write byte budget, independently of
read bytes. The common operation-count admission bound still applies to both
directions. A producer must reserve enough operation slots for receives.

The shared warm and cold vectored conformance suites check validation,
allocation identity, ordered partial progress, abandonment, and close.
Their exhaustion companions verify resource rejection before effects. The
blocked-operation companion suites use a deliberately full directional
pipe to check retained ownership and independent acknowledgement reads.

```rust
use kr_runtime_io::{ColdFile, SimDisk, SimStorageConfig, WriteAtRequest};
use kr_runtime::SimRuntime;

let mut runtime = SimRuntime::default();
let disk = SimDisk::default();
let file = ColdFile::new(disk.open(runtime.handle(), SimStorageConfig::default())?);

runtime.block_on(async move {
    file.write_at(WriteAtRequest::new(0, b"record".to_vec()))
        .await
        .unwrap();
    file.sync().await.unwrap();
})?;
# Ok::<(), Box<dyn std::error::Error>>(())
```

## Validation

The shared conformance suite is available with the `test-support` feature.

```text
cargo test -p kr-runtime-io --all-targets --all-features
cargo clippy -p kr-runtime-io --all-targets --all-features -- -D warnings
cargo fmt -p kr-runtime-io -- --check
```
