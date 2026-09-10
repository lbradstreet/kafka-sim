# kr-runtime-io-uring

`kr-runtime-io-uring` is the Linux production provider for the owned operations in
`kr-runtime-io`. It currently provides:

- `UringFile`, an exclusively locked single-file `FileIoSubmit` implementation;
- `UringNetwork` / `UringListener`, the `NetworkProvider<SocketAddr>` control
  plane using io_uring connect and accept; and
- `UringByteStream`, its connected `ByteStream` data plane using io_uring recv,
  send, and shutdown;
- `UringDatagram` / `UringDatagramSocket`, an IPv4/IPv6 UDP
  `DatagramProvider<Address = SocketAddr>` using io_uring sendmsg and recvmsg.

Each file handle uses a bounded FIFO actor and publishes commuting reads or
non-overlapping writes as a bounded SQE batch. Provider control and listener
actors share one eventfd-driven reactor; each connected stream shares another
between its read and write/control actors. Datagram sockets share their
provider's reactor. Independent operations can therefore remain kernel-visible
at the same time, while a pending receive cannot consume another stream's or
the provider's queue depth. Streams get that isolation from owning a reactor
outright; datagram sockets share one, so an armed receive draws instead on
capacity reserved for it and never competes with a send. Submission depth and
in-flight capacity are configured separately for that reason: an SQE slot is
reclaimed once the kernel consumes it, so a ring carries far more concurrent
operations than it has entries. Every actor keeps its descriptor and caller-owned
buffer live until the reactor dispatches its terminal CQE. Dropping a response
future does not remove an admitted command. Short reads and writes are returned
normally for the caller to advance and retry.

`UringIoPool` / `PooledUringFile` are a parallel implementation of the file
contract — the first stage of the shared-budgeted-rings design in
`DESIGN.md` — usable alongside `UringFile`. One pool owns one ring, one
coordinator thread running every registered file's state machine, and a
small blocking pool, so thread count is fixed regardless of how many files
are open. Reads, writes, fsync, and both length observations (`len` and the
post-fsync durable length, as statx — part of the pool's probed kernel
floor) are submitted as routed SQEs whose terminal results arrive on the
coordinator's single completion channel; only `set_len`'s `ftruncate` uses
the blocking pool. Each
file pipelines the commuting prefix of its queue — consecutive reads, or
non-overlapping writes — concurrently onto the ring, with a per-file reorder
buffer delivering responses in admission order and fences (`sync`,
`set_len`, `len`, overlapping writes) waiting for the pipeline to drain;
files whose turn arrives while the shared `max_in_flight` budget is
exhausted wait in a FIFO of ready files. Both implementations run the same
warm and cold file conformance suites.

`UringNetPool` / `PooledUringStream` share one ring thread and one coordinator
across connected streams. Each stream reserves one sustained operation slot
per direction, so a blocked send leaves receives and other streams able to
progress. `PooledUringStream` implements `ByteStreamVectoredSubmit`; the cold
facade exposes `ColdStream::write_vectored`.

Vectored submission validates the shared contract's 64-segment cap, payload
length, and full retained allocation sizes against `max_operation_bytes`.
The original segment vector travels to the ring thread, which constructs
bounded native iovec and msghdr storage and submits `SendMsg(MSG_NOSIGNAL)`.
The kernel reads directly from the shared spans: no contiguous payload staging
or extra shared-owner clone is created. The ring retains both payload and
metadata until its terminal CQE, then returns the same segment vector with
exact prefix progress. Its chunk cap may stop within any segment. Vectored
and contiguous writes share one FIFO and one active-send slot. Abandoning a
response does not release payload ownership; close interrupts active sends,
and pool teardown drains their completions before joining its threads.

Pool construction probes `sendmsg` along with its existing stream operations.
The native vectored path has shared warm/cold conformance checks and focused
tests for blocked sends, queue exhaustion, allocation identity, and teardown.

Actor channels provide admission and backpressure; they are not the execution
queue. The reactor drains admitted work into the kernel SQ, keeps up to
`ring_entries` user SQEs in flight, and dispatches CQEs by `user_data` instead
of waiting for each submission before publishing the next one.

File open/create, advisory locking, metadata lookup, and `ftruncate` remain
provider lifecycle operations implemented with the standard OS interfaces.
`UringFile::open_with_outcome` syncs the containing directory before reporting
success, reports whether it created the path, and preserves completion certainty
if a later initialization step fails. A new directory entry is therefore not
acknowledged without a namespace durability fence; callers of
`UringFile::from_file` own that responsibility because no path is available
there. Positional reads and writes and full-file sync use io_uring.
TCP socket creation, bind, and listen remain documented lifecycle syscalls.
Connection establishment and acceptance use io_uring. Provider, listener,
stream, backlog, operation, and queue counts are fixed by
`UringNetworkProviderConfig`; admitted connect and accept commands remain FIFO
even when their response is dropped. Listener close is out-of-band so it can
wake and fail a pending accept. Each accept attempt is linked to a short kernel
timeout, and the actor drains both CQEs before checking the close gate or
retrying, so no accept remains armed across shutdown. Awaiting close fences the
descriptor, binding, and listener-capacity release. Socket sends suppress
`SIGPIPE`, and common peer-disconnect errnos map to the provider-neutral
`ConnectionClosed` category.

The deterministic provider implements the same `NetworkProvider`,
`NetworkListener`, and `ByteStream` contracts without exposing SQEs or CQEs.
Both implementations run the same shared control-plane and connected-stream
conformance checks, including eager abandoned operations, accept FIFO order,
EOF, close rejection, and idempotency.

Each bound UDP socket uses independent bounded send and receive actors. The
receive side intentionally admits one operation at a time and arms a single
cancellable recvmsg rather than polling. Absolute deadlines and close are
therefore observable without depending on `shutdown(2)` to cancel an
unconnected socket: both are served by cancelling the armed operation, and the
terminal CQE is always awaited so no pointer target is freed while the kernel
may still write to it. Because an armed receive holds its SQE for as long as it
waits, the provider reserves one slot per socket, sized by `max_sockets`, so
idle sockets cannot exhaust the depth sends need. Caller buffers stay untouched on
failure; reusable provider scratch storage receives each packet and is selected
only after a valid CQE. Sendmsg is nonblocking and atomic; a nonnegative short
completion poisons and retires that directional driver. Awaiting close fences
both actors, all admitted buffers, the descriptor, and release of the local
binding. The simulator and Linux UDP provider run the same datagram conformance
check, including empty messages, truncation with full length, source addresses,
abandoned responses, deadlines, and rebind after close.

The provider control actor intentionally preserves FIFO connection setup.
Each connect is linked to the configured `connect_timeout`, bounding how long a
blackholed peer can delay later provider commands and teardown. The shared
provider reactor continues to dispatch listener work, while stream and datagram
reactors continue their already-admitted work. There is no per-request
cancellation token yet.

On Linux:

```text
cargo test -p kr-runtime-io-uring --all-targets
cargo clippy -p kr-runtime-io-uring --all-targets -- -D warnings
```
