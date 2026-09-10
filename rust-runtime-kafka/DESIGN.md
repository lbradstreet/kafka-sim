# Single-owner async runtime design

## Thesis

The runtime is the authority for scheduling, time, behavioral randomness, and
I/O completion delivery. Production state machines should use the same async
interfaces under simulation and real execution; a driver changes, not the
application algorithm.

A deterministic run is a function of more than a seed:

```text
runtime and binary version
+ root seed and RNG/replay schema
+ realized configuration and fault plan
+ workload specification
+ initial persistent-state identity
+ recorded external inputs
+ driver protocol and version
+ stop/drain policy and action, virtual-time, and wall-time budgets
```

The eventual replay artifact must preserve that manifest. A seed without the
code, realized configuration, and driving protocol is not a timeless schedule.

The executor supplies schedules and effects. It is not the correctness oracle.
Application-level model, invariant, metamorphic, durability, replica, metadata,
and liveness checkers belong above it.

## Kernel contract

### Interface first

The task-facing surface is concrete and deliberately narrow:

```text
Handle          = simulation task creation, virtual time, and seeded randomness
SimRuntime      = exclusive deterministic step/run control
HostHandle      = owner-local host task creation, host-relative time, and seeded randomness
RuntimeHandle   = portable sum of Handle and HostHandle for runtime-agnostic actors
HostSendHandle  = cross-thread admission of Send tasks
HostControl     = cross-thread stop request and HostStatus observation
HostRuntime     = exclusive host block_on/shutdown control
```

`SimRuntime` / `Handle` and `HostRuntime` / `HostHandle` are concrete sibling
APIs rather than providers behind an executor trait hierarchy. Their local lanes
use the shared `JoinHandle`, `AbortHandle`, and `Sleep` types and do not require
`Send`, matching the one-thread cooperative model. A borrowed, `!Send` root is
accepted at either exclusive `block_on` boundary. `JoinHandle::abort` is
explicit, while dropping a join handle only detaches it.

`RuntimeHandle` is the one portable task-facing capability: an explicit enum
over the two concrete handles covering their shared local surface (`now`,
`spawn`, `sleep`, `sleep_until`, and the workload random operations).
Application actors written against it start unchanged on either executor;
wiring code may still match on the variant to reach executor-only surfaces.
Runtime construction, driving, and cross-thread admission remain concrete and
are deliberately outside the portable surface. The free `yield_now` function
needs no handle at all, and `Sleep` itself retains a `RuntimeHandle` as its
timer route, so the kernel and applications share one sim-or-host carrier.
Quarry's broker is the reference pattern: one `start_broker` entry point takes
`impl Into<RuntimeHandle>` and serves both executors, rather than exposing
per-runtime constructors or a private dispatch enum.

`HostSendHandle` is the deliberately narrower cross-thread surface. It is
`Clone + Send + Sync` and admits futures and outputs that are `Send + 'static`
through bounded ingress. It does not let tasks migrate: the `HostRuntime` owner
remains the only thread that polls them. These spawns return the thread-safe
`HostSendJoinHandle`, while local spawns on both runtimes use the shared kernel
`JoinHandle`. `HostControl` separately exposes idempotent stop and the
`HostStatus` lifecycle: `Running`, `StopRequested`, `Stopped`, or `Failed`.

Model and campaign helpers accept concrete `RandomHandle`s scoped to distinct
random streams. Runtime driving likewise uses concrete runtime types. No shared
runtime trait is planned; `RuntimeHandle` is a closed sum over the two concrete
handles, not an open provider abstraction.

Owned I/O submission contracts sit below the executor. Base traits admit local
providers; blanket `Send*` companion traits carry thread-mobility bounds for
the handle and every associated future. Cancellation, buffer ownership,
ambiguous completion, backpressure, and durability remain defined by the base
contracts and shared provider tests.

### Cooperative execution

Each runtime owner polls one future at a time. A poll runs until the future
returns `Pending` or `Ready`; neither executor can preempt a future that fails to
yield. In simulation, one poll is an atomic, zero-virtual-time action: timers and
completions cannot interleave with CPU-only work until that work yields or awaits
a modeled operation. The harness therefore has both a scheduler-action budget
and, eventually, an independent process wall-time watchdog. The watchdog may
terminate a hung simulation process, but host elapsed time never advances the
simulation clock. Host execution observes elapsed host time only after control
returns to the scheduler.

Task IDs are `(slot, generation)`. Slab slots may be reused, but a stale waker
cannot address a new generation. No scheduling decision depends on a pointer or
allocation address.

### Simulation ready ordering

Each transition from waiting to ready receives a checked, monotonic enqueue
sequence. Runnable tasks are held in one owner-local FIFO queue:

```text
enqueue sequence ascending
```

The kernel never randomizes this ready ordering and never consumes the
`Schedule` random stream to break ready-queue ties. Seed-dependent interleaving
diversity is introduced in replay-visible layers above the executor, in two
forms that compose:

- **Harness-scripted plans.** A campaign derives explicit I/O and network
  latency and fault plans from the scoped `Schedule` and `Fault` streams. A
  provider consumes such a retained plan exactly, without drawing: a scripted
  delay is an exact virtual-time assertion and is never perturbed.
- **Provider latency models.** A simulated provider may be constructed with a
  versioned `SimLatencyModel` and a `Schedule` random source, and then draws at
  most one jitter value per admitted operation to perturb its *own* completion
  latency (`SIM_LATENCY_MODEL_VERSION`). The draw happens after every admission
  check, so a rejected operation consumes none. This is what makes two
  concurrently admitted operations complete in seed-dependent order; with the
  default `Fixed` model a workload explores one completion order per seed.

Resulting completions wake tasks through the same FIFO queue, so all diversity
remains a function of modeled time rather than of executor nondeterminism.

The ready queue contains at most one entry per active task and is bounded by
`max_tasks`. Cancellation removes a ready task with a bounded linear retain;
the deliberately simple reference scheduler does not maintain a second index.

Wake-ups only record notifications; they never poll inline. The
standard `Waker` contract requires a `Send + Sync` target even though tasks are
local, so each task signal contains atomic notification state and a small
atomics-only bridge. Owner-thread wakes append to a local pending queue found
through a thread-local registry of weak queues keyed by stable bridge identity;
foreign threads are rejected before this lookup. Shutdown unregisters the queue
before user cleanup. At a scheduler boundary the owner drains pending wakes in
FIFO order without scanning task slots or sorting, so admission costs depend on
pending wakes, not the number of live tasks. Retiring a task disables its signal
and removes any pending wake, keeping pending storage bounded by live tasks.
Repeated wake-ups while a task is already notified coalesce without
changing its position. A wake during a poll causes exactly one subsequent poll
if the task returned `Pending`. Owner-thread wakes after completion or
cancellation are generation-checked no-ops; any foreign-thread wake is rejected
as nondeterministic ingress.

Simulation has no general synchronized command queue. Cancellation uses a
separate owner-admitted queue and consumes one scheduler action per task, so a
destructor that creates and aborts another task remains bounded by
`max_steps_per_run` rather than making one `step` call unbounded. Invoking a
shared `AbortHandle` from a foreign thread is classified the same way as a
foreign wake: the next simulation drive reports nondeterministic ingress.

### Host ready admission

Host execution accepts the cross-thread completion order that simulation
rejects. A task signal has a stopped latch and one atomic pending marker.
Repeated notifications coalesce until the owner clears that marker immediately
before polling the task. Every newly admitted notification enters a
mutex-protected, bounded ingress queue and unparks the owner thread; stale task
IDs are ignored after generation validation.

`HostSendHandle` uses the same ingress boundary for `Send` task admission, and
`HostSendJoinHandle::abort` uses it for cross-thread cancellation. Local
`AbortHandle` requests remain owner-local. Capacity failure is reported to a
send-spawn caller and is fatal if dropping an already-issued wake or abort would
strand work. `HostControl::request_stop` is an independent atomic transition
and unpark, so stop cannot be rejected by an ordinary full ingress queue.

One host scheduler turn drains at most `max_ingress_per_turn` entries and checks
every timer now due, even while tasks remain continuously ready. After firing
timers it starts another admission turn before polling; otherwise it polls one
ready task or parks until the next deadline. This lets timer wakes use ingress
capacity before another task poll can refill it. The bound limits producer work
per turn; it does not promise a stable
ordering across racing foreign threads. Host FIFO behavior applies to the order
the owner admits, not to an unknowable global call order.

### Simulation virtual time

`RuntimeInstant` and `RuntimeDuration` represent `u64` nanoseconds; simulation
code may use their `SimInstant` and `SimDuration` aliases. There are no
floating-point deadline comparisons and no reads of a host clock.
`RuntimeInstant` is deliberately a numeric logical-time coordinate rather than
a runtime-identity capability; moving one between runtimes preserves that
number, while a `Sleep` retains and checks the identity of the runtime that
created it.

Simulation begins at its configured `start_time` (zero by default).
`RuntimeConfig::derived_start_time` maps a root seed to a pinned, nonzero
start instant so campaigns can flush out absolute-time assumptions; the
mapping consumes no random-stream draws, is versioned by
`START_TIME_DERIVATION_VERSION`, and the exact instant a run used is pinned
by its `RuntimeReproduction` configuration. A start time beyond a configured
`max_time` is rejected at construction.

Ready work always runs before time advances. When no task is ready, the runtime
jumps directly to the earliest timer deadline. All active timers at that deadline
fire in registration order; their tasks then enter the FIFO ready queue.
Cancelled timers are physically removed so tombstones cannot
cause ghost time jumps or unbounded heap growth.

An already-expired timer, including a zero-duration sleep, completes immediately.
`yield_now` is the explicit operation for moving a task to the back of its ready
queue. Deadline overflow and timer capacity exhaustion are typed errors.
Each pending sleep retains its ordered `(deadline, registration sequence)` key,
so cancellation physically removes exactly one timer in logarithmic time while
preserving stable equal-deadline firing order.
Timer registrations are owner-thread `Rc` state, matching the non-`Send`
runtime handle and sleep contract. Only task-waker notification flags are
thread-safe; virtual-time state remains entirely owner-local.

A pending sleep may move between tasks in the same runtime. Its first scheduling
event identifies the initial registrant; firing, cancellation, and shutdown
diagnostics identify the latest task whose pending poll installed the waiter.
Reusing an equivalent waker still updates that task identity.

### Host-relative time

`HostRuntime` captures `std::time::Instant::now()` at construction.
`RuntimeInstant::ZERO` denotes that epoch, and `HostHandle::now` exposes elapsed
nanoseconds on the same public numeric coordinate used by `Sleep`. Converting
the elapsed value is checked; exceeding the `u64` range would require more than
584 years. This coordinate carries no runtime identity, but each `Sleep` retains
a private route and rejects polling from a task owned by another runtime.

Host timers keep the same capacity, cancellation, and stable equal-deadline
rules as simulation. When no task or ingress item is ready, the owner parks for
the duration until the earliest deadline. Parking may return spuriously, so the
scheduler always rereads host time and its queues before deciding what is due.
An unpark token closes the check-then-park race with a concurrent foreign wake
or stop request.

### Cancellation and joining

Dropping `JoinHandle` detaches; it does not cancel. `abort` is explicit,
idempotent, and routes to the owning runtime's cancellation queue. Simulation
admits owner-thread aborts as individual scheduler actions; host execution
admits them through bounded ingress. Applying cancellation drops the task future
exactly once on the runtime owner thread while publishing the task identity for
diagnostics. Completion before the abort boundary wins; an abort processed
before the next poll wins otherwise.

Rust cancellation is drop-based, but cancellation never implies rollback of an
external side effect. This distinction becomes load-bearing for I/O.

Task panics are caught at the polling boundary, converted into stable panic
records, and delivered to joiners. Simulation treats any task panic as fatal
because continuing would make a failed model run look successful. Host
execution isolates a spawned-task panic to `JoinError::Panicked` and continues
running unrelated tasks; a host root panic is returned from `block_on`.

Runtime shutdown resolves remaining joins with `RuntimeStopped` before dropping
their futures. Panic text is retained at a UTF-8 boundary up to 4 KiB with an
explicit truncation bit. Simulation cancellation events distinguish explicit
abort, failed-root cleanup, and runtime shutdown; snapshots expose whether
shutdown has occurred.

Simulation latches the first fatal error with its failure-point snapshot before
teardown. Every later driving or checked-shutdown operation returns that same
error, so a harness cannot accidentally turn a failed run into a successful
terminal observation. Error disposition is explicit: stalls, driving bounds,
reentrant attempts, root cancellation, and task-limit rejection are resumable;
clean `RuntimeStopped` observations and root rejection by an already-stopped
runtime are terminal; task, destructor, waker, nondeterministic-ingress, and
sequence or identifier exhaustion failures are fatal.

Host execution has no deterministic snapshot to attach. A destructor or waker
panic, ingress failure that could strand work, or sequence exhaustion moves
`HostStatus` to `Failed` and is retained as a `HostRunError`; a clean stop moves
it to `Stopped`. A spawned-task panic alone does neither.

`finish` is the canonical consuming ownership boundary. It performs checked
shutdown and returns any teardown failure after all tasks have been resolved.
Plain `Drop` remains best-effort because Rust destructors cannot return an
error. Simulation harnesses and host owners must therefore use `finish` before
reporting a run or service lifetime as successful.

Structured capture of task, destructor, and waker panics requires
`panic = "unwind"`. Simulation campaign and replay builds must use that panic
strategy; host builds need it when callers rely on structured containment.
Under `panic = "abort"`, the process terminates before either runtime can return
a structured failure. The library does not impose this as a compile-time
restriction.

### Simulation driving and terminal states

`step` performs one inspectable scheduler action: one cancellation, one future
poll, or one time jump that promotes all equal-deadline timers. It otherwise
reports `Idle` or `Stalled`, and reports `Stopped` after terminal shutdown.

`run_until_stalled` is bounded and returns:

- `Idle`: no task remains;
- `Stalled`: tasks remain but no ready task or timer can wake them; or
- `Stopped`: the runtime has entered its terminal stopped state; or
- `RunError`: panic, nondeterministic external input, time/step limit, or an
  internal exhausted sequence.

Pending forever is sometimes an intentional model of a failed disk or network,
so `Stalled` is data for the harness rather than always a runtime bug.

`block_on` waits only for its root future. Other spawned tasks remain active; a
test that requires global cleanup must explicitly drain and inspect the runtime.
The root occupies a scheduler-visible scoped slot rather than the detached task
storage: it may borrow caller stack data and return a borrowed output, but its
future is always removed and dropped before `block_on` returns. This preserves
ordinary FIFO, wake, timer, trace, and failure semantics without unsafe
lifetime erasure.
When `block_on` fails, its initiating kind and pre-cleanup snapshot remain the
primary `RunError`. If cancelling or dropping that root also fails, the error
retains the cleanup failure separately and becomes fatal without replacing the
evidence that caused cleanup to begin.

The replay manifest includes a versioned driver specification, not just runtime
configuration. It pins the driver algorithm and entry point, the stop condition,
the post-root drain policy, scheduler-action and virtual-time budgets, the
process watchdog policy, and how terminal `Idle` or `Stalled` observations are
handled. Campaign execution and exact replay use one canonical harness-owned
driver for that specification so different call patterns cannot masquerade as
the same replay input.

### Host driving and terminal states

`HostRuntime::new(HostConfig)` validates fixed task, timer, ingress, and
per-turn ingress bounds. `block_on` admits a scheduler-visible borrowed root and
drives until that root completes, stop is requested, or a root/runtime error is
reported. As in simulation, the root may borrow caller state and return a
borrowed output; it is removed and dropped before `block_on` returns. Driving
either runtime recursively from a task poll is rejected.

An idle host runtime is not a simulation stall. It parks indefinitely when no
timer exists and parks until the next host deadline otherwise. A task waker,
send-spawn, abort, or `HostControl::request_stop` unparks it. Foreign arrival
order is intentionally not replayable.

`HostControl::request_stop` performs the atomic `Running -> StopRequested`
transition once. The owner observes it at a scheduler boundary, makes every
remaining join and sleep terminal, drops all futures under containment, and
publishes `Stopped` or `Failed`. `HostControl::status` returns the current
`HostStatus`; it observes lifecycle without driving tasks or exposing an
occupancy-sized snapshot.

## Deterministic choice and replay

The kernel uses a pinned, portable PRNG and explicitly implemented range and
boolean mappings. One user seed derives named, domain-separated streams for
workload, topology, higher-level schedule-affecting choices, and faults, plus a
diagnostic stream that cannot perturb behavioral choices. The `Schedule` stream
belongs to deterministic race/select and modeled completion timing, not kernel
ready ordering. Algorithm names, derivation, mapping rules, and draw counters
are replay-format contracts with golden tests.

`HostHandle` uses the same pinned stream derivation and mappings from
`HostConfig::seed`, but host task interleaving determines draw order. A host
seed makes an isolated sequence reproducible only when the caller preserves its
own logical ordering; it is not a scheduler replay guarantee.

All runtime-owned random sources are sealed by terminal shutdown. Later choice
requests return `RandomError::RuntimeStopped` before consuming a draw; in
simulation they also return before reserving a trace sequence. Read-only
position inspection remains available for final diagnostics. Argument
validation takes precedence and likewise consumes no state.

The kernel exposes a separately versioned `RuntimeReproduction` fragment that
pins its exact `RuntimeConfig`, root seed, and RNG contract. A complete replay
manifest remains harness-owned because only the harness knows its driver,
workload, fault plan, initial state, and watchdog policy. A versioned terminal
`DeterminismCheckpoint` captures virtual time, scheduler progress, total task
enqueue and timer-registration volume, behavioral RNG positions, and bounded
active-resource counts. It is a cheap rerun canary, not a correctness proof or
complete replay identity; the harness also compares its typed outcome and
application/model-checker result.

Structured diagnostic events are opt-in. When enabled they receive a monotonic
event sequence, virtual time, task identity, and relevant choice or
timer identity. `RecordingTrace` retains a bounded prefix by default and can
instead retain a tail or prefix plus tail. `SbeRecordingTrace` is the
byte-capacity counterpart. It allocates its configured prefix and tail buffers
at construction, length-frames each event, and encodes directly into unused
buffer space. A complete frame is retained or omitted; capacity includes the
four-byte frame length and eight-byte SBE message header. Recording a
schema-valid runtime event performs no allocation and no I/O, although tail
eviction may compact retained bytes in place. The typed `TraceEvent` remains the
runtime-facing domain contract; SBE is a sink and storage representation rather
than the executor API.

Both recorders compute the same versioned trace fingerprint over every
ordering-valid observed event before retention. The byte recorder also folds an
event before sizing or encoding it, so byte capacity and SBE encoding do not
change the canonical digest. The fingerprint is defined by the trace schema's
typed field encoding, not by Rust memory layout or serialized SBE bytes. It
therefore verifies the diagnostic stream only; it does not participate in
untraced execution or define replay identity. The default runtime checks that
tracing is disabled before constructing `EventKind` or `TraceEvent`.
`SamplingTrace` adds deterministic every-N filtering at that same
pre-construction boundary. Sequence numbers still advance for rejected samples,
so retained events preserve their positions in the unsampled diagnostic stream.

Byte retention has explicit gap semantics. Prefix retention closes permanently
when the next complete frame does not fit, so a later small event cannot hide an
earlier omission by backfilling. Tail retention evicts whole oldest frames. An
event that is too large or cannot be encoded is a suffix barrier: the previous
tail is cleared before later events are admitted, preserving the claim that the
tail is a contiguous suffix. Encoding failures are counted separately as well
as being absent/dropped, and binary artifact export refuses such a recorder
rather than silently publishing an incomplete encoding.

### Binary trace artifact

The persisted binary trace is a versioned container, separate from the runtime
event schema. The current compatibility coordinates are:

```text
container magic/version       DSTRSBE\0 / 1
SBE schema ID/version         1 / 1 (semantic version 2.0.0)
artifact envelope schema      8
diagnostic trace schema       5
runtime reproduction schema   3
determinism checkpoint schema 3
```

The 16-byte container preamble is followed by little-endian, `u32`-length-
framed SBE messages in a declared order: one artifact header, deterministic RNG
stream checkpoints, active-task snapshots, then retained runtime events. A frame
length includes its own four-byte prefix. The artifact header records byte- or
event-count capacity units, retention and sampling policy, omission and ordering
metadata, terminal runtime state, and the canonical trace fingerprint. Readers
validate declared counts, bounds, frame lengths, template IDs, schema
coordinates, and trailing data before accepting the artifact.

Artifact schema 7 describes the binary envelope; it is not the SBE schema
version. Runtime traces have no parallel JSON or NDJSON persisted form. The
browser validates the container and decodes it directly into bounded in-memory
presentation records, using `BigInt` before producing decimal strings for
full-width integers. This avoids a JSON-sized intermediate. Export from
`SbeRecordingTrace` is zero-copy within the exporter:
it visits retained event slices directly and writes them without decoding,
allocating, or re-encoding the event frames. Header and snapshot encoding still
uses ordinary post-run allocations, and the destination writer or operating
system may copy the bytes. All filesystem I/O occurs after the simulation
reaches the diagnostic point, so host failures and latency cannot perturb
simulated scheduling.

Compatibility is intentionally strict in the current reader. It rejects an
unknown container version, SBE schema ID or version, artifact schema, diagnostic
trace schema, template, or noncanonical metadata instead of guessing. Evolving
the SBE XML therefore requires an explicit compatibility decision and golden
round-trip/corruption tests.

The browser decoder is pinned to the Rust implementation by a committed,
version-labelled SBE golden. Cargo tests reproduce it from an exhaustive typed
fixture. A dependency-free Deno suite decodes the same binary and checks exact
semantics plus truncation, framing, schema, UTF-8, enum, bound, duplicate,
overflow, sampling, canonical-presence, and ordering corruption classes. Run
both sides with `./scripts/check-trace-viewer-sbe.sh`.

Generated Rust codecs come from SBE tool `1.38.1` using the pinned checksum in
`scripts/regenerate-trace-sbe-codecs.sh`; after placing the named `sbe-all` jar
at the default path (or setting `SBE_JAR`), run:

```text
./scripts/regenerate-trace-sbe-codecs.sh
cargo test -p kr-runtime-trace-wire -p kr-runtime -p kr-runtime-trace-tool
```

Generated files are replaced as a set and must not be edited by hand. Run
`./scripts/regenerate-trace-sbe-codecs.sh --check` to regenerate into a
temporary directory and verify the committed Rust codec and browser IR without
changing the worktree. `check-trace-viewer-sbe.sh` includes this drift check and
therefore also requires the pinned SBE jar.

Terminal lifecycle remains explicit in a recorded trace: runtime stop, caught
waker failures, and task-destructor failures are structured events. The same
conditions are typed runtime outcomes even when tracing is disabled, so a
harness does not depend on observability to detect correctness failures.

The current implementation covers executor, timer, cancellation, panic, and
random-choice events. Future I/O and fault trace records may carry
provider-private correlation when it is needed to interpret those records.
That observability metadata does not belong in the shared I/O result contract
unless a portable consumer requires the identity as part of its semantics.

Exact replay verification is a subsequent harness layer. It should stop at the
first expected/actual divergence and must not force a recorded choice into code
whose control flow has already diverged.

Reachable configured limits and scheduler identifier/sequence exhaustion are
reported as typed errors where the operation can remain resumable.
Lifetime-scale `u64` bookkeeping counters, including RNG draws, enabled trace
event sequences, and recorder dropped-event counts, are currently treated as
invariant exhaustion and panic. Any future attempt to make that exhaustion
recoverable must redesign all of those infallible paths together rather than
changing only one counter.

## Completion-based I/O boundary

The executor must not know about io-uring CQEs, epoll flags, simulator files, or
TCP internals. Drivers accept owned requests and return normalized completion
events:

```text
application future
    -> owned request
    -> simulator or production driver
    -> normalized terminal completion
    -> ordinary task wake notification
```

A deterministic provider completes on the simulation owner or through a
recorded model event. A production provider may complete from its dedicated OS
thread: `HostRuntime` coalesces that ordinary waker into bounded ingress and
unparks its owner, while `SimRuntime` rejects the same foreign-thread wake as
nondeterministic input. The I/O contract itself does not depend on either
executor.

Drivers may use private generation keys, operation tokens, or trace correlation
to manage their own lifecycles. Those implementation details stay behind the
boundary: a simulator or tracing layer alone is not a reason to make every
provider allocate and return identities. Add shared identity only when a
portable caller needs it for correctness, and document the namespace and
lifetime that caller can rely on. Diagnostics needed by only one provider
belong in its status, trace, observer, or test-support boundary instead of the
portable completion. Retry and deduplication identities that must cross a
datagram transport belong in the caller's protocol payload.

Public file APIs should take and return owned buffers:

```text
read_at(OwnedBuf, offset)  -> ReadResult  { buffer, bytes_read }
write_at(OwnedBuf, offset) -> WriteResult { buffer, bytes_written }
sync / truncate / close
```

Partial reads and writes are normal. `sync` is an explicit durability fence.
Batch/vectored operations and bounded submission/backpressure are part of the
interface rather than afterthoughts.

The driver owns every buffer and resource referenced by an in-flight operation
until terminal completion. At the warm submission boundary, dropping the
waiting future abandons only its response: the admitted operation remains in
FIFO order, the driver must not free a buffer that the kernel may still use,
and the abandoned response's waker is cleared so a late completion cannot wake
a stale task. The io_uring provider retains request ownership until it
consumes the matching terminal CQE and only then releases or returns resources.

Every I/O domain splits that boundary into two layers. The warm `*Submit`
traits (`FileIoSubmit`, `NetworkProviderSubmit`, `NetworkListenerSubmit`,
`ByteStreamSubmit`, `DatagramProviderSubmit`, `DatagramSocketSubmit`) are the
provider-facing eager contract: a `submit_*` call attempts validation and
bounded admission during the method invocation, and its response future is a
completion ticket. The cold handles (`ColdFile`, `ColdNetwork`,
`ColdListener`, `ColdStream`, `ColdDatagramNetwork`, `ColdDatagramSocket`)
are the application-facing default: calling an operation method constructs an
owned `'static` future and admits nothing; the first poll attempts admission
exactly once, and a never-polled future has not started. Successful `listen`,
`connect`, `accept`, and `bind` operations return cold-wrapped handles, and
the exclusive lower listener, stream, and socket handles are retained through
private shared ownership so cold futures stay owned without exposing public
cloning. Ordering and `sync` fencing follow first-poll admission order on the
cold side and invocation order on the warm side. Post-admission semantics —
abandonment, buffer retention, and completion certainty — are identical in
both layers; the staged migration is recorded in
`COLD-IO-FUTURES-PROPOSAL.md`. `kr-runtime-ring` deliberately stays on the warm
file boundary because invocation-order fencing is part of its durability
contract.

### Shared completion primitives

Providers do not hand-roll response futures. `kr-runtime-io`'s public `completion`
module defines the two single-response primitives every provider completes
through: an owner-thread `LocalOperation` for simulation and owner-local
providers, and a thread-safe `SyncOperation` for providers that complete from
another thread. External provider crates obtain a pair through
`SyncOperation::channel`; `kr-runtime-io-uring`'s `UringOperation` and the
per-provider future names inside `kr-runtime-io` are aliases of these types, so one
implementation defines polling, waker coalescing, permit release, and
poll-after-completion policy for every provider.

The primitives encode two rules new providers must not relearn:

- A completing provider never runs foreign code while holding its own state.
  Wakes are panic-contained, and the thread-safe cell returns the pending
  waker from completion so a provider can commit terminal state under its
  lock and notify after releasing it. Outputs, permits, and wakers are
  likewise destroyed only outside provider state, because their destructors
  can run arbitrary caller code that re-enters the provider.
- Observability gating is cell data, not bespoke provider machinery. The
  owner-thread cell carries the delivery-delay flag and clogged-link gate
  count the network simulator uses: a stored output becomes observable only
  once the scheduled delay elapsed and every gate reopened. Bounded admission
  uses the shared permit pools, whose permits release when the output is
  consumed or its terminal response is discarded.

Side-effecting errors carry completion certainty:

- `NotApplied`: the effect definitely did not occur;
- `Applied`: the effect completed even if a later reporting step failed; or
- `MayHaveApplied`: the caller must resolve ambiguous completion safely.

The crate exposes this vocabulary as `CompletionCertainty`,
`CompletionError<E>`, and `CompletionResult<T, E>`. `kr-runtime-io` uses the same
types for `FileIoSubmit`, `ColdFile`, and `ByteStream`; Quarry uses them for
ring operations and durable queue recovery.

`SimStorage` and `SimNetwork` use those contracts for virtual delay, partial
completion, bounded admission, directional disruption, and scripted
before/after/ambiguous errors. `SimStorage` additionally carries a versioned
pipeline model (`SIM_PIPELINE_MODEL_VERSION`): its commuting-overlap mode
completes reads and non-overlapping writes in latency order rather than
admission order, exercising the completion reordering the Linux file
pipelines genuinely produce, while fencing and effect equivalence stay
fixed. `UringFile` and `UringByteStream` implement the
same requests with real Linux completions. These are semantic io_uring models,
not SQ/CQ ABI emulators: application and ring futures remain unchanged when the
provider changes.

The shared `RingWriter` contract is Quarry's only durable-record primitive.
`MemoryRing` is its logical reference provider. The checksummed
`FileRing<SimStorage>` runs the real circular framing, alternating-checkpoint,
sync, and recovery state machine over deterministic storage faults; the Linux
`UringRing` drives that same file-ring engine over `UringFile`. Quarry appends
versioned queue records and installs each submit or acknowledgement in memory
only after `sync` publishes the expected durable tail. Recovery fences any
accepted suffix, replays bounded pages from the durable interval, validates the
complete history, and appends a fresh lease-token incarnation. Quarry currently
never trims, so its ring must retain the complete logical history from position
zero and eventually reports capacity exhaustion under an unbounded workload.

## FoundationDB-style layers

The runtime kernel stays small. FoundationDB's broader method grows as layers
above it, in this order:

1. Pinned choice streams, separately versioned reproduction/checkpoint/trace
   contracts, a harness-owned replay manifest and canonical driver, subprocess
   isolation/watchdog, retained failure artifacts, exact replay verification,
   and a bounded many-seed campaign runner; plus task groups, cancellation
   tokens, deterministic race/select, and async values.
2. Owned completion operations and simulated/production storage and network
   providers with shared conformance tests. The initial `kr-runtime-io` and
   `kr-runtime-io-uring` implementations are complete; broader operations remain
   incremental work.
3. Separate fault mechanisms for legal perturbation, tagged operational faults,
   environmental chaos, and deliberate product mutations that test checkers.
   Every injector has provenance and a witnessed hit count.
4. A bounded simulated byte-stream network: partial I/O, backpressure, latency
   tails, asymmetric clogs, hard directional partitions, half-close, disconnect,
   and pending-forever operations.
5. A layered simulated filesystem: accepted versus durable writes, overlapping
   reordering, `sync` barriers, torn/lost/corrupt unsynced writes, rename and
   truncate semantics, stalls, capacity, and reboot.
6. Virtual processes, machines, zones, halls, and regions as explicit harness
   state; correlated kill/reboot/delete behavior; and a centralized
   replication-aware survivability policy.
7. Valid randomized topology/configuration generation and boundary-amplifying
   knobs rather than arbitrary invalid states.
8. Composable `setup -> start -> check -> metrics` workloads combining semantic
   state machines, traffic, causal faults, and progress monitors.
9. Online safety checking during chaos, followed by "stop faults, drain by
   condition, then audit" for clean quiescent runs.
10. Restart/compatibility campaigns, mutation tests of named checkers, semantic
    coverage probes, and large-scale campaign selection and prioritization.
11. `HostRuntime` supplies the initial real-clock task host for ordinary
    completion wakers. Broader reactor integration, sanitizer, hardware, and
    true-concurrency testing remain necessary for phenomena cooperative
    simulation cannot model. The initial file and connected-TCP io_uring
    providers remain separate audited adapters rather than runtime internals.

The io_uring backend lives in the separate `kr-runtime-io-uring` crate. This kernel
keeps `unsafe_code` forbidden, while the provider contains the tightly scoped
kernel-facing code and Linux-only validation.

Topology generation, replication policy, workload composition, and global
consistency checking do not belong in the executor crate merely because they
use it.

## Known fidelity boundary

Cooperative deterministic execution explores logical interleavings, delays,
failures, and crash protocols represented by its models. It does not reproduce
CPU data races, lock-free memory ordering, kernel scheduling, real filesystem
durability, device firmware, congestion control, MTUs, or TLS cryptography.
Those require complementary production-driver, sanitizer, hardware, and host
cluster tests.

A future poll is atomic and consumes no virtual time. Consequently a timer or
completion cannot race CPU-only computation inside one poll; the computation
must explicitly yield or await a modeled delayed operation. If CPU-duration
races become important, they need an explicit simulated CPU-cost operation,
never measurement of host poll duration.

Outstanding-byte backpressure is currently a simulation-only bound. The
simulated storage and network providers reserve a byte budget at admission
(`max_outstanding_bytes`, `max_outstanding_read_bytes`,
`max_outstanding_write_bytes`) and reject past it with `ResourceExhausted`.
The charge denominates pinned allocation, not payload: a buffer is charged
at its capacity (for network reads, the length it may grow to, if larger),
so a pooled or reused buffer charges its full allocation rather than the
bytes one request moves, and an allocation the whole budget cannot hold is
rejected as over-large up front instead of as resumable exhaustion;
the host providers bound operation counts, queue depth, and per-operation
sizes, but admit any number of outstanding bytes. A simulation configured
with a lowered budget therefore rejects operations a host run would admit.
This is deliberate — the sim bound exists to explore rejection paths, and its
defaults are sized to stay out of the way — but it is a fidelity divergence,
not an equivalence, until the deferred host-side bound below is built.

## Deferred designs

Mechanisms that fit this architecture but have no test to earn them yet.
Build one only when the trigger below arrives, never speculatively.

### Node-generation invalidation for process restart

**Trigger:** quarry (or another harness) begins modeling whole-process
kill/restart — tasks torn down, provider handles invalidated, and storage
rolled back to its durable snapshot as one atomic transition — rather than
today's storage-only crash/recovery.

**Mechanism.** Apply the task slab's `(slot, generation)` pattern one level
up, as madsim does with its per-node generation counter: a simulated "node"
owns a generation number, every provider handle created under it captures the
current generation, and every admitted operation validates its captured
generation before touching shared state. A restart increments the generation,
so every stale handle and in-flight operation from the previous incarnation
fails closed with a typed error (the network analogue of `ConnectionReset`;
storage should fence with the existing `RecoveryRequired` shape) instead of
being merely unlikely to be used. This composes with existing invariants: the
storage rollback reuses the durable-snapshot machinery the fsync-gating model
already maintains, and the fenced errors carry `CompletionCertainty` like any
other rejection.

**Why not now.** Nothing exercises multi-process restart, so the mechanism
would ship without an oracle proving it earns its keep — the opposite of how
this workspace grows. When the trigger arrives, the campaign that needs it
supplies the test: kill a node mid-operation, assert every pre-restart handle
observes the typed fence exactly, and assert the reference model agrees with
the durable rollback.

### Buggify-style cooperative fault points

**Trigger:** the first time broker or application logic wants to force a rare
branch under simulation — a slow path, a full batch, a delayed retry — and
the provider-level fault plan cannot reach it because the branch lives above
the I/O boundary.

**Mechanism.** FoundationDB's `buggify`, expressed as a capability instead of
an ambient macro: a method on the simulation `Handle` that draws from the
existing `Fault` stream and returns whether a named fault site should fire
this time. Fault draws are already domain-separated, versioned, and
trace-visible, so sites inherit replay and observability for free. The host
counterpart is constantly `false`, compiled into the portable surface so
production-adjacent code carries no simulation dependency and no branch cost
beyond the call.

**Open questions to settle at build time.** Whether sites are identified by
static tags (stable across runs, diffable in traces) or call sites; whether
per-site firing rates are fixed, configured per campaign, or drawn once per
run from the `Scenario` stream; and whether the portable `RuntimeHandle`
exposes the method (ergonomic for shared actors) or only the concrete sim
`Handle` (keeps the portable surface minimal). Validate arguments before
consuming a draw, as every random operation here already does.

**Why not now.** A fault point delivers nothing until code adopts it, and no
current branch in quarry needs forcing that the provider fault plan cannot
already reach. Build it alongside its first real consumer so the site
taxonomy is shaped by an actual bug hunt rather than invented up front.

### Outstanding-byte backpressure in the host providers

**Trigger:** the first deployment or benchmark where a host provider's
admitted-but-incomplete operations hold enough caller memory to matter — or
the first application that tunes the simulated byte budgets low enough to
depend on rejection behavior the host cannot reproduce.

**Mechanism.** The denomination-generalized permit pools already exist in
both flavors: the simulated providers reserve through `LocalPermitPool` and
the thread-safe `SyncPermitPool` is the same mechanism for host submission
paths. A host provider acquires a byte-charged permit at admission alongside
its existing count bounds and attaches it to the operation's completion
state, releasing on terminal consumption or abandonment exactly as the sim
does. Config gains the same `max_outstanding_*` knobs with the same names so
a scenario file means one thing in both executors.

**Why not now.** No workload has demonstrated the bound earning its keep on
the host side, and an untested limit is a new way to wedge production I/O
rather than protect it. When it lands it must arrive with a conformance
check in `kr_runtime_io::conformance` that drives a provider to its byte budget
and asserts the `ResourceExhausted` rejection shape, run by both executors —
the check is what retires the fidelity divergence recorded above, so the two
must land together.

### Shared budgeted rings with runtime I/O placement

**Trigger:** the first workload where per-handle thread and descriptor cost
binds. The current io_uring architecture spends two threads per open file
and three threads and three descriptors per connected stream, so a few
thousand concurrent handles exhaust default descriptor limits and scheduler
capacity long before the kernel does.

**Mechanism.** Three restructurings that compose, each preserving the warm
`*Submit` contracts and the shared conformance suites unchanged:

- *Event-driven handles.* Per-handle actor logic — batching, FIFO ordering,
  recovery fencing — becomes a mailbox-driven state machine instead of
  blocking sequential code. Completions are dispatched by `user_data` to
  the handle's machine rather than parked on per-operation channels, and an
  explicit reorder buffer completes responses in admission order from
  unordered CQEs. Per-handle FIFO rests on exclusive checkout — one thread
  runs a given handle's machine at a time — not on a dedicated thread.
- *Shared rings.* Handles become registered residents of a provider ring
  rather than ring owners, isolated by the class-budget mechanism the
  datagram provider already proves out: every handle reserves an
  admission-time minimum of in-flight budget, the completion queue is sized
  to the sum of reservations, and cancels stay exempt and out-of-band. Two
  shared threads per provider replace the per-handle fleet: a kernel-facing
  reactor that runs no foreign code, and a coordinator that runs every
  state machine, response completion, and waker (contained, and required to
  be O(1)). The coordinator may later dissolve into `HostRuntime` tasks,
  leaving the reactor as the only thread the provider adds.
- *Placement as policy.* With the handle-to-ring binding indirect,
  placement is a runtime admission decision at open, connect, accept, and
  bind: dedicated (a fleet-of-one ring — today's architecture surviving as
  a degenerate policy for latency-critical handles), provider-shared,
  per-core sharded, or tiered. Ring-to-reactor and machine-to-coordinator
  bindings are independently placeable, and `IORING_SETUP_ATTACH_WQ` shares
  kernel async workers across a ring fleet. Live migration exists only as a
  quiesce fence — stop admitting, cancel armed work, drain to terminal CQE,
  re-register, resume — which is rebalancing-grade, not load-following.
  Placement stays host-only wiring, off the portable surface.

Blocking lifecycle syscalls — open and create, `ftruncate`, metadata
lookups, the containing-directory fsync — leave the hot threads for a
small bounded blocking pool, or for io_uring opcodes where the supported
kernel floor allows, so one handle's lifecycle work cannot convoy every
other handle's completions. A dedicated-placement tier may instead be
permitted to block its own lane.

**Invariants that must survive.** Single submitter and reaper per ring.
Buffer liveness to the terminal CQE, held in loop-owned slots. Per-handle
bounded admission — a shared ingress bound would let one chatty handle
consume everyone's backpressure. And placement must stay semantically
invisible: a handle's reserved minimum makes its rejection behavior
independent of co-tenants, and any shared burst capacity beyond it is
best-effort and belongs in the fidelity boundary beside the byte budgets.
Poison blast radius grows from one handle to the provider; that is still
fail-closed, but it is observable and needs its own tests.

**Why not now.** No workload has hit the thread or descriptor wall, and the
blocking-sequential actors are the easiest code in the crate to audit; the
state-machine rewrite spends that clarity and must buy scale with it. When
the trigger arrives, build in stages — the datagram receive path first
(already one-operation-at-a-time and cancel-driven), files second with the
reorder buffer, streams and ring-sharing last — each stage validated by the
unchanged conformance suites plus saturation-style host tests per placement
policy. Static topology configuration comes first, the migration fence
second, and heuristic rebalancing only when a workload demonstrates the
need, since that is the piece that resists an oracle.

**Status.** The file domain exists as a parallel implementation:
`kr_runtime_io_uring::UringIoPool` registers files onto one shared ring with a
coordinator thread and a bounded blocking pool, pipelining each file's
commuting command prefix (reads, or non-overlapping writes) concurrently
and completing responses in admission order through a per-file reorder
buffer, with fences waiting for the pipeline to drain. It coexists with
`UringFile` — both run the same warm and cold conformance suites, and the
`file_multi_inflight` benchmark compares them — so the architectures can be
measured on real workloads before anything migrates.

The stream domain exists the same way: `kr_runtime_io_uring::UringNetPool` is a
full provider — listen, connect, accept, and registered pre-connected
streams — on one shared ring, one coordinator, and one descriptor per
connection. Both directions of stream I/O are peer-gated, so each
registered stream reserves one sustained slot per direction at
construction, realizing the admission-time-minimum invariant directly: no
budget waiter queue exists to starve, and a stalled peer consumes only its
own reservation. Control-plane connect and accept run as routed
linked-timeout pairs on the transient budget, with accept re-armed on
expiry so listener close drains within a bound instead of needing a
routed cancellation path. It coexists with `UringNetwork` under the same
warm and cold stream and control-plane conformance suites. Datagram
consolidation, placement policy, and migration remain unbuilt.
