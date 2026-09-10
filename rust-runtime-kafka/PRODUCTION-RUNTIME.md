# Runtime and production I/O boundaries

`kr-runtime` provides two concrete, single-owner executors:

| Runtime | Clock | Foreign-thread task wakes | Intended use |
| --- | --- | --- | --- |
| `SimRuntime` | deterministic virtual nanoseconds | rejected as nondeterministic input | simulation, replay, and modeled faults |
| `HostRuntime` | host-monotonic nanoseconds since construction | accepted through bounded ingress and used to unpark the owner | host application tasks and production I/O completions |

They share task, join, cancellation, timer, and panic-containment machinery, but
they are siblings rather than providers behind a `Runtime` trait. Wiring code
chooses a concrete runtime; application actors that only need the shared local
surface can take the portable `RuntimeHandle` sum type and run unchanged on
either executor. Simulation-only stepping, snapshots, trace artifacts, and
replay semantics do not leak into host execution, while host time and
nondeterministic completion order do not leak into simulation.

Both runtimes poll one future at a time on an owner thread. A local task may be
`!Send`, and a future that does not return from `poll` cannot be preempted. Run
one `HostRuntime` per thread or core when several independent execution lanes
are needed; `HostRuntime` is not a work-stealing executor.

## Concrete task surfaces

`SimRuntime::handle()` returns the simulation `Handle`. `HostRuntime::handle()`
returns a cloneable, owner-local `HostHandle` for spawning `'static` futures
that may be `!Send`, creating sleeps, reading host time, and drawing
from the runtime's seeded random streams. `HostHandle::current()` is available
while one of that runtime's tasks is being polled.

`RuntimeHandle` wraps either local handle behind one enum (`From` conversions
exist for both) and exposes their shared surface: `now`, `spawn`, `sleep`,
`sleep_until`, and the workload random operations. Yielding needs no handle;
use the free `yield_now` function.
`RuntimeHandle::current()` resolves the owning executor from inside any task.
It is the intended parameter type for runtime-agnostic application actors;
executor-specific code can match on the variant.

`HostRuntime::send_handle()` returns a `HostSendHandle`. It is `Clone + Send +
Sync` and admits bounded spawns whose future and output are `Send + 'static`.
The task is still polled only by the runtime owner; `Send` permits transfer
through ingress and does not imply task migration.

Local spawns on both executors return the shared `JoinHandle<T>` and
`AbortHandle` types. A send-spawn returns `HostSendJoinHandle<T>`, whose join
state and abort route may cross threads. Dropping either join handle detaches
the task; cancellation is requested explicitly through `abort` and is applied
at a scheduler boundary. Completion before that boundary wins. Cancellation
drops the future but cannot roll back an I/O effect that was already submitted.

`Handle::sleep` and `HostHandle::sleep` likewise return the shared `Sleep`
future. A sleep retains its originating runtime route, rejects polling from a
different runtime, unregisters on drop, and becomes terminal when its runtime
stops. `RuntimeInstant` and `RuntimeDuration` are the shared public integer-
nanosecond coordinates; simulation code may use the exact `SimInstant` and
`SimDuration` aliases. Simulation advances them virtually, while host execution
maps them to elapsed host monotonic time from an epoch captured at construction.

## The wake boundary

The standard `Waker` contract permits a waker to cross threads even when the
underlying task is local. The two executors deliberately assign different
meaning to that event.

`SimRuntime` accepts owner-thread wakes into its deterministic ordered admission
path. A foreign-thread wake latches `NondeterministicExternalWake`; simulation
must consume a recorded or modeled external-event order instead of inheriting
kernel scheduling order.

`HostRuntime` accepts foreign-thread wakes. Each task signal coalesces repeated
notifications, and every newly admitted wake enters bounded ingress and calls
`Thread::unpark` on the owner. The owner drains at most
`HostConfig::max_ingress_per_turn` entries before polling ready work, so an
external producer cannot make a single scheduler turn unbounded. Stale task IDs
remain harmless because the shared task slab is generation tagged.

This is the bridge used by production I/O. `UringFile`, `UringByteStream`, and
the other Linux providers keep their existing dedicated actor/reactor threads.
When one of their ordinary operation futures becomes ready, its stored waker
enters the host runtime and unparks the task owner. The runtime itself does not
own an io_uring instance and does not expose SQEs or CQEs.

```text
SimRuntime -> Handle -> deterministic provider/model

HostRuntime -> HostHandle task -> UringFile operation
    ^                                |
    | ordinary Waker                 v
    +------ bounded host ingress <- dedicated io_uring host

HostRuntime -> HostHandle task -> UringRing operation
    ^                                |
    +------ bounded host ingress <- dedicated ring host
```

## Lifecycle and failure policy

`HostRuntime::new(HostConfig)` fixes the seed and task, timer, and ingress
bounds. The controller exposes `handle()`, `send_handle()`, `control()`,
`block_on`, `shutdown`, and consuming `finish` operations. `block_on` drives a
borrowed root until it completes, stop is requested, or driving reports an
error; it does not require the root to be `Send` or `'static`.

`HostControl` may cross threads. `request_stop()` is idempotent, bypasses normal
bounded ingress, and unparks the owner. `status()` returns the `HostStatus`
lifecycle enum: `Running`, `StopRequested`, `Stopped`, or `Failed`. Reading
status does not require access to owner-local scheduler state.

Spawned-task panics resolve that task's `JoinHandle` with
`JoinError::Panicked`; unrelated host tasks continue. A root panic is returned
from `block_on`. A panic while dropping a task or invoking a registered waker
invalidates host execution and moves the runtime to `Failed`. As in simulation,
structured panic capture requires `panic = "unwind"`; a `panic = "abort"`
process terminates before the runtime can return a structured error.

Shutdown first makes joins and timers terminal, then drops task futures under
panic containment. Call `finish` at the ownership boundary so a teardown
failure is observable; plain `Drop` can only perform best-effort cleanup.

## Choosing the boundary

Use `SimRuntime` for reproducible schedules, virtual time, modeled faults,
snapshots, and replay artifacts. Use `HostRuntime` to host the same style of
single-owner application future while accepting real completion wakes. Keep
application state machines dependent on the owned I/O and ring contracts rather
than on io_uring details or an executor substitution trait.

`HostRuntime` does not add work stealing, a Tokio adapter, epoll/kqueue reactors,
or non-Linux production I/O. Those remain separate concerns. The `Send*`
companion traits in `kr-runtime-io` and the ring crates continue to describe
whether provider handles and operation futures may cross threads; they do not
strengthen ordering, durability, cancellation, or completion-certainty
semantics.

The one execution resource the host runtime does provision is the blocking
capability: `HostRuntime::blocking` lazily spawns `blocking_workers` threads
and returns the cloneable `HostBlocking` handle for punting fire-and-forget
blocking closures — the seam io_uring providers plug into through
`UringEnv::on_runtime`, so provider lifecycle syscalls and runtime-owned
workers converge without an executor trait. Jobs own their completion path
and must guard terminal reporting; the workers contain per-job panics and
live as long as any capability clone, not the runtime, so submission is
infallible and provider teardown never races runtime shutdown. Simulation
deliberately has no analog: sim providers complete in virtual time, and the
capability never appears on the portable surface.
