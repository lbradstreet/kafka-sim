---
name: kr-runtime-app-dev
description: How to build and test applications on the kr-runtime runtime — the programming model (portable RuntimeHandle actors, single-owner executors, owned-completion I/O), sim/host wiring, and the testing ladder from seed sweeps to model campaigns with fault injection. Use when writing or reviewing application code, actors, providers, or tests in this workspace, or when scaffolding a new component on kr-runtime.
---

# Building and testing applications on kr-runtime

kr-runtime is a simulation-first async runtime: the same application state machine
runs under a deterministic `SimRuntime` (virtual time, seeded randomness,
modeled faults, exact replay) and a production `HostRuntime` (monotonic time,
real I/O completions). A driver changes, not the algorithm. This skill
describes how to structure application code for that model and how to test it.

Authoritative references: `DESIGN.md` (contract), `PRODUCTION-RUNTIME.md`
(runtime boundary), `io/kr-runtime-io/README.md` (I/O contracts),
`dogfood/quarry/` (the reference application), `CLAUDE.md` (workspace
disciplines). When this skill and those disagree, they win.

## Mental model

- **Two concrete executors, no runtime trait.** `SimRuntime` and `HostRuntime`
  are siblings sharing a task/timer/RNG/panic kernel. There is deliberately no
  executor trait; wiring code picks a concrete runtime.
- **`RuntimeHandle` is the portable capability** (`src/handle.rs`): a closed
  enum over sim `Handle` and `HostHandle` exposing the shared surface — `now`,
  `spawn`, `sleep`, `sleep_until`, and workload randomness. Application actors
  take `RuntimeHandle` (or `impl Into<RuntimeHandle>` at entry points) and run
  unchanged on both executors. Construction, driving, and cross-thread
  admission stay concrete and outside the portable surface.
- **Single-owner cooperative polling.** Each runtime polls one future at a
  time on its owner thread; tasks may be `!Send`. A poll is atomic and
  zero-virtual-time in simulation — CPU-only work must `yield_now().await` or
  await a modeled operation before timers can interleave. There is no work
  stealing; run one `HostRuntime` per thread when multiple lanes are needed.
- **Determinism is structural.** Simulation code never reads the host clock,
  OS entropy, or thread identity. A foreign-thread wake inside simulation is a
  typed fatal error (`NondeterministicExternalWake`), not accepted input.
  `HostRuntime` accepts foreign wakes through bounded ingress — that is the
  bridge production I/O providers use.

## Building an application

### Layering (quarry is the template)

1. **Passive engine** — a deterministic state machine that reads no clock and
   draws no randomness; the caller supplies every timestamp (quarry's
   `InMemoryQueue`). This layer gets an independent oracle.
2. **Actor** — runs the engine on either executor through `RuntimeHandle`,
   with a bounded command mailbox, one-shot replies, immediate `Backpressure`
   on a full mailbox, and explicit shutdown (quarry's `start_broker`,
   `dogfood/quarry/src/broker.rs`).
3. **Durable/IO layer** — generic over an owned-completion provider trait, so
   the same logic runs over a sim provider, a memory reference provider, or
   the Linux io_uring provider (quarry's `DurableQueue<R: RingWriter>`).

### Entry points take `impl Into<RuntimeHandle>`

```rust
pub fn start_broker(
    handle: impl Into<RuntimeHandle>,
    config: QueueConfig,
    command_capacity: usize,
) -> Result<(QueueClient, BrokerJoin), BrokerStartError> {
    let handle = handle.into();
    // validate args, build engine and bounded channel...
    let join = handle.spawn(async move { run_broker(...).await })?;
    Ok((client, join))
}
```

`Handle: Into<RuntimeHandle>` and `HostHandle: Into<RuntimeHandle>`, so
`start_broker(runtime.handle(), ...)` compiles for both runtimes. Inside the
actor use only `handle.now()`, `handle.spawn(...)`, `handle.sleep(...)`,
`handle.random_*()`. Never expose per-runtime constructors or a private
dispatch enum. `RuntimeHandle::current()` resolves the owning executor from
inside a task; the free `kr_runtime::yield_now()` needs no handle.

### Tasks, joins, cancellation

- `handle.spawn(fut)?` returns `JoinHandle<T>`; `.await` yields
  `Result<T, JoinError>` (`Cancelled | Panicked(PanicRecord) | RuntimeStopped`).
- Dropping a `JoinHandle` **detaches**; cancellation is explicit via
  `.abort()` / `AbortHandle`, applied at a scheduler boundary. Completion
  before that boundary wins. Cancellation never rolls back a submitted I/O
  effect.
- Spawn failure is typed: `SpawnError::ResourceExhausted { resource, limit }`.
  Handle it (quarry falls back to lazy lease expiry when the task limit is
  hit) — do not `unwrap` spawns on bounded-runtime paths.
- Panics: a sim task panic is **fatal to the run** (a failed model run must
  not look successful); a host spawned-task panic resolves that join with
  `JoinError::Panicked` and unrelated tasks continue. Structured capture
  requires `panic = "unwind"`.

### Time and randomness

- `RuntimeInstant` / `RuntimeDuration` are integer nanoseconds
  (`SimInstant` / `SimDuration` are aliases). Simulation advances them
  virtually; host maps them to elapsed monotonic time from construction.
- Handle randomness (`random_u64`, `random_below`, `random_bool_ratio`) draws
  from the **Workload** stream only. Privileged domain-separated streams
  (`Schedule`, `Scenario`, `Fault`, `Debug`) come from
  `SimRuntime::random_source(RandomStream::…)` — harness-side, never mixed,
  arguments validated before consuming a draw.

### I/O: cold handles over owned completions

Application code uses the **cold** handles from `kr-runtime-io`: `ColdFile`,
`ColdNetwork` / `ColdListener` / `ColdStream`, `ColdDatagramNetwork` /
`ColdDatagramSocket`. Calling an operation method constructs a future and
admits nothing; the first poll attempts admission exactly once; a never-polled
future has not started. The warm `*Submit` traits (eager admission at the
method call) are for providers and systems code like `kr-runtime-ring`.

The buffer contract: requests take owned `Vec<u8>` buffers, and the same
allocation comes back on success **and** failure
(`ReadAtSuccess { buffer, bytes_read }`,
`WriteAtFailure { error, buffer, bytes_transferred }`, …). Dropping a future
after admission abandons only the response — the effect and the buffer stay
with the provider until terminal completion. Only an explicit cancellation
operation cancels; `Future::drop` does not.

Every side-effecting failure carries `CompletionCertainty`
(`NotApplied` / `Applied` / `MayHaveApplied`) via
`CompletionError<E>` / `CompletionResult<T, E>`. Application layers must fail
closed on ambiguity: on `MayHaveApplied`, poison the affected state and fence
every later operation (quarry returns `RecoveryRequired` until the handle is
discarded and recovered). Validation and backpressure rejections are always
`NotApplied` and happen before any effect.

Providers to wire in:

| Provider | Needs | Use |
| --- | --- | --- |
| `SimStorage` / `SimNetwork` / `SimDatagramNetwork` | sim `Handle` | deterministic campaigns; scripted faults, latency, crash/reopen |
| `MemoryFile` / `MemoryNetwork` / `MemoryDatagramNetwork` | nothing | fast reference tests, concurrent host-side tests |
| `UringFile` / `UringByteStream` / `UringRing` (Linux) | `HostRuntime` | production; completions arrive as ordinary wakes through bounded host ingress |

### Wiring and lifecycle

```rust
// Simulation
let mut runtime = SimRuntime::new(RuntimeConfig { seed, start_time: RuntimeConfig::derived_start_time(seed), ..Default::default() });
let (client, broker) = start_broker(runtime.handle(), config, cap)?;
runtime.block_on(async move { /* drive the client */ })?;
runtime.finish()?;   // or step()/run_until_stalled() for inspectable driving

// Host
let mut runtime = HostRuntime::new(HostConfig::default())?;
let (client, broker) = start_broker(runtime.handle(), config, cap)?;
runtime.block_on(async move { /* same code */ })?;
runtime.finish()?;
```

- `block_on` waits only for its root; other tasks remain. A harness needing
  quiescence calls `run_until_stalled()` and inspects
  `Idle` / `Stalled` / `Stopped` (`Stalled` can be an intentional model of a
  dead disk — it is data, not automatically a bug).
- **Always `finish()` at the ownership boundary.** Plain `Drop` is
  best-effort and silently loses teardown failures.
- Sim fatal errors latch: after the first fatal `RunError`, every later drive
  returns it. `RunError::disposition()` classifies
  Resumable / Terminal / Fatal.
- Cross-thread on host: `send_handle()` admits `Send + 'static` tasks through
  bounded ingress (returns `HostSendJoinHandle`); `control()` gives idempotent
  `request_stop()` and `status()` (`Running | StopRequested | Stopped | Failed`).

### API conventions for new code

Follow `CLAUDE.md` exactly; the short list: `#[non_exhaustive]` error enums
with hand-written `Display`, offending values in the variant, `# Errors` docs;
`checked_add` plus a typed `*Exhausted` error for any advancing counter, with
a regression test proving a failed advance leaves state untouched; exhaustion
errors use the shared `ResourceExhausted { resource, limit }` shape;
`#[must_use]` on pure accessors; capabilities are cloneable handle types, not
traits; `#![forbid(unsafe_code)]` everywhere except `kr-runtime-io-uring`.

## Testing

The ladder, from cheapest to strongest: unit tests on the passive engine →
conformance suite for providers → seed-swept randomized tests → sim/host
parity tests → model campaigns with fault injection. New application features
should land with the first three at minimum; state machines with an oracle
deserve a campaign.

### Seed sweeps (`kr_runtime::seed_sweep!`)

Behind kr-runtime's `test-support` feature
(`kr-runtime = { path = ..., features = ["test-support"] }` in dev-dependencies):

```rust
kr_runtime::seed_sweep!(8, |seed| {
    let config = RuntimeConfig {
        seed,
        start_time: RuntimeConfig::derived_start_time(seed),  // always pair these
        ..RuntimeConfig::default()
    };
    // build runtime, run scenario, assert; compare determinism_checkpoint()
    // across reruns for replay-identity tests
});
```

`KR_RUNTIME_SEED=3` pins one seed for reproduction; `KR_RUNTIME_SEEDS=64` widens the
sweep; a failing seed prints a copy-pasteable
`KR_RUNTIME_SEED=<n> cargo test -p <pkg> <test> -- --exact --nocapture` line; a
zero-seed sweep fails rather than silently passing. `derived_start_time`
gives each seed a nonzero virtual epoch so absolute-time assumptions fail a
seed instead of hiding at zero. Campaigns use
`campaign_seed_range(prefix, default)` instead, which reads
`<PREFIX>_SEED_OFFSET` / `<PREFIX>_SEED_MULTIPLIER` for disjoint shards.

### Campaign architecture (copy `dogfood/quarry/tests/campaign.rs`)

A campaign is a per-seed loop with these mandatory parts:

1. **Bounded config** — seed, `derived_start_time(seed)`, explicit
   `max_tasks` / `max_timers` / `max_steps_per_run` / `max_time`.
2. **Domain-separated RNG** — `Scenario` picks a swarm profile, `Workload`
   generates operations, `Schedule` injects arrival jitter, `Fault` drives
   fault plans. Never mix streams.
3. **Passive reference model checked after every operation** — each op runs
   against the real system, then a `check_*` method computes the expected
   result and diffs it; the first divergence records `(step, message, model
   dump)` and stops.
4. **Oracle meta-tests** — separate tests mutate a known-good fixture
   (remove a job, add a phantom, flip a status, inject a wrong result) and
   assert the checker rejects each one, and that a rejected result does not
   count as coverage.
5. **Two-level coverage gating** — a per-seed baseline of events every seed
   must hit, plus an aggregate assertion that the merged coverage across all
   seeds hits every category. A campaign that silently exercises nothing must
   fail.
6. **Repro on failure** — print the seed, campaign version, and an exact
   command (e.g. `QUARRY_SEED=<n> cargo test -p quarry --test campaign
   reproduce_seed_from_environment -- --ignored --exact --nocapture`), backed
   by an `#[ignore]` test that reads the env var and replays that seed traced.
7. **Named regression seeds** — a `[("name", seed)]` corpus replayed every
   run regardless of sharding.
8. **Shrinking** — on failure, minimize the op sequence with the
   attempt-bounded `bounded_ddmin`
   (`dogfood/quarry/tests/support/mod.rs`), replaying subsequences through
   the same `run_seed_case(seed, traced, Some(ops), ...)` entry point.
9. **Passive observability** — the ordinary sweep runs untraced; a failing
   seed is rerun with tracing and the test asserts tracing changed nothing,
   then rerun again to assert identical replay.

### Fault injection and certainty truth tables

Sim providers take explicit fault scripts:
`SimStorage::inject(SimFault { operation, delay, outcome, .. })`, plus
`.crash()` / reopen; `SimNetwork` scripts per-link latency, clogs, partitions,
and faults. Generate plans from the `Fault` stream and retain the realized
plan for replay. Assert `status().pending_faults` / `fault_hits` so a test
that injected nothing fails.

Test certainty as an explicit table — fault kind × expected
`CompletionCertainty` × poisoned? × effect applied? — and assert all four
columns per row, exactly (accepted/durable cursors advance by the exact
expected amount). See
`dogfood/quarry/tests/durability.rs::append_completion_matrix_controls_poisoning_and_acceptance`.
After any `MayHaveApplied`, assert every later operation is fenced
(`assert_every_operation_is_fenced` in quarry's test support).

### Parity tests

Factor the scenario as `async fn scenario(handle: RuntimeHandle) -> Output`,
run it under `SimRuntime::block_on` and `HostRuntime::block_on`, and
`assert_eq!` the outputs (`tests/host/parity.rs`). Providers get parity by
running the same conformance suite under both runtimes.

### Cancellation safety

Poll to `Pending` with a noop waker, drop the future, then assert exact
provider side effects and subsequent fencing:

```rust
fn poll_pending_then_drop<F: Future>(future: F) {
    let mut future = Box::pin(future);
    assert!(matches!(
        future.as_mut().poll(&mut Context::from_waker(Waker::noop())),
        Poll::Pending
    ));
}   // dropped here — response abandoned, effect NOT rolled back
```

Then assert the provider observed the admitted effect (call counters,
accepted/durable cursors) and that ambiguous state is fenced. See
`dogfood/quarry/tests/durability_cancellation.rs`. For actor-level
cancellation, `.abort()` a task mid-operation and assert accepted state is
not rolled back and stale tokens are fenced
(`dogfood/quarry/tests/cancellation.rs`).

### Conformance suite

Any new provider implements the relevant `*Submit` trait and runs the shared
suite unchanged (`kr-runtime-io` `test-support` feature):

```rust
let mut runtime = SimRuntime::default();
runtime
    .block_on(kr_runtime_io::conformance::check_empty_file(provider))
    .expect("runtime completes")
    .expect("provider conforms");
```

Checkers exist for files, byte streams, network providers, and datagrams, in
warm and cold variants. New conformance checks go in `kr_runtime_io::conformance`,
not in per-provider tests. Meta-test the suite with a deliberately wrong
provider when adding a check.

### Assertion style

Exact values, not tolerances: virtual-time deadlines, ring cursors, RNG draw
counts, and task/timer counts are asserted exactly
(`assert_eq!(runtime.snapshot().now.as_nanos(), 10_000_000)`). Test names are
descriptive sentences (`stale_waker_cannot_wake_a_reused_task_slot`).
White-box helpers are `*_for_test` behind `#[cfg(test)]`; negative type
guarantees are `compile_fail` doctests.

## Commands

```text
just t                         # all portable tests (excludes Linux-only uring crates)
just tp <pkg> <filter>         # one package, filtered
just r                         # format, lint, doctest, and test gate
cargo test --workspace --all-targets --all-features
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo fmt --all -- --check
```

`kr-runtime-io-uring` and `kr-runtime-ring-uring` build only on Linux; verify changes to
them on a Linux machine or VM.

## Pitfalls

- **`block_on` returning is not quiescence** — spawned tasks remain; drain
  with `run_until_stalled` and check the outcome variant.
- **Dropping is not cancelling** — join handles detach, admitted I/O futures
  abandon only the response. Abort explicitly; reconcile effects.
- **A tight loop starves timers** in simulation (a poll is atomic and takes
  zero virtual time) — yield or await a modeled operation.
- **Foreign wakes/aborts are fatal in sim** — anything touching another
  thread (channels with foreign senders, provider threads) belongs on
  `HostRuntime` or behind a modeled sim provider.
- **A seed alone is not a repro** — the driver, config, workload, fault plan,
  and budgets are part of replay identity; pin them in source and print the
  full command.
- **Don't weaken the engine into an I/O-shaped mock** — test the real code
  path over fault-injectable sim providers (quarry drives the real
  `FileRing` over `SimStorage`, not a queue-shaped stub).
- **`panic = "abort"` builds lose structured panic capture** — campaigns and
  replay require `panic = "unwind"`.
- **Traced and untraced runs must match** — observability is passive; assert
  the determinism checkpoint is unchanged when tracing is enabled.
