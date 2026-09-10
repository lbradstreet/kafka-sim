# Workspace conventions

Distilled contributor rules for this workspace. `DESIGN.md` is the
authoritative contract; `BENCHMARKING.md` is the measurement protocol;
`PRODUCTION-RUNTIME.md` is the runtime-boundary guide. This file states the
recurring disciplines the code enforces, so new code matches them without
re-reading everything.

## Workspace map

- Root crate `kr-runtime`: the two concrete executors (`SimRuntime`,
  `HostRuntime`), the portable `RuntimeHandle` sum, and the shared
  task/timer/RNG/panic/trace kernel.
- `io/kr-runtime-io`: owned-completion I/O contracts, the sim and memory providers,
  the shared `completion` module every provider future is built on, and the
  provider conformance suite (behind the `test-support` feature).
- `io/kr-runtime-io-uring`: the Linux production provider. The only crate allowed
  `unsafe`.
- `storage/kr-runtime-ring`, `storage/kr-runtime-ring-uring`: the checksummed ring format
  and its Linux io_uring driver.
- `trace/kr-runtime-trace-wire` and `trace/kr-runtime-ring-trace-wire`: the generated SBE
  codecs for the runtime and storage trace artifacts (the storage codec sits
  beside the runtime codec, no longer nested under the viewer). `tools/trace-tool`:
  the binary trace artifact tooling and static viewer.
- `dogfood/quarry`: the reference application and simulation-campaign
  harness.

## Correctness disciplines

**Fail closed on ambiguity.** An ambiguous state is never carried past the
boundary that observed it: simulation latches the first fatal error and
returns it from every later drive; the ring poisons itself on any
`MayHaveApplied` and fences all later operations with `RecoveryRequired`;
format decoders accept only canonical encodings; artifact readers strict-
reject unknown versions, modes, and tags instead of defaulting.

**Certainty-tagged side effects.** Every side-effecting failure carries
`CompletionCertainty` (`NotApplied` / `Applied` / `MayHaveApplied`).
Validation and backpressure rejections are always `NotApplied` and happen
before any effect. Test certainty behavior as an explicit truth table
(certainty × poisoned × accepted/durable), as in quarry's durability tests.

**Typed exhaustion, checked arithmetic.** Scheduler identifiers, sequences and
configured limits use checked arithmetic and typed `*Exhausted` errors, and a
failed advance must leave state untouched — write the regression test proving
it (see `TimerStore` tests). As specified in `DESIGN.md`, lifetime-scale RNG draw,
enabled trace sequence and recorder dropped-event counters currently panic on
exhaustion; making those infallible paths recoverable requires a coordinated
API redesign. Other `expect`/`unreachable!` calls are reserved for proof-carrying
invariants ("timer was just observed"), never for fallible external conditions. The
storage layer treats allocation failure as a typed error via `try_reserve`.

**Panic policy has three tiers plus one exception.** (1) Contain and discard
with `contain_panic` at committed notification boundaries: every waker wake,
clone, and drop, and every future drop during cleanup. (2) Capture into a
bounded `PanicRecord` at every poll boundary via `catch_unwind`. (3) Panic
deliberately, uncontained, on API misuse (polling a completed future), with a
`# Panics` doc section. The exception: `std::process::abort()` only where
honest unwinding is impossible because the kernel may still own caller
memory — arm `FailStopOnPanic` at the top of every pointer-bearing io_uring
region. A cleanup failure never replaces the initiating error; it is attached
as secondary context.

**No foreign code under provider state.** Completing an operation can run a
caller waker or destructor, which can panic or re-enter the provider. Commit
terminal state under the lock, then wake and drop outside it: use the shared
`completion` primitives (`SyncCell::set_output` returns the waker for a
deferred wake), the `transact`/`Actions` pattern in memory providers, and
`lock_unpoisoned` instead of `.lock().unwrap()`. Dedup wakers with
`will_wake` before cloning. Providers never hand-roll response futures; use
`kr_runtime_io::completion::{LocalOperation, SyncOperation}`.

**Determinism is versioned and structurally enforced.** Simulation code never
reads the host clock, OS entropy, or thread identity; foreign-thread wakes
are typed errors, not accepted input. Randomness comes from domain-separated
streams (`Workload`, `Scenario`, `Schedule`, `Fault`, `Debug`) derived from
one seed — never mix domains, and validate arguments before consuming a draw.
Anything replay-affecting (RNG mapping, checkpoint shape, trace schema,
artifact container) carries a version constant pinned by a golden test, so a
contract change fails a test until the version is deliberately bumped.
Observability must be passive: a traced run and an untraced run produce the
same determinism checkpoint.

## API conventions

- Errors are `#[non_exhaustive]` enums with hand-written `Display`, an
  `std::error::Error` impl, and the offending values carried in the variant.
  Runtime errors classify themselves via `const fn disposition()`
  (Resumable / Terminal / Fatal). Document fallible methods with `# Errors`
  sections — `sim.rs` and `handle.rs` are the canonical style; a fallible
  public method without one is drift, not license.
- `#[must_use]` on pure accessors and constructors; `const fn` wherever the
  body allows; overflow-prone constructors return `Option` or a typed error.
- Capabilities are cloneable handle types, not traits. Runtime-agnostic
  application code takes `RuntimeHandle` (or `impl Into<RuntimeHandle>` at
  entry points, like quarry's `start_broker`); driving and cross-thread
  admission stay concrete per executor. There is no executor trait.
- Ownership boundaries are explicit: consuming `finish()` performs checked
  teardown, `Drop` is best-effort only. I/O requests take owned buffers and
  return the same allocation on success and failure.
- Every I/O domain has two layers, and application code uses the cold one.
  The cold handles (`ColdFile`, `ColdNetwork`, `ColdListener`, `ColdStream`,
  `ColdDatagramNetwork`, `ColdDatagramSocket`) build an owned `'static` future
  and admit nothing; the first poll admits exactly once, so a never-polled
  future has not started, and successful `listen`, `connect`, `accept`, and
  `bind` return cold-wrapped handles so code never falls through to the warm
  boundary by accident. The warm `*Submit` traits beneath them admit eagerly
  during the method call and belong to providers and to systems code that
  intentionally needs explicit submission. Ordering and `sync` fencing follow
  first-poll order on the cold side and invocation order on the warm side —
  `kr-runtime-ring` stays warm because invocation-order fencing is part of its
  durability contract. Post-admission semantics are identical in both layers:
  dropping a future abandons only the response, never the effect or the
  buffer, so a dropped admitted receive may still consume one datagram.
- `unsafe` exists only in `kr-runtime-io-uring` (`unsafe_op_in_unsafe_fn = "deny"`),
  funneled into `ring.rs`, and every unsafe block carries a `// SAFETY`
  comment. Everything else is `#![forbid(unsafe_code)]`.
- Generated code (`trace/kr-runtime-trace-wire`, browser IR) is never hand-edited;
  regenerate with the pinned-checksum scripts and verify with their
  `--check` mode.

## Testing conventions

- Test names are descriptive sentences:
  `stale_waker_cannot_wake_a_reused_task_slot`. White-box injection helpers
  are `*_for_test` behind `#[cfg(test)]`. Negative type guarantees (a handle
  that must not be `Send`) are `compile_fail` doctests.
- New providers run the shared conformance suites unchanged — both the warm
  suite and its cold counterpart, which are paired per domain
  (`check_datagram_provider` / `check_cold_datagram_provider`, and likewise
  for files, networks, and streams). New conformance checks go in
  `kr_runtime_io::conformance`, reusable scaffolding in the per-domain
  `test_support` modules. Sim/host behavioral equivalence is asserted by
  parity tests that drive one actor through both executors.
- Campaign style (quarry is the model): a passive reference model checked
  after every operation; oracles meta-tested to reject mutated, missing, and
  stale state; two-level coverage gating (per-seed baseline plus aggregate)
  so a campaign that silently exercises nothing fails; failures print the
  seed, campaign version, and a copy-pasteable repro command; named
  regression seeds are kept and replayed; failing inputs are shrunk with the
  attempt-bounded `bounded_ddmin`.
- Ordinary randomized tests get multi-seed coverage from `kr_runtime::seed_sweep!`
  (behind kr-runtime's `test-support` feature): seeds default to `0..N`,
  `KR_RUNTIME_SEED` pins one seed and `KR_RUNTIME_SEEDS` overrides the width, a failing
  seed prints a copy-pasteable repro command, and a zero-seed sweep fails
  rather than silently passing. Pair the seed with
  `RuntimeConfig::derived_start_time(seed)` so sweeps also vary the epoch.
- Cancellation safety is tested by polling to `Pending`, dropping the future,
  and asserting exact provider side effects and subsequent fencing.
- Exact assertions over tolerances: virtual-time deadlines, ring cursors, and
  draw counts are asserted to exact values.

## Process

- Commits: imperative sentence-case subject, body explains why, one logical
  change per commit. No conventional-commit prefixes.
- Benchmarks follow `BENCHMARKING.md`: the three overhead questions are never
  combined, baselines must be matched (QD1 vs batching are different
  benchmarks with different names), environments are recorded, absolute
  numbers are reported alongside ratios, and smoke mode is not a result.
- `cargo test --workspace`, `cargo clippy --workspace --all-targets`, and
  `cargo fmt --check` are expected clean. `kr-runtime-io-uring` compiles and tests
  only on Linux; changes to it must be verified on a Linux machine or VM.

## Known caveats — conventions with no compiler behind them

- `FailStopOnPanic` is armed by convention at the top of every
  pointer-bearing io_uring region; nothing ties it to `PendingTransfer` by
  type. A new kernel-owned-buffer code path must arm it explicitly or it
  silently loses the abort guarantee.
- Datagram failures carry `Option<Vec<u8>>`, unlike storage's mandatory
  buffer field. A send or receive rejection must still return the caller's
  buffer via `DatagramFailure::with_buffer`; `without_buffer` is only for
  operations that never took one.
- Bounded-resource exhaustion errors use the `ResourceExhausted
  { resource, limit }` shape in every domain; do not introduce new
  domain-specific spellings of "the queue is full".
- `storage/memory.rs` completes synchronously under its mutex; the comment
  at its `FileIo` impl explains why pending semantics must move to the
  `completion` primitives.

## Feature log

`AGENTS.md` tracks wanted-but-unscheduled features. Add new feature ideas
there; check it before proposing work that may already be listed.
