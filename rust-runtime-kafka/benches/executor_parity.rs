//! Matched owner-local workloads across both executor implementations.
//!
//! Every group runs one portable workload, written once against
//! [`RuntimeHandle`], on the untraced `SimRuntime` and on `HostRuntime`. The
//! executors share the task, timer, and join kernel, so a case difference
//! measures their scheduler policies: the simulation cases include virtual
//! time and deterministic wake admission, while the host cases include
//! monotonic clock reads and the per-turn ingress and lifecycle checks. No
//! case involves foreign threads, parking, tracing, or I/O; see
//! `host_overhead` for cross-thread wake costs and `scheduler_overhead` for
//! direct-execution baselines of the simulation cases.
//!
//! `portable_handle_dispatch` answers a narrower question: what the
//! `RuntimeHandle` enum dispatch adds over calling the same concrete handle.
//! Its concrete and portable cases are comparable only within one executor.

use std::cell::RefCell;
use std::hint::black_box;
use std::pin::pin;
use std::rc::Rc;
use std::task::{Poll, Waker};

use criterion::{BatchSize, Criterion, Throughput, criterion_group, criterion_main};
use kr_runtime::rng::{DeterministicRng, RandomStream};
use kr_runtime::{
    Handle, HostConfig, HostHandle, HostRuntime, RuntimeConfig, RuntimeHandle, RuntimeInstant,
    SimRuntime, yield_now,
};

const YIELDS_PER_RUN: usize = 1_024;
const TASKS_PER_RUN: usize = 256;
const TIMERS_PER_RUN: usize = 256;
const DRAWS_PER_RUN: usize = 1_024;
const PING_PONG_ROUNDS: usize = 512;
const SEED: u64 = 17;

#[inline]
fn work_value(value: usize) -> u64 {
    let value = black_box(value as u64);
    value.wrapping_mul(0x9e37_79b9_7f4a_7c15).rotate_left(17)
}

fn expected_checksum(count: usize) -> u64 {
    (0..count).fold(0, |checksum, value| {
        checksum.wrapping_add(work_value(value))
    })
}

/// Both executors derive the workload stream identically from the root seed,
/// so one expected checksum validates every random-draw case.
fn expected_draw_checksum(count: usize) -> u64 {
    let mut rng = DeterministicRng::from_root_seed(SEED, RandomStream::Workload);
    (0..count).fold(0, |checksum, _| checksum.wrapping_add(rng.next_u64()))
}

fn sim_runtime() -> SimRuntime {
    // The untraced fast path; runtime construction stays outside timing.
    SimRuntime::new(RuntimeConfig {
        seed: SEED,
        ..RuntimeConfig::default()
    })
}

fn host_runtime() -> HostRuntime {
    HostRuntime::new(HostConfig {
        seed: SEED,
        ..HostConfig::default()
    })
    .expect("benchmark host runtime config is valid")
}

async fn yielding_workload(count: usize) -> u64 {
    let mut checksum = 0_u64;
    for value in 0..black_box(count) {
        yield_now().await;
        checksum = checksum.wrapping_add(work_value(value));
    }
    checksum
}

async fn spawn_join_workload(handle: RuntimeHandle, count: usize) -> u64 {
    let mut checksum = 0_u64;
    for value in 0..black_box(count) {
        let task = handle
            .spawn(async move { work_value(value) })
            .expect("bounded benchmark task spawn succeeds");
        checksum = checksum.wrapping_add(task.await.expect("spawned task completes"));
    }
    checksum
}

/// Registers one far-future timer, polls it pending once, and drops it so
/// cancellation removes the registration. No case ever waits for a deadline.
async fn timer_register_cancel_workload(handle: RuntimeHandle, count: usize) -> usize {
    let mut registered = 0_usize;
    for _ in 0..black_box(count) {
        let mut sleep = pin!(handle.sleep_until(RuntimeInstant::MAX));
        std::future::poll_fn(|context| {
            assert!(
                sleep.as_mut().poll(context).is_pending(),
                "far-future benchmark timer completed"
            );
            Poll::Ready(())
        })
        .await;
        registered += 1;
    }
    registered
}

/// One single-slot mailbox: the minimal actor channel. Send stores the value
/// and wakes the receiver; receive takes the value or parks its waker.
type Slot = Rc<RefCell<SlotState>>;

struct SlotState {
    value: Option<u64>,
    waker: Option<Waker>,
}

fn empty_slot() -> Slot {
    Rc::new(RefCell::new(SlotState {
        value: None,
        waker: None,
    }))
}

fn slot_send(slot: &Slot, value: u64) {
    let waker = {
        let mut state = slot.borrow_mut();
        debug_assert!(state.value.is_none(), "benchmark slot overwritten");
        state.value = Some(value);
        state.waker.take()
    };
    if let Some(waker) = waker {
        waker.wake();
    }
}

async fn slot_recv(slot: &Slot) -> u64 {
    std::future::poll_fn(|context| {
        let mut state = slot.borrow_mut();
        if let Some(value) = state.value.take() {
            Poll::Ready(value)
        } else {
            state.waker = Some(context.waker().clone());
            Poll::Pending
        }
    })
    .await
}

/// Two actors exchange request/reply rounds through single-slot mailboxes.
/// Each round costs one wake and one poll per side, so the case measures the
/// cross-task wake-to-poll scheduling path rather than channel machinery.
async fn ping_pong_workload(handle: RuntimeHandle, rounds: usize) -> u64 {
    let to_pong = empty_slot();
    let to_ping = empty_slot();
    let pong = {
        let to_pong = Rc::clone(&to_pong);
        let to_ping = Rc::clone(&to_ping);
        handle
            .spawn(async move {
                for _ in 0..rounds {
                    let value = slot_recv(&to_pong).await;
                    slot_send(&to_ping, work_value(value as usize));
                }
            })
            .expect("pong actor spawn succeeds")
    };

    let mut checksum = 0_u64;
    for value in 0..black_box(rounds) {
        slot_send(&to_pong, value as u64);
        checksum = checksum.wrapping_add(slot_recv(&to_ping).await);
    }
    pong.await.expect("pong actor completes");
    checksum
}

async fn random_draw_workload(handle: RuntimeHandle, count: usize) -> u64 {
    let mut checksum = 0_u64;
    for _ in 0..black_box(count) {
        checksum = checksum.wrapping_add(handle.random_u64().expect("runtime is active"));
    }
    checksum
}

async fn concrete_sim_draw_workload(handle: Handle, count: usize) -> u64 {
    let mut checksum = 0_u64;
    for _ in 0..black_box(count) {
        checksum = checksum.wrapping_add(handle.random_u64().expect("runtime is active"));
    }
    checksum
}

async fn concrete_host_draw_workload(handle: HostHandle, count: usize) -> u64 {
    let mut checksum = 0_u64;
    for _ in 0..black_box(count) {
        checksum = checksum.wrapping_add(handle.random_u64().expect("runtime is active"));
    }
    checksum
}

fn executor_parity(criterion: &mut Criterion) {
    let expected_yields = expected_checksum(YIELDS_PER_RUN);
    let expected_tasks = expected_checksum(TASKS_PER_RUN);
    let expected_draws = expected_draw_checksum(DRAWS_PER_RUN);

    let mut group = criterion.benchmark_group("executor_parity/yield_poll_wake");
    group.throughput(Throughput::Elements(YIELDS_PER_RUN as u64));
    group.bench_function("sim_runtime_untraced", |bencher| {
        bencher.iter_batched_ref(
            || (sim_runtime(), Some(yielding_workload(YIELDS_PER_RUN))),
            |(runtime, future)| {
                let checksum = runtime
                    .block_on(future.take().expect("fresh benchmark future"))
                    .expect("bounded yield workload completes");
                assert_eq!(checksum, expected_yields);
                black_box(checksum)
            },
            BatchSize::PerIteration,
        );
    });
    group.bench_function("host_runtime", |bencher| {
        bencher.iter_batched_ref(
            || (host_runtime(), Some(yielding_workload(YIELDS_PER_RUN))),
            |(runtime, future)| {
                let checksum = runtime
                    .block_on(future.take().expect("fresh benchmark future"))
                    .expect("bounded yield workload completes");
                assert_eq!(checksum, expected_yields);
                black_box(checksum)
            },
            BatchSize::PerIteration,
        );
    });
    group.finish();

    let mut group = criterion.benchmark_group("executor_parity/spawn_join_sequential");
    group.throughput(Throughput::Elements(TASKS_PER_RUN as u64));
    group.bench_function("sim_runtime_untraced", |bencher| {
        bencher.iter_batched_ref(
            || {
                let runtime = sim_runtime();
                let handle = RuntimeHandle::from(runtime.handle());
                (runtime, Some(spawn_join_workload(handle, TASKS_PER_RUN)))
            },
            |(runtime, future)| {
                let checksum = runtime
                    .block_on(future.take().expect("fresh benchmark future"))
                    .expect("bounded spawn/join workload completes");
                assert_eq!(checksum, expected_tasks);
                black_box(checksum)
            },
            BatchSize::PerIteration,
        );
    });
    group.bench_function("host_runtime", |bencher| {
        bencher.iter_batched_ref(
            || {
                let runtime = host_runtime();
                let handle = RuntimeHandle::from(runtime.handle());
                (runtime, Some(spawn_join_workload(handle, TASKS_PER_RUN)))
            },
            |(runtime, future)| {
                let checksum = runtime
                    .block_on(future.take().expect("fresh benchmark future"))
                    .expect("bounded spawn/join workload completes");
                assert_eq!(checksum, expected_tasks);
                black_box(checksum)
            },
            BatchSize::PerIteration,
        );
    });
    group.finish();

    let expected_rounds = expected_checksum(PING_PONG_ROUNDS);
    let mut group = criterion.benchmark_group("executor_parity/actor_ping_pong");
    group.throughput(Throughput::Elements(PING_PONG_ROUNDS as u64));
    group.bench_function("sim_runtime_untraced", |bencher| {
        bencher.iter_batched_ref(
            || {
                let runtime = sim_runtime();
                let handle = RuntimeHandle::from(runtime.handle());
                (runtime, Some(ping_pong_workload(handle, PING_PONG_ROUNDS)))
            },
            |(runtime, future)| {
                let checksum = runtime
                    .block_on(future.take().expect("fresh benchmark future"))
                    .expect("bounded ping/pong workload completes");
                assert_eq!(checksum, expected_rounds);
                black_box(checksum)
            },
            BatchSize::PerIteration,
        );
    });
    group.bench_function("host_runtime", |bencher| {
        bencher.iter_batched_ref(
            || {
                let runtime = host_runtime();
                let handle = RuntimeHandle::from(runtime.handle());
                (runtime, Some(ping_pong_workload(handle, PING_PONG_ROUNDS)))
            },
            |(runtime, future)| {
                let checksum = runtime
                    .block_on(future.take().expect("fresh benchmark future"))
                    .expect("bounded ping/pong workload completes");
                assert_eq!(checksum, expected_rounds);
                black_box(checksum)
            },
            BatchSize::PerIteration,
        );
    });
    group.finish();

    // Registration and cancellation share the deadline-ordered timer store;
    // the host case additionally reads the monotonic clock on every poll.
    let mut group = criterion.benchmark_group("executor_parity/timer_register_cancel");
    group.throughput(Throughput::Elements(TIMERS_PER_RUN as u64));
    group.bench_function("sim_runtime_untraced", |bencher| {
        bencher.iter_batched_ref(
            || {
                let runtime = sim_runtime();
                let handle = RuntimeHandle::from(runtime.handle());
                (
                    runtime,
                    Some(timer_register_cancel_workload(handle, TIMERS_PER_RUN)),
                )
            },
            |(runtime, future)| {
                let registered = runtime
                    .block_on(future.take().expect("fresh benchmark future"))
                    .expect("bounded timer workload completes");
                assert_eq!(registered, TIMERS_PER_RUN);
                black_box(registered)
            },
            BatchSize::PerIteration,
        );
    });
    group.bench_function("host_runtime", |bencher| {
        bencher.iter_batched_ref(
            || {
                let runtime = host_runtime();
                let handle = RuntimeHandle::from(runtime.handle());
                (
                    runtime,
                    Some(timer_register_cancel_workload(handle, TIMERS_PER_RUN)),
                )
            },
            |(runtime, future)| {
                let registered = runtime
                    .block_on(future.take().expect("fresh benchmark future"))
                    .expect("bounded timer workload completes");
                assert_eq!(registered, TIMERS_PER_RUN);
                black_box(registered)
            },
            BatchSize::PerIteration,
        );
    });
    group.finish();

    // `direct_rng` is the generator floor without any runtime state checks.
    // Both executors derive the same stream, so every case must produce the
    // same checksum.
    let mut group = criterion.benchmark_group("executor_parity/workload_random_draws");
    group.throughput(Throughput::Elements(DRAWS_PER_RUN as u64));
    group.bench_function("direct_rng", |bencher| {
        bencher.iter_batched_ref(
            || DeterministicRng::from_root_seed(SEED, RandomStream::Workload),
            |rng| {
                let checksum = (0..black_box(DRAWS_PER_RUN))
                    .fold(0_u64, |checksum, _| checksum.wrapping_add(rng.next_u64()));
                assert_eq!(checksum, expected_draws);
                black_box(checksum)
            },
            BatchSize::PerIteration,
        );
    });
    group.bench_function("sim_runtime_untraced", |bencher| {
        bencher.iter_batched_ref(
            || {
                let runtime = sim_runtime();
                let handle = RuntimeHandle::from(runtime.handle());
                (runtime, Some(random_draw_workload(handle, DRAWS_PER_RUN)))
            },
            |(runtime, future)| {
                let checksum = runtime
                    .block_on(future.take().expect("fresh benchmark future"))
                    .expect("bounded draw workload completes");
                assert_eq!(checksum, expected_draws);
                black_box(checksum)
            },
            BatchSize::PerIteration,
        );
    });
    group.bench_function("host_runtime", |bencher| {
        bencher.iter_batched_ref(
            || {
                let runtime = host_runtime();
                let handle = RuntimeHandle::from(runtime.handle());
                (runtime, Some(random_draw_workload(handle, DRAWS_PER_RUN)))
            },
            |(runtime, future)| {
                let checksum = runtime
                    .block_on(future.take().expect("fresh benchmark future"))
                    .expect("bounded draw workload completes");
                assert_eq!(checksum, expected_draws);
                black_box(checksum)
            },
            BatchSize::PerIteration,
        );
    });
    group.finish();

    // Concrete-vs-portable pairs isolate the `RuntimeHandle` match dispatch
    // on the highest-frequency handle call. Compare within one executor only.
    let mut group = criterion.benchmark_group("executor_parity/portable_handle_dispatch");
    group.throughput(Throughput::Elements(DRAWS_PER_RUN as u64));
    group.bench_function("sim_concrete_handle", |bencher| {
        bencher.iter_batched_ref(
            || {
                let runtime = sim_runtime();
                let handle = runtime.handle();
                (
                    runtime,
                    Some(concrete_sim_draw_workload(handle, DRAWS_PER_RUN)),
                )
            },
            |(runtime, future)| {
                let checksum = runtime
                    .block_on(future.take().expect("fresh benchmark future"))
                    .expect("bounded draw workload completes");
                assert_eq!(checksum, expected_draws);
                black_box(checksum)
            },
            BatchSize::PerIteration,
        );
    });
    group.bench_function("sim_portable_handle", |bencher| {
        bencher.iter_batched_ref(
            || {
                let runtime = sim_runtime();
                let handle = RuntimeHandle::from(runtime.handle());
                (runtime, Some(random_draw_workload(handle, DRAWS_PER_RUN)))
            },
            |(runtime, future)| {
                let checksum = runtime
                    .block_on(future.take().expect("fresh benchmark future"))
                    .expect("bounded draw workload completes");
                assert_eq!(checksum, expected_draws);
                black_box(checksum)
            },
            BatchSize::PerIteration,
        );
    });
    group.bench_function("host_concrete_handle", |bencher| {
        bencher.iter_batched_ref(
            || {
                let runtime = host_runtime();
                let handle = runtime.handle();
                (
                    runtime,
                    Some(concrete_host_draw_workload(handle, DRAWS_PER_RUN)),
                )
            },
            |(runtime, future)| {
                let checksum = runtime
                    .block_on(future.take().expect("fresh benchmark future"))
                    .expect("bounded draw workload completes");
                assert_eq!(checksum, expected_draws);
                black_box(checksum)
            },
            BatchSize::PerIteration,
        );
    });
    group.bench_function("host_portable_handle", |bencher| {
        bencher.iter_batched_ref(
            || {
                let runtime = host_runtime();
                let handle = RuntimeHandle::from(runtime.handle());
                (runtime, Some(random_draw_workload(handle, DRAWS_PER_RUN)))
            },
            |(runtime, future)| {
                let checksum = runtime
                    .block_on(future.take().expect("fresh benchmark future"))
                    .expect("bounded draw workload completes");
                assert_eq!(checksum, expected_draws);
                black_box(checksum)
            },
            BatchSize::PerIteration,
        );
    });
    group.finish();
}

criterion_group!(benches, executor_parity);
criterion_main!(benches);
