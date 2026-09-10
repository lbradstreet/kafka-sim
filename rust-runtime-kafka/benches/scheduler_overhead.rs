//! Microbenchmarks for the untraced deterministic scheduler.
//!
//! These measurements isolate cooperative scheduling mechanics. They are not
//! an end-to-end application comparison and should not be extrapolated to
//! storage or network throughput. In particular, `direct_calls` is only a
//! lower bound; `direct_boxed_futures` is the closer task-shaped baseline for
//! spawn benchmarks.

use std::future::Future;
use std::hint::black_box;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::{Context, Poll, Wake, Waker};

use criterion::{BatchSize, BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use kr_runtime::{Handle, PollResult, RuntimeConfig, SimRuntime, Step, yield_now};

const YIELDS_PER_RUN: usize = 1_024;
const SEQUENTIAL_TASKS_PER_RUN: usize = 256;
const FANOUT_TASKS_PER_RUN: usize = 256;

struct CountingWake {
    count: AtomicUsize,
}

impl CountingWake {
    fn new() -> Self {
        Self {
            count: AtomicUsize::new(0),
        }
    }

    fn count(&self) -> usize {
        self.count.load(Ordering::Relaxed)
    }
}

impl Wake for CountingWake {
    fn wake(self: Arc<Self>) {
        self.count.fetch_add(1, Ordering::Relaxed);
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.count.fetch_add(1, Ordering::Relaxed);
    }
}

struct DirectDriver<F> {
    future: Pin<Box<F>>,
    wake: Arc<CountingWake>,
    waker: Waker,
}

impl<F> DirectDriver<F>
where
    F: Future,
{
    fn new(future: F) -> Self {
        let wake = Arc::new(CountingWake::new());
        let waker = Waker::from(Arc::clone(&wake));
        // The direct driver is intentionally task-shaped. Make the owned
        // future escape so release optimization cannot scalar-replace the
        // allocation that the benchmark claims to include.
        let future = black_box(Box::pin(future));
        Self {
            future,
            wake,
            waker,
        }
    }

    fn drive_to_completion(&mut self, max_polls: usize) -> (F::Output, usize, usize) {
        let mut polls = 0;
        loop {
            assert!(polls < max_polls, "direct future exceeded its poll bound");
            polls += 1;
            let mut context = Context::from_waker(&self.waker);
            match self.future.as_mut().poll(&mut context) {
                Poll::Ready(output) => return (output, polls, self.wake.count()),
                Poll::Pending => {}
            }
        }
    }
}

fn runtime_and_future<F>(future: F) -> (SimRuntime, Option<F>) {
    // `SimRuntime::new` is the fast path: no trace sink is installed and trace
    // events are not constructed. Runtime creation is excluded from timing.
    (SimRuntime::new(RuntimeConfig::default()), Some(future))
}

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

async fn yielding_workload(count: usize) -> u64 {
    let mut checksum = 0_u64;
    for value in 0..black_box(count) {
        yield_now().await;
        checksum = checksum.wrapping_add(work_value(value));
    }
    checksum
}

async fn sequential_spawn_join_workload(handle: Handle, count: usize) -> u64 {
    let mut checksum = 0_u64;
    for value in 0..black_box(count) {
        let task = handle
            .spawn(async move { work_value(value) })
            .expect("bounded sequential task spawn succeeds");
        checksum = checksum.wrapping_add(task.await.expect("spawned task completes"));
    }
    checksum
}

async fn fanout_fanin_workload(handle: Handle, count: usize) -> u64 {
    let mut tasks = Vec::with_capacity(count);
    for value in 0..black_box(count) {
        tasks.push(
            handle
                .spawn(async move { work_value(value) })
                .expect("bounded fanout task spawn succeeds"),
        );
    }

    let mut checksum = 0_u64;
    for task in tasks {
        checksum = checksum.wrapping_add(task.await.expect("spawned task completes"));
    }
    checksum
}

fn direct_calls(count: usize) -> u64 {
    (0..black_box(count)).fold(0, |checksum, value| {
        checksum.wrapping_add(work_value(value))
    })
}

fn poll_ready_future<F>(future: &mut Pin<Box<F>>, waker: &Waker) -> F::Output
where
    F: Future + ?Sized,
{
    let mut context = Context::from_waker(waker);
    match future.as_mut().poll(&mut context) {
        Poll::Ready(output) => output,
        Poll::Pending => panic!("ready direct-baseline future unexpectedly returned pending"),
    }
}

fn boxed_ready_future(value: usize) -> Pin<Box<dyn Future<Output = u64>>> {
    let task: Pin<Box<dyn Future<Output = u64>>> = Box::pin(async move { work_value(value) });
    // Without an escape barrier, the immediately-polled sequential case can
    // be scalar-replaced and cease to measure an owned future allocation.
    black_box(task)
}

fn direct_boxed_futures(count: usize, waker: &Waker) -> u64 {
    let mut checksum = 0_u64;
    for value in 0..black_box(count) {
        let mut task = boxed_ready_future(value);
        checksum = checksum.wrapping_add(poll_ready_future(&mut task, waker));
    }
    checksum
}

fn direct_boxed_fanout(count: usize, waker: &Waker) -> u64 {
    let mut tasks: Vec<Pin<Box<dyn Future<Output = u64>>>> = Vec::with_capacity(count);
    for value in 0..black_box(count) {
        tasks.push(boxed_ready_future(value));
    }

    tasks.into_iter().fold(0, |checksum, mut task| {
        checksum.wrapping_add(poll_ready_future(&mut task, waker))
    })
}

fn scheduler_overhead(criterion: &mut Criterion) {
    let expected_yields = expected_checksum(YIELDS_PER_RUN);
    let expected_sequential = expected_checksum(SEQUENTIAL_TASKS_PER_RUN);
    let expected_fanout = expected_checksum(FANOUT_TASKS_PER_RUN);

    // Both cases drive the exact same future. The direct driver still performs
    // a real `Wake` call and verifies every `Pending` poll requested progress;
    // the runtime case adds ready-queue, task-state, and root-join mechanics.
    let mut group = criterion.benchmark_group("scheduler_overhead/yield_poll_wake");
    group.throughput(Throughput::Elements(YIELDS_PER_RUN as u64));
    group.bench_function("direct_poll_wake", |bencher| {
        bencher.iter_batched(
            || yielding_workload(YIELDS_PER_RUN),
            |future| {
                // Executor-owned future allocation and waker construction are
                // timed, just as root-task registration is in `block_on`.
                let mut driver = DirectDriver::new(future);
                let (checksum, polls, wakes) =
                    driver.drive_to_completion(YIELDS_PER_RUN.saturating_add(1));
                assert_eq!(checksum, expected_yields);
                assert_eq!(polls, YIELDS_PER_RUN + 1);
                assert_eq!(wakes, YIELDS_PER_RUN);
                black_box(checksum)
            },
            BatchSize::PerIteration,
        );
    });
    group.bench_function("sim_runtime_untraced", |bencher| {
        bencher.iter_batched_ref(
            || runtime_and_future(yielding_workload(YIELDS_PER_RUN)),
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

    // `direct_calls` is an algorithmic floor. `direct_boxed_futures` includes
    // one owned future allocation and poll per unit and is the meaningful
    // comparison for the runtime's sequential spawn/join lifecycle.
    let mut group = criterion.benchmark_group("scheduler_overhead/spawn_join_sequential");
    group.throughput(Throughput::Elements(SEQUENTIAL_TASKS_PER_RUN as u64));
    group.bench_function("direct_calls", |bencher| {
        bencher.iter(|| {
            let checksum = direct_calls(SEQUENTIAL_TASKS_PER_RUN);
            assert_eq!(checksum, expected_sequential);
            black_box(checksum)
        });
    });
    group.bench_function("direct_boxed_futures", |bencher| {
        bencher.iter_batched_ref(
            || Waker::from(Arc::new(CountingWake::new())),
            |waker| {
                let checksum = direct_boxed_futures(SEQUENTIAL_TASKS_PER_RUN, waker);
                assert_eq!(checksum, expected_sequential);
                black_box(checksum)
            },
            BatchSize::PerIteration,
        );
    });
    group.bench_function("sim_runtime_untraced", |bencher| {
        bencher.iter_batched_ref(
            || {
                let runtime = SimRuntime::new(RuntimeConfig::default());
                let future =
                    sequential_spawn_join_workload(runtime.handle(), SEQUENTIAL_TASKS_PER_RUN);
                (runtime, Some(future))
            },
            |(runtime, future)| {
                let checksum = runtime
                    .block_on(future.take().expect("fresh benchmark future"))
                    .expect("bounded sequential spawn/join workload completes");
                assert_eq!(checksum, expected_sequential);
                black_box(checksum)
            },
            BatchSize::PerIteration,
        );
    });
    group.finish();

    // Fanout/fanin keeps all child tasks live together. Its direct baseline
    // likewise constructs the full batch before polling it, but it does not
    // emulate scheduler ordering, task IDs, or join notification.
    let mut group = criterion.benchmark_group("scheduler_overhead/fanout_fanin");
    group.throughput(Throughput::Elements(FANOUT_TASKS_PER_RUN as u64));
    group.bench_function("direct_boxed_futures", |bencher| {
        bencher.iter_batched_ref(
            || Waker::from(Arc::new(CountingWake::new())),
            |waker| {
                let checksum = direct_boxed_fanout(FANOUT_TASKS_PER_RUN, waker);
                assert_eq!(checksum, expected_fanout);
                black_box(checksum)
            },
            BatchSize::PerIteration,
        );
    });
    group.bench_function("sim_runtime_untraced", |bencher| {
        bencher.iter_batched_ref(
            || {
                let runtime = SimRuntime::new(RuntimeConfig::default());
                let future = fanout_fanin_workload(runtime.handle(), FANOUT_TASKS_PER_RUN);
                (runtime, Some(future))
            },
            |(runtime, future)| {
                let checksum = runtime
                    .block_on(future.take().expect("fresh benchmark future"))
                    .expect("bounded fanout/fanin workload completes");
                assert_eq!(checksum, expected_fanout);
                black_box(checksum)
            },
            BatchSize::PerIteration,
        );
    });
    group.finish();
}

fn sparse_ready_step(criterion: &mut Criterion) {
    let mut group = criterion.benchmark_group("scheduler_overhead/sparse_ready_step");
    group.throughput(Throughput::Elements(1));
    for dormant_tasks in [0, 1_000, 10_000, 100_000] {
        group.bench_with_input(
            BenchmarkId::new("sim_runtime_untraced", dormant_tasks),
            &dormant_tasks,
            |bencher, &dormant_tasks| {
                let mut runtime = SimRuntime::new(RuntimeConfig {
                    max_tasks: dormant_tasks + 1,
                    ..RuntimeConfig::default()
                });
                for _ in 0..dormant_tasks {
                    runtime
                        .handle()
                        .spawn(std::future::pending::<()>())
                        .unwrap();
                }
                // Registration and initial polls are fixture setup. Only the
                // one continuously ready task is polled in the timed region.
                for _ in 0..dormant_tasks {
                    assert!(matches!(runtime.step().unwrap(), Step::TaskPolled { .. }));
                }
                let active = runtime
                    .handle()
                    .spawn(std::future::poll_fn(|context| {
                        context.waker().wake_by_ref();
                        Poll::<()>::Pending
                    }))
                    .unwrap();
                let expected = Step::TaskPolled {
                    task: active.id(),
                    result: PollResult::Pending,
                };
                bencher.iter(|| {
                    assert_eq!(black_box(runtime.step().unwrap()), expected);
                });
            },
        );
    }
    group.finish();
}

criterion_group!(benches, scheduler_overhead, sparse_ready_step);
criterion_main!(benches);
