//! Microbenchmarks for the single-owner host scheduler.
//!
//! These cases isolate host task admission and wake delivery. They are not
//! end-to-end I/O benchmarks: the foreign-thread cases include the persistent
//! worker's channel and OS scheduling costs as part of the measured boundary.

use std::future::Future;
use std::hint::black_box;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, mpsc};
use std::task::{Context, Poll, Waker};
use std::thread;
use std::time::Duration;

use criterion::{BatchSize, Criterion, Throughput, criterion_group, criterion_main};
use kr_runtime::{HostHandle, HostRuntime};

const SPAWNS_PER_RUN: usize = 256;
const WAKES_PER_RUN: usize = 256;
const PARK_SETTLE_TIME: Duration = Duration::from_micros(50);

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

async fn sequential_spawn_join(handle: HostHandle, count: usize) -> u64 {
    let mut checksum = 0_u64;
    for value in 0..black_box(count) {
        let task = handle
            .spawn(async move { work_value(value) })
            .expect("bounded host task spawn succeeds");
        checksum = checksum.wrapping_add(task.await.expect("host task completes"));
    }
    checksum
}

struct SameThreadWake {
    remaining: usize,
}

impl Future for SameThreadWake {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        if self.remaining == 0 {
            return Poll::Ready(());
        }
        self.remaining -= 1;
        context.waker().wake_by_ref();
        Poll::Pending
    }
}

enum WorkerMessage {
    Wake { waker: Waker, settle_time: Duration },
    Stop,
}

struct WakeWorker {
    sender: mpsc::Sender<WorkerMessage>,
    completed: Arc<AtomicUsize>,
    thread: Option<thread::JoinHandle<()>>,
}

impl WakeWorker {
    fn new() -> Self {
        let (sender, receiver) = mpsc::channel();
        let completed = Arc::new(AtomicUsize::new(0));
        let worker_completed = Arc::clone(&completed);
        let thread = thread::spawn(move || {
            while let Ok(message) = receiver.recv() {
                match message {
                    WorkerMessage::Wake { waker, settle_time } => {
                        if !settle_time.is_zero() {
                            thread::sleep(settle_time);
                        }
                        worker_completed.fetch_add(1, Ordering::Release);
                        waker.wake();
                    }
                    WorkerMessage::Stop => break,
                }
            }
        });
        Self {
            sender,
            completed,
            thread: Some(thread),
        }
    }

    fn workload(&self, target: usize, settle_time: Duration) -> ForeignWake {
        self.completed.store(0, Ordering::Release);
        ForeignWake {
            sender: self.sender.clone(),
            completed: Arc::clone(&self.completed),
            target,
            requested: 0,
            settle_time,
        }
    }
}

impl Drop for WakeWorker {
    fn drop(&mut self) {
        let _ = self.sender.send(WorkerMessage::Stop);
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

struct ForeignWake {
    sender: mpsc::Sender<WorkerMessage>,
    completed: Arc<AtomicUsize>,
    target: usize,
    requested: usize,
    settle_time: Duration,
}

impl Future for ForeignWake {
    type Output = usize;

    fn poll(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let completed = self.completed.load(Ordering::Acquire);
        if completed == self.target {
            return Poll::Ready(completed);
        }
        if self.requested == completed {
            self.requested += 1;
            self.sender
                .send(WorkerMessage::Wake {
                    waker: context.waker().clone(),
                    settle_time: self.settle_time,
                })
                .expect("foreign wake worker remains available");
        }
        Poll::Pending
    }
}

fn host_overhead(criterion: &mut Criterion) {
    let expected = expected_checksum(SPAWNS_PER_RUN);
    let mut group = criterion.benchmark_group("host_overhead/spawn_join_sequential");
    group.throughput(Throughput::Elements(SPAWNS_PER_RUN as u64));
    group.bench_function("host_runtime", |bencher| {
        bencher.iter_batched_ref(
            || {
                let runtime = HostRuntime::default();
                let future = sequential_spawn_join(runtime.handle(), SPAWNS_PER_RUN);
                (runtime, Some(future))
            },
            |(runtime, future)| {
                let checksum = runtime
                    .block_on(future.take().expect("fresh spawn workload"))
                    .expect("bounded spawn workload completes");
                assert_eq!(checksum, expected);
                black_box(checksum)
            },
            BatchSize::PerIteration,
        );
    });
    group.finish();

    let mut group = criterion.benchmark_group("host_overhead/same_thread_wake_poll");
    group.throughput(Throughput::Elements(WAKES_PER_RUN as u64));
    group.bench_function("host_runtime", |bencher| {
        bencher.iter_batched_ref(
            || {
                (
                    HostRuntime::default(),
                    Some(SameThreadWake {
                        remaining: WAKES_PER_RUN,
                    }),
                )
            },
            |(runtime, future)| {
                runtime
                    .block_on(future.take().expect("fresh same-thread wake workload"))
                    .expect("same-thread wake workload completes");
            },
            BatchSize::PerIteration,
        );
    });
    group.finish();

    let worker = WakeWorker::new();
    let mut group = criterion.benchmark_group("host_overhead/foreign_wake_poll");
    group.throughput(Throughput::Elements(WAKES_PER_RUN as u64));
    group.bench_function("host_runtime", |bencher| {
        bencher.iter_batched_ref(
            || {
                (
                    HostRuntime::default(),
                    Some(worker.workload(WAKES_PER_RUN, Duration::ZERO)),
                )
            },
            |(runtime, future)| {
                let completed = runtime
                    .block_on(future.take().expect("fresh foreign wake workload"))
                    .expect("foreign wake workload completes");
                assert_eq!(completed, WAKES_PER_RUN);
                black_box(completed)
            },
            BatchSize::PerIteration,
        );
    });
    group.finish();

    // The fixed delay gives the owner time to reach `park`. It is intentionally
    // included in the result, so this case is a host round-trip canary rather
    // than an estimate of the `park`/`unpark` instructions in isolation.
    let mut group = criterion.benchmark_group("host_overhead/park_unpark_round_trip");
    group.throughput(Throughput::Elements(1));
    group.bench_function("host_runtime", |bencher| {
        bencher.iter_batched_ref(
            || {
                (
                    HostRuntime::default(),
                    Some(worker.workload(1, PARK_SETTLE_TIME)),
                )
            },
            |(runtime, future)| {
                let completed = runtime
                    .block_on(future.take().expect("fresh park workload"))
                    .expect("park/unpark workload completes");
                assert_eq!(completed, 1);
                black_box(completed)
            },
            BatchSize::PerIteration,
        );
    });
    group.finish();
}

criterion_group!(benches, host_overhead);
criterion_main!(benches);
