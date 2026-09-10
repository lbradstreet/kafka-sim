use std::hint::black_box;
use std::num::NonZeroU64;
use std::rc::Rc;

use criterion::measurement::WallTime;
use criterion::{
    BatchSize, BenchmarkGroup, Criterion, Throughput, criterion_group, criterion_main,
};
use kr_runtime::trace::sbe::{SbeRecordingTrace, SbeTraceRetention};
use kr_runtime::trace::{RecordingTrace, SamplingTrace, TraceRetention};
use kr_runtime::{RuntimeConfig, SimDuration, SimRuntime, yield_now};

const YIELDS_PER_RUN: usize = 256;
const TIMERS_PER_RUN: usize = 256;
const RANDOM_DRAWS_PER_RUN: usize = 1_024;

// Every event emitted by these three workloads fits in at most 64 framed SBE
// bytes. These capacities therefore approximate the existing event-count
// policies: 4,096 prefix events, 256 tail events, or 128 prefix + 128 tail.
const SBE_PREFIX_CAPACITY_BYTES: usize = 4_096 * 64;
const SBE_TAIL_CAPACITY_BYTES: usize = 256 * 64;
const SBE_SPLIT_CAPACITY_BYTES: usize = 128 * 64;

fn run_yield_workload(mut runtime: SimRuntime) {
    runtime
        .block_on(async move {
            for _ in 0..black_box(YIELDS_PER_RUN) {
                yield_now().await;
            }
        })
        .expect("bounded yield workload completes");
}

fn run_timer_workload(mut runtime: SimRuntime) {
    let handle = runtime.handle();
    runtime
        .block_on(async move {
            for _ in 0..black_box(TIMERS_PER_RUN) {
                handle
                    .sleep(SimDuration::from_nanos(1))
                    .await
                    .expect("bounded timer workload completes");
            }
        })
        .expect("bounded timer workload completes");
}

fn run_random_workload(mut runtime: SimRuntime) {
    let handle = runtime.handle();
    runtime
        .block_on(async move {
            for _ in 0..black_box(RANDOM_DRAWS_PER_RUN) {
                black_box(handle.random_u64().expect("runtime is active"));
            }
        })
        .expect("bounded random workload completes");
}

fn bench_runtime_modes(group: &mut BenchmarkGroup<'_, WallTime>, workload: fn(SimRuntime)) {
    // Per-iteration setup is outside the timed routine, so this compares
    // executor work and event handling rather than recorder allocation.
    group.bench_function("disabled", |b| {
        b.iter_batched(
            || SimRuntime::new(RuntimeConfig::default()),
            workload,
            BatchSize::PerIteration,
        );
    });
    group.bench_function("sampling_every_64", |b| {
        b.iter_batched(
            || {
                SimRuntime::with_trace(
                    RuntimeConfig::default(),
                    Rc::new(SamplingTrace::new(
                        Rc::new(RecordingTrace::new(4_096)),
                        NonZeroU64::new(64).expect("sampling period is nonzero"),
                    )),
                )
            },
            workload,
            BatchSize::PerIteration,
        );
    });
    group.bench_function("recording_capacity_0", |b| {
        b.iter_batched(
            || SimRuntime::with_trace(RuntimeConfig::default(), Rc::new(RecordingTrace::new(0))),
            workload,
            BatchSize::PerIteration,
        );
    });
    group.bench_function("recording_capacity_4096", |b| {
        b.iter_batched(
            || {
                SimRuntime::with_trace(
                    RuntimeConfig::default(),
                    Rc::new(RecordingTrace::new(4_096)),
                )
            },
            workload,
            BatchSize::PerIteration,
        );
    });
    group.bench_function("recording_tail_capacity_256", |b| {
        b.iter_batched(
            || {
                SimRuntime::with_trace(
                    RuntimeConfig::default(),
                    Rc::new(RecordingTrace::with_retention(TraceRetention::Tail {
                        capacity: 256,
                    })),
                )
            },
            workload,
            BatchSize::PerIteration,
        );
    });
    group.bench_function("recording_prefix_128_tail_128", |b| {
        b.iter_batched(
            || {
                SimRuntime::with_trace(
                    RuntimeConfig::default(),
                    Rc::new(RecordingTrace::with_retention(
                        TraceRetention::PrefixAndTail {
                            prefix_capacity: 128,
                            tail_capacity: 128,
                        },
                    )),
                )
            },
            workload,
            BatchSize::PerIteration,
        );
    });
    group.bench_function("sbe_sampling_every_64", |b| {
        b.iter_batched(
            || {
                SimRuntime::with_trace(
                    RuntimeConfig::default(),
                    Rc::new(SamplingTrace::new(
                        Rc::new(SbeRecordingTrace::new(SBE_PREFIX_CAPACITY_BYTES)),
                        NonZeroU64::new(64).expect("sampling period is nonzero"),
                    )),
                )
            },
            workload,
            BatchSize::PerIteration,
        );
    });
    group.bench_function("sbe_recording_capacity_bytes_0", |b| {
        b.iter_batched(
            || SimRuntime::with_trace(RuntimeConfig::default(), Rc::new(SbeRecordingTrace::new(0))),
            workload,
            BatchSize::PerIteration,
        );
    });
    group.bench_function("sbe_recording_capacity_bytes_262144", |b| {
        b.iter_batched(
            || {
                SimRuntime::with_trace(
                    RuntimeConfig::default(),
                    Rc::new(SbeRecordingTrace::new(SBE_PREFIX_CAPACITY_BYTES)),
                )
            },
            workload,
            BatchSize::PerIteration,
        );
    });
    group.bench_function("sbe_recording_tail_capacity_bytes_16384", |b| {
        b.iter_batched(
            || {
                SimRuntime::with_trace(
                    RuntimeConfig::default(),
                    Rc::new(SbeRecordingTrace::with_retention(SbeTraceRetention::Tail {
                        capacity_bytes: SBE_TAIL_CAPACITY_BYTES,
                    })),
                )
            },
            workload,
            BatchSize::PerIteration,
        );
    });
    group.bench_function("sbe_recording_prefix_bytes_8192_tail_bytes_8192", |b| {
        b.iter_batched(
            || {
                SimRuntime::with_trace(
                    RuntimeConfig::default(),
                    Rc::new(SbeRecordingTrace::with_retention(
                        SbeTraceRetention::PrefixAndTail {
                            prefix_capacity_bytes: SBE_SPLIT_CAPACITY_BYTES,
                            tail_capacity_bytes: SBE_SPLIT_CAPACITY_BYTES,
                        },
                    )),
                )
            },
            workload,
            BatchSize::PerIteration,
        );
    });
}

fn runtime_trace(c: &mut Criterion) {
    let mut group = c.benchmark_group("runtime_trace_yield_execution");
    group.throughput(Throughput::Elements(YIELDS_PER_RUN as u64));
    bench_runtime_modes(&mut group, run_yield_workload);
    group.finish();

    let mut group = c.benchmark_group("runtime_trace_timer_execution");
    group.throughput(Throughput::Elements(TIMERS_PER_RUN as u64));
    bench_runtime_modes(&mut group, run_timer_workload);
    group.finish();

    let mut group = c.benchmark_group("runtime_trace_random_execution");
    group.throughput(Throughput::Elements(RANDOM_DRAWS_PER_RUN as u64));
    bench_runtime_modes(&mut group, run_random_workload);
    group.finish();
}

criterion_group!(benches, runtime_trace);
criterion_main!(benches);
