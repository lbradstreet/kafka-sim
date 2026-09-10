//! Microbenchmarks for the `ColdFile` façade over the warm file boundary.
//!
//! These measurements answer one question: what does the cold façade add to
//! an operation that would otherwise use the eager `FileIoSubmit` handle directly?
//! They are matched pairs — the same provider, payload, and drive loop with
//! only the façade differing — and are valid for relative comparison on a
//! developer machine. They are not I/O throughput results and say nothing
//! about io_uring; the production overhead question stays in the
//! `kr-runtime-io-uring` `file_overhead` suite.

use std::future::Future;
use std::hint::black_box;
use std::pin::pin;
use std::task::{Context, Poll, Waker};

use criterion::{BatchSize, Criterion, criterion_group, criterion_main};
use kr_runtime::{RuntimeConfig, SimRuntime};
use kr_runtime_io::storage::{
    ColdFile, FileIoSubmit, MemoryFile, MemoryFileConfig, ReadAtRequest, SimDisk, SimStorage,
    SimStorageConfig, WriteAtRequest,
};

const PAYLOAD_BYTES: usize = 64;

/// Polls an operation expected to complete on its first poll.
fn drive_ready<F: Future>(future: F) -> F::Output {
    let mut future = pin!(future);
    let mut context = Context::from_waker(Waker::noop());
    match future.as_mut().poll(&mut context) {
        Poll::Ready(output) => output,
        Poll::Pending => panic!("memory-backed operation must complete on first poll"),
    }
}

fn memory_file() -> MemoryFile {
    MemoryFile::from_durable_bytes(MemoryFileConfig::default(), vec![0; PAYLOAD_BYTES])
        .expect("memory file config is valid")
}

fn sim_fixture() -> (SimRuntime, SimStorage) {
    let runtime = SimRuntime::new(RuntimeConfig::default());
    let storage = SimDisk::default()
        .open(runtime.handle(), SimStorageConfig::default())
        .expect("open simulated disk");
    (runtime, storage)
}

fn report_future_sizes() {
    let memory = memory_file();
    let cold_memory = ColdFile::new(memory.clone());
    let (_runtime, storage) = sim_fixture();
    let cold_sim = ColdFile::new(storage.clone());
    // Constructing (and dropping unpolled) admits nothing on the cold side;
    // the warm sim future is dropped as an abandoned admitted response.
    eprintln!(
        "future sizes (bytes): memory warm write {}, memory cold write {}, \
         sim warm write {}, sim cold write {}",
        size_of_val(&memory.submit_write_at(WriteAtRequest::new(0, Vec::new()))),
        size_of_val(&cold_memory.write_at(WriteAtRequest::new(0, Vec::new()))),
        size_of_val(&storage.submit_write_at(WriteAtRequest::new(0, Vec::new()))),
        size_of_val(&cold_sim.write_at(WriteAtRequest::new(0, Vec::new()))),
    );
}

fn cold_file_overhead(criterion: &mut Criterion) {
    report_future_sizes();

    // Immediately ready completion: construct one write and drive it to
    // terminal with a noop waker. The memory provider completes at admission,
    // so the pair isolates the façade's clone + wrapper-poll cost.
    let mut group = criterion.benchmark_group("cold_file_overhead/memory_ready_write");
    let file = memory_file();
    group.bench_function("warm", |bencher| {
        bencher.iter_batched(
            || vec![0xa5; PAYLOAD_BYTES],
            |payload| drive_ready(file.submit_write_at(WriteAtRequest::new(0, payload))),
            BatchSize::SmallInput,
        );
    });
    let cold = ColdFile::new(file.clone());
    group.bench_function("cold", |bencher| {
        bencher.iter_batched(
            || vec![0xa5; PAYLOAD_BYTES],
            |payload| drive_ready(cold.write_at(WriteAtRequest::new(0, payload))),
            BatchSize::SmallInput,
        );
    });
    group.finish();

    let mut group = criterion.benchmark_group("cold_file_overhead/memory_ready_read");
    let file = memory_file();
    group.bench_function("warm", |bencher| {
        bencher.iter_batched(
            || vec![0; PAYLOAD_BYTES],
            |buffer| drive_ready(file.submit_read_at(ReadAtRequest::new(0, buffer))),
            BatchSize::SmallInput,
        );
    });
    let cold = ColdFile::new(file.clone());
    group.bench_function("cold", |bencher| {
        bencher.iter_batched(
            || vec![0; PAYLOAD_BYTES],
            |buffer| drive_ready(cold.read_at(ReadAtRequest::new(0, buffer))),
            BatchSize::SmallInput,
        );
    });
    group.finish();

    // Cold construction and unpolled drop. There is no matched warm case:
    // dropping a warm future abandons an already-applied effect, which is a
    // different operation. `request_only` is the allocation floor.
    let mut group = criterion.benchmark_group("cold_file_overhead/construct_drop_unpolled");
    group.bench_function("request_only", |bencher| {
        bencher.iter_batched(
            || vec![0xa5; PAYLOAD_BYTES],
            |payload| black_box(WriteAtRequest::new(0, payload)),
            BatchSize::SmallInput,
        );
    });
    let file = memory_file();
    let cold = ColdFile::new(file);
    group.bench_function("cold", |bencher| {
        bencher.iter_batched(
            || vec![0xa5; PAYLOAD_BYTES],
            |payload| {
                let future = cold.write_at(WriteAtRequest::new(0, payload));
                black_box(&future);
            },
            BatchSize::SmallInput,
        );
    });
    group.finish();

    // Pending completion through the simulated provider: admission, worker
    // execution, and completion wake all run inside the timed region on both
    // sides, so the pair isolates the façade over a genuinely suspending
    // backend. Payload allocation is inside the timed region on both sides.
    let mut group = criterion.benchmark_group("cold_file_overhead/sim_pending_write");
    let (mut runtime, storage) = sim_fixture();
    group.bench_function("warm", |bencher| {
        bencher.iter(|| {
            let payload = vec![0xa5; PAYLOAD_BYTES];
            runtime
                .block_on(storage.submit_write_at(WriteAtRequest::new(0, payload)))
                .expect("runtime completes")
                .expect("simulated write succeeds")
        });
    });
    let cold = ColdFile::new(storage.clone());
    group.bench_function("cold", |bencher| {
        bencher.iter(|| {
            let payload = vec![0xa5; PAYLOAD_BYTES];
            runtime
                .block_on(cold.write_at(WriteAtRequest::new(0, payload)))
                .expect("runtime completes")
                .expect("simulated write succeeds")
        });
    });
    group.finish();
}

criterion_group!(benches, cold_file_overhead);
criterion_main!(benches);
