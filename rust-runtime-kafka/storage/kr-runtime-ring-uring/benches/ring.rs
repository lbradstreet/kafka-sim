//! Comparable API-level microbenchmarks for the simulated and Linux file rings.
//!
//! `file_sim_zero_latency` measures the real file-ring state machine over the
//! deterministic storage provider with zero virtual latency. Its sync still
//! snapshots the entire configured simulated file, so it is a CPU/runtime
//! baseline rather than a model of disk latency. `file_uring_buffered` measures
//! buffered kernel I/O and real durability fences on the selected filesystem.
//!
//! `file_uring_odsync` and `file_pooled_odsync` open the same backing file
//! with `O_DSYNC`, so every frame write is a durability write-through with
//! real device latency instead of a page-cache copy. They measure the
//! per-write durability cost that buffered mode hides. Note what they do
//! not measure: concurrent same-file `O_DSYNC` writes serialize on the
//! inode write lock on ext4 and XFS, so an append plan's pipelined frames
//! still complete at one write-through per write time — measured costs stay
//! linear in frame count whether or not the layers above submit them
//! concurrently. Overlapping durable writes to one file needs the shared
//! inode locking of aligned `O_DIRECT` writes, and `O_DIRECT` is
//! deliberately not offered: the ring packs frames at unaligned offsets and
//! lengths, and the owned `Vec<u8>` request buffers carry no memory
//! alignment, so direct I/O would reject every frame write with `EINVAL`.
//! The write-through backends skip the read group because reads do not pass
//! through `O_DSYNC`.

use std::hint::black_box;
use std::mem;
use std::time::{Duration, Instant};

use criterion::measurement::WallTime;
use criterion::{
    BatchSize, BenchmarkGroup, BenchmarkId, Criterion, SamplingMode, Throughput, criterion_group,
    criterion_main,
};
use kr_runtime_ring::file::FileRingConfig;
use kr_runtime_ring::{
    AppendSuccess, ReadPage, ReadRequest, RingCursor, RingLimits, SyncSuccess, TrimSuccess,
};

#[cfg(target_os = "linux")]
#[path = "ring/pooled.rs"]
mod pooled;
#[path = "ring/sim.rs"]
mod sim;
#[cfg(target_os = "linux")]
#[path = "ring/uring.rs"]
mod uring;

#[cfg(target_os = "linux")]
use pooled::PooledBenchRing;
use sim::SimBenchRing;
#[cfg(target_os = "linux")]
use uring::{UringBenchRing, WriteThroughBenchRing};

const SIM_BACKEND: &str = "file_sim_zero_latency";
#[cfg(target_os = "linux")]
const URING_BACKEND: &str = "file_uring_buffered";
#[cfg(target_os = "linux")]
const POOLED_BACKEND: &str = "file_pooled_buffered";
#[cfg(target_os = "linux")]
const URING_ODSYNC_BACKEND: &str = "file_uring_odsync";
#[cfg(target_os = "linux")]
const POOLED_ODSYNC_BACKEND: &str = "file_pooled_odsync";

const DATA_CAPACITY_BYTES: u64 = 4 * 1_024 * 1_024;
const MAX_LIVE_RECORDS: usize = 4_096;
const MAX_LIVE_PAYLOAD_BYTES: usize = 2 * 1_024 * 1_024;
const MAX_RECORD_BYTES: usize = 16 * 1_024;
const MAX_BATCH_RECORDS: usize = 64;
const MAX_BATCH_BYTES: usize = 1_024 * 1_024;
const MAX_IO_REQUEST_BYTES: usize = 64 * 1_024;
const COMMAND_QUEUE_CAPACITY: usize = 64;
const MAX_MAINTENANCE_BURST: u64 = 64;

const SAMPLE_SIZE: usize = 20;
const WARM_UP_TIME: Duration = Duration::from_secs(1);
const MEASUREMENT_TIME: Duration = Duration::from_secs(3);

#[derive(Clone, Copy)]
struct Scenario {
    id: &'static str,
    records: usize,
    bytes_per_record: usize,
}

impl Scenario {
    fn payload_bytes(self) -> usize {
        self.records
            .checked_mul(self.bytes_per_record)
            .expect("benchmark scenario payload fits usize")
    }

    fn buffers(self) -> Vec<Vec<u8>> {
        (0..self.records)
            .map(|index| vec![(index as u8).wrapping_mul(31); self.bytes_per_record])
            .collect()
    }

    fn maintenance_window(self) -> u64 {
        let by_records = MAX_LIVE_RECORDS / self.records;
        let by_payload = MAX_LIVE_PAYLOAD_BYTES / self.payload_bytes();
        u64::try_from(by_records.min(by_payload))
            .expect("bounded maintenance window fits u64")
            .clamp(1, MAX_MAINTENANCE_BURST)
    }
}

const SCENARIOS: [Scenario; 4] = [
    Scenario {
        id: "r1-p64",
        records: 1,
        bytes_per_record: 64,
    },
    Scenario {
        id: "r1-p4096",
        records: 1,
        bytes_per_record: 4_096,
    },
    Scenario {
        id: "r16-p1024",
        records: 16,
        bytes_per_record: 1_024,
    },
    Scenario {
        id: "r64-p16384",
        records: 64,
        bytes_per_record: 16 * 1_024,
    },
];

trait BenchRing {
    fn append(&mut self, records: Vec<Vec<u8>>) -> AppendSuccess;
    fn sync(&mut self) -> SyncSuccess;
    fn trim(&mut self, before: RingCursor) -> TrimSuccess;
    fn read(&mut self, request: ReadRequest) -> ReadPage;
}

fn file_ring_config() -> FileRingConfig {
    FileRingConfig {
        limits: RingLimits {
            max_record_bytes: MAX_RECORD_BYTES,
            max_live_records: MAX_LIVE_RECORDS,
            max_live_payload_bytes: MAX_LIVE_PAYLOAD_BYTES,
            max_read_records: MAX_BATCH_RECORDS,
            max_read_bytes: MAX_BATCH_BYTES,
            max_batch_records: MAX_BATCH_RECORDS,
            max_batch_bytes: MAX_BATCH_BYTES,
        },
        data_capacity_bytes: DATA_CAPACITY_BYTES,
        max_io_request_bytes: MAX_IO_REQUEST_BYTES,
        command_queue_capacity: COMMAND_QUEUE_CAPACITY,
    }
}

#[cfg(target_os = "linux")]
fn uring_config() -> kr_runtime_ring_uring::UringRingConfig {
    let file = file_ring_config();
    kr_runtime_ring_uring::UringRingConfig {
        limits: file.limits,
        data_capacity_bytes: file.data_capacity_bytes,
        max_io_request_bytes: file.max_io_request_bytes,
        command_queue_capacity: file.command_queue_capacity,
        ring_entries: 8,
        max_io_chunk_bytes: file.max_io_request_bytes,
        startup_timeout: Duration::from_secs(30),
        shutdown_timeout: Duration::from_secs(30),
    }
}

fn configure(group: &mut BenchmarkGroup<'_, WallTime>) {
    group
        .sample_size(SAMPLE_SIZE)
        .warm_up_time(WARM_UP_TIME)
        .measurement_time(MEASUREMENT_TIME)
        .sampling_mode(SamplingMode::Flat);
}

fn register_accepted_append<B, F>(
    group: &mut BenchmarkGroup<'_, WallTime>,
    backend_name: &'static str,
    factory: F,
) where
    B: BenchRing,
    F: Copy + Fn() -> B,
{
    for scenario in SCENARIOS {
        let mut fixture = None;
        let maintenance_window = scenario.maintenance_window();
        group.throughput(Throughput::Bytes(scenario.payload_bytes() as u64));
        group.bench_function(
            BenchmarkId::new(backend_name, scenario.id),
            move |bencher| {
                let (ring, buffers) =
                    fixture.get_or_insert_with(|| (factory(), scenario.buffers()));
                bencher.iter_custom(|iterations| {
                    let mut measured = Duration::ZERO;
                    let mut remaining = iterations;
                    while remaining != 0 {
                        let burst = remaining.min(maintenance_window);
                        let mut tail = RingCursor::START;
                        let started = Instant::now();
                        for _ in 0..burst {
                            let appended = ring.append(mem::take(buffers));
                            tail = appended.next_cursor;
                            *buffers = appended.records;
                        }
                        measured += started.elapsed();
                        black_box(tail);
                        checkpoint_trim_reclaim(ring, tail);
                        remaining -= burst;
                    }
                    measured
                });
            },
        );
    }
}

fn register_dirty_sync<B, F>(
    group: &mut BenchmarkGroup<'_, WallTime>,
    backend_name: &'static str,
    factory: F,
) where
    B: BenchRing,
    F: Copy + Fn() -> B,
{
    for scenario in SCENARIOS {
        let mut fixture = None;
        let maintenance_window = scenario.maintenance_window();
        group.throughput(Throughput::Bytes(scenario.payload_bytes() as u64));
        group.bench_function(
            BenchmarkId::new(backend_name, scenario.id),
            move |bencher| {
                let (ring, buffers) =
                    fixture.get_or_insert_with(|| (factory(), scenario.buffers()));
                bencher.iter_custom(|iterations| {
                    let mut measured = Duration::ZERO;
                    let mut remaining = iterations;
                    while remaining != 0 {
                        let burst = remaining.min(maintenance_window);
                        let mut tail = RingCursor::START;
                        for _ in 0..burst {
                            let appended = ring.append(mem::take(buffers));
                            tail = appended.next_cursor;
                            *buffers = appended.records;

                            let started = Instant::now();
                            let checkpoint = ring.sync();
                            measured += started.elapsed();
                            black_box(checkpoint);
                        }
                        trim_reclaim(ring, tail);
                        remaining -= burst;
                    }
                    measured
                });
            },
        );
    }
}

fn register_hot_read<B, F>(
    group: &mut BenchmarkGroup<'_, WallTime>,
    backend_name: &'static str,
    factory: F,
) where
    B: BenchRing,
    F: Copy + Fn() -> B,
{
    for scenario in SCENARIOS {
        let mut fixture = None;
        group.throughput(Throughput::Bytes(scenario.payload_bytes() as u64));
        group.bench_function(
            BenchmarkId::new(backend_name, scenario.id),
            move |bencher| {
                let (ring, request) = fixture.get_or_insert_with(|| {
                    let mut ring = factory();
                    let appended = ring.append(scenario.buffers());
                    assert_eq!(appended.next_cursor.get(), scenario.records as u64);
                    let checkpoint = ring.sync();
                    assert_eq!(checkpoint.durable_tail, appended.next_cursor);
                    let request = ReadRequest::new(
                        RingCursor::START,
                        scenario.records,
                        scenario.payload_bytes(),
                    );
                    (ring, request)
                });
                let request = *request;
                bencher.iter_batched(|| (), |_| ring.read(request), BatchSize::PerIteration);
            },
        );
    }
}

fn register_lifecycle<B, F>(
    group: &mut BenchmarkGroup<'_, WallTime>,
    backend_name: &'static str,
    factory: F,
) where
    B: BenchRing,
    F: Copy + Fn() -> B,
{
    for scenario in SCENARIOS {
        let mut fixture = None;
        group.throughput(Throughput::Bytes(scenario.payload_bytes() as u64));
        group.bench_function(
            BenchmarkId::new(backend_name, scenario.id),
            move |bencher| {
                let (ring, buffers) =
                    fixture.get_or_insert_with(|| (factory(), scenario.buffers()));
                bencher.iter(|| {
                    let appended = ring.append(mem::take(buffers));
                    let tail = appended.next_cursor;
                    *buffers = appended.records;
                    let first_checkpoint = ring.sync();
                    let trimmed = ring.trim(tail);
                    let second_checkpoint = ring.sync();
                    black_box((first_checkpoint, trimmed, second_checkpoint));
                });
            },
        );
    }
}

fn checkpoint_trim_reclaim<B: BenchRing>(ring: &mut B, tail: RingCursor) {
    let checkpoint = ring.sync();
    assert_eq!(checkpoint.durable_tail, tail);
    trim_reclaim(ring, tail);
}

fn trim_reclaim<B: BenchRing>(ring: &mut B, tail: RingCursor) {
    let trimmed = ring.trim(tail);
    assert_eq!(trimmed.accepted_head, tail);
    let checkpoint = ring.sync();
    assert_eq!(checkpoint.durable_head, tail);
    assert_eq!(checkpoint.durable_tail, tail);
}

fn ring_benchmarks(criterion: &mut Criterion) {
    let mut append = criterion.benchmark_group("ring/append_accepted_maintenance_excluded");
    configure(&mut append);
    register_accepted_append(&mut append, SIM_BACKEND, SimBenchRing::new);
    #[cfg(target_os = "linux")]
    register_accepted_append(&mut append, URING_BACKEND, UringBenchRing::new);
    #[cfg(target_os = "linux")]
    register_accepted_append(&mut append, POOLED_BACKEND, PooledBenchRing::new);
    #[cfg(target_os = "linux")]
    register_accepted_append(
        &mut append,
        URING_ODSYNC_BACKEND,
        WriteThroughBenchRing::new,
    );
    #[cfg(target_os = "linux")]
    register_accepted_append(
        &mut append,
        POOLED_ODSYNC_BACKEND,
        PooledBenchRing::new_write_through,
    );
    append.finish();

    let mut sync = criterion.benchmark_group("ring/sync_dirty_append_excluded");
    configure(&mut sync);
    register_dirty_sync(&mut sync, SIM_BACKEND, SimBenchRing::new);
    #[cfg(target_os = "linux")]
    register_dirty_sync(&mut sync, URING_BACKEND, UringBenchRing::new);
    #[cfg(target_os = "linux")]
    register_dirty_sync(&mut sync, POOLED_BACKEND, PooledBenchRing::new);
    #[cfg(target_os = "linux")]
    register_dirty_sync(&mut sync, URING_ODSYNC_BACKEND, WriteThroughBenchRing::new);
    #[cfg(target_os = "linux")]
    register_dirty_sync(
        &mut sync,
        POOLED_ODSYNC_BACKEND,
        PooledBenchRing::new_write_through,
    );
    sync.finish();

    let mut read = criterion.benchmark_group("ring/read_durable_fixed_hot_page");
    configure(&mut read);
    register_hot_read(&mut read, SIM_BACKEND, SimBenchRing::new);
    #[cfg(target_os = "linux")]
    register_hot_read(&mut read, URING_BACKEND, UringBenchRing::new);
    #[cfg(target_os = "linux")]
    register_hot_read(&mut read, POOLED_BACKEND, PooledBenchRing::new);
    read.finish();

    let mut lifecycle = criterion.benchmark_group("ring/append_sync_trim_sync");
    configure(&mut lifecycle);
    register_lifecycle(&mut lifecycle, SIM_BACKEND, SimBenchRing::new);
    #[cfg(target_os = "linux")]
    register_lifecycle(&mut lifecycle, URING_BACKEND, UringBenchRing::new);
    #[cfg(target_os = "linux")]
    register_lifecycle(&mut lifecycle, POOLED_BACKEND, PooledBenchRing::new);
    #[cfg(target_os = "linux")]
    register_lifecycle(
        &mut lifecycle,
        URING_ODSYNC_BACKEND,
        WriteThroughBenchRing::new,
    );
    #[cfg(target_os = "linux")]
    register_lifecycle(
        &mut lifecycle,
        POOLED_ODSYNC_BACKEND,
        PooledBenchRing::new_write_through,
    );
    lifecycle.finish();
}

criterion_group!(benches, ring_benchmarks);
criterion_main!(benches);
