//! Multi-inflight buffered-file throughput cases.
//!
//! Every case submits `INFLIGHT` operations before awaiting any of them, so
//! both backends see the whole batch at once. Two shapes are measured, reads
//! and writes separately, hot-cache and unfenced:
//!
//! - `one_file_qd8`: eight operations on one file at disjoint offsets. Both
//!   backends may keep the whole commuting batch in flight at once — the
//!   actor as one published SQE batch, the pool through its per-file
//!   pipeline and reorder buffer.
//! - `eight_files_qd1`: one operation on each of eight files. The actor
//!   provider spends two threads and one ring per file; the pool multiplexes
//!   all eight onto one ring, so this shape is the pool's intended case.
//!
//! Both backends use the same ring depth, chunk limit, payload, and file
//! fixtures. Setup, backend construction, and validation stay outside
//! measured time.

use std::fs;
use std::future::Future;
use std::path::{Path, PathBuf};
use std::pin::pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::task::{Context, Poll, Waker};
use std::thread::{self, Thread};
use std::time::{Duration, Instant};

use criterion::measurement::WallTime;
use criterion::{BenchmarkGroup, BenchmarkId, Criterion, SamplingMode, Throughput};
use kr_runtime_io::{FileIoSubmit, ReadAtRequest, WriteAtRequest};
use kr_runtime_io_uring::{
    PooledUringFile, UringFile, UringFileConfig, UringIoPool, UringPoolConfig,
};

const BENCH_DIRECTORY_ENV: &str = "KR_RUNTIME_IO_URING_BENCH_DIR";
const RING_ENTRIES: u32 = 8;
const INFLIGHT: usize = 8;
const MAX_IO_CHUNK_BYTES: usize = 256 * 1_024;
const MAX_FILE_BYTES: u64 = 8 * 1_024 * 1_024;

const SAMPLE_SIZE: usize = 20;
const WARM_UP_TIME: Duration = Duration::from_secs(1);
const MEASUREMENT_TIME: Duration = Duration::from_secs(3);

const SIZES: [usize; 2] = [4 * 1_024, 64 * 1_024];

static NEXT_DIRECTORY: AtomicU64 = AtomicU64::new(0);

struct ThreadWake(Thread);

impl std::task::Wake for ThreadWake {
    fn wake(self: Arc<Self>) {
        self.0.unpark();
    }
}

fn block_on<T>(waker: &Waker, future: impl Future<Output = T>) -> T {
    let mut future = pin!(future);
    let mut context = Context::from_waker(waker);
    loop {
        match future.as_mut().poll(&mut context) {
            Poll::Ready(output) => return output,
            Poll::Pending => thread::park(),
        }
    }
}

fn payload(size: usize) -> Vec<u8> {
    (0..size).map(|index| (index % 251) as u8).collect()
}

fn file_config() -> UringFileConfig {
    UringFileConfig {
        max_read_bytes: MAX_IO_CHUNK_BYTES,
        max_write_bytes: MAX_IO_CHUNK_BYTES,
        max_file_bytes: MAX_FILE_BYTES,
        command_queue_capacity: 64,
        ring_entries: RING_ENTRIES,
        max_io_chunk_bytes: MAX_IO_CHUNK_BYTES,
    }
}

fn pool_config() -> UringPoolConfig {
    UringPoolConfig {
        max_read_bytes: MAX_IO_CHUNK_BYTES,
        max_write_bytes: MAX_IO_CHUNK_BYTES,
        max_file_bytes: MAX_FILE_BYTES,
        file_queue_capacity: 64,
        ring_entries: RING_ENTRIES,
        max_in_flight: INFLIGHT,
        blocking_threads: 1,
        max_io_chunk_bytes: MAX_IO_CHUNK_BYTES,
    }
}

/// One benchmark unit: a handle to submit on plus the offset it targets.
struct Lane<H> {
    handle: H,
    offset: u64,
}

/// Keeps whatever owns the handles (the pool) alive through the measurement.
struct Fixture<H> {
    lanes: Vec<Lane<H>>,
    _holder: Option<UringIoPool>,
}

fn open_backing_file(path: &Path, length: usize) -> fs::File {
    let expected = payload(length);
    fs::write(path, &expected).expect("prepare benchmark file");
    fs::File::options()
        .read(true)
        .write(true)
        .open(path)
        .expect("open benchmark file")
}

/// Eight lanes on one file at disjoint offsets.
fn uring_one_file(directory: &Path, size: usize) -> Fixture<UringFile> {
    let path = directory.join("one.bin");
    drop(open_backing_file(&path, size * INFLIGHT));
    let file = UringFile::open_with_outcome(&path, file_config())
        .expect("open UringFile")
        .into_parts()
        .0;
    let lanes = (0..INFLIGHT)
        .map(|index| Lane {
            handle: file.clone(),
            offset: (index * size) as u64,
        })
        .collect();
    Fixture {
        lanes,
        _holder: None,
    }
}

fn pooled_one_file(directory: &Path, size: usize) -> Fixture<PooledUringFile> {
    let path = directory.join("one.bin");
    let backing = open_backing_file(&path, size * INFLIGHT);
    let pool = UringIoPool::new(pool_config()).expect("create pool");
    let file = pool.register_file(backing).expect("register pooled file");
    let lanes = (0..INFLIGHT)
        .map(|index| Lane {
            handle: file.clone(),
            offset: (index * size) as u64,
        })
        .collect();
    Fixture {
        lanes,
        _holder: Some(pool),
    }
}

/// One lane on each of eight files.
fn uring_eight_files(directory: &Path, size: usize) -> Fixture<UringFile> {
    let lanes = (0..INFLIGHT)
        .map(|index| {
            let path = directory.join(format!("file-{index}.bin"));
            drop(open_backing_file(&path, size));
            let file = UringFile::open_with_outcome(&path, file_config())
                .expect("open UringFile")
                .into_parts()
                .0;
            Lane {
                handle: file,
                offset: 0,
            }
        })
        .collect();
    Fixture {
        lanes,
        _holder: None,
    }
}

fn pooled_eight_files(directory: &Path, size: usize) -> Fixture<PooledUringFile> {
    let pool = UringIoPool::new(pool_config()).expect("create pool");
    let lanes = (0..INFLIGHT)
        .map(|index| {
            let path = directory.join(format!("file-{index}.bin"));
            let backing = open_backing_file(&path, size);
            let file = pool.register_file(backing).expect("register pooled file");
            Lane {
                handle: file,
                offset: 0,
            }
        })
        .collect();
    Fixture {
        lanes,
        _holder: Some(pool),
    }
}

/// Submits one read per lane, then awaits them in submission order.
fn batch_read<H: FileIoSubmit>(
    waker: &Waker,
    lanes: &[Lane<H>],
    buffers: &mut Vec<Vec<u8>>,
    size: usize,
) -> u64 {
    let mut responses = Vec::with_capacity(lanes.len());
    for lane in lanes {
        let mut buffer = buffers.pop().expect("read buffer is available");
        buffer.resize(size, 0);
        responses.push(
            lane.handle
                .submit_read_at(ReadAtRequest::new(lane.offset, buffer)),
        );
    }
    let mut transferred = 0_u64;
    for response in responses {
        let success = block_on(waker, response).expect("benchmark read");
        transferred += u64::try_from(success.bytes_read).expect("read length fits counter");
        buffers.push(success.buffer);
    }
    transferred
}

/// Submits one write per lane, then awaits them in submission order.
fn batch_write<H: FileIoSubmit>(
    waker: &Waker,
    lanes: &[Lane<H>],
    buffers: &mut Vec<Vec<u8>>,
    size: usize,
) -> u64 {
    let mut responses = Vec::with_capacity(lanes.len());
    for lane in lanes {
        let mut buffer = buffers.pop().expect("write buffer is available");
        buffer.resize(size, 7);
        responses.push(
            lane.handle
                .submit_write_at(WriteAtRequest::new(lane.offset, buffer)),
        );
    }
    let mut transferred = 0_u64;
    for response in responses {
        let success = block_on(waker, response).expect("benchmark write");
        transferred += u64::try_from(success.bytes_written).expect("write length fits counter");
        buffers.push(success.buffer);
    }
    transferred
}

pub(crate) fn multi_inflight_benchmarks(criterion: &mut Criterion) {
    let mut group = criterion.benchmark_group("io_uring_file_multi/read_one_file_qd8_hot_buffered");
    configure(&mut group);
    register(&mut group, "uring_file", uring_one_file, batch_read);
    register(&mut group, "pooled_file", pooled_one_file, batch_read);
    group.finish();

    let mut group = criterion.benchmark_group("io_uring_file_multi/write_one_file_qd8_buffered");
    configure(&mut group);
    register(&mut group, "uring_file", uring_one_file, batch_write);
    register(&mut group, "pooled_file", pooled_one_file, batch_write);
    group.finish();

    let mut group =
        criterion.benchmark_group("io_uring_file_multi/read_eight_files_qd1_hot_buffered");
    configure(&mut group);
    register(&mut group, "uring_file", uring_eight_files, batch_read);
    register(&mut group, "pooled_file", pooled_eight_files, batch_read);
    group.finish();

    let mut group = criterion.benchmark_group("io_uring_file_multi/write_eight_files_qd1_buffered");
    configure(&mut group);
    register(&mut group, "uring_file", uring_eight_files, batch_write);
    register(&mut group, "pooled_file", pooled_eight_files, batch_write);
    group.finish();
}

fn configure(group: &mut BenchmarkGroup<'_, WallTime>) {
    group
        .sample_size(SAMPLE_SIZE)
        .warm_up_time(WARM_UP_TIME)
        .measurement_time(MEASUREMENT_TIME)
        .sampling_mode(SamplingMode::Flat);
}

fn register<H, F, R>(
    group: &mut BenchmarkGroup<'_, WallTime>,
    backend_name: &'static str,
    fixture: F,
    routine: R,
) where
    H: FileIoSubmit,
    F: Copy + Fn(&Path, usize) -> Fixture<H> + 'static,
    R: Copy + Fn(&Waker, &[Lane<H>], &mut Vec<Vec<u8>>, usize) -> u64 + 'static,
{
    for size in SIZES {
        let label = size_label(size);
        let batch_bytes = (size * INFLIGHT) as u64;
        group.throughput(Throughput::Bytes(batch_bytes));
        group.bench_function(BenchmarkId::new(backend_name, label), move |bencher| {
            let directory = BenchmarkDirectory::new(backend_name, label);
            let fixture = fixture(directory.path(), size);
            let waker = Waker::from(Arc::new(ThreadWake(thread::current())));
            let mut buffers: Vec<Vec<u8>> = (0..INFLIGHT).map(|_| vec![0; size]).collect();

            // Prove the fixture before measuring it.
            let probed = routine(&waker, &fixture.lanes, &mut buffers, size);
            assert_eq!(probed, batch_bytes, "probe batch transferred fully");

            bencher.iter_custom(|iterations| {
                let mut transferred = 0_u64;
                let started = Instant::now();
                for _ in 0..iterations {
                    transferred += routine(&waker, &fixture.lanes, &mut buffers, size);
                }
                let elapsed = started.elapsed();
                assert_eq!(
                    transferred,
                    batch_bytes * iterations,
                    "every measured batch transferred fully"
                );
                elapsed
            });
        });
    }
}

fn size_label(size: usize) -> &'static str {
    match size {
        4_096 => "4KiB",
        65_536 => "64KiB",
        _ => unreachable!("unlabeled benchmark size"),
    }
}

struct BenchmarkDirectory(PathBuf);

impl BenchmarkDirectory {
    fn new(backend: &str, size: &str) -> Self {
        let base =
            std::env::var_os(BENCH_DIRECTORY_ENV).map_or_else(std::env::temp_dir, PathBuf::from);
        let ordinal = NEXT_DIRECTORY.fetch_add(1, Ordering::Relaxed);
        let path = base.join(format!(
            "kr-runtime-io-uring-multi-{backend}-{size}-{}-{ordinal}",
            std::process::id()
        ));
        fs::create_dir_all(&path).expect("create benchmark directory");
        Self(path)
    }

    fn path(&self) -> &Path {
        &self.0
    }
}

impl Drop for BenchmarkDirectory {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}
