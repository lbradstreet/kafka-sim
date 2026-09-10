//! Comparable QD1 buffered-file microbenchmarks.
//!
//! `raw_direct` drives io_uring on the caller thread. `raw_actor` adds only a
//! bounded command actor and owned-buffer handoff, making it the topology-
//! matched baseline for `uring_file`. All three use the same ring size, maximum
//! I/O chunk, equivalent file fixture, and owned `Vec<u8>` request/response
//! semantics. File setup, backend construction, and result validation are
//! excluded from measured time.

use std::fs::{self, File, OpenOptions};
use std::future::Future;
use std::hint::black_box;
use std::io;
use std::mem;
use std::os::fd::AsRawFd;
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};
use std::pin::pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{self, Receiver, SyncSender, TrySendError};
use std::task::{Context, Poll, Wake, Waker};
use std::thread::{self, JoinHandle, Thread};
use std::time::{Duration, Instant};

use criterion::measurement::WallTime;
use criterion::{BenchmarkGroup, BenchmarkId, Criterion, SamplingMode, Throughput};
use io_uring::{IoUring, opcode, squeue, types};
use kr_runtime_io::{FileIoSubmit, ReadAtRequest, WriteAtRequest};
use kr_runtime_io_uring::{UringFile, UringFileConfig};

const BENCH_DIRECTORY_ENV: &str = "KR_RUNTIME_IO_URING_BENCH_DIR";
const RING_ENTRIES: u32 = 8;
const MAX_IO_CHUNK_BYTES: usize = 256 * 1_024;
const COMMAND_QUEUE_CAPACITY: usize = 64;
const MAX_FILE_BYTES: u64 = 1024 * 1_024;
const FILE_OFFSET: u64 = 0;

const SAMPLE_SIZE: usize = 20;
const WARM_UP_TIME: Duration = Duration::from_secs(1);
const MEASUREMENT_TIME: Duration = Duration::from_secs(3);

const SIZES: [usize; 4] = [64, 4 * 1_024, 64 * 1_024, 256 * 1_024];

static NEXT_DIRECTORY: AtomicU64 = AtomicU64::new(0);

struct Transfer {
    buffer: Vec<u8>,
    transferred: usize,
}

trait BenchFile {
    fn read(&mut self, buffer: Vec<u8>) -> io::Result<Transfer>;
    fn write(&mut self, buffer: Vec<u8>) -> io::Result<Transfer>;
}

pub(crate) fn file_benchmarks(criterion: &mut Criterion) {
    let mut reads = criterion.benchmark_group("io_uring_file/read_at_qd1_hot_buffered");
    configure(&mut reads);
    register_reads(&mut reads);
    reads.finish();

    let mut writes = criterion.benchmark_group("io_uring_file/write_at_qd1_buffered");
    configure(&mut writes);
    register_writes(&mut writes);
    writes.finish();
}

fn configure(group: &mut BenchmarkGroup<'_, WallTime>) {
    group
        .sample_size(SAMPLE_SIZE)
        .warm_up_time(WARM_UP_TIME)
        .measurement_time(MEASUREMENT_TIME)
        .sampling_mode(SamplingMode::Flat);
}

fn register_reads(group: &mut BenchmarkGroup<'_, WallTime>) {
    register_read_backend(group, "raw_direct", |path| {
        RawDirect::open(path).expect("open raw-direct backend")
    });
    register_read_backend(group, "raw_actor", |path| {
        RawActor::open(path).expect("open raw-actor backend")
    });
    register_read_backend(group, "uring_file", ProviderFile::open);
}

fn register_read_backend<B, F>(
    group: &mut BenchmarkGroup<'_, WallTime>,
    backend_name: &'static str,
    factory: F,
) where
    B: BenchFile,
    F: Copy + Fn(&Path) -> B + 'static,
{
    for size in SIZES {
        let case = Size::new(size);
        group.throughput(Throughput::Bytes(size as u64));
        group.bench_function(BenchmarkId::new(backend_name, case.label), move |bencher| {
            let location = BenchmarkFile::new("read", backend_name, case.label);
            let expected = payload(size);
            fs::write(location.path(), &expected).expect("prepare benchmark read file");
            let mut backend = factory(location.path());
            let mut buffer = vec![0; size];

            // Prove the fixture and implementation before measuring it.
            let probe = backend.read(mem::take(&mut buffer)).expect("probe read");
            validate_read(&probe, &expected);
            buffer = probe.buffer;

            bencher.iter_custom(|iterations| {
                let mut transferred = 0_u64;
                let started = Instant::now();
                for _ in 0..iterations {
                    let transfer = backend
                        .read(mem::take(&mut buffer))
                        .expect("benchmark read");
                    transferred = transferred
                        .checked_add(
                            u64::try_from(transfer.transferred)
                                .expect("transfer length fits benchmark counter"),
                        )
                        .expect("benchmark transfer count does not overflow");
                    buffer = transfer.buffer;
                    black_box(buffer.first());
                    black_box(buffer.last());
                }
                let measured = started.elapsed();
                assert_eq!(
                    transferred,
                    iterations
                        .checked_mul(size as u64)
                        .expect("expected benchmark byte count does not overflow"),
                    "short benchmark read"
                );
                assert_eq!(buffer, expected, "benchmark read returned wrong bytes");
                measured
            });
        });
    }
}

fn register_writes(group: &mut BenchmarkGroup<'_, WallTime>) {
    register_write_backend(group, "raw_direct", |path| {
        RawDirect::open(path).expect("open raw-direct backend")
    });
    register_write_backend(group, "raw_actor", |path| {
        RawActor::open(path).expect("open raw-actor backend")
    });
    register_write_backend(group, "uring_file", ProviderFile::open);
}

fn register_write_backend<B, F>(
    group: &mut BenchmarkGroup<'_, WallTime>,
    backend_name: &'static str,
    factory: F,
) where
    B: BenchFile,
    F: Copy + Fn(&Path) -> B + 'static,
{
    for size in SIZES {
        let case = Size::new(size);
        group.throughput(Throughput::Bytes(size as u64));
        group.bench_function(BenchmarkId::new(backend_name, case.label), move |bencher| {
            let location = BenchmarkFile::new("write", backend_name, case.label);
            prepare_write_file(location.path(), size);
            let expected = payload(size);
            let mut buffer = expected.clone();
            let mut backend = factory(location.path());

            // Prove buffer ownership and the physical result before timing.
            let probe = backend.write(mem::take(&mut buffer)).expect("probe write");
            validate_write(&probe, &expected);
            buffer = probe.buffer;
            validate_file(location.path(), &expected);

            bencher.iter_custom(|iterations| {
                let mut transferred = 0_u64;
                let started = Instant::now();
                for _ in 0..iterations {
                    let transfer = backend
                        .write(mem::take(&mut buffer))
                        .expect("benchmark write");
                    transferred = transferred
                        .checked_add(
                            u64::try_from(transfer.transferred)
                                .expect("transfer length fits benchmark counter"),
                        )
                        .expect("benchmark transfer count does not overflow");
                    buffer = transfer.buffer;
                    black_box(buffer.as_ptr());
                }
                let measured = started.elapsed();
                assert_eq!(
                    transferred,
                    iterations
                        .checked_mul(size as u64)
                        .expect("expected benchmark byte count does not overflow"),
                    "short benchmark write"
                );
                assert_eq!(buffer, expected, "write did not return its input buffer");
                validate_file(location.path(), &expected);
                measured
            });
        });
    }
}

#[derive(Clone, Copy)]
struct Size {
    label: &'static str,
}

impl Size {
    const fn new(bytes: usize) -> Self {
        let label = match bytes {
            64 => "64B",
            4_096 => "4KiB",
            65_536 => "64KiB",
            262_144 => "256KiB",
            _ => panic!("unlabelled benchmark size"),
        };
        Self { label }
    }
}

fn payload(size: usize) -> Vec<u8> {
    (0..size)
        .map(|index| (index as u8).wrapping_mul(31).wrapping_add(17))
        .collect()
}

fn prepare_write_file(path: &Path, size: usize) {
    let file = File::create(path).expect("create benchmark write file");
    file.set_len(size as u64)
        .expect("preallocate benchmark write file");
}

fn validate_read(transfer: &Transfer, expected: &[u8]) {
    assert_eq!(transfer.transferred, expected.len(), "short benchmark read");
    assert_eq!(
        transfer.buffer, expected,
        "benchmark read returned wrong bytes"
    );
}

fn validate_write(transfer: &Transfer, expected: &[u8]) {
    assert_eq!(
        transfer.transferred,
        expected.len(),
        "short benchmark write"
    );
    assert_eq!(
        transfer.buffer, expected,
        "benchmark write did not return its input buffer"
    );
}

fn validate_file(path: &Path, expected: &[u8]) {
    let file = File::open(path).expect("open benchmark file for validation");
    let mut actual = vec![0; expected.len()];
    file.read_exact_at(&mut actual, FILE_OFFSET)
        .expect("read benchmark file for validation");
    assert_eq!(actual, expected, "benchmark write stored wrong bytes");
}

fn file_config() -> UringFileConfig {
    UringFileConfig {
        max_read_bytes: MAX_IO_CHUNK_BYTES,
        max_write_bytes: MAX_IO_CHUNK_BYTES,
        max_file_bytes: MAX_FILE_BYTES,
        command_queue_capacity: COMMAND_QUEUE_CAPACITY,
        ring_entries: RING_ENTRIES,
        max_io_chunk_bytes: MAX_IO_CHUNK_BYTES,
    }
}

struct RawDirect {
    file: File,
    ring: IoUring,
    next_user_data: u64,
    poisoned: bool,
}

impl RawDirect {
    fn open(path: &Path) -> io::Result<Self> {
        let file = OpenOptions::new().read(true).write(true).open(path)?;
        Self::from_file(file)
    }

    fn from_file(file: File) -> io::Result<Self> {
        let ring = IoUring::builder().dontfork().build(RING_ENTRIES)?;
        Ok(Self {
            file,
            ring,
            next_user_data: 1,
            poisoned: false,
        })
    }

    fn allocate_user_data(&mut self) -> io::Result<u64> {
        let user_data = self.next_user_data;
        let Some(next_user_data) = self.next_user_data.checked_add(1) else {
            self.poisoned = true;
            return Err(io::Error::other(
                "raw io_uring user_data identifier space is exhausted",
            ));
        };
        self.next_user_data = next_user_data;
        Ok(user_data)
    }

    fn submit_one(&mut self, entry: &squeue::Entry, user_data: u64) -> io::Result<i32> {
        if self.poisoned {
            return Err(io::Error::other("raw io_uring driver is poisoned"));
        }
        let pushed = {
            let mut submission = self.ring.submission();
            // SAFETY: this QD1 driver retains the descriptor and owned buffer
            // until it consumes the only operation's matching CQE below.
            unsafe { submission.push(entry) }
        };
        pushed.map_err(|_| io::Error::other("raw io_uring submission queue was full"))?;

        let mut submitted = false;
        let mut unexpected_user_data = None;
        loop {
            let wait = self.ring.submit_and_wait(1);
            if wait.as_ref().is_ok_and(|count| *count > 0) {
                submitted = true;
            }

            if let Some(completion) = self.ring.completion().next() {
                if completion.user_data() != user_data {
                    unexpected_user_data.get_or_insert(completion.user_data());
                } else {
                    let result = completion.result();
                    if let Some(actual) = unexpected_user_data {
                        self.poisoned = true;
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            format!(
                                "raw io_uring returned unexpected user_data {actual} while waiting for {user_data}"
                            ),
                        ));
                    }
                    return Ok(result);
                }
            }

            match wait {
                Ok(_) => thread::yield_now(),
                Err(error) if !submitted && !retry_enter(&error) => {
                    // Keep the userspace-only SQE from ever being submitted
                    // after its request buffer has returned to the caller.
                    self.poisoned = true;
                    return Err(error);
                }
                Err(_) => thread::yield_now(),
            }
        }
    }

    fn decode_transfer(buffer: Vec<u8>, result: i32, requested: usize) -> io::Result<Transfer> {
        if result < 0 {
            return Err(io::Error::from_raw_os_error(-result));
        }
        let transferred = result as usize;
        if transferred > requested {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("io_uring transferred {transferred} bytes for {requested}-byte request"),
            ));
        }
        Ok(Transfer {
            buffer,
            transferred,
        })
    }
}

impl BenchFile for RawDirect {
    fn read(&mut self, mut buffer: Vec<u8>) -> io::Result<Transfer> {
        let requested = validate_raw_buffer(&buffer)?;
        let user_data = self.allocate_user_data()?;
        let entry = opcode::Read::new(
            types::Fd(self.file.as_raw_fd()),
            buffer.as_mut_ptr(),
            requested as u32,
        )
        .offset(FILE_OFFSET)
        .build()
        .user_data(user_data);
        let result = self.submit_one(&entry, user_data)?;
        Self::decode_transfer(buffer, result, requested)
    }

    fn write(&mut self, buffer: Vec<u8>) -> io::Result<Transfer> {
        let requested = validate_raw_buffer(&buffer)?;
        let user_data = self.allocate_user_data()?;
        let entry = opcode::Write::new(
            types::Fd(self.file.as_raw_fd()),
            buffer.as_ptr(),
            requested as u32,
        )
        .offset(FILE_OFFSET)
        .build()
        .user_data(user_data);
        let result = self.submit_one(&entry, user_data)?;
        Self::decode_transfer(buffer, result, requested)
    }
}

fn validate_raw_buffer(buffer: &[u8]) -> io::Result<usize> {
    if buffer.is_empty() || buffer.len() > MAX_IO_CHUNK_BYTES {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!(
                "raw request size must be in 1..={MAX_IO_CHUNK_BYTES}, got {}",
                buffer.len()
            ),
        ));
    }
    Ok(buffer.len())
}

fn retry_enter(error: &io::Error) -> bool {
    error.kind() == io::ErrorKind::Interrupted
        || matches!(error.raw_os_error(), Some(libc::EAGAIN) | Some(libc::EBUSY))
}

enum ActorCommand {
    Read { buffer: Vec<u8> },
    Write { buffer: Vec<u8> },
}

struct RawActor {
    sender: Option<SyncSender<ActorCommand>>,
    response: Receiver<io::Result<Transfer>>,
    join: Option<JoinHandle<()>>,
}

impl RawActor {
    fn open(path: &Path) -> io::Result<Self> {
        let file = OpenOptions::new().read(true).write(true).open(path)?;
        let (sender, receiver) = mpsc::sync_channel(COMMAND_QUEUE_CAPACITY);
        let (response_sender, response) = mpsc::sync_channel(1);
        let (ready_sender, ready_receiver) = mpsc::sync_channel(1);
        let join = thread::Builder::new()
            .name("kr-runtime-io-uring-bench-raw-actor".to_owned())
            .spawn(move || match RawDirect::from_file(file) {
                Ok(mut driver) => {
                    if ready_sender.send(Ok(())).is_ok() {
                        run_raw_actor(&mut driver, receiver, response_sender);
                    }
                }
                Err(error) => {
                    let _ = ready_sender.send(Err(error));
                }
            })?;

        match ready_receiver.recv() {
            Ok(Ok(())) => Ok(Self {
                sender: Some(sender),
                response,
                join: Some(join),
            }),
            Ok(Err(error)) => {
                let _ = join.join();
                Err(error)
            }
            Err(_) => {
                let _ = join.join();
                Err(io::Error::other("raw actor stopped during startup"))
            }
        }
    }

    fn execute(&self, command: ActorCommand) -> io::Result<Transfer> {
        let sender = self
            .sender
            .as_ref()
            .ok_or_else(|| io::Error::other("raw actor is stopped"))?;
        match sender.try_send(command) {
            Ok(()) => {}
            Err(TrySendError::Full(_)) => {
                return Err(io::Error::other("raw actor command queue is full"));
            }
            Err(TrySendError::Disconnected(_)) => {
                return Err(io::Error::other("raw actor is stopped"));
            }
        }
        self.response
            .recv()
            .map_err(|_| io::Error::other("raw actor stopped before responding"))?
    }
}

impl BenchFile for RawActor {
    fn read(&mut self, buffer: Vec<u8>) -> io::Result<Transfer> {
        self.execute(ActorCommand::Read { buffer })
    }

    fn write(&mut self, buffer: Vec<u8>) -> io::Result<Transfer> {
        self.execute(ActorCommand::Write { buffer })
    }
}

impl Drop for RawActor {
    fn drop(&mut self) {
        self.sender.take();
        if let Some(join) = self.join.take() {
            let _ = join.join();
        }
    }
}

fn run_raw_actor(
    driver: &mut RawDirect,
    receiver: Receiver<ActorCommand>,
    response: SyncSender<io::Result<Transfer>>,
) {
    while let Ok(command) = receiver.recv() {
        match command {
            ActorCommand::Read { buffer } => {
                let _ = response.send(driver.read(buffer));
            }
            ActorCommand::Write { buffer } => {
                let _ = response.send(driver.write(buffer));
            }
        }
    }
}

struct ThreadWake(Thread);

impl Wake for ThreadWake {
    fn wake(self: Arc<Self>) {
        self.0.unpark();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.0.unpark();
    }
}

struct ProviderFile {
    file: UringFile,
    waker: Waker,
}

impl ProviderFile {
    fn open(path: &Path) -> Self {
        let file = UringFile::open_with_outcome(path, file_config())
            .expect("open UringFile benchmark backend")
            .into_parts()
            .0;
        let waker = Waker::from(Arc::new(ThreadWake(thread::current())));
        Self { file, waker }
    }

    fn block_on<T>(&self, future: impl Future<Output = T>) -> T {
        let mut future = pin!(future);
        let mut context = Context::from_waker(&self.waker);
        loop {
            match future.as_mut().poll(&mut context) {
                Poll::Ready(output) => return output,
                Poll::Pending => thread::park(),
            }
        }
    }
}

impl BenchFile for ProviderFile {
    fn read(&mut self, buffer: Vec<u8>) -> io::Result<Transfer> {
        self.block_on(
            self.file
                .submit_read_at(ReadAtRequest::new(FILE_OFFSET, buffer)),
        )
        .map(|success| Transfer {
            buffer: success.buffer,
            transferred: success.bytes_read,
        })
        .map_err(|error| io::Error::other(error.to_string()))
    }

    fn write(&mut self, buffer: Vec<u8>) -> io::Result<Transfer> {
        self.block_on(
            self.file
                .submit_write_at(WriteAtRequest::new(FILE_OFFSET, buffer)),
        )
        .map(|success| Transfer {
            buffer: success.buffer,
            transferred: success.bytes_written,
        })
        .map_err(|error| io::Error::other(error.to_string()))
    }
}

struct BenchmarkFile {
    directory: PathBuf,
    path: PathBuf,
}

impl BenchmarkFile {
    fn new(operation: &str, backend: &str, size: &str) -> Self {
        let base =
            std::env::var_os(BENCH_DIRECTORY_ENV).map_or_else(std::env::temp_dir, PathBuf::from);
        fs::create_dir_all(&base).expect("create io_uring benchmark base directory");

        loop {
            let ordinal = NEXT_DIRECTORY.fetch_add(1, Ordering::Relaxed);
            let directory = base.join(format!(
                "kr-runtime-io-uring-bench-{operation}-{backend}-{size}-{}-{ordinal}",
                std::process::id()
            ));
            match fs::create_dir(&directory) {
                Ok(()) => {
                    let path = directory.join("file.bin");
                    return Self { directory, path };
                }
                Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {}
                Err(error) => panic!(
                    "create io_uring benchmark directory {}: {error}",
                    directory.display()
                ),
            }
        }
    }

    fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for BenchmarkFile {
    fn drop(&mut self) {
        if !thread::panicking() {
            fs::remove_dir_all(&self.directory).unwrap_or_else(|error| {
                panic!(
                    "remove io_uring benchmark directory {}: {error}",
                    self.directory.display()
                )
            });
        }
    }
}
