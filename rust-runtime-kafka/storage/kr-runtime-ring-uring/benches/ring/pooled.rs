//! Pooled-provider backend for the ring benchmark.
//!
//! Runs the identical `FileRingDriver` state machine and fence cadence as the
//! `file_uring_buffered` backend, with the storage session provided by the
//! shared-ring `UringIoPool` instead of a dedicated per-file `UringFile`
//! actor pair. The ring engine, format, limits, and filesystem semantics are
//! unchanged, so the two backends are directly comparable.

use std::fs::{self, File};
use std::future::Future;
use std::os::unix::fs::OpenOptionsExt;
use std::path::PathBuf;
use std::pin::pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, mpsc};
use std::task::{Context, Poll, Wake, Waker};
use std::thread::{self, JoinHandle, Thread};

use kr_runtime_io_uring::{PooledUringFile, UringIoPool, UringPoolConfig};
use kr_runtime_ring::file::{FileRing, FileRingDriver};
use kr_runtime_ring::{
    AppendRequest, AppendSuccess, ReadPage, ReadRequest, RingCursor, RingReader, RingWriter,
    SyncSuccess, TrimSuccess,
};

use super::{BenchRing, MAX_IO_REQUEST_BYTES, file_ring_config};

static NEXT_DIRECTORY: AtomicU64 = AtomicU64::new(0);

struct ThreadWake(Thread);

impl Wake for ThreadWake {
    fn wake(self: Arc<Self>) {
        self.0.unpark();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.0.unpark();
    }
}

fn block_on<T>(future: impl Future<Output = T>) -> T {
    let waker = Waker::from(Arc::new(ThreadWake(thread::current())));
    let mut future = pin!(future);
    let mut context = Context::from_waker(&waker);
    loop {
        match future.as_mut().poll(&mut context) {
            Poll::Ready(output) => return output,
            Poll::Pending => thread::park(),
        }
    }
}

fn pool_config() -> UringPoolConfig {
    let physical = file_ring_config()
        .physical_file_bytes()
        .expect("benchmark ring physical length is valid");
    UringPoolConfig {
        max_read_bytes: MAX_IO_REQUEST_BYTES,
        max_write_bytes: MAX_IO_REQUEST_BYTES,
        max_file_bytes: physical,
        file_queue_capacity: super::COMMAND_QUEUE_CAPACITY,
        ring_entries: 8,
        max_in_flight: 8,
        blocking_threads: 1,
        max_io_chunk_bytes: MAX_IO_REQUEST_BYTES,
    }
}

pub(super) struct PooledBenchRing {
    // Teardown order: the ring handle drops first so the driver actor
    // completes and its thread can be joined, then the pool drains, then the
    // directory is removed.
    ring: Option<FileRing<PooledUringFile>>,
    driver: Option<JoinHandle<()>>,
    pool: Option<UringIoPool>,
    directory: PathBuf,
    waker: Waker,
}

impl PooledBenchRing {
    pub(super) fn new() -> Self {
        Self::with_open_flags(0)
    }

    /// Opens the backing file with `O_DSYNC`, so every completed frame write
    /// is a durability write-through with real device latency.
    pub(super) fn new_write_through() -> Self {
        Self::with_open_flags(libc::O_DSYNC)
    }

    fn with_open_flags(flags: i32) -> Self {
        let directory = create_unique_directory();
        let path = directory.join("ring.dstr");
        let backing = File::options()
            .read(true)
            .write(true)
            .create_new(true)
            .custom_flags(flags)
            .open(&path)
            .expect("create pooled benchmark ring file");
        let pool = UringIoPool::new(pool_config()).expect("create benchmark pool");
        let file = pool
            .register_file(backing)
            .expect("register pooled benchmark file");

        let (ready_sender, ready_receiver) = mpsc::sync_channel(1);
        let driver = thread::Builder::new()
            .name("kr-runtime-file-ring-pooled-bench".to_owned())
            .spawn(move || {
                let created = block_on(FileRingDriver::create(file, file_ring_config()));
                match created {
                    Ok(driver) => {
                        let (ring, actor) = driver.start();
                        if ready_sender.send(Ok(ring)).is_ok() {
                            block_on(actor);
                        }
                    }
                    Err(error) => {
                        let _ = ready_sender.send(Err(format!("{error:?}")));
                    }
                }
            })
            .expect("spawn pooled benchmark driver thread");
        let ring = ready_receiver
            .recv()
            .expect("pooled benchmark driver reports readiness")
            .unwrap_or_else(|error| panic!("create pooled benchmark ring: {error}"));
        let waker = Waker::from(Arc::new(ThreadWake(thread::current())));

        Self {
            ring: Some(ring),
            driver: Some(driver),
            pool: Some(pool),
            directory,
            waker,
        }
    }

    fn ring(&self) -> &FileRing<PooledUringFile> {
        self.ring.as_ref().expect("pooled benchmark ring is open")
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

impl BenchRing for PooledBenchRing {
    fn append(&mut self, records: Vec<Vec<u8>>) -> AppendSuccess {
        self.block_on(self.ring().append(AppendRequest::new(records)))
            .expect("append to pooled benchmark ring")
    }

    fn sync(&mut self) -> SyncSuccess {
        self.block_on(self.ring().sync())
            .expect("sync pooled benchmark ring")
    }

    fn trim(&mut self, before: RingCursor) -> TrimSuccess {
        self.block_on(self.ring().trim(before))
            .expect("trim pooled benchmark ring")
    }

    fn read(&mut self, request: ReadRequest) -> ReadPage {
        self.block_on(self.ring().read(request))
            .expect("read pooled benchmark ring")
    }
}

impl Drop for PooledBenchRing {
    fn drop(&mut self) {
        drop(self.ring.take());
        if let Some(driver) = self.driver.take() {
            let _ = driver.join();
        }
        drop(self.pool.take());
        let remove = fs::remove_dir_all(&self.directory);
        if !thread::panicking() {
            remove.expect("remove pooled benchmark directory");
        }
    }
}

fn create_unique_directory() -> PathBuf {
    let base = std::env::var_os("KR_RUNTIME_RING_URING_BENCH_DIR")
        .map_or_else(std::env::temp_dir, PathBuf::from);
    fs::create_dir_all(&base).expect("create pooled benchmark base directory");

    loop {
        let ordinal = NEXT_DIRECTORY.fetch_add(1, Ordering::Relaxed);
        let directory = base.join(format!(
            "kr-runtime-ring-pooled-bench-{}-{ordinal}",
            std::process::id()
        ));
        match fs::create_dir(&directory) {
            Ok(()) => return directory,
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {}
            Err(error) => panic!(
                "create pooled benchmark directory {}: {error}",
                directory.display()
            ),
        }
    }
}
