use std::fs;
use std::future::Future;
use std::path::PathBuf;
use std::pin::pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::task::{Context, Poll, Wake, Waker};
use std::thread::{self, Thread};

use kr_runtime_ring::{
    AppendRequest, AppendSuccess, ReadPage, ReadRequest, RingCursor, RingReader, RingWriter,
    SyncSuccess, TrimSuccess,
};
use kr_runtime_ring_uring::UringRing;

use super::{BenchRing, uring_config};

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

pub(super) struct UringBenchRing {
    ring: Option<UringRing>,
    directory: PathBuf,
    waker: Waker,
}

impl UringBenchRing {
    pub(super) fn new() -> Self {
        let directory = create_unique_directory();
        let path = directory.join("ring.dstr");
        let ring = match UringRing::create(&path, uring_config()) {
            Ok(ring) => ring,
            Err(error) => {
                let _ = fs::remove_dir_all(&directory);
                panic!("create io_uring benchmark ring: {error}");
            }
        };
        let waker = Waker::from(Arc::new(ThreadWake(thread::current())));

        Self {
            ring: Some(ring),
            directory,
            waker,
        }
    }

    fn ring(&self) -> &UringRing {
        self.ring.as_ref().expect("io_uring benchmark ring is open")
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

impl BenchRing for UringBenchRing {
    fn append(&mut self, records: Vec<Vec<u8>>) -> AppendSuccess {
        self.block_on(self.ring().append(AppendRequest::new(records)))
            .expect("append to io_uring benchmark ring")
    }

    fn sync(&mut self) -> SyncSuccess {
        self.block_on(self.ring().sync())
            .expect("sync io_uring benchmark ring")
    }

    fn trim(&mut self, before: RingCursor) -> TrimSuccess {
        self.block_on(self.ring().trim(before))
            .expect("trim io_uring benchmark ring")
    }

    fn read(&mut self, request: ReadRequest) -> ReadPage {
        self.block_on(self.ring().read(request))
            .expect("read io_uring benchmark ring")
    }
}

impl Drop for UringBenchRing {
    fn drop(&mut self) {
        let close = self.ring.take().map(UringRing::close);
        let remove = fs::remove_dir_all(&self.directory);

        if thread::panicking() {
            return;
        }
        if let Some(result) = close {
            result.expect("close io_uring benchmark ring");
        }
        remove.expect("remove io_uring benchmark directory");
    }
}

fn create_unique_directory() -> PathBuf {
    let base = std::env::var_os("KR_RUNTIME_RING_URING_BENCH_DIR")
        .map_or_else(std::env::temp_dir, PathBuf::from);
    fs::create_dir_all(&base).expect("create io_uring benchmark base directory");

    loop {
        let ordinal = NEXT_DIRECTORY.fetch_add(1, Ordering::Relaxed);
        let directory = base.join(format!(
            "kr-runtime-ring-uring-bench-{}-{ordinal}",
            std::process::id()
        ));
        match fs::create_dir(&directory) {
            Ok(()) => return directory,
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {}
            Err(error) => panic!(
                "create io_uring benchmark directory {}: {error}",
                directory.display()
            ),
        }
    }
}

/// Per-file-actor backend whose backing file is opened with `O_DSYNC`.
///
/// Every completed frame write is a durability write-through with real
/// device latency, so this backend exposes the per-write cost that the
/// buffered backend absorbs into the page cache. It hosts the same
/// `FileRingDriver` state machine over `UringFile::from_file` on a dedicated
/// driver thread, mirroring the pooled backend's scaffold.
pub(super) struct WriteThroughBenchRing {
    // Teardown order: the ring handle drops first so the driver actor
    // completes and its thread can be joined, then the directory is removed.
    ring: Option<kr_runtime_ring::file::FileRing<kr_runtime_io_uring::UringFile>>,
    driver: Option<thread::JoinHandle<()>>,
    directory: PathBuf,
    waker: Waker,
}

fn uring_file_config() -> kr_runtime_io_uring::UringFileConfig {
    let physical = super::file_ring_config()
        .physical_file_bytes()
        .expect("benchmark ring physical length is valid");
    kr_runtime_io_uring::UringFileConfig {
        max_read_bytes: super::MAX_IO_REQUEST_BYTES,
        max_write_bytes: super::MAX_IO_REQUEST_BYTES,
        max_file_bytes: physical,
        command_queue_capacity: super::COMMAND_QUEUE_CAPACITY,
        ring_entries: 8,
        max_io_chunk_bytes: super::MAX_IO_REQUEST_BYTES,
    }
}

fn block_on_local<T>(future: impl Future<Output = T>) -> T {
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

impl WriteThroughBenchRing {
    pub(super) fn new() -> Self {
        use std::os::unix::fs::OpenOptionsExt;

        let directory = create_unique_directory();
        let path = directory.join("ring.dstr");
        let backing = std::fs::File::options()
            .read(true)
            .write(true)
            .create_new(true)
            .custom_flags(libc::O_DSYNC)
            .open(&path)
            .expect("create write-through benchmark ring file");
        let file = kr_runtime_io_uring::UringFile::from_file(backing, uring_file_config())
            .expect("open write-through benchmark session");

        let (ready_sender, ready_receiver) = std::sync::mpsc::sync_channel(1);
        let driver = thread::Builder::new()
            .name("kr-runtime-file-ring-dsync-bench".to_owned())
            .spawn(move || {
                let created = block_on_local(kr_runtime_ring::file::FileRingDriver::create(
                    file,
                    super::file_ring_config(),
                ));
                match created {
                    Ok(driver) => {
                        let (ring, actor) = driver.start();
                        if ready_sender.send(Ok(ring)).is_ok() {
                            block_on_local(actor);
                        }
                    }
                    Err(error) => {
                        let _ = ready_sender.send(Err(format!("{error:?}")));
                    }
                }
            })
            .expect("spawn write-through benchmark driver thread");
        let ring = ready_receiver
            .recv()
            .expect("write-through benchmark driver reports readiness")
            .unwrap_or_else(|error| panic!("create write-through benchmark ring: {error}"));
        let waker = Waker::from(Arc::new(ThreadWake(thread::current())));

        Self {
            ring: Some(ring),
            driver: Some(driver),
            directory,
            waker,
        }
    }

    fn ring(&self) -> &kr_runtime_ring::file::FileRing<kr_runtime_io_uring::UringFile> {
        self.ring
            .as_ref()
            .expect("write-through benchmark ring is open")
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

impl BenchRing for WriteThroughBenchRing {
    fn append(&mut self, records: Vec<Vec<u8>>) -> AppendSuccess {
        self.block_on(self.ring().append(AppendRequest::new(records)))
            .expect("append to write-through benchmark ring")
    }

    fn sync(&mut self) -> SyncSuccess {
        self.block_on(self.ring().sync())
            .expect("sync write-through benchmark ring")
    }

    fn trim(&mut self, before: RingCursor) -> TrimSuccess {
        self.block_on(self.ring().trim(before))
            .expect("trim write-through benchmark ring")
    }

    fn read(&mut self, request: ReadRequest) -> ReadPage {
        self.block_on(self.ring().read(request))
            .expect("read write-through benchmark ring")
    }
}

impl Drop for WriteThroughBenchRing {
    fn drop(&mut self) {
        drop(self.ring.take());
        if let Some(driver) = self.driver.take() {
            let _ = driver.join();
        }
        let remove = fs::remove_dir_all(&self.directory);
        if !thread::panicking() {
            remove.expect("remove write-through benchmark directory");
        }
    }
}
