//! Fixed-duration at-capacity churn: append latency under concurrent trims.
//!
//! A custom driver, per the measurement protocol: per-operation tail latency
//! needs a histogram over a sustained run, which Criterion's steady-state
//! estimates cannot provide. One thread appends as fast as admission allows,
//! absorbing typed capacity refusals; a second concurrently maintains the
//! ring with the sync-trim-sync checkpoint cadence. The measured quantity is
//! time-to-accepted-append — including any wait for reclaim — plus the
//! maintenance fence latencies and the refusal rate. Results are absolute
//! and per-backend; this benchmark is not comparable to `ring`, which
//! measures isolated operations far from capacity.
//!
//! `KR_RUNTIME_RING_CHURN_SECS` overrides the measured duration per backend, and
//! `KR_RUNTIME_RING_URING_BENCH_DIR` selects the target filesystem.

#[cfg(target_os = "linux")]
mod driver {
    use std::fs::{self, File};
    use std::future::Future;
    use std::path::PathBuf;
    use std::pin::pin;
    use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
    use std::sync::{Arc, mpsc};
    use std::task::{Context, Poll, Wake, Waker};
    use std::thread::{self, JoinHandle, Thread};
    use std::time::{Duration, Instant};

    use kr_runtime_io_uring::{PooledUringFile, UringIoPool, UringPoolConfig};
    use kr_runtime_ring::file::{FileRing, FileRingConfig, FileRingDriver};
    use kr_runtime_ring::{
        AppendRequest, RingCursor, RingError, RingLimits, RingReader, RingStatus, RingWriter,
    };
    use kr_runtime_ring_uring::{UringRing, UringRingConfig};

    const RECORD_BYTES: usize = 1_024;
    const DATA_CAPACITY_BYTES: u64 = 4 * 1_024 * 1_024;
    const MAX_LIVE_PAYLOAD_BYTES: usize = 2 * 1_024 * 1_024;
    const MAX_LIVE_RECORDS: usize = 4_096;
    const MAX_IO_REQUEST_BYTES: usize = 64 * 1_024;
    const COMMAND_QUEUE_CAPACITY: usize = 64;
    const RING_ENTRIES: u32 = 8;
    /// The trimmer engages above this much live payload...
    const TRIM_TRIGGER_BYTES: usize = 1_536 * 1_024;
    /// ...and trims back to this many retained records.
    const KEEP_RECORDS: u64 = 512;
    const DEFAULT_SECONDS: u64 = 10;

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

    fn limits() -> RingLimits {
        RingLimits {
            max_record_bytes: 16 * 1_024,
            max_live_records: MAX_LIVE_RECORDS,
            max_live_payload_bytes: MAX_LIVE_PAYLOAD_BYTES,
            max_read_records: 64,
            max_read_bytes: 1_024 * 1_024,
            max_batch_records: 64,
            max_batch_bytes: 1_024 * 1_024,
        }
    }

    fn file_ring_config() -> FileRingConfig {
        FileRingConfig {
            limits: limits(),
            data_capacity_bytes: DATA_CAPACITY_BYTES,
            max_io_request_bytes: MAX_IO_REQUEST_BYTES,
            command_queue_capacity: COMMAND_QUEUE_CAPACITY,
        }
    }

    /// One churnable ring session usable from two threads.
    trait ChurnHandle: Clone + Send + 'static {
        fn append(&self, payload: Vec<u8>) -> Result<RingCursor, (RingError, Vec<u8>)>;
        fn sync(&self) -> RingCursor;
        fn trim(&self, before: RingCursor);
        fn status(&self) -> RingStatus;
    }

    #[derive(Clone)]
    struct ActorHandle(UringRing);

    impl ChurnHandle for ActorHandle {
        fn append(&self, payload: Vec<u8>) -> Result<RingCursor, (RingError, Vec<u8>)> {
            match block_on(self.0.append(AppendRequest::new(vec![payload]))) {
                Ok(success) => Ok(success.next_cursor),
                Err(error) => {
                    let mut failure = error.into_parts().1;
                    let payload = failure.records.pop().expect("rejected payload returns");
                    Err((failure.error, payload))
                }
            }
        }

        fn sync(&self) -> RingCursor {
            block_on(self.0.sync())
                .expect("sync churn ring")
                .durable_tail
        }

        fn trim(&self, before: RingCursor) {
            block_on(self.0.trim(before)).expect("trim churn ring");
        }

        fn status(&self) -> RingStatus {
            block_on(self.0.status()).expect("status of churn ring")
        }
    }

    #[derive(Clone)]
    struct PooledHandle(FileRing<PooledUringFile>);

    impl ChurnHandle for PooledHandle {
        fn append(&self, payload: Vec<u8>) -> Result<RingCursor, (RingError, Vec<u8>)> {
            match block_on(self.0.append(AppendRequest::new(vec![payload]))) {
                Ok(success) => Ok(success.next_cursor),
                Err(error) => {
                    let mut failure = error.into_parts().1;
                    let payload = failure.records.pop().expect("rejected payload returns");
                    Err((failure.error, payload))
                }
            }
        }

        fn sync(&self) -> RingCursor {
            block_on(self.0.sync())
                .expect("sync churn ring")
                .durable_tail
        }

        fn trim(&self, before: RingCursor) {
            block_on(self.0.trim(before)).expect("trim churn ring");
        }

        fn status(&self) -> RingStatus {
            block_on(self.0.status()).expect("status of churn ring")
        }
    }

    struct Percentiles {
        p50: Duration,
        p90: Duration,
        p99: Duration,
        p999: Duration,
        max: Duration,
    }

    fn percentiles(mut samples: Vec<Duration>) -> Option<Percentiles> {
        if samples.is_empty() {
            return None;
        }
        samples.sort_unstable();
        let at = |q: f64| {
            let index = ((samples.len() - 1) as f64 * q).round() as usize;
            samples[index]
        };
        Some(Percentiles {
            p50: at(0.50),
            p90: at(0.90),
            p99: at(0.99),
            p999: at(0.999),
            max: *samples.last().expect("nonempty samples"),
        })
    }

    fn is_capacity(error: &RingError) -> bool {
        matches!(
            error,
            RingError::RecordCapacityReached { .. }
                | RingError::PayloadCapacityReached { .. }
                | RingError::PhysicalCapacityReached { .. }
        )
    }

    fn churn<H: ChurnHandle>(name: &str, handle: &H) {
        let seconds = std::env::var("KR_RUNTIME_RING_CHURN_SECS")
            .ok()
            .and_then(|value| value.parse().ok())
            .unwrap_or(DEFAULT_SECONDS);
        let duration = Duration::from_secs(seconds);

        // Reach capacity before measuring: the run must start where refusals
        // and reclaim already bind.
        let mut prefilled = 0_u64;
        loop {
            match handle.append(vec![0xa5; RECORD_BYTES]) {
                Ok(_) => prefilled += 1,
                Err((error, _)) => {
                    assert!(is_capacity(&error), "unexpected prefill refusal: {error:?}");
                    break;
                }
            }
        }

        let done = Arc::new(AtomicBool::new(false));
        let trimmer_done = Arc::clone(&done);
        let trimmer_handle = handle.clone();
        let trimmer = thread::spawn(move || {
            let mut fences = Vec::new();
            let mut trims = 0_u64;
            while !trimmer_done.load(Ordering::Acquire) {
                let status = trimmer_handle.status();
                if status.accepted_live_payload_bytes > TRIM_TRIGGER_BYTES {
                    let started = Instant::now();
                    let durable = trimmer_handle.sync();
                    fences.push(started.elapsed());
                    trimmer_handle
                        .trim(RingCursor::new(durable.get().saturating_sub(KEEP_RECORDS)));
                    let started = Instant::now();
                    trimmer_handle.sync();
                    fences.push(started.elapsed());
                    trims += 1;
                } else {
                    thread::park_timeout(Duration::from_micros(200));
                }
            }
            (fences, trims)
        });

        let mut latencies = Vec::with_capacity(1 << 20);
        let mut refusals = 0_u64;
        let mut payload = vec![0xa5; RECORD_BYTES];
        let started = Instant::now();
        let deadline = started + duration;
        while Instant::now() < deadline {
            let attempt_started = Instant::now();
            loop {
                match handle.append(std::mem::take(&mut payload)) {
                    Ok(_) => break,
                    Err((error, returned)) => {
                        assert!(is_capacity(&error), "unexpected refusal: {error:?}");
                        refusals += 1;
                        payload = returned;
                        thread::park_timeout(Duration::from_micros(50));
                    }
                }
            }
            latencies.push(attempt_started.elapsed());
            payload = vec![0xa5; RECORD_BYTES];
        }
        let elapsed = started.elapsed();
        done.store(true, Ordering::Release);
        let (fences, trims) = trimmer.join().expect("trimmer thread");

        let accepted = latencies.len() as u64;
        let throughput =
            (accepted as f64 * RECORD_BYTES as f64) / elapsed.as_secs_f64() / (1024.0 * 1024.0);
        println!("\n== {name} ==");
        println!(
            "duration {:.1}s, prefilled {prefilled} records to capacity",
            elapsed.as_secs_f64()
        );
        println!(
            "accepted {accepted} appends ({:.0}/s, {throughput:.1} MiB/s payload), \
             {refusals} capacity refusals ({:.2} per accepted append)",
            accepted as f64 / elapsed.as_secs_f64(),
            refusals as f64 / accepted.max(1) as f64,
        );
        if let Some(p) = percentiles(latencies) {
            println!(
                "append-to-accept latency: p50 {:?}, p90 {:?}, p99 {:?}, p99.9 {:?}, max {:?}",
                p.p50, p.p90, p.p99, p.p999, p.max
            );
        }
        if let Some(p) = percentiles(fences) {
            println!(
                "maintenance fence latency over {trims} sync-trim-sync rounds: \
                 p50 {:?}, p90 {:?}, p99 {:?}, p99.9 {:?}, max {:?}",
                p.p50, p.p90, p.p99, p.p999, p.max
            );
        }
    }

    fn bench_directory() -> PathBuf {
        let base = std::env::var_os("KR_RUNTIME_RING_URING_BENCH_DIR")
            .map_or_else(std::env::temp_dir, PathBuf::from);
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let directory = base.join(format!(
            "kr-runtime-ring-churn-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir_all(&directory).expect("create churn benchmark directory");
        directory
    }

    fn run_actor_backend() {
        let directory = bench_directory();
        let file = file_ring_config();
        let config = UringRingConfig {
            limits: file.limits,
            data_capacity_bytes: file.data_capacity_bytes,
            max_io_request_bytes: file.max_io_request_bytes,
            command_queue_capacity: file.command_queue_capacity,
            ring_entries: RING_ENTRIES,
            max_io_chunk_bytes: file.max_io_request_bytes,
            startup_timeout: Duration::from_secs(30),
            shutdown_timeout: Duration::from_secs(30),
        };
        let ring = UringRing::create(directory.join("ring.dstr"), config)
            .expect("create actor churn ring");
        churn("file_uring_buffered", &ActorHandle(ring.clone()));
        ring.close().expect("close actor churn ring");
        let _ = fs::remove_dir_all(&directory);
    }

    struct PooledSession {
        ring: Option<FileRing<PooledUringFile>>,
        driver: Option<JoinHandle<()>>,
        pool: Option<UringIoPool>,
    }

    impl Drop for PooledSession {
        fn drop(&mut self) {
            drop(self.ring.take());
            if let Some(driver) = self.driver.take() {
                let _ = driver.join();
            }
            drop(self.pool.take());
        }
    }

    fn run_pooled_backend() {
        let directory = bench_directory();
        let file_config = file_ring_config();
        let physical = file_config
            .physical_file_bytes()
            .expect("churn ring physical length is valid");
        let pool_config = UringPoolConfig {
            max_read_bytes: MAX_IO_REQUEST_BYTES,
            max_write_bytes: MAX_IO_REQUEST_BYTES,
            max_file_bytes: physical,
            file_queue_capacity: COMMAND_QUEUE_CAPACITY,
            ring_entries: RING_ENTRIES,
            max_in_flight: 8,
            blocking_threads: 1,
            max_io_chunk_bytes: MAX_IO_REQUEST_BYTES,
        };
        let backing = File::options()
            .read(true)
            .write(true)
            .create_new(true)
            .open(directory.join("ring.dstr"))
            .expect("create pooled churn ring file");
        let pool = UringIoPool::new(pool_config).expect("create churn pool");
        let registered = pool
            .register_file(backing)
            .expect("register pooled churn file");

        let (ready_sender, ready_receiver) = mpsc::sync_channel(1);
        let driver = thread::Builder::new()
            .name("kr-runtime-ring-churn-pooled".to_owned())
            .spawn(
                move || match block_on(FileRingDriver::create(registered, file_ring_config())) {
                    Ok(driver) => {
                        let (ring, actor) = driver.start();
                        if ready_sender.send(Ok(ring)).is_ok() {
                            block_on(actor);
                        }
                    }
                    Err(error) => {
                        let _ = ready_sender.send(Err(format!("{error:?}")));
                    }
                },
            )
            .expect("spawn pooled churn driver");
        let ring = ready_receiver
            .recv()
            .expect("pooled churn driver reports readiness")
            .unwrap_or_else(|error| panic!("create pooled churn ring: {error}"));
        let session = PooledSession {
            ring: Some(ring.clone()),
            driver: Some(driver),
            pool: Some(pool),
        };
        churn("file_pooled_buffered", &PooledHandle(ring));
        drop(session);
        let _ = fs::remove_dir_all(&directory);
    }

    pub(crate) fn run() {
        run_actor_backend();
        run_pooled_backend();
    }
}

#[cfg(target_os = "linux")]
fn main() {
    driver::run();
}

#[cfg(not(target_os = "linux"))]
fn main() {
    println!("ring_churn requires Linux io_uring");
}
