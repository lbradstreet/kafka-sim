//! Drives the file-ring engine over the shared-ring pooled file provider.
//!
//! Mirrors the durability shape of the `UringRing` tests: records synced
//! through one pooled session must survive teardown and recover through a
//! fresh pooled session, proving the pooled provider's write pipelining and
//! fencing satisfy the ring's invocation-order durability contract.

#![cfg(target_os = "linux")]

use std::fs::{self, File};
use std::future::Future;
use std::path::PathBuf;
use std::pin::pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, mpsc};
use std::task::{Context, Poll, Wake, Waker};
use std::thread::{self, JoinHandle, Thread};

use kr_runtime_io_uring::{PooledUringFile, UringIoPool, UringPoolConfig};
use kr_runtime_ring::file::{FileRing, FileRingConfig, FileRingDriver};
use kr_runtime_ring::{AppendRequest, ReadRequest, RingCursor, RingReader, RingWriter};

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

struct TestDirectory(PathBuf);

impl TestDirectory {
    fn new(name: &str) -> Self {
        let ordinal = NEXT_DIRECTORY.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "kr-runtime-ring-pooled-{name}-{}-{ordinal}",
            std::process::id()
        ));
        fs::create_dir_all(&path).expect("create isolated test directory");
        Self(path)
    }

    fn ring_path(&self) -> PathBuf {
        self.0.join("ring.dstr")
    }
}

impl Drop for TestDirectory {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

fn pool_config_for(file: &FileRingConfig, max_io_chunk_bytes: usize) -> UringPoolConfig {
    let physical = file
        .physical_file_bytes()
        .expect("test ring physical length is valid");
    UringPoolConfig {
        max_read_bytes: file.max_io_request_bytes,
        max_write_bytes: file.max_io_request_bytes,
        max_file_bytes: physical,
        file_queue_capacity: file.command_queue_capacity,
        ring_entries: 4,
        max_in_flight: 4,
        blocking_threads: 1,
        max_io_chunk_bytes,
    }
}

/// A pooled-backed ring session: the driver actor runs on its own thread,
/// exactly as `UringRing` hosts it, with storage from a shared pool.
struct PooledRing {
    ring: Option<FileRing<PooledUringFile>>,
    driver: Option<JoinHandle<()>>,
    pool: Option<UringIoPool>,
}

enum Mode {
    Create,
    Recover,
}

impl PooledRing {
    fn start(directory: &TestDirectory, max_io_chunk_bytes: usize, mode: &Mode) -> Self {
        Self::start_with(
            directory,
            FileRingConfig::default(),
            max_io_chunk_bytes,
            mode,
        )
    }

    fn start_with(
        directory: &TestDirectory,
        file_config: FileRingConfig,
        max_io_chunk_bytes: usize,
        mode: &Mode,
    ) -> Self {
        let pool_config = pool_config_for(&file_config, max_io_chunk_bytes);
        let backing = match mode {
            Mode::Create => File::options()
                .read(true)
                .write(true)
                .create_new(true)
                .open(directory.ring_path()),
            Mode::Recover => File::options()
                .read(true)
                .write(true)
                .open(directory.ring_path()),
        }
        .expect("open pooled ring backing file");
        let pool = UringIoPool::new(pool_config).expect("create pool");
        let file = pool.register_file(backing).expect("register pooled file");

        let create = matches!(mode, Mode::Create);
        let (ready_sender, ready_receiver) = mpsc::sync_channel(1);
        let driver = thread::Builder::new()
            .name("kr-runtime-file-ring-pooled-test".to_owned())
            .spawn(move || {
                let recovered = block_on(async {
                    if create {
                        FileRingDriver::create(file, file_config).await
                    } else {
                        FileRingDriver::open(file, file_config).await
                    }
                });
                match recovered {
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
            .expect("spawn pooled ring driver thread");
        let ring = ready_receiver
            .recv()
            .expect("pooled ring driver reports readiness")
            .unwrap_or_else(|error| panic!("start pooled ring: {error}"));
        Self {
            ring: Some(ring),
            driver: Some(driver),
            pool: Some(pool),
        }
    }

    fn ring(&self) -> &FileRing<PooledUringFile> {
        self.ring.as_ref().expect("pooled ring is open")
    }
}

impl Drop for PooledRing {
    fn drop(&mut self) {
        drop(self.ring.take());
        if let Some(driver) = self.driver.take() {
            let _ = driver.join();
        }
        drop(self.pool.take());
    }
}

#[test]
fn synced_records_survive_reopen_through_a_fresh_pool() {
    // Small chunks force partial lower-level transfers through the pooled
    // provider's pipeline, the same shape the UringRing suite exercises.
    for chunk in [37, 64 * 1_024] {
        let directory = TestDirectory::new(&format!("recovery-{chunk}"));
        let payloads = vec![vec![b'x'; 700], vec![b'y'; 11], vec![b'z'; 257]];
        {
            let session = PooledRing::start(&directory, chunk, &Mode::Create);
            let appended = block_on(session.ring().append(AppendRequest::new(payloads.clone())))
                .expect("append to pooled ring");
            assert_eq!(appended.next_cursor, RingCursor::new(3));
            let synced = block_on(session.ring().sync()).expect("sync pooled ring");
            assert_eq!(synced.durable_tail, RingCursor::new(3));
        }

        let session = PooledRing::start(&directory, chunk, &Mode::Recover);
        let status = block_on(session.ring().status()).expect("status of recovered ring");
        assert_eq!(status.durable_tail, RingCursor::new(3));
        let page = block_on(
            session
                .ring()
                .read(ReadRequest::new(RingCursor::START, 3, 4_096)),
        )
        .expect("read recovered records");
        let recovered: Vec<Vec<u8>> = page
            .records
            .into_iter()
            .map(|record| record.buffer)
            .collect();
        assert_eq!(recovered, payloads, "chunk {chunk} lost synced records");
    }
}

#[test]
fn concurrent_appends_from_two_handles_stay_fifo_per_invocation() {
    // Two clones of the ring handle interleave appends and a sync; the
    // engine orders them by invocation, and the pooled provider must
    // preserve that order through its pipelined writes.
    let directory = TestDirectory::new("two-handles");
    let session = PooledRing::start(&directory, 64 * 1_024, &Mode::Create);
    let second = session.ring().clone();

    let first_append = session
        .ring()
        .append(AppendRequest::new(vec![vec![1u8; 64]]));
    let second_append = second.append(AppendRequest::new(vec![vec![2u8; 64]]));
    let sync = session.ring().sync();

    let first = block_on(first_append).expect("first append");
    let appended = block_on(second_append).expect("second append");
    let synced = block_on(sync).expect("sync");
    assert_eq!(first.next_cursor, RingCursor::new(1));
    assert_eq!(appended.next_cursor, RingCursor::new(2));
    assert_eq!(synced.durable_tail, RingCursor::new(2));

    let page = block_on(
        session
            .ring()
            .read(ReadRequest::new(RingCursor::START, 2, 4_096)),
    )
    .expect("read both records");
    assert_eq!(page.records.len(), 2);
    assert_eq!(page.records[0].buffer, vec![1u8; 64]);
    assert_eq!(page.records[1].buffer, vec![2u8; 64]);
}

/// Payload for the record whose cursor position is `seq`, sized to vary
/// wrap points and verifiable from the cursor alone.
fn churn_payload(seq: u64) -> Vec<u8> {
    let len = 500 + ((seq as usize * 97) % 900);
    vec![(seq % 251) as u8; len]
}

#[test]
fn appends_race_trims_at_capacity_across_wraparound() {
    // A deliberately tiny ring: ~400 KiB of appends cycle a 64 KiB data
    // area many times while capacity limits bind constantly. One handle
    // appends and absorbs typed capacity rejections; a second concurrently
    // trims and checkpoints to reclaim space. This drives wrapped records,
    // capacity refusals, trims, and durability fences through the pooled
    // provider's pipeline at once, and then proves nothing accepted was
    // lost or reordered — including across recovery through a fresh pool.
    use std::sync::atomic::AtomicBool;
    use std::time::{Duration, Instant};

    use kr_runtime_ring::{RingError, RingLimits};

    const TOTAL_RECORDS: u64 = 400;
    const KEEP_LIVE: u64 = 8;
    const TRIM_THRESHOLD: usize = 24;

    let file_config = FileRingConfig {
        limits: RingLimits {
            max_record_bytes: 4_096,
            max_live_records: 64,
            max_live_payload_bytes: 32 * 1_024,
            max_read_records: 64,
            max_read_bytes: 64 * 1_024,
            max_batch_records: 16,
            max_batch_bytes: 16 * 1_024,
        },
        data_capacity_bytes: 64 * 1_024,
        ..FileRingConfig::default()
    };
    let data_capacity = file_config.data_capacity_bytes;

    let directory = TestDirectory::new("capacity-churn");
    let session = PooledRing::start_with(&directory, file_config, 64 * 1_024, &Mode::Create);
    let appender_ring = session.ring().clone();
    let trimmer_ring = session.ring().clone();
    let done = Arc::new(AtomicBool::new(false));
    let trimmer_done = Arc::clone(&done);
    // The trimmer stays inert until the appender has provably filled the
    // ring, so the first capacity refusal is deterministic rather than a
    // race the trimmer can win.
    let saturated = Arc::new(AtomicBool::new(false));
    let appender_saturated = Arc::clone(&saturated);

    let appender = thread::spawn(move || {
        let mut rejections = 0_u64;
        let mut appended_bytes = 0_u64;
        for seq in 1..=TOTAL_RECORDS {
            let mut payload = churn_payload(seq);
            let deadline = Instant::now() + Duration::from_secs(60);
            loop {
                match block_on(appender_ring.append(AppendRequest::new(vec![payload]))) {
                    Ok(success) => {
                        assert_eq!(
                            success.next_cursor,
                            RingCursor::new(seq),
                            "appends assign strictly sequential cursors"
                        );
                        appended_bytes += churn_payload(seq).len() as u64;
                        break;
                    }
                    Err(error) => {
                        assert_eq!(
                            error.certainty(),
                            kr_runtime::CompletionCertainty::NotApplied,
                            "a capacity rejection precedes any effect"
                        );
                        let failure = error.into_parts().1;
                        assert!(
                            matches!(
                                failure.error,
                                RingError::RecordCapacityReached { .. }
                                    | RingError::PayloadCapacityReached { .. }
                                    | RingError::PhysicalCapacityReached { .. }
                            ),
                            "unexpected append rejection at capacity: {:?}",
                            failure.error
                        );
                        appender_saturated.store(true, Ordering::Release);
                        let detail = format!("{:?}", failure.error);
                        let mut records = failure.records;
                        assert_eq!(records.len(), 1, "the rejected batch came back");
                        payload = records.pop().expect("rejected payload is present");
                        rejections += 1;
                        if Instant::now() >= deadline {
                            let status = block_on(appender_ring.status());
                            panic!(
                                "record {seq} starved behind concurrent trims; \
                                 last rejection: {detail}; status: {status:?}"
                            );
                        }
                        thread::park_timeout(Duration::from_micros(200));
                    }
                }
            }
        }
        (rejections, appended_bytes)
    });

    let trimmer = thread::spawn(move || {
        let mut trims = 0_u64;
        while !trimmer_done.load(Ordering::Acquire) {
            if !saturated.load(Ordering::Acquire) {
                thread::park_timeout(Duration::from_micros(100));
                continue;
            }
            let status = block_on(trimmer_ring.status()).expect("status during concurrent churn");
            let payload_pressure = status.accepted_live_payload_bytes > (20 * 1_024);
            if payload_pressure || status.accepted_live_records > TRIM_THRESHOLD {
                // The checkpoint protocol: records must be durable before
                // they may be trimmed, and the trim itself must be
                // checkpointed before its physical space is reclaimed —
                // sync, trim, sync, the cadence the ring benchmark's
                // maintenance uses.
                let checkpoint =
                    block_on(trimmer_ring.sync()).expect("pre-trim checkpoint during churn");
                let durable = checkpoint.durable_tail.get();
                let before = RingCursor::new(durable.saturating_sub(KEEP_LIVE));
                let trimmed =
                    block_on(trimmer_ring.trim(before)).expect("trim during concurrent churn");
                assert!(
                    trimmed.accepted_head.get() >= before.get().min(durable),
                    "trim never moves the head backwards"
                );
                block_on(trimmer_ring.sync()).expect("reclaim checkpoint during churn");
                trims += 1;
                thread::park_timeout(Duration::from_micros(500));
            } else {
                thread::park_timeout(Duration::from_micros(100));
            }
        }
        trims
    });

    let (rejections, appended_bytes) = appender.join().expect("appender thread");
    done.store(true, Ordering::Release);
    let trims = trimmer.join().expect("trimmer thread");

    // Coverage gates: the run must actually have hit capacity, reclaimed
    // space, and wrapped the data area several times — otherwise this test
    // silently exercised nothing.
    assert!(rejections > 0, "no append was ever refused at capacity");
    assert!(trims > 0, "no trim ever reclaimed space");
    assert!(
        appended_bytes > 3 * data_capacity,
        "appends did not wrap the data area: {appended_bytes} bytes through {data_capacity}"
    );

    // Quiesce, then verify every retained record against its cursor.
    let synced = block_on(session.ring().sync()).expect("final sync");
    assert_eq!(synced.durable_tail, RingCursor::new(TOTAL_RECORDS));
    let verify = |ring: &FileRing<PooledUringFile>| {
        let status = block_on(ring.status()).expect("status after churn");
        let mut cursor = status.durable_head;
        while cursor < status.durable_tail {
            let page = block_on(ring.read(ReadRequest::new(cursor, 64, 64 * 1_024)))
                .expect("read retained records");
            assert!(
                !page.records.is_empty(),
                "an empty page before the durable tail at {cursor:?}"
            );
            for record in &page.records {
                cursor = RingCursor::new(cursor.get() + 1);
                assert_eq!(
                    record.buffer,
                    churn_payload(cursor.get()),
                    "record {cursor:?} corrupted by concurrent churn"
                );
            }
        }
        assert_eq!(cursor, status.durable_tail);
    };
    verify(session.ring());

    // The same state must recover through a brand-new pool session.
    let final_status = block_on(session.ring().status()).expect("final status");
    drop(session);
    let file_config = FileRingConfig {
        limits: RingLimits {
            max_record_bytes: 4_096,
            max_live_records: 64,
            max_live_payload_bytes: 32 * 1_024,
            max_read_records: 64,
            max_read_bytes: 64 * 1_024,
            max_batch_records: 16,
            max_batch_bytes: 16 * 1_024,
        },
        data_capacity_bytes: 64 * 1_024,
        ..FileRingConfig::default()
    };
    let recovered = PooledRing::start_with(&directory, file_config, 64 * 1_024, &Mode::Recover);
    let status = block_on(recovered.ring().status()).expect("recovered status");
    assert_eq!(status.durable_tail, final_status.durable_tail);
    assert_eq!(status.durable_head, final_status.durable_head);
    verify(recovered.ring());
}
