#![cfg(target_os = "linux")]

use std::fs::{self, OpenOptions};
use std::future::Future;
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::pin::pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::task::{Context, Poll, Wake, Waker};
use std::thread::{self, Thread};
use std::time::{Duration, Instant};

use kr_runtime::CompletionCertainty;
use kr_runtime::{SimDuration, SimInstant};
use kr_runtime_ring::conformance::check_ring_contract;
use kr_runtime_ring::{AppendRequest, ReadRequest, RingCursor, RingLimits, RingReader, RingWriter};
use kr_runtime_ring_uring::{UringRing, UringRingConfig, UringRingOpenError};
use quarry::{
    AckOutcome, DurableQueue, QueueConfig, RecoveryConfig, RequestId, SubmitOutcome, SubmitRequest,
    WorkerId,
};

static NEXT_TEST_DIRECTORY: AtomicU64 = AtomicU64::new(0);
const OPERATION_TIMEOUT: Duration = Duration::from_secs(30);

struct ThreadWake(Thread);

impl Wake for ThreadWake {
    fn wake(self: Arc<Self>) {
        self.0.unpark();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.0.unpark();
    }
}

fn block_on<F>(future: F) -> F::Output
where
    F: Future,
{
    let waker = Waker::from(Arc::new(ThreadWake(thread::current())));
    let mut context = Context::from_waker(&waker);
    let mut future = pin!(future);
    let deadline = Instant::now() + OPERATION_TIMEOUT;
    loop {
        match future.as_mut().poll(&mut context) {
            Poll::Ready(output) => return output,
            Poll::Pending => {
                let remaining = deadline.saturating_duration_since(Instant::now());
                assert!(
                    !remaining.is_zero(),
                    "io_uring ring operation did not complete within {OPERATION_TIMEOUT:?}"
                );
                thread::park_timeout(remaining);
            }
        }
    }
}

struct TestDirectory {
    path: PathBuf,
}

impl TestDirectory {
    fn new(name: &str) -> Self {
        let ordinal = NEXT_TEST_DIRECTORY.fetch_add(1, Ordering::Relaxed);
        let base = std::env::var_os("KR_RUNTIME_RING_URING_TEST_DIR")
            .map_or_else(std::env::temp_dir, PathBuf::from);
        let path = base.join(format!(
            "kr-runtime-ring-uring-{name}-{}-{ordinal}",
            std::process::id()
        ));
        fs::create_dir_all(&path).expect("create isolated io_uring test directory");
        Self { path }
    }

    fn ring_path(&self) -> PathBuf {
        self.path.join("ring.dstr")
    }
}

impl Drop for TestDirectory {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.path);
    }
}

fn config(max_io_chunk_bytes: usize) -> UringRingConfig {
    UringRingConfig {
        limits: RingLimits {
            max_record_bytes: 256,
            max_live_records: 16,
            max_live_payload_bytes: 1_024,
            max_read_records: 2,
            max_read_bytes: 256,
            max_batch_records: 4,
            max_batch_bytes: 512,
        },
        data_capacity_bytes: 4_096,
        max_io_request_bytes: 256,
        command_queue_capacity: 8,
        ring_entries: 4,
        max_io_chunk_bytes,
        startup_timeout: OPERATION_TIMEOUT,
        shutdown_timeout: OPERATION_TIMEOUT,
    }
}

fn create(path: &Path, config: UringRingConfig) -> UringRing {
    UringRing::create(path, config).expect("create io_uring ring")
}

fn open(path: &Path, config: UringRingConfig) -> UringRing {
    UringRing::open(path, config).expect("open io_uring ring")
}

fn close(ring: UringRing) {
    ring.close().expect("close io_uring ring");
}

#[test]
fn shared_contract_runs_over_partial_kernel_io() {
    let directory = TestDirectory::new("conformance");
    let ring = create(&directory.ring_path(), config(3));

    block_on(check_ring_contract(&ring))
        .unwrap_or_else(|message| panic!("io_uring ring conformance failed: {message}"));
    let status = block_on(ring.status()).expect("read status after conformance");
    assert_eq!(status.durable_head, RingCursor::new(1));
    assert_eq!(status.durable_tail, RingCursor::new(4));
    assert!(status.physical.is_some());
    close(ring);
}

#[test]
fn synced_records_survive_close_and_reopen() {
    let directory = TestDirectory::new("reopen");
    let path = directory.ring_path();
    let config = config(7);
    let durable = vec![b"first".to_vec(), b"second".to_vec()];
    let unsynced = b"discard-on-reopen".to_vec();

    let ring = create(&path, config);
    assert_eq!(
        fs::metadata(&path).expect("stat created ring").len(),
        8_192 + config.data_capacity_bytes,
        "create did not install the exact v1 file length"
    );
    block_on(ring.append(AppendRequest::new(durable.clone()))).expect("append durable records");
    block_on(ring.sync()).expect("sync records");
    block_on(ring.append(AppendRequest::new(vec![unsynced]))).expect("append accepted suffix");
    close(ring);

    let reopened = open(&path, config);
    let page = block_on(reopened.read(ReadRequest::new(RingCursor::START, 2, 64)))
        .expect("read recovered records");
    assert_eq!(
        page.records
            .iter()
            .map(|record| record.buffer.clone())
            .collect::<Vec<_>>(),
        durable
    );
    assert_eq!(page.next_cursor, RingCursor::new(2));
    assert!(!page.has_more);
    close(reopened);
}

#[test]
fn durable_queue_submit_ack_and_recovery_use_the_real_ring() {
    let directory = TestDirectory::new("durable-queue");
    let path = directory.ring_path();
    let ring_config = config(3);
    let queue_config = QueueConfig {
        active_capacity: 4,
        max_payload_bytes: 64,
        max_claim_batch: 2,
        completed_history_capacity: 4,
    };
    let recovery = RecoveryConfig::new(2, 256, 32);
    let request = SubmitRequest {
        request_id: RequestId::new(7),
        payload: b"real-io-uring-job".to_vec(),
        not_before: SimInstant::ZERO,
    };

    let ring = create(&path, ring_config);
    let mut queue = block_on(DurableQueue::recover(queue_config, ring, recovery))
        .expect("initialize durable queue");
    assert_eq!(queue.incarnation(), 1);

    let submitted =
        block_on(queue.submit(request.clone(), SimInstant::ZERO)).expect("durably submit job");
    let job_id = submitted.job_id();
    assert_eq!(submitted, SubmitOutcome::Submitted { job_id });
    let leased = queue
        .claim(
            WorkerId::new(1),
            1,
            SimDuration::from_nanos(10),
            SimInstant::ZERO,
        )
        .expect("claim submitted job")
        .pop()
        .expect("one job was eligible");
    assert_eq!(
        block_on(queue.ack(job_id, leased.lease_token, SimInstant::ZERO))
            .expect("durably acknowledge job"),
        AckOutcome::Completed
    );
    close(queue.into_ring());

    let ring = open(&path, ring_config);
    let mut recovered = block_on(DurableQueue::recover(queue_config, ring, recovery))
        .expect("recover durable queue");
    assert_eq!(recovered.incarnation(), 2);
    assert_eq!(
        block_on(recovered.submit(request, SimInstant::ZERO))
            .expect("deduplicate recovered completed request"),
        SubmitOutcome::DuplicateCompleted { job_id }
    );
    let snapshot = recovered
        .snapshot(SimInstant::ZERO)
        .expect("snapshot recovered queue");
    assert!(snapshot.jobs.is_empty());
    assert_eq!(snapshot.completed.len(), 1);
    assert_eq!(snapshot.completed[0].job_id, job_id);
    assert_eq!(snapshot.completed[0].ack_token, leased.lease_token);
    close(recovered.into_ring());
}

#[test]
fn invalid_config_has_no_path_side_effect() {
    let directory = TestDirectory::new("invalid-config");
    let path = directory.ring_path();
    let invalid = UringRingConfig {
        ring_entries: 3,
        ..config(64)
    };

    let error = match UringRing::create(&path, invalid) {
        Ok(unexpected) => {
            close(unexpected);
            panic!("created a ring with an invalid kernel-ring shape");
        }
        Err(error) => error,
    };
    assert_eq!(error.certainty(), CompletionCertainty::NotApplied);
    assert!(matches!(
        error.error(),
        UringRingOpenError::InvalidConfig {
            field: "ring_entries",
            ..
        }
    ));
    assert!(!path.exists(), "invalid create touched the ring path");
}

#[test]
fn final_close_drains_admitted_append_and_sync() {
    let directory = TestDirectory::new("close-drain");
    let path = directory.ring_path();
    let config = config(7);
    let payload = b"drained-before-close".to_vec();
    let ring = create(&path, config);

    let append = ring.append(AppendRequest::new(vec![payload.clone()]));
    let sync = ring.sync();
    close(ring);

    let appended = block_on(append).expect("close drained admitted append");
    assert_eq!(appended.records, vec![payload.clone()]);
    let synced = block_on(sync).expect("close drained admitted sync");
    assert_eq!(synced.durable_tail, RingCursor::new(1));

    let reopened = open(&path, config);
    let page = block_on(reopened.read(ReadRequest::new(RingCursor::START, 1, 256)))
        .expect("read close-drained record after recovery");
    assert_eq!(page.records.len(), 1);
    assert_eq!(page.records[0].buffer, payload);
    close(reopened);
}

#[test]
fn final_clone_owns_lock_and_teardown() {
    let directory = TestDirectory::new("clone-lifetime");
    let path = directory.ring_path();
    let config = config(64);
    let first = create(&path, config);
    let last = first.clone();

    close(first);
    let locked = match UringRing::open(&path, config) {
        Ok(unexpected) => {
            close(unexpected);
            panic!("second session acquired the live ring lock");
        }
        Err(error) => error,
    };
    assert_eq!(locked.certainty(), CompletionCertainty::NotApplied);
    assert_eq!(*locked.error(), UringRingOpenError::AlreadyLocked);

    close(last);
    close(open(&path, config));
}

#[test]
fn opening_a_missing_ring_does_not_create_it() {
    let directory = TestDirectory::new("missing-open");
    let path = directory.ring_path();
    let error = match UringRing::open(&path, config(64)) {
        Ok(unexpected) => {
            close(unexpected);
            panic!("opened a missing ring");
        }
        Err(error) => error,
    };
    assert_eq!(error.certainty(), CompletionCertainty::NotApplied);
    assert!(matches!(error.error(), UringRingOpenError::Io { .. }));
    assert!(!path.exists(), "open created the missing ring path");
}

#[test]
fn create_never_truncates_an_existing_nonempty_file() {
    let directory = TestDirectory::new("nonempty-create");
    let path = directory.ring_path();
    let original = b"not-an-empty-ring";
    fs::write(&path, original).expect("write preexisting file");

    let error = match UringRing::create(&path, config(64)) {
        Ok(unexpected) => {
            close(unexpected);
            panic!("created a ring over a nonempty file");
        }
        Err(error) => error,
    };
    assert_eq!(error.certainty(), CompletionCertainty::NotApplied);
    assert!(matches!(error.error(), UringRingOpenError::Corrupt { .. }));
    assert_eq!(fs::read(&path).expect("reread existing file"), original);
}

#[test]
fn corrupt_sole_superblock_fails_closed_without_modification() {
    let directory = TestDirectory::new("corrupt-superblock");
    let path = directory.ring_path();
    let config = config(64);
    close(create(&path, config));

    let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(&path)
        .expect("open ring for corruption");
    let mut magic = [0_u8; 1];
    file.read_exact(&mut magic).expect("read superblock magic");
    magic[0] ^= 1;
    file.seek(SeekFrom::Start(0))
        .expect("seek to first superblock");
    file.write_all(&magic).expect("corrupt first superblock");
    file.sync_all().expect("sync test corruption");
    drop(file);
    let corrupt_bytes = fs::read(&path).expect("snapshot corrupt ring");

    let error = match UringRing::open(&path, config) {
        Ok(unexpected) => {
            close(unexpected);
            panic!("opened a ring with no valid superblock");
        }
        Err(error) => error,
    };
    assert_eq!(error.certainty(), CompletionCertainty::NotApplied);
    assert!(matches!(error.error(), UringRingOpenError::Corrupt { .. }));
    assert_eq!(
        fs::read(&path).expect("reread ring after failed recovery"),
        corrupt_bytes,
        "fail-closed recovery modified the corrupt ring"
    );
}
