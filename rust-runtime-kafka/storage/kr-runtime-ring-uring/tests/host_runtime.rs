#![cfg(target_os = "linux")]

use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use kr_runtime::HostRuntime;
use kr_runtime_ring::{AppendRequest, ReadRequest, RingCursor, RingLimits, RingReader, RingWriter};
use kr_runtime_ring_uring::{UringRing, UringRingConfig};

static NEXT_DIRECTORY: AtomicU64 = AtomicU64::new(0);

struct TestDirectory(PathBuf);

impl TestDirectory {
    fn new() -> Self {
        let ordinal = NEXT_DIRECTORY.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "kr-runtime-ring-uring-host-runtime-{}-{ordinal}",
            std::process::id()
        ));
        fs::create_dir_all(&path).expect("create isolated host-runtime test directory");
        Self(path)
    }

    fn ring(&self) -> PathBuf {
        self.0.join("ring.dstr")
    }
}

impl Drop for TestDirectory {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

fn config() -> UringRingConfig {
    UringRingConfig {
        limits: RingLimits {
            max_record_bytes: 64,
            max_live_records: 8,
            max_live_payload_bytes: 256,
            max_read_records: 2,
            max_read_bytes: 128,
            max_batch_records: 2,
            max_batch_bytes: 128,
        },
        data_capacity_bytes: 4_096,
        max_io_request_bytes: 128,
        command_queue_capacity: 4,
        ring_entries: 4,
        max_io_chunk_bytes: 128,
        startup_timeout: Duration::from_secs(30),
        shutdown_timeout: Duration::from_secs(30),
    }
}

fn create(path: &Path) -> UringRing {
    UringRing::create(path, config()).expect("create io_uring ring")
}

#[test]
fn host_runtime_drives_ring_append_sync_and_read() {
    let directory = TestDirectory::new();
    let ring = create(&directory.ring());
    let mut runtime = HostRuntime::default();
    let payloads = vec![b"alpha".to_vec(), b"beta".to_vec()];

    let (appended, synced, read) = runtime
        .block_on(async {
            let appended = ring.append(AppendRequest::new(payloads.clone())).await;
            let synced = ring.sync().await;
            let read = ring.read(ReadRequest::new(RingCursor::START, 2, 128)).await;
            (appended, synced, read)
        })
        .expect("host runtime drives io_uring ring futures");

    assert_eq!(
        appended
            .expect("append records through host runtime")
            .next_cursor,
        RingCursor::new(2)
    );
    assert_eq!(
        synced
            .expect("sync records through host runtime")
            .durable_tail,
        RingCursor::new(2)
    );
    let read = read.expect("read records through host runtime");
    assert_eq!(
        read.records
            .iter()
            .map(|record| record.buffer.clone())
            .collect::<Vec<_>>(),
        payloads
    );
    assert_eq!(read.next_cursor, RingCursor::new(2));
    assert!(!read.has_more);

    ring.close().expect("close io_uring ring");
    runtime.finish().expect("host runtime finishes cleanly");
}
