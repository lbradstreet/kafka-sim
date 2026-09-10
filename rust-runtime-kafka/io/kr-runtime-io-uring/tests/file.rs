#![cfg(target_os = "linux")]

mod common;

use std::fs;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};

use std::future::poll_fn;
use std::task::Poll;

use common::block_on;
use kr_runtime_io::conformance::{check_cold_empty_file, check_empty_file};
use kr_runtime_io::{ColdFile, FileIoSubmit, ReadAtRequest, WriteAtRequest};
use kr_runtime_io_uring::{UringFile, UringFileConfig, UringFileOpenError};

static NEXT_DIRECTORY: AtomicU64 = AtomicU64::new(0);

struct TestDirectory(PathBuf);

impl TestDirectory {
    fn new(name: &str) -> Self {
        let ordinal = NEXT_DIRECTORY.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "kr-runtime-io-uring-{name}-{}-{ordinal}",
            std::process::id()
        ));
        fs::create_dir_all(&path).expect("create isolated test directory");
        Self(path)
    }

    fn file(&self) -> PathBuf {
        self.0.join("file.bin")
    }
}

impl Drop for TestDirectory {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

fn config(chunk: usize) -> UringFileConfig {
    UringFileConfig {
        max_read_bytes: 256,
        max_write_bytes: 256,
        max_file_bytes: 16 * 1024,
        command_queue_capacity: 8,
        ring_entries: 4,
        max_io_chunk_bytes: chunk,
    }
}

fn open_file(path: impl AsRef<std::path::Path>, config: UringFileConfig) -> UringFile {
    UringFile::open_with_outcome(path, config)
        .expect("open io_uring file")
        .into_parts()
        .0
}

#[test]
fn shared_file_contract_handles_tiny_cqe_transfers() {
    for chunk in [1, 3, 7] {
        let directory = TestDirectory::new(&format!("conformance-{chunk}"));
        let file = open_file(directory.file(), config(chunk));
        block_on(check_empty_file(file.clone()))
            .unwrap_or_else(|message| panic!("{chunk}-byte conformance failed: {message}"));
        drop(file);
    }
}

#[test]
fn shared_cold_file_contract_handles_tiny_cqe_transfers() {
    for chunk in [1, 3, 7] {
        let directory = TestDirectory::new(&format!("cold-conformance-{chunk}"));
        let file = open_file(directory.file(), config(chunk));
        block_on(check_cold_empty_file(ColdFile::new(file)))
            .unwrap_or_else(|message| panic!("{chunk}-byte cold conformance failed: {message}"));
    }
}

#[test]
fn cold_write_polled_once_then_dropped_is_fenced_by_sync() {
    let directory = TestDirectory::new("cold-drop-polled-write");
    let file = open_file(directory.file(), config(3));
    let cold = ColdFile::new(file);

    // One poll attempts admission whether or not the response is already
    // complete; dropping afterwards abandons only the observation.
    let synced = block_on(async {
        let mut future = Box::pin(cold.write_at(WriteAtRequest::new(0, b"abc".to_vec())));
        let _ = poll_fn(|context| Poll::Ready(future.as_mut().poll(context))).await;
        drop(future);
        cold.sync().await
    })
    .expect("sync fences the admitted cold write");
    assert_eq!(synced.durable_len, 3);

    let read = block_on(cold.read_at(ReadAtRequest::new(0, vec![0; 3])))
        .expect("read back the fenced cold write");
    assert_eq!(read.buffer, b"abc");
}

#[test]
fn dropping_the_last_capability_through_an_unpolled_cold_future_joins_the_actor() {
    let directory = TestDirectory::new("cold-unpolled-teardown");
    let file = open_file(directory.file(), config(3));
    let cold = ColdFile::new(file);

    let constructed = cold.len();
    drop(cold);
    // The unpolled future holds the final file capability; dropping it must
    // run the ordinary ingress-close-and-join teardown without hanging.
    drop(constructed);
}

#[test]
fn dropped_write_response_remains_before_sync() {
    let directory = TestDirectory::new("drop-write");
    let file = open_file(directory.file(), config(3));

    drop(file.submit_write_at(WriteAtRequest::new(0, b"abc".to_vec())));
    let synced = block_on(file.submit_sync()).expect("sync dropped write");
    assert_eq!(synced.durable_len, 3);
    let read = block_on(file.submit_read_at(ReadAtRequest::new(0, vec![0; 3])))
        .expect("read dropped write");
    assert_eq!(read.buffer, b"abc");
}

#[test]
fn queued_file_batches_preserve_conflicts_and_barriers() {
    let directory = TestDirectory::new("queued-ordering");
    let file = open_file(directory.file(), config(256));

    let first = file.submit_write_at(WriteAtRequest::new(0, b"1111".to_vec()));
    let overlapping = file.submit_write_at(WriteAtRequest::new(2, b"2222".to_vec()));
    block_on(first).expect("write first overlapping range");
    block_on(overlapping).expect("write later overlapping range");
    let contents = block_on(file.submit_read_at(ReadAtRequest::new(0, vec![0; 6])))
        .expect("read overlapping-write result");
    assert_eq!(contents.buffer, b"112222");

    block_on(file.submit_set_len(0)).expect("clear file before read/write ordering");
    let before_write = file.submit_read_at(ReadAtRequest::new(4, vec![0; 2]));
    let later_write = file.submit_write_at(WriteAtRequest::new(4, b"zz".to_vec()));
    assert!(
        block_on(before_write)
            .expect("read invoked before extending write")
            .buffer
            .is_empty()
    );
    block_on(later_write).expect("write after ordered read");

    block_on(file.submit_set_len(0)).expect("clear file before barrier ordering");
    let before_shrink = file.submit_write_at(WriteAtRequest::new(0, b"abcd".to_vec()));
    let shrink = file.submit_set_len(2);
    let after_shrink = file.submit_write_at(WriteAtRequest::new(4, b"z".to_vec()));
    block_on(before_shrink).expect("write before set-len barrier");
    block_on(shrink).expect("apply set-len barrier");
    block_on(after_shrink).expect("write after set-len barrier");
    let contents = block_on(file.submit_read_at(ReadAtRequest::new(0, vec![0; 5])))
        .expect("read set-len ordering result");
    assert_eq!(contents.buffer, b"ab\0\0z");
}

#[test]
fn every_clone_retains_the_exclusive_session_lock() {
    let directory = TestDirectory::new("lock");
    let path = directory.file();
    let first = open_file(&path, config(3));
    let clone = first.clone();
    let locked = UringFile::open_with_outcome(&path, config(3))
        .err()
        .expect("second session must fail");
    assert_eq!(locked.error(), &UringFileOpenError::AlreadyLocked);

    drop(first);
    let locked = UringFile::open_with_outcome(&path, config(3))
        .err()
        .expect("clone must retain the lock");
    assert_eq!(locked.error(), &UringFileOpenError::AlreadyLocked);
    drop(clone);

    let reopened = open_file(&path, config(3));
    drop(reopened);
}

#[test]
fn tracked_open_distinguishes_created_and_existing_paths() {
    let directory = TestDirectory::new("open-outcome");
    let path = directory.file();

    let (created, created_path) = UringFile::open_with_outcome(&path, config(3))
        .expect("create tracked file")
        .into_parts();
    assert!(created_path);
    drop(created);

    let (reopened, created_path) = UringFile::open_with_outcome(&path, config(3))
        .expect("reopen tracked file")
        .into_parts();
    assert!(!created_path);
    drop(reopened);
}
