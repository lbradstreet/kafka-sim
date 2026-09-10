#![cfg(target_os = "linux")]

mod common;

use std::fs::{self, File};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};

use common::block_on;
use kr_runtime_io::conformance::{check_cold_empty_file, check_empty_file};
use kr_runtime_io::{ColdFile, FileIoSubmit, ReadAtRequest, WriteAtRequest};
use kr_runtime_io_uring::{
    PooledUringFile, UringEnv, UringEnvConfig, UringIoPool, UringPoolConfig,
};

static NEXT_DIRECTORY: AtomicU64 = AtomicU64::new(0);

struct TestDirectory(PathBuf);

impl TestDirectory {
    fn new(name: &str) -> Self {
        let ordinal = NEXT_DIRECTORY.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "kr-runtime-io-uring-pooled-{name}-{}-{ordinal}",
            std::process::id()
        ));
        fs::create_dir_all(&path).expect("create isolated test directory");
        Self(path)
    }

    fn file(&self, name: &str) -> PathBuf {
        self.0.join(name)
    }
}

impl Drop for TestDirectory {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

fn config(chunk: usize) -> UringPoolConfig {
    UringPoolConfig {
        max_read_bytes: 256,
        max_write_bytes: 256,
        max_file_bytes: 16 * 1024,
        file_queue_capacity: 8,
        ring_entries: 4,
        max_in_flight: 4,
        blocking_threads: 1,
        max_io_chunk_bytes: chunk,
    }
}

fn register(pool: &UringIoPool, path: PathBuf) -> PooledUringFile {
    let file = File::options()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(path)
        .expect("open backing file");
    pool.register_file(file).expect("register pooled file")
}

#[test]
fn pooled_file_contract_handles_tiny_cqe_transfers() {
    for chunk in [1, 3, 7] {
        let directory = TestDirectory::new(&format!("conformance-{chunk}"));
        let pool = UringIoPool::new(config(chunk)).expect("create pool");
        let file = register(&pool, directory.file("file.bin"));
        block_on(check_empty_file(file.clone()))
            .unwrap_or_else(|message| panic!("{chunk}-byte conformance failed: {message}"));
        drop(file);
    }
}

#[test]
fn pooled_cold_file_contract_handles_tiny_cqe_transfers() {
    for chunk in [1, 3, 7] {
        let directory = TestDirectory::new(&format!("cold-conformance-{chunk}"));
        let pool = UringIoPool::new(config(chunk)).expect("create pool");
        let file = register(&pool, directory.file("file.bin"));
        block_on(check_cold_empty_file(ColdFile::new(file)))
            .unwrap_or_else(|message| panic!("{chunk}-byte cold conformance failed: {message}"));
    }
}

#[test]
fn two_pools_share_one_blocking_environment() {
    // One single-threaded environment serves both pools' ftruncate traffic:
    // every set_len completing exactly proves the shared worker drained
    // both tenants' jobs, and the interleaved writes prove data-plane
    // operations never depended on the environment. Conformance over a
    // shared environment then proves with_env changes no contract.
    let directory = TestDirectory::new("shared-env");
    let env = UringEnv::new(UringEnvConfig {
        blocking_threads: 1,
    })
    .expect("create shared environment");
    let first_pool = UringIoPool::with_env(config(64), &env).expect("create first pool");
    let second_pool = UringIoPool::with_env(config(64), &env).expect("create second pool");
    let first = register(&first_pool, directory.file("first.bin"));
    let second = register(&second_pool, directory.file("second.bin"));

    for round in 1..=8u64 {
        let first_len = first.submit_set_len(round * 16);
        let second_len = second.submit_set_len(round * 24);
        let first_write = first.submit_write_at(WriteAtRequest::new(0, vec![round as u8; 8]));
        let second_write = second.submit_write_at(WriteAtRequest::new(0, vec![round as u8; 8]));
        assert_eq!(
            block_on(first_len)
                .expect("first pool set_len completes")
                .len,
            round * 16,
            "round {round}"
        );
        assert_eq!(
            block_on(second_len)
                .expect("second pool set_len completes")
                .len,
            round * 24,
            "round {round}"
        );
        block_on(first_write).expect("first pool write completes");
        block_on(second_write).expect("second pool write completes");
    }

    let conformance_file = register(&first_pool, directory.file("conformance.bin"));
    block_on(check_empty_file(conformance_file))
        .unwrap_or_else(|message| panic!("shared-environment conformance failed: {message}"));

    drop((first, second, first_pool, second_pool));
    // The environment outlives its tenants and stops cleanly after them.
    drop(env);
}

#[test]
fn a_runtime_provisioned_environment_serves_the_pool_past_runtime_drop() {
    // The environment's workers come from the host runtime's blocking
    // capability, whose lifetime follows its clones rather than the
    // runtime: dropping the runtime first must change nothing for the
    // pool's ftruncate traffic, and conformance over the runtime-backed
    // path must match the environment-owned backing exactly.
    let directory = TestDirectory::new("runtime-env");
    let runtime = kr_runtime::HostRuntime::new(kr_runtime::HostConfig {
        blocking_workers: 1,
        ..kr_runtime::HostConfig::default()
    })
    .expect("create host runtime");
    let env = UringEnv::on_runtime(runtime.blocking().expect("provision blocking capability"));
    drop(runtime);

    let pool = UringIoPool::with_env(config(64), &env).expect("create pool");
    let file = register(&pool, directory.file("file.bin"));
    for round in 1..=4u64 {
        let resized = block_on(file.submit_set_len(round * 32)).expect("set_len completes");
        assert_eq!(resized.len, round * 32, "round {round}");
    }
    block_on(check_empty_file(register(
        &pool,
        directory.file("conformance.bin"),
    )))
    .unwrap_or_else(|message| panic!("runtime-backed conformance failed: {message}"));

    drop((file, pool));
    drop(env);
}

#[test]
fn many_files_share_one_ring_past_its_in_flight_budget() {
    // Twice as many concurrent operations as the ring budget admits, so the
    // waiter path — a file parked until a completion frees budget — must be
    // taken for the test to complete at all.
    let directory = TestDirectory::new("shared-budget");
    let pool = UringIoPool::new(config(64)).expect("create pool");
    let files: Vec<PooledUringFile> = (0..8)
        .map(|index| register(&pool, directory.file(&format!("file-{index}.bin"))))
        .collect();

    let writes: Vec<_> = files
        .iter()
        .enumerate()
        .map(|(index, file)| file.submit_write_at(WriteAtRequest::new(0, vec![index as u8; 64])))
        .collect();
    for (index, write) in writes.into_iter().enumerate() {
        let success = block_on(write).expect("write completes");
        assert_eq!(success.bytes_written, 64, "file {index} wrote fully");
    }

    let reads: Vec<_> = files
        .iter()
        .map(|file| file.submit_read_at(ReadAtRequest::new(0, vec![0; 64])))
        .collect();
    for (index, read) in reads.into_iter().enumerate() {
        let success = block_on(read).expect("read completes");
        assert_eq!(
            success.buffer,
            vec![index as u8; 64],
            "file {index} read back its own contents"
        );
    }
}

#[test]
fn concurrent_syncs_on_separate_files_share_the_minimum_budget() {
    // Two files whose syncs each need both of the two-slot ring budget, so
    // one sync must wait while the other's fsync-statx pair is in flight.
    // The coordinator once spun forever when the pair's first completion
    // freed one slot and the parked sync retried for two, so completing at
    // all is the regression assertion; iterated to give the interleaving
    // many chances to arise.
    let directory = TestDirectory::new("sync-budget");
    let mut tight = config(64);
    tight.max_in_flight = 2;
    let pool = UringIoPool::new(tight).expect("create pool");
    let first = register(&pool, directory.file("first.bin"));
    let second = register(&pool, directory.file("second.bin"));

    for round in 0..50u8 {
        let first_write = first.submit_write_at(WriteAtRequest::new(0, vec![round; 8]));
        let second_write = second.submit_write_at(WriteAtRequest::new(0, vec![round; 8]));
        let first_sync = first.submit_sync();
        let second_sync = second.submit_sync();
        block_on(first_write).expect("first write completes");
        block_on(second_write).expect("second write completes");
        let first_durable = block_on(first_sync).expect("first sync completes");
        assert_eq!(first_durable.durable_len, 8, "round {round}");
        let second_durable = block_on(second_sync).expect("second sync completes");
        assert_eq!(second_durable.durable_len, 8, "round {round}");
    }
}

#[test]
fn overlapping_writes_apply_in_admission_order() {
    // Non-overlapping writes pipeline concurrently; an overlapping write is
    // a batch fence that may not start until every earlier write's response
    // has been delivered. A chain where each write overlaps its predecessor
    // therefore layers strictly in admission order.
    let directory = TestDirectory::new("overlap");
    let pool = UringIoPool::new(config(64)).expect("create pool");
    let file = register(&pool, directory.file("file.bin"));

    let first = file.submit_write_at(WriteAtRequest::new(0, vec![b'a'; 8]));
    let second = file.submit_write_at(WriteAtRequest::new(4, vec![b'b'; 8]));
    let third = file.submit_write_at(WriteAtRequest::new(8, vec![b'c'; 8]));
    for write in [first, second, third] {
        let success = block_on(write).expect("write completes");
        assert_eq!(success.bytes_written, 8);
    }

    let read =
        block_on(file.submit_read_at(ReadAtRequest::new(0, vec![0; 16]))).expect("read completes");
    let mut expected = Vec::new();
    expected.extend_from_slice(b"aaaa");
    expected.extend_from_slice(b"bbbb");
    expected.extend_from_slice(b"cccccccc");
    assert_eq!(read.buffer, expected, "writes layered in admission order");
}

#[test]
fn a_read_admitted_behind_a_write_observes_that_write() {
    // Reads and writes never share the in-flight pipeline, so a read
    // submitted after a write must observe its effect even when both are
    // submitted before either is awaited. Iterated to give an ordering
    // regression many chances to interleave.
    let directory = TestDirectory::new("read-after-write");
    let pool = UringIoPool::new(config(64)).expect("create pool");
    let file = register(&pool, directory.file("file.bin"));

    for round in 0..100u8 {
        let write = file.submit_write_at(WriteAtRequest::new(0, vec![round; 16]));
        let read = file.submit_read_at(ReadAtRequest::new(0, vec![0; 16]));
        block_on(write).expect("write completes");
        let success = block_on(read).expect("read completes");
        assert_eq!(
            success.buffer,
            vec![round; 16],
            "round {round} read overtook its preceding write"
        );
    }
}

#[test]
fn a_burst_past_the_per_file_bound_rejects_cleanly_and_terminalizes() {
    let directory = TestDirectory::new("admission");
    let mut small = config(64);
    small.file_queue_capacity = 2;
    let pool = UringIoPool::new(small).expect("create pool");
    let file = register(&pool, directory.file("file.bin"));

    // The coordinator drains concurrently, so which submissions reject is
    // timing-dependent; what must hold is that every response terminalizes
    // and every rejection carries exactly the bounded-admission shape with
    // its buffer returned.
    let responses: Vec<_> = (0..32)
        .map(|index| file.submit_write_at(WriteAtRequest::new(0, vec![index as u8; 8])))
        .collect();
    let mut completed = 0;
    for response in responses {
        match block_on(response) {
            Ok(success) => {
                assert_eq!(success.bytes_written, 8);
                completed += 1;
            }
            Err(error) => {
                assert_eq!(
                    error.certainty(),
                    kr_runtime::CompletionCertainty::NotApplied,
                    "an admission rejection precedes any effect"
                );
                assert_eq!(error.error().buffer.len(), 8, "the buffer came back");
                assert_eq!(
                    error.error().error,
                    kr_runtime_io::StorageError::ResourceExhausted {
                        resource: "queued file commands",
                        limit: 2,
                    }
                );
            }
        }
    }
    assert!(completed > 0, "no submission ever completed");
}
