#![cfg(target_os = "linux")]

use std::fs;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};

use kr_runtime::{HostConfig, HostRuntime};
use kr_runtime_io::network::{ColdStream, ReadRequest, WriteRequest};
use kr_runtime_io::{ColdFile, FileIoSubmit, ReadAtRequest, WriteAtRequest};
use kr_runtime_io_uring::{
    UringEnv, UringFile, UringFileConfig, UringIoPool, UringNetPool, UringNetPoolConfig,
    UringPoolConfig,
};

static NEXT_DIRECTORY: AtomicU64 = AtomicU64::new(0);

struct TestDirectory(PathBuf);

impl TestDirectory {
    fn new() -> Self {
        let ordinal = NEXT_DIRECTORY.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "kr-runtime-io-uring-host-runtime-{}-{ordinal}",
            std::process::id()
        ));
        fs::create_dir_all(&path).expect("create isolated host-runtime test directory");
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

fn config() -> UringFileConfig {
    UringFileConfig {
        max_read_bytes: 64,
        max_write_bytes: 64,
        max_file_bytes: 4 * 1024,
        command_queue_capacity: 4,
        ring_entries: 4,
        max_io_chunk_bytes: 64,
    }
}

#[test]
fn host_runtime_drives_file_write_sync_and_read() {
    let directory = TestDirectory::new();
    let file = UringFile::open_with_outcome(directory.file(), config())
        .expect("open io_uring file")
        .into_parts()
        .0;
    let mut runtime = HostRuntime::default();
    let payload = b"host-runtime-file".to_vec();

    let (written, synced, read) = runtime
        .block_on(async {
            let written = file
                .submit_write_at(WriteAtRequest::new(0, payload.clone()))
                .await;
            let synced = file.submit_sync().await;
            let read = file
                .submit_read_at(ReadAtRequest::new(0, vec![0; payload.len()]))
                .await;
            (written, synced, read)
        })
        .expect("host runtime drives io_uring file futures");

    let written = written.expect("write file through host runtime");
    assert_eq!(written.bytes_written, payload.len());
    assert_eq!(written.buffer, payload);
    assert_eq!(
        synced.expect("sync file through host runtime").durable_len,
        payload.len() as u64
    );
    assert_eq!(
        read.expect("read file through host runtime").buffer,
        payload
    );

    drop(file);
    runtime.finish().expect("host runtime finishes cleanly");
}

#[test]
fn host_runtime_composes_pooled_providers_on_its_blocking_workers() {
    // The blessed host composition: one runtime whose blocking capability
    // backs the file pool's environment, one file pool, one stream pool,
    // application code on cold handles. Total provider threads for any
    // number of files and connections: two reactors, two coordinators, and
    // the runtime's blocking workers — no per-handle fleets.
    let directory = TestDirectory::new();
    let mut runtime = HostRuntime::new(HostConfig {
        blocking_workers: 1,
        ..HostConfig::default()
    })
    .expect("create host runtime");
    let env = UringEnv::on_runtime(
        runtime
            .blocking()
            .expect("provision runtime blocking workers"),
    );

    let file_pool =
        UringIoPool::with_env(UringPoolConfig::default(), &env).expect("create file pool");
    let net_pool = UringNetPool::new(UringNetPoolConfig::default()).expect("create stream pool");

    let backing = fs::File::options()
        .read(true)
        .write(true)
        .create(true)
        .truncate(true)
        .open(directory.file())
        .expect("open backing file");
    let file = ColdFile::new(
        file_pool
            .register_file(backing)
            .expect("register pooled file"),
    );

    let listener =
        std::net::TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, 0)).expect("bind loopback");
    let address = listener.local_addr().expect("listener address");
    let client_socket = std::net::TcpStream::connect(address).expect("connect loopback");
    let (server_socket, _) = listener.accept().expect("accept loopback");
    let client = ColdStream::new(
        net_pool
            .register_stream(client_socket)
            .expect("register client stream"),
    );
    let server = ColdStream::new(
        net_pool
            .register_stream(server_socket)
            .expect("register server stream"),
    );

    let payload = b"pooled-composition".to_vec();
    let expected = payload.clone();
    let (resized, durable, echoed) = runtime
        .block_on(async move {
            // set_len runs on the runtime-provisioned blocking worker; the
            // transfers run on the pools' shared rings.
            let resized = file.set_len(64).await.expect("set_len completes");
            let written = file
                .write_at(WriteAtRequest::new(0, payload.clone()))
                .await
                .expect("file write completes");
            assert_eq!(written.bytes_written, payload.len());
            let durable = file.sync().await.expect("sync completes");

            let sent = client
                .write(WriteRequest {
                    buffer: payload.clone(),
                })
                .await
                .expect("stream write completes");
            assert_eq!(sent.bytes_written, payload.len());
            let mut received = Vec::new();
            while received.len() < payload.len() {
                let result = server
                    .read(ReadRequest {
                        buffer: received,
                        max_bytes: payload.len(),
                    })
                    .await
                    .expect("stream read completes");
                assert!(result.bytes_read > 0, "peer stream ended early");
                received = result.buffer;
            }
            client.close().await.expect("close client");
            server.close().await.expect("close server");
            (resized, durable, received)
        })
        .expect("host runtime drives the pooled composition");

    assert_eq!(resized.len, 64);
    assert_eq!(
        durable.durable_len, 64,
        "sync observed the truncated length"
    );
    assert_eq!(echoed, expected);

    runtime.finish().expect("host runtime finishes cleanly");
    // The environment and pools outlive the runtime by design; teardown
    // drains in tenant order.
    drop((file_pool, net_pool));
    drop(env);
}
