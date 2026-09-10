//! Barrier round trips across many loopback connections.
//!
//! Each iteration submits one write on every connection before awaiting
//! any, then one read on every connection before awaiting any, so all
//! connections have work in flight together while each individual
//! connection stays at queue depth one. Every connection echoes through
//! its own std-blocking peer thread with `TCP_NODELAY` set; the backends
//! differ only in how the measured side is driven:
//!
//! - `uring_stream`: three threads and a private ring per connection;
//! - `pooled_stream`: one coordinator and one shared ring for all of them.
//!
//! The per-stream backend stops at 64 connections — beyond that its thread
//! and descriptor cost approaches common host limits, which is the wall the
//! pool exists to remove — while the pool is also measured at 256. Setup,
//! registration, peer threads, and buffers stay outside measured time.

use std::future::Future;
use std::io::{Read, Write};
use std::net::{Ipv4Addr, SocketAddr, TcpListener, TcpStream};
use std::pin::pin;
use std::sync::Arc;
use std::task::{Context, Poll, Waker};
use std::thread::{self, JoinHandle, Thread};
use std::time::{Duration, Instant};

use criterion::measurement::WallTime;
use criterion::{BenchmarkGroup, BenchmarkId, Criterion, SamplingMode, Throughput};
use kr_runtime_io::network::{ByteStreamSubmit, ReadRequest, WriteRequest};
use kr_runtime_io_uring::{UringByteStream, UringNetPool, UringNetPoolConfig, UringNetworkConfig};

const RING_ENTRIES: u32 = 8;
const MAX_OPERATION_BYTES: usize = 256 * 1_024;
const MAX_IO_CHUNK_BYTES: usize = 64 * 1_024;
const SIZE: usize = 4 * 1_024;

const SAMPLE_SIZE: usize = 20;
const WARM_UP_TIME: Duration = Duration::from_secs(1);
const MEASUREMENT_TIME: Duration = Duration::from_secs(3);

/// Connection counts served by both backends.
const SHARED_COUNTS: [usize; 2] = [8, 64];
/// Connection counts served by the pool alone.
const POOLED_ONLY_COUNTS: [usize; 1] = [256];

struct ThreadWake(Thread);

impl std::task::Wake for ThreadWake {
    fn wake(self: Arc<Self>) {
        self.0.unpark();
    }
}

fn block_on<T>(waker: &Waker, future: impl Future<Output = T>) -> T {
    let mut future = pin!(future);
    let mut context = Context::from_waker(waker);
    loop {
        match future.as_mut().poll(&mut context) {
            Poll::Ready(output) => return output,
            Poll::Pending => thread::park(),
        }
    }
}

fn payload(size: usize) -> Vec<u8> {
    (0..size).map(|index| (index % 251) as u8).collect()
}

fn stream_config() -> UringNetworkConfig {
    UringNetworkConfig {
        command_queue_capacity: 64,
        ring_entries: RING_ENTRIES,
        max_operation_bytes: MAX_OPERATION_BYTES,
        max_io_chunk_bytes: MAX_IO_CHUNK_BYTES,
        connect_timeout: Duration::from_secs(10),
    }
}

fn pool_config(connections: usize) -> UringNetPoolConfig {
    UringNetPoolConfig {
        max_streams: connections,
        command_queue_capacity: 64,
        ring_entries: RING_ENTRIES,
        max_operation_bytes: MAX_OPERATION_BYTES,
        max_io_chunk_bytes: MAX_IO_CHUNK_BYTES,
        max_listeners: 4,
        max_listener_backlog: 1_024,
        connect_timeout: Duration::from_secs(10),
    }
}

struct EchoPeer {
    join: Option<JoinHandle<()>>,
}

impl EchoPeer {
    fn start(mut socket: TcpStream) -> Self {
        let join = thread::Builder::new()
            .name("stream-bench-echo".to_owned())
            .spawn(move || {
                let mut buffer = vec![0_u8; SIZE];
                loop {
                    if socket.read_exact(&mut buffer).is_err() {
                        return;
                    }
                    if socket.write_all(&buffer).is_err() {
                        return;
                    }
                }
            })
            .expect("spawn echo peer");
        Self { join: Some(join) }
    }
}

impl Drop for EchoPeer {
    fn drop(&mut self) {
        if let Some(join) = self.join.take() {
            let _ = join.join();
        }
    }
}

/// One measured connection: its stream handle and reusable buffers.
struct Lane<S> {
    stream: S,
    send: Option<Vec<u8>>,
    receive: Option<Vec<u8>>,
}

/// Every lane, its peers, and whatever owns the handles (the pool), dropped
/// in field order so streams close before peers are joined.
struct Fixture<S> {
    lanes: Vec<Lane<S>>,
    _holder: Option<UringNetPool>,
    _peers: Vec<EchoPeer>,
}

fn connected_sockets(connections: usize) -> (Vec<TcpStream>, Vec<EchoPeer>) {
    let listener = TcpListener::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)))
        .expect("bind loopback listener");
    let address = listener.local_addr().expect("read listener address");
    let mut sockets = Vec::with_capacity(connections);
    let mut peers = Vec::with_capacity(connections);
    for _ in 0..connections {
        let client = TcpStream::connect(address).expect("connect loopback client");
        let (server, _) = listener.accept().expect("accept loopback client");
        client.set_nodelay(true).expect("disable client Nagle");
        server.set_nodelay(true).expect("disable server Nagle");
        sockets.push(client);
        peers.push(EchoPeer::start(server));
    }
    (sockets, peers)
}

fn lanes_from<S>(sockets: Vec<TcpStream>, wrap: impl Fn(TcpStream) -> S) -> Vec<Lane<S>> {
    sockets
        .into_iter()
        .map(|socket| Lane {
            stream: wrap(socket),
            send: Some(payload(SIZE)),
            receive: Some(Vec::with_capacity(SIZE)),
        })
        .collect()
}

fn uring_fixture(connections: usize) -> Fixture<UringByteStream> {
    let (sockets, peers) = connected_sockets(connections);
    Fixture {
        lanes: lanes_from(sockets, |socket| {
            UringByteStream::from_tcp_stream(socket, stream_config())
                .expect("wrap per-stream backend")
        }),
        _holder: None,
        _peers: peers,
    }
}

fn pooled_fixture(connections: usize) -> Fixture<kr_runtime_io_uring::PooledUringStream> {
    let (sockets, peers) = connected_sockets(connections);
    let pool = UringNetPool::new(pool_config(connections)).expect("create stream pool");
    let lanes = lanes_from(sockets, |socket| {
        pool.register_stream(socket)
            .expect("register pooled backend")
    });
    Fixture {
        lanes,
        _holder: Some(pool),
        _peers: peers,
    }
}

/// One barrier round trip: every lane's write submitted before any is
/// awaited, then every lane's read submitted before any is awaited, with a
/// short per-lane completion loop for the rare partial transfer.
fn barrier_round_trip<S: ByteStreamSubmit>(waker: &Waker, lanes: &mut [Lane<S>]) {
    let mut writes = Vec::with_capacity(lanes.len());
    for lane in lanes.iter_mut() {
        let buffer = lane.send.take().expect("send buffer is available");
        writes.push(lane.stream.submit_write(WriteRequest { buffer }));
    }
    for (lane, response) in lanes.iter_mut().zip(writes) {
        let mut result = block_on(waker, response).expect("benchmark write");
        let mut written = result.bytes_written;
        while written < SIZE {
            let tail = result.buffer[result.bytes_written..].to_vec();
            result = block_on(
                waker,
                lane.stream.submit_write(WriteRequest { buffer: tail }),
            )
            .expect("benchmark write tail");
            written += result.bytes_written;
        }
        lane.send = Some(if result.buffer.len() == SIZE {
            result.buffer
        } else {
            // A partial-write tail replaced the original buffer; rebuild
            // the full payload off the rare path.
            payload(SIZE)
        });
    }
    let mut reads = Vec::with_capacity(lanes.len());
    for lane in lanes.iter_mut() {
        let mut buffer = lane.receive.take().expect("receive buffer is available");
        buffer.clear();
        reads.push(lane.stream.submit_read(ReadRequest {
            buffer,
            max_bytes: SIZE,
        }));
    }
    for (lane, response) in lanes.iter_mut().zip(reads) {
        let mut result = block_on(waker, response).expect("benchmark read");
        while result.buffer.len() < SIZE {
            assert!(result.bytes_read > 0, "echo stream ended early");
            let missing = SIZE - result.buffer.len();
            result = block_on(
                waker,
                lane.stream.submit_read(ReadRequest {
                    buffer: result.buffer,
                    max_bytes: missing,
                }),
            )
            .expect("benchmark read tail");
        }
        lane.receive = Some(result.buffer);
    }
}

fn register_backend<S, F>(
    group: &mut BenchmarkGroup<'_, WallTime>,
    backend_name: &'static str,
    counts: &[usize],
    fixture: F,
) where
    S: ByteStreamSubmit,
    F: Copy + Fn(usize) -> Fixture<S> + 'static,
{
    for &connections in counts {
        group.throughput(Throughput::Elements(connections as u64));
        group.bench_function(
            BenchmarkId::new(backend_name, format!("{connections}conns")),
            move |bencher| {
                let mut fixture = fixture(connections);
                let waker = Waker::from(Arc::new(ThreadWake(thread::current())));

                // Prove the fixture before measuring it.
                barrier_round_trip(&waker, &mut fixture.lanes);
                for lane in &fixture.lanes {
                    assert_eq!(
                        lane.receive.as_deref().expect("probed receive buffer"),
                        lane.send.as_deref().expect("probed send buffer"),
                        "probe round trip echoed every lane"
                    );
                }

                bencher.iter_custom(|iterations| {
                    let started = Instant::now();
                    for _ in 0..iterations {
                        barrier_round_trip(&waker, &mut fixture.lanes);
                    }
                    started.elapsed()
                });
            },
        );
    }
}

pub(crate) fn many_connections_benchmarks(criterion: &mut Criterion) {
    let mut group = criterion.benchmark_group("io_uring_stream_many/echo_barrier_4KiB");
    group
        .sample_size(SAMPLE_SIZE)
        .warm_up_time(WARM_UP_TIME)
        .measurement_time(MEASUREMENT_TIME)
        .sampling_mode(SamplingMode::Flat);
    register_backend(&mut group, "uring_stream", &SHARED_COUNTS, uring_fixture);
    register_backend(&mut group, "pooled_stream", &SHARED_COUNTS, pooled_fixture);
    register_backend(
        &mut group,
        "pooled_stream",
        &POOLED_ONLY_COUNTS,
        pooled_fixture,
    );
    group.finish();
}
