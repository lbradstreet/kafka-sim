//! One-connection loopback echo round trips at queue depth one.
//!
//! Every case performs the identical logical operation — send `size` bytes,
//! then read the peer's `size`-byte echo — against the same std-blocking
//! echo peer thread, so the backends differ only in how the measured side
//! submits its I/O:
//!
//! - `std_blocking`: `write_all` and `read_exact` syscalls on a blocking
//!   socket, the loopback floor with no ring, actors, or owned futures;
//! - `uring_stream`: `UringByteStream`, whose read and write actors block
//!   per operation on a private ring;
//! - `pooled_stream`: `PooledUringStream`, whose operations run as routed
//!   sustained SQEs on the shared pool ring.
//!
//! All sockets set `TCP_NODELAY`, payloads fit one I/O chunk, and setup —
//! connections, registration, peer threads, buffers — stays outside
//! measured time.

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

const SAMPLE_SIZE: usize = 20;
const WARM_UP_TIME: Duration = Duration::from_secs(1);
const MEASUREMENT_TIME: Duration = Duration::from_secs(3);

const SIZES: [usize; 2] = [64, 4 * 1_024];

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

fn pool_config() -> UringNetPoolConfig {
    UringNetPoolConfig {
        max_streams: 4,
        command_queue_capacity: 64,
        ring_entries: RING_ENTRIES,
        max_operation_bytes: MAX_OPERATION_BYTES,
        max_io_chunk_bytes: MAX_IO_CHUNK_BYTES,
        max_listeners: 4,
        max_listener_backlog: 64,
        connect_timeout: Duration::from_secs(10),
    }
}

/// One connected loopback pair: the measured socket plus its echo peer,
/// which reads exactly `size` bytes and writes them back until the measured
/// side closes.
struct EchoPeer {
    join: Option<JoinHandle<()>>,
}

impl EchoPeer {
    fn start(mut socket: TcpStream, size: usize) -> Self {
        let join = thread::Builder::new()
            .name("stream-bench-echo".to_owned())
            .spawn(move || {
                let mut buffer = vec![0_u8; size];
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
        // The measured socket is gone by now, so the peer observed EOF.
        if let Some(join) = self.join.take() {
            let _ = join.join();
        }
    }
}

/// Returns the peer first: bindings drop in reverse order, so the measured
/// socket (or the stream wrapping it) must be introduced after the peer for
/// its close to precede — and release — the peer join.
fn connected_pair(size: usize) -> (EchoPeer, TcpStream) {
    let listener = TcpListener::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)))
        .expect("bind loopback listener");
    let address = listener.local_addr().expect("read listener address");
    let client = TcpStream::connect(address).expect("connect loopback client");
    let (server, _) = listener.accept().expect("accept loopback client");
    client.set_nodelay(true).expect("disable client Nagle");
    server.set_nodelay(true).expect("disable server Nagle");
    (EchoPeer::start(server, size), client)
}

/// Sends `payload` fully, then reads exactly its length back into `scratch`.
fn submit_round_trip<S: ByteStreamSubmit>(
    waker: &Waker,
    stream: &S,
    mut payload: Vec<u8>,
    mut scratch: Vec<u8>,
    size: usize,
) -> (Vec<u8>, Vec<u8>) {
    let mut written = 0;
    loop {
        let result = block_on(waker, stream.submit_write(WriteRequest { buffer: payload }))
            .expect("benchmark write");
        written += result.bytes_written;
        if written == size {
            payload = if result.buffer.len() == size {
                result.buffer
            } else {
                // A partial-write tail replaced the original buffer;
                // rebuild the full payload off the rare path.
                self::payload(size)
            };
            break;
        }
        // Partial socket writes are possible in principle; keep the round
        // trip honest without optimizing a path the steady state never takes.
        payload = result.buffer[result.bytes_written..].to_vec();
    }
    scratch.clear();
    let mut buffer = scratch;
    while buffer.len() < size {
        let missing = size - buffer.len();
        let result = block_on(
            waker,
            stream.submit_read(ReadRequest {
                buffer,
                max_bytes: missing,
            }),
        )
        .expect("benchmark read");
        assert!(result.bytes_read > 0, "echo stream ended early");
        buffer = result.buffer;
    }
    (payload, buffer)
}

fn blocking_round_trip(socket: &mut TcpStream, payload: &[u8], scratch: &mut [u8]) {
    socket.write_all(payload).expect("benchmark write");
    socket.read_exact(scratch).expect("benchmark read");
}

fn register_submit_backend<S, F>(
    group: &mut BenchmarkGroup<'_, WallTime>,
    backend_name: &'static str,
    fixture: F,
) where
    S: ByteStreamSubmit,
    F: Copy + Fn(TcpStream) -> S + 'static,
{
    for size in SIZES {
        group.throughput(Throughput::Elements(1));
        group.bench_function(
            BenchmarkId::new(backend_name, size_label(size)),
            move |bencher| {
                let (_peer, socket) = connected_pair(size);
                let stream = fixture(socket);
                let waker = Waker::from(Arc::new(ThreadWake(thread::current())));
                let mut payload = Some(payload(size));
                let mut scratch = Some(Vec::with_capacity(size));

                // Prove the fixture before measuring it.
                let (send, receive) = submit_round_trip(
                    &waker,
                    &stream,
                    payload.take().expect("payload is available"),
                    scratch.take().expect("scratch is available"),
                    size,
                );
                assert_eq!(receive, send, "probe round trip echoed the payload");
                payload = Some(send);
                scratch = Some(receive);

                bencher.iter_custom(|iterations| {
                    let mut send = payload.take().expect("payload is available");
                    let mut receive = scratch.take().expect("scratch is available");
                    let started = Instant::now();
                    for _ in 0..iterations {
                        (send, receive) = submit_round_trip(&waker, &stream, send, receive, size);
                    }
                    let elapsed = started.elapsed();
                    assert_eq!(receive.len(), size, "every measured round trip completed");
                    payload = Some(send);
                    scratch = Some(receive);
                    elapsed
                });
            },
        );
    }
}

fn register_blocking_backend(group: &mut BenchmarkGroup<'_, WallTime>) {
    for size in SIZES {
        group.throughput(Throughput::Elements(1));
        group.bench_function(
            BenchmarkId::new("std_blocking", size_label(size)),
            move |bencher| {
                let (_peer, mut socket) = connected_pair(size);
                let send = payload(size);
                let mut receive = vec![0_u8; size];

                blocking_round_trip(&mut socket, &send, &mut receive);
                assert_eq!(receive, send, "probe round trip echoed the payload");

                bencher.iter_custom(|iterations| {
                    let started = Instant::now();
                    for _ in 0..iterations {
                        blocking_round_trip(&mut socket, &send, &mut receive);
                    }
                    started.elapsed()
                });
            },
        );
    }
}

pub(crate) fn stream_overhead_benchmarks(criterion: &mut Criterion) {
    let mut group = criterion.benchmark_group("io_uring_stream_echo/round_trip_qd1");
    group
        .sample_size(SAMPLE_SIZE)
        .warm_up_time(WARM_UP_TIME)
        .measurement_time(MEASUREMENT_TIME)
        .sampling_mode(SamplingMode::Flat);
    register_blocking_backend(&mut group);
    register_submit_backend(&mut group, "uring_stream", |socket| {
        UringByteStream::from_tcp_stream(socket, stream_config()).expect("wrap per-stream backend")
    });
    register_submit_backend(&mut group, "pooled_stream", |socket| {
        // The pool outlives the pool handle through the stream's own
        // shared-state reference, so constructing it here is sound.
        UringNetPool::new(pool_config())
            .expect("create stream pool")
            .register_stream(socket)
            .expect("register pooled backend")
    });
    group.finish();
}

fn size_label(size: usize) -> &'static str {
    match size {
        64 => "64B",
        4_096 => "4KiB",
        _ => unreachable!("unlabeled benchmark size"),
    }
}
