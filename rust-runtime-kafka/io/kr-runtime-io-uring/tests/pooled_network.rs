#![cfg(target_os = "linux")]

mod common;

use std::future::Future;
use std::io::{self, Write};
use std::net::{Ipv4Addr, Ipv6Addr, SocketAddr, TcpListener, TcpStream};
use std::os::fd::AsRawFd;
use std::pin::pin;
use std::task::{Context, Poll, Waker};
use std::time::{Duration, Instant};

use common::block_on;
use kr_runtime::{CompletionCertainty, CompletionError, CompletionResult};
use kr_runtime_io::conformance::{
    check_bounded_blocked_vectored_stream_provider,
    check_cold_bounded_blocked_vectored_stream_provider,
    check_cold_exhausted_vectored_stream_provider, check_cold_network_provider,
    check_cold_stream_pair, check_cold_vectored_stream_provider, check_connected_stream_pair,
    check_exhausted_vectored_stream_provider, check_network_provider,
    check_vectored_stream_provider,
};
use kr_runtime_io::network::{
    ByteStreamSubmit, ByteStreamVectoredSubmit, ColdNetwork, ColdStream, ConnectRequest,
    ListenRequest, NetworkError, NetworkListenerSubmit, NetworkProviderSubmit, ReadRequest,
    SharedBytes, VectoredWriteRequest, WriteRequest, WriteSegment,
};
use kr_runtime_io_uring::{
    PooledUringStream, UringNetPool, UringNetPoolConfig, UringNetPoolOpenError,
};

fn config(chunk: usize) -> UringNetPoolConfig {
    UringNetPoolConfig {
        max_streams: 32,
        command_queue_capacity: 8,
        ring_entries: 8,
        max_operation_bytes: 64 * 1024,
        max_io_chunk_bytes: chunk,
        max_listeners: 8,
        max_listener_backlog: 64,
        connect_timeout: Duration::from_secs(10),
    }
}

fn expect_rejection<T>(
    result: CompletionResult<T, kr_runtime_io::network::NetworkFailure>,
    context: &str,
) -> CompletionError<kr_runtime_io::network::NetworkFailure> {
    match result {
        Err(error) => error,
        Ok(_) => panic!("{context}: unexpectedly succeeded"),
    }
}

fn loopback_any() -> SocketAddr {
    SocketAddr::from((Ipv4Addr::LOCALHOST, 0))
}

fn tcp_pair() -> (TcpStream, TcpStream) {
    let listener = TcpListener::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)))
        .expect("bind loopback listener");
    let address = listener.local_addr().expect("read listener address");
    let client = TcpStream::connect(address).expect("connect loopback client");
    let (server, _) = listener.accept().expect("accept loopback client");
    (client, server)
}

fn registered_pair(pool: &UringNetPool) -> (PooledUringStream, PooledUringStream) {
    let (client, server) = tcp_pair();
    (
        pool.register_stream(client).expect("register client"),
        pool.register_stream(server).expect("register server"),
    )
}

fn poll_once<F: Future + Unpin>(future: &mut F) -> Poll<F::Output> {
    let mut context = Context::from_waker(Waker::noop());
    pin!(future).poll(&mut context)
}

fn fill_tcp_send_buffer(stream: &mut TcpStream) -> usize {
    let bytes: libc::c_int = 4 * 1_024;
    // SAFETY: the descriptor is live and `bytes` is initialized storage with
    // the exact length supplied to setsockopt.
    let result = unsafe {
        libc::setsockopt(
            stream.as_raw_fd(),
            libc::SOL_SOCKET,
            libc::SO_SNDBUF,
            std::ptr::from_ref(&bytes).cast(),
            size_of::<libc::c_int>() as libc::socklen_t,
        )
    };
    assert_eq!(
        result,
        0,
        "shrink send buffer: {}",
        io::Error::last_os_error()
    );
    stream
        .set_nonblocking(true)
        .expect("enable nonblocking fill");
    let chunk = [0x5a; 16 * 1_024];
    let mut written = 0usize;
    loop {
        match stream.write(&chunk) {
            Ok(0) => panic!("send-buffer fill made zero progress"),
            Ok(count) => written = written.checked_add(count).expect("test send prefix fits"),
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => break,
            Err(error) => panic!("fill TCP send buffer: {error}"),
        }
    }
    stream
        .set_nonblocking(false)
        .expect("restore blocking socket mode");
    written
}

fn write_all(stream: &PooledUringStream, mut remaining: Vec<u8>) {
    while !remaining.is_empty() {
        let result = block_on(stream.submit_write(WriteRequest { buffer: remaining }))
            .expect("write TCP prefix");
        assert!(result.bytes_written > 0);
        remaining = result.buffer[result.bytes_written..].to_vec();
    }
}

fn read_exact(stream: &PooledUringStream, expected: usize) -> Vec<u8> {
    let mut contents = Vec::new();
    while contents.len() < expected {
        let result = block_on(stream.submit_read(ReadRequest {
            buffer: Vec::new(),
            max_bytes: expected - contents.len(),
        }))
        .expect("read TCP prefix");
        assert!(result.bytes_read > 0);
        assert!(!result.end_of_stream);
        contents.extend(result.buffer);
    }
    contents
}

#[test]
fn pooled_stream_pair_passes_shared_conformance() {
    // The warm suite's dropped-write step needs one admitted write to
    // deliver its whole buffer, so it runs at full chunk size like the
    // per-stream conformance test; tiny-chunk behavior is covered by the
    // cold suite and the chunk-bound test below.
    let pool = UringNetPool::new(config(64 * 1024)).expect("create pool");
    let (client, server) = registered_pair(&pool);
    block_on(check_connected_stream_pair(&client, &server))
        .unwrap_or_else(|message| panic!("conformance failed: {message}"));
}

#[test]
fn tiny_cqe_chunks_bound_each_completion_exactly() {
    for chunk in [1usize, 3] {
        let pool = UringNetPool::new(config(chunk)).expect("create pool");
        let (client, server) = registered_pair(&pool);

        // A write larger than the chunk admits exactly one chunk per
        // completion: the socket buffer is empty, so the kernel accepts the
        // full admitted length, which the config caps.
        let message = b"partial-write";
        let write = block_on(client.submit_write(WriteRequest {
            buffer: message.to_vec(),
        }))
        .expect("first partial write completes");
        assert_eq!(write.bytes_written, chunk, "{chunk}-byte chunk bound");
        write_all(&client, write.buffer[write.bytes_written..].to_vec());
        assert_eq!(
            read_exact(&server, message.len()),
            message,
            "stream order is preserved across {chunk}-byte partial completions"
        );

        // A read never exceeds the chunk bound in one completion, and the
        // remainder arrives through later reads in order.
        write_all(&server, b"reply".to_vec());
        let first = block_on(client.submit_read(ReadRequest {
            buffer: Vec::new(),
            max_bytes: b"reply".len(),
        }))
        .expect("first partial read completes");
        assert!(
            first.bytes_read > 0 && first.bytes_read <= chunk,
            "{chunk}-byte chunk bound admitted {} bytes",
            first.bytes_read
        );
        let mut contents = first.buffer;
        contents.extend(read_exact(&client, b"reply".len() - contents.len()));
        assert_eq!(contents, b"reply");

        block_on(client.submit_close()).expect("close client");
        block_on(server.submit_close()).expect("close server");
    }
}

#[test]
fn pooled_cold_stream_pair_passes_cold_conformance() {
    for chunk in [1, 3, 7, 64 * 1024] {
        let pool = UringNetPool::new(config(chunk)).expect("create pool");
        let (client, server) = registered_pair(&pool);
        block_on(check_cold_stream_pair(
            &ColdStream::new(client),
            &ColdStream::new(server),
        ))
        .unwrap_or_else(|message| panic!("{chunk}-byte cold conformance failed: {message}"));
    }
}

#[test]
fn pooled_provider_passes_shared_control_plane_conformance() {
    let pool = UringNetPool::new(config(64 * 1024)).expect("create pool");
    block_on(check_network_provider(
        &pool,
        ListenRequest {
            address: loopback_any(),
            backlog: 4,
        },
        loopback_any(),
        loopback_any(),
    ))
    .unwrap_or_else(|message| panic!("pooled provider conformance failed: {message}"));
}

#[test]
fn pooled_provider_passes_cold_control_plane_conformance() {
    let pool = UringNetPool::new(config(64 * 1024)).expect("create pool");
    let cold = ColdNetwork::new(pool);
    block_on(check_cold_network_provider(
        &cold,
        ListenRequest {
            address: loopback_any(),
            backlog: 4,
        },
        loopback_any(),
        loopback_any(),
    ))
    .unwrap_or_else(|message| panic!("pooled cold provider conformance failed: {message}"));
}

#[test]
fn pooled_provider_passes_shared_control_plane_conformance_over_ipv6() {
    let ipv6_any = SocketAddr::from((Ipv6Addr::LOCALHOST, 0));
    let probe = match TcpListener::bind(ipv6_any) {
        Ok(probe) => probe,
        Err(error)
            if matches!(
                error.kind(),
                std::io::ErrorKind::AddrNotAvailable | std::io::ErrorKind::Unsupported
            ) =>
        {
            eprintln!("skipping IPv6 conformance: host has no IPv6 loopback ({error})");
            return;
        }
        Err(error) => panic!("IPv6 availability probe failed unexpectedly: {error}"),
    };
    drop(probe);

    let pool = UringNetPool::new(config(64 * 1024)).expect("create pool");
    block_on(check_network_provider(
        &pool,
        ListenRequest {
            address: ipv6_any,
            backlog: 4,
        },
        ipv6_any,
        ipv6_any,
    ))
    .unwrap_or_else(|message| panic!("pooled IPv6 provider conformance failed: {message}"));
}

#[test]
fn pooled_listen_and_accept_bounds_reject_before_side_effect() {
    let mut tight = config(64 * 1024);
    tight.max_listeners = 1;
    tight.max_streams = 2;
    let pool = UringNetPool::new(tight).expect("create pool");

    for backlog in [0, 65] {
        let refused = expect_rejection(
            block_on(pool.submit_listen(ListenRequest {
                address: loopback_any(),
                backlog,
            })),
            "out-of-bound backlog",
        );
        assert_eq!(refused.certainty(), CompletionCertainty::NotApplied);
        assert_eq!(
            refused.error().error(),
            &NetworkError::InvalidRequest {
                reason: "listener backlog is outside the configured bound",
            },
            "backlog {backlog}"
        );
    }

    let listener = block_on(pool.submit_listen(ListenRequest {
        address: loopback_any(),
        backlog: 4,
    }))
    .expect("first listen binds");
    let refused = expect_rejection(
        block_on(pool.submit_listen(ListenRequest {
            address: loopback_any(),
            backlog: 4,
        })),
        "second listen",
    );
    assert_eq!(refused.certainty(), CompletionCertainty::NotApplied);
    assert_eq!(
        refused.error().error(),
        &NetworkError::ResourceExhausted {
            resource: "io_uring pooled listeners",
            limit: 1,
        }
    );

    // With every stream permit held by registered streams, accept and
    // connect reject before consuming a connection or creating a socket.
    let (client, server) = registered_pair(&pool);
    let refused_accept = expect_rejection(block_on(listener.submit_accept()), "over-bound accept");
    assert_eq!(refused_accept.certainty(), CompletionCertainty::NotApplied);
    assert_eq!(
        refused_accept.error().error(),
        &NetworkError::ResourceExhausted {
            resource: "io_uring pooled network streams",
            limit: 2,
        }
    );
    let refused_connect = expect_rejection(
        block_on(pool.submit_connect(ConnectRequest {
            local: loopback_any(),
            remote: listener.local_address(),
        })),
        "over-bound connect",
    );
    assert_eq!(refused_connect.certainty(), CompletionCertainty::NotApplied);
    assert_eq!(
        refused_connect.error().error(),
        &NetworkError::ResourceExhausted {
            resource: "io_uring pooled network streams",
            limit: 2,
        }
    );

    block_on(client.submit_close()).expect("close client");
    block_on(server.submit_close()).expect("close server");
    block_on(listener.submit_close()).expect("close listener");
}

#[test]
fn pending_accepts_reject_with_not_applied_when_the_listener_closes() {
    let pool = UringNetPool::new(config(64 * 1024)).expect("create pool");
    let listener = block_on(pool.submit_listen(ListenRequest {
        address: loopback_any(),
        backlog: 4,
    }))
    .expect("listen binds");

    let first_accept = listener.submit_accept();
    let second_accept = listener.submit_accept();
    block_on(listener.submit_close()).expect("close completes");

    for (index, accept) in [first_accept, second_accept].into_iter().enumerate() {
        let rejected = expect_rejection(block_on(accept), "pending accept at close");
        assert_eq!(
            rejected.certainty(),
            CompletionCertainty::NotApplied,
            "accept {index} rejected before any connection was consumed"
        );
        assert_eq!(rejected.error().error(), &NetworkError::ListenerClosed);
    }

    block_on(listener.submit_close()).expect("repeated close is idempotent");
    let late = expect_rejection(block_on(listener.submit_accept()), "accept after close");
    assert_eq!(late.error().error(), &NetworkError::ListenerClosed);
}

#[test]
fn pending_pooled_read_does_not_block_writes_on_the_same_stream() {
    let pool = UringNetPool::new(config(64 * 1024)).expect("create pool");
    let (client, server) = registered_pair(&pool);

    let pending_client_read = client.submit_read(ReadRequest {
        buffer: Vec::new(),
        max_bytes: 2,
    });
    // Admission is eager, so the client's receive is or will be armed on the
    // shared ring. Its independent write direction must still deliver bytes.
    write_all(&client, b"up".to_vec());
    assert_eq!(read_exact(&server, 2), b"up");

    write_all(&server, b"ok".to_vec());
    let response = block_on(pending_client_read).expect("complete pending client read");
    assert_eq!(response.buffer, b"ok");
    assert_eq!(response.bytes_read, 2);

    block_on(client.submit_close()).expect("close client");
    block_on(server.submit_close()).expect("close server");
}

#[test]
fn every_stream_can_hold_an_armed_read_while_writes_complete() {
    // Every registered stream arms a receive at once against silent peers,
    // occupying its reserved sustained slot; the sibling reservation must
    // still carry every stream's writes. This is the reservation analog of
    // the datagram saturation suite: if receives shared the writes' budget,
    // this test could not complete.
    let mut pool_config = config(64 * 1024);
    pool_config.max_streams = 16;
    let pool = UringNetPool::new(pool_config).expect("create pool");
    let pairs: Vec<(PooledUringStream, PooledUringStream)> =
        (0..8).map(|_| registered_pair(&pool)).collect();

    let armed: Vec<_> = pairs
        .iter()
        .flat_map(|(client, server)| {
            [
                client.submit_read(ReadRequest {
                    buffer: Vec::new(),
                    max_bytes: 4,
                }),
                server.submit_read(ReadRequest {
                    buffer: Vec::new(),
                    max_bytes: 4,
                }),
            ]
        })
        .collect();

    for (index, (client, server)) in pairs.iter().enumerate() {
        write_all(client, vec![index as u8; 4]);
        write_all(server, vec![index as u8 ^ 0xff; 4]);
    }

    for (index, read) in armed.into_iter().enumerate() {
        let result = block_on(read).expect("armed read completes");
        assert_eq!(result.bytes_read, 4, "stream {index} read fully");
        let expected = if index % 2 == 0 {
            vec![(index / 2) as u8 ^ 0xff; 4]
        } else {
            vec![(index / 2) as u8; 4]
        };
        assert_eq!(
            result.buffer, expected,
            "stream {index} read its peer's bytes"
        );
    }
}

#[test]
fn explicit_close_interrupts_an_armed_pooled_send_and_returns_every_owned_buffer() {
    let (mut client, server) = tcp_pair();
    fill_tcp_send_buffer(&mut client);
    let mut tight = config(64 * 1024);
    tight.command_queue_capacity = 1;
    let pool = UringNetPool::new(tight).expect("create pool");
    let client = pool.register_stream(client).expect("register full client");

    let mut first_buffer = Vec::with_capacity(64 * 1_024);
    first_buffer.resize(64 * 1_024, 0xa5);
    let first_pointer = first_buffer.as_ptr();
    let first = client.submit_write(WriteRequest {
        buffer: first_buffer,
    });

    // With a one-entry command queue, admitting a second write proves that
    // the coordinator has popped the first command and armed its send.
    let deadline = Instant::now() + Duration::from_secs(2);
    let (queued, queued_pointer) = loop {
        assert!(
            Instant::now() < deadline,
            "coordinator did not arm the first send"
        );
        let mut buffer = Vec::with_capacity(32);
        buffer.extend_from_slice(b"queued");
        let pointer = buffer.as_ptr();
        let mut candidate = client.submit_write(WriteRequest { buffer });
        match poll_once(&mut candidate) {
            Poll::Pending => break (candidate, pointer),
            Poll::Ready(Err(error))
                if matches!(
                    error.error().error(),
                    NetworkError::ResourceExhausted { .. }
                ) =>
            {
                std::thread::yield_now();
            }
            Poll::Ready(other) => panic!("unexpected second-write admission result: {other:?}"),
        }
    };

    let mut close = client.submit_close();
    assert!(
        matches!(poll_once(&mut close), Poll::Ready(Ok(()))),
        "full close must bypass the write FIFO and interrupt the armed send"
    );

    let first = block_on(first).expect_err("interrupted armed send fails");
    assert_eq!(first.error().error(), &NetworkError::ConnectionClosed);
    let (_, first) = first.into_parts();
    let first_buffer = first.into_buffer().expect("armed write returns its buffer");
    assert_eq!(first_buffer.as_ptr(), first_pointer);

    let queued = block_on(queued).expect_err("queued write observes the close");
    assert_eq!(queued.certainty(), CompletionCertainty::NotApplied);
    assert_eq!(queued.error().error(), &NetworkError::ConnectionClosed);
    let (_, queued) = queued.into_parts();
    let queued_buffer = queued
        .into_buffer()
        .expect("queued write returns its buffer");
    assert_eq!(queued_buffer.as_ptr(), queued_pointer);

    drop(server);
}

#[test]
fn stream_registration_is_bounded_and_permits_release_on_drop() {
    let mut tiny = config(64 * 1024);
    tiny.max_streams = 2;
    let pool = UringNetPool::new(tiny).expect("create pool");
    let (client, server) = registered_pair(&pool);

    let (extra_client, _extra_server) = tcp_pair();
    match pool.register_stream(extra_client) {
        Err(UringNetPoolOpenError::ResourceExhausted { resource, limit }) => {
            assert_eq!(resource, "io_uring pooled network streams");
            assert_eq!(limit, 2);
        }
        other => panic!(
            "third registration must exhaust the stream bound, got {:?}",
            other.map(|_| "a stream")
        ),
    }

    // A released permit frees exactly one registration. The permit drops
    // when the handle's coordinator state drains, which follows the close
    // asynchronously, so acquisition is retried briefly.
    block_on(client.submit_close()).expect("close client");
    drop(client);
    let deadline = Instant::now() + Duration::from_secs(5);
    let (retry_client, _retry_server) = tcp_pair();
    let mut candidate = Some(retry_client);
    loop {
        match pool.register_stream(candidate.take().expect("candidate socket is present")) {
            Ok(_registered) => break,
            Err(UringNetPoolOpenError::ResourceExhausted { .. }) => {
                assert!(
                    Instant::now() < deadline,
                    "dropped stream never released its registration permit"
                );
                let (fresh, _server) = tcp_pair();
                candidate = Some(fresh);
                std::thread::yield_now();
            }
            Err(other) => panic!("unexpected registration failure: {other}"),
        }
    }

    block_on(server.submit_close()).expect("close server");
}

#[test]
fn a_burst_past_the_per_direction_bound_rejects_cleanly_and_terminalizes() {
    let mut tight = config(64 * 1024);
    tight.command_queue_capacity = 2;
    let pool = UringNetPool::new(tight).expect("create pool");
    let (client, server) = registered_pair(&pool);

    // The coordinator drains concurrently, so which submissions reject is
    // timing-dependent; what must hold is that every response terminalizes
    // and every rejection carries exactly the bounded-admission shape with
    // its buffer returned.
    let responses: Vec<_> = (0..32)
        .map(|index| {
            client.submit_write(WriteRequest {
                buffer: vec![index as u8; 8],
            })
        })
        .collect();
    let mut completed = 0;
    let mut drained = Vec::new();
    for response in responses {
        match block_on(response) {
            Ok(success) => {
                assert!(success.bytes_written > 0);
                completed += success.bytes_written;
            }
            Err(error) => {
                assert_eq!(
                    error.certainty(),
                    CompletionCertainty::NotApplied,
                    "an admission rejection precedes any effect"
                );
                let (_, failure) = error.into_parts();
                assert_eq!(
                    failure.error(),
                    &NetworkError::ResourceExhausted {
                        resource: "io_uring pooled network command queue",
                        limit: 2,
                    }
                );
                let buffer = failure.into_buffer().expect("the buffer came back");
                assert_eq!(buffer.len(), 8);
            }
        }
    }
    assert!(completed > 0, "no submission ever completed");
    while drained.len() < completed {
        let result = block_on(server.submit_read(ReadRequest {
            buffer: Vec::new(),
            max_bytes: completed - drained.len(),
        }))
        .expect("drain accepted bytes");
        assert!(result.bytes_read > 0);
        drained.extend(result.buffer);
    }

    block_on(client.submit_close()).expect("close client");
    block_on(server.submit_close()).expect("close server");
}

fn vectored_request(owner: &SharedBytes, range: std::ops::Range<u32>) -> VectoredWriteRequest {
    VectoredWriteRequest {
        segments: vec![WriteSegment {
            bytes: owner.clone(),
            range,
        }],
    }
}

#[test]
fn pooled_vectored_streams_pass_shared_warm_and_cold_conformance() {
    for chunk in [1, 3, 5, 64 * 1024] {
        for cold in [false, true] {
            let pool = UringNetPool::new(config(chunk)).expect("create vectored pool");
            let (client, server) = registered_pair(&pool);
            let result = if cold {
                block_on(check_cold_vectored_stream_provider(
                    &ColdStream::new(client),
                    &ColdStream::new(server),
                ))
            } else {
                block_on(check_vectored_stream_provider(&client, &server))
            };
            result.unwrap_or_else(|message| panic!("chunk={chunk} cold={cold}: {message}"));
        }
    }
}

#[test]
fn pooled_vectored_streams_pass_shared_blocked_warm_and_cold_conformance() {
    const PENDING_BYTES: usize = 1024 * 1024;
    for cold in [false, true] {
        let (mut socket, peer) = tcp_pair();
        let already_written = fill_tcp_send_buffer(&mut socket);
        let mut limits = config(PENDING_BYTES);
        limits.max_operation_bytes = 2 * PENDING_BYTES;
        let pool = UringNetPool::new(limits).expect("create pool");
        let client = pool.register_stream(socket).expect("register sender");
        let server = pool.register_stream(peer).expect("register receiver");
        // Fill more than the socket windows can absorb while the peer is silent.
        // Either this send or the shared check's following one-byte send stays
        // blocked. One kernel send may still report short progress: the check
        // measures the observed prefix instead of predicting its completion.
        let pending_buffer = vec![0x5a; PENDING_BYTES];
        let pointer = pending_buffer.as_ptr();
        let capacity = pending_buffer.capacity();
        let pending = client.submit_write(WriteRequest {
            buffer: pending_buffer,
        });
        let prefix = vec![0x5a; already_written + PENDING_BYTES];
        let result = if cold {
            block_on(check_cold_bounded_blocked_vectored_stream_provider(
                &ColdStream::new(client.clone()),
                &ColdStream::new(server.clone()),
                &prefix,
            ))
        } else {
            block_on(check_bounded_blocked_vectored_stream_provider(
                &client, &server, &prefix,
            ))
        };
        let observed_prefix = result.unwrap_or_else(|message| panic!("cold={cold}: {message}"));
        let preceding = block_on(pending).expect("preceding send completes");
        assert_eq!(preceding.buffer.as_ptr(), pointer);
        assert_eq!(preceding.buffer.capacity(), capacity);
        assert!(preceding.buffer.iter().all(|byte| *byte == 0x5a));
        assert!(preceding.bytes_written > 0 && preceding.bytes_written <= PENDING_BYTES);
        assert_eq!(
            observed_prefix,
            already_written + preceding.bytes_written,
            "cold={cold}: peer bytes must match actual preceding write progress"
        );
        eprintln!(
            "blocked vectored cold={cold}: prefilled={already_written}, preceding={}, observed={observed_prefix}",
            preceding.bytes_written
        );
        block_on(client.submit_close()).expect("close sender");
        block_on(server.submit_close()).expect("close receiver");
    }
}

#[test]
fn pooled_vectored_exhaustion_and_close_return_every_original_allocation() {
    for cold in [false, true] {
        let (mut socket, peer) = tcp_pair();
        fill_tcp_send_buffer(&mut socket);
        let mut limits = config(1024 * 1024);
        limits.max_operation_bytes = 1024 * 1024;
        limits.command_queue_capacity = 1;
        let pool = UringNetPool::new(limits).expect("create pool");
        let client = pool.register_stream(socket).expect("register sender");
        let owner = SharedBytes::from(vec![0xa5; 1024 * 1024]);
        let first_segments = vectored_request(&owner, 0..1024 * 1024).segments;
        let first_pointer = first_segments.as_ptr();
        let first = client.submit_write_vectored(VectoredWriteRequest {
            segments: first_segments,
        });
        let queued_owner = SharedBytes::from(vec![42]);
        let deadline = Instant::now() + Duration::from_secs(2);
        let (queued, queued_pointer) = loop {
            assert!(Instant::now() < deadline, "first vectored send never armed");
            let segments = vectored_request(&queued_owner, 0..1).segments;
            let pointer = segments.as_ptr();
            let mut candidate = client.submit_write_vectored(VectoredWriteRequest { segments });
            match poll_once(&mut candidate) {
                Poll::Pending => break (candidate, pointer),
                Poll::Ready(Err(error))
                    if matches!(
                        error.error().error(),
                        NetworkError::ResourceExhausted { .. }
                    ) =>
                {
                    std::thread::yield_now()
                }
                other => panic!("unexpected queue fixture completion: {other:?}"),
            }
        };
        if cold {
            block_on(check_cold_exhausted_vectored_stream_provider(
                &ColdStream::new(client.clone()),
            ))
            .expect("cold exhausted conformance");
        } else {
            block_on(check_exhausted_vectored_stream_provider(&client))
                .expect("warm exhausted conformance");
        }
        assert_eq!(owner.strong_count(), 2);
        assert_eq!(queued_owner.strong_count(), 2);
        block_on(client.submit_close()).expect("close bypasses send queue");
        match block_on(first) {
            Ok(success) => {
                assert_eq!(success.segments.as_ptr(), first_pointer);
                assert!(success.bytes_written <= owner.len());
            }
            Err(error) => {
                assert_eq!(error.error().segments.as_ptr(), first_pointer);
                assert_eq!(error.error().error(), &NetworkError::ConnectionClosed);
            }
        }
        let failure = block_on(queued).expect_err("queued send is rejected at close");
        assert_eq!(failure.certainty(), CompletionCertainty::NotApplied);
        assert_eq!(failure.error().segments.as_ptr(), queued_pointer);
        assert_eq!(failure.error().bytes_transferred(), 0);
        drop(failure);
        assert_eq!(owner.strong_count(), 1);
        assert_eq!(queued_owner.strong_count(), 1);
        drop(peer);
    }
}

#[test]
fn abandoning_native_vectored_send_releases_payload_only_after_pool_drain() {
    let (mut socket, peer) = tcp_pair();
    fill_tcp_send_buffer(&mut socket);
    let mut limits = config(1024 * 1024);
    limits.max_operation_bytes = 1024 * 1024;
    let pool = UringNetPool::new(limits).expect("create pool");
    let stream = pool.register_stream(socket).expect("register sender");
    let mut owner = SharedBytes::from(vec![0x55; 1024 * 1024]);
    let mut response = stream.submit_write_vectored(vectored_request(&owner, 0..1024 * 1024));
    assert!(poll_once(&mut response).is_pending());
    drop(response);
    assert_eq!(owner.strong_count(), 2);
    assert!(owner.try_as_mut().is_none());
    drop(stream);
    drop(pool); // Joins the coordinator and reactor after their terminal CQEs.
    assert_eq!(owner.strong_count(), 1);
    assert!(owner.try_as_mut().is_some());
    drop(peer);
}

#[test]
fn connection_guard_survives_native_drain_and_unconsumed_terminal_buffer() {
    let (socket, peer) = tcp_pair();
    let pool = UringNetPool::new(config(8)).unwrap();
    let stream = pool.register_stream(socket).unwrap();
    let guard = std::sync::Arc::new(());
    stream.attach_lifetime_guard(guard.clone()).unwrap();
    let response = stream.submit_read(ReadRequest {
        buffer: Vec::with_capacity(8),
        max_bytes: 8,
    });
    drop(stream);
    drop(pool);
    assert_eq!(std::sync::Arc::strong_count(&guard), 2);
    drop(block_on(response));
    assert_eq!(std::sync::Arc::strong_count(&guard), 1);
    drop(peer);
}
