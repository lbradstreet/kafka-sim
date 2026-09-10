#![cfg(target_os = "linux")]

mod common;

use std::future::Future;
use std::io::{self, Write};
use std::mem::size_of;
use std::net::{Ipv4Addr, Ipv6Addr, SocketAddr, TcpListener, TcpStream};
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};
use std::pin::Pin;
use std::task::{Context, Poll, Waker};
use std::time::{Duration, Instant};

use common::block_on;
use kr_runtime::CompletionCertainty;
use kr_runtime_io::conformance::{
    check_cold_network_provider, check_connected_stream_pair, check_network_provider,
};
use kr_runtime_io::network::{
    ByteStreamSubmit, ColdNetwork, ConnectRequest, ListenRequest, NetworkError,
    NetworkListenerSubmit, NetworkProviderSubmit, ReadRequest, WriteRequest,
};
use kr_runtime_io_uring::{
    UringByteStream, UringNetwork, UringNetworkConfig, UringNetworkProviderConfig,
};

fn config() -> UringNetworkConfig {
    UringNetworkConfig {
        command_queue_capacity: 8,
        ring_entries: 4,
        max_operation_bytes: 256,
        max_io_chunk_bytes: 2,
        connect_timeout: Duration::from_millis(250),
    }
}

fn provider_config() -> UringNetworkProviderConfig {
    UringNetworkProviderConfig {
        stream: config(),
        max_listeners: 8,
        max_streams: 16,
        max_listener_backlog: 8,
    }
}

fn loopback_any() -> SocketAddr {
    SocketAddr::from((Ipv4Addr::LOCALHOST, 0))
}

fn poll_once<F: Future + Unpin>(future: &mut F) -> Poll<F::Output> {
    let mut context = Context::from_waker(Waker::noop());
    Pin::new(future).poll(&mut context)
}

fn reserve_non_listening_tcp_port() -> (OwnedFd, SocketAddr) {
    // SAFETY: these constants request a standard IPv4 TCP socket and transfer
    // no Rust-owned resource on failure.
    let raw_fd = unsafe {
        libc::socket(
            libc::AF_INET,
            libc::SOCK_STREAM | libc::SOCK_CLOEXEC,
            libc::IPPROTO_TCP,
        )
    };
    assert!(
        raw_fd >= 0,
        "create refusal probe: {}",
        std::io::Error::last_os_error()
    );
    // SAFETY: successful socket(2) returned a fresh descriptor and ownership
    // moves immediately into `OwnedFd`.
    let socket = unsafe { OwnedFd::from_raw_fd(raw_fd) };

    // SAFETY: zero initializes sockaddr padding before every meaningful field
    // is assigned.
    let mut address: libc::sockaddr_in = unsafe { std::mem::zeroed() };
    address.sin_family = libc::AF_INET as libc::sa_family_t;
    address.sin_port = 0;
    address.sin_addr = libc::in_addr {
        s_addr: u32::from_ne_bytes(Ipv4Addr::LOCALHOST.octets()),
    };
    // SAFETY: the descriptor and exact sockaddr pointer/length stay live for
    // the duration of bind(2).
    let bound = unsafe {
        libc::bind(
            socket.as_raw_fd(),
            std::ptr::from_ref(&address).cast::<libc::sockaddr>(),
            size_of::<libc::sockaddr_in>() as libc::socklen_t,
        )
    };
    assert_eq!(
        bound,
        0,
        "bind refusal probe: {}",
        std::io::Error::last_os_error()
    );

    let mut length = size_of::<libc::sockaddr_in>() as libc::socklen_t;
    // SAFETY: getsockname writes at most `length` initialized bytes to the
    // live sockaddr storage and updates the live socklen value.
    let named = unsafe {
        libc::getsockname(
            socket.as_raw_fd(),
            std::ptr::from_mut(&mut address).cast::<libc::sockaddr>(),
            &mut length,
        )
    };
    assert_eq!(
        named,
        0,
        "name refusal probe: {}",
        std::io::Error::last_os_error()
    );
    assert_eq!(length as usize, size_of::<libc::sockaddr_in>());
    (
        socket,
        SocketAddr::from((Ipv4Addr::LOCALHOST, u16::from_be(address.sin_port))),
    )
}

fn connected_pair() -> (UringByteStream, UringByteStream) {
    let listener = TcpListener::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)))
        .expect("bind loopback listener");
    let address = listener.local_addr().expect("read listener address");
    let client = TcpStream::connect(address).expect("connect loopback client");
    let (server, _) = listener.accept().expect("accept loopback client");
    (
        UringByteStream::from_tcp_stream(client, config()).expect("wrap client stream"),
        UringByteStream::from_tcp_stream(server, config()).expect("wrap server stream"),
    )
}

fn fill_tcp_send_buffer(stream: &mut TcpStream) {
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
    loop {
        match stream.write(&chunk) {
            Ok(0) => panic!("send-buffer fill made zero progress"),
            Ok(_) => {}
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => break,
            Err(error) => panic!("fill TCP send buffer: {error}"),
        }
    }
    stream
        .set_nonblocking(false)
        .expect("restore blocking socket mode");
}

fn write_all(stream: &UringByteStream, mut remaining: Vec<u8>) {
    while !remaining.is_empty() {
        let result = block_on(stream.submit_write(WriteRequest { buffer: remaining }))
            .expect("write TCP prefix");
        assert!(result.bytes_written > 0);
        remaining = result.buffer[result.bytes_written..].to_vec();
    }
}

fn read_exact(stream: &UringByteStream, expected: usize) -> Vec<u8> {
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
fn connected_stream_supports_partial_io_half_close_and_idempotent_close() {
    let (client, server) = connected_pair();

    let zero = block_on(server.submit_read(ReadRequest {
        buffer: b"prefix".to_vec(),
        max_bytes: 0,
    }))
    .expect("zero-capacity read");
    assert_eq!(zero.buffer, b"prefix");
    assert_eq!(zero.bytes_read, 0);
    assert!(!zero.end_of_stream);

    drop(client.submit_write(WriteRequest {
        buffer: b"go".to_vec(),
    }));
    assert_eq!(read_exact(&server, 2), b"go");

    write_all(&client, b"hello".to_vec());
    assert_eq!(read_exact(&server, 5), b"hello");

    block_on(client.submit_shutdown_write()).expect("half-close client writer");
    let eof = block_on(server.submit_read(ReadRequest {
        buffer: Vec::new(),
        max_bytes: 1,
    }))
    .expect("read client EOF");
    assert_eq!(eof.bytes_read, 0);
    assert!(eof.end_of_stream);

    block_on(client.submit_close()).expect("close client");
    block_on(client.submit_close()).expect("repeat client close");
    block_on(server.submit_close()).expect("close server");
    block_on(server.submit_close()).expect("repeat server close");
}

#[test]
fn real_stream_passes_shared_conformance() {
    let (client, server) = connected_pair();
    block_on(check_connected_stream_pair(&client, &server))
        .unwrap_or_else(|message| panic!("real byte stream conformance failed: {message}"));
}

#[test]
fn pending_local_read_does_not_block_local_write_progress() {
    let (client, server) = connected_pair();

    let pending_client_read = client.submit_read(ReadRequest {
        buffer: Vec::new(),
        max_bytes: 2,
    });
    // Admission is eager, so the client's read actor is now waiting in Recv.
    // Its independent write actor must still deliver bytes to the server.
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
fn explicit_close_interrupts_an_active_send_and_returns_every_owned_buffer() {
    let listener = TcpListener::bind(loopback_any()).expect("bind loopback listener");
    let address = listener.local_addr().expect("listener address");
    let mut client = TcpStream::connect(address).expect("connect loopback client");
    let (server, _) = listener.accept().expect("accept loopback client");
    fill_tcp_send_buffer(&mut client);
    let config = UringNetworkConfig {
        command_queue_capacity: 1,
        max_operation_bytes: 64 * 1_024,
        max_io_chunk_bytes: 64 * 1_024,
        ..config()
    };
    let client = UringByteStream::from_tcp_stream(client, config).expect("wrap full client socket");

    let mut first_buffer = Vec::with_capacity(64 * 1_024);
    first_buffer.resize(64 * 1_024, 0xa5);
    let first_pointer = first_buffer.as_ptr();
    let first = client.submit_write(WriteRequest {
        buffer: first_buffer,
    });

    // With a one-entry command queue, admitting a second write proves that the
    // actor has removed the first command and is blocked in its send CQE.
    let deadline = Instant::now() + Duration::from_secs(2);
    let (queued, queued_pointer) = loop {
        assert!(
            Instant::now() < deadline,
            "write actor did not enter the active send"
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
        "full close must bypass the write FIFO and interrupt its active send"
    );

    let first = block_on(first).expect_err("interrupted active send fails");
    assert_eq!(first.error().error(), &NetworkError::ConnectionClosed);
    let (_, first) = first.into_parts();
    let first_buffer = first
        .into_buffer()
        .expect("active write returns its buffer");
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
fn real_provider_passes_shared_control_plane_conformance() {
    let provider = UringNetwork::new(provider_config()).expect("start io_uring network provider");
    block_on(check_network_provider(
        &provider,
        ListenRequest {
            address: loopback_any(),
            backlog: 4,
        },
        loopback_any(),
        loopback_any(),
    ))
    .unwrap_or_else(|message| panic!("real network provider conformance failed: {message}"));
}

#[test]
fn real_provider_passes_cold_control_plane_conformance() {
    let provider = UringNetwork::new(provider_config()).expect("start io_uring network provider");
    let cold = ColdNetwork::new(provider);
    block_on(check_cold_network_provider(
        &cold,
        ListenRequest {
            address: loopback_any(),
            backlog: 4,
        },
        loopback_any(),
        loopback_any(),
    ))
    .unwrap_or_else(|message| panic!("real cold network conformance failed: {message}"));
}

#[test]
fn real_provider_passes_shared_control_plane_conformance_over_ipv6() {
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

    let provider = UringNetwork::new(provider_config()).expect("start io_uring network provider");
    block_on(check_network_provider(
        &provider,
        ListenRequest {
            address: ipv6_any,
            backlog: 4,
        },
        ipv6_any,
        ipv6_any,
    ))
    .unwrap_or_else(|message| panic!("real IPv6 provider conformance failed: {message}"));
}

#[test]
fn connection_refusal_and_duplicate_binding_are_normalized() {
    let provider = UringNetwork::new(provider_config()).expect("start io_uring network provider");
    let (_refusal_probe, refused_address) = reserve_non_listening_tcp_port();

    let refused = block_on(provider.submit_connect(ConnectRequest {
        local: loopback_any(),
        remote: refused_address,
    }))
    .err()
    .expect("unused port must refuse connection");
    assert_eq!(refused.error().error(), &NetworkError::ConnectionRefused);

    let first = block_on(provider.submit_listen(ListenRequest {
        address: loopback_any(),
        backlog: 2,
    }))
    .expect("bind first listener");
    let duplicate = block_on(provider.submit_listen(ListenRequest {
        address: first.local_address(),
        backlog: 2,
    }))
    .err()
    .expect("duplicate bind must fail");
    assert_eq!(duplicate.error().error(), &NetworkError::AddressInUse);
    block_on(first.submit_close()).expect("close first listener");
}

#[test]
#[ignore = "requires KR_RUNTIME_IO_URING_BLACKHOLE_ADDRESS and an external packet-drop rule"]
fn configured_connect_timeout_bounds_a_blackholed_peer() {
    let remote = std::env::var("KR_RUNTIME_IO_URING_BLACKHOLE_ADDRESS")
        .expect("set KR_RUNTIME_IO_URING_BLACKHOLE_ADDRESS to a packet-dropped socket address")
        .parse()
        .expect("blackhole address must be a socket address");
    let timeout = Duration::from_millis(25);
    let config = UringNetworkProviderConfig {
        stream: UringNetworkConfig {
            connect_timeout: timeout,
            ..config()
        },
        ..provider_config()
    };
    let provider = UringNetwork::new(config).expect("start io_uring network provider");

    let started = Instant::now();
    let error = block_on(provider.submit_connect(ConnectRequest {
        local: SocketAddr::from((Ipv4Addr::UNSPECIFIED, 0)),
        remote,
    }))
    .err()
    .expect("packet-dropped connect must reach its linked timeout");
    drop(provider);

    assert_eq!(error.certainty(), CompletionCertainty::MayHaveApplied);
    assert!(
        matches!(
            error.error().error(),
            NetworkError::Backend {
                raw_os_error: Some(libc::ETIMEDOUT),
                ..
            }
        ),
        "unexpected blackholed-connect failure: {error:?}"
    );
    assert!(
        started.elapsed() < Duration::from_secs(2),
        "connect and provider teardown exceeded the configured bound by an unreasonable margin"
    );
}

#[test]
fn listener_close_fences_release_and_rejects_pending_accept() {
    let config = UringNetworkProviderConfig {
        max_listeners: 1,
        ..provider_config()
    };
    let provider = UringNetwork::new(config).expect("start io_uring network provider");
    let listener = block_on(provider.submit_listen(ListenRequest {
        address: loopback_any(),
        backlog: 2,
    }))
    .expect("bind listener");
    let address = listener.local_address();

    let listener_limit = block_on(provider.submit_listen(ListenRequest {
        address: loopback_any(),
        backlog: 2,
    }))
    .err()
    .expect("second listener must exhaust configured permit");
    assert_eq!(
        listener_limit.error().error(),
        &NetworkError::ResourceExhausted {
            resource: "io_uring listeners",
            limit: 1,
        }
    );

    let pending = listener.submit_accept();
    block_on(listener.submit_close()).expect("close listener");
    let closed = block_on(pending)
        .err()
        .expect("pending accept must fail on close");
    assert_eq!(closed.error().error(), &NetworkError::ListenerClosed);

    let rebound = block_on(provider.submit_listen(ListenRequest {
        address,
        backlog: 2,
    }))
    .expect("close completion must fence binding and permit release");
    block_on(rebound.submit_close()).expect("close rebound listener");

    let closed_again = block_on(listener.submit_accept())
        .err()
        .expect("closed listener handle must reject later accepts");
    assert_eq!(closed_again.error().error(), &NetworkError::ListenerClosed);
}

#[test]
fn backlog_and_live_stream_bounds_reject_before_side_effect() {
    let config = UringNetworkProviderConfig {
        max_streams: 1,
        max_listener_backlog: 2,
        ..provider_config()
    };
    let provider = UringNetwork::new(config).expect("start io_uring network provider");
    for backlog in [0, 3] {
        let error = block_on(provider.submit_listen(ListenRequest {
            address: loopback_any(),
            backlog,
        }))
        .err()
        .expect("out-of-range backlog must fail");
        assert!(matches!(
            error.error().error(),
            NetworkError::InvalidRequest { .. }
        ));
    }

    let listener = block_on(provider.submit_listen(ListenRequest {
        address: loopback_any(),
        backlog: 2,
    }))
    .expect("bind listener");
    let first = block_on(provider.submit_connect(ConnectRequest {
        local: loopback_any(),
        remote: listener.local_address(),
    }))
    .expect("connect first bounded stream");
    let full = block_on(provider.submit_connect(ConnectRequest {
        local: loopback_any(),
        remote: listener.local_address(),
    }))
    .err()
    .expect("second live stream must exhaust configured permit");
    assert_eq!(
        full.error().error(),
        &NetworkError::ResourceExhausted {
            resource: "io_uring connected streams",
            limit: 1,
        }
    );
    drop(first);
    block_on(listener.submit_close()).expect("close listener");
}

#[test]
fn accept_stream_limit_does_not_dequeue_a_connection() {
    let config = UringNetworkProviderConfig {
        max_streams: 2,
        ..provider_config()
    };
    let provider = UringNetwork::new(config).expect("start io_uring network provider");
    let listener = block_on(provider.submit_listen(ListenRequest {
        address: loopback_any(),
        backlog: 2,
    }))
    .expect("bind listener");

    let first_accept = listener.submit_accept();
    let first_client = block_on(provider.submit_connect(ConnectRequest {
        local: loopback_any(),
        remote: listener.local_address(),
    }))
    .expect("connect first bounded client");
    let first_server = block_on(first_accept).expect("accept first bounded client");

    let mut external_client = TcpStream::connect(listener.local_address())
        .expect("queue external client outside provider permits");
    let full = block_on(listener.submit_accept())
        .err()
        .expect("accept must reserve a stream permit before dequeuing");
    assert_eq!(
        full.error().error(),
        &NetworkError::ResourceExhausted {
            resource: "io_uring connected streams",
            limit: 2,
        }
    );

    drop(first_client);
    let second_server = block_on(listener.submit_accept())
        .expect("rejected accept must leave external connection queued");
    external_client
        .write_all(b"ok")
        .expect("write external marker");
    assert_eq!(read_exact(&second_server, 2), b"ok");

    drop(first_server);
    drop(second_server);
    drop(external_client);
    block_on(listener.submit_close()).expect("close listener");
}

#[test]
fn listener_close_drains_every_admitted_accept_after_queue_saturation() {
    let config = UringNetworkProviderConfig {
        stream: UringNetworkConfig {
            command_queue_capacity: 1,
            ..config()
        },
        max_streams: 16,
        ..provider_config()
    };
    let provider = UringNetwork::new(config).expect("start io_uring network provider");
    let listener = block_on(provider.submit_listen(ListenRequest {
        address: loopback_any(),
        backlog: 2,
    }))
    .expect("bind listener");

    let accepts: Vec<_> = (0..8).map(|_| listener.submit_accept()).collect();
    block_on(listener.submit_close()).expect("close saturated listener");

    let mut closed = 0;
    let mut full = 0;
    for accept in accepts {
        let error = block_on(accept)
            .err()
            .expect("no accept can succeed without a client");
        match error.error().error() {
            NetworkError::ListenerClosed => closed += 1,
            NetworkError::ResourceExhausted {
                resource: "io_uring listener accept command queue",
                limit: 1,
            } => full += 1,
            other => panic!("unexpected saturated accept error: {other}"),
        }
    }
    assert!(closed >= 1, "at least one accept must have been admitted");
    assert!(full >= 1, "the one-entry accept queue must saturate");
}
