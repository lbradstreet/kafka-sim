#![cfg(target_os = "linux")]

use kr_runtime::{CompletionCertainty, HostConfig, HostRuntime};
use kr_runtime_io::{
    conformance::{
        check_cold_network_provider, check_cold_stream_pair, check_cold_vectored_stream_provider,
        check_connected_stream_pair, check_network_provider, check_vectored_stream_provider,
    },
    network::{
        ByteStreamSubmit, ByteStreamVectoredSubmit, ColdNetwork, ColdStream, ConnectRequest,
        ListenRequest, NetworkError, NetworkListenerSubmit, NetworkProviderSubmit, ReadRequest,
        SharedBytes, VectoredWriteRequest, WriteRequest, WriteSegment,
    },
};
use kr_runtime_io_readiness::{ReadinessConfig, ReadinessNet, ReadinessStream};
use std::{
    future::Future,
    net::{Ipv4Addr, SocketAddr},
    pin::pin,
    sync::Arc,
    task::{Context, Poll, Wake, Waker},
    thread::{self, Thread},
    time::{Duration, Instant},
};

struct ThreadWake(Thread);
impl Wake for ThreadWake {
    fn wake(self: Arc<Self>) {
        self.0.unpark();
    }
    fn wake_by_ref(self: &Arc<Self>) {
        self.0.unpark();
    }
}
fn block_on<F: Future>(future: F) -> F::Output {
    let waker = Waker::from(Arc::new(ThreadWake(thread::current())));
    let mut cx = Context::from_waker(&waker);
    let mut future = pin!(future);
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        match future.as_mut().poll(&mut cx) {
            Poll::Ready(value) => return value,
            Poll::Pending => {
                let left = deadline.saturating_duration_since(Instant::now());
                assert!(!left.is_zero(), "readiness test exceeded ten seconds");
                thread::park_timeout(left);
            }
        }
    }
}
fn address() -> SocketAddr {
    SocketAddr::from((Ipv4Addr::LOCALHOST, 0))
}
fn config() -> ReadinessConfig {
    ReadinessConfig {
        max_streams: 32,
        max_listeners: 8,
        max_listener_backlog: 32,
        max_control_operations: 32,
        max_read_operations: 32,
        max_write_operations: 32,
        max_operation_bytes: 65536,
        max_outstanding_read_bytes: 1024 * 1024,
        max_outstanding_write_bytes: 1024 * 1024,
        max_chunk_bytes: 2,
        socket_buffer_bytes: 4096,
        ..ReadinessConfig::default()
    }
}
fn pair(net: &ReadinessNet) -> (ReadinessStream, ReadinessStream) {
    let listener = block_on(net.submit_listen(ListenRequest {
        address: address(),
        backlog: 8,
    }))
    .unwrap();
    let accept = listener.submit_accept();
    let client = block_on(net.submit_connect(ConnectRequest {
        local: address(),
        remote: listener.local_address(),
    }))
    .unwrap();
    let server = block_on(accept).unwrap();
    block_on(listener.submit_close()).unwrap();
    (client, server)
}
#[test]
fn native_tcp_passes_shared_warm_and_cold_stream_and_network_contracts() {
    let net = ReadinessNet::new(config()).unwrap();
    let (left, right) = pair(&net);
    block_on(check_connected_stream_pair(&left, &right)).unwrap();
    let (left, right) = pair(&net);
    block_on(check_cold_stream_pair(
        &ColdStream::new(left),
        &ColdStream::new(right),
    ))
    .unwrap();
    block_on(check_network_provider(
        &net,
        ListenRequest {
            address: address(),
            backlog: 8,
        },
        address(),
        address(),
    ))
    .unwrap();
    block_on(check_cold_network_provider(
        &ColdNetwork::new(net.clone()),
        ListenRequest {
            address: address(),
            backlog: 8,
        },
        address(),
        address(),
    ))
    .unwrap();
    assert_eq!(net.status().streams, 0);
    assert_eq!(net.status().listeners, 0);
}
#[test]
fn native_sendmsg_passes_shared_partial_vectored_and_cold_contracts() {
    let net = ReadinessNet::new(config()).unwrap();
    let (left, right) = pair(&net);
    block_on(check_vectored_stream_provider(&left, &right)).unwrap();
    let (left, right) = pair(&net);
    block_on(check_cold_vectored_stream_provider(
        &ColdStream::new(left),
        &ColdStream::new(right),
    ))
    .unwrap();
    assert_eq!(net.status().outstanding_write_bytes, 0);
}
#[test]
fn abandoned_pending_read_keeps_its_reservation_until_close_completes() {
    let net = ReadinessNet::new(ReadinessConfig {
        max_read_operations: 1,
        max_outstanding_read_bytes: 4,
        ..config()
    })
    .unwrap();
    let (left, right) = pair(&net);
    drop(left.submit_read(ReadRequest {
        buffer: Vec::new(),
        max_bytes: 4,
    }));
    assert_eq!(net.status().read_operations, 1);
    assert_eq!(net.status().outstanding_read_bytes, 4);
    let error = block_on(left.submit_read(ReadRequest {
        buffer: Vec::new(),
        max_bytes: 1,
    }))
    .unwrap_err();
    assert_eq!(error.certainty(), CompletionCertainty::NotApplied);
    assert!(matches!(
        error.error().error(),
        NetworkError::ResourceExhausted {
            resource: "read operations",
            limit: 1
        }
    ));
    block_on(left.submit_close()).unwrap();
    assert_eq!(net.status().read_operations, 0);
    assert_eq!(net.status().outstanding_read_bytes, 0);
    block_on(right.submit_close()).unwrap();
}
#[test]
fn unconsumed_vectored_output_holds_write_credit_without_consuming_read_reserve() {
    let net = ReadinessNet::new(ReadinessConfig {
        max_outstanding_write_bytes: 4,
        max_outstanding_read_bytes: 4,
        ..config()
    })
    .unwrap();
    let (left, right) = pair(&net);
    let bytes = SharedBytes::from(vec![1, 2, 3, 4]);
    let pending = left.submit_write_vectored(VectoredWriteRequest {
        segments: vec![WriteSegment {
            bytes: bytes.clone(),
            range: 0..4,
        }],
    });
    assert_eq!(net.status().outstanding_write_bytes, 4);
    let error = block_on(left.submit_write(WriteRequest { buffer: vec![9] })).unwrap_err();
    assert_eq!(error.certainty(), CompletionCertainty::NotApplied);
    assert!(matches!(
        error.error().error(),
        NetworkError::ResourceExhausted {
            resource: "outstanding write bytes",
            limit: 4
        }
    ));
    let read = left.submit_read(ReadRequest {
        buffer: vec![],
        max_bytes: 1,
    });
    block_on(right.submit_write(WriteRequest { buffer: vec![7] })).unwrap_err();
    // Write byte capacity is provider-wide, so the peer also cannot consume it.
    // Dropping a ready or pending response alone cannot steal the read reserve.
    assert_eq!(net.status().read_operations, 1);
    let result = block_on(pending).unwrap();
    assert_eq!(result.bytes_written, 2);
    drop(result);
    assert_eq!(net.status().outstanding_write_bytes, 0);
    block_on(right.submit_write(WriteRequest { buffer: vec![7] })).unwrap();
    assert_eq!(block_on(read).unwrap().buffer, [7]);
    assert_eq!(bytes.strong_count(), 1);
    block_on(left.submit_close()).unwrap();
    block_on(right.submit_close()).unwrap();
}
#[test]
fn close_has_reserved_admission_when_data_and_control_operations_are_full() {
    let net = ReadinessNet::new(ReadinessConfig {
        max_control_operations: 1,
        max_read_operations: 1,
        ..config()
    })
    .unwrap();
    // One control slot cannot hold accept+connect concurrently, so create the
    // peer with the standard library while testing the real provider listener.
    let listener = block_on(net.submit_listen(ListenRequest {
        address: address(),
        backlog: 4,
    }))
    .unwrap();
    let peer = std::net::TcpStream::connect(listener.local_address()).unwrap();
    let stream = block_on(listener.submit_accept()).unwrap();
    let accept = listener.submit_accept();
    let read = stream.submit_read(ReadRequest {
        buffer: vec![],
        max_bytes: 1,
    });
    assert_eq!(net.status().control_operations, 1);
    assert_eq!(net.status().read_operations, 1);
    block_on(stream.submit_close()).unwrap();
    assert!(block_on(read).is_err());
    block_on(listener.submit_close()).unwrap();
    assert!(block_on(accept).is_err());
    drop(peer);
    assert_eq!(net.status().streams, 0);
    assert_eq!(net.status().listeners, 0);
}
#[test]
fn host_runtime_drives_the_same_owned_operations_and_finishes_cleanly() {
    let mut runtime = HostRuntime::new(HostConfig::default()).unwrap();
    let net = ReadinessNet::new(config()).unwrap();
    let (left, right) = pair(&net);
    runtime
        .block_on(check_vectored_stream_provider(&left, &right))
        .unwrap()
        .unwrap();
    drop(left);
    drop(right);
    drop(net);
    runtime.finish().unwrap();
}
#[test]
fn invalid_limits_and_oversized_owned_buffers_fail_before_admission() {
    assert!(
        ReadinessNet::new(ReadinessConfig {
            max_segments: 0,
            ..config()
        })
        .is_err()
    );
    assert!(
        ReadinessNet::new(ReadinessConfig {
            max_segments: 1025,
            ..config()
        })
        .is_err()
    );
    assert!(
        ReadinessNet::new(ReadinessConfig {
            max_streams: usize::MAX,
            ..config()
        })
        .is_err()
    );
    let net = ReadinessNet::new(config()).unwrap();
    let (left, right) = pair(&net);
    let buffer = Vec::with_capacity(config().max_operation_bytes + 1);
    let ptr = buffer.as_ptr();
    let capacity = buffer.capacity();
    let error = block_on(left.submit_write(WriteRequest { buffer })).unwrap_err();
    assert_eq!(error.certainty(), CompletionCertainty::NotApplied);
    let returned = error.into_parts().1.into_buffer().unwrap();
    assert_eq!(returned.as_ptr(), ptr);
    assert_eq!(returned.capacity(), capacity);
    assert_eq!(net.status().write_operations, 0);
    block_on(left.submit_close()).unwrap();
    block_on(right.submit_close()).unwrap();
}

#[test]
fn connection_guard_survives_closed_handle_until_terminal_buffer_is_consumed() {
    let net = ReadinessNet::new(config()).unwrap();
    let (left, right) = pair(&net);
    let guard = Arc::new(());
    left.attach_lifetime_guard(guard.clone()).unwrap();
    let read = left.submit_read(ReadRequest {
        buffer: Vec::with_capacity(8),
        max_bytes: 8,
    });
    block_on(left.submit_close()).unwrap();
    drop(left);
    assert_eq!(Arc::strong_count(&guard), 2);
    assert!(block_on(read).is_err());
    assert_eq!(Arc::strong_count(&guard), 1);
    block_on(right.submit_close()).unwrap();
}
