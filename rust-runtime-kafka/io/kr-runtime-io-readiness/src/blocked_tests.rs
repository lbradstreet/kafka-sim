//! Real TCP pressure with an explicit syscall observation, never a timed guess.
use super::*;
use kr_runtime_io::{
    conformance::{
        check_bounded_blocked_vectored_stream_provider,
        check_cold_bounded_blocked_vectored_stream_provider,
    },
    network::{ColdStream, SharedBytes, WriteSegment},
};
use std::{
    future::Future,
    pin::pin,
    sync::mpsc,
    task::{Context, Poll, Wake, Waker},
    thread::{self, Thread},
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
            Poll::Ready(result) => return result,
            Poll::Pending => {
                let remaining = deadline.saturating_duration_since(Instant::now());
                assert!(
                    !remaining.is_zero(),
                    "blocked conformance exceeded ten seconds"
                );
                thread::park_timeout(remaining);
            }
        }
    }
}
fn pair(network: &ReadinessNet) -> (ReadinessStream, ReadinessStream) {
    let address = "127.0.0.1:0".parse().unwrap();
    let listener = block_on(network.submit_listen(ListenRequest {
        address,
        backlog: 2,
    }))
    .unwrap();
    let accept = listener.submit_accept();
    let left = block_on(network.submit_connect(ConnectRequest {
        local: address,
        remote: listener.local_address(),
    }))
    .unwrap();
    let right = block_on(accept).unwrap();
    block_on(listener.submit_close()).unwrap();
    (left, right)
}

#[test]
fn native_sendmsg_eagain_passes_shared_blocked_warm_and_cold_conformance() {
    const BURST: usize = 32;
    const BYTES: usize = 64 * 1024;
    for cold in [false, true] {
        let network = ReadinessNet::new(ReadinessConfig {
            max_streams: 2,
            max_listeners: 1,
            max_listener_backlog: 2,
            max_control_operations: 4,
            max_read_operations: 2,
            max_write_operations: BURST + 2,
            max_operation_bytes: BYTES,
            max_outstanding_read_bytes: 2 * BYTES,
            max_outstanding_write_bytes: (BURST + 2) * BYTES,
            max_segments: 8,
            max_chunk_bytes: BYTES,
            socket_buffer_bytes: 4096,
            ..ReadinessConfig::default()
        })
        .unwrap();
        let (left, right) = pair(&network);
        let (observer, observed) = mpsc::sync_channel(1);
        *lock(&network.owner.shared.vectored_eagain) = Some(observer);

        // A bounded burst exceeds the deliberately small socket windows while
        // the receiver is silent. Every completion can legally be a short send.
        // Retain completed responses too, so both queued and terminal ownership
        // participate in the final byte/reservation conservation assertions.
        let mut preceding = Vec::with_capacity(BURST);
        for _ in 0..BURST {
            let owner = SharedBytes::from(vec![0x5a; BYTES]);
            let segments = vec![
                WriteSegment {
                    bytes: owner.clone(),
                    range: 0..(BYTES / 2) as u32,
                },
                WriteSegment {
                    bytes: owner.clone(),
                    range: (BYTES / 2) as u32..BYTES as u32,
                },
            ];
            let pointer = segments.as_ptr();
            let capacity = segments.capacity();
            let response = left.submit_write_vectored(VectoredWriteRequest { segments });
            preceding.push((owner, pointer, capacity, response));
        }
        observed
            .recv_timeout(Duration::from_secs(10))
            .expect("real sendmsg EAGAIN was not observed");
        assert_eq!(network.status().write_operations, BURST);
        assert_eq!(network.status().outstanding_write_bytes, BURST * BYTES);

        let possible_prefix = vec![0x5a; BURST * BYTES];
        let observed_prefix = if cold {
            let left = ColdStream::new(left);
            let right = ColdStream::new(right);
            let result = block_on(check_cold_bounded_blocked_vectored_stream_provider(
                &left,
                &right,
                &possible_prefix,
            ))
            .unwrap();
            block_on(left.close()).unwrap();
            block_on(right.close()).unwrap();
            result
        } else {
            let result = block_on(check_bounded_blocked_vectored_stream_provider(
                &left,
                &right,
                &possible_prefix,
            ))
            .unwrap();
            block_on(left.submit_close()).unwrap();
            block_on(right.submit_close()).unwrap();
            result
        };
        let mut actual_prefix = 0;
        for (owner, pointer, capacity, response) in preceding {
            let result = block_on(response).unwrap();
            assert_eq!(result.segments.as_ptr(), pointer);
            assert_eq!(result.segments.capacity(), capacity);
            assert_eq!(result.segments.len(), 2);
            assert_eq!(result.segments[0].range, 0..(BYTES / 2) as u32);
            assert_eq!(result.segments[1].range, (BYTES / 2) as u32..BYTES as u32);
            assert!(
                result
                    .segments
                    .iter()
                    .all(|segment| segment.bytes.shares_allocation(&owner))
            );
            assert!(result.bytes_written > 0 && result.bytes_written <= BYTES);
            assert!(owner.as_slice().iter().all(|byte| *byte == 0x5a));
            actual_prefix += result.bytes_written;
            drop(result);
            assert_eq!(owner.strong_count(), 1);
        }
        assert_eq!(
            observed_prefix, actual_prefix,
            "cold={cold}: exact actual write progress"
        );
        let status = network.status();
        assert_eq!(status.streams, 0);
        assert_eq!(status.listeners, 0);
        assert_eq!(status.read_operations, 0);
        assert_eq!(status.write_operations, 0);
        assert_eq!(status.control_operations, 0);
        assert_eq!(status.close_operations, 0);
        assert_eq!(status.outstanding_read_bytes, 0);
        assert_eq!(status.outstanding_write_bytes, 0);
        assert_eq!(status.queued_commands, 0);
        assert!(!status.stopped);
        eprintln!(
            "readiness blocked vectored cold={cold}: EAGAIN observed, preceding={actual_prefix}, observed={observed_prefix}, zero retained operations/bytes"
        );
    }
}
