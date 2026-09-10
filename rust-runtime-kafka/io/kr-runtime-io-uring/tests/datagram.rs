#![cfg(target_os = "linux")]

mod common;

use std::net::{Ipv4Addr, Ipv6Addr, SocketAddr, UdpSocket};
use std::thread;
use std::time::{Duration, Instant};

use common::block_on;
use kr_runtime::CompletionCertainty;
use kr_runtime_io::conformance::{check_cold_datagram_provider, check_datagram_provider};
use kr_runtime_io::datagram::{
    ColdDatagramNetwork, DatagramBindRequest, DatagramError, DatagramProviderSubmit,
    DatagramSocketSubmit, RecvFromRequest, SendToRequest,
};
use kr_runtime_io_uring::{UringDatagram, UringDatagramConfig};

const PROMPT_BOUND: Duration = Duration::from_secs(5);

type UringSocket = <UringDatagram as DatagramProviderSubmit>::Socket;

fn config() -> UringDatagramConfig {
    UringDatagramConfig {
        command_queue_capacity: 8,
        ring_entries: 8,
        max_sockets: 16,
        max_datagram_bytes: 65_507,
        max_operation_bytes: 256 * 1_024,
    }
}

fn provider() -> UringDatagram {
    UringDatagram::new(config()).expect("start io_uring datagram provider")
}

fn provider_with(config: UringDatagramConfig) -> UringDatagram {
    UringDatagram::new(config).expect("start configured io_uring datagram provider")
}

fn ipv4_any() -> SocketAddr {
    SocketAddr::from((Ipv4Addr::LOCALHOST, 0))
}

fn ipv6_any() -> SocketAddr {
    SocketAddr::from((Ipv6Addr::LOCALHOST, 0))
}

fn expired_instant() -> Instant {
    let now = Instant::now();
    now.checked_sub(Duration::from_secs(1)).unwrap_or(now)
}

fn bind(provider: &UringDatagram, address: SocketAddr) -> UringSocket {
    block_on(provider.submit_bind(DatagramBindRequest { address }))
        .expect("bind io_uring datagram socket")
}

fn close(socket: &UringSocket) {
    block_on(socket.submit_close()).expect("close io_uring datagram socket");
}

fn ipv6_loopback_available() -> bool {
    match UdpSocket::bind(ipv6_any()) {
        Ok(probe) => {
            drop(probe);
            true
        }
        Err(error)
            if matches!(
                error.kind(),
                std::io::ErrorKind::AddrNotAvailable | std::io::ErrorKind::Unsupported
            ) =>
        {
            eprintln!("skipping IPv6 datagram test: host has no IPv6 loopback ({error})");
            false
        }
        Err(error) => panic!("IPv6 datagram availability probe failed unexpectedly: {error}"),
    }
}

#[test]
fn real_datagram_provider_passes_shared_ipv4_conformance() {
    let provider = provider();
    block_on(check_datagram_provider(
        &provider,
        ipv4_any(),
        ipv4_any(),
        ipv4_any(),
        expired_instant(),
    ))
    .unwrap_or_else(|message| panic!("real IPv4 datagram conformance failed: {message}"));
}

#[test]
fn real_datagram_provider_passes_cold_ipv4_conformance() {
    let cold = ColdDatagramNetwork::new(provider());
    block_on(check_cold_datagram_provider(
        &cold,
        ipv4_any(),
        ipv4_any(),
        expired_instant(),
    ))
    .unwrap_or_else(|message| panic!("real IPv4 cold datagram conformance failed: {message}"));
}

#[test]
fn real_datagram_provider_passes_shared_ipv6_conformance() {
    if !ipv6_loopback_available() {
        return;
    }

    let provider = provider();
    block_on(check_datagram_provider(
        &provider,
        ipv6_any(),
        ipv6_any(),
        ipv6_any(),
        expired_instant(),
    ))
    .unwrap_or_else(|message| panic!("real IPv6 datagram conformance failed: {message}"));
}

#[test]
fn live_deadline_returns_exact_buffer_and_leaves_receive_path_reusable() {
    let provider = provider();
    let socket = bind(&provider, ipv4_any());
    let mut buffer = Vec::with_capacity(64);
    buffer.extend_from_slice(b"deadline-prefix");
    let pointer = buffer.as_ptr();
    let expected = buffer.clone();

    let started = Instant::now();
    let error = block_on(socket.submit_recv_from_until(
        RecvFromRequest {
            buffer,
            max_bytes: 64,
        },
        started + Duration::from_millis(50),
    ))
    .expect_err("empty socket must reach its live receive deadline");
    assert!(
        started.elapsed() < PROMPT_BOUND,
        "deadline receive did not terminalize promptly"
    );
    assert_eq!(error.certainty(), CompletionCertainty::NotApplied);
    assert_eq!(error.error().error(), &DatagramError::DeadlineExceeded);
    let (_, failure) = error.into_parts();
    let returned = failure
        .into_buffer()
        .expect("deadline failure returns its receive buffer");
    assert_eq!(returned.as_ptr(), pointer);
    assert_eq!(returned, expected);

    let empty = block_on(socket.submit_try_recv_from(RecvFromRequest {
        buffer: b"after-timeout".to_vec(),
        max_bytes: 64,
    }))
    .expect_err("timeout must not consume a datagram");
    assert_eq!(empty.certainty(), CompletionCertainty::NotApplied);
    assert_eq!(empty.error().error(), &DatagramError::WouldBlock);

    close(&socket);
}

#[test]
fn packet_before_deadline_wins_and_receive_path_remains_reusable() {
    let provider = provider();
    let sender = bind(&provider, ipv4_any());
    let receiver = bind(&provider, ipv4_any());

    let pending = receiver.submit_recv_from_until(
        RecvFromRequest {
            buffer: b"prefix:".to_vec(),
            max_bytes: 64,
        },
        Instant::now() + Duration::from_secs(2),
    );
    let sent = block_on(sender.submit_send_to(SendToRequest {
        buffer: b"first".to_vec(),
        destination: receiver.local_addr(),
    }))
    .expect("send packet before deadline");
    assert_eq!(sent.bytes_sent, 5);

    let received = block_on(pending).expect("packet completion wins before deadline");
    assert_eq!(received.buffer, b"prefix:first");
    assert_eq!(received.bytes_received, 5);
    assert_eq!(received.datagram_len, 5);
    assert_eq!(received.source, sender.local_addr());

    block_on(sender.submit_send_to(SendToRequest {
        buffer: b"second".to_vec(),
        destination: receiver.local_addr(),
    }))
    .expect("send after successful deadline receive");
    let second = block_on(receiver.submit_recv_from_until(
        RecvFromRequest {
            buffer: Vec::new(),
            max_bytes: 64,
        },
        Instant::now() + Duration::from_secs(2),
    ))
    .expect("receive path remains reusable after deadline receive");
    assert_eq!(second.buffer, b"second");
    assert_eq!(second.source, sender.local_addr());

    close(&sender);
    close(&receiver);
}

#[test]
fn dropped_receive_response_still_consumes_exactly_one_datagram() {
    let provider = provider();
    let sender = bind(&provider, ipv4_any());
    let receiver = bind(&provider, ipv4_any());

    drop(receiver.submit_recv_from(RecvFromRequest {
        buffer: Vec::new(),
        max_bytes: 64,
    }));
    block_on(sender.submit_send_to(SendToRequest {
        buffer: b"consumed-by-abandoned-response".to_vec(),
        destination: receiver.local_addr(),
    }))
    .expect("send to abandoned receive");

    let deadline = Instant::now() + PROMPT_BOUND;
    loop {
        assert!(
            Instant::now() < deadline,
            "abandoned receive did not terminalize"
        );
        match block_on(receiver.submit_try_recv_from(RecvFromRequest {
            buffer: b"probe".to_vec(),
            max_bytes: 64,
        })) {
            Err(error)
                if matches!(
                    error.error().error(),
                    DatagramError::ResourceExhausted { .. }
                ) =>
            {
                thread::yield_now();
            }
            Err(error) if error.error().error() == &DatagramError::WouldBlock => {
                assert_eq!(error.certainty(), CompletionCertainty::NotApplied);
                break;
            }
            Err(error) => panic!("unexpected abandoned-receive probe failure: {error}"),
            Ok(completion) => {
                panic!("dropped receive response failed to consume the packet: {completion:?}")
            }
        }
    }

    block_on(sender.submit_send_to(SendToRequest {
        buffer: b"live".to_vec(),
        destination: receiver.local_addr(),
    }))
    .expect("send after abandoned receive completion");
    let live = block_on(receiver.submit_recv_from(RecvFromRequest {
        buffer: Vec::new(),
        max_bytes: 64,
    }))
    .expect("receive path remains live");
    assert_eq!(live.buffer, b"live");

    close(&sender);
    close(&receiver);
}

#[test]
fn close_interrupts_active_receive_returns_allocation_and_fences_rebind() {
    let provider = provider();
    let socket = bind(&provider, ipv4_any());
    let address = socket.local_addr();
    let mut buffer = Vec::with_capacity(64);
    buffer.extend_from_slice(b"close-prefix");
    let pointer = buffer.as_ptr();
    let expected = buffer.clone();
    let pending = socket.submit_recv_from(RecvFromRequest {
        buffer,
        max_bytes: 64,
    });

    drop(socket.submit_close());
    let started = Instant::now();
    block_on(socket.submit_close()).expect("repeated close joins the eager dropped close");
    assert!(
        started.elapsed() < PROMPT_BOUND,
        "close did not interrupt an active receive promptly"
    );

    let error = block_on(pending).expect_err("active receive observes socket close");
    assert_eq!(error.certainty(), CompletionCertainty::NotApplied);
    assert_eq!(error.error().error(), &DatagramError::SocketClosed);
    let (_, failure) = error.into_parts();
    let returned = failure
        .into_buffer()
        .expect("closed receive returns its owned buffer");
    assert_eq!(returned.as_ptr(), pointer);
    assert_eq!(returned, expected);

    let rebound = bind(&provider, address);
    close(&rebound);
}

#[test]
fn deadline_receive_is_rejected_instead_of_stranded_behind_active_receive() {
    let provider = provider();
    let socket = bind(&provider, ipv4_any());
    let active = socket.submit_recv_from(RecvFromRequest {
        buffer: b"active".to_vec(),
        max_bytes: 64,
    });
    let mut buffer = Vec::with_capacity(64);
    buffer.extend_from_slice(b"bounded");
    let pointer = buffer.as_ptr();
    let expected = buffer.clone();

    let started = Instant::now();
    let error = block_on(socket.submit_recv_from_until(
        RecvFromRequest {
            buffer,
            max_bytes: 64,
        },
        started + Duration::from_secs(2),
    ))
    .expect_err("single-receive provider rejects concurrent deadline receive");
    assert!(
        started.elapsed() < PROMPT_BOUND,
        "concurrent deadline receive was stranded behind active receive"
    );
    assert_eq!(error.certainty(), CompletionCertainty::NotApplied);
    assert!(matches!(
        error.error().error(),
        DatagramError::ResourceExhausted { limit: 1, .. }
    ));
    let (_, failure) = error.into_parts();
    let returned = failure
        .into_buffer()
        .expect("admission rejection returns its receive buffer");
    assert_eq!(returned.as_ptr(), pointer);
    assert_eq!(returned, expected);

    close(&socket);
    let active = block_on(active).expect_err("close interrupts the active receive");
    assert_eq!(active.certainty(), CompletionCertainty::NotApplied);
    assert_eq!(active.error().error(), &DatagramError::SocketClosed);
}

#[test]
fn mismatched_destination_family_is_rejected_with_exact_send_buffer() {
    let provider = provider();
    let socket = bind(&provider, ipv4_any());
    let mut buffer = Vec::with_capacity(64);
    buffer.extend_from_slice(b"wrong-family");
    let pointer = buffer.as_ptr();
    let expected = buffer.clone();

    let error = block_on(socket.submit_send_to(SendToRequest {
        buffer,
        destination: SocketAddr::from((Ipv6Addr::LOCALHOST, 9)),
    }))
    .expect_err("IPv4 socket must reject an IPv6 destination");
    assert_eq!(error.certainty(), CompletionCertainty::NotApplied);
    assert_eq!(error.error().error(), &DatagramError::AddressFamilyMismatch);
    let (_, failure) = error.into_parts();
    let returned = failure
        .into_buffer()
        .expect("family mismatch returns its send buffer");
    assert_eq!(returned.as_ptr(), pointer);
    assert_eq!(returned, expected);

    close(&socket);
}

/// With nothing admitted, both actors block indefinitely on an empty command
/// queue. No interval expires to let them notice a close, so the close path has
/// to wake them by disconnecting the queues. If it did not, this test would
/// hang rather than fail, which is why the elapsed bound is asserted first.
#[test]
fn close_wakes_actors_blocked_on_an_empty_command_queue() {
    let provider = provider();
    let socket = bind(&provider, ipv4_any());
    let address = socket.local_addr();

    let started = Instant::now();
    close(&socket);
    assert!(
        started.elapsed() < PROMPT_BOUND,
        "close did not wake actors blocked on their command queues"
    );

    // A successful close completion is delivered only once every actor has
    // exited, and rebinding proves the socket itself was released with them.
    let rebound = bind(&provider, address);
    close(&rebound);
}

#[test]
fn retained_vec_capacity_is_bounded_and_returned_exactly() {
    let config = config();
    let provider = provider_with(config);
    let socket = bind(&provider, ipv4_any());

    let mut send_buffer = Vec::with_capacity(config.max_operation_bytes + 1);
    send_buffer.extend_from_slice(b"send");
    let send_pointer = send_buffer.as_ptr();
    let send = block_on(socket.submit_send_to(SendToRequest {
        buffer: send_buffer,
        destination: socket.local_addr(),
    }))
    .expect_err("oversized retained send allocation must be rejected");
    assert_eq!(send.certainty(), CompletionCertainty::NotApplied);
    assert!(matches!(
        send.error().error(),
        DatagramError::ResourceExhausted { .. }
    ));
    let (_, send) = send.into_parts();
    let returned_send = send.into_buffer().expect("send buffer returned");
    assert_eq!(returned_send.as_ptr(), send_pointer);

    let mut receive_buffer = Vec::with_capacity(config.max_operation_bytes + 1);
    receive_buffer.extend_from_slice(b"receive");
    let receive_pointer = receive_buffer.as_ptr();
    let receive = block_on(socket.submit_try_recv_from(RecvFromRequest {
        buffer: receive_buffer,
        max_bytes: 1,
    }))
    .expect_err("oversized retained receive allocation must be rejected");
    assert_eq!(receive.certainty(), CompletionCertainty::NotApplied);
    assert!(matches!(
        receive.error().error(),
        DatagramError::ResourceExhausted { .. }
    ));
    let (_, receive) = receive.into_parts();
    let returned_receive = receive.into_buffer().expect("receive buffer returned");
    assert_eq!(returned_receive.as_ptr(), receive_pointer);

    close(&socket);
}

/// A datagram that reaches the receive has already left the kernel's socket
/// buffer and cannot be put back. A close arriving at the same moment may
/// therefore report the packet or report that nothing was consumed, but it must
/// never consume the datagram and then discard it: the peer has no way to learn
/// that the payload it believes was delivered is gone.
#[test]
fn close_racing_a_delivered_datagram_reports_it_rather_than_dropping_it() {
    const ATTEMPTS: usize = 64;
    const PAYLOAD: &[u8] = b"race-payload";

    let mut delivered = 0_usize;
    let mut closed = 0_usize;
    for attempt in 0..ATTEMPTS {
        let provider = provider();
        let socket = bind(&provider, ipv4_any());
        let address = socket.local_addr();
        let pending = socket.submit_recv_from(RecvFromRequest {
            buffer: Vec::with_capacity(64),
            max_bytes: 64,
        });

        let sender = UdpSocket::bind(ipv4_any()).expect("bind racing sender");
        sender
            .send_to(PAYLOAD, address)
            .expect("send the racing datagram");
        // No synchronization: the close is issued while the datagram is in
        // flight, so which side wins is left to the kernel.
        drop(socket.submit_close());

        match block_on(pending) {
            Ok(received) => {
                assert_eq!(
                    received.buffer, PAYLOAD,
                    "attempt {attempt} delivered a corrupted datagram"
                );
                assert_eq!(received.datagram_len, PAYLOAD.len());
                delivered += 1;
            }
            Err(error) => {
                assert_eq!(
                    error.error().error(),
                    &DatagramError::SocketClosed,
                    "attempt {attempt} failed for a reason other than the close"
                );
                assert_eq!(
                    error.certainty(),
                    CompletionCertainty::NotApplied,
                    "attempt {attempt} reported a close that may have consumed the datagram"
                );
                closed += 1;
            }
        }
    }

    assert_eq!(
        delivered + closed,
        ATTEMPTS,
        "every attempt must reach one of the two legal outcomes"
    );
    // The datagram is sent before the close is requested, so a run in which the
    // packet never once wins the race is not exercising the delivery path this
    // test exists to cover.
    assert!(
        delivered > 0,
        "no attempt delivered the datagram; the racing receive path went untested"
    );
}
