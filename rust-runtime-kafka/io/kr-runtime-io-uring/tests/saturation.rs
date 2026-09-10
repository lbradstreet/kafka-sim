#![cfg(target_os = "linux")]
//! Behavior of the shared datagram reactor when armed receives outnumber the
//! ring's submission depth.
//!
//! The rest of the suite exercises one operation at a time, so it never reaches
//! the state where an idle socket competes with an unrelated socket's send.
//! That state is the provider's normal resting condition — every bound socket
//! may hold one armed receive — and it is where removing the receive poll loop
//! wedged the ring.

mod common;

use std::net::{Ipv4Addr, SocketAddr};
use std::time::{Duration, Instant};

use common::block_on;
use kr_runtime::CompletionCertainty;
use kr_runtime_io::datagram::{
    DatagramBindRequest, DatagramError, DatagramProviderSubmit, DatagramSocketSubmit,
    RecvFromRequest, SendToRequest,
};
use kr_runtime_io::network::{
    ConnectRequest, ListenRequest, NetworkListenerSubmit, NetworkProviderSubmit,
};
use kr_runtime_io_uring::{
    UringDatagram, UringDatagramConfig, UringNetwork, UringNetworkConfig,
    UringNetworkProviderConfig,
};

/// Far more sockets than ring entries, on purpose. Every socket may arm a
/// receive, so the reactor must carry four times its submission depth in
/// simultaneously in-flight operations and still admit sends.
fn saturating_config() -> UringDatagramConfig {
    UringDatagramConfig {
        command_queue_capacity: 8,
        ring_entries: 4,
        max_sockets: 16,
        max_datagram_bytes: 65_507,
        max_operation_bytes: 256 * 1_024,
    }
}

fn localhost() -> SocketAddr {
    SocketAddr::from((Ipv4Addr::LOCALHOST, 0))
}

fn bind_all(
    provider: &UringDatagram,
    count: usize,
) -> Vec<<UringDatagram as DatagramProviderSubmit>::Socket> {
    (0..count)
        .map(|index| {
            block_on(provider.submit_bind(DatagramBindRequest {
                address: localhost(),
            }))
            .unwrap_or_else(|error| panic!("bind socket {index}: {error:?}"))
        })
        .collect()
}

#[test]
fn a_send_completes_while_every_socket_holds_an_armed_receive() {
    let config = saturating_config();
    let provider = UringDatagram::new(config).expect("start provider");
    let sockets = bind_all(&provider, config.max_sockets);

    // Admission is eager, so every one of these is armed in the kernel by the
    // time the call returns — sixteen simultaneously in-flight recvmsg entries
    // against a four-entry ring.
    let mut armed: Vec<_> = sockets
        .iter()
        .map(|socket| {
            socket.submit_recv_from(RecvFromRequest {
                buffer: Vec::new(),
                max_bytes: 64,
            })
        })
        .collect();

    // With one shared budget this send could not be staged: every slot is held
    // by a receive waiting for a peer that will never send, and nothing
    // completes to release one.
    let destination = sockets[1].local_addr();
    let sent = block_on(sockets[0].submit_send_to(SendToRequest {
        buffer: b"saturated".to_vec(),
        destination,
    }))
    .expect("send while every socket holds an armed receive");
    assert_eq!(sent.bytes_sent, 9);

    // The receive that was already armed on the destination takes the packet.
    let received = block_on(armed.remove(1)).expect("armed receive takes the packet");
    assert_eq!(received.buffer, b"saturated");
    assert_eq!(received.bytes_received, 9);

    // Closing retires the fifteen receives still armed. Each must terminalize
    // and return its buffer rather than stranding a kernel-visible entry.
    for socket in &sockets {
        block_on(socket.submit_close()).expect("close saturated socket");
    }
    for pending in armed {
        let error = block_on(pending).expect_err("an armed receive is retired by close");
        assert_eq!(error.error().error(), &DatagramError::SocketClosed);
        assert_eq!(error.certainty(), CompletionCertainty::NotApplied);
        assert!(
            error.error().buffer().is_some(),
            "a retired receive must return its buffer"
        );
    }
}

#[test]
fn repeated_saturation_cycles_leave_no_operation_behind() {
    // A soak over the arm/send/close cycle. Each round builds a fresh provider,
    // so a leaked in-flight entry shows up as a reactor that will not join and
    // a round that never finishes. `block_on`'s timeout turns that into a
    // failure rather than a hung suite.
    let started = Instant::now();
    for round in 0..24 {
        // Vary how much of the socket pool is armed, so the ring is exercised
        // both below and well above its submission depth.
        let sockets_in_round = 1 + round % saturating_config().max_sockets;
        let provider = UringDatagram::new(saturating_config()).expect("start provider");
        let sockets = bind_all(&provider, sockets_in_round);

        let armed: Vec<_> = sockets
            .iter()
            .map(|socket| {
                socket.submit_recv_from(RecvFromRequest {
                    buffer: Vec::new(),
                    max_bytes: 64,
                })
            })
            .collect();

        let destination = sockets[0].local_addr();
        block_on(sockets[sockets_in_round - 1].submit_send_to(SendToRequest {
            buffer: b"round".to_vec(),
            destination,
        }))
        .unwrap_or_else(|error| panic!("round {round} send: {error:?}"));

        for socket in &sockets {
            block_on(socket.submit_close())
                .unwrap_or_else(|error| panic!("round {round} close: {error:?}"));
        }
        for pending in armed {
            // Either the packet landed here or close retired it; both are
            // terminal, and neither may hang.
            let _ = block_on(pending);
        }
        drop(sockets);
        drop(provider);
    }
    assert!(
        started.elapsed() < Duration::from_secs(60),
        "saturation soak took {:?}, which indicates operations are not being retired promptly",
        started.elapsed()
    );
}

/// The network provider's control ring is shared by every listener's accept
/// actor and by connects, the same shape that wedged the datagram reactor.
/// Accepts are linked to a short kernel timeout so they terminalize on their
/// own, which is what should keep them from holding capacity indefinitely —
/// asserted here rather than assumed.
fn saturating_network_config() -> UringNetworkProviderConfig {
    UringNetworkProviderConfig {
        stream: UringNetworkConfig {
            command_queue_capacity: 8,
            ring_entries: 4,
            max_operation_bytes: 256,
            max_io_chunk_bytes: 64,
            connect_timeout: Duration::from_millis(500),
        },
        // Four times the control ring's submission depth, each able to hold an
        // armed accept, and each accept costs two linked entries.
        max_listeners: 16,
        // Each armed accept reserves a stream permit for the connection it may
        // produce, so this has to exceed the number armed or the connect is
        // refused on stream capacity before it ever reaches the ring — which is
        // correct provider behavior, just not what this test is measuring.
        max_streams: 64,
        max_listener_backlog: 8,
    }
}

#[test]
fn a_connect_completes_while_every_listener_holds_an_armed_accept() {
    let provider = UringNetwork::new(saturating_network_config()).expect("start provider");
    let config = saturating_network_config();

    let listeners: Vec<_> = (0..config.max_listeners)
        .map(|index| {
            block_on(provider.submit_listen(ListenRequest {
                address: localhost(),
                backlog: 1,
            }))
            .unwrap_or_else(|error| panic!("listen {index}: {error:?}"))
        })
        .collect();

    // Every listener arms an accept: sixteen logical operations, thirty-two
    // linked submission entries, against a four-entry control ring.
    let armed: Vec<_> = listeners
        .iter()
        .map(NetworkListenerSubmit::submit_accept)
        .collect();

    // A connect shares that ring. If armed accepts can starve it, this hangs.
    let destination = listeners[0].local_address();
    let stream = block_on(provider.submit_connect(ConnectRequest {
        local: localhost(),
        remote: destination,
    }))
    .expect("connect while every listener holds an armed accept");

    // The accept armed on the target listener takes the connection.
    drop(stream);
    drop(armed);
    for listener in &listeners {
        block_on(listener.submit_close()).expect("close saturated listener");
    }
}
