use kr_runtime::{CompletionCertainty, RuntimeConfig, SimDuration, SimInstant, SimRuntime};
use kr_runtime_io::datagram::{
    DatagramBindRequest, DatagramDirection, DatagramError, DatagramProviderSubmit,
    DatagramSocketSubmit, DatagramTruncation, RecvFromRequest, ScriptedDatagramFault,
    SendToRequest, SimDatagramAfterEnqueueCertainty, SimDatagramConfig, SimDatagramFault,
    SimDatagramLinkConfig, SimDatagramNetwork, SimDatagramSocket,
};
use kr_runtime_io::network::{NetworkAddress, NodeId};
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll, Waker};

#[cfg(feature = "test-support")]
use kr_runtime_io::conformance::check_datagram_provider;

fn address(node: u64, port: u16) -> NetworkAddress {
    NetworkAddress {
        node: NodeId(node),
        port,
    }
}

fn direction(source: NetworkAddress, destination: NetworkAddress) -> DatagramDirection {
    DatagramDirection {
        source,
        destination,
    }
}

fn runtime_and_network(config: SimDatagramConfig) -> (SimRuntime, SimDatagramNetwork) {
    let runtime = SimRuntime::new(RuntimeConfig::default());
    let network =
        SimDatagramNetwork::new(runtime.handle(), config).expect("valid datagram simulator config");
    (runtime, network)
}

fn bind(
    runtime: &mut SimRuntime,
    network: &SimDatagramNetwork,
    address: NetworkAddress,
) -> SimDatagramSocket {
    runtime
        .block_on(network.submit_bind(DatagramBindRequest { address }))
        .expect("runtime drives bind")
        .expect("bind succeeds")
}

fn send(
    runtime: &mut SimRuntime,
    socket: &SimDatagramSocket,
    destination: NetworkAddress,
    payload: &[u8],
) {
    let result = runtime
        .block_on(socket.submit_send_to(SendToRequest {
            buffer: payload.to_vec(),
            destination,
        }))
        .expect("runtime drives send")
        .expect("send succeeds");
    assert_eq!(result.buffer, payload);
    assert_eq!(result.bytes_sent, payload.len());
}

fn receive(
    runtime: &mut SimRuntime,
    socket: &SimDatagramSocket,
    max_bytes: usize,
) -> kr_runtime_io::datagram::RecvFromResult<NetworkAddress> {
    runtime
        .block_on(socket.submit_recv_from(RecvFromRequest {
            buffer: Vec::new(),
            max_bytes,
        }))
        .expect("runtime drives receive")
        .expect("receive succeeds")
}

fn assert_would_block(runtime: &mut SimRuntime, socket: &SimDatagramSocket) {
    let error = runtime
        .block_on(socket.submit_try_recv_from(RecvFromRequest {
            buffer: b"owned".to_vec(),
            max_bytes: 64,
        }))
        .expect("runtime drives try receive")
        .expect_err("no datagram is available");
    assert_eq!(error.certainty(), CompletionCertainty::NotApplied);
    assert_eq!(error.error().error(), &DatagramError::WouldBlock);
    assert_eq!(error.error().buffer(), Some(&b"owned"[..]));
}

fn push_fault(
    network: &SimDatagramNetwork,
    direction: DatagramDirection,
    send_ordinal: u64,
    tag: u64,
    action: SimDatagramFault,
) {
    network
        .push_fault(ScriptedDatagramFault {
            tag,
            direction,
            send_ordinal,
            action,
        })
        .expect("valid bounded fault script");
}

fn poll_once<F: Future + Unpin>(future: &mut F) -> Poll<F::Output> {
    let waker = Waker::noop();
    let mut context = Context::from_waker(waker);
    Pin::new(future).poll(&mut context)
}

#[test]
fn provider_supports_owned_atomic_datagrams_and_one_socket_many_peers() {
    let (mut runtime, network) = runtime_and_network(SimDatagramConfig::default());
    let a_addr = address(1, 4_001);
    let b_addr = address(2, 4_002);
    let c_addr = address(3, 4_003);
    let a = bind(&mut runtime, &network, a_addr);
    let b = bind(&mut runtime, &network, b_addr);
    let c = bind(&mut runtime, &network, c_addr);

    let duplicate = runtime
        .block_on(network.submit_bind(DatagramBindRequest { address: a_addr }))
        .expect("runtime drives duplicate bind")
        .expect_err("binding is exclusive");
    assert_eq!(duplicate.certainty(), CompletionCertainty::NotApplied);
    assert_eq!(duplicate.error().error(), &DatagramError::AddressInUse);

    send(&mut runtime, &a, b_addr, b"");
    let empty = runtime
        .block_on(b.submit_recv_from(RecvFromRequest {
            buffer: b"prefix".to_vec(),
            max_bytes: 0,
        }))
        .expect("runtime drives empty receive")
        .expect("empty datagram received");
    assert_eq!(empty.buffer, b"prefix");
    assert_eq!(empty.bytes_received, 0);
    assert_eq!(empty.datagram_len, 0);
    assert_eq!(empty.source, a_addr);
    assert_eq!(empty.truncation, DatagramTruncation::Complete);

    send(&mut runtime, &a, b_addr, b"bravo");
    send(&mut runtime, &a, c_addr, b"charlie");
    assert_eq!(receive(&mut runtime, &b, 64).buffer, b"bravo");
    let from_a = receive(&mut runtime, &c, 64);
    assert_eq!(from_a.buffer, b"charlie");
    assert_eq!(from_a.source, a_addr);

    drop(b.submit_recv_from(RecvFromRequest {
        buffer: Vec::new(),
        max_bytes: 64,
    }));
    send(&mut runtime, &a, b_addr, b"abandoned-response");
    let live_receive = b.submit_recv_from(RecvFromRequest {
        buffer: Vec::new(),
        max_bytes: 64,
    });
    send(&mut runtime, &a, b_addr, b"visible");
    let visible = runtime
        .block_on(live_receive)
        .expect("runtime drives live receive")
        .expect("second receive succeeds");
    assert_eq!(visible.buffer, b"visible");
    assert_would_block(&mut runtime, &b);

    let original = b"write-after-close".to_vec();
    runtime
        .block_on(a.submit_close())
        .expect("runtime drives close")
        .expect("close succeeds");
    runtime
        .block_on(a.submit_close())
        .expect("runtime drives repeated close")
        .expect("repeated close succeeds");
    let rejected = runtime
        .block_on(a.submit_send_to(SendToRequest {
            buffer: original.clone(),
            destination: b_addr,
        }))
        .expect("runtime drives rejected send")
        .expect_err("closed socket rejects send");
    assert_eq!(rejected.certainty(), CompletionCertainty::NotApplied);
    assert_eq!(rejected.error().error(), &DatagramError::SocketClosed);
    assert_eq!(rejected.error().buffer(), Some(original.as_slice()));
}

#[cfg(feature = "test-support")]
#[test]
fn simulated_provider_passes_shared_datagram_conformance() {
    let (mut runtime, network) = runtime_and_network(SimDatagramConfig::default());
    runtime
        .block_on(check_datagram_provider(
            &network,
            address(21, 14_001),
            address(22, 14_002),
            address(23, 14_003),
            SimInstant::ZERO,
        ))
        .expect("runtime drives shared conformance")
        .expect("simulated datagram provider conforms");
}

#[test]
fn send_completion_and_packet_delivery_are_independent() {
    let (mut runtime, network) = runtime_and_network(SimDatagramConfig::default());
    let a_addr = address(1, 5_001);
    let b_addr = address(2, 5_002);
    let a = bind(&mut runtime, &network, a_addr);
    let b = bind(&mut runtime, &network, b_addr);
    let ab = direction(a_addr, b_addr);

    network
        .set_link(
            ab,
            SimDatagramLinkConfig {
                delivery_latency: SimDuration::from_nanos(10),
                send_completion_latency: SimDuration::from_nanos(5),
                ..SimDatagramLinkConfig::default()
            },
        )
        .expect("link config");
    send(&mut runtime, &a, b_addr, b"completion-first");
    assert_eq!(runtime.snapshot().now, SimInstant::from_nanos(5));
    assert_would_block(&mut runtime, &b);
    assert_eq!(receive(&mut runtime, &b, 64).buffer, b"completion-first");
    assert_eq!(runtime.snapshot().now, SimInstant::from_nanos(10));

    network
        .set_link(
            ab,
            SimDatagramLinkConfig {
                delivery_latency: SimDuration::from_nanos(5),
                send_completion_latency: SimDuration::from_nanos(10),
                ..SimDatagramLinkConfig::default()
            },
        )
        .expect("link config");
    let mut send_future = a.submit_send_to(SendToRequest {
        buffer: b"delivery-first".to_vec(),
        destination: b_addr,
    });
    let receive_future = b.submit_recv_from(RecvFromRequest {
        buffer: Vec::new(),
        max_bytes: 64,
    });
    let result = runtime
        .block_on(receive_future)
        .expect("runtime drives receive")
        .expect("delivery arrives first");
    assert_eq!(result.buffer, b"delivery-first");
    assert_eq!(runtime.snapshot().now, SimInstant::from_nanos(15));
    assert!(poll_once(&mut send_future).is_pending());
    runtime
        .block_on(send_future)
        .expect("runtime drives later completion")
        .expect("send eventually completes");
    assert_eq!(runtime.snapshot().now, SimInstant::from_nanos(20));
}

#[test]
fn per_send_delay_reorders_datagrams_deterministically() {
    let (mut runtime, network) = runtime_and_network(SimDatagramConfig::default());
    let a_addr = address(1, 6_001);
    let b_addr = address(2, 6_002);
    let a = bind(&mut runtime, &network, a_addr);
    let b = bind(&mut runtime, &network, b_addr);
    let ab = direction(a_addr, b_addr);
    push_fault(
        &network,
        ab,
        1,
        10,
        SimDatagramFault::Delay {
            additional: SimDuration::from_nanos(10),
        },
    );

    let first = a.submit_send_to(SendToRequest {
        buffer: b"first".to_vec(),
        destination: b_addr,
    });
    let second = a.submit_send_to(SendToRequest {
        buffer: b"second".to_vec(),
        destination: b_addr,
    });
    runtime
        .block_on(first)
        .expect("runtime drives first completion")
        .expect("first locally accepted");
    runtime
        .block_on(second)
        .expect("runtime drives second completion")
        .expect("second locally accepted");

    assert_eq!(receive(&mut runtime, &b, 64).buffer, b"second");
    assert_eq!(receive(&mut runtime, &b, 64).buffer, b"first");
}

#[test]
fn loss_and_partitions_are_silent_and_directional() {
    let (mut runtime, network) = runtime_and_network(SimDatagramConfig::default());
    let a_addr = address(1, 7_001);
    let b_addr = address(2, 7_002);
    let a = bind(&mut runtime, &network, a_addr);
    let b = bind(&mut runtime, &network, b_addr);
    let ab = direction(a_addr, b_addr);
    push_fault(&network, ab, 1, 20, SimDatagramFault::Drop);

    send(&mut runtime, &a, b_addr, b"lost");
    assert_would_block(&mut runtime, &b);
    network
        .set_partitioned(ab, true)
        .expect("partition direction");
    send(&mut runtime, &a, b_addr, b"partitioned");
    assert_would_block(&mut runtime, &b);

    send(&mut runtime, &b, a_addr, b"reverse-still-open");
    assert_eq!(receive(&mut runtime, &a, 64).buffer, b"reverse-still-open");

    network.set_partitioned(ab, false).expect("heal direction");
    send(&mut runtime, &a, b_addr, b"healed");
    assert_eq!(receive(&mut runtime, &b, 64).buffer, b"healed");
    assert_eq!(network.status().counters.dropped_by_fault_or_partition, 2);
}

#[test]
fn duplication_corruption_and_both_truncation_layers_are_explicit() {
    let (mut runtime, network) = runtime_and_network(SimDatagramConfig::default());
    let a_addr = address(1, 8_001);
    let b_addr = address(2, 8_002);
    let a = bind(&mut runtime, &network, a_addr);
    let b = bind(&mut runtime, &network, b_addr);
    let ab = direction(a_addr, b_addr);
    push_fault(
        &network,
        ab,
        1,
        30,
        SimDatagramFault::Corrupt {
            offset: 1,
            xor: 0x20,
        },
    );
    push_fault(&network, ab, 1, 31, SimDatagramFault::Truncate { len: 3 });
    push_fault(
        &network,
        ab,
        1,
        32,
        SimDatagramFault::Duplicate {
            additional_copies: 1,
        },
    );

    send(&mut runtime, &a, b_addr, b"abcd");
    let first = receive(&mut runtime, &b, 64);
    let second = receive(&mut runtime, &b, 64);
    assert_eq!(first.buffer, b"aBc");
    assert_eq!(second.buffer, first.buffer);
    assert_eq!(first.datagram_len, 3);
    assert_eq!(first.truncation, DatagramTruncation::Complete);
    assert_eq!(second.truncation, DatagramTruncation::Complete);
    assert_eq!(network.status().counters.delivery_events, 2);
    assert_would_block(&mut runtime, &b);

    send(&mut runtime, &a, b_addr, b"wxyz");
    let clipped = receive(&mut runtime, &b, 2);
    assert_eq!(clipped.buffer, b"wx");
    assert_eq!(clipped.bytes_received, 2);
    assert_eq!(clipped.datagram_len, 4);
    assert_eq!(clipped.truncation, DatagramTruncation::Truncated);
}

#[test]
fn duplicate_delivery_capacity_is_reserved_atomically() {
    let config = SimDatagramConfig {
        max_scheduled_datagrams: 2,
        default_link: SimDatagramLinkConfig {
            delivery_latency: SimDuration::from_nanos(100),
            ..SimDatagramLinkConfig::default()
        },
        ..SimDatagramConfig::default()
    };
    let (mut runtime, network) = runtime_and_network(config);
    let a_addr = address(1, 9_001);
    let b_addr = address(2, 9_002);
    let a = bind(&mut runtime, &network, a_addr);
    let b = bind(&mut runtime, &network, b_addr);
    let ab = direction(a_addr, b_addr);
    push_fault(
        &network,
        ab,
        2,
        40,
        SimDatagramFault::Duplicate {
            additional_copies: 1,
        },
    );

    let first = a.submit_send_to(SendToRequest {
        buffer: b"occupies-one-slot".to_vec(),
        destination: b_addr,
    });
    let duplicate_buffer = b"must-be-atomic".to_vec();
    let rejected = runtime
        .block_on(a.submit_send_to(SendToRequest {
            buffer: duplicate_buffer.clone(),
            destination: b_addr,
        }))
        .expect("runtime drives queue rejection")
        .expect_err("two-copy plan cannot fit one remaining slot");
    assert_eq!(rejected.certainty(), CompletionCertainty::NotApplied);
    assert_eq!(rejected.error().buffer(), Some(duplicate_buffer.as_slice()));
    assert!(matches!(
        rejected.error().error(),
        DatagramError::ResourceExhausted { .. }
    ));
    assert_eq!(network.status().scheduled_datagrams, 1);

    runtime
        .block_on(first)
        .expect("runtime drives first completion")
        .expect("first send succeeds");
    let only_packet = receive(&mut runtime, &b, 64);
    assert_eq!(only_packet.buffer, b"occupies-one-slot");
    assert_would_block(&mut runtime, &b);
    assert_eq!(network.status().scheduled_datagrams, 0);
}

#[test]
fn before_and_after_enqueue_faults_have_distinct_certainty_and_effect() {
    let (mut runtime, network) = runtime_and_network(SimDatagramConfig::default());
    let a_addr = address(1, 10_001);
    let b_addr = address(2, 10_002);
    let a = bind(&mut runtime, &network, a_addr);
    let b = bind(&mut runtime, &network, b_addr);
    let ab = direction(a_addr, b_addr);
    push_fault(&network, ab, 1, 50, SimDatagramFault::FailBefore);
    push_fault(
        &network,
        ab,
        2,
        51,
        SimDatagramFault::ErrorAfterEnqueue {
            certainty: SimDatagramAfterEnqueueCertainty::MayHaveApplied,
        },
    );

    let before_buffer = b"before".to_vec();
    let before = runtime
        .block_on(a.submit_send_to(SendToRequest {
            buffer: before_buffer.clone(),
            destination: b_addr,
        }))
        .expect("runtime drives fail-before")
        .expect_err("fail before is reported");
    assert_eq!(before.certainty(), CompletionCertainty::NotApplied);
    assert_eq!(before.error().error(), &DatagramError::Injected { tag: 50 });
    assert_eq!(before.error().buffer(), Some(before_buffer.as_slice()));
    assert_eq!(before.error().bytes_transferred(), 0);
    assert_would_block(&mut runtime, &b);

    let after_buffer = b"after".to_vec();
    let after = runtime
        .block_on(a.submit_send_to(SendToRequest {
            buffer: after_buffer.clone(),
            destination: b_addr,
        }))
        .expect("runtime drives after-enqueue failure")
        .expect_err("after enqueue error is reported");
    assert_eq!(after.certainty(), CompletionCertainty::MayHaveApplied);
    assert_eq!(after.error().error(), &DatagramError::Injected { tag: 51 });
    assert_eq!(after.error().buffer(), Some(after_buffer.as_slice()));
    assert_eq!(after.error().bytes_transferred(), after_buffer.len());
    assert_eq!(receive(&mut runtime, &b, 64).buffer, b"after");

    push_fault(&network, ab, 3, 52, SimDatagramFault::Drop);
    push_fault(
        &network,
        ab,
        3,
        53,
        SimDatagramFault::ErrorAfterEnqueue {
            certainty: SimDatagramAfterEnqueueCertainty::Applied,
        },
    );
    let dropped_error = runtime
        .block_on(a.submit_send_to(SendToRequest {
            buffer: b"accepted-then-lost".to_vec(),
            destination: b_addr,
        }))
        .expect("runtime drives dropped after-enqueue failure")
        .expect_err("after-enqueue error composes with loss");
    assert_eq!(dropped_error.certainty(), CompletionCertainty::Applied);
    assert_eq!(
        dropped_error.error().error(),
        &DatagramError::Injected { tag: 53 }
    );
    assert_would_block(&mut runtime, &b);
    assert_eq!(network.status().counters.fault_hits, 4);
}

#[test]
fn delayed_delivery_resolves_the_binding_when_the_event_fires() {
    let (mut runtime, network) = runtime_and_network(SimDatagramConfig::default());
    let source_addr = address(1, 21_001);
    let destination_addr = address(2, 21_002);
    let source = bind(&mut runtime, &network, source_addr);
    network
        .set_link(
            direction(source_addr, destination_addr),
            SimDatagramLinkConfig {
                delivery_latency: SimDuration::from_nanos(10),
                ..SimDatagramLinkConfig::default()
            },
        )
        .expect("delayed exact direction");

    send(&mut runtime, &source, destination_addr, b"bound-after-send");
    let first_binding = bind(&mut runtime, &network, destination_addr);
    assert_eq!(
        receive(&mut runtime, &first_binding, 64).buffer,
        b"bound-after-send"
    );

    send(
        &mut runtime,
        &source,
        destination_addr,
        b"delivered-after-rebind",
    );
    runtime
        .block_on(first_binding.submit_close())
        .expect("runtime drives destination close")
        .expect("destination closes before delivery");
    let rebound = bind(&mut runtime, &network, destination_addr);
    let result = receive(&mut runtime, &rebound, 64);
    assert_eq!(result.buffer, b"delivered-after-rebind");
    assert_eq!(result.source, source_addr);
}

#[test]
fn deadline_receive_expires_independently_of_an_earlier_receive() {
    let (mut runtime, network) = runtime_and_network(SimDatagramConfig::default());
    let a_addr = address(1, 11_001);
    let b_addr = address(2, 11_002);
    let a = bind(&mut runtime, &network, a_addr);
    let b = bind(&mut runtime, &network, b_addr);

    let first = b.submit_recv_from(RecvFromRequest {
        buffer: Vec::new(),
        max_bytes: 64,
    });
    let deadline_buffer = b"deadline-prefix".to_vec();
    let deadline = b.submit_recv_from_until(
        RecvFromRequest {
            buffer: deadline_buffer.clone(),
            max_bytes: 64,
        },
        SimInstant::from_nanos(5),
    );
    let expired = runtime
        .block_on(deadline)
        .expect("runtime drives deadline")
        .expect_err("second receive expires independently");
    assert_eq!(runtime.snapshot().now, SimInstant::from_nanos(5));
    assert_eq!(expired.certainty(), CompletionCertainty::NotApplied);
    assert_eq!(expired.error().error(), &DatagramError::DeadlineExceeded);
    assert_eq!(expired.error().buffer(), Some(deadline_buffer.as_slice()));

    send(&mut runtime, &a, b_addr, b"for-first");
    let first = runtime
        .block_on(first)
        .expect("runtime drives earlier receive")
        .expect("earlier receive remains live");
    assert_eq!(first.buffer, b"for-first");

    network
        .set_link(
            direction(a_addr, b_addr),
            SimDatagramLinkConfig {
                delivery_latency: SimDuration::from_nanos(6),
                ..SimDatagramLinkConfig::default()
            },
        )
        .expect("delayed link");
    let just_too_late = b.submit_recv_from_until(
        RecvFromRequest {
            buffer: Vec::new(),
            max_bytes: 64,
        },
        SimInstant::from_nanos(10),
    );
    let send_future = a.submit_send_to(SendToRequest {
        buffer: b"after-deadline".to_vec(),
        destination: b_addr,
    });
    let expired = runtime
        .block_on(just_too_late)
        .expect("runtime drives second deadline")
        .expect_err("delivery is later than deadline");
    assert_eq!(expired.error().error(), &DatagramError::DeadlineExceeded);
    runtime
        .block_on(send_future)
        .expect("runtime drives local completion")
        .expect("local send succeeds");
    assert_eq!(receive(&mut runtime, &b, 64).buffer, b"after-deadline");
}

#[test]
fn close_cancels_receives_waits_for_send_completion_and_releases_binding() {
    let (mut runtime, network) = runtime_and_network(SimDatagramConfig::default());
    let a_addr = address(1, 12_001);
    let b_addr = address(2, 12_002);
    let a = bind(&mut runtime, &network, a_addr);
    let b = bind(&mut runtime, &network, b_addr);
    network
        .set_link(
            direction(a_addr, b_addr),
            SimDatagramLinkConfig {
                send_completion_latency: SimDuration::from_nanos(10),
                ..SimDatagramLinkConfig::default()
            },
        )
        .expect("delayed completion link");

    let pending_buffer = b"pending".to_vec();
    let pending_receive = a.submit_recv_from(RecvFromRequest {
        buffer: pending_buffer.clone(),
        max_bytes: 64,
    });
    let send_future = a.submit_send_to(SendToRequest {
        buffer: b"in-flight".to_vec(),
        destination: b_addr,
    });
    let mut close_future = a.submit_close();
    let cancelled = runtime
        .block_on(pending_receive)
        .expect("runtime observes cancelled receive")
        .expect_err("close cancels pending receive");
    assert_eq!(cancelled.certainty(), CompletionCertainty::NotApplied);
    assert_eq!(cancelled.error().error(), &DatagramError::SocketClosed);
    assert_eq!(cancelled.error().buffer(), Some(pending_buffer.as_slice()));
    assert!(poll_once(&mut close_future).is_pending());

    runtime
        .block_on(close_future)
        .expect("runtime drives fenced close")
        .expect("close waits then succeeds");
    assert_eq!(runtime.snapshot().now, SimInstant::from_nanos(10));
    runtime
        .block_on(send_future)
        .expect("send response remains observable")
        .expect("earlier send completed before close");
    assert_eq!(receive(&mut runtime, &b, 64).buffer, b"in-flight");

    let rebound = bind(&mut runtime, &network, a_addr);
    assert_eq!(rebound.local_addr(), a_addr);
}

#[test]
fn invalid_config_and_fault_composition_fail_closed() {
    let runtime = SimRuntime::new(RuntimeConfig::default());
    let invalid = SimDatagramConfig {
        max_pending_receives_per_socket: 0,
        ..SimDatagramConfig::default()
    };
    assert!(matches!(
        SimDatagramNetwork::new(runtime.handle(), invalid),
        Err(DatagramError::InvalidConfig { .. })
    ));

    let network = SimDatagramNetwork::new(runtime.handle(), SimDatagramConfig::default())
        .expect("valid network");
    let ab = direction(address(1, 13_001), address(2, 13_002));
    push_fault(&network, ab, 1, 60, SimDatagramFault::FailBefore);
    let incompatible = network.push_fault(ScriptedDatagramFault {
        tag: 61,
        direction: ab,
        send_ordinal: 1,
        action: SimDatagramFault::Drop,
    });
    assert!(matches!(
        incompatible,
        Err(DatagramError::InvalidRequest { .. })
    ));
    assert_eq!(network.status().pending_faults, 1);
}

#[test]
fn dropped_plans_skip_irrelevant_delivery_validation() {
    let (mut runtime, network) = runtime_and_network(SimDatagramConfig::default());
    let a_addr = address(1, 16_001);
    let b_addr = address(2, 16_002);
    let a = bind(&mut runtime, &network, a_addr);
    let b = bind(&mut runtime, &network, b_addr);
    let ab = direction(a_addr, b_addr);
    push_fault(&network, ab, 1, 70, SimDatagramFault::Drop);
    push_fault(
        &network,
        ab,
        1,
        71,
        SimDatagramFault::Delay {
            additional: SimDuration::MAX,
        },
    );
    push_fault(
        &network,
        ab,
        1,
        72,
        SimDatagramFault::Delay {
            additional: SimDuration::from_nanos(1),
        },
    );
    push_fault(
        &network,
        ab,
        1,
        73,
        SimDatagramFault::Duplicate {
            additional_copies: usize::MAX,
        },
    );

    send(&mut runtime, &a, b_addr, b"silently-dropped");
    assert_eq!(network.status().scheduled_datagrams, 0);
    assert_would_block(&mut runtime, &b);
}

#[test]
fn duplicate_copy_count_overflow_fails_closed() {
    let config = SimDatagramConfig {
        max_duplicate_copies: usize::MAX,
        ..SimDatagramConfig::default()
    };
    let (mut runtime, network) = runtime_and_network(config);
    let a_addr = address(1, 17_001);
    let b_addr = address(2, 17_002);
    let a = bind(&mut runtime, &network, a_addr);
    let ab = direction(a_addr, b_addr);
    push_fault(
        &network,
        ab,
        1,
        80,
        SimDatagramFault::Duplicate {
            additional_copies: usize::MAX,
        },
    );
    let buffer = b"overflow".to_vec();
    let error = runtime
        .block_on(a.submit_send_to(SendToRequest {
            buffer: buffer.clone(),
            destination: b_addr,
        }))
        .expect("runtime drives overflow rejection")
        .expect_err("copy-count overflow is rejected");
    assert_eq!(error.certainty(), CompletionCertainty::NotApplied);
    assert_eq!(error.error().buffer(), Some(buffer.as_slice()));
    assert!(matches!(
        error.error().error(),
        DatagramError::ResourceExhausted { .. }
    ));
    assert_eq!(network.status().scheduled_datagrams, 0);
}

#[test]
fn same_instant_deadline_race_uses_runtime_registration_order() {
    let (mut runtime, network) = runtime_and_network(SimDatagramConfig::default());
    let a_addr = address(1, 18_001);
    let b_addr = address(2, 18_002);
    let a = bind(&mut runtime, &network, a_addr);
    let b = bind(&mut runtime, &network, b_addr);
    network
        .set_link(
            direction(a_addr, b_addr),
            SimDatagramLinkConfig {
                delivery_latency: SimDuration::from_nanos(5),
                ..SimDatagramLinkConfig::default()
            },
        )
        .expect("same-instant link");

    let first_send = a.submit_send_to(SendToRequest {
        buffer: b"delivery-registered-first".to_vec(),
        destination: b_addr,
    });
    let first_receive = b.submit_recv_from_until(
        RecvFromRequest {
            buffer: Vec::new(),
            max_bytes: 64,
        },
        SimInstant::from_nanos(5),
    );
    let result = runtime
        .block_on(first_receive)
        .expect("runtime drives same-instant delivery")
        .expect("earlier-registered delivery wins");
    assert_eq!(result.buffer, b"delivery-registered-first");
    runtime
        .block_on(first_send)
        .expect("first send completion observable")
        .expect("first send succeeds");

    let second_receive = b.submit_recv_from_until(
        RecvFromRequest {
            buffer: b"unchanged".to_vec(),
            max_bytes: 64,
        },
        SimInstant::from_nanos(10),
    );
    let second_send = a.submit_send_to(SendToRequest {
        buffer: b"deadline-registered-first".to_vec(),
        destination: b_addr,
    });
    let error = runtime
        .block_on(second_receive)
        .expect("runtime drives same-instant deadline")
        .expect_err("earlier-registered deadline wins");
    assert_eq!(error.certainty(), CompletionCertainty::NotApplied);
    assert_eq!(error.error().error(), &DatagramError::DeadlineExceeded);
    assert_eq!(error.error().buffer(), Some(&b"unchanged"[..]));
    runtime
        .block_on(second_send)
        .expect("second send completion observable")
        .expect("second send succeeds");
    assert_eq!(
        receive(&mut runtime, &b, 64).buffer,
        b"deadline-registered-first"
    );

    send(&mut runtime, &a, b_addr, b"already-queued");
    let already_elapsed = runtime.snapshot().now;
    let expired = runtime
        .block_on(b.submit_recv_from_until(
            RecvFromRequest {
                buffer: Vec::new(),
                max_bytes: 64,
            },
            already_elapsed,
        ))
        .expect("runtime drives already elapsed receive")
        .expect_err("synchronous elapsed deadline wins over queued packet");
    assert_eq!(expired.error().error(), &DatagramError::DeadlineExceeded);
    assert_eq!(receive(&mut runtime, &b, 64).buffer, b"already-queued");
}

#[test]
fn driver_task_admission_failure_rolls_back_the_whole_send() {
    let mut runtime = SimRuntime::new(RuntimeConfig {
        max_tasks: 1,
        ..RuntimeConfig::default()
    });
    let network = SimDatagramNetwork::new(runtime.handle(), SimDatagramConfig::default())
        .expect("valid network");
    let a_addr = address(1, 19_001);
    let b_addr = address(2, 19_002);
    let a = bind(&mut runtime, &network, a_addr);
    network
        .set_link(
            direction(a_addr, b_addr),
            SimDatagramLinkConfig {
                delivery_latency: SimDuration::from_nanos(100),
                ..SimDatagramLinkConfig::default()
            },
        )
        .expect("delayed link");
    let buffer = b"atomic-spawn".to_vec();
    let mut send_future = a.submit_send_to(SendToRequest {
        buffer: buffer.clone(),
        destination: b_addr,
    });
    let Poll::Ready(result) = poll_once(&mut send_future) else {
        panic!("completion-task admission failure must be immediately observable");
    };
    let error = result.expect_err("partial driver admission rejects the send");
    assert_eq!(error.certainty(), CompletionCertainty::NotApplied);
    assert_eq!(
        error.error().error(),
        &DatagramError::CompletionDriverUnavailable
    );
    assert_eq!(error.error().buffer(), Some(buffer.as_slice()));
    assert_eq!(network.status().scheduled_datagrams, 0);
    assert_eq!(network.status().scheduled_bytes, 0);
    let mut receive = a.submit_recv_from(RecvFromRequest {
        buffer: b"receive-admission".to_vec(),
        max_bytes: 1,
    });
    let Poll::Ready(result) = poll_once(&mut receive) else {
        panic!("receive watcher admission failure must be immediately observable");
    };
    let error = result.expect_err("receive watcher cannot be admitted");
    assert_eq!(error.certainty(), CompletionCertainty::NotApplied);
    assert_eq!(
        error.error().error(),
        &DatagramError::CompletionDriverUnavailable
    );
    assert_eq!(error.error().buffer(), Some(&b"receive-admission"[..]));
    assert_eq!(network.status().pending_receives, 0);
    runtime
        .run_until_stalled()
        .expect("runtime retires the rolled-back delivery task");
    assert!(runtime.snapshot().tasks.is_empty());
}

#[test]
fn runtime_shutdown_terminalizes_send_and_deadline_bookkeeping() {
    let (mut runtime, network) = runtime_and_network(SimDatagramConfig::default());
    let a_addr = address(1, 20_001);
    let b_addr = address(2, 20_002);
    let a = bind(&mut runtime, &network, a_addr);
    let b = bind(&mut runtime, &network, b_addr);
    network
        .set_link(
            direction(a_addr, b_addr),
            SimDatagramLinkConfig {
                delivery_latency: SimDuration::from_nanos(20),
                send_completion_latency: SimDuration::from_nanos(10),
                ..SimDatagramLinkConfig::default()
            },
        )
        .expect("delayed link");
    let mut send_future = a.submit_send_to(SendToRequest {
        buffer: b"owned-send".to_vec(),
        destination: b_addr,
    });
    let mut blocking_future = b.submit_recv_from(RecvFromRequest {
        buffer: b"owned-blocking-receive".to_vec(),
        max_bytes: 64,
    });
    let mut deadline_future = b.submit_recv_from_until(
        RecvFromRequest {
            buffer: b"owned-receive".to_vec(),
            max_bytes: 64,
        },
        SimInstant::from_nanos(30),
    );

    runtime.shutdown().expect("runtime shuts down");
    let Poll::Ready(send_result) = poll_once(&mut send_future) else {
        panic!("shutdown terminalizes the send");
    };
    let send_error = send_result.expect_err("send driver stopped");
    assert_eq!(send_error.certainty(), CompletionCertainty::Applied);
    assert_eq!(
        send_error.error().error(),
        &DatagramError::CompletionDriverUnavailable
    );
    assert_eq!(send_error.error().buffer(), Some(&b"owned-send"[..]));

    let Poll::Ready(blocking_result) = poll_once(&mut blocking_future) else {
        panic!("shutdown terminalizes the blocking receive");
    };
    let blocking_error = blocking_result.expect_err("blocking receive driver stopped");
    assert_eq!(blocking_error.certainty(), CompletionCertainty::NotApplied);
    assert_eq!(
        blocking_error.error().error(),
        &DatagramError::CompletionDriverUnavailable
    );
    assert_eq!(
        blocking_error.error().buffer(),
        Some(&b"owned-blocking-receive"[..])
    );

    let Poll::Ready(receive_result) = poll_once(&mut deadline_future) else {
        panic!("shutdown terminalizes the deadline receive");
    };
    let receive_error = receive_result.expect_err("receive driver stopped");
    assert_eq!(receive_error.certainty(), CompletionCertainty::NotApplied);
    assert_eq!(
        receive_error.error().error(),
        &DatagramError::CompletionDriverUnavailable
    );
    assert_eq!(receive_error.error().buffer(), Some(&b"owned-receive"[..]));
    let status = network.status();
    assert_eq!(status.scheduled_datagrams, 0);
    assert_eq!(status.pending_receives, 0);

    let mut rejected_after_shutdown = a.submit_send_to(SendToRequest {
        buffer: b"never-scheduled".to_vec(),
        destination: b_addr,
    });
    let Poll::Ready(result) = poll_once(&mut rejected_after_shutdown) else {
        panic!("stopped runtime rejects delivery-task admission immediately");
    };
    let error = result.expect_err("driver task cannot be admitted after shutdown");
    assert_eq!(error.certainty(), CompletionCertainty::NotApplied);
    assert_eq!(
        error.error().error(),
        &DatagramError::CompletionDriverUnavailable
    );
    assert_eq!(network.status().scheduled_datagrams, 0);
    assert_eq!(network.status().scheduled_bytes, 0);
}

#[test]
fn operation_bounds_reject_oversized_owned_allocations_exactly() {
    let config = SimDatagramConfig {
        max_datagram_bytes: 8,
        max_operation_bytes: 8,
        ..SimDatagramConfig::default()
    };
    let (mut runtime, network) = runtime_and_network(config);
    let a_addr = address(1, 15_001);
    let b_addr = address(2, 15_002);
    let a = bind(&mut runtime, &network, a_addr);

    let mut send_buffer = Vec::with_capacity(32);
    send_buffer.push(7);
    let send_pointer = send_buffer.as_ptr();
    let send_error = runtime
        .block_on(a.submit_send_to(SendToRequest {
            buffer: send_buffer,
            destination: b_addr,
        }))
        .expect("runtime drives allocation rejection")
        .expect_err("oversized retained send allocation is rejected");
    assert_eq!(send_error.certainty(), CompletionCertainty::NotApplied);
    let returned = send_error.error().buffer().expect("send buffer returned");
    assert_eq!(returned, [7]);
    assert_eq!(returned.as_ptr(), send_pointer);

    let mut receive_buffer = Vec::with_capacity(32);
    receive_buffer.push(9);
    let receive_pointer = receive_buffer.as_ptr();
    let receive_error = runtime
        .block_on(a.submit_recv_from(RecvFromRequest {
            buffer: receive_buffer,
            max_bytes: 1,
        }))
        .expect("runtime drives receive allocation rejection")
        .expect_err("oversized retained receive allocation is rejected");
    assert_eq!(receive_error.certainty(), CompletionCertainty::NotApplied);
    let returned = receive_error
        .error()
        .buffer()
        .expect("receive buffer returned");
    assert_eq!(returned, [9]);
    assert_eq!(returned.as_ptr(), receive_pointer);
}
