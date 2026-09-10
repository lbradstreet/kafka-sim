use kr_runtime::{
    CompletionCertainty, CompletionError, RuntimeConfig, SimDuration, SimInstant, SimRuntime,
};
#[cfg(feature = "test-support")]
use kr_runtime_io::conformance::{check_connected_stream_pair, check_network_provider};
use kr_runtime_io::network::{
    AfterFaultCertainty, ByteStreamSubmit, ConnectRequest, FaultOutcome, LinkConfig, LinkKey,
    LinkState, ListenRequest, NetworkAddress, NetworkConfig, NetworkError, NetworkListenerSubmit,
    NetworkOperationKind, NetworkProviderSubmit, NodeId, ReadRequest, ScriptedFault, SimNetwork,
    WriteRequest,
};
use std::future::Future;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll, Wake, Waker};

fn runtime_and_network(config: NetworkConfig) -> (SimRuntime, SimNetwork) {
    let runtime = SimRuntime::new(RuntimeConfig::default());
    let network = SimNetwork::new(runtime.handle(), config).expect("valid network config");
    (runtime, network)
}

#[test]
fn control_requests_accept_provider_native_address_types() {
    let address = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 8080);
    let listen: ListenRequest<SocketAddr> = ListenRequest {
        address,
        backlog: 8,
    };
    let connect: ConnectRequest<SocketAddr> = ConnectRequest {
        local: SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0),
        remote: address,
    };

    assert_eq!(listen.address, address);
    assert_eq!(connect.remote, address);
}

fn poll_once<F: Future + Unpin>(future: &mut F) -> Poll<F::Output> {
    let waker = Waker::noop();
    let mut context = Context::from_waker(waker);
    Pin::new(future).poll(&mut context)
}

fn assert_inflight_exhausted(error: &CompletionError<kr_runtime_io::network::NetworkFailure>) {
    assert_eq!(error.certainty(), CompletionCertainty::NotApplied);
    assert_eq!(
        error.error().error(),
        &NetworkError::ResourceExhausted {
            resource: "inflight operations",
            limit: 1,
        }
    );
}

#[test]
fn connected_pair_performs_owned_partial_io() {
    let config = NetworkConfig {
        directional_buffer_bytes: 8,
        default_link: LinkConfig {
            max_chunk_bytes: 3,
            ..LinkConfig::default()
        },
        ..NetworkConfig::default()
    };
    let (mut runtime, network) = runtime_and_network(config);
    let (left, right) = network.connected_pair(NodeId(1), NodeId(2)).expect("pair");

    let write = runtime
        .block_on(left.submit_write(WriteRequest {
            buffer: b"abcdef".to_vec(),
        }))
        .expect("runtime")
        .expect("write");
    assert_eq!(write.buffer, b"abcdef");
    assert_eq!(write.bytes_written, 3);

    let read = runtime
        .block_on(right.submit_read(ReadRequest {
            buffer: vec![b'!'],
            max_bytes: 7,
        }))
        .expect("runtime")
        .expect("read");
    assert_eq!(read.buffer, b"!abc");
    assert_eq!(read.bytes_read, 3);
    assert!(!read.end_of_stream);
}

#[cfg(feature = "test-support")]
#[test]
fn simulated_stream_passes_shared_conformance() {
    let config = NetworkConfig {
        directional_buffer_bytes: 32,
        default_link: LinkConfig {
            max_chunk_bytes: 2,
            ..LinkConfig::default()
        },
        ..NetworkConfig::default()
    };
    let (mut runtime, network) = runtime_and_network(config);
    let (left, right) = network.connected_pair(NodeId(1), NodeId(2)).expect("pair");
    runtime
        .block_on(async move { check_connected_stream_pair(&left, &right).await })
        .expect("runtime completes")
        .expect("simulated byte stream conforms");
}

#[cfg(feature = "test-support")]
#[test]
fn simulated_provider_passes_shared_control_plane_conformance() {
    let config = NetworkConfig {
        directional_buffer_bytes: 32,
        default_link: LinkConfig {
            max_chunk_bytes: 2,
            ..LinkConfig::default()
        },
        ..NetworkConfig::default()
    };
    let (mut runtime, network) = runtime_and_network(config);
    runtime
        .block_on(async move {
            check_network_provider(
                &network,
                ListenRequest {
                    address: NetworkAddress {
                        node: NodeId(20),
                        port: 7_000,
                    },
                    backlog: 4,
                },
                NetworkAddress {
                    node: NodeId(10),
                    port: 4_001,
                },
                NetworkAddress {
                    node: NodeId(11),
                    port: 4_002,
                },
            )
            .await
        })
        .expect("runtime completes")
        .expect("simulated network provider conforms");
}

#[test]
fn full_directional_buffer_write_waits_until_a_read_frees_capacity() {
    let config = NetworkConfig {
        directional_buffer_bytes: 4,
        default_link: LinkConfig {
            max_chunk_bytes: 16,
            ..LinkConfig::default()
        },
        ..NetworkConfig::default()
    };
    let (mut runtime, network) = runtime_and_network(config);
    let (left, right) = network.connected_pair(NodeId(1), NodeId(2)).expect("pair");

    let first = runtime
        .block_on(left.submit_write(WriteRequest {
            buffer: b"full".to_vec(),
        }))
        .expect("runtime")
        .expect("first write");
    assert_eq!(first.bytes_written, 4);

    let mut pending = left.submit_write(WriteRequest {
        buffer: b"x".to_vec(),
    });
    assert!(poll_once(&mut pending).is_pending());
    assert_eq!(network.status().inflight_operations, 1);

    let drained = runtime
        .block_on(right.submit_read(ReadRequest {
            buffer: Vec::new(),
            max_bytes: 1,
        }))
        .expect("runtime")
        .expect("read frees one byte");
    assert_eq!(drained.buffer, b"f");
    let Poll::Ready(Ok(completed)) = poll_once(&mut pending) else {
        panic!("pending write should complete after capacity is available");
    };
    assert_eq!(completed.buffer, b"x");
    assert_eq!(completed.bytes_written, 1);
    assert_eq!(network.status().inflight_operations, 0);

    let remaining = runtime
        .block_on(right.submit_read(ReadRequest {
            buffer: Vec::new(),
            max_bytes: 4,
        }))
        .expect("runtime")
        .expect("read remaining bytes");
    assert_eq!(remaining.buffer, b"ullx");
}

#[test]
fn admitted_read_retains_its_open_gate_across_a_later_clog() {
    let (runtime, network) = runtime_and_network(NetworkConfig::default());
    let (left, right) = network.connected_pair(NodeId(1), NodeId(2)).expect("pair");
    let link = LinkKey {
        from: NodeId(1),
        to: NodeId(2),
    };
    let mut read = right.submit_read(ReadRequest {
        buffer: Vec::new(),
        max_bytes: 1,
    });
    assert!(poll_once(&mut read).is_pending());

    network
        .set_link(
            link,
            LinkConfig {
                state: LinkState::Clogged,
                ..LinkConfig::default()
            },
        )
        .expect("clog link after read admission");
    let mut write = left.submit_write(WriteRequest {
        buffer: b"x".to_vec(),
    });
    assert!(poll_once(&mut write).is_pending());
    let Poll::Ready(Ok(read)) = poll_once(&mut read) else {
        panic!("later clog must not retroactively gate an Open-admitted read");
    };
    assert_eq!(read.buffer, b"x");

    network.set_link(link, LinkConfig::default()).expect("open");
    let Poll::Ready(Ok(write)) = poll_once(&mut write) else {
        panic!("opening the captured write gate must release completion");
    };
    assert_eq!(write.bytes_written, 1);
    assert_eq!(runtime.snapshot().now, SimInstant::ZERO);
}

#[test]
fn pending_read_captures_link_plan_and_fault_at_admission() {
    let config = NetworkConfig {
        default_link: LinkConfig {
            max_chunk_bytes: 1,
            ..LinkConfig::default()
        },
        ..NetworkConfig::default()
    };
    let (mut runtime, network) = runtime_and_network(config);
    let (left, right) = network.connected_pair(NodeId(1), NodeId(2)).expect("pair");
    let link = LinkKey {
        from: NodeId(1),
        to: NodeId(2),
    };
    network
        .push_fault(ScriptedFault::fail_after(
            NetworkOperationKind::Read,
            70,
            AfterFaultCertainty::Applied,
        ))
        .expect("read fault");
    let mut read = right.submit_read(ReadRequest {
        buffer: Vec::new(),
        max_bytes: 2,
    });
    assert!(poll_once(&mut read).is_pending());
    assert_eq!(network.status().pending_faults, 0);
    assert_eq!(network.status().fault_hits, 1);

    network
        .set_link(
            link,
            LinkConfig {
                latency: SimDuration::from_nanos(50),
                max_chunk_bytes: 2,
                state: LinkState::Open,
            },
        )
        .expect("change link after read admission");
    let write = left.submit_write(WriteRequest {
        buffer: b"ab".to_vec(),
    });
    let Poll::Ready(Err(error)) = poll_once(&mut read) else {
        panic!("admitted read should use its original zero-latency plan");
    };
    assert_eq!(error.certainty(), CompletionCertainty::Applied);
    assert_eq!(error.error().error(), &NetworkError::Injected { tag: 70 });
    assert_eq!(error.error().bytes_transferred(), 1);
    assert_eq!(
        error.error().clone().into_buffer().as_deref(),
        Some(&b"a"[..])
    );

    runtime
        .block_on(write)
        .expect("runtime drives changed write latency")
        .expect("write succeeds");
    assert_eq!(runtime.snapshot().now, SimInstant::from_nanos(50));
}

#[test]
fn capacity_waiter_captures_link_plan_and_fault_at_admission() {
    let config = NetworkConfig {
        directional_buffer_bytes: 2,
        default_link: LinkConfig {
            max_chunk_bytes: 1,
            ..LinkConfig::default()
        },
        ..NetworkConfig::default()
    };
    let (mut runtime, network) = runtime_and_network(config);
    let (left, right) = network.connected_pair(NodeId(1), NodeId(2)).expect("pair");
    for byte in [b"a".to_vec(), b"b".to_vec()] {
        runtime
            .block_on(left.submit_write(WriteRequest { buffer: byte }))
            .expect("runtime")
            .expect("fill direction");
    }
    network
        .push_fault(ScriptedFault::fail_after(
            NetworkOperationKind::Write,
            71,
            AfterFaultCertainty::Applied,
        ))
        .expect("write fault");
    let mut pending = left.submit_write(WriteRequest {
        buffer: b"cd".to_vec(),
    });
    assert!(poll_once(&mut pending).is_pending());
    assert_eq!(network.status().pending_faults, 0);
    assert_eq!(network.status().fault_hits, 1);

    network
        .set_link(
            LinkKey {
                from: NodeId(1),
                to: NodeId(2),
            },
            LinkConfig {
                latency: SimDuration::from_nanos(50),
                max_chunk_bytes: 2,
                state: LinkState::Open,
            },
        )
        .expect("change link after write admission");
    let read = right.submit_read(ReadRequest {
        buffer: Vec::new(),
        max_bytes: 2,
    });
    let Poll::Ready(Err(error)) = poll_once(&mut pending) else {
        panic!("capacity waiter should retain its original zero-latency plan");
    };
    assert_eq!(error.certainty(), CompletionCertainty::Applied);
    assert_eq!(error.error().error(), &NetworkError::Injected { tag: 71 });
    assert_eq!(error.error().bytes_transferred(), 1);

    let read = runtime
        .block_on(read)
        .expect("runtime drives changed read latency")
        .expect("read succeeds");
    assert_eq!(read.buffer, b"ab");
    assert_eq!(runtime.snapshot().now, SimInstant::from_nanos(50));
}

#[test]
fn capacity_blocked_writes_complete_in_fifo_order() {
    let config = NetworkConfig {
        max_inflight_operations: 3,
        directional_buffer_bytes: 1,
        ..NetworkConfig::default()
    };
    let (mut runtime, network) = runtime_and_network(config);
    let (left, right) = network.connected_pair(NodeId(1), NodeId(2)).expect("pair");
    runtime
        .block_on(left.submit_write(WriteRequest {
            buffer: b"a".to_vec(),
        }))
        .expect("runtime")
        .expect("fill direction");

    let mut first = left.submit_write(WriteRequest {
        buffer: b"b".to_vec(),
    });
    let mut second = left.submit_write(WriteRequest {
        buffer: b"c".to_vec(),
    });
    assert!(poll_once(&mut first).is_pending());
    assert!(poll_once(&mut second).is_pending());
    assert_eq!(network.status().inflight_operations, 2);

    let first_byte = runtime
        .block_on(right.submit_read(ReadRequest {
            buffer: Vec::new(),
            max_bytes: 1,
        }))
        .expect("runtime")
        .expect("drain initial byte");
    assert_eq!(first_byte.buffer, b"a");
    assert!(matches!(poll_once(&mut first), Poll::Ready(Ok(_))));
    assert!(poll_once(&mut second).is_pending());

    let second_byte = runtime
        .block_on(right.submit_read(ReadRequest {
            buffer: Vec::new(),
            max_bytes: 1,
        }))
        .expect("runtime")
        .expect("drain first pending write");
    assert_eq!(second_byte.buffer, b"b");
    assert!(matches!(poll_once(&mut second), Poll::Ready(Ok(_))));
    let third_byte = runtime
        .block_on(right.submit_read(ReadRequest {
            buffer: Vec::new(),
            max_bytes: 1,
        }))
        .expect("runtime")
        .expect("drain second pending write");
    assert_eq!(third_byte.buffer, b"c");
}

#[test]
fn capacity_blocked_writes_respect_the_inflight_bound() {
    let config = NetworkConfig {
        max_inflight_operations: 1,
        directional_buffer_bytes: 1,
        ..NetworkConfig::default()
    };
    let (mut runtime, network) = runtime_and_network(config);
    let (left, right) = network.connected_pair(NodeId(1), NodeId(2)).expect("pair");
    runtime
        .block_on(left.submit_write(WriteRequest {
            buffer: b"a".to_vec(),
        }))
        .expect("runtime")
        .expect("fill direction");
    let mut pending = left.submit_write(WriteRequest {
        buffer: b"b".to_vec(),
    });
    assert!(poll_once(&mut pending).is_pending());
    assert_eq!(network.status().inflight_operations, 1);

    let rejected = runtime
        .block_on(left.submit_write(WriteRequest {
            buffer: b"c".to_vec(),
        }))
        .expect("runtime")
        .expect_err("pending write retains the only permit");
    assert_inflight_exhausted(&rejected);
    assert_eq!(rejected.into_parts().1.into_buffer(), Some(b"c".to_vec()));

    drop(right);
    assert!(matches!(poll_once(&mut pending), Poll::Ready(Err(_))));
    assert_eq!(network.status().inflight_operations, 0);
}

#[test]
fn capacity_blocked_write_survives_clog_and_completes_when_link_opens() {
    let config = NetworkConfig {
        directional_buffer_bytes: 1,
        ..NetworkConfig::default()
    };
    let (_runtime, network) = runtime_and_network(config);
    let (left, right) = network.connected_pair(NodeId(1), NodeId(2)).expect("pair");
    let link = LinkKey {
        from: NodeId(1),
        to: NodeId(2),
    };
    assert!(matches!(
        poll_once(&mut left.submit_write(WriteRequest {
            buffer: b"a".to_vec(),
        })),
        Poll::Ready(Ok(_))
    ));
    network
        .set_link(
            link,
            LinkConfig {
                state: LinkState::Clogged,
                ..LinkConfig::default()
            },
        )
        .expect("clog link");
    let mut write = left.submit_write(WriteRequest {
        buffer: b"b".to_vec(),
    });
    let mut read = right.submit_read(ReadRequest {
        buffer: Vec::new(),
        max_bytes: 1,
    });
    assert!(poll_once(&mut write).is_pending());
    assert!(poll_once(&mut read).is_pending());

    network
        .set_link(link, LinkConfig::default())
        .expect("open link");

    assert!(matches!(poll_once(&mut read), Poll::Ready(Ok(_))));
    let Poll::Ready(Ok(write)) = poll_once(&mut write) else {
        panic!("capacity-blocked write should complete when the link opens");
    };
    assert_eq!(write.buffer, b"b");
    assert_eq!(write.bytes_written, 1);
}

#[test]
fn close_and_half_close_terminalize_admitted_capacity_waiters_with_buffers() {
    for close_local_write_half in [false, true] {
        let config = NetworkConfig {
            directional_buffer_bytes: 1,
            ..NetworkConfig::default()
        };
        let (mut runtime, network) = runtime_and_network(config);
        let (left, right) = network.connected_pair(NodeId(1), NodeId(2)).expect("pair");
        runtime
            .block_on(left.submit_write(WriteRequest {
                buffer: b"a".to_vec(),
            }))
            .expect("runtime")
            .expect("fill direction");
        network
            .push_fault(ScriptedFault::fail_after(
                NetworkOperationKind::Write,
                99,
                AfterFaultCertainty::Applied,
            ))
            .expect("capture write fault at admission");
        let mut pending = left.submit_write(WriteRequest {
            buffer: b"owned".to_vec(),
        });
        assert!(poll_once(&mut pending).is_pending());
        assert_eq!(network.status().pending_faults, 0);
        assert_eq!(network.status().fault_hits, 1);

        if close_local_write_half {
            runtime
                .block_on(left.submit_shutdown_write())
                .expect("runtime")
                .expect("shutdown local write half");
        } else {
            runtime
                .block_on(right.submit_close())
                .expect("runtime")
                .expect("close receiving peer");
        }

        let Poll::Ready(Err(error)) = poll_once(&mut pending) else {
            panic!("closing a required half must terminalize the pending write");
        };
        assert_eq!(error.certainty(), CompletionCertainty::NotApplied);
        assert_eq!(
            error.error().error(),
            if close_local_write_half {
                &NetworkError::WriteClosed
            } else {
                &NetworkError::ConnectionClosed
            }
        );
        assert_eq!(error.into_parts().1.into_buffer(), Some(b"owned".to_vec()));
        assert_eq!(network.status().inflight_operations, 0);
        assert_eq!(network.status().pending_faults, 0);
        assert_eq!(network.status().fault_hits, 1);
    }
}

#[test]
fn latency_advances_only_virtual_time() {
    let (mut runtime, network) = runtime_and_network(NetworkConfig::default());
    let handle = runtime.handle();
    let (left, right) = network
        .connected_pair(NodeId(10), NodeId(20))
        .expect("pair");
    network
        .set_link(
            LinkKey {
                from: NodeId(10),
                to: NodeId(20),
            },
            LinkConfig {
                latency: SimDuration::from_nanos(7),
                ..LinkConfig::default()
            },
        )
        .expect("link");

    runtime
        .block_on(left.submit_write(WriteRequest {
            buffer: b"x".to_vec(),
        }))
        .expect("runtime")
        .expect("write");
    assert_eq!(handle.now().as_nanos(), 7);
    runtime
        .block_on(right.submit_read(ReadRequest {
            buffer: Vec::new(),
            max_bytes: 1,
        }))
        .expect("runtime")
        .expect("read");
    assert_eq!(handle.now().as_nanos(), 14);
}

#[test]
fn connect_captures_both_handshake_gates_and_link_latencies() {
    let (mut runtime, network) = runtime_and_network(NetworkConfig::default());
    let handle = runtime.handle();
    let server = NetworkAddress {
        node: NodeId(20),
        port: 7_000,
    };
    let client = NetworkAddress {
        node: NodeId(10),
        port: 4_000,
    };
    let listener = runtime
        .block_on(network.submit_listen(ListenRequest {
            address: server,
            backlog: 1,
        }))
        .expect("runtime")
        .expect("listen");
    let forward = LinkKey {
        from: client.node,
        to: server.node,
    };
    let reverse = LinkKey {
        from: server.node,
        to: client.node,
    };
    network
        .set_link(
            forward,
            LinkConfig {
                latency: SimDuration::from_nanos(7),
                state: LinkState::Clogged,
                ..LinkConfig::default()
            },
        )
        .expect("clog forward handshake direction");
    network
        .set_link(
            reverse,
            LinkConfig {
                latency: SimDuration::from_nanos(11),
                state: LinkState::Clogged,
                ..LinkConfig::default()
            },
        )
        .expect("clog reverse handshake direction");

    let mut connect = network.submit_connect(ConnectRequest {
        local: client,
        remote: server,
    });
    assert!(poll_once(&mut connect).is_pending());
    runtime
        .run_until_stalled()
        .expect("handshake delay elapses under virtual time");
    assert_eq!(handle.now().as_nanos(), 18);
    assert!(poll_once(&mut connect).is_pending());

    network
        .set_link(forward, LinkConfig::default())
        .expect("open first handshake direction");
    assert!(poll_once(&mut connect).is_pending());
    network
        .set_link(reverse, LinkConfig::default())
        .expect("open second handshake direction");
    assert!(matches!(poll_once(&mut connect), Poll::Ready(Ok(_))));
    assert!(
        runtime
            .block_on(listener.submit_accept())
            .expect("runtime")
            .is_ok()
    );
}

#[test]
fn connect_with_default_links_completes_without_driving_the_runtime() {
    let (_runtime, network) = runtime_and_network(NetworkConfig::default());
    let server = NetworkAddress {
        node: NodeId(2),
        port: 7_000,
    };
    let mut listen = network.submit_listen(ListenRequest {
        address: server,
        backlog: 1,
    });
    let Poll::Ready(Ok(_listener)) = poll_once(&mut listen) else {
        panic!("zero-delay listen should complete at submission");
    };
    let mut connect = network.submit_connect(ConnectRequest {
        local: NetworkAddress {
            node: NodeId(1),
            port: 4_000,
        },
        remote: server,
    });
    assert!(matches!(poll_once(&mut connect), Poll::Ready(Ok(_))));
}

#[test]
fn connect_rejects_handshake_latency_overflow_before_creating_a_pair() {
    let (mut runtime, network) = runtime_and_network(NetworkConfig::default());
    let server = NetworkAddress {
        node: NodeId(2),
        port: 7_000,
    };
    let client = NetworkAddress {
        node: NodeId(1),
        port: 4_000,
    };
    let _listener = runtime
        .block_on(network.submit_listen(ListenRequest {
            address: server,
            backlog: 1,
        }))
        .expect("runtime")
        .expect("listen");
    network
        .set_link(
            LinkKey {
                from: client.node,
                to: server.node,
            },
            LinkConfig {
                latency: SimDuration::MAX,
                ..LinkConfig::default()
            },
        )
        .expect("maximum forward latency");
    network
        .set_link(
            LinkKey {
                from: server.node,
                to: client.node,
            },
            LinkConfig {
                latency: SimDuration::from_nanos(1),
                ..LinkConfig::default()
            },
        )
        .expect("reverse latency");

    let error = runtime
        .block_on(network.submit_connect(ConnectRequest {
            local: client,
            remote: server,
        }))
        .expect("runtime")
        .expect_err("handshake latency overflows");
    assert_eq!(error.certainty(), CompletionCertainty::NotApplied);
    assert!(matches!(
        error.error().error(),
        NetworkError::InvalidRequest { .. }
    ));
    assert_eq!(network.status().connections, 0);
}

#[test]
fn scripted_terminal_outcomes_share_checked_link_completion_latency() {
    let outcomes = [
        FaultOutcome::Continue,
        FaultOutcome::FailBefore { tag: 1 },
        FaultOutcome::FailAfter {
            tag: 2,
            certainty: AfterFaultCertainty::Applied,
        },
        FaultOutcome::FailAfter {
            tag: 3,
            certainty: AfterFaultCertainty::MayHaveApplied,
        },
    ];
    for operation in [
        NetworkOperationKind::Read,
        NetworkOperationKind::Write,
        NetworkOperationKind::ShutdownWrite,
        NetworkOperationKind::Close,
    ] {
        for outcome in outcomes {
            let (mut runtime, network) = runtime_and_network(NetworkConfig::default());
            let handle = runtime.handle();
            let (left, right) = network.connected_pair(NodeId(1), NodeId(2)).expect("pair");
            let link = LinkKey {
                from: NodeId(1),
                to: NodeId(2),
            };
            network
                .set_link(
                    link,
                    LinkConfig {
                        latency: SimDuration::from_nanos(5),
                        ..LinkConfig::default()
                    },
                )
                .expect("link latency");
            network
                .push_fault(ScriptedFault {
                    operation,
                    extra_latency: SimDuration::from_nanos(3),
                    max_bytes: None,
                    outcome,
                })
                .expect("script terminal outcome");

            let result = match operation {
                NetworkOperationKind::Read => runtime
                    .block_on(right.submit_read(ReadRequest {
                        buffer: Vec::new(),
                        max_bytes: 0,
                    }))
                    .expect("runtime")
                    .map(|_| ()),
                NetworkOperationKind::Write => runtime
                    .block_on(left.submit_write(WriteRequest {
                        buffer: b"x".to_vec(),
                    }))
                    .expect("runtime")
                    .map(|_| ()),
                NetworkOperationKind::ShutdownWrite => runtime
                    .block_on(left.submit_shutdown_write())
                    .expect("runtime"),
                NetworkOperationKind::Close => {
                    runtime.block_on(left.submit_close()).expect("runtime")
                }
                _ => unreachable!("test covers stream operations"),
            };
            assert_eq!(
                handle.now().as_nanos(),
                8,
                "operation={operation:?} outcome={outcome:?}"
            );
            match outcome {
                FaultOutcome::Continue => assert!(result.is_ok()),
                FaultOutcome::FailBefore { .. } => assert_eq!(
                    result.expect_err("before fault fails").certainty(),
                    CompletionCertainty::NotApplied
                ),
                FaultOutcome::FailAfter {
                    certainty: AfterFaultCertainty::Applied,
                    ..
                } => assert_eq!(
                    result.expect_err("after fault fails").certainty(),
                    CompletionCertainty::Applied
                ),
                FaultOutcome::FailAfter {
                    certainty: AfterFaultCertainty::MayHaveApplied,
                    ..
                } => assert_eq!(
                    result.expect_err("ambiguous after fault fails").certainty(),
                    CompletionCertainty::MayHaveApplied
                ),
                FaultOutcome::StallBefore => unreachable!(),
            }
        }
    }
}

#[test]
fn fail_before_latency_overflow_is_not_applied_and_preserves_the_write_buffer() {
    let (mut runtime, network) = runtime_and_network(NetworkConfig::default());
    let (left, right) = network.connected_pair(NodeId(1), NodeId(2)).expect("pair");
    network
        .set_link(
            LinkKey {
                from: NodeId(1),
                to: NodeId(2),
            },
            LinkConfig {
                latency: SimDuration::MAX,
                ..LinkConfig::default()
            },
        )
        .expect("maximum link latency");
    network
        .push_fault(ScriptedFault {
            operation: NetworkOperationKind::Write,
            extra_latency: SimDuration::from_nanos(1),
            max_bytes: None,
            outcome: FaultOutcome::FailBefore { tag: 1 },
        })
        .expect("fault");

    let error = runtime
        .block_on(left.submit_write(WriteRequest {
            buffer: b"owned".to_vec(),
        }))
        .expect("runtime")
        .expect_err("completion latency overflows");
    assert_eq!(error.certainty(), CompletionCertainty::NotApplied);
    assert!(matches!(
        error.error().error(),
        NetworkError::InvalidRequest { .. }
    ));
    assert_eq!(error.into_parts().1.into_buffer(), Some(b"owned".to_vec()));

    let mut read = right.submit_read(ReadRequest {
        buffer: Vec::new(),
        max_bytes: 1,
    });
    assert!(poll_once(&mut read).is_pending());
}

#[test]
fn runtime_shutdown_releases_delayed_write_completion() {
    for timer_started in [false, true] {
        let config = NetworkConfig {
            default_link: LinkConfig {
                latency: SimDuration::from_nanos(50),
                ..LinkConfig::default()
            },
            ..NetworkConfig::default()
        };
        let (mut runtime, network) = runtime_and_network(config);
        let (left, _right) = network.connected_pair(NodeId(1), NodeId(2)).expect("pair");
        let mut write = left.submit_write(WriteRequest {
            buffer: b"applied".to_vec(),
        });
        assert!(poll_once(&mut write).is_pending());
        assert_eq!(network.status().inflight_operations, 1);
        if timer_started {
            runtime
                .step()
                .expect("delay task registers its completion timer");
        }

        runtime.shutdown().expect("runtime shuts down cleanly");

        let Poll::Ready(Ok(completed)) = poll_once(&mut write) else {
            panic!("shutdown must release the already-applied write completion");
        };
        assert_eq!(completed.buffer, b"applied");
        assert_eq!(completed.bytes_written, 7);
        assert_eq!(network.status().inflight_operations, 0);
    }
}

#[test]
fn runtime_shutdown_releases_delayed_read_completion() {
    for timer_started in [false, true] {
        let (mut runtime, network) = runtime_and_network(NetworkConfig::default());
        let (left, right) = network.connected_pair(NodeId(1), NodeId(2)).expect("pair");
        let write = runtime
            .block_on(left.submit_write(WriteRequest {
                buffer: b"consumed".to_vec(),
            }))
            .expect("runtime drives seed write")
            .expect("seed write succeeds");
        assert_eq!(write.bytes_written, 8);
        network
            .set_link(
                LinkKey {
                    from: NodeId(1),
                    to: NodeId(2),
                },
                LinkConfig {
                    latency: SimDuration::from_nanos(50),
                    ..LinkConfig::default()
                },
            )
            .expect("delay incoming direction");
        let mut read = right.submit_read(ReadRequest {
            buffer: Vec::new(),
            max_bytes: 8,
        });
        assert!(poll_once(&mut read).is_pending());
        assert_eq!(network.status().inflight_operations, 1);
        if timer_started {
            runtime
                .step()
                .expect("delay task registers its completion timer");
        }

        runtime.shutdown().expect("runtime shuts down cleanly");

        let Poll::Ready(Ok(completed)) = poll_once(&mut read) else {
            panic!("shutdown must release the already-consumed read completion");
        };
        assert_eq!(completed.buffer, b"consumed");
        assert_eq!(completed.bytes_read, 8);
        assert_eq!(network.status().inflight_operations, 0);
    }
}

struct PanicWake;

impl Wake for PanicWake {
    fn wake(self: Arc<Self>) {
        panic!("intentional network response wake panic");
    }
}

#[test]
fn panicking_response_waker_does_not_kill_the_delay_task() {
    let config = NetworkConfig {
        default_link: LinkConfig {
            latency: SimDuration::from_nanos(7),
            ..LinkConfig::default()
        },
        ..NetworkConfig::default()
    };
    let (mut runtime, network) = runtime_and_network(config);
    let (left, _right) = network.connected_pair(NodeId(1), NodeId(2)).expect("pair");
    let mut write = left.submit_write(WriteRequest {
        buffer: b"wake".to_vec(),
    });
    let panic_waker = Waker::from(Arc::new(PanicWake));
    let mut context = Context::from_waker(&panic_waker);
    assert!(Pin::new(&mut write).poll(&mut context).is_pending());

    runtime
        .run_until_stalled()
        .expect("response waker panic is contained by the delivery task");

    let Poll::Ready(Ok(completed)) = poll_once(&mut write) else {
        panic!("write must retain its terminal response");
    };
    assert_eq!(completed.bytes_written, 4);
    assert_eq!(network.status().inflight_operations, 0);
}

#[test]
fn directional_clog_holds_completion_until_opened() {
    let (_runtime, network) = runtime_and_network(NetworkConfig::default());
    let (left, right) = network.connected_pair(NodeId(1), NodeId(2)).expect("pair");
    let link = LinkKey {
        from: NodeId(1),
        to: NodeId(2),
    };
    network
        .set_link(
            link,
            LinkConfig {
                state: LinkState::Clogged,
                ..LinkConfig::default()
            },
        )
        .expect("clog");

    let mut write = left.submit_write(WriteRequest {
        buffer: b"queued".to_vec(),
    });
    let mut read = right.submit_read(ReadRequest {
        buffer: Vec::new(),
        max_bytes: 64,
    });
    assert!(poll_once(&mut write).is_pending());
    assert!(poll_once(&mut read).is_pending());

    network.set_link(link, LinkConfig::default()).expect("open");
    assert!(matches!(poll_once(&mut write), Poll::Ready(Ok(_))));
    let Poll::Ready(Ok(read)) = poll_once(&mut read) else {
        panic!("read should complete when link opens");
    };
    assert_eq!(read.buffer, b"queued");
}

#[test]
fn link_overrides_are_bounded_and_default_profiles_are_not_retained() {
    let config = NetworkConfig {
        max_link_overrides: 1,
        ..NetworkConfig::default()
    };
    let (_runtime, network) = runtime_and_network(config);
    let first = LinkKey {
        from: NodeId(1),
        to: NodeId(2),
    };
    let second = LinkKey {
        from: NodeId(3),
        to: NodeId(4),
    };

    network
        .set_link(first, LinkConfig::default())
        .expect("default-equivalent profile needs no override slot");
    assert_eq!(network.status().link_overrides, 0);
    network
        .set_link(
            first,
            LinkConfig {
                latency: SimDuration::from_nanos(1),
                ..LinkConfig::default()
            },
        )
        .expect("first override");
    assert_eq!(network.status().link_overrides, 1);
    network
        .set_link(
            first,
            LinkConfig {
                latency: SimDuration::from_nanos(2),
                ..LinkConfig::default()
            },
        )
        .expect("replacing an override does not consume another slot");

    assert_eq!(
        network
            .set_link(
                second,
                LinkConfig {
                    state: LinkState::Partitioned,
                    ..LinkConfig::default()
                },
            )
            .expect_err("second override exceeds the bound"),
        NetworkError::ResourceExhausted {
            resource: "link overrides",
            limit: 1,
        }
    );
    network
        .set_link(first, LinkConfig::default())
        .expect("returning to the default releases the slot");
    assert_eq!(network.status().link_overrides, 0);
    network
        .set_link(
            second,
            LinkConfig {
                state: LinkState::Partitioned,
                ..LinkConfig::default()
            },
        )
        .expect("released slot can be reused");
    assert_eq!(network.status().link_overrides, 1);
}

#[test]
fn blocked_link_keys_remain_bounded_after_observer_abandonment() {
    let config = NetworkConfig {
        max_link_overrides: 2,
        max_blocked_links: 1,
        ..NetworkConfig::default()
    };
    let (mut runtime, network) = runtime_and_network(config);
    let (first_left, _first_right) = network.connected_pair(NodeId(1), NodeId(2)).expect("pair");
    let (second_left, _second_right) = network.connected_pair(NodeId(3), NodeId(4)).expect("pair");
    let first = LinkKey {
        from: NodeId(1),
        to: NodeId(2),
    };
    let second = LinkKey {
        from: NodeId(3),
        to: NodeId(4),
    };
    for link in [first, second] {
        network
            .set_link(
                link,
                LinkConfig {
                    state: LinkState::Clogged,
                    ..LinkConfig::default()
                },
            )
            .expect("clog link");
    }

    let first_write = first_left.submit_write(WriteRequest {
        buffer: b"first".to_vec(),
    });
    assert_eq!(network.status().blocked_links, 1);
    let error = runtime
        .block_on(second_left.submit_write(WriteRequest {
            buffer: b"second".to_vec(),
        }))
        .expect("runtime")
        .expect_err("second blocked link exceeds the bound");
    assert_eq!(error.certainty(), CompletionCertainty::NotApplied);
    assert_eq!(
        error.error().error(),
        &NetworkError::ResourceExhausted {
            resource: "blocked links",
            limit: 1,
        }
    );
    assert_eq!(error.into_parts().1.into_buffer(), Some(b"second".to_vec()));

    drop(first_write);
    assert_eq!(
        network.status().blocked_links,
        1,
        "abandonment cannot release an admitted clogged completion"
    );
    network
        .set_link(first, LinkConfig::default())
        .expect("open first");
    assert_eq!(network.status().blocked_links, 0);
    let mut retried = second_left.submit_write(WriteRequest {
        buffer: b"second".to_vec(),
    });
    assert!(poll_once(&mut retried).is_pending());
    assert_eq!(network.status().blocked_links, 1);
    network
        .set_link(second, LinkConfig::default())
        .expect("opening a link releases and removes its blocked gates");
    assert!(matches!(poll_once(&mut retried), Poll::Ready(Ok(_))));
    assert_eq!(network.status().blocked_links, 0);
}

#[test]
fn half_close_drains_bytes_then_reports_eof() {
    let (mut runtime, network) = runtime_and_network(NetworkConfig::default());
    let (left, right) = network.connected_pair(NodeId(1), NodeId(2)).expect("pair");
    runtime
        .block_on(left.submit_write(WriteRequest {
            buffer: b"last".to_vec(),
        }))
        .expect("runtime")
        .expect("write");
    runtime
        .block_on(left.submit_shutdown_write())
        .expect("runtime")
        .expect("shutdown");

    let last = runtime
        .block_on(right.submit_read(ReadRequest {
            buffer: Vec::new(),
            max_bytes: 16,
        }))
        .expect("runtime")
        .expect("last bytes");
    assert_eq!(last.buffer, b"last");
    assert!(!last.end_of_stream);

    let eof = runtime
        .block_on(right.submit_read(ReadRequest {
            buffer: Vec::new(),
            max_bytes: 16,
        }))
        .expect("runtime")
        .expect("eof");
    assert_eq!(eof.bytes_read, 0);
    assert!(eof.end_of_stream);
}

#[test]
fn dropping_admitted_write_future_does_not_rollback_bytes() {
    let (mut runtime, network) = runtime_and_network(NetworkConfig::default());
    let (left, right) = network.connected_pair(NodeId(1), NodeId(2)).expect("pair");
    let write = left.submit_write(WriteRequest {
        buffer: b"survives".to_vec(),
    });
    drop(write);
    assert_eq!(network.status().inflight_operations, 0);

    let read = runtime
        .block_on(right.submit_read(ReadRequest {
            buffer: Vec::new(),
            max_bytes: 64,
        }))
        .expect("runtime")
        .expect("read");
    assert_eq!(read.buffer, b"survives");
}

#[test]
fn dropping_pending_read_abandons_response_but_consumes_future_bytes() {
    let (mut runtime, network) = runtime_and_network(NetworkConfig::default());
    let (left, right) = network.connected_pair(NodeId(1), NodeId(2)).expect("pair");
    let pending = right.submit_read(ReadRequest {
        buffer: b"prefix".to_vec(),
        max_bytes: 64,
    });
    assert_eq!(network.status().inflight_operations, 1);
    drop(pending);
    assert_eq!(network.status().inflight_operations, 1);

    runtime
        .block_on(left.submit_write(WriteRequest {
            buffer: b"consumed".to_vec(),
        }))
        .expect("runtime")
        .expect("write");
    assert_eq!(network.status().inflight_operations, 0);

    let visible = right.submit_read(ReadRequest {
        buffer: Vec::new(),
        max_bytes: 64,
    });
    runtime
        .block_on(left.submit_write(WriteRequest {
            buffer: b"visible".to_vec(),
        }))
        .expect("runtime")
        .expect("second write");
    let read = runtime.block_on(visible).expect("runtime").expect("read");
    assert_eq!(read.buffer, b"visible");
}

#[test]
fn scripted_faults_are_fifo_per_operation_and_report_completion_certainty() {
    let (mut runtime, network) = runtime_and_network(NetworkConfig::default());
    let (left, right) = network.connected_pair(NodeId(1), NodeId(2)).expect("pair");
    network
        .push_fault(ScriptedFault::fail_before(NetworkOperationKind::Write, 41))
        .expect("fault");
    network
        .push_fault(ScriptedFault::fail_before(NetworkOperationKind::Read, 40))
        .expect("fault");

    // Per-kind queues let the read rule fire without consuming or being
    // blocked behind the earlier write rule.
    let read_before = runtime
        .block_on(right.submit_read(ReadRequest {
            buffer: Vec::new(),
            max_bytes: 0,
        }))
        .expect("runtime")
        .expect_err("injected read failure");
    assert_eq!(read_before.certainty(), CompletionCertainty::NotApplied);
    assert_eq!(
        read_before.error().error(),
        &NetworkError::Injected { tag: 40 }
    );
    assert_eq!(network.status().pending_faults, 1);

    let before = runtime
        .block_on(left.submit_write(WriteRequest {
            buffer: b"a".to_vec(),
        }))
        .expect("runtime")
        .expect_err("injected before failure");
    assert_eq!(before.certainty(), CompletionCertainty::NotApplied);
    assert_eq!(before.error().bytes_transferred(), 0);

    network
        .push_fault(ScriptedFault::fail_after(
            NetworkOperationKind::Write,
            42,
            AfterFaultCertainty::Applied,
        ))
        .expect("fault");
    let after = runtime
        .block_on(left.submit_write(WriteRequest {
            buffer: b"b".to_vec(),
        }))
        .expect("runtime")
        .expect_err("injected after failure");
    assert_eq!(after.certainty(), CompletionCertainty::Applied);
    assert_eq!(after.error().bytes_transferred(), 1);

    let delivered = runtime
        .block_on(right.submit_read(ReadRequest {
            buffer: Vec::new(),
            max_bytes: 1,
        }))
        .expect("runtime")
        .expect("read applied bytes");
    assert_eq!(delivered.buffer, b"b");

    network
        .push_fault(ScriptedFault::fail_after(
            NetworkOperationKind::Write,
            43,
            AfterFaultCertainty::MayHaveApplied,
        ))
        .expect("fault");
    let ambiguous = runtime
        .block_on(left.submit_write(WriteRequest {
            buffer: b"c".to_vec(),
        }))
        .expect("runtime")
        .expect_err("injected ambiguous after failure");
    assert_eq!(ambiguous.certainty(), CompletionCertainty::MayHaveApplied);
    assert_eq!(ambiguous.error().bytes_transferred(), 1);

    let ambiguously_delivered = runtime
        .block_on(right.submit_read(ReadRequest {
            buffer: Vec::new(),
            max_bytes: 1,
        }))
        .expect("runtime")
        .expect("read ambiguously applied bytes");
    assert_eq!(ambiguously_delivered.buffer, b"c");
    assert_eq!(network.status().fault_hits, 4);
}

#[test]
fn rejected_operations_preserve_faults_and_admitted_operations_consume_them() {
    let config = NetworkConfig {
        max_operation_bytes: 4,
        directional_buffer_bytes: 1,
        ..NetworkConfig::default()
    };
    let (mut runtime, network) = runtime_and_network(config);
    let (left, right) = network.connected_pair(NodeId(1), NodeId(2)).expect("pair");

    network
        .push_fault(ScriptedFault::fail_before(NetworkOperationKind::Write, 1))
        .expect("fault");
    let invalid = runtime
        .block_on(left.submit_write(WriteRequest { buffer: vec![0; 5] }))
        .expect("runtime")
        .expect_err("oversized write is invalid");
    assert!(matches!(
        invalid.error().error(),
        NetworkError::InvalidRequest { .. }
    ));
    assert_eq!(network.status().pending_faults, 1);
    assert_eq!(network.status().fault_hits, 0);

    let link = LinkKey {
        from: NodeId(1),
        to: NodeId(2),
    };
    network
        .set_link(
            link,
            LinkConfig {
                state: LinkState::Partitioned,
                ..LinkConfig::default()
            },
        )
        .expect("partition");
    let partitioned = runtime
        .block_on(left.submit_write(WriteRequest {
            buffer: b"x".to_vec(),
        }))
        .expect("runtime")
        .expect_err("partition blocks write");
    assert_eq!(
        partitioned.error().error(),
        &NetworkError::Partitioned { link }
    );
    assert_eq!(network.status().pending_faults, 1);

    network
        .set_link(link, LinkConfig::default())
        .expect("reopen link");
    let injected = runtime
        .block_on(left.submit_write(WriteRequest {
            buffer: b"x".to_vec(),
        }))
        .expect("runtime")
        .expect_err("valid write consumes retained fault");
    assert_eq!(injected.error().error(), &NetworkError::Injected { tag: 1 });

    runtime
        .block_on(left.submit_write(WriteRequest {
            buffer: b"a".to_vec(),
        }))
        .expect("runtime")
        .expect("fill directional buffer");
    network
        .push_fault(ScriptedFault::fail_before(NetworkOperationKind::Write, 2))
        .expect("fault");
    let mut capacity_blocked = left.submit_write(WriteRequest {
        buffer: b"b".to_vec(),
    });
    let Poll::Ready(Err(injected)) = poll_once(&mut capacity_blocked) else {
        panic!("a valid admission should consume its before-effect fault immediately");
    };
    assert_eq!(injected.error().error(), &NetworkError::Injected { tag: 2 });
    assert_eq!(injected.into_parts().1.into_buffer(), Some(b"b".to_vec()));
    assert_eq!(network.status().pending_faults, 0);
    assert_eq!(network.status().fault_hits, 2);

    let drained = runtime
        .block_on(right.submit_read(ReadRequest {
            buffer: Vec::new(),
            max_bytes: 1,
        }))
        .expect("runtime")
        .expect("original buffered byte remains readable");
    assert_eq!(drained.buffer, b"a");
}

#[test]
fn closed_endpoints_do_not_consume_read_or_write_faults() {
    let (mut runtime, network) = runtime_and_network(NetworkConfig::default());
    let (left, right) = network.connected_pair(NodeId(1), NodeId(2)).expect("pair");
    network
        .push_fault(ScriptedFault::fail_before(NetworkOperationKind::Write, 4))
        .expect("fault");
    drop(right);
    let closed_write = runtime
        .block_on(left.submit_write(WriteRequest {
            buffer: b"owned".to_vec(),
        }))
        .expect("runtime")
        .expect_err("peer is closed");
    assert_eq!(
        closed_write.error().error(),
        &NetworkError::ConnectionClosed
    );
    assert_eq!(network.status().pending_faults, 1);
    assert_eq!(network.status().fault_hits, 0);

    let (_peer, closed_reader) = network.connected_pair(NodeId(3), NodeId(4)).expect("pair");
    runtime
        .block_on(closed_reader.submit_close())
        .expect("runtime")
        .expect("close reader");
    network
        .push_fault(ScriptedFault::fail_before(NetworkOperationKind::Read, 5))
        .expect("fault");
    let closed_read = runtime
        .block_on(closed_reader.submit_read(ReadRequest {
            buffer: b"owned".to_vec(),
            max_bytes: 1,
        }))
        .expect("runtime")
        .expect_err("local read side is closed");
    assert_eq!(closed_read.error().error(), &NetworkError::ConnectionClosed);
    assert_eq!(network.status().pending_faults, 2);
    assert_eq!(network.status().fault_hits, 0);
}

#[test]
fn zero_byte_partial_write_is_success_when_capacity_is_available() {
    let (mut runtime, network) = runtime_and_network(NetworkConfig::default());
    let (left, _right) = network.connected_pair(NodeId(1), NodeId(2)).expect("pair");
    network
        .push_fault(ScriptedFault::partial(NetworkOperationKind::Write, 0))
        .expect("fault");

    let result = runtime
        .block_on(left.submit_write(WriteRequest {
            buffer: b"owned".to_vec(),
        }))
        .expect("runtime")
        .expect("scripted zero-byte write is a successful partial write");
    assert_eq!(result.bytes_written, 0);
    assert_eq!(result.buffer, b"owned");
    assert_eq!(network.status().fault_hits, 1);
}

#[test]
fn read_request_bounds_the_result_buffer() {
    let config = NetworkConfig {
        max_operation_bytes: 4,
        ..NetworkConfig::default()
    };
    let (mut runtime, network) = runtime_and_network(config);
    let (_left, right) = network.connected_pair(NodeId(1), NodeId(2)).expect("pair");
    network
        .push_fault(ScriptedFault::fail_before(NetworkOperationKind::Read, 3))
        .expect("fault");

    let error = runtime
        .block_on(right.submit_read(ReadRequest {
            buffer: b"abc".to_vec(),
            max_bytes: 2,
        }))
        .expect("runtime")
        .expect_err("result buffer would exceed operation bound");
    assert_eq!(error.certainty(), CompletionCertainty::NotApplied);
    assert!(matches!(
        error.error().error(),
        NetworkError::InvalidRequest { .. }
    ));
    assert_eq!(error.into_parts().1.into_buffer(), Some(b"abc".to_vec()));
    assert_eq!(network.status().pending_faults, 1);
    assert_eq!(network.status().fault_hits, 0);

    let injected = runtime
        .block_on(right.submit_read(ReadRequest {
            buffer: Vec::new(),
            max_bytes: 1,
        }))
        .expect("runtime")
        .expect_err("valid read consumes retained fault");
    assert_eq!(injected.error().error(), &NetworkError::Injected { tag: 3 });
}

#[test]
fn operation_bounds_include_reserved_buffer_capacity() {
    let config = NetworkConfig {
        max_operation_bytes: 4,
        ..NetworkConfig::default()
    };
    let (mut runtime, network) = runtime_and_network(config);
    let (left, right) = network.connected_pair(NodeId(1), NodeId(2)).expect("pair");

    let mut read_buffer = Vec::with_capacity(5);
    read_buffer.push(b'r');
    let read = runtime
        .block_on(right.submit_read(ReadRequest {
            buffer: read_buffer,
            max_bytes: 1,
        }))
        .expect("runtime")
        .expect_err("reserved read allocation exceeds the provider bound")
        .into_parts()
        .1
        .into_buffer()
        .expect("read buffer is returned");
    assert!(read.capacity() > 4);

    let mut write_buffer = Vec::with_capacity(5);
    write_buffer.push(b'w');
    let write = runtime
        .block_on(left.submit_write(WriteRequest {
            buffer: write_buffer,
        }))
        .expect("runtime")
        .expect_err("reserved write allocation exceeds the provider bound")
        .into_parts()
        .1
        .into_buffer()
        .expect("write buffer is returned");
    assert!(write.capacity() > 4);
    assert_eq!(network.status().inflight_operations, 0);
}

#[test]
fn partition_and_disconnect_are_directional() {
    let (mut runtime, network) = runtime_and_network(NetworkConfig::default());
    let (left, right) = network.connected_pair(NodeId(1), NodeId(2)).expect("pair");
    network
        .set_link(
            LinkKey {
                from: NodeId(1),
                to: NodeId(2),
            },
            LinkConfig {
                state: LinkState::Partitioned,
                ..LinkConfig::default()
            },
        )
        .expect("partition");

    let error = runtime
        .block_on(left.submit_write(WriteRequest {
            buffer: b"blocked".to_vec(),
        }))
        .expect("runtime")
        .expect_err("partitioned");
    assert_eq!(error.certainty(), CompletionCertainty::NotApplied);
    assert!(matches!(
        error.error().error(),
        NetworkError::Partitioned { .. }
    ));

    let read_error = runtime
        .block_on(right.submit_read(ReadRequest {
            buffer: b"read-owned".to_vec(),
            max_bytes: 1,
        }))
        .expect("runtime")
        .expect_err("read direction is partitioned");
    assert_eq!(read_error.certainty(), CompletionCertainty::NotApplied);
    assert!(matches!(
        read_error.error().error(),
        NetworkError::Partitioned { .. }
    ));
    assert_eq!(
        read_error.into_parts().1.into_buffer(),
        Some(b"read-owned".to_vec())
    );

    runtime
        .block_on(right.submit_write(WriteRequest {
            buffer: b"reverse".to_vec(),
        }))
        .expect("runtime")
        .expect("reverse direction remains open");
    drop(left);
    let closed = runtime
        .block_on(right.submit_write(WriteRequest {
            buffer: b"after close".to_vec(),
        }))
        .expect("runtime")
        .expect_err("peer disconnected");
    assert_eq!(closed.error().error(), &NetworkError::ConnectionClosed);
}

#[test]
fn connect_fails_when_either_handshake_direction_is_partitioned() {
    let server = NetworkAddress {
        node: NodeId(2),
        port: 7_000,
    };
    let client = NetworkAddress {
        node: NodeId(1),
        port: 4_001,
    };

    for partitioned_link in [
        LinkKey {
            from: client.node,
            to: server.node,
        },
        LinkKey {
            from: server.node,
            to: client.node,
        },
    ] {
        let (mut runtime, network) = runtime_and_network(NetworkConfig::default());
        let _listener = runtime
            .block_on(network.submit_listen(ListenRequest {
                address: server,
                backlog: 1,
            }))
            .expect("runtime")
            .expect("listen");
        network
            .set_link(
                partitioned_link,
                LinkConfig {
                    state: LinkState::Partitioned,
                    ..LinkConfig::default()
                },
            )
            .expect("partition");

        let error = runtime
            .block_on(network.submit_connect(ConnectRequest {
                local: client,
                remote: server,
            }))
            .expect("runtime")
            .expect_err("a TCP handshake needs both directions");
        assert_eq!(error.certainty(), CompletionCertainty::NotApplied);
        assert_eq!(
            error.error().error(),
            &NetworkError::Partitioned {
                link: partitioned_link,
            }
        );
        assert_eq!(network.status().connections, 0);
    }
}

#[test]
fn abandoned_pending_operations_remain_bounded_until_terminal_completion() {
    let config = NetworkConfig {
        max_inflight_operations: 1,
        ..NetworkConfig::default()
    };
    let (mut runtime, network) = runtime_and_network(config);
    let (left, right) = network.connected_pair(NodeId(1), NodeId(2)).expect("pair");
    let first = right.submit_read(ReadRequest {
        buffer: Vec::new(),
        max_bytes: 1,
    });
    let full = runtime
        .block_on(right.submit_read(ReadRequest {
            buffer: b"owned".to_vec(),
            max_bytes: 1,
        }))
        .expect("runtime")
        .expect_err("inflight bound");
    assert_eq!(full.certainty(), CompletionCertainty::NotApplied);
    assert_eq!(
        full.error().error(),
        &NetworkError::ResourceExhausted {
            resource: "inflight operations",
            limit: 1,
        }
    );
    assert_eq!(full.into_parts().1.into_buffer(), Some(b"owned".to_vec()));
    drop(first);
    assert_eq!(network.status().inflight_operations, 1);
    drop(left);
    assert_eq!(network.status().inflight_operations, 0);
}

#[test]
fn dropped_stalled_read_retains_request_and_inflight_permit() {
    let config = NetworkConfig {
        max_inflight_operations: 1,
        ..NetworkConfig::default()
    };
    let (mut runtime, network) = runtime_and_network(config);
    let (left, right) = network.connected_pair(NodeId(1), NodeId(2)).expect("pair");
    network
        .push_fault(ScriptedFault::stall(NetworkOperationKind::Read))
        .expect("fault");

    let mut stalled = right.submit_read(ReadRequest {
        buffer: b"retained read buffer".to_vec(),
        max_bytes: 1,
    });
    assert!(poll_once(&mut stalled).is_pending());
    drop(stalled);
    assert_eq!(network.status().inflight_operations, 1);

    let full = runtime
        .block_on(left.submit_write(WriteRequest {
            buffer: b"not admitted".to_vec(),
        }))
        .expect("runtime")
        .expect_err("stalled read retains the only permit");
    assert_inflight_exhausted(&full);
    assert_eq!(
        full.into_parts().1.into_buffer(),
        Some(b"not admitted".to_vec())
    );
}

#[test]
fn dropped_stalled_write_retains_request_and_inflight_permit() {
    let config = NetworkConfig {
        max_inflight_operations: 1,
        ..NetworkConfig::default()
    };
    let (mut runtime, network) = runtime_and_network(config);
    let (left, right) = network.connected_pair(NodeId(1), NodeId(2)).expect("pair");
    network
        .push_fault(ScriptedFault::stall(NetworkOperationKind::Write))
        .expect("fault");

    let mut stalled = left.submit_write(WriteRequest {
        buffer: b"retained write buffer".to_vec(),
    });
    assert!(poll_once(&mut stalled).is_pending());
    drop(stalled);
    assert_eq!(network.status().inflight_operations, 1);

    let full = runtime
        .block_on(right.submit_read(ReadRequest {
            buffer: b"not admitted".to_vec(),
            max_bytes: 1,
        }))
        .expect("runtime")
        .expect_err("stalled write retains the only permit");
    assert_inflight_exhausted(&full);
    assert_eq!(
        full.into_parts().1.into_buffer(),
        Some(b"not admitted".to_vec())
    );
}

#[test]
fn dropped_stalled_control_operations_retain_inflight_permits() {
    for stalled_kind in [
        NetworkOperationKind::ShutdownWrite,
        NetworkOperationKind::Close,
    ] {
        let config = NetworkConfig {
            max_inflight_operations: 1,
            ..NetworkConfig::default()
        };
        let (mut runtime, network) = runtime_and_network(config);
        let (left, _right) = network.connected_pair(NodeId(1), NodeId(2)).expect("pair");
        network
            .push_fault(ScriptedFault::stall(stalled_kind))
            .expect("fault");

        let mut stalled = match stalled_kind {
            NetworkOperationKind::ShutdownWrite => left.submit_shutdown_write(),
            NetworkOperationKind::Close => left.submit_close(),
            _ => unreachable!("test enumerates stream control operations"),
        };
        assert!(
            poll_once(&mut stalled).is_pending(),
            "kind={stalled_kind:?}"
        );
        drop(stalled);
        assert_eq!(
            network.status().inflight_operations,
            1,
            "kind={stalled_kind:?}"
        );

        let full = runtime
            .block_on(match stalled_kind {
                NetworkOperationKind::ShutdownWrite => left.submit_close(),
                NetworkOperationKind::Close => left.submit_shutdown_write(),
                _ => unreachable!("test enumerates stream control operations"),
            })
            .expect("runtime")
            .expect_err("stalled control operation retains the only permit");
        assert_inflight_exhausted(&full);
    }
}

#[test]
fn invalid_and_connection_bounds_fail_closed() {
    let runtime = SimRuntime::new(RuntimeConfig::default());
    let invalid = NetworkConfig {
        directional_buffer_bytes: 0,
        ..NetworkConfig::default()
    };
    assert!(matches!(
        SimNetwork::new(runtime.handle(), invalid),
        Err(NetworkError::InvalidConfig { .. })
    ));
    for invalid in [
        NetworkConfig {
            max_link_overrides: 0,
            ..NetworkConfig::default()
        },
        NetworkConfig {
            max_blocked_links: 0,
            ..NetworkConfig::default()
        },
    ] {
        assert!(matches!(
            SimNetwork::new(runtime.handle(), invalid),
            Err(NetworkError::InvalidConfig { .. })
        ));
    }

    let config = NetworkConfig {
        max_connections: 1,
        ..NetworkConfig::default()
    };
    let network = SimNetwork::new(runtime.handle(), config).expect("valid");
    let _pair = network
        .connected_pair(NodeId(1), NodeId(2))
        .expect("first pair");
    assert_eq!(
        network.connected_pair(NodeId(3), NodeId(4)).unwrap_err(),
        NetworkError::ResourceExhausted {
            resource: "connections",
            limit: 1,
        }
    );
}

#[test]
fn listen_connect_accept_produces_a_working_stream_pair() {
    let (mut runtime, network) = runtime_and_network(NetworkConfig::default());
    let server_address = NetworkAddress {
        node: NodeId(20),
        port: 7000,
    };
    let listener = runtime
        .block_on(network.submit_listen(ListenRequest {
            address: server_address,
            backlog: 4,
        }))
        .expect("runtime")
        .expect("listen");
    assert_eq!(listener.local_address(), server_address);

    let accept = listener.submit_accept();
    assert_eq!(network.status().inflight_operations, 1);
    let client = runtime
        .block_on(network.submit_connect(ConnectRequest {
            local: NetworkAddress {
                node: NodeId(10),
                port: 4000,
            },
            remote: server_address,
        }))
        .expect("runtime")
        .expect("connect");
    let server = runtime.block_on(accept).expect("runtime").expect("accept");

    runtime
        .block_on(client.submit_write(WriteRequest {
            buffer: b"hello".to_vec(),
        }))
        .expect("runtime")
        .expect("write");
    let read = runtime
        .block_on(server.submit_read(ReadRequest {
            buffer: Vec::new(),
            max_bytes: 16,
        }))
        .expect("runtime")
        .expect("read");
    assert_eq!(read.buffer, b"hello");
}

#[test]
fn backlog_and_address_binding_are_bounded() {
    let (mut runtime, network) = runtime_and_network(NetworkConfig::default());
    let address = NetworkAddress {
        node: NodeId(2),
        port: 9,
    };
    let listener = runtime
        .block_on(network.submit_listen(ListenRequest {
            address,
            backlog: 1,
        }))
        .expect("runtime")
        .expect("listen");

    let duplicate = runtime
        .block_on(network.submit_listen(ListenRequest {
            address,
            backlog: 1,
        }))
        .expect("runtime")
        .expect_err("exclusive binding");
    assert_eq!(duplicate.error().error(), &NetworkError::AddressInUse);

    let first_client = runtime
        .block_on(network.submit_connect(ConnectRequest {
            local: NetworkAddress {
                node: NodeId(1),
                port: 1,
            },
            remote: address,
        }))
        .expect("runtime")
        .expect("first connect");
    let full = runtime
        .block_on(network.submit_connect(ConnectRequest {
            local: NetworkAddress {
                node: NodeId(3),
                port: 2,
            },
            remote: address,
        }))
        .expect("runtime")
        .expect_err("backlog full");
    assert_eq!(
        full.error().error(),
        &NetworkError::BacklogFull { capacity: 1 }
    );

    let _server = runtime
        .block_on(listener.submit_accept())
        .expect("runtime")
        .expect("drain backlog");
    drop(first_client);
}

#[test]
fn live_client_endpoints_are_bound_exclusively() {
    let (mut runtime, network) = runtime_and_network(NetworkConfig::default());
    let server_address = NetworkAddress {
        node: NodeId(2),
        port: 9,
    };
    let client_address = NetworkAddress {
        node: NodeId(1),
        port: 1,
    };
    let listener = runtime
        .block_on(network.submit_listen(ListenRequest {
            address: server_address,
            backlog: 2,
        }))
        .expect("runtime")
        .expect("listen");
    let client = runtime
        .block_on(network.submit_connect(ConnectRequest {
            local: client_address,
            remote: server_address,
        }))
        .expect("runtime")
        .expect("first connect");

    let duplicate = runtime
        .block_on(network.submit_connect(ConnectRequest {
            local: client_address,
            remote: server_address,
        }))
        .expect("runtime")
        .expect_err("live client binding is exclusive");
    assert_eq!(duplicate.certainty(), CompletionCertainty::NotApplied);
    assert_eq!(duplicate.error().error(), &NetworkError::AddressInUse);
    assert_eq!(network.status().connections, 1);

    let colliding_listener = runtime
        .block_on(network.submit_listen(ListenRequest {
            address: client_address,
            backlog: 1,
        }))
        .expect("runtime")
        .expect_err("listener cannot reuse a live client endpoint");
    assert_eq!(
        colliding_listener.error().error(),
        &NetworkError::AddressInUse
    );

    drop(client);
    let rebound = runtime
        .block_on(network.submit_listen(ListenRequest {
            address: client_address,
            backlog: 1,
        }))
        .expect("runtime")
        .expect("dropping the client releases its simulated binding");
    runtime
        .block_on(rebound.submit_close())
        .expect("runtime")
        .expect("close rebound listener");
    runtime
        .block_on(listener.submit_close())
        .expect("runtime")
        .expect("close server listener");
}

#[test]
fn dropped_accept_consumes_one_fifo_connection_and_listener_close_rejects_waiters() {
    let (mut runtime, network) = runtime_and_network(NetworkConfig::default());
    let address = NetworkAddress {
        node: NodeId(2),
        port: 10,
    };
    let listener = runtime
        .block_on(network.submit_listen(ListenRequest {
            address,
            backlog: 1,
        }))
        .expect("runtime")
        .expect("listen");
    let abandoned = listener.submit_accept();
    drop(abandoned);
    assert_eq!(network.status().inflight_operations, 1);

    let abandoned_client = runtime
        .block_on(network.submit_connect(ConnectRequest {
            local: NetworkAddress {
                node: NodeId(1),
                port: 3,
            },
            remote: address,
        }))
        .expect("runtime")
        .expect("first connection completes abandoned accept");
    assert_eq!(network.status().inflight_operations, 0);
    let closed = runtime
        .block_on(abandoned_client.submit_write(WriteRequest {
            buffer: b"no receiver".to_vec(),
        }))
        .expect("runtime")
        .expect_err("abandoned accepted stream is discarded");
    assert_eq!(closed.error().error(), &NetworkError::ConnectionClosed);

    let client = runtime
        .block_on(network.submit_connect(ConnectRequest {
            local: NetworkAddress {
                node: NodeId(3),
                port: 4,
            },
            remote: address,
        }))
        .expect("runtime")
        .expect("second connection enters the empty backlog");

    let full = runtime
        .block_on(network.submit_connect(ConnectRequest {
            local: NetworkAddress {
                node: NodeId(4),
                port: 5,
            },
            remote: address,
        }))
        .expect("runtime")
        .expect_err("second connection occupies the one-entry backlog");
    assert_eq!(
        full.error().error(),
        &NetworkError::BacklogFull { capacity: 1 }
    );

    let server = runtime
        .block_on(listener.submit_accept())
        .expect("runtime")
        .expect("next accept receives the second connection");
    runtime
        .block_on(client.submit_write(WriteRequest {
            buffer: b"second".to_vec(),
        }))
        .expect("runtime")
        .expect("second client write");
    let received = runtime
        .block_on(server.submit_read(ReadRequest {
            buffer: Vec::new(),
            max_bytes: 16,
        }))
        .expect("runtime")
        .expect("second server read");
    assert_eq!(received.buffer, b"second");
    drop((client, server));

    let pending = listener.submit_accept();
    runtime
        .block_on(listener.submit_close())
        .expect("runtime")
        .expect("close listener");
    let closed = runtime
        .block_on(pending)
        .expect("runtime")
        .expect_err("pending accept rejected");
    assert_eq!(closed.error().error(), &NetworkError::ListenerClosed);

    let refused = runtime
        .block_on(network.submit_connect(ConnectRequest {
            local: NetworkAddress {
                node: NodeId(1),
                port: 6,
            },
            remote: address,
        }))
        .expect("runtime")
        .expect_err("binding was removed");
    assert_eq!(refused.error().error(), &NetworkError::ConnectionRefused);

    let rebound = runtime
        .block_on(network.submit_listen(ListenRequest {
            address,
            backlog: 1,
        }))
        .expect("runtime")
        .expect("address reusable");
    drop(rebound);
}

/// Writes blocked on directional capacity must not starve the reads that free
/// it.
///
/// A blocked write keeps its byte charge until capacity or a terminal close
/// resolves it, and abandoning its response does not release that charge. If
/// reads drew on the same budget, a direction filled with blocked writes could
/// never be drained: the read that would unblock them would be refused for
/// bytes the blocked writes are holding.
#[test]
fn capacity_blocked_writes_cannot_starve_the_reads_that_unblock_them() {
    let (mut runtime, network) = runtime_and_network(NetworkConfig {
        directional_buffer_bytes: 4,
        max_operation_bytes: 8,
        max_outstanding_read_bytes: 8,
        max_outstanding_write_bytes: 8,
        default_link: LinkConfig {
            max_chunk_bytes: 4,
            ..LinkConfig::default()
        },
        ..NetworkConfig::default()
    });
    let (left, right) = network
        .connected_pair(NodeId(1), NodeId(2))
        .expect("connected pair");

    let filled = runtime
        .block_on(left.submit_write(WriteRequest {
            buffer: vec![1u8; 4],
        }))
        .expect("runtime completes")
        .expect("the first write fits the direction's capacity");
    assert_eq!(filled.bytes_written, 4);
    assert_eq!(network.status().outstanding_write_bytes, 0);

    // Both writes block on the full direction and retain their charges,
    // committing the entire write budget.
    let blocked_first = left.submit_write(WriteRequest {
        buffer: vec![2u8; 4],
    });
    let blocked_second = left.submit_write(WriteRequest {
        buffer: vec![3u8; 4],
    });
    let status = network.status();
    assert_eq!(status.outstanding_write_bytes, 8);
    assert_eq!(status.outstanding_read_bytes, 0);

    // A further write is correctly refused: the write budget really is spent.
    let refused = runtime
        .block_on(left.submit_write(WriteRequest {
            buffer: vec![4u8; 4],
        }))
        .expect("runtime completes")
        .expect_err("the write budget is exhausted");
    assert_eq!(
        refused.error().error(),
        &NetworkError::ResourceExhausted {
            resource: "outstanding write bytes",
            limit: 8,
        }
    );

    // The read draws on its own budget, so it is admitted and frees capacity.
    let drained = runtime
        .block_on(right.submit_read(ReadRequest {
            buffer: Vec::new(),
            max_bytes: 4,
        }))
        .expect("runtime completes")
        .expect("a read is never starved by blocked writes");
    assert_eq!(drained.bytes_read, 4);
    assert_eq!(drained.buffer, vec![1u8; 4]);

    // Draining unblocked the first write, which releases its charge.
    let unblocked = runtime
        .block_on(blocked_first)
        .expect("runtime completes")
        .expect("the formerly blocked write completes");
    assert_eq!(unblocked.bytes_written, 4);
    assert_eq!(network.status().outstanding_write_bytes, 4);

    let drained = runtime
        .block_on(right.submit_read(ReadRequest {
            buffer: Vec::new(),
            max_bytes: 4,
        }))
        .expect("runtime completes")
        .expect("the direction drains");
    assert_eq!(drained.buffer, vec![2u8; 4]);
    runtime
        .block_on(blocked_second)
        .expect("runtime completes")
        .expect("the second blocked write completes");
    assert_eq!(network.status().outstanding_write_bytes, 0);
}

#[test]
fn a_byte_budget_below_the_per_operation_maximum_is_rejected_at_construction() {
    let runtime = SimRuntime::new(RuntimeConfig::default());
    for (config, reason) in [
        (
            NetworkConfig {
                max_operation_bytes: 64,
                max_outstanding_read_bytes: 32,
                ..NetworkConfig::default()
            },
            "max_outstanding_read_bytes is below max_operation_bytes",
        ),
        (
            NetworkConfig {
                max_operation_bytes: 64,
                max_outstanding_write_bytes: 32,
                ..NetworkConfig::default()
            },
            "max_outstanding_write_bytes is below max_operation_bytes",
        ),
    ] {
        let error = SimNetwork::new(runtime.handle(), config)
            .err()
            .expect("an inconsistent byte budget is refused");
        assert_eq!(error, NetworkError::InvalidConfig { reason });
    }
}

#[test]
fn read_and_write_byte_charges_are_released_when_their_output_is_consumed() {
    let (mut runtime, network) = runtime_and_network(NetworkConfig {
        max_operation_bytes: 16,
        max_outstanding_read_bytes: 16,
        max_outstanding_write_bytes: 16,
        ..NetworkConfig::default()
    });
    let (left, right) = network
        .connected_pair(NodeId(1), NodeId(2))
        .expect("connected pair");

    // A read charges the size its buffer may reach: a two-byte prefix plus six
    // more bytes commits eight, not two.
    let read = right.submit_read(ReadRequest {
        buffer: vec![0xaa, 0xbb],
        max_bytes: 6,
    });
    assert_eq!(network.status().outstanding_read_bytes, 8);

    let write = left.submit_write(WriteRequest {
        buffer: vec![7u8; 5],
    });
    assert_eq!(network.status().outstanding_write_bytes, 5);

    runtime
        .block_on(write)
        .expect("runtime completes")
        .expect("write succeeds");
    assert_eq!(network.status().outstanding_write_bytes, 0);

    runtime
        .block_on(read)
        .expect("runtime completes")
        .expect("read succeeds");
    let status = network.status();
    assert_eq!(status.outstanding_read_bytes, 0);
    assert_eq!(status.outstanding_write_bytes, 0);
}

#[test]
fn clogged_write_retains_supplemental_guard_until_gate_open_or_provider_teardown() {
    use kr_runtime_io::completion::CompletionGuard;
    use std::sync::atomic::{AtomicUsize, Ordering};
    struct Guard(Arc<AtomicUsize>);
    impl Drop for Guard {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }
    for reopen in [false, true] {
        let (runtime, network) = runtime_and_network(NetworkConfig::default());
        let (left, right) = network.connected_pair(NodeId(1), NodeId(2)).unwrap();
        let link = LinkKey {
            from: NodeId(1),
            to: NodeId(2),
        };
        network
            .set_link(
                link,
                LinkConfig {
                    state: LinkState::Clogged,
                    ..LinkConfig::default()
                },
            )
            .unwrap();
        let drops = Arc::new(AtomicUsize::new(0));
        let mut write = left.submit_write(WriteRequest { buffer: vec![1; 8] });
        write.attach_completion_guard(Arc::new(Guard(drops.clone())));
        assert!(poll_once(&mut write).is_pending());
        drop(write);
        assert_eq!(drops.load(Ordering::SeqCst), 0);
        assert_eq!(network.status().inflight_operations, 1);
        assert_eq!(network.status().outstanding_write_bytes, 8);
        if reopen {
            network.set_link(link, LinkConfig::default()).unwrap();
            assert_eq!(drops.load(Ordering::SeqCst), 1);
            assert_eq!(network.status().inflight_operations, 0);
            assert_eq!(network.status().outstanding_write_bytes, 0);
        }
        drop(left);
        drop(right);
        drop(network);
        drop(runtime);
        assert_eq!(drops.load(Ordering::SeqCst), 1);
    }
}
