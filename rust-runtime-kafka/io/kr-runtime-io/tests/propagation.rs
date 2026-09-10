use kr_runtime::{CompletionCertainty, RuntimeConfig, SimDuration, SimInstant, SimRuntime};
use kr_runtime_io::network::*;
use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll, Waker},
};

fn profile(latency: u64, holes: &[(u64, u64)], failures: &[(u64, u64)]) -> PropagationProfile {
    let windows = |v: &[(u64, u64)]| {
        v.iter()
            .map(|&(start, end)| PropagationWindow {
                start: SimInstant::from_nanos(start),
                end: SimInstant::from_nanos(end),
            })
            .collect()
    };
    PropagationProfile {
        latency: SimDuration::from_nanos(latency),
        black_holes: windows(holes),
        fail_fast: windows(failures),
    }
}
fn setup(capacity: usize, chunk: usize) -> (SimRuntime, SimNetwork) {
    let runtime = SimRuntime::new(RuntimeConfig::default());
    let network = SimNetwork::new(
        runtime.handle(),
        NetworkConfig {
            directional_buffer_bytes: capacity,
            default_link: LinkConfig {
                max_chunk_bytes: chunk,
                ..LinkConfig::default()
            },
            ..NetworkConfig::default()
        },
    )
    .unwrap();
    (runtime, network)
}
fn read(n: usize) -> ReadRequest {
    ReadRequest {
        buffer: Vec::new(),
        max_bytes: n,
    }
}
fn write(bytes: &[u8]) -> WriteRequest {
    WriteRequest {
        buffer: bytes.to_vec(),
    }
}
fn poll<F: Future + Unpin>(future: &mut F) -> Poll<F::Output> {
    Pin::new(future).poll(&mut Context::from_waker(Waker::noop()))
}
fn until(runtime: &mut SimRuntime, at: u64) {
    runtime
        .block_on(runtime.handle().sleep_until(SimInstant::from_nanos(at)))
        .unwrap()
        .unwrap();
}
fn reclaimed(runtime: &mut SimRuntime, network: &SimNetwork) {
    runtime.run_until_stalled().unwrap();
    let status = network.status();
    assert_eq!(
        (
            status.connections,
            status.inflight_operations,
            status.outstanding_read_bytes,
            status.outstanding_write_bytes
        ),
        (0, 0, 0, 0)
    );
    let snapshot = runtime.snapshot();
    let checkpoint = snapshot.determinism_checkpoint();
    assert_eq!(
        (
            checkpoint.live_tasks,
            checkpoint.live_timers,
            checkpoint.ready_tasks
        ),
        (0, 0, 0)
    );
}

#[test]
fn asymmetric_arrival_is_independent_of_local_acceptance_and_read_admission() {
    let (mut rt, net) = setup(16, 16);
    let (left, right) = net
        .connected_pair_with_propagation(
            NodeId(1),
            NodeId(2),
            profile(7, &[], &[]),
            profile(19, &[], &[]),
        )
        .unwrap();
    let mut to_right = right.submit_read(read(16));
    let mut to_left = left.submit_read(read(16));
    assert_eq!(
        rt.block_on(left.submit_write(write(b"abc")))
            .unwrap()
            .unwrap()
            .bytes_written,
        3
    );
    rt.block_on(right.submit_write(write(b"xyz")))
        .unwrap()
        .unwrap();
    assert_eq!(rt.snapshot().now, SimInstant::ZERO);
    assert!(poll(&mut to_right).is_pending());
    until(&mut rt, 6);
    assert!(poll(&mut to_right).is_pending());
    assert_eq!(rt.block_on(to_right).unwrap().unwrap().buffer, b"abc");
    assert_eq!(rt.snapshot().now.as_nanos(), 7);
    assert!(poll(&mut to_left).is_pending());
    assert_eq!(rt.block_on(to_left).unwrap().unwrap().buffer, b"xyz");
    assert_eq!(rt.snapshot().now.as_nanos(), 19);
    drop((left, right));
    reclaimed(&mut rt, &net);
}

#[test]
fn black_hole_hides_preexisting_bytes_and_keeps_them_charged_until_reopening() {
    let (mut rt, net) = setup(3, 3);
    let (left, right) = net
        .connected_pair_with_propagation(
            NodeId(1),
            NodeId(2),
            profile(7, &[(5, 20)], &[]),
            profile(0, &[], &[]),
        )
        .unwrap();
    let mut receive = right.submit_read(read(8));
    rt.block_on(left.submit_write(write(b"abc")))
        .unwrap()
        .unwrap();
    let mut blocked = left.submit_write(write(b"def"));
    until(&mut rt, 19);
    assert!(poll(&mut receive).is_pending());
    assert!(poll(&mut blocked).is_pending());
    assert_eq!(net.status().outstanding_write_bytes, 3);
    assert_eq!(rt.block_on(receive).unwrap().unwrap().buffer, b"abc");
    assert_eq!(rt.snapshot().now.as_nanos(), 20);
    rt.block_on(blocked).unwrap().unwrap();
    assert_eq!(
        rt.block_on(right.submit_read(read(8)))
            .unwrap()
            .unwrap()
            .buffer,
        b"def"
    );
    assert_eq!(rt.snapshot().now.as_nanos(), 27);
    drop((left, right));
    reclaimed(&mut rt, &net);
}

#[test]
fn already_visible_bytes_remain_readable_during_an_outage_and_touching_windows_join() {
    let (mut rt, net) = setup(8, 8);
    let (left, right) = net
        .connected_pair_with_propagation(
            NodeId(1),
            NodeId(2),
            profile(2, &[(5, 10), (10, 20)], &[]),
            profile(0, &[], &[]),
        )
        .unwrap();
    rt.block_on(left.submit_write(write(b"old")))
        .unwrap()
        .unwrap();
    until(&mut rt, 6);
    rt.block_on(left.submit_write(write(b"new")))
        .unwrap()
        .unwrap();
    assert_eq!(
        rt.block_on(right.submit_read(read(8)))
            .unwrap()
            .unwrap()
            .buffer,
        b"old"
    );
    assert_eq!(rt.snapshot().now.as_nanos(), 6);
    let mut receive = right.submit_read(read(8));
    until(&mut rt, 19);
    assert!(poll(&mut receive).is_pending());
    assert_eq!(rt.block_on(receive).unwrap().unwrap().buffer, b"new");
    assert_eq!(rt.snapshot().now.as_nanos(), 20);
    drop((left, right));
    reclaimed(&mut rt, &net);
}

#[test]
fn fail_fast_retires_pending_io_before_same_time_arrival_and_allows_fresh_setup_at_end() {
    let (mut rt, net) = setup(3, 3);
    let failure = profile(5, &[], &[(5, 20)]);
    let (left, right) = net
        .connected_pair_with_propagation(
            NodeId(1),
            NodeId(2),
            failure.clone(),
            profile(0, &[], &[]),
        )
        .unwrap();
    let receive = right.submit_read(read(3));
    rt.block_on(left.submit_write(write(b"old")))
        .unwrap()
        .unwrap();
    let blocked = left.submit_write(write(b"new"));
    let error = rt.block_on(receive).unwrap().unwrap_err();
    assert_eq!(error.certainty(), CompletionCertainty::NotApplied);
    assert_eq!(error.error().bytes_transferred(), 0);
    assert_eq!(rt.snapshot().now.as_nanos(), 5);
    assert_eq!(
        rt.block_on(blocked).unwrap().unwrap_err().certainty(),
        CompletionCertainty::NotApplied
    );
    assert!(
        net.connected_pair_with_propagation(
            NodeId(1),
            NodeId(2),
            failure.clone(),
            profile(0, &[], &[])
        )
        .is_err()
    );
    until(&mut rt, 20);
    let (replacement, peer) = net
        .connected_pair_with_propagation(NodeId(1), NodeId(2), failure, profile(0, &[], &[]))
        .unwrap();
    rt.block_on(replacement.submit_write(write(b"ok")))
        .unwrap()
        .unwrap();
    assert_eq!(
        rt.block_on(peer.submit_read(read(3)))
            .unwrap()
            .unwrap()
            .buffer,
        b"ok"
    );
    drop((left, right, replacement, peer));
    reclaimed(&mut rt, &net);
}

#[test]
fn local_close_cancels_hidden_bytes_and_future_failure_timers_without_waiting_for_recovery() {
    let (mut rt, net) = setup(3, 3);
    let (left, right) = net
        .connected_pair_with_propagation(
            NodeId(1),
            NodeId(2),
            profile(7, &[(0, 1000)], &[(2000, 3000)]),
            profile(0, &[], &[]),
        )
        .unwrap();
    let receive = right.submit_read(read(3));
    rt.block_on(left.submit_write(write(b"old")))
        .unwrap()
        .unwrap();
    let blocked = left.submit_write(write(b"new"));
    until(&mut rt, 5);
    rt.block_on(left.submit_close()).unwrap().unwrap();
    assert!(rt.block_on(receive).unwrap().is_err());
    assert!(rt.block_on(blocked).unwrap().is_err());
    drop((left, right));
    reclaimed(&mut rt, &net);
    assert_eq!(rt.snapshot().now.as_nanos(), 5);
}

#[test]
fn one_way_response_black_hole_does_not_delay_request_visibility() {
    let (mut rt, net) = setup(16, 16);
    let (client, broker) = net
        .connected_pair_with_propagation(
            NodeId(1),
            NodeId(2),
            profile(2, &[], &[]),
            profile(3, &[(0, 20)], &[]),
        )
        .unwrap();
    rt.block_on(client.submit_write(write(b"request")))
        .unwrap()
        .unwrap();
    assert_eq!(
        rt.block_on(broker.submit_read(read(16)))
            .unwrap()
            .unwrap()
            .buffer,
        b"request"
    );
    assert_eq!(rt.snapshot().now.as_nanos(), 2);
    rt.block_on(broker.submit_write(write(b"response")))
        .unwrap()
        .unwrap();
    let mut receive = client.submit_read(read(16));
    until(&mut rt, 19);
    assert!(poll(&mut receive).is_pending());
    assert_eq!(rt.block_on(receive).unwrap().unwrap().buffer, b"response");
    assert_eq!(rt.snapshot().now.as_nanos(), 20);
    drop((client, broker));
    reclaimed(&mut rt, &net);
}

#[test]
fn vectored_partial_writes_preserve_order_and_exactly_bound_the_transit_population() {
    for capacity in 1..=4 {
        for chunk in 1..=4 {
            let (mut rt, net) = setup(capacity, chunk);
            let (left, right) = net
                .connected_pair_with_propagation(
                    NodeId(1),
                    NodeId(2),
                    profile(3, &[(5, 11)], &[]),
                    profile(0, &[], &[]),
                )
                .unwrap();
            let source: Vec<_> = (0..23u8).collect();
            let mut sent = 0;
            let mut received = Vec::new();
            while sent < source.len() {
                let bytes = SharedBytes::from(source[sent..].to_vec());
                let result = rt
                    .block_on(left.submit_write_vectored(VectoredWriteRequest {
                        segments: vec![WriteSegment {
                            range: 0..bytes.len() as u32,
                            bytes,
                        }],
                    }))
                    .unwrap()
                    .unwrap();
                assert_eq!(
                    result.bytes_written,
                    (source.len() - sent).min(capacity).min(chunk)
                );
                sent += result.bytes_written;
                received.extend(
                    rt.block_on(right.submit_read(read(32)))
                        .unwrap()
                        .unwrap()
                        .buffer,
                );
                assert_eq!(received, source[..sent]);
            }
            drop((left, right));
            reclaimed(&mut rt, &net);
        }
    }
}

#[test]
fn invalid_profiles_do_not_allocate_connections_or_tasks() {
    let (mut rt, net) = setup(8, 8);
    for p in [
        profile(0, &[(5, 5)], &[]),
        profile(0, &[(5, 10), (9, 20)], &[]),
        profile(0, &[(5, 10)], &[(9, 20)]),
    ] {
        assert!(
            net.connected_pair_with_propagation(
                NodeId(1),
                NodeId(2),
                p,
                PropagationProfile::default()
            )
            .is_err()
        );
    }
    reclaimed(&mut rt, &net);
}

#[test]
fn propagation_can_precede_local_completion_and_half_close_preserves_transit_order() {
    let (mut rt, net) = setup(16, 16);
    net.set_link(
        LinkKey {
            from: NodeId(1),
            to: NodeId(2),
        },
        LinkConfig {
            latency: SimDuration::from_nanos(20),
            max_chunk_bytes: 16,
            state: LinkState::Open,
        },
    )
    .unwrap();
    let (left, right) = net
        .connected_pair_with_propagation(
            NodeId(1),
            NodeId(2),
            profile(5, &[], &[]),
            profile(0, &[], &[]),
        )
        .unwrap();
    let mut send = left.submit_write(write(b"abc"));
    let shutdown = left.submit_shutdown_write();
    // Read local completion also uses the directional link latency; inspect
    // the already-admitted bytes with a zero-delay later read operation.
    net.set_link(
        LinkKey {
            from: NodeId(1),
            to: NodeId(2),
        },
        LinkConfig::default(),
    )
    .unwrap();
    assert_eq!(
        rt.block_on(right.submit_read(read(16)))
            .unwrap()
            .unwrap()
            .buffer,
        b"abc"
    );
    assert_eq!(rt.snapshot().now.as_nanos(), 5);
    assert!(poll(&mut send).is_pending());
    assert!(
        rt.block_on(right.submit_read(read(16)))
            .unwrap()
            .unwrap()
            .end_of_stream
    );
    assert_eq!(rt.snapshot().now.as_nanos(), 5);
    rt.block_on(send).unwrap().unwrap();
    rt.block_on(shutdown).unwrap().unwrap();
    assert_eq!(rt.snapshot().now.as_nanos(), 20);
    drop((left, right));
    reclaimed(&mut rt, &net);
}

#[test]
fn partial_vectored_failure_retains_known_local_progress_without_early_peer_visibility() {
    let (mut rt, net) = setup(8, 8);
    let (left, right) = net
        .connected_pair_with_propagation(
            NodeId(1),
            NodeId(2),
            profile(5, &[], &[]),
            profile(0, &[], &[]),
        )
        .unwrap();
    net.push_fault(ScriptedFault {
        operation: NetworkOperationKind::Write,
        extra_latency: SimDuration::ZERO,
        max_bytes: Some(3),
        outcome: FaultOutcome::FailAfter {
            tag: 7,
            certainty: AfterFaultCertainty::MayHaveApplied,
        },
    })
    .unwrap();
    let mut receive = right.submit_read(read(8));
    let result = rt
        .block_on(left.submit_write_vectored(VectoredWriteRequest {
            segments: vec![
                WriteSegment {
                    bytes: SharedBytes::from(b"ab".to_vec()),
                    range: 0..2,
                },
                WriteSegment {
                    bytes: SharedBytes::from(b"cdef".to_vec()),
                    range: 0..4,
                },
            ],
        }))
        .unwrap()
        .unwrap_err();
    assert_eq!(result.error().bytes_transferred(), 3);
    assert_eq!(result.certainty(), CompletionCertainty::MayHaveApplied);
    assert!(poll(&mut receive).is_pending());
    assert_eq!(rt.block_on(receive).unwrap().unwrap().buffer, b"abc");
    assert_eq!(rt.snapshot().now.as_nanos(), 5);
    drop((left, right));
    reclaimed(&mut rt, &net);
}

#[test]
fn propagation_task_exhaustion_rolls_back_before_any_bytes_become_visible() {
    let mut rt = SimRuntime::new(RuntimeConfig {
        max_tasks: 1,
        ..RuntimeConfig::default()
    });
    let net = SimNetwork::new(rt.handle(), NetworkConfig::default()).unwrap();
    let blocker = rt.handle().spawn(std::future::pending::<()>()).unwrap();
    assert!(
        net.connected_pair_with_propagation(
            NodeId(1),
            NodeId(2),
            profile(0, &[], &[(5, 10)]),
            PropagationProfile::default()
        )
        .is_err()
    );
    assert_eq!(net.status().connections, 0);
    let (left, right) = net
        .connected_pair_with_propagation(
            NodeId(1),
            NodeId(2),
            profile(5, &[], &[]),
            PropagationProfile::default(),
        )
        .unwrap();
    let mut write = left.submit_write(write(b"never visible"));
    let Poll::Ready(Err(error)) = poll(&mut write) else {
        panic!("exhausted task slot must refuse the write");
    };
    assert_eq!(error.certainty(), CompletionCertainty::NotApplied);
    assert_eq!(error.error().bytes_transferred(), 0);
    drop(right.submit_read(read(32)));
    drop((left, right));
    blocker.abort();
    reclaimed(&mut rt, &net);
    assert_eq!(rt.snapshot().now, SimInstant::ZERO);
}
