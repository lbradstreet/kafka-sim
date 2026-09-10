use super::*;
use crate::{conformance::*, network::*};
use kr_runtime::{CompletionCertainty, RuntimeConfig, SimDuration, SimRuntime};
use std::pin::Pin;
use std::task::{Context, Poll, Waker};

fn request(owner: &SharedBytes) -> VectoredWriteRequest {
    VectoredWriteRequest {
        segments: vec![WriteSegment {
            bytes: owner.clone(),
            range: 0..1,
        }],
    }
}
fn poll_once<F: Future + Unpin>(future: &mut F) -> Poll<F::Output> {
    Pin::new(future).poll(&mut Context::from_waker(Waker::noop()))
}
fn memory_config() -> MemoryNetworkConfig {
    MemoryNetworkConfig {
        max_operation_bytes: 64,
        directional_buffer_bytes: 16,
        max_chunk_bytes: 3,
        ..MemoryNetworkConfig::default()
    }
}
fn sim_config() -> NetworkConfig {
    NetworkConfig {
        max_operation_bytes: 64,
        directional_buffer_bytes: 16,
        default_link: LinkConfig {
            max_chunk_bytes: 3,
            ..LinkConfig::default()
        },
        ..NetworkConfig::default()
    }
}

#[test]
fn memory_and_simulated_streams_pass_shared_warm_and_cold_vectored_conformance() {
    for cold in [false, true] {
        let mut runtime = SimRuntime::default();
        let network = MemoryNetwork::new(memory_config()).unwrap();
        let (left, right) = network.connected_pair().unwrap();
        runtime
            .block_on(async move {
                if cold {
                    check_cold_vectored_stream_provider(
                        &ColdStream::new(left),
                        &ColdStream::new(right),
                    )
                    .await
                } else {
                    check_vectored_stream_provider(&left, &right).await
                }
            })
            .unwrap()
            .unwrap();
        let mut runtime = SimRuntime::default();
        let network = SimNetwork::new(runtime.handle(), sim_config()).unwrap();
        let (left, right) = network.connected_pair(NodeId(1), NodeId(2)).unwrap();
        runtime
            .block_on(async move {
                if cold {
                    check_cold_vectored_stream_provider(
                        &ColdStream::new(left),
                        &ColdStream::new(right),
                    )
                    .await
                } else {
                    check_vectored_stream_provider(&left, &right).await
                }
            })
            .unwrap()
            .unwrap();
    }
}

#[test]
fn memory_and_simulated_streams_pass_shared_blocked_vectored_conformance() {
    for cold in [false, true] {
        let mut runtime = SimRuntime::default();
        let network = MemoryNetwork::new(MemoryNetworkConfig {
            directional_buffer_bytes: 4,
            ..memory_config()
        })
        .unwrap();
        let (left, right) = network.connected_pair().unwrap();
        runtime
            .block_on(async move {
                for byte in b"full" {
                    left.submit_write(WriteRequest {
                        buffer: vec![*byte],
                    })
                    .await
                    .unwrap();
                }
                if cold {
                    check_cold_blocked_vectored_stream_provider(
                        &ColdStream::new(left),
                        &ColdStream::new(right),
                        b"full",
                    )
                    .await
                } else {
                    check_blocked_vectored_stream_provider(&left, &right, b"full").await
                }
            })
            .unwrap()
            .unwrap();
        assert_eq!(network.status().inflight_operations, 0);
        let mut runtime = SimRuntime::default();
        let network = SimNetwork::new(
            runtime.handle(),
            NetworkConfig {
                directional_buffer_bytes: 4,
                ..sim_config()
            },
        )
        .unwrap();
        let (left, right) = network.connected_pair(NodeId(1), NodeId(2)).unwrap();
        runtime
            .block_on(async move {
                for byte in b"full" {
                    left.submit_write(WriteRequest {
                        buffer: vec![*byte],
                    })
                    .await
                    .unwrap();
                }
                if cold {
                    check_cold_blocked_vectored_stream_provider(
                        &ColdStream::new(left),
                        &ColdStream::new(right),
                        b"full",
                    )
                    .await
                } else {
                    check_blocked_vectored_stream_provider(&left, &right, b"full").await
                }
            })
            .unwrap()
            .unwrap();
        assert_eq!(network.status().inflight_operations, 0);
        assert_eq!(network.status().outstanding_write_bytes, 0);
    }
}

#[test]
fn validation_charges_unique_whole_allocations_including_nested_views() {
    let owner = SharedBytes::from(vec![0; 64]);
    let small = owner.slice(12..16).unwrap().slice(1..2).unwrap();
    let alias = request(&small);
    assert_eq!(
        alias.validate(64, 64).unwrap(),
        VectoredWriteSize {
            payload_bytes: 1,
            retained_bytes: 64
        }
    );
    assert!(alias.validate(64, 63).is_err());
    let repeated = VectoredWriteRequest {
        segments: vec![alias.segments[0].clone(); 8],
    };
    assert_eq!(
        repeated.validate(64, 64).unwrap(),
        VectoredWriteSize {
            payload_bytes: 8,
            retained_bytes: 64
        }
    );
    let distinct = SharedBytes::from(vec![0; 64]);
    let separate = VectoredWriteRequest {
        segments: vec![
            alias.segments[0].clone(),
            request(&distinct).segments.remove(0),
        ],
    };
    assert!(separate.validate(64, 64).is_err());
}

#[test]
fn byte_rejection_is_not_applied_and_does_not_consume_the_next_fault() {
    let mut runtime = SimRuntime::default();
    let network = SimNetwork::new(runtime.handle(), sim_config()).unwrap();
    let (left, _right) = network.connected_pair(NodeId(1), NodeId(2)).unwrap();
    network
        .push_fault(ScriptedFault::fail_before(NetworkOperationKind::Write, 19))
        .unwrap();
    let large = SharedBytes::from(vec![0; 65]);
    let segments = request(&large).segments;
    let pointer = segments.as_ptr();
    let rejection = runtime
        .block_on(left.submit_write_vectored(VectoredWriteRequest { segments }))
        .unwrap()
        .unwrap_err();
    assert_eq!(rejection.certainty(), CompletionCertainty::NotApplied);
    assert!(matches!(
        rejection.error().error(),
        NetworkError::InvalidRequest { .. }
    ));
    assert_eq!(rejection.error().segments.as_ptr(), pointer);
    assert_eq!(network.status().fault_hits, 0);
    assert_eq!(network.status().inflight_operations, 0);
    let valid = SharedBytes::from(vec![1]);
    let failure = runtime
        .block_on(left.submit_write_vectored(request(&valid)))
        .unwrap()
        .unwrap_err();
    assert_eq!(failure.error().error(), &NetworkError::Injected { tag: 19 });
    assert_eq!(network.status().fault_hits, 1);
}

#[test]
fn write_byte_exhaustion_returns_ownership_and_does_not_take_read_reserves() {
    let mut runtime = SimRuntime::default();
    let network = SimNetwork::new(
        runtime.handle(),
        NetworkConfig {
            max_operation_bytes: 8,
            max_outstanding_write_bytes: 8,
            max_outstanding_read_bytes: 8,
            directional_buffer_bytes: 1,
            ..NetworkConfig::default()
        },
    )
    .unwrap();
    let (left, right) = network.connected_pair(NodeId(1), NodeId(2)).unwrap();
    runtime
        .block_on(right.submit_write(WriteRequest { buffer: vec![7] }))
        .unwrap()
        .unwrap();
    runtime
        .block_on(left.submit_write(WriteRequest { buffer: vec![9] }))
        .unwrap()
        .unwrap();
    let owner = SharedBytes::from(vec![42; 8]);
    let mut blocked = left.submit_write_vectored(request(&owner));
    assert!(poll_once(&mut blocked).is_pending());
    drop(blocked);
    assert_eq!(owner.strong_count(), 2);
    assert_eq!(network.status().outstanding_write_bytes, 8);
    let segments = request(&owner).segments;
    let pointer = segments.as_ptr();
    let failure = runtime
        .block_on(left.submit_write_vectored(VectoredWriteRequest { segments }))
        .unwrap()
        .unwrap_err();
    assert_eq!(failure.certainty(), CompletionCertainty::NotApplied);
    assert_eq!(failure.error().bytes_transferred(), 0);
    assert_eq!(failure.error().segments.as_ptr(), pointer);
    assert_eq!(
        failure.error().error(),
        &NetworkError::ResourceExhausted {
            resource: "outstanding write bytes",
            limit: 8
        }
    );
    drop(failure);
    let ack = runtime
        .block_on(left.submit_read(ReadRequest {
            buffer: vec![],
            max_bytes: 1,
        }))
        .unwrap()
        .unwrap();
    assert_eq!(ack.buffer, [7]);
    assert_eq!(network.status().outstanding_read_bytes, 0);
    assert_eq!(network.status().outstanding_write_bytes, 8);
    assert_eq!(
        runtime
            .block_on(right.submit_read(ReadRequest {
                buffer: vec![],
                max_bytes: 1
            }))
            .unwrap()
            .unwrap()
            .buffer,
        [9]
    );
    assert_eq!(
        runtime
            .block_on(right.submit_read(ReadRequest {
                buffer: vec![],
                max_bytes: 1
            }))
            .unwrap()
            .unwrap()
            .buffer,
        [42]
    );
    assert_eq!(owner.strong_count(), 1);
    assert_eq!(network.status().outstanding_write_bytes, 0);
}

#[test]
fn cold_admission_happens_on_first_poll_and_stall_retains_until_teardown() {
    let runtime = SimRuntime::default();
    let network = SimNetwork::new(runtime.handle(), sim_config()).unwrap();
    let (left, right) = network.connected_pair(NodeId(1), NodeId(2)).unwrap();
    network
        .push_fault(ScriptedFault::stall(NetworkOperationKind::Write))
        .unwrap();
    let owner = SharedBytes::from(vec![1]);
    let cold = ColdStream::new(left);
    let never = cold.write_vectored(request(&owner));
    assert_eq!(network.status().inflight_operations, 0);
    assert_eq!(network.status().fault_hits, 0);
    drop(never);
    assert_eq!(owner.strong_count(), 1);
    let mut admitted = Box::pin(cold.write_vectored(request(&owner)));
    for _ in 0..3 {
        assert!(poll_once(&mut admitted).is_pending());
    }
    assert_eq!(network.status().fault_hits, 1);
    assert_eq!(network.status().inflight_operations, 1);
    drop(admitted);
    assert_eq!(owner.strong_count(), 2);
    drop(cold);
    drop(right);
    drop(network);
    assert_eq!(owner.strong_count(), 1);
    runtime.finish().unwrap();
}

#[test]
fn exact_prefix_and_certainty_match_the_oracle_across_segment_boundaries_and_faults() {
    let mut crossed_boundary = 0;
    let mut partial_segment = 0;
    let mut after_effect = 0;
    for seed in 0u64..96 {
        let config = RuntimeConfig {
            seed,
            ..RuntimeConfig::default()
        };
        let mut runtime = SimRuntime::new(config);
        let limit = (seed as usize % 7) + 1;
        let network = SimNetwork::new(
            runtime.handle(),
            NetworkConfig {
                default_link: LinkConfig {
                    max_chunk_bytes: limit,
                    latency: SimDuration::from_nanos(seed % 5),
                    ..LinkConfig::default()
                },
                ..sim_config()
            },
        )
        .unwrap();
        let (left, right) = network.connected_pair(NodeId(1), NodeId(2)).unwrap();
        let a = SharedBytes::from(vec![99, 1, 2, 99]);
        let b = SharedBytes::from(vec![99, 3, 4, 5, 99]);
        let c = SharedBytes::from(vec![99, 6, 7, 8, 9, 99]);
        let expected = [1, 2, 3, 4, 5, 6, 7, 8, 9];
        let choice = seed % 4;
        match choice {
            1 => network
                .push_fault(ScriptedFault::fail_before(
                    NetworkOperationKind::Write,
                    seed,
                ))
                .unwrap(),
            2 => network
                .push_fault(ScriptedFault::fail_after(
                    NetworkOperationKind::Write,
                    seed,
                    AfterFaultCertainty::Applied,
                ))
                .unwrap(),
            3 => network
                .push_fault(ScriptedFault::fail_after(
                    NetworkOperationKind::Write,
                    seed,
                    AfterFaultCertainty::MayHaveApplied,
                ))
                .unwrap(),
            _ => {}
        }
        let segments = vec![
            WriteSegment {
                bytes: a.clone(),
                range: 1..3,
            },
            WriteSegment {
                bytes: b.clone(),
                range: 1..4,
            },
            WriteSegment {
                bytes: c.clone(),
                range: 1..5,
            },
        ];
        let pointer = segments.as_ptr();
        let output = runtime
            .block_on(left.submit_write_vectored(VectoredWriteRequest { segments }))
            .unwrap();
        let progress = if choice == 1 { 0 } else { limit };
        match output {
            Ok(result) => {
                assert_eq!(choice, 0, "seed={seed}");
                assert_eq!(result.bytes_written, progress, "seed={seed}");
                assert_eq!(result.segments.as_ptr(), pointer);
            }
            Err(error) => {
                let certainty = match choice {
                    1 => CompletionCertainty::NotApplied,
                    2 => CompletionCertainty::Applied,
                    3 => CompletionCertainty::MayHaveApplied,
                    _ => panic!("seed={seed} unexpected failure"),
                };
                assert_eq!(error.certainty(), certainty, "seed={seed}");
                assert_eq!(error.error().bytes_transferred(), progress, "seed={seed}");
                assert_eq!(error.error().segments.as_ptr(), pointer);
            }
        }
        if progress > 0 {
            let result = runtime
                .block_on(right.submit_read(ReadRequest {
                    buffer: vec![],
                    max_bytes: progress,
                }))
                .unwrap()
                .unwrap();
            assert_eq!(result.buffer, expected[..progress], "seed={seed}");
            crossed_boundary += usize::from(progress > 2);
            partial_segment += usize::from(progress != 2 && progress != 5);
            after_effect += usize::from(choice >= 2);
        }
        assert_eq!(a.strong_count(), 1, "seed={seed}");
        assert_eq!(b.strong_count(), 1, "seed={seed}");
        assert_eq!(c.strong_count(), 1, "seed={seed}");
        assert_eq!(network.status().outstanding_write_bytes, 0, "seed={seed}");
    }
    assert!(crossed_boundary > 0 && partial_segment > 0 && after_effect > 0);
}

#[test]
fn memory_vectored_futures_are_send_and_buffers_cannot_be_reused_while_owned() {
    fn send<T: Send>(_: T) {}
    let network = MemoryNetwork::new(memory_config()).unwrap();
    let (left, _right) = network.connected_pair().unwrap();
    let mut owner = SharedBytes::from(vec![1]);
    let response = left.submit_write_vectored(request(&owner));
    assert!(owner.try_as_mut().is_none());
    send(response);
    assert!(owner.try_as_mut().is_some());
    send(ColdStream::new(left).write_vectored(request(&owner)));
}

#[test]
fn memory_and_simulated_streams_pass_shared_exhausted_vectored_conformance() {
    for cold in [false, true] {
        let mut runtime = SimRuntime::default();
        let network = MemoryNetwork::new(MemoryNetworkConfig {
            directional_buffer_bytes: 1,
            max_inflight_operations: 1,
            ..memory_config()
        })
        .unwrap();
        let (left, right) = network.connected_pair().unwrap();
        runtime
            .block_on(left.submit_write(WriteRequest { buffer: vec![1] }))
            .unwrap()
            .unwrap();
        let owner = SharedBytes::from(vec![42]);
        drop(left.submit_write_vectored(request(&owner)));
        runtime
            .block_on(async move {
                if cold {
                    check_cold_exhausted_vectored_stream_provider(&ColdStream::new(left)).await
                } else {
                    check_exhausted_vectored_stream_provider(&left).await
                }
            })
            .unwrap()
            .unwrap();
        drop(right);
        assert_eq!(owner.strong_count(), 1);
        assert_eq!(network.status().inflight_operations, 0);

        let mut runtime = SimRuntime::default();
        let network = SimNetwork::new(
            runtime.handle(),
            NetworkConfig {
                directional_buffer_bytes: 1,
                max_inflight_operations: 1,
                ..sim_config()
            },
        )
        .unwrap();
        let (left, right) = network.connected_pair(NodeId(1), NodeId(2)).unwrap();
        runtime
            .block_on(left.submit_write(WriteRequest { buffer: vec![1] }))
            .unwrap()
            .unwrap();
        let owner = SharedBytes::from(vec![42]);
        drop(left.submit_write_vectored(request(&owner)));
        runtime
            .block_on(async move {
                if cold {
                    check_cold_exhausted_vectored_stream_provider(&ColdStream::new(left)).await
                } else {
                    check_exhausted_vectored_stream_provider(&left).await
                }
            })
            .unwrap()
            .unwrap();
        drop(right);
        assert_eq!(owner.strong_count(), 1);
        assert_eq!(network.status().inflight_operations, 0);
    }
}
