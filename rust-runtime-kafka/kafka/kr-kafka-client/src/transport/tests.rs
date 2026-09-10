use super::*;
#[cfg(feature = "request-observation")]
mod observations;
use kr_kafka_protocol::{
    plan::{EncodeLimits, Records},
    wire::Writer,
};
use kr_runtime::{RuntimeDuration, SimRuntime};
use kr_runtime_io::network::{
    AfterFaultCertainty, ByteStreamSubmit, ColdStream, FaultOutcome, LinkConfig, MemoryNetwork,
    MemoryNetworkConfig, NetworkConfig, NetworkOperationKind, NodeId, ScriptedFault, SimNetwork,
};
use std::{
    future::poll_fn,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    task::Waker,
};

fn protocol_plan(correlation: i32, chunks: &[SharedBytes]) -> SendPlan<'static> {
    let mut writer = Writer::new(13, true, EncodeLimits::default());
    writer.write_i32(0).unwrap();
    writer.write_i32(correlation).unwrap();
    writer.write_records(&Records::Chunks(chunks)).unwrap();
    writer.finish_frame().unwrap().try_into_owned().unwrap()
}
fn plan(correlation: i32, chunks: &[SharedBytes]) -> OwnedSendPlan {
    OwnedSendPlan::from_protocol(
        protocol_plan(correlation, chunks),
        PlanLimits {
            coalesce_below_bytes: 0,
            ..PlanLimits::default()
        },
    )
    .unwrap()
}
fn response(correlation: i32, payload: &[u8]) -> Vec<u8> {
    let mut bytes = Vec::from(i32::try_from(payload.len() + 4).unwrap().to_be_bytes());
    bytes.extend_from_slice(&correlation.to_be_bytes());
    bytes.extend_from_slice(payload);
    bytes
}
fn config(mode: WriteMode) -> DriverConfig {
    DriverConfig {
        mode,
        staging_bytes: 11,
        max_operation_bytes: 128,
        max_inflight_requests: 5,
        rx_bytes: 128,
    }
}
fn memory() -> MemoryNetwork {
    MemoryNetwork::new(MemoryNetworkConfig {
        max_operation_bytes: 128,
        directional_buffer_bytes: 17,
        max_chunk_bytes: 3,
        ..MemoryNetworkConfig::default()
    })
    .unwrap()
}
fn simulated(runtime: &SimRuntime) -> SimNetwork {
    SimNetwork::new(
        runtime.handle(),
        NetworkConfig {
            max_operation_bytes: 128,
            directional_buffer_bytes: 17,
            default_link: LinkConfig {
                max_chunk_bytes: 3,
                ..LinkConfig::default()
            },
            ..NetworkConfig::default()
        },
    )
    .unwrap()
}

#[derive(Debug)]
enum Event {
    Admitted(i32),
    Write(i32, usize, usize, CompletionCertainty),
    Frame(i32, Vec<u8>),
    Retiring(RetireReason),
    Retired(i32, usize, CompletionCertainty),
    Released,
}
async fn next<S: ByteStreamVectoredSubmit>(
    driver: &mut ConnectionDriver<S>,
    now: RuntimeInstant,
) -> Event {
    poll_fn(|cx| match driver.poll_event(cx, now) {
        Poll::Pending => Poll::Pending,
        Poll::Ready(None) => panic!("driver exhausted before expected event"),
        Poll::Ready(Some(event)) => Poll::Ready(match event {
            DriverEvent::WriteAdmitted { correlation } => Event::Admitted(correlation),
            DriverEvent::WriteProgress {
                correlation,
                bytes,
                confirmed,
                certainty,
            } => Event::Write(correlation, bytes, confirmed, certainty),
            DriverEvent::Frame { correlation, bytes } => Event::Frame(correlation, bytes.to_vec()),
            DriverEvent::Retiring { reason } => Event::Retiring(reason),
            DriverEvent::RequestRetired {
                correlation,
                confirmed,
                certainty,
            } => Event::Retired(correlation, confirmed, certainty),
            DriverEvent::Released => Event::Released,
        }),
    })
    .await
}

async fn server<S: ByteStreamSubmit>(stream: S, requests: Vec<Vec<u8>>, responses: Vec<Vec<u8>>) {
    let stream = ColdStream::new(stream);
    for (expected, response) in requests.into_iter().zip(responses) {
        let mut read = Vec::new();
        while read.len() < expected.len() {
            let max_bytes = expected.len() - read.len();
            let result = stream
                .read(ReadRequest {
                    buffer: read,
                    max_bytes,
                })
                .await
                .unwrap();
            assert!(!result.end_of_stream);
            read = result.buffer;
        }
        assert_eq!(read, expected);
        let mut written = 0;
        while written < response.len() {
            let result = stream
                .write(WriteRequest {
                    buffer: response[written..].to_vec(),
                })
                .await
                .unwrap();
            assert!(result.bytes_written > 0);
            written += result.bytes_written;
        }
    }
    stream.close().await.unwrap();
}

fn roundtrip<S: ByteStreamVectoredSubmit>(
    runtime: &mut SimRuntime,
    left: S,
    right: S,
    mode: WriteMode,
) -> (Vec<Vec<u8>>, u64) {
    let chunks = [
        SharedBytes::from((0..23_u8).collect::<Vec<_>>()),
        SharedBytes::from(vec![92; 19]),
    ];
    let requests: Vec<_> = (40..43)
        .map(|id| protocol_plan(id, &chunks).to_vec().unwrap())
        .collect();
    let responses: Vec<_> = (40..43).map(|id| response(id, &[id as u8; 7])).collect();
    let expected = responses.clone();
    let server = runtime
        .handle()
        .spawn(server(right, requests.clone(), responses))
        .unwrap();
    let mut driver = ConnectionDriver::new(left, config(mode)).unwrap();
    for correlation in 40..43 {
        driver
            .enqueue(SendRequest {
                correlation,
                deadline: RuntimeInstant::MAX,
                plan: plan(correlation, &chunks),
            })
            .unwrap();
    }
    let result = runtime
        .block_on(async {
            let mut frames = Vec::new();
            let mut progress = [0; 3];
            let mut writes = 0;
            let mut admitted = Vec::new();
            loop {
                match next(&mut driver, RuntimeInstant::ZERO).await {
                    Event::Admitted(id) => {
                        assert!(!admitted.contains(&id));
                        admitted.push(id);
                    }
                    Event::Write(id, bytes, confirmed, certainty) => {
                        let index = usize::try_from(id - 40).unwrap();
                        assert!(bytes > 0 && bytes <= 3);
                        assert_eq!(progress[index] + bytes, confirmed);
                        assert_eq!(certainty, CompletionCertainty::Applied);
                        progress[index] = confirmed;
                        writes += 1;
                    }
                    Event::Frame(id, bytes) => {
                        assert_eq!(id, 40 + frames.len() as i32);
                        frames.push(bytes);
                        if frames.len() == 3 {
                            driver.retire(RetireReason::Requested);
                        }
                    }
                    Event::Retiring(_) => assert_eq!(frames.len(), 3),
                    Event::Released => break,
                    Event::Retired(..) => panic!("successful frame lost its FIFO request"),
                }
            }
            assert!(writes > 3, "partial write path was not exercised");
            assert_eq!(admitted, [40, 41, 42]);
            assert_eq!(
                progress.to_vec(),
                requests.iter().map(Vec::len).collect::<Vec<_>>()
            );
            assert_eq!(frames, expected);
            assert_eq!(driver.pending_requests(), 0);
            server.await.unwrap();
            frames
        })
        .unwrap();
    (result, driver.staged_copies())
}

#[test]
fn memory_and_simulated_streams_have_identical_staging_and_vectored_frames() {
    let mut baseline = None;
    for mode in [WriteMode::Staging, WriteMode::Vectored] {
        let mut runtime = SimRuntime::default();
        let network = memory();
        let (left, right) = network.connected_pair().unwrap();
        let (frames, copies) = roundtrip(&mut runtime, left, right, mode);
        if mode == WriteMode::Staging {
            assert!(copies > 3 * 42);
        } else {
            assert_eq!(copies, 0);
        }
        assert_eq!(network.status().inflight_operations, 0);
        if let Some(expected) = &baseline {
            assert_eq!(&frames, expected);
        } else {
            baseline = Some(frames);
        }
        runtime.finish().unwrap();

        let mut runtime = SimRuntime::default();
        let network = simulated(&runtime);
        let (left, right) = network.connected_pair(NodeId(1), NodeId(2)).unwrap();
        let (frames, _) = roundtrip(&mut runtime, left, right, mode);
        assert_eq!(&frames, baseline.as_ref().unwrap());
        assert_eq!(network.status().inflight_operations, 0);
        drop(network);
        runtime.finish().unwrap();
    }
}

#[test]
fn response_visible_before_delayed_write_completion_parks_until_the_owned_write_wakes() {
    let mut runtime = SimRuntime::new(kr_runtime::RuntimeConfig {
        max_steps_per_run: 1000,
        ..Default::default()
    });
    let network = SimNetwork::new(
        runtime.handle(),
        NetworkConfig {
            max_operation_bytes: 128,
            directional_buffer_bytes: 128,
            default_link: LinkConfig {
                max_chunk_bytes: 128,
                ..Default::default()
            },
            ..Default::default()
        },
    )
    .unwrap();
    let (left, right) = network.connected_pair(NodeId(1), NodeId(2)).unwrap();
    network
        .push_fault(ScriptedFault {
            operation: NetworkOperationKind::Write,
            extra_latency: RuntimeDuration::from_nanos(100),
            max_bytes: None,
            outcome: FaultOutcome::Continue,
        })
        .unwrap();
    let mut driver = ConnectionDriver::new(left, config(WriteMode::Vectored)).unwrap();
    let expected = protocol_plan(1, &[]).to_vec().unwrap();
    driver
        .enqueue(SendRequest {
            correlation: 1,
            deadline: RuntimeInstant::MAX,
            plan: plan(1, &[]),
        })
        .unwrap();
    let peer = runtime
        .handle()
        .spawn(server(right, vec![expected], vec![response(1, &[])]))
        .unwrap();
    runtime
        .block_on(async {
            let mut saw_progress = false;
            loop {
                match next(&mut driver, RuntimeInstant::ZERO).await {
                    Event::Write(..) => saw_progress = true,
                    Event::Frame(id, _) => {
                        assert!(saw_progress);
                        assert_eq!(id, 1);
                        driver.retire(RetireReason::Requested);
                    }
                    Event::Released => break,
                    Event::Retired(..) => {
                        panic!("response was lost while the write notification lagged")
                    }
                    _ => {}
                }
            }
            peer.await.unwrap();
        })
        .unwrap();
    assert_eq!(network.status().inflight_operations, 0);
    drop(network);
    runtime.finish().unwrap();
}

#[test]
fn owned_conversion_coalesces_small_runs_and_preserves_large_allocation_identity() {
    let chunks = [
        SharedBytes::from(vec![1; 20]),
        SharedBytes::from(vec![2; 30]),
        SharedBytes::from(vec![3; 512]),
        SharedBytes::from(vec![4; 10]),
        SharedBytes::from(vec![5; 10]),
    ];
    let protocol = protocol_plan(2, &chunks);
    let exact = protocol.to_vec().unwrap();
    let normalized = OwnedSendPlan::from_protocol(protocol, PlanLimits::default()).unwrap();
    assert_eq!(normalized.segments.len(), 3);
    assert!(normalized.segments[1].shares_allocation(&chunks[2]));
    assert_eq!(
        normalized
            .segments
            .iter()
            .flat_map(|span| span.as_slice())
            .copied()
            .collect::<Vec<_>>(),
        exact
    );
    assert_eq!(normalized.coalesced_bytes, exact.len() - 512);
    let excessive: Vec<_> = (0..5).map(|_| SharedBytes::from(vec![1; 512])).collect();
    assert!(matches!(
        OwnedSendPlan::from_protocol(
            protocol_plan(0, &excessive),
            PlanLimits {
                max_segments: 4,
                ..PlanLimits::default()
            }
        ),
        Err(TransportError::ResourceExhausted {
            resource: "plan segments",
            limit: 4
        })
    ));
    assert!(matches!(
        OwnedSendPlan::from_protocol(
            protocol_plan(0, &chunks),
            PlanLimits {
                max_coalesced_bytes: 1,
                ..PlanLimits::default()
            }
        ),
        Err(TransportError::ResourceExhausted {
            resource: "coalesced bytes",
            ..
        })
    ));
    let mut borrowed = Writer::new(0, false, EncodeLimits::default());
    borrowed
        .write_records(&Records::Borrowed(b"static but still borrowed"))
        .unwrap();
    assert!(borrowed.finish().unwrap().into_shared_segments().is_err());
}

#[test]
fn seeded_confirmed_cursors_never_skip_a_partial_stage_or_mutate_retry_bytes() {
    for seed in 1..65_usize {
        let chunks = [
            SharedBytes::from(vec![1; 17]),
            SharedBytes::from(vec![2; 13]),
        ];
        let expected = protocol_plan(1, &chunks).to_vec().unwrap();
        for vectored in [false, true] {
            let mut cursor = plan(1, &chunks).into_cursor();
            let mut actual = Vec::new();
            let mut buffer = Vec::with_capacity(16);
            let mut stages = 0;
            while cursor.remaining() != 0 {
                let staged = if vectored {
                    let stage = cursor.stage_vectored(64, 1 + seed % 3).unwrap();
                    stage.validate(1 + seed % 3, 64).unwrap();
                    stage
                        .segments
                        .iter()
                        .flat_map(|span| {
                            span.bytes.as_slice()
                                [span.range.start as usize..span.range.end as usize]
                                .iter()
                        })
                        .copied()
                        .collect::<Vec<_>>()
                } else {
                    cursor.stage_contiguous(&mut buffer, 1 + seed % 16).unwrap();
                    buffer.clone()
                };
                let count = (1 + (seed + stages) % staged.len()).min(staged.len());
                assert!(cursor.stage_vectored(64, 4).is_err());
                let before = cursor.confirmed();
                assert!(cursor.confirm(staged.len() + 1).is_err());
                assert_eq!(cursor.confirmed(), before);
                actual.extend_from_slice(&staged[..count]);
                cursor.confirm(count).unwrap();
                stages += 1;
            }
            assert_eq!(actual, expected, "seed={seed} vectored={vectored}");
            assert_eq!(cursor.confirmed(), expected.len());
            assert_eq!(chunks[0].as_slice(), &[1; 17]);
        }
    }
}

#[test]
fn enqueue_is_cold_and_bounds_allocation_before_any_request_prefix_is_sent() {
    let network = memory();
    let (left, _right) = network.connected_pair().unwrap();
    let mut driver = ConnectionDriver::new(left, config(WriteMode::Vectored)).unwrap();
    let huge = SharedBytes::from(vec![0; 256]).slice(0..1).unwrap();
    let rejected = driver
        .enqueue(SendRequest {
            correlation: 1,
            deadline: RuntimeInstant::MAX,
            plan: plan(1, &[huge]),
        })
        .unwrap_err();
    assert!(matches!(
        rejected.error,
        TransportError::ResourceExhausted {
            resource: "retained write allocation",
            limit: 128
        }
    ));
    for correlation in 0..5 {
        driver
            .enqueue(SendRequest {
                correlation,
                deadline: RuntimeInstant::MAX,
                plan: plan(correlation, &[]),
            })
            .unwrap();
    }
    assert!(matches!(
        driver
            .enqueue(SendRequest {
                correlation: 6,
                deadline: RuntimeInstant::MAX,
                plan: plan(6, &[])
            })
            .unwrap_err()
            .error,
        TransportError::ResourceExhausted {
            resource: "inflight requests",
            limit: 5
        }
    ));
    assert_eq!(network.status().inflight_operations, 0);
    assert_eq!(network.status().pending_writes, 0);
}

#[test]
fn write_failure_certainty_is_sticky_across_partial_success_and_later_not_applied() {
    for mode in [WriteMode::Staging, WriteMode::Vectored] {
        for first_success in [false, true] {
            for after in [
                None,
                Some(AfterFaultCertainty::Applied),
                Some(AfterFaultCertainty::MayHaveApplied),
            ] {
                let mut runtime = SimRuntime::default();
                let network = simulated(&runtime);
                let (left, _right) = network.connected_pair(NodeId(1), NodeId(2)).unwrap();
                if first_success {
                    network
                        .push_fault(ScriptedFault {
                            operation: NetworkOperationKind::Write,
                            extra_latency: RuntimeDuration::ZERO,
                            max_bytes: Some(2),
                            outcome: FaultOutcome::Continue,
                        })
                        .unwrap();
                }
                network
                    .push_fault(after.map_or_else(
                        || ScriptedFault::fail_before(NetworkOperationKind::Write, 99),
                        |certainty| {
                            ScriptedFault::fail_after(NetworkOperationKind::Write, 99, certainty)
                        },
                    ))
                    .unwrap();
                let mut driver = ConnectionDriver::new(left, config(mode)).unwrap();
                driver
                    .enqueue(SendRequest {
                        correlation: 1,
                        deadline: RuntimeInstant::MAX,
                        plan: plan(1, &[SharedBytes::from(vec![1; 32])]),
                    })
                    .unwrap();
                let terminal = runtime
                    .block_on(async {
                        let mut terminal = None;
                        loop {
                            match next(&mut driver, RuntimeInstant::ZERO).await {
                                Event::Retired(id, bytes, certainty) => {
                                    assert_eq!(id, 1);
                                    terminal = Some((bytes, certainty));
                                }
                                Event::Released => break,
                                Event::Frame(..) => {
                                    panic!("socket failure produced a Kafka response")
                                }
                                _ => {}
                            }
                        }
                        terminal.unwrap()
                    })
                    .unwrap();
                let expected = match after {
                    Some(AfterFaultCertainty::MayHaveApplied) => {
                        CompletionCertainty::MayHaveApplied
                    }
                    Some(AfterFaultCertainty::Applied) => CompletionCertainty::Applied,
                    None if first_success => CompletionCertainty::Applied,
                    None => CompletionCertainty::NotApplied,
                };
                assert_eq!(
                    terminal.1, expected,
                    "mode={mode:?} first={first_success} after={after:?}"
                );
                assert_eq!(terminal.0 > 0, first_success || after.is_some());
                assert_eq!(network.status().inflight_operations, 0);
                assert!(driver.is_released());
            }
        }
    }
}

#[test]
fn timeout_of_a_blocked_write_retains_read_write_close_until_their_actual_terminal_events() {
    let network = memory();
    let (left, right) = network.connected_pair().unwrap();
    let mut runtime = SimRuntime::default();
    runtime
        .block_on(async {
            let mut written = 0;
            while written < 17 {
                written += left
                    .submit_write(WriteRequest {
                        buffer: vec![0; 17 - written],
                    })
                    .await
                    .unwrap()
                    .bytes_written;
            }
        })
        .unwrap();
    let mut driver = ConnectionDriver::new(left, config(WriteMode::Vectored)).unwrap();
    driver
        .enqueue(SendRequest {
            correlation: 7,
            deadline: RuntimeInstant::from_nanos(10),
            plan: plan(7, &[SharedBytes::from(vec![5; 10])]),
        })
        .unwrap();
    assert!(matches!(
        driver.poll_event(
            &mut Context::from_waker(Waker::noop()),
            RuntimeInstant::ZERO
        ),
        Poll::Ready(Some(DriverEvent::WriteAdmitted { correlation: 7 }))
    ));
    assert!(
        driver
            .poll_event(
                &mut Context::from_waker(Waker::noop()),
                RuntimeInstant::ZERO
            )
            .is_pending()
    );
    assert_eq!(network.status().inflight_operations, 2);
    assert_eq!(network.status().pending_writes, 1);
    assert_eq!(network.status().pending_reads, 1);
    let mut terminal = None;
    runtime
        .block_on(async {
            loop {
                match next(&mut driver, RuntimeInstant::from_nanos(10)).await {
                    Event::Retiring(reason) => {
                        assert_eq!(reason, RetireReason::Deadline { correlation: 7 });
                        assert!(!driver.is_released());
                        assert_eq!(driver.next_deadline(), None);
                    }
                    Event::Retired(id, confirmed, certainty) => {
                        terminal = Some((id, confirmed, certainty))
                    }
                    Event::Released => break,
                    _ => {}
                }
            }
            right.submit_close().await.unwrap();
        })
        .unwrap();
    assert_eq!(terminal, Some((7, 0, CompletionCertainty::NotApplied)));
    assert_eq!(network.status().inflight_operations, 0);
    assert_eq!(network.status().pending_reads, 0);
    assert_eq!(network.status().pending_writes, 0);
}

#[test]
fn malformed_oversized_and_out_of_order_frames_retire_without_allocating_a_larger_rx() {
    let cases = [
        (-1_i32).to_be_bytes().to_vec(),
        125_i32.to_be_bytes().to_vec(),
        response(99, &[]),
    ];
    for bytes in cases {
        let mut runtime = SimRuntime::default();
        let network = memory();
        let (left, right) = network.connected_pair().unwrap();
        let mut driver = ConnectionDriver::new(left, config(WriteMode::Staging)).unwrap();
        driver
            .enqueue(SendRequest {
                correlation: 1,
                deadline: RuntimeInstant::MAX,
                plan: plan(1, &[]),
            })
            .unwrap();
        let actual = bytes.clone();
        let peer = runtime
            .handle()
            .spawn(async move {
                let stream = ColdStream::new(right);
                let mut sent = 0;
                while sent < actual.len() {
                    sent += stream
                        .write(WriteRequest {
                            buffer: actual[sent..].to_vec(),
                        })
                        .await
                        .unwrap()
                        .bytes_written;
                }
                // Consume the request so unrelated directional pressure cannot mask
                // the response parser. EOF will follow the driver's retirement.
                loop {
                    let read = stream
                        .read(ReadRequest {
                            buffer: vec![],
                            max_bytes: 128,
                        })
                        .await;
                    if read.is_err() || read.as_ref().is_ok_and(|read| read.end_of_stream) {
                        break;
                    }
                }
            })
            .unwrap();
        let mut reason = None;
        runtime
            .block_on(async {
                loop {
                    match next(&mut driver, RuntimeInstant::ZERO).await {
                        Event::Retiring(value) => reason = Some(value),
                        Event::Frame(..) => panic!("malformed response escaped validation"),
                        Event::Released => break,
                        _ => {}
                    }
                }
                peer.await.unwrap();
            })
            .unwrap();
        assert!(
            matches!(reason, Some(RetireReason::Protocol(_))),
            "bytes={bytes:?} reason={reason:?}"
        );
        assert_eq!(network.status().pending_reads, 0);
    }
}

#[test]
fn shared_payload_guard_survives_driver_and_observer_drop_until_provider_terminal_release() {
    struct Guard(Arc<AtomicUsize>);
    impl Drop for Guard {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }
    let mut runtime = SimRuntime::default();
    let network = simulated(&runtime);
    let (left, right) = network.connected_pair(NodeId(1), NodeId(2)).unwrap();
    network
        .push_fault(ScriptedFault::stall(NetworkOperationKind::Write))
        .unwrap();
    let dropped = Arc::new(AtomicUsize::new(0));
    let bytes = SharedBytes::from(vec![7; 10])
        .attach_guard(Arc::new(Guard(Arc::clone(&dropped))))
        .unwrap();
    let mut driver = ConnectionDriver::new(left, config(WriteMode::Vectored)).unwrap();
    driver
        .enqueue(SendRequest {
            correlation: 1,
            deadline: RuntimeInstant::MAX,
            plan: plan(1, std::slice::from_ref(&bytes)),
        })
        .unwrap();
    drop(bytes);
    assert!(matches!(
        driver.poll_event(
            &mut Context::from_waker(Waker::noop()),
            RuntimeInstant::ZERO
        ),
        Poll::Ready(Some(DriverEvent::WriteAdmitted { correlation: 1 }))
    ));
    assert!(
        driver
            .poll_event(
                &mut Context::from_waker(Waker::noop()),
                RuntimeInstant::ZERO
            )
            .is_pending()
    );
    assert_eq!(network.status().fault_hits, 1);
    drop(driver);
    assert_eq!(
        dropped.load(Ordering::SeqCst),
        0,
        "observer drop released provider-held credit"
    );
    drop(right);
    drop(network);
    runtime.run_until_stalled().unwrap();
    runtime.finish().unwrap();
    assert_eq!(dropped.load(Ordering::SeqCst), 1);
}

#[test]
fn closing_one_topic_fences_the_whole_unsubmitted_multi_topic_plan() {
    for mode in [WriteMode::Staging, WriteMode::Vectored] {
        let mut runtime = SimRuntime::default();
        let network = memory();
        let (left, _right) = network.connected_pair().unwrap();
        let first = WriteFence::new();
        let second = WriteFence::new();
        let mut plan = plan(17, &[SharedBytes::from(vec![3; 23])]);
        plan.retain_write_fences(vec![first, second.clone()])
            .unwrap();
        second.close();
        assert!(plan.retain_write_fences(Vec::new()).unwrap_err().is_empty());
        let replacement = vec![WriteFence::new()];
        let pointer = replacement.as_ptr();
        let rejected = plan.retain_write_fences(replacement).unwrap_err();
        assert_eq!(
            rejected.as_ptr(),
            pointer,
            "rejection returns the original vector"
        );
        assert!(
            !plan.writes_allowed(),
            "neither clearing nor replacing may reopen the plan"
        );
        let mut driver = ConnectionDriver::new(left, config(mode)).unwrap();
        driver
            .enqueue(SendRequest {
                correlation: 17,
                deadline: RuntimeInstant::MAX,
                plan,
            })
            .unwrap();
        runtime
            .block_on(async {
                let mut retired = false;
                loop {
                    match next(&mut driver, RuntimeInstant::ZERO).await {
                        Event::Retiring(RetireReason::Requested) => {}
                        Event::Retired(17, 0, CompletionCertainty::NotApplied) => retired = true,
                        Event::Released => break,
                        event => panic!("fenced plan reached provider: {event:?}"),
                    }
                    assert_eq!(network.status().buffered_bytes, 0);
                }
                assert!(retired);
            })
            .unwrap();
        assert_eq!(network.status().inflight_operations, 0);
        assert_eq!(network.status().buffered_bytes, 0);
    }
}

#[test]
fn topic_close_after_write_admitted_keeps_eager_provider_evidence() {
    for mode in [WriteMode::Staging, WriteMode::Vectored] {
        let mut runtime = SimRuntime::default();
        let network = memory();
        let (left, _right) = network.connected_pair().unwrap();
        let fence = WriteFence::new();
        let mut plan = plan(19, &[SharedBytes::from(vec![3; 23])]);
        plan.retain_write_fences(vec![fence.clone()]).unwrap();
        let mut driver = ConnectionDriver::new(left, config(mode)).unwrap();
        driver
            .enqueue(SendRequest {
                correlation: 19,
                deadline: RuntimeInstant::MAX,
                plan,
            })
            .unwrap();
        assert!(matches!(
            driver.poll_event(
                &mut Context::from_waker(Waker::noop()),
                RuntimeInstant::ZERO
            ),
            Poll::Ready(Some(DriverEvent::WriteAdmitted { correlation: 19 }))
        ));
        // ByteStreamSubmit is eager: this event follows the actual provider
        // call, even though the returned response has never been polled.
        assert_eq!(network.status().buffered_bytes, 3);
        fence.close();
        driver.retire(RetireReason::Requested);
        runtime
            .block_on(async {
                let mut retired = false;
                loop {
                    match next(&mut driver, RuntimeInstant::ZERO).await {
                        Event::Retiring(RetireReason::Requested)
                        | Event::Write(19, 3, 3, CompletionCertainty::Applied) => {}
                        Event::Retired(19, 3, CompletionCertainty::Applied) => retired = true,
                        Event::Released => break,
                        event => panic!("admitted evidence was rewritten: {event:?}"),
                    }
                }
                assert!(retired);
            })
            .unwrap();
        assert_eq!(network.status().inflight_operations, 0);
    }
}

#[test]
fn closing_topic_after_admission_does_not_release_stalled_provider_payload() {
    struct Guard(Arc<AtomicUsize>);
    impl Drop for Guard {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }
    let mut runtime = SimRuntime::default();
    let network = simulated(&runtime);
    let (left, right) = network.connected_pair(NodeId(1), NodeId(2)).unwrap();
    network
        .push_fault(ScriptedFault::stall(NetworkOperationKind::Write))
        .unwrap();
    let dropped = Arc::new(AtomicUsize::new(0));
    let bytes = SharedBytes::from(vec![7; 10])
        .attach_guard(Arc::new(Guard(Arc::clone(&dropped))))
        .unwrap();
    let fence = WriteFence::new();
    let mut plan = plan(23, &[bytes]);
    plan.retain_write_fences(vec![fence.clone()]).unwrap();
    let mut driver = ConnectionDriver::new(left, config(WriteMode::Vectored)).unwrap();
    driver
        .enqueue(SendRequest {
            correlation: 23,
            deadline: RuntimeInstant::MAX,
            plan,
        })
        .unwrap();
    assert!(matches!(
        driver.poll_event(
            &mut Context::from_waker(Waker::noop()),
            RuntimeInstant::ZERO
        ),
        Poll::Ready(Some(DriverEvent::WriteAdmitted { correlation: 23 }))
    ));
    fence.close();
    assert_eq!(network.status().fault_hits, 1);
    drop(driver);
    assert_eq!(dropped.load(Ordering::SeqCst), 0);
    drop(right);
    drop(network);
    runtime.run_until_stalled().unwrap();
    runtime.finish().unwrap();
    assert_eq!(dropped.load(Ordering::SeqCst), 1);
}

#[test]
fn control_frame_owns_original_capacity_and_validates_prefix_before_use() {
    let mut frame = Vec::with_capacity(32);
    frame.extend_from_slice(&4i32.to_be_bytes());
    frame.extend_from_slice(&(-7i32).to_be_bytes());
    let pointer = frame.as_ptr();
    let plan = OwnedSendPlan::from_frame(frame, 32).unwrap();
    assert_eq!(plan.segments()[0].as_slice().as_ptr(), pointer);
    assert_eq!(plan.metadata_bytes(), 32);
    assert!(OwnedSendPlan::from_frame(vec![0, 0, 0, 5, 0, 0, 0, 0], 32).is_err());
    assert!(OwnedSendPlan::from_frame(vec![255, 255, 255, 255, 0, 0, 0, 0], 32).is_err());
    assert!(OwnedSendPlan::from_frame(Vec::with_capacity(64), 32).is_err());
}

fn header_produce_plan(
    payloads: &[Vec<SharedBytes>],
    client_id: &str,
    promote: bool,
) -> SendPlan<'static> {
    use kr_kafka_protocol::{
        Request,
        produce_request::{
            self,
            v13::{PartitionProduceData, ProduceRequest, TopicProduceData},
        },
        wire::Sequence,
    };
    let chunks: Vec<_> = payloads
        .iter()
        .map(|payload| {
            let mut chunks = vec![SharedBytes::from(vec![0xa5; 61])];
            chunks.extend(payload.iter().cloned());
            chunks
        })
        .collect();
    let partitions: Vec<_> = chunks
        .iter()
        .enumerate()
        .map(|(index, chunks)| PartitionProduceData {
            index: index as i32,
            records: Some(if promote {
                Records::HeaderAndChunks {
                    header: chunks[0].as_slice(),
                    chunks: &chunks[1..],
                }
            } else {
                Records::Chunks(chunks)
            }),
            ..Default::default()
        })
        .collect();
    let topics = [TopicProduceData {
        topic_id: [1; 16],
        partition_data: Sequence::from_slice(&partitions),
        ..Default::default()
    }];
    Request::ProduceRequest(produce_request::View::V13(ProduceRequest {
        transactional_id: None,
        acks: -1,
        timeout_ms: 1000,
        topic_data: Sequence::from_slice(&topics),
        ..Default::default()
    }))
    .plan_frame(13, 1, Some(client_id), EncodeLimits::default())
    .unwrap()
    .try_into_owned()
    .unwrap()
}

#[test]
fn promoted_headers_obey_actual_partition_segment_cap_with_coalescing_disabled_and_long_client_id()
{
    for count in [1, 4, 64] {
        let payloads: Vec<_> = (0..count)
            .map(|_| {
                vec![
                    SharedBytes::from(vec![1; 512]),
                    SharedBytes::from(vec![2; 513]),
                ]
            })
            .collect();
        for client_len in [0, 32767] {
            let client = "x".repeat(client_len);
            let ordinary = header_produce_plan(&payloads, &client, false);
            let expected = ordinary.to_vec().unwrap();
            for threshold in [0, 512] {
                let protocol = header_produce_plan(&payloads, &client, true);
                assert_eq!(
                    protocol.metadata_len(),
                    ordinary.metadata_len() + 61 * count
                );
                let metadata_capacity = protocol.metadata_capacity();
                assert!(metadata_capacity >= protocol.metadata_len());
                let plan = OwnedSendPlan::from_protocol(
                    protocol,
                    PlanLimits {
                        max_segments: 3 * count + 4,
                        coalesce_below_bytes: threshold,
                        ..PlanLimits::default()
                    },
                )
                .unwrap();
                assert!(plan.segments().len() <= 3 * count + 4);
                assert_eq!(plan.metadata_bytes(), metadata_capacity);
                assert_eq!(
                    plan.coalesced_bytes(),
                    0,
                    "headers belong to metadata before optional coalescing"
                );
                assert_eq!(
                    plan.segments()
                        .iter()
                        .flat_map(|span| span.as_slice())
                        .copied()
                        .collect::<Vec<_>>(),
                    expected
                );
                for payload in payloads.iter().flatten() {
                    assert!(
                        plan.segments()
                            .iter()
                            .any(|span| span.shares_allocation(payload)
                                && span.as_slice().as_ptr() == payload.as_slice().as_ptr()),
                        "large payload was copied"
                    );
                }
                // The whole metadata arena remains owned even for one tiny
                // trailer subview, and metadata credits cover that allocation.
                let trailer = plan.segments().last().unwrap();
                assert_eq!(trailer.allocation_len(), metadata_capacity);
            }
        }
    }
    let excessive: Vec<_> = (0..4)
        .map(|_| vec![SharedBytes::from(vec![3; 512]); 3])
        .collect();
    assert!(matches!(
        OwnedSendPlan::from_protocol(
            header_produce_plan(&excessive, "", true),
            PlanLimits {
                max_segments: 16,
                coalesce_below_bytes: 0,
                ..PlanLimits::default()
            }
        ),
        Err(TransportError::ResourceExhausted {
            resource: "plan segments",
            limit: 16
        })
    ));
}

#[test]
fn promoted_header_metadata_credit_survives_provider_ownership_after_driver_drop() {
    struct Guard(Arc<AtomicUsize>);
    impl Drop for Guard {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }
    let mut runtime = SimRuntime::default();
    let network = SimNetwork::new(
        runtime.handle(),
        NetworkConfig {
            max_operation_bytes: 2048,
            directional_buffer_bytes: 17,
            default_link: LinkConfig {
                max_chunk_bytes: 3,
                ..LinkConfig::default()
            },
            ..NetworkConfig::default()
        },
    )
    .unwrap();
    let (left, right) = network.connected_pair(NodeId(1), NodeId(2)).unwrap();
    network
        .push_fault(ScriptedFault::stall(NetworkOperationKind::Write))
        .unwrap();
    let dropped = Arc::new(AtomicUsize::new(0));
    let payloads = vec![vec![
        SharedBytes::from(vec![4; 512]),
        SharedBytes::from(vec![5; 512]),
    ]];
    let mut plan = OwnedSendPlan::from_protocol(
        header_produce_plan(&payloads, "long-client", true),
        PlanLimits {
            max_segments: 7,
            coalesce_below_bytes: 0,
            ..PlanLimits::default()
        },
    )
    .unwrap();
    let capacity = plan.segments()[0].allocation_len();
    assert_eq!(plan.metadata_bytes(), capacity);
    assert!(capacity >= 61);
    plan.retain_metadata_guard(Arc::new(Guard(dropped.clone())));
    let mut driver = ConnectionDriver::new(
        left,
        DriverConfig {
            max_operation_bytes: 2048,
            ..config(WriteMode::Vectored)
        },
    )
    .unwrap();
    driver
        .enqueue(SendRequest {
            correlation: 1,
            deadline: RuntimeInstant::MAX,
            plan,
        })
        .unwrap();
    assert!(matches!(
        driver.poll_event(
            &mut Context::from_waker(Waker::noop()),
            RuntimeInstant::ZERO
        ),
        Poll::Ready(Some(DriverEvent::WriteAdmitted { correlation: 1 }))
    ));
    assert!(
        driver
            .poll_event(
                &mut Context::from_waker(Waker::noop()),
                RuntimeInstant::ZERO
            )
            .is_pending()
    );
    drop(driver);
    drop(payloads);
    assert_eq!(dropped.load(Ordering::SeqCst), 0);
    drop(right);
    drop(network);
    runtime.run_until_stalled().unwrap();
    runtime.finish().unwrap();
    assert_eq!(dropped.load(Ordering::SeqCst), 1);
}

#[test]
fn generic_connection_accepts_six_requests_and_rejects_seventh_without_admission() {
    let network = memory();
    let (left, _right) = network.connected_pair().unwrap();
    let mut driver = ConnectionDriver::new(
        left,
        DriverConfig {
            max_inflight_requests: 6,
            ..config(WriteMode::Vectored)
        },
    )
    .unwrap();
    for correlation in 0..6 {
        driver
            .enqueue(SendRequest {
                correlation,
                deadline: RuntimeInstant::MAX,
                plan: OwnedSendPlan::from_frame(response(correlation, b"request"), 128).unwrap(),
            })
            .unwrap();
    }
    let rejected = driver
        .enqueue(SendRequest {
            correlation: 6,
            deadline: RuntimeInstant::MAX,
            plan: OwnedSendPlan::from_frame(response(6, b"request"), 128).unwrap(),
        })
        .unwrap_err();
    assert_eq!(
        rejected.error,
        TransportError::ResourceExhausted {
            resource: "inflight requests",
            limit: 6
        }
    );
    assert_eq!(rejected.request.correlation, 6);
    assert_eq!(driver.pending_requests(), 6);
    assert_eq!(network.status().inflight_operations, 0);
    assert_eq!(network.status().pending_writes, 0);
    drop(driver);
    assert_eq!(network.status().inflight_operations, 0);
    for maximum in [0, usize::MAX] {
        let (left, _right) = network.connected_pair().unwrap();
        let result = ConnectionDriver::new(
            left,
            DriverConfig {
                max_inflight_requests: maximum,
                ..config(WriteMode::Vectored)
            },
        );
        assert!(matches!(
            result,
            Err(TransportError::InvalidConfig(_)) | Err(TransportError::AllocationFailed)
        ));
    }
}
