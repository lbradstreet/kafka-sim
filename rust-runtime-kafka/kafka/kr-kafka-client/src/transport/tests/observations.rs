use super::*;
use std::sync::Mutex;

#[derive(Clone, Debug, PartialEq, Eq)]
enum Row {
    Dispatch(i32, Vec<u8>),
    Written(i32),
    Finished(i32, bool, usize, CompletionCertainty, String),
}
#[derive(Default)]
struct Capture(Mutex<Vec<(u64, Row)>>);
impl RequestObserver for Capture {
    fn observe(&self, now: RuntimeInstant, event: RequestObservation<'_>) {
        let row = match event {
            RequestObservation::Dispatched { correlation, plan } => Row::Dispatch(
                correlation,
                plan.segments()
                    .iter()
                    .flat_map(|s| s.as_slice())
                    .copied()
                    .collect(),
            ),
            RequestObservation::WriteCompleted { correlation } => Row::Written(correlation),
            RequestObservation::Finished {
                correlation,
                dispatched,
                confirmed,
                certainty,
                result,
            } => Row::Finished(
                correlation,
                dispatched,
                confirmed,
                certainty,
                format!("{result:?}"),
            ),
        };
        self.0.lock().unwrap().push((now.as_nanos(), row));
    }
}

fn observed_roundtrip(
    observed: bool,
    mode: WriteMode,
) -> (
    kr_runtime::DeterminismCheckpoint,
    Vec<String>,
    Vec<(u64, Row)>,
) {
    let mut runtime = SimRuntime::default();
    let network = SimNetwork::new(
        runtime.handle(),
        NetworkConfig {
            max_operation_bytes: 128,
            directional_buffer_bytes: 17,
            default_link: LinkConfig {
                max_chunk_bytes: 3,
                latency: RuntimeDuration::from_nanos(7),
                ..LinkConfig::default()
            },
            ..NetworkConfig::default()
        },
    )
    .unwrap();
    let (left, right) = network.connected_pair(NodeId(1), NodeId(2)).unwrap();
    let chunks = [SharedBytes::from((0..23u8).collect::<Vec<_>>())];
    let requests: Vec<_> = (40..43)
        .map(|id| protocol_plan(id, &chunks).to_vec().unwrap())
        .collect();
    let server = runtime
        .handle()
        .spawn(server(
            right,
            requests.clone(),
            (40..43).map(|id| response(id, b"ok")).collect(),
        ))
        .unwrap();
    let capture = Arc::new(Capture::default());
    let mut driver = ConnectionDriver::new(left, config(mode)).unwrap();
    if observed {
        driver.set_request_observer(capture.clone()).unwrap();
    }
    for correlation in 40..43 {
        driver
            .enqueue(SendRequest {
                correlation,
                deadline: RuntimeInstant::MAX,
                plan: plan(correlation, &chunks),
            })
            .unwrap();
    }
    assert!(
        capture.0.lock().unwrap().is_empty(),
        "enqueue must remain cold"
    );
    let handle = runtime.handle();
    let events = runtime
        .block_on(async {
            let mut frames = 0;
            let mut events = Vec::new();
            loop {
                let (frame, released) = poll_fn(|cx| {
                    driver.poll_event(cx, handle.now()).map(|event| {
                        let event = event.expect("not released yet");
                        events.push(format!("{event:?}"));
                        (
                            matches!(event, DriverEvent::Frame { .. }),
                            matches!(event, DriverEvent::Released),
                        )
                    })
                })
                .await;
                if frame {
                    frames += 1;
                }
                if frames == 3 {
                    driver.retire(RetireReason::Requested);
                }
                if released {
                    break;
                }
            }
            server.await.unwrap();
            events
        })
        .unwrap();
    drop(driver);
    runtime.run_until_stalled().unwrap();
    assert_eq!(network.status().inflight_operations, 0);
    let snapshot = runtime.snapshot();
    let checkpoint = snapshot.determinism_checkpoint();
    let rows = capture.0.lock().unwrap().clone();
    if observed {
        for correlation in 40..43 {
            let dispatch: Vec<_> = rows
                .iter()
                .filter(|(_, r)| matches!(r,Row::Dispatch(id,_) if *id == correlation))
                .collect();
            let written: Vec<_> = rows
                .iter()
                .filter(|(_, r)| *r == Row::Written(correlation))
                .collect();
            let finished: Vec<_> = rows
                .iter()
                .filter(|(_, r)| matches!(r,Row::Finished(id,..) if *id == correlation))
                .collect();
            assert_eq!((dispatch.len(), written.len(), finished.len()), (1, 1, 1));
            assert_eq!(
                dispatch[0].1,
                Row::Dispatch(correlation, requests[(correlation - 40) as usize].clone())
            );
            assert!(dispatch[0].0 < written[0].0 && written[0].0 <= finished[0].0);
            assert!(
                matches!(&finished[0].1,Row::Finished(_,true,_,CompletionCertainty::Applied,result) if result == "Response")
            );
        }
    }
    (checkpoint, events, rows)
}

#[test]
fn observation_preserves_checkpoint_and_events_for_multiple_partial_requests() {
    for mode in [WriteMode::Staging, WriteMode::Vectored] {
        let (baseline, events, empty) = observed_roundtrip(false, mode);
        let (observed, observed_events, rows) = observed_roundtrip(true, mode);
        assert_eq!(baseline, observed);
        assert_eq!(events, observed_events);
        assert!(empty.is_empty());
        assert_eq!(rows.len(), 9);
    }
}

#[test]
fn failed_first_submission_is_an_attempt_and_queued_unsent_requests_are_distinct() {
    for mode in [WriteMode::Staging, WriteMode::Vectored] {
        for partial in [false, true] {
            let mut rt = SimRuntime::default();
            let network = simulated(&rt);
            let (left, right) = network.connected_pair(NodeId(1), NodeId(2)).unwrap();
            if partial {
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
                .push_fault(ScriptedFault::fail_before(NetworkOperationKind::Write, 99))
                .unwrap();
            let capture = Arc::new(Capture::default());
            let mut driver = ConnectionDriver::new(left, config(mode)).unwrap();
            driver.set_request_observer(capture.clone()).unwrap();
            for correlation in 1..=2 {
                driver
                    .enqueue(SendRequest {
                        correlation,
                        deadline: RuntimeInstant::MAX,
                        plan: plan(correlation, &[SharedBytes::from(vec![42; 32])]),
                    })
                    .unwrap();
            }
            rt.block_on(async {
                while !matches!(
                    next(&mut driver, RuntimeInstant::from_nanos(13)).await,
                    Event::Released
                ) {}
            })
            .unwrap();
            let rows = capture.0.lock().unwrap();
            assert_eq!(rows.len(), 3);
            assert!(rows.iter().all(|(now, _)| *now == 13));
            assert!(matches!(rows[0].1, Row::Dispatch(1, _)));
            assert!(
                matches!(rows[1].1,Row::Finished(1,true,bytes,_,_) if bytes == if partial {2} else {0})
            );
            assert!(matches!(
                rows[2].1,
                Row::Finished(2, false, 0, CompletionCertainty::NotApplied, _)
            ));
            assert!(rows.iter().all(|(_, r)| !matches!(r, Row::Written(_))));
            drop(rows);
            drop((driver, right));
            rt.run_until_stalled().unwrap();
            assert_eq!(network.status().inflight_operations, 0);
        }
    }
}
