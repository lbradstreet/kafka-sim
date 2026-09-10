use kr_kafka_broker_model::{BrokerAction, BrokerConfig, BrokerEndpoint, BrokerModel, FaultPlan};
use kr_kafka_client::control::Negotiation;
use kr_kafka_producer::{
    actor::{ActorConfig, ProducerActor},
    client::ClientClock,
    config::{Compression, ProducerConfig},
    connector::{ConnectError, ConnectTarget, Connected, Connector},
    control::{ControlCodec, Probe},
    credit::Resource,
    engine::ProducerEngine,
    transport::{
        ConnectionDriver, DriverEvent, OwnedSendPlan, RetireReason, SendRequest, WriteMode,
    },
    types::*,
};
use kr_runtime::{RuntimeConfig, RuntimeDuration, RuntimeHandle, RuntimeInstant, SimRuntime};
use kr_runtime_io::network::{
    ByteStreamSubmit, ByteStreamVectoredSubmit, ColdStream, LinkConfig, NetworkConfig,
    NetworkError, NodeId, ReadRequest, SimNetwork, WriteRequest,
};
use std::{
    cell::RefCell,
    collections::VecDeque,
    future::{Future, poll_fn},
    pin::Pin,
    rc::Rc,
    task::Poll,
};

struct ModelConnector<S> {
    handle: RuntimeHandle,
    pair: Rc<dyn Fn() -> Result<(S, S), NetworkError>>,
    model: Rc<RefCell<BrokerModel>>,
    faults: Rc<RefCell<VecDeque<FaultPlan>>>,
    config: ProducerConfig,
}

fn memory_connector(handle: RuntimeHandle) -> ModelConnector<kr_runtime_io::network::MemoryStream> {
    let network = kr_runtime_io::network::MemoryNetwork::new(Default::default()).unwrap();
    ModelConnector {
        handle,
        pair: Rc::new(move || network.connected_pair()),
        model: model(13),
        faults: Default::default(),
        config: config(Compression::None),
    }
}

#[test]
fn construction_rejects_mismatched_runtime_clock_before_connecting() {
    let sim = SimRuntime::new(Default::default());
    let host = kr_runtime::HostRuntime::default();
    let other_host = kr_runtime::HostRuntime::default();
    for (handle, clock) in [
        (
            RuntimeHandle::Sim(sim.handle()),
            ClientClock::Host(host.control()),
        ),
        (RuntimeHandle::Host(host.handle()), ClientClock::Simulation),
        (
            RuntimeHandle::Host(host.handle()),
            ClientClock::Host(other_host.control()),
        ),
    ] {
        let connector = memory_connector(handle.clone());
        let engine = ProducerEngine::new(config(Compression::None), None).unwrap();
        let credits = engine.credits();
        assert!(matches!(
            ProducerActor::new(handle, engine, connector, clock, ActorConfig::default()),
            Err(kr_kafka_producer::actor::ActorError::InvalidConfig)
        ));
        assert!(credits.is_empty());
    }
}

#[test]
fn simulation_ingress_rejects_another_runtime_on_the_same_owner_thread() {
    use kr_kafka_producer::client::ClientError;
    let mut owner = SimRuntime::new(RuntimeConfig {
        start_time: RuntimeInstant::from_nanos(100),
        ..Default::default()
    });
    let mut other = SimRuntime::new(RuntimeConfig {
        start_time: RuntimeInstant::from_nanos(900),
        ..Default::default()
    });
    let handle = RuntimeHandle::Sim(owner.handle());
    let engine = ProducerEngine::new(config(Compression::None), None).unwrap();
    let (client, actor) = ProducerActor::new(
        handle.clone(),
        engine,
        memory_connector(handle),
        ClientClock::Simulation,
        ActorConfig::default(),
    )
    .unwrap();
    assert_eq!(
        other
            .block_on(async { client.open_topic("events") })
            .unwrap(),
        Err(ClientError::ClockUnavailable)
    );
    assert_eq!(client.status().unwrap().open_topics, 0);
    assert!(
        owner
            .block_on(async { client.open_topic("events") })
            .unwrap()
            .is_ok()
    );
    assert_eq!(client.status().unwrap().open_topics, 1);
    drop(actor);
    let mut events = [Event::Fatal { code: 0 }; 8];
    client.poll_events(&mut events);
    assert!(client.credits().is_empty());
    owner.finish().unwrap();
    other.finish().unwrap();
}
impl<S: ByteStreamVectoredSubmit> Connector for ModelConnector<S> {
    type Stream = S;
    type ConnectFuture = Pin<Box<dyn Future<Output = Result<Connected<S>, ConnectError>>>>;
    fn connect(&mut self, target: ConnectTarget) -> Self::ConnectFuture {
        let pair = self.pair.clone();
        let handle = self.handle.clone();
        let model = self.model.clone();
        let faults = self.faults.clone();
        let config = self.config.clone();
        Box::pin(async move {
            let (left, right) = pair().map_err(ConnectError::Network)?;
            let broker = target.broker_id.unwrap_or(1);
            let server = serve(right, model, faults, broker);
            // One bounded modeled connection task, never one task per record.
            handle
                .spawn(server)
                .map_err(|_| ConnectError::ResourceExhausted)?;
            let codec = ControlCodec::from_config(&config).map_err(ConnectError::Protocol)?;
            let mut driver = ConnectionDriver::new(left, target.driver)
                .map_err(|_| ConnectError::InvalidConfiguration)?;
            let request = codec
                .api_versions_request(-1, Probe::V3)
                .map_err(ConnectError::Protocol)?;
            driver
                .enqueue(SendRequest {
                    correlation: -1,
                    deadline: target.deadline,
                    plan: OwnedSendPlan::from_frame(
                        request,
                        config.rx_bytes_per_connection as usize,
                    )
                    .unwrap(),
                })
                .unwrap();
            {
                let negotiated = poll_fn(|cx| match driver.poll_event(cx, handle.now()) {
                    Poll::Pending => Poll::Pending,
                    Poll::Ready(Some(DriverEvent::Frame { bytes, .. })) => Poll::Ready(Some(
                        codec.shared().parse_api_versions(bytes, -1, Probe::V3),
                    )),
                    Poll::Ready(Some(DriverEvent::Released)) | Poll::Ready(None) => {
                        Poll::Ready(None)
                    }
                    Poll::Ready(Some(_)) => {
                        cx.waker().wake_by_ref();
                        Poll::Pending
                    }
                })
                .await;
                match negotiated {
                    Some(Ok(Negotiation::Ready(capabilities))) => Ok(Connected {
                        driver,
                        capabilities,
                    }),
                    Some(result) => {
                        let error = result
                            .err()
                            .map_or(ConnectError::InvalidConfiguration, ConnectError::Protocol);
                        driver.retire(RetireReason::Requested);
                        poll_fn(|cx| {
                            loop {
                                match driver.poll_event(cx, handle.now()) {
                                    Poll::Pending => return Poll::Pending,
                                    Poll::Ready(None)
                                    | Poll::Ready(Some(DriverEvent::Released)) => {
                                        return Poll::Ready(());
                                    }
                                    Poll::Ready(Some(_)) => {}
                                }
                            }
                        })
                        .await;
                        Err(error)
                    }
                    None => Err(ConnectError::Timeout),
                }
            }
        })
    }
}
async fn serve<S: ByteStreamSubmit>(
    stream: S,
    model: Rc<RefCell<BrokerModel>>,
    faults: Rc<RefCell<VecDeque<FaultPlan>>>,
    broker: i32,
) {
    let stream = ColdStream::new(stream);
    loop {
        let mut bytes = Vec::new();
        let mut expected = 4;
        while bytes.len() < expected {
            let max_bytes = (expected - bytes.len()).min(1024 * 1024);
            let Ok(result) = stream
                .read(ReadRequest {
                    buffer: bytes,
                    max_bytes,
                })
                .await
            else {
                return;
            };
            bytes = result.buffer;
            if result.end_of_stream {
                let _ = stream.close().await;
                return;
            }
            if bytes.len() == 4 && expected == 4 {
                let payload = i32::from_be_bytes(bytes[..4].try_into().unwrap());
                assert!((4..=1024 * 1024).contains(&payload));
                expected = payload as usize + 4;
            }
        }
        let fault = if bytes[4..6] == [0, 0] {
            faults.borrow_mut().pop_front().unwrap_or_default()
        } else {
            FaultPlan::default()
        };
        let action = model
            .borrow_mut()
            .handle_frame(broker, &bytes, fault)
            .unwrap();
        match action {
            BrokerAction::Reply(response) => {
                let mut offset = 0;
                while offset < response.len() {
                    let Ok(result) = stream
                        .write(WriteRequest {
                            buffer: response[offset..].to_vec(),
                        })
                        .await
                    else {
                        return;
                    };
                    assert!(result.bytes_written > 0);
                    offset += result.bytes_written;
                }
            }
            BrokerAction::Disconnect { .. } => {
                let _ = stream.close().await;
                return;
            }
            BrokerAction::DropRequest | BrokerAction::DropResponse { .. } => {}
        }
    }
}
fn config(compression: Compression) -> ProducerConfig {
    ProducerConfig {
        compression,
        codec_contexts: 1,
        record_descriptors: 128,
        delivery_event_capacity: 128,
        release_event_capacity: 16,
        max_live_leases: 16,
        max_batches: 128,
        max_open_topics: 4,
        pending_records_per_topic: 128,
        brokers_max: 2,
        input_bytes: 1024 * 1024,
        compressed_bytes: 512 * 1024,
        batch_target_bytes: 256,
        batch_hard_bytes: 8192,
        request_target_bytes: 8192,
        request_hard_bytes: 16384,
        output_chunk_bytes: 4096,
        progressive_threshold: 64,
        mailbox_capacity: 4,
        max_submission_records: 128,
        max_completions_per_poll: 8,
        max_submissions_per_poll: 4,
        request_timeout: RuntimeDuration::from_nanos(50_000_000),
        delivery_timeout: RuntimeDuration::from_nanos(1_000_000_000),
        ..ProducerConfig::default()
    }
}
fn model(max_version: i16) -> Rc<RefCell<BrokerModel>> {
    let mut model = BrokerModel::new(BrokerConfig {
        produce_max_version: max_version,
        ..Default::default()
    })
    .unwrap();
    model
        .add_broker(BrokerEndpoint {
            id: 1,
            host: "model".into(),
            port: 9092,
        })
        .unwrap();
    model.create_topic("events", &[1, 1, 1]).unwrap();
    Rc::new(RefCell::new(model))
}

fn simulated_case(
    seed: u64,
    chunk: usize,
    mode: WriteMode,
    compression: Compression,
    faults: Vec<FaultPlan>,
    version: i16,
) -> (Vec<Event>, Rc<RefCell<BrokerModel>>) {
    simulated_case_with_budget(seed, chunk, mode, compression, faults, version, 8)
}
fn simulated_case_with_budget(
    seed: u64,
    chunk: usize,
    mode: WriteMode,
    compression: Compression,
    faults: Vec<FaultPlan>,
    version: i16,
    completions: u32,
) -> (Vec<Event>, Rc<RefCell<BrokerModel>>) {
    let mut runtime = SimRuntime::new(RuntimeConfig {
        seed,
        max_steps_per_run: 250_000,
        max_time: Some(RuntimeInstant::from_nanos(2_000_000_000)),
        ..Default::default()
    });
    let mut config = config(compression);
    config.max_completions_per_poll = completions;
    let network = SimNetwork::new(
        runtime.handle(),
        NetworkConfig {
            max_operation_bytes: 1024 * 1024,
            directional_buffer_bytes: 257,
            default_link: LinkConfig {
                max_chunk_bytes: chunk,
                ..Default::default()
            },
            ..Default::default()
        },
    )
    .unwrap();
    let model = model(version);
    let handle = RuntimeHandle::Sim(runtime.handle());
    let connector = ModelConnector {
        handle: handle.clone(),
        pair: Rc::new(move || network.connected_pair(NodeId(1), NodeId(2))),
        model: model.clone(),
        faults: Rc::new(RefCell::new(faults.into())),
        config: config.clone(),
    };
    let engine = ProducerEngine::new(config, None).unwrap();
    let credits = engine.credits();
    let (client, actor) = ProducerActor::new(
        handle.clone(),
        engine,
        connector,
        ClientClock::Simulation,
        ActorConfig {
            write_mode: mode,
            encode_bytes_per_poll: 128,
            ..Default::default()
        },
    )
    .unwrap();
    let topic = client.open_topic_at("events", handle.now()).unwrap();
    let records: Vec<_> = (0..32)
        .map(|index| RecordDescriptor {
            topic,
            partition_hint: Some(index % 3),
            lane_hint: None,
            key: Some(b""),
            value: Some(b"payload with repetitive repetitive content"),
            headers: &[],
            timestamp_ms: 1234,
            user_token: index as u64,
            delivery_timeout: None,
        })
        .collect();
    assert_eq!(client.submit_copy_at(handle.now(), &records).accepted, 32);
    let flush = client.flush_at(handle.now()).unwrap();
    client
        .close_at(handle.now(), RuntimeDuration::from_nanos(1_500_000_000))
        .unwrap();
    let actor = handle.spawn(actor).unwrap();
    let events = runtime
        .block_on(async {
            let mut events = Vec::new();
            loop {
                let next = poll_fn(|cx| client.poll_event(cx)).await.unwrap();
                let Some(event) = next else {
                    break;
                };
                events.push(event);
                if matches!(event, Event::Closed { .. }) {
                    break;
                }
            }
            let status = actor.await.unwrap().unwrap();
            assert!(status.closed);
            assert_eq!(status.accepted, 32);
            assert_eq!(status.terminal, 32);
            events
        })
        .unwrap();
    runtime.run_until_stalled().unwrap();
    let deliveries: Vec<_> = events
        .iter()
        .filter_map(|event| match event {
            Event::Delivery(d) => Some(d),
            _ => None,
        })
        .collect();
    assert_eq!(deliveries.len(), 32, "{events:?}");
    let mut tokens = deliveries.iter().map(|d| d.token.0).collect::<Vec<_>>();
    tokens.sort();
    assert_eq!(tokens, (1..=32).collect::<Vec<_>>());
    let flush_index = events
        .iter()
        .position(|e| *e == Event::FlushDone { token: flush })
        .unwrap();
    assert!(
        events[flush_index + 1..]
            .iter()
            .all(|event| !matches!(event, Event::Delivery(_)))
    );
    assert!(
        credits.is_empty(),
        "{:?}",
        credits
            .snapshot()
            .iter()
            .enumerate()
            .filter(|(_, p)| p.held != 0)
            .collect::<Vec<_>>()
    );
    assert_eq!(
        credits.snapshot()[Resource::CompressedBytes as usize].held,
        0
    );
    let measured = client.status().unwrap();
    assert_eq!(
        measured.copied_input_bytes,
        32 * b"payload with repetitive repetitive content".len() as u64
    );
    assert!(measured.telemetry.actor_polls > 0);
    assert_eq!(measured.telemetry.max_host_poll_nanos, 0);
    assert!(measured.telemetry.max_codec_quantum_bytes <= 128);
    assert!(!measured.telemetry.overflowed);
    if version == 13 {
        if chunk > 1 {
            assert!(
                measured.telemetry.actor_polls < 10_000,
                "idle retry waits must park: {:?}",
                measured.telemetry
            );
        }
        assert!(measured.telemetry.codec_input_bytes > 0);
        assert!(measured.telemetry.produce_wire_bytes_confirmed > 0);
        if mode == WriteMode::Vectored {
            assert_eq!(measured.telemetry.produce_staging_copy_bytes, 0);
        } else {
            assert!(
                measured.telemetry.produce_staging_copy_bytes
                    >= measured.telemetry.produce_wire_bytes_confirmed
            );
        }
    }
    (events, model)
}

#[test]
fn actor_negotiates_resolves_batches_flushes_and_drains_close() {
    for (index, chunk) in [1, 7, 4096].into_iter().enumerate() {
        for mode in [WriteMode::Staging, WriteMode::Vectored] {
            for compression in [Compression::None, Compression::Zstd { level: 1 }] {
                let (events, model) =
                    simulated_case(index as u64, chunk, mode, compression, vec![], 13);
                assert!(
                    events
                        .iter()
                        .filter_map(|e| match e {
                            Event::Delivery(d) => Some(d),
                            _ => None,
                        })
                        .all(|d| d.outcome.kind == DeliveryKind::Acked),
                    "{events:?}"
                );
                let broker = model.borrow();
                assert_eq!(
                    broker
                        .log()
                        .iter()
                        .map(|batch| batch.records.len())
                        .sum::<usize>(),
                    32
                );
                for record in broker.log().iter().flat_map(|b| &b.records) {
                    assert_eq!(record.key.as_deref(), Some(&b""[..]));
                    assert_eq!(
                        record.value.as_deref(),
                        Some(&b"payload with repetitive repetitive content"[..])
                    );
                }
            }
        }
    }
}

#[test]
fn broker_size_rejections_do_not_fail_the_actor_runtime() {
    use kr_kafka_protocol::errors::{MESSAGE_TOO_LARGE, RECORD_LIST_TOO_LARGE};
    for error in [MESSAGE_TOO_LARGE, RECORD_LIST_TOO_LARGE] {
        for seed in 0..8 {
            for mode in [WriteMode::Staging, WriteMode::Vectored] {
                let (events, model) = simulated_case(
                    seed,
                    if seed % 2 == 0 { 4096 } else { 7 },
                    mode,
                    Compression::None,
                    vec![
                        FaultPlan {
                            reject_before_commit: Some(error),
                            ..Default::default()
                        };
                        128
                    ],
                    13,
                );
                let context = format!("seed={seed} mode={mode:?} broker_error={error}");
                let deliveries = events
                    .iter()
                    .filter_map(|event| match event {
                        Event::Delivery(delivery) => Some(delivery),
                        _ => None,
                    })
                    .collect::<Vec<_>>();
                assert!(
                    deliveries.iter().any(|delivery| delivery.outcome
                        == DeliveryOutcome::not_written(FailureReason::CompressedTooLarge)),
                    "{context}"
                );
                assert!(
                    deliveries
                        .iter()
                        .all(|delivery| delivery.outcome.kind != DeliveryKind::Acked
                            && matches!(
                                delivery.outcome.reason,
                                FailureReason::CompressedTooLarge
                                    | FailureReason::SequenceUnresolved
                            )),
                    "{context}"
                );
                // A younger request already admitted before quarantine can lose
                // its response. A subsequent size rejection must not erase that
                // uncertainty; the existing identity policy then fences safely.
                for event in &events {
                    if let Event::Fatal { code } = event {
                        assert_eq!(*code, FailureReason::SequenceUnresolved as u32, "{context}");
                        assert!(
                            deliveries
                                .iter()
                                .any(|delivery| delivery.outcome.kind == DeliveryKind::Unknown),
                            "{context}"
                        );
                    }
                }
                assert!(model.borrow().log().is_empty(), "{context}");
            }
        }
    }
}

#[test]
fn strict_topic_identity_rejects_old_broker_without_produce() {
    let (events, model) = simulated_case(81, 7, WriteMode::Vectored, Compression::None, vec![], 9);
    assert!(model.borrow().log().is_empty());
    assert!(
        events
            .iter()
            .any(|event| matches!(event, Event::Fatal { .. }))
    );
    assert!(
        events
            .iter()
            .filter_map(|e| match e {
                Event::Delivery(d) => Some(d),
                _ => None,
            })
            .all(|d| d.outcome.kind == DeliveryKind::NotWritten)
    );
}

#[test]
fn lost_response_retries_the_same_identity_without_duplicate_append() {
    let fault = FaultPlan {
        drop_after_commit: true,
        ..Default::default()
    };
    let (events, model) = simulated_case(
        921,
        7,
        WriteMode::Vectored,
        Compression::Zstd { level: 1 },
        vec![fault],
        13,
    );
    assert!(
        events
            .iter()
            .filter_map(|e| match e {
                Event::Delivery(d) => Some(d),
                _ => None,
            })
            .all(|d| d.outcome.kind == DeliveryKind::Acked),
        "{events:?}"
    );
    assert_eq!(
        model
            .borrow()
            .log()
            .iter()
            .map(|batch| batch.records.len())
            .sum::<usize>(),
        32
    );
}

#[test]
fn unpolled_owner_abort_settles_every_accepted_record_and_late_native_release() {
    use kr_kafka_producer::input::LeasedRecordDescriptor;
    let runtime = SimRuntime::default();
    let handle = RuntimeHandle::Sim(runtime.handle());
    let network = SimNetwork::new(runtime.handle(), NetworkConfig::default()).unwrap();
    let config = config(Compression::None);
    let connector = ModelConnector {
        handle: handle.clone(),
        pair: Rc::new(move || network.connected_pair(NodeId(1), NodeId(2))),
        model: model(13),
        faults: Rc::new(RefCell::new(VecDeque::new())),
        config: config.clone(),
    };
    let engine = ProducerEngine::new(config, None).unwrap();
    let credits = engine.credits();
    let (client, actor) = ProducerActor::new(
        handle.clone(),
        engine,
        connector,
        ClientClock::Simulation,
        ActorConfig::default(),
    )
    .unwrap();
    let topic = client.open_topic_at("events", handle.now()).unwrap();
    let mut committed = client.acquire(64, 0).unwrap();
    committed.as_mut_slice()[..4].copy_from_slice(b"data");
    let lease = committed.commit(4).unwrap();
    let records = [LeasedRecordDescriptor {
        topic,
        partition_hint: Some(0),
        lane_hint: None,
        key: None,
        value: Some(0..4),
        headers: &[],
        timestamp_ms: 0,
        user_token: 81,
        delivery_timeout: None,
    }];
    assert_eq!(
        client
            .submit_leased_at(handle.now(), lease, &records)
            .accepted,
        1
    );
    let copy = [RecordDescriptor {
        topic,
        partition_hint: Some(1),
        lane_hint: None,
        key: None,
        value: Some(b"copy"),
        headers: &[],
        timestamp_ms: 0,
        user_token: 82,
        delivery_timeout: None,
    }];
    assert_eq!(client.submit_copy_at(handle.now(), &copy).accepted, 1);
    let flush = client.flush_at(handle.now()).unwrap();
    // This writable buffer outlives both the runtime owner and its abort cleanup.
    let writable = client.acquire(128, 0).unwrap();
    let late = writable.lease_id();
    drop(actor);
    let mut buffer = [Event::Fatal { code: 0 }; 32];
    let n = client.poll_events(&mut buffer);
    let events = &buffer[..n];
    assert_eq!(
        events
            .iter()
            .filter(|e| matches!(e, Event::Delivery(_)))
            .count(),
        2
    );
    assert!(events.contains(&Event::InputReleased { lease }));
    assert!(!events.iter().any(|e| matches!(e, Event::Closed { .. })));
    let fence = events
        .iter()
        .position(|e| *e == Event::FlushDone { token: flush })
        .unwrap();
    assert!(
        events[fence + 1..]
            .iter()
            .all(|e| !matches!(e, Event::Delivery(_)))
    );
    assert!(
        events
            .iter()
            .filter_map(|e| match e {
                Event::Delivery(d) => Some(d),
                _ => None,
            })
            .all(|d| d.outcome.kind == DeliveryKind::NotWritten)
    );
    assert_eq!(credits.snapshot()[Resource::InputBytes as usize].held, 128);
    drop(writable);
    let n = client.poll_events(&mut buffer);
    assert_eq!(&buffer[..n], &[Event::InputReleased { lease: late }]);
    assert!(credits.is_empty(), "{:?}", credits.snapshot());
}

#[test]
fn host_actor_accepts_foreign_thread_bulk_and_uses_the_same_broker_contract() {
    use kr_runtime::HostRuntime;
    use kr_runtime_io::network::{MemoryNetwork, MemoryNetworkConfig};
    let mut runtime = HostRuntime::default();
    let handle = RuntimeHandle::Host(runtime.handle());
    let config = config(Compression::Zstd { level: 1 });
    let network = MemoryNetwork::new(MemoryNetworkConfig {
        max_operation_bytes: 1024 * 1024,
        directional_buffer_bytes: 127,
        max_chunk_bytes: 7,
        ..Default::default()
    })
    .unwrap();
    let model = model(13);
    let connector = ModelConnector {
        handle: handle.clone(),
        pair: Rc::new(move || network.connected_pair()),
        model: model.clone(),
        faults: Rc::new(RefCell::new(VecDeque::new())),
        config: config.clone(),
    };
    let engine = ProducerEngine::new(config, None).unwrap();
    let credits = engine.credits();
    let (client, actor) = ProducerActor::new(
        handle.clone(),
        engine,
        connector,
        ClientClock::Host(runtime.control()),
        ActorConfig::default(),
    )
    .unwrap();
    let submitter = client.clone();
    std::thread::spawn(move || {
        let topic = submitter.open_topic("events").unwrap();
        let record = RecordDescriptor {
            topic,
            partition_hint: Some(0),
            lane_hint: None,
            key: Some(b"foreign"),
            value: Some(b"thread"),
            headers: &[],
            timestamp_ms: 987,
            user_token: 444,
            delivery_timeout: None,
        };
        assert_eq!(submitter.submit_copy(&[record; 12]).accepted, 12);
        submitter.flush().unwrap();
        submitter
            .close(RuntimeDuration::from_nanos(5_000_000_000))
            .unwrap();
    })
    .join()
    .unwrap();
    let actor = handle.spawn(actor).unwrap();
    let events = runtime
        .block_on(async {
            let mut events = Vec::new();
            while let Some(event) = poll_fn(|cx| client.poll_event(cx)).await.unwrap() {
                events.push(event);
                if matches!(event, Event::Closed { .. }) {
                    break;
                }
            }
            assert!(actor.await.unwrap().unwrap().closed);
            events
        })
        .unwrap();
    runtime.finish().unwrap();
    let deliveries: Vec<_> = events
        .iter()
        .filter_map(|event| match event {
            Event::Delivery(d) => Some(d),
            _ => None,
        })
        .collect();
    assert_eq!(deliveries.len(), 12);
    assert!(
        deliveries
            .iter()
            .all(|d| d.outcome.kind == DeliveryKind::Acked)
    );
    assert_eq!(
        model
            .borrow()
            .log()
            .iter()
            .map(|batch| batch.records.len())
            .sum::<usize>(),
        12
    );
    assert!(credits.is_empty());
}

#[test]
fn single_item_control_and_metadata_visits_complete_real_protocol_roundtrip() {
    let (events, model) =
        simulated_case_with_budget(91, 7, WriteMode::Vectored, Compression::None, vec![], 13, 1);
    assert_eq!(events.iter().filter(|event| matches!(event, Event::Delivery(delivery) if delivery.outcome.kind == DeliveryKind::Acked)).count(), 32);
    assert_eq!(
        model
            .borrow()
            .log()
            .iter()
            .map(|batch| batch.records.len())
            .sum::<usize>(),
        32
    );
}

#[test]
fn healthy_close_crosses_metadata_age_while_provider_retirement_is_pending() {
    use kr_runtime::SimDuration;
    use kr_runtime_io::network::{FaultOutcome, NetworkOperationKind, ScriptedFault};

    for mode in [WriteMode::Staging, WriteMode::Vectored] {
        let mut runtime = SimRuntime::new(RuntimeConfig {
            seed: 513,
            max_steps_per_run: 50_000,
            max_time: Some(RuntimeInstant::from_nanos(100_000_000)),
            ..Default::default()
        });
        let handle = RuntimeHandle::Sim(runtime.handle());
        let network = SimNetwork::new(
            runtime.handle(),
            NetworkConfig {
                max_operation_bytes: 1024 * 1024,
                ..Default::default()
            },
        )
        .unwrap();
        let mut config = config(Compression::None);
        config.metadata_max_age = RuntimeDuration::from_nanos(1_000_000);
        config.max_completions_per_poll = 1;
        let pair_network = network.clone();
        let broker = model(13);
        let connector = ModelConnector {
            handle: handle.clone(),
            pair: Rc::new(move || pair_network.connected_pair(NodeId(1), NodeId(2))),
            model: broker.clone(),
            faults: Default::default(),
            config: config.clone(),
        };
        let engine = ProducerEngine::new(config, None).unwrap();
        let credits = engine.credits();
        let (client, actor) = ProducerActor::new(
            handle.clone(),
            engine,
            connector,
            ClientClock::Simulation,
            ActorConfig {
                write_mode: mode,
                ..Default::default()
            },
        )
        .unwrap();
        let topic = client.open_topic_at("events", handle.now()).unwrap();
        let record = RecordDescriptor {
            topic,
            partition_hint: Some(0),
            lane_hint: None,
            key: Some(b"close"),
            value: Some(b"already acknowledged"),
            headers: &[],
            timestamp_ms: 123,
            user_token: 99,
            delivery_timeout: None,
        };
        assert_eq!(client.submit_copy_at(handle.now(), &[record]).accepted, 1);
        let flush = client.flush_at(handle.now()).unwrap();
        let mut join = handle.spawn(actor).unwrap();
        let events = runtime
            .block_on(async {
                let mut events = Vec::new();
                loop {
                    let event = poll_fn(|cx| client.poll_event(cx)).await.unwrap().unwrap();
                    events.push(event);
                    if event == (Event::FlushDone { token: flush }) {
                        break;
                    }
                }
                assert!(events.iter().any(|event| matches!(event,
                    Event::Delivery(delivery) if delivery.outcome.kind == DeliveryKind::Acked)));
                // The real simulator retains an admitted Close completion for
                // ten metadata periods. Effects may occur earlier; ownership
                // and the actor's join must wait for terminal publication.
                network
                    .push_fault(ScriptedFault {
                        operation: NetworkOperationKind::Close,
                        extra_latency: SimDuration::from_nanos(10_000_000),
                        max_bytes: None,
                        outcome: FaultOutcome::Continue,
                    })
                    .unwrap();
                let closing_at = handle.now();
                client
                    .close_at(closing_at, RuntimeDuration::from_nanos(50_000_000))
                    .unwrap();
                handle
                    .sleep(RuntimeDuration::from_nanos(2_000_000))
                    .await
                    .unwrap();
                assert!(network.status().inflight_operations > 0);
                assert!(
                    !credits.is_empty(),
                    "provider retirement still owns its reservation"
                );
                poll_fn(|cx| {
                    assert!(Pin::new(&mut join).poll(cx).is_pending());
                    Poll::Ready(())
                })
                .await;
                while let Some(event) = poll_fn(|cx| client.poll_event(cx)).await.unwrap() {
                    events.push(event);
                    if matches!(event, Event::Closed { .. }) {
                        break;
                    }
                }
                let status = join
                    .await
                    .unwrap()
                    .expect("healthy close must join successfully");
                assert!(status.closed && !status.failed);
                assert_eq!((status.accepted, status.terminal), (1, 1));
                assert!(handle.now().as_nanos() >= closing_at.as_nanos() + 10_000_000);
                events
            })
            .unwrap();
        runtime.finish().unwrap();
        assert_eq!(network.status().fault_hits, 1);
        assert_eq!(network.status().inflight_operations, 0);
        assert!(credits.is_empty(), "{:?}", credits.snapshot());
        assert_eq!(broker.borrow().log()[0].records.len(), 1);
        assert_eq!(
            events
                .iter()
                .filter(|e| matches!(e, Event::Delivery(_)))
                .count(),
            1
        );
        assert_eq!(
            events
                .iter()
                .filter(|e| matches!(e, Event::Closed { unresolved: 0 }))
                .count(),
            1
        );
        assert!(!events.iter().any(|e| matches!(e, Event::Fatal { .. })));
    }
}
