use super::*;
use crate::{connector::Connected, control::Probe, topic::PartitionMetadata};
use kr_kafka_broker_model::{
    BrokerAction, BrokerConfig, BrokerEndpoint as ModelEndpoint, BrokerModel, FaultPlan,
};
use kr_kafka_client::control::Negotiation;
use kr_kafka_protocol::{self as wire, Request};
use kr_runtime_io::network::{
    ByteStreamSubmit, MemoryNetwork, MemoryNetworkConfig, MemoryStream, ReadRequest, WriteRequest,
};
use std::{cell::RefCell, rc::Rc, task::Waker};

#[derive(Default)]
struct Script {
    gate: bool,
    drop_metadata: usize,
    corrupt_metadata: bool,
    starts: usize,
    frames: Vec<Vec<u8>>,
}
struct Server {
    stream: MemoryStream,
    read: Option<Pin<Box<<MemoryStream as ByteStreamSubmit>::ReadResponse>>>,
    write: Option<Pin<Box<<MemoryStream as ByteStreamSubmit>::WriteResponse>>>,
    incoming: Vec<u8>,
    outgoing: Vec<u8>,
    closed: bool,
}
struct ModelConnector {
    network: MemoryNetwork,
    model: Rc<RefCell<BrokerModel>>,
    script: Rc<RefCell<Script>>,
    servers: Rc<RefCell<Vec<Server>>>,
}
impl ModelConnector {
    fn new(version: i16) -> (Self, TopicId) {
        let mut model = BrokerModel::new(BrokerConfig {
            produce_max_version: version,
            ..Default::default()
        })
        .unwrap();
        model
            .add_broker(ModelEndpoint {
                id: 0,
                host: "localhost".into(),
                port: 9092,
            })
            .unwrap();
        let topic = TopicId(model.create_topic("events", &[0, 0]).unwrap());
        (
            Self {
                network: MemoryNetwork::new(MemoryNetworkConfig {
                    max_operation_bytes: 1024 * 1024,
                    ..Default::default()
                })
                .unwrap(),
                model: Rc::new(RefCell::new(model)),
                script: Rc::new(RefCell::new(Script {
                    gate: true,
                    ..Default::default()
                })),
                servers: Rc::new(RefCell::new(Vec::new())),
            },
            topic,
        )
    }
    fn pump(&self) {
        let mut cx = Context::from_waker(Waker::noop());
        for server in &mut *self.servers.borrow_mut() {
            if server.closed {
                continue;
            }
            for _ in 0..32 {
                let mut progressed = false;
                if let Some(read) = &mut server.read
                    && let Poll::Ready(result) = read.as_mut().poll(&mut cx)
                {
                    server.read = None;
                    progressed = true;
                    match result {
                        Ok(result) if !result.end_of_stream => {
                            server.incoming.extend(result.buffer)
                        }
                        _ => {
                            server.closed = true;
                            break;
                        }
                    }
                }
                if let Some(write) = &mut server.write
                    && let Poll::Ready(result) = write.as_mut().poll(&mut cx)
                {
                    server.write = None;
                    progressed = true;
                    match result {
                        Ok(result) => {
                            let mut bytes = result.buffer;
                            let len = bytes.len();
                            bytes.copy_within(result.bytes_written.., 0);
                            bytes.truncate(len - result.bytes_written);
                            server.outgoing = bytes;
                        }
                        Err(_) => {
                            server.closed = true;
                            break;
                        }
                    }
                }
                if server.incoming.len() >= 4 {
                    let len = i32::from_be_bytes(server.incoming[..4].try_into().unwrap());
                    assert!((0..65536).contains(&len));
                    let len = len as usize + 4;
                    if server.incoming.len() >= len {
                        let frame: Vec<_> = server.incoming.drain(..len).collect();
                        let api = i16::from_be_bytes(frame[4..6].try_into().unwrap());
                        let mut script = self.script.borrow_mut();
                        script.frames.push(frame.clone());
                        let drop = api == 3 && script.drop_metadata > 0;
                        if drop {
                            script.drop_metadata -= 1;
                        }
                        let action = self
                            .model
                            .borrow_mut()
                            .handle_frame(
                                0,
                                &frame,
                                FaultPlan {
                                    drop_after_parse: drop,
                                    ..Default::default()
                                },
                            )
                            .unwrap();
                        if let BrokerAction::Reply(mut bytes) = action {
                            if api == 3 && script.corrupt_metadata {
                                bytes[7] ^= 1;
                            }
                            assert!(server.outgoing.is_empty() && server.write.is_none());
                            server.outgoing = bytes;
                        }
                        progressed = true;
                    }
                }
                if server.write.is_none() && !server.outgoing.is_empty() {
                    server.write = Some(Box::pin(server.stream.submit_write(WriteRequest {
                        buffer: std::mem::take(&mut server.outgoing),
                    })));
                    progressed = true;
                }
                if server.read.is_none() {
                    server.read = Some(Box::pin(server.stream.submit_read(ReadRequest {
                        buffer: Vec::new(),
                        max_bytes: 4096,
                    })));
                    progressed = true;
                }
                if !progressed {
                    break;
                }
            }
        }
    }
    fn metadata_requests(&self) -> usize {
        self.script
            .borrow()
            .frames
            .iter()
            .filter(|frame| frame[4..6] == 3i16.to_be_bytes())
            .count()
    }
}
struct SetupFuture {
    network: MemoryNetwork,
    script: Rc<RefCell<Script>>,
    servers: Rc<RefCell<Vec<Server>>>,
    target: Option<ConnectTarget>,
    driver: Option<ConnectionDriver<MemoryStream>>,
    codec: ControlCodec,
    done: bool,
}
impl Unpin for SetupFuture {}
impl Future for SetupFuture {
    type Output = std::result::Result<Connected<MemoryStream>, ConnectError>;
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        assert!(!self.done);
        if let Some(target) = self.target.take() {
            self.script.borrow_mut().starts += 1;
            let (left, right) = self.network.connected_pair().unwrap();
            self.servers.borrow_mut().push(Server {
                stream: right,
                read: None,
                write: None,
                incoming: Vec::new(),
                outgoing: Vec::new(),
                closed: false,
            });
            let mut driver = ConnectionDriver::new(left, target.driver).unwrap();
            let frame = self.codec.api_versions_request(-1, Probe::V3).unwrap();
            let mut plan = OwnedSendPlan::from_frame(frame, 65536).unwrap();
            if let Some(guard) = target.lifetime_guard {
                plan.retain_metadata_guard(guard);
            }
            driver
                .enqueue(SendRequest {
                    correlation: -1,
                    deadline: RuntimeInstant::from_nanos(1_000_000),
                    plan,
                })
                .unwrap();
            self.driver = Some(driver);
        }
        if !self.script.borrow().gate {
            return Poll::Pending;
        }
        // Separate immutable codec and mutable driver fields explicitly.
        let this = &mut *self;
        let driver = this.driver.as_mut().unwrap();
        let response = match driver.poll_event(cx, RuntimeInstant::ZERO) {
            Poll::Ready(Some(DriverEvent::Frame { bytes, .. })) => {
                this.codec.shared().parse_api_versions(bytes, -1, Probe::V3)
            }
            Poll::Ready(Some(DriverEvent::Retiring { reason })) => {
                panic!("model setup retired: {reason:?}")
            }
            _ => {
                cx.waker().wake_by_ref();
                return Poll::Pending;
            }
        };
        this.done = true;
        match response {
            Ok(Negotiation::Ready(capabilities)) => Poll::Ready(Ok(Connected {
                driver: this.driver.take().unwrap(),
                capabilities,
            })),
            Ok(_) => Poll::Ready(Err(ConnectError::InvalidConfiguration)),
            Err(error) => Poll::Ready(Err(ConnectError::Protocol(error))),
        }
    }
}
impl Connector for ModelConnector {
    type Stream = MemoryStream;
    type ConnectFuture = SetupFuture;
    fn connect(&mut self, target: ConnectTarget) -> Self::ConnectFuture {
        SetupFuture {
            network: self.network.clone(),
            script: self.script.clone(),
            servers: self.servers.clone(),
            target: Some(target),
            driver: None,
            codec: ControlCodec::new("setup".into(), false, Default::default()).unwrap(),
            done: false,
        }
    }
}

fn settings() -> ProducerConfig {
    ProducerConfig {
        request_timeout: RuntimeDuration::from_nanos(5),
        delivery_timeout: RuntimeDuration::from_nanos(100),
        retry_backoff_min: RuntimeDuration::from_nanos(1),
        retry_backoff_max: RuntimeDuration::from_nanos(4),
        max_attempts: 3,
        ..Default::default()
    }
}
fn fixture(
    version: i16,
) -> (
    ControlPlane<ModelConnector>,
    ModelConnector,
    TopicCache,
    SharedCredits,
    TopicHandle,
    TopicId,
) {
    let config = settings();
    let mut limits = [0; Resource::COUNT];
    limits[Resource::ControlReserve as usize] = config.control_reserve_bytes;
    let credits = SharedCredits::new(limits, 1).unwrap();
    let plane = ControlPlane::new(&config, credits.clone(), DriverConfig::default()).unwrap();
    let (connector, id) = ModelConnector::new(version);
    let mut topics = TopicCache::new(
        1024,
        1024,
        RuntimeDuration::from_nanos(100),
        RuntimeDuration::from_nanos(1000),
    )
    .unwrap();
    let handle = topics.open("events", RuntimeInstant::ZERO).unwrap();
    (plane, connector, topics, credits, handle, id)
}
fn step(
    plane: &mut ControlPlane<ModelConnector>,
    connector: &mut ModelConnector,
    topics: &TopicCache,
    now: u64,
) -> Option<ControlEvent> {
    let now = RuntimeInstant::from_nanos(now);
    plane
        .prepare(now, connector, topics, WorkBudget::default())
        .unwrap();
    let event = plane.poll_event(
        &mut Context::from_waker(Waker::noop()),
        now,
        connector,
        topics,
    );
    connector.pump();
    match event {
        Poll::Ready(ControlEvent::Progress) | Poll::Pending => None,
        Poll::Ready(event) => Some(event),
    }
}
fn event(
    plane: &mut ControlPlane<ModelConnector>,
    connector: &mut ModelConnector,
    topics: &TopicCache,
    now: u64,
) -> ControlEvent {
    for _ in 0..256 {
        if let Some(event) = step(plane, connector, topics, now) {
            return event;
        }
    }
    panic!(
        "control stalled at {now}; deadline={:?}, frames={}, net={:?}, servers={:?}, setupdriver={:?}",
        plane.next_deadline(),
        connector.script.borrow().frames.len(),
        connector.network.status(),
        connector
            .servers
            .borrow()
            .iter()
            .map(|s| (
                s.closed,
                s.incoming.len(),
                s.outgoing.len(),
                s.read.is_some(),
                s.write.is_some()
            ))
            .collect::<Vec<_>>(),
        plane
            .connecting
            .as_ref()
            .and_then(|s| s.future.driver.as_ref())
            .map(|d| (d.pending_requests(), d.is_retiring(), d.next_deadline()))
    );
}
fn stop(
    plane: &mut ControlPlane<ModelConnector>,
    connector: &mut ModelConnector,
    topics: &TopicCache,
) {
    plane.stop();
    assert!(matches!(
        event(plane, connector, topics, 0),
        ControlEvent::Released
    ));
    assert_eq!(plane.obligations(), 0);
    connector.pump();
    assert_eq!(connector.network.status().inflight_operations, 0);
}

#[test]
fn control_frames_are_fifo_and_metadata_switches_from_name_to_immutable_id() {
    let (mut plane, mut connector, mut topics, credits, handle, id) = fixture(13);
    plane.queue_metadata(vec![handle, handle]).unwrap();
    plane.queue_identity(None).unwrap();
    let ControlEvent::Metadata { handles, update } = event(&mut plane, &mut connector, &topics, 0)
    else {
        panic!("metadata must lead FIFO");
    };
    assert_eq!(handles, [handle]);
    assert_eq!(update.topics[0].id, id);
    let partitions: Vec<_> = update.topics[0]
        .partitions
        .iter()
        .map(|p| p.metadata)
        .collect();
    topics
        .apply(handle, id, &partitions, RuntimeInstant::ZERO)
        .unwrap();
    let ControlEvent::Identity(first) = event(&mut plane, &mut connector, &topics, 0) else {
        panic!("identity must follow metadata");
    };
    plane.queue_metadata(vec![handle]).unwrap();
    assert!(matches!(
        event(&mut plane, &mut connector, &topics, 0),
        ControlEvent::Metadata { .. }
    ));
    let script = connector.script.borrow();
    let frames: Vec<_> = script
        .frames
        .iter()
        .filter(|f| f[4..6] == 3i16.to_be_bytes())
        .collect();
    for (index, frame) in frames.iter().enumerate() {
        let request = wire::frame::decode_request(frame, Default::default()).unwrap();
        let Request::MetadataRequest(wire::metadata_request::View::V12(request)) = request.body
        else {
            unreachable!()
        };
        let topic = request.topics.unwrap().iter().next().unwrap().unwrap();
        assert!(!request.allow_auto_topic_creation);
        assert_eq!(topic.name, (index == 0).then_some("events"));
        assert_eq!(topic.topic_id, if index == 0 { [0; 16] } else { id.0 });
    }
    drop(script);
    plane.queue_identity(Some(first)).unwrap();
    let ControlEvent::Identity(second) = event(&mut plane, &mut connector, &topics, 0) else {
        unreachable!()
    };
    assert_ne!(first.producer_id, second.producer_id);
    assert_eq!(second.epoch, 0);
    // A successful response with throttle zero must not leave an expired timer
    // on the idle connection: that would pin an actor's virtual time at zero.
    assert_eq!(plane.next_deadline(), None);
    stop(&mut plane, &mut connector, &topics);
    assert!(credits.is_empty());
}

#[test]
fn reserve_leaves_two_data_setup_slots_and_unpolled_setup_is_cold() {
    let (mut plane, mut connector, topics, credits, handle, _) = fixture(13);
    let expected = settings().control_reserve_bytes - DATA_SETUP_RESERVED_BYTES;
    assert_eq!(
        credits.snapshot()[Resource::ControlReserve as usize].held,
        expected
    );
    let data = credits
        .reserve(&[Claim {
            resource: Resource::ControlReserve,
            amount: DATA_SETUP_RESERVED_BYTES,
            lane: 0,
        }])
        .unwrap();
    assert!(
        credits
            .reserve(&[Claim {
                resource: Resource::ControlReserve,
                amount: 1,
                lane: 0
            }])
            .is_err()
    );
    plane.queue_metadata(vec![handle]).unwrap();
    plane
        .prepare(
            RuntimeInstant::ZERO,
            &mut connector,
            &topics,
            WorkBudget::default(),
        )
        .unwrap();
    assert_eq!(connector.script.borrow().starts, 0);
    stop(&mut plane, &mut connector, &topics);
    assert_eq!(connector.script.borrow().starts, 0);
    assert_eq!(
        credits.snapshot()[Resource::ControlReserve as usize].held,
        DATA_SETUP_RESERVED_BYTES
    );
    drop(data);
    assert!(credits.is_empty());
}

#[test]
fn admitted_setup_survives_stop_and_its_late_driver_is_closed_before_credit_release() {
    let (mut plane, mut connector, topics, credits, _, _) = fixture(13);
    connector.script.borrow_mut().gate = false;
    plane.queue_identity(None).unwrap();
    assert!(step(&mut plane, &mut connector, &topics, 0).is_none());
    assert_eq!(connector.script.borrow().starts, 1);
    plane.stop();
    for _ in 0..4 {
        assert!(step(&mut plane, &mut connector, &topics, 100).is_none());
    }
    assert!(!credits.is_empty());
    assert!(plane.obligations() > 0);
    connector.script.borrow_mut().gate = true;
    assert!(matches!(
        event(&mut plane, &mut connector, &topics, 100),
        ControlEvent::Released
    ));
    assert!(credits.is_empty());
    connector.pump();
    assert_eq!(connector.network.status().inflight_operations, 0);
    assert!(
        connector
            .script
            .borrow()
            .frames
            .iter()
            .all(|f| f[4..6] == 18i16.to_be_bytes())
    );
}

#[test]
fn setup_deadline_discards_cold_work_but_drains_admitted_work_before_retry() {
    for admitted in [false, true] {
        let (mut plane, mut connector, topics, credits, handle, _) = fixture(13);
        connector.script.borrow_mut().gate = false;
        plane.queue_metadata(vec![handle]).unwrap();
        plane
            .prepare(
                RuntimeInstant::ZERO,
                &mut connector,
                &topics,
                WorkBudget::default(),
            )
            .unwrap();
        if admitted {
            assert!(step(&mut plane, &mut connector, &topics, 0).is_none());
        }
        assert!(step(&mut plane, &mut connector, &topics, 5).is_none());
        assert_eq!(connector.script.borrow().starts, usize::from(admitted));
        assert_eq!(connector.metadata_requests(), 0);
        if admitted {
            // The old timeout cannot keep waking the actor while it awaits a
            // real setup completion; that completion must be closed first.
            assert_eq!(plane.next_deadline(), None);
            assert!(plane.connecting.is_some());
            connector.script.borrow_mut().gate = true;
            for _ in 0..64 {
                assert!(step(&mut plane, &mut connector, &topics, 6).is_none());
            }
            assert_eq!(connector.script.borrow().starts, 1);
            assert_eq!(connector.metadata_requests(), 0);
            assert_eq!(plane.next_deadline(), Some(RuntimeInstant::from_nanos(7)));
        } else {
            assert!(plane.connecting.is_none());
            assert_eq!(plane.next_deadline(), Some(RuntimeInstant::from_nanos(6)));
            connector.script.borrow_mut().gate = true;
        }
        assert!(matches!(
            event(
                &mut plane,
                &mut connector,
                &topics,
                if admitted { 7 } else { 6 }
            ),
            ControlEvent::Metadata { .. }
        ));
        assert_eq!(connector.script.borrow().starts, 1 + usize::from(admitted));
        assert_eq!(connector.metadata_requests(), 1);
        stop(&mut plane, &mut connector, &topics);
        assert!(credits.is_empty());
    }
}

#[test]
fn dropped_metadata_response_retires_then_retries_the_same_bound_id_after_backoff() {
    let (mut plane, mut connector, mut topics, credits, handle, id) = fixture(13);
    topics
        .apply(
            handle,
            id,
            &[PartitionMetadata {
                leader: 0,
                leader_epoch: 0,
            }; 2],
            RuntimeInstant::ZERO,
        )
        .unwrap();
    connector.script.borrow_mut().drop_metadata = 1;
    plane.queue_metadata(vec![handle]).unwrap();
    for _ in 0..32 {
        assert!(step(&mut plane, &mut connector, &topics, 0).is_none());
    }
    assert_eq!(connector.metadata_requests(), 1);
    assert_eq!(plane.next_deadline(), Some(RuntimeInstant::from_nanos(5)));
    for _ in 0..32 {
        assert!(step(&mut plane, &mut connector, &topics, 5).is_none());
    }
    assert_eq!(connector.metadata_requests(), 1);
    assert_eq!(plane.next_deadline(), Some(RuntimeInstant::from_nanos(6)));
    assert!(matches!(
        event(&mut plane, &mut connector, &topics, 6),
        ControlEvent::Metadata { .. }
    ));
    assert_eq!(connector.metadata_requests(), 2);
    assert_eq!(connector.script.borrow().starts, 2);
    for frame in connector
        .script
        .borrow()
        .frames
        .iter()
        .filter(|f| f[4..6] == 3i16.to_be_bytes())
    {
        let Request::MetadataRequest(wire::metadata_request::View::V12(request)) =
            wire::frame::decode_request(frame, Default::default())
                .unwrap()
                .body
        else {
            unreachable!()
        };
        let topic = request.topics.unwrap().iter().next().unwrap().unwrap();
        assert_eq!(topic.topic_id, id.0);
        assert_eq!(topic.name, None);
    }
    stop(&mut plane, &mut connector, &topics);
    assert!(credits.is_empty());
}

#[test]
fn older_broker_and_corrupted_response_fail_closed_without_metadata_mutation() {
    for version in [9, 13] {
        let (mut plane, mut connector, topics, credits, handle, _) = fixture(version);
        connector.script.borrow_mut().corrupt_metadata = version == 13;
        plane.queue_metadata(vec![handle]).unwrap();
        let before = topics.get(handle).unwrap().clone();
        assert!(matches!(
            event(&mut plane, &mut connector, &topics, 0),
            ControlEvent::Fatal(FailureReason::ProtocolViolation)
        ));
        assert_eq!(topics.get(handle).unwrap(), &before);
        assert!(matches!(
            event(&mut plane, &mut connector, &topics, 0),
            ControlEvent::Released
        ));
        assert!(credits.is_empty());
        connector.pump();
        assert_eq!(connector.network.status().inflight_operations, 0);
    }
}

#[test]
fn expired_resolution_does_not_create_io_and_queue_bounds_are_explicit() {
    let (mut plane, mut connector, topics, credits, handle, _) = fixture(13);
    let excessive = Vec::with_capacity(1025);
    assert_eq!(
        plane.queue_metadata(excessive),
        Err(FailureReason::ResourceExhausted)
    );
    plane.queue_metadata(vec![handle]).unwrap();
    assert!(
        matches!(event(&mut plane, &mut connector, &topics, 100), ControlEvent::MetadataFailed { handles } if handles == [handle])
    );
    assert_eq!(connector.script.borrow().starts, 0);
    plane.queue_identity(None).unwrap();
    plane.queue_identity(None).unwrap();
    assert_eq!(
        plane.queue_identity(Some(ProducerIdentity {
            producer_id: 1,
            epoch: 0
        })),
        Err(FailureReason::ProtocolViolation)
    );
    stop(&mut plane, &mut connector, &topics);
    assert!(credits.is_empty());
}

#[test]
fn preparation_zero_one_quotas_visit_handles_and_selectors_before_frame_admission() {
    let (mut plane, mut connector, mut topics, credits, first, _) = fixture(13);
    plane.queue_identity(None).unwrap();
    assert!(matches!(
        event(&mut plane, &mut connector, &topics, 0),
        ControlEvent::Identity(_)
    ));
    let second = topics.open("second", RuntimeInstant::ZERO).unwrap();
    let third = topics.open("third", RuntimeInstant::ZERO).unwrap();
    let fourth = topics.open("fourth", RuntimeInstant::ZERO).unwrap();
    plane.request_handles = 3;
    plane
        .queue_metadata(vec![first, second, third, fourth])
        .unwrap();
    for quota in [
        WorkBudget { items: 0, bytes: 1 },
        WorkBudget { items: 1, bytes: 0 },
    ] {
        let progress = plane
            .prepare(RuntimeInstant::ZERO, &mut connector, &topics, quota)
            .unwrap();
        assert_eq!((progress.items, progress.bytes), (0, 0));
        assert!(progress.remaining_immediate);
        assert!(plane.current.is_none());
    }
    let quota = WorkBudget { items: 1, bytes: 1 };
    // Group activation, three raw handle visits, then three selector snapshots.
    // No frame can be admitted until the following, separately charged quantum.
    for step in 0..7 {
        let progress = plane
            .prepare(RuntimeInstant::ZERO, &mut connector, &topics, quota)
            .unwrap();
        assert_eq!(progress.items, 1, "step={step}");
        assert!(progress.remaining_immediate);
        let work = plane.current.as_ref().unwrap();
        let WorkKind::Metadata(handles) = &work.kind else {
            panic!("metadata work")
        };
        assert_eq!(handles.len(), step.min(3));
        assert_eq!(work.selectors.len(), step.saturating_sub(3));
        assert_eq!(plane.driver.as_ref().unwrap().pending_requests(), 0);
    }
    let progress = plane
        .prepare(RuntimeInstant::ZERO, &mut connector, &topics, quota)
        .unwrap();
    assert_eq!(progress.items, 1);
    assert!(!progress.remaining_immediate);
    assert_eq!(plane.driver.as_ref().unwrap().pending_requests(), 1);
    assert_eq!(
        plane.queued.len(),
        1,
        "remainder retained in its original FIFO group"
    );
    let ControlEvent::Metadata { handles, .. } = event(&mut plane, &mut connector, &topics, 0)
    else {
        panic!("metadata response")
    };
    assert_eq!(handles, [first, second, third]);
    let script = connector.script.borrow();
    let frame = script
        .frames
        .iter()
        .find(|frame| frame[4..6] == 3i16.to_be_bytes())
        .unwrap();
    let expected = plane
        .codec
        .metadata_request(
            1,
            &[
                MetadataSelector::Name("events"),
                MetadataSelector::Name("second"),
                MetadataSelector::Name("third"),
            ],
        )
        .unwrap();
    assert_eq!(
        *frame, expected,
        "incremental selection preserves exact canonical frame bytes"
    );
    drop(script);
    let ControlEvent::Metadata { handles, .. } = event(&mut plane, &mut connector, &topics, 0)
    else {
        panic!("remainder response")
    };
    assert_eq!(handles, [fourth]);
    assert_eq!(plane.next_deadline(), None);
    stop(&mut plane, &mut connector, &topics);
    assert!(credits.is_empty());
}

#[test]
fn expired_partial_preparation_reports_every_selected_handle_without_cold_io() {
    let (mut plane, mut connector, mut topics, credits, first, _) = fixture(13);
    let second = topics.open("second", RuntimeInstant::ZERO).unwrap();
    let third = topics.open("third", RuntimeInstant::ZERO).unwrap();
    plane.queue_metadata(vec![first, second, third]).unwrap();
    let now = RuntimeInstant::from_nanos(100);
    for _ in 0..4 {
        let progress = plane
            .prepare(
                now,
                &mut connector,
                &topics,
                WorkBudget { items: 1, bytes: 1 },
            )
            .unwrap();
        assert_eq!(progress.items, 1);
        assert!(progress.remaining_immediate);
        assert!(plane.pending.is_none());
    }
    let progress = plane
        .prepare(
            now,
            &mut connector,
            &topics,
            WorkBudget { items: 1, bytes: 1 },
        )
        .unwrap();
    assert_eq!(progress.items, 1);
    assert!(!progress.remaining_immediate);
    assert!(
        matches!(plane.pending.take(), Some(ControlEvent::MetadataFailed { handles }) if handles == [first, second, third])
    );
    assert_eq!(connector.script.borrow().starts, 0);
    assert!(plane.current.is_none());
    // A failed request releases duplicate membership, allowing a later refresh.
    plane.queue_metadata(vec![first]).unwrap();
    assert_eq!(plane.queued.len(), 1);
    stop(&mut plane, &mut connector, &topics);
    assert!(credits.is_empty());
}

#[test]
fn stop_releases_partial_queue_but_retained_metadata_guard_outlives_control() {
    let (mut plane, mut connector, topics, credits, first, _) = fixture(13);
    plane.queue_metadata(vec![first]).unwrap();
    plane
        .prepare(
            RuntimeInstant::ZERO,
            &mut connector,
            &topics,
            WorkBudget { items: 1, bytes: 1 },
        )
        .unwrap();
    assert!(plane.current.is_some());
    assert!(plane.connecting.is_none());
    let retained = plane.working_guard().unwrap();
    stop(&mut plane, &mut connector, &topics);
    assert_eq!(connector.script.borrow().starts, 0);
    assert!(!credits.is_empty());
    drop(plane);
    assert!(!credits.is_empty());
    drop(retained);
    assert!(credits.is_empty());
}
