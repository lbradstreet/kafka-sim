//! Traversal probes wrap the real in-memory transport; stalled setup is a
//! deterministic fault boundary and never fabricates a negotiated connection.
use super::*;
use crate::config::{BrokerEndpoint, ProducerConfig};
use crate::connector::Connected;
use kr_runtime::SimRuntime;
use kr_runtime_io::network::{
    ByteStreamSubmit, ByteStreamVectoredSubmit, MemoryNetwork, MemoryStream, ReadRequest,
    VectoredWriteRequest, WriteRequest,
};
use std::{cell::RefCell, future::poll_fn, rc::Rc};

struct ObservedStream {
    inner: MemoryStream,
    key: u64,
    reads: Arc<Mutex<BTreeMap<u64, usize>>>,
}
impl ByteStreamSubmit for ObservedStream {
    type ReadResponse = <MemoryStream as ByteStreamSubmit>::ReadResponse;
    type WriteResponse = <MemoryStream as ByteStreamSubmit>::WriteResponse;
    type ControlResponse = <MemoryStream as ByteStreamSubmit>::ControlResponse;
    fn submit_read(&self, request: ReadRequest) -> Self::ReadResponse {
        *self.reads.lock().unwrap().entry(self.key).or_default() += 1;
        self.inner.submit_read(request)
    }
    fn submit_write(&self, request: WriteRequest) -> Self::WriteResponse {
        self.inner.submit_write(request)
    }
    fn submit_shutdown_write(&self) -> Self::ControlResponse {
        self.inner.submit_shutdown_write()
    }
    fn submit_close(&self) -> Self::ControlResponse {
        self.inner.submit_close()
    }
}
impl ByteStreamVectoredSubmit for ObservedStream {
    type WriteVectoredResponse = <MemoryStream as ByteStreamVectoredSubmit>::WriteVectoredResponse;
    fn max_segments(&self) -> usize {
        self.inner.max_segments()
    }
    fn submit_write_vectored(&self, request: VectoredWriteRequest) -> Self::WriteVectoredResponse {
        self.inner.submit_write_vectored(request)
    }
}
struct StalledConnector {
    network: MemoryNetwork,
    peers: Arc<Mutex<BTreeMap<u64, MemoryStream>>>,
    reads: Arc<Mutex<BTreeMap<u64, usize>>>,
    visits: Rc<RefCell<Vec<u64>>>,
    negotiate: bool,
}
impl Connector for StalledConnector {
    type Stream = ObservedStream;
    type ConnectFuture =
        Pin<Box<dyn Future<Output = Result<Connected<ObservedStream>, ConnectError>>>>;
    fn connect(&mut self, target: ConnectTarget) -> Self::ConnectFuture {
        let key = target.broker_id.unwrap_or(0) as u64;
        let network = self.network.clone();
        let peers = self.peers.clone();
        let reads = self.reads.clone();
        let visits = self.visits.clone();
        let negotiate = self.negotiate;
        let mut future: Self::ConnectFuture = Box::pin(async move {
            let (left, right) = network.connected_pair().map_err(ConnectError::Network)?;
            if negotiate {
                let capabilities = negotiate_memory(&left, &right).await;
                peers.lock().unwrap().insert(key, right);
                return Ok(Connected {
                    driver: ConnectionDriver::new(
                        ObservedStream {
                            inner: left,
                            key,
                            reads,
                        },
                        target.driver,
                    )
                    .unwrap(),
                    capabilities,
                });
            }
            peers.lock().unwrap().insert(key, right);
            let stream = ObservedStream {
                inner: left,
                key,
                reads,
            };
            stream
                .submit_read(ReadRequest {
                    buffer: Vec::new(),
                    max_bytes: 1,
                })
                .await
                .map_err(|e| ConnectError::Network(e.error().error().clone()))?;
            Err(ConnectError::Timeout)
        });
        Box::pin(poll_fn(move |cx| {
            visits.borrow_mut().push(key);
            future.as_mut().poll(cx)
        }))
    }
}

async fn negotiate_memory(
    client: &MemoryStream,
    server: &MemoryStream,
) -> kr_kafka_client::control::Capabilities {
    use crate::control::{ControlCodec, Probe};
    use kr_kafka_broker_model::{BrokerAction, BrokerModel, FaultPlan};
    use kr_kafka_client::control::Negotiation;
    let codec = ControlCodec::from_config(&ProducerConfig::default()).unwrap();
    let request = codec.api_versions_request(-1, Probe::V3).unwrap();
    let request_bytes = request.len();
    assert_eq!(
        client
            .submit_write(WriteRequest { buffer: request })
            .await
            .unwrap()
            .bytes_written,
        request_bytes
    );
    let received = server
        .submit_read(ReadRequest {
            buffer: Vec::new(),
            max_bytes: request_bytes,
        })
        .await
        .unwrap();
    assert_eq!(received.bytes_read, request_bytes);
    let mut broker = BrokerModel::new(Default::default()).unwrap();
    broker
        .add_broker(kr_kafka_broker_model::BrokerEndpoint {
            id: 0,
            host: "memory-control".into(),
            port: 9092,
        })
        .unwrap();
    let BrokerAction::Reply(response) = broker
        .handle_frame(0, &received.buffer, FaultPlan::default())
        .unwrap()
    else {
        panic!("ApiVersions must return a real broker response");
    };
    let response_bytes = response.len();
    assert_eq!(
        server
            .submit_write(WriteRequest { buffer: response })
            .await
            .unwrap()
            .bytes_written,
        response_bytes
    );
    let received = client
        .submit_read(ReadRequest {
            buffer: Vec::new(),
            max_bytes: response_bytes,
        })
        .await
        .unwrap();
    assert_eq!(received.bytes_read, response_bytes);
    let Negotiation::Ready(capabilities) = codec
        .shared()
        .parse_api_versions(&received.buffer, -1, Probe::V3)
        .unwrap()
    else {
        panic!("modern broker supports the producer capability profile");
    };
    capabilities
}
struct Harness {
    actor: ProducerActor<StalledConnector>,
    _client: ProducerClient,
    _runtime: SimRuntime,
}
impl Harness {
    fn new() -> Self {
        Self::with_config(ProducerConfig {
            codec_contexts: 1,
            ..Default::default()
        })
    }
    fn with_config(config: ProducerConfig) -> Self {
        let runtime = SimRuntime::new(Default::default());
        let engine = ProducerEngine::new(config, None).unwrap();
        let connector = StalledConnector {
            network: MemoryNetwork::new(Default::default()).unwrap(),
            peers: Default::default(),
            reads: Default::default(),
            visits: Default::default(),
            negotiate: false,
        };
        let (client, actor) = ProducerActor::new(
            RuntimeHandle::Sim(runtime.handle()),
            engine,
            connector,
            ClientClock::Simulation,
            ActorConfig::default(),
        )
        .unwrap();
        Self {
            actor,
            _client: client,
            _runtime: runtime,
        }
    }
    fn setup(&mut self, key: u64) {
        self.actor
            .queue_connection(
                ConnectionKey(key),
                ConnectTarget {
                    endpoint: BrokerEndpoint {
                        host: "stalled-memory-peer".into(),
                        port: 9092,
                    },
                    broker_id: Some(key as i32),
                    lane: 0,
                    deadline: RuntimeInstant::from_nanos(1_000_000),
                    driver: self.actor.driver,
                    lifetime_guard: None,
                },
            )
            .unwrap();
    }
    fn driver(&mut self, key: u64) {
        let (left, right) = self.actor.connector.network.connected_pair().unwrap();
        self.actor
            .connector
            .peers
            .lock()
            .unwrap()
            .insert(key, right);
        let stream = ObservedStream {
            inner: left,
            key,
            reads: self.actor.connector.reads.clone(),
        };
        self.actor.connections.insert(
            ConnectionKey(key),
            ConnectionDriver::new(
                stream,
                DriverConfig {
                    mode: WriteMode::Vectored,
                    staging_bytes: 64,
                    max_operation_bytes: 128,
                    max_inflight_requests: 5,
                    rx_bytes: 64,
                },
            )
            .unwrap(),
        );
        self.actor.io_restart_required = true;
    }
    fn phase(&mut self, budget: u32) -> bool {
        self.actor
            .poll_io(
                &mut Context::from_waker(Waker::noop()),
                RuntimeInstant::ZERO,
                budget,
                true,
            )
            .unwrap()
    }
    fn continue_sweep(&self) -> bool {
        self.actor.io_sweep_active || self.actor.io_restart_required
    }
    fn assert_deadlines(&self) {
        let expected: BTreeSet<_> = self
            .actor
            .connections
            .iter()
            .filter_map(|(key, driver)| driver.next_deadline().map(|at| (at, *key)))
            .chain(
                self.actor
                    .connecting
                    .iter()
                    .filter(|(_, setup)| !setup.polled && setup.retire.is_none())
                    .map(|(key, setup)| (setup.deadline, *key)),
            )
            .collect();
        assert_eq!(self.actor.io_deadlines, expected);
    }
}

fn pending_records(budget: u32, count: usize, partition: i32) -> (Harness, TopicHandle) {
    let mut h = Harness::with_config(ProducerConfig {
        max_completions_per_poll: budget,
        max_submissions_per_poll: 1,
        lanes: 2,
        record_descriptors: 128,
        delivery_event_capacity: 128,
        pending_records_per_topic: 128,
        max_live_leases: 4,
        release_event_capacity: 4,
        max_batches: 128,
        max_open_topics: 4,
        brokers_max: 1,
        input_bytes: 65536,
        codec_contexts: 1,
        ..Default::default()
    });
    let first = h
        ._client
        .open_topic_at("first", RuntimeInstant::ZERO)
        .unwrap();
    let rest = h
        ._client
        .open_topic_at("unresolved", RuntimeInstant::ZERO)
        .unwrap();
    let records: Vec<_> = (0..count)
        .map(|index| RecordDescriptor {
            topic: if index == 0 { first } else { rest },
            partition_hint: Some(partition),
            lane_hint: None,
            key: None,
            value: Some(b"payload"),
            headers: &[],
            timestamp_ms: 0,
            user_token: index as u64,
            delivery_timeout: None,
        })
        .collect();
    assert_eq!(
        h._client
            .submit_copy_at(RuntimeInstant::ZERO, &records)
            .accepted as usize,
        count
    );
    for _ in 0..count + 4 {
        h.actor
            .ingress(
                &mut Context::from_waker(Waker::noop()),
                RuntimeInstant::ZERO,
            )
            .unwrap();
    }
    assert_eq!(h.actor.engine.pending_records().count(), count);
    (h, first)
}

fn resolve_first(h: &mut Harness, first: TopicHandle) {
    use crate::control::{BrokerNode, MetadataPartition, MetadataTopic, MetadataUpdate};
    h.actor
        .apply_control(
            ControlEvent::Metadata {
                handles: vec![first],
                update: MetadataUpdate {
                    throttle_ms: 0,
                    cluster_id: None,
                    controller_id: 0,
                    brokers: vec![BrokerNode {
                        id: 0,
                        host: "broker".into(),
                        port: 9092,
                        rack: None,
                    }],
                    topics: vec![MetadataTopic {
                        requested_index: 0,
                        id: TopicId([9; 16]),
                        name: Some("first".into()),
                        error_code: 0,
                        partitions: (0..2)
                            .map(|index| MetadataPartition {
                                replicas: Vec::new(),
                                isr: Vec::new(),
                                offline: Vec::new(),
                                index,
                                error_code: 0,
                                metadata: crate::topic::PartitionMetadata {
                                    leader: 0,
                                    leader_epoch: 0,
                                },
                            })
                            .collect(),
                    }],
                },
            },
            RuntimeInstant::ZERO,
        )
        .unwrap();
    // Metadata completion is deliberately budgeted; this helper establishes
    // the Ready precondition without changing the pending-sweep visit budget.
    while h.actor.engine.has_metadata_work() {
        h.actor.engine.on_deadline(
            RuntimeInstant::ZERO,
            WorkBudget {
                bytes: 4096,
                items: 1,
            },
        );
        h.actor.metadata_notices(1);
    }
    assert!(h.actor.engine.take_metadata_error().is_none());
}

#[test]
fn terminal_close_restarts_a_parked_control_sweep_and_retires_its_idle_read() {
    let mut h = Harness::with_config(ProducerConfig {
        codec_contexts: 1,
        rx_bytes_per_connection: 64 * 1024,
        ..Default::default()
    });
    h.actor.connector.negotiate = true;
    // Establish the real ApiVersions exchange, then stop before preparing the
    // queued identity request. The negotiated driver owns an idle pending read.
    assert!(h.actor.orders(RuntimeInstant::ZERO).unwrap());
    h.actor
        .control
        .prepare(
            RuntimeInstant::ZERO,
            &mut h.actor.connector,
            h.actor.engine.topics(),
            WorkBudget {
                bytes: 4096,
                items: 64,
            },
        )
        .unwrap();
    assert!(h.phase(64));
    assert!(!h.phase(64));
    assert!(!h.continue_sweep());
    assert_eq!(h.actor.connector.network.status().pending_reads, 1);
    assert_eq!(h.actor.connector.visits.borrow().as_slice(), &[0]);

    h._client
        .close_at(RuntimeInstant::ZERO, RuntimeDuration::from_nanos(1_000_000))
        .unwrap();
    h.actor
        .ingress(
            &mut Context::from_waker(Waker::noop()),
            RuntimeInstant::ZERO,
        )
        .unwrap();
    assert!(!h.actor.orders(RuntimeInstant::ZERO).unwrap());
    assert!(h.actor.control_draining);
    assert_eq!(h.actor.control.next_deadline(), None);
    // No provider wake or clock advance intervenes. This gated second phase
    // must admit retirement instead of waiting for the close deadline.
    assert!(
        h.actor
            .poll_io(
                &mut Context::from_waker(Waker::noop()),
                RuntimeInstant::ZERO,
                64,
                false,
            )
            .unwrap()
    );
    for _ in 0..8 {
        if h.actor.control.obligations() == 0 {
            break;
        }
        h.phase(64);
    }
    assert_eq!(h.actor.control.obligations(), 0);
    let network = h.actor.connector.network.status();
    assert_eq!(network.pending_reads, 0);
    assert_eq!(network.inflight_operations, 0);
    assert_eq!(network.buffered_bytes, 0);
    assert_eq!(h.actor.connector.visits.borrow().as_slice(), &[0]);
    assert_eq!(h.actor.handle.now(), RuntimeInstant::ZERO);
}

#[test]
fn terminal_close_discards_queued_control_work_and_drains_reserved_ownership() {
    let mut h = Harness::with_config(ProducerConfig {
        codec_contexts: 1,
        max_completions_per_poll: 1,
        ..Default::default()
    });
    let topic = h
        ._client
        .open_topic_at("queued", RuntimeInstant::ZERO)
        .unwrap();
    h.actor
        .ingress(
            &mut Context::from_waker(Waker::noop()),
            RuntimeInstant::ZERO,
        )
        .unwrap();
    // Queue work before the close command, and leave the engine's original
    // metadata/identity orders pending behind the same terminal boundary.
    h.actor.control.queue_metadata(vec![topic]).unwrap();
    h.actor.control.queue_identity(None).unwrap();
    assert!(h.actor.control.obligations() >= 3);
    h._client
        .close_at(RuntimeInstant::ZERO, RuntimeDuration::from_nanos(1_000_000))
        .unwrap();
    h.actor
        .ingress(
            &mut Context::from_waker(Waker::noop()),
            RuntimeInstant::ZERO,
        )
        .unwrap();
    let credits = h.actor.engine.credits();
    let handle = h.actor.handle.clone();
    let visits = h.actor.connector.visits.clone();
    let network = h.actor.connector.network.clone();
    let joined = handle.spawn(h.actor).unwrap();
    let (events, status) = h
        ._runtime
        .block_on(async {
            let mut events = Vec::new();
            while let Some(event) = poll_fn(|cx| h._client.poll_event(cx)).await.unwrap() {
                events.push(event);
                if matches!(event, Event::Closed { .. }) {
                    break;
                }
            }
            (events, joined.await.unwrap().unwrap())
        })
        .unwrap();
    h._runtime.finish().unwrap();
    assert!(status.closed && !status.failed);
    assert!(events.contains(&Event::Closed { unresolved: 0 }));
    assert!(events.contains(&Event::TopicFailed {
        topic,
        code: FailureReason::Closed as u32
    }));
    assert!(
        !events
            .iter()
            .any(|event| matches!(event, Event::Fatal { .. }))
    );
    assert!(
        visits.borrow().is_empty(),
        "obsolete queued work must remain cold"
    );
    assert_eq!(network.status().inflight_operations, 0);
    assert!(credits.is_empty(), "{:?}", credits.snapshot());
}

#[test]
fn closing_with_unsettled_records_keeps_control_errors_visible() {
    let (mut h, _) = pending_records(1, 1, 0);
    h._client
        .close_at(RuntimeInstant::ZERO, RuntimeDuration::from_nanos(1_000_000))
        .unwrap();
    h.actor
        .ingress(
            &mut Context::from_waker(Waker::noop()),
            RuntimeInstant::ZERO,
        )
        .unwrap();
    assert!(h.actor.closing);
    assert_eq!(
        (
            h.actor.engine.status().terminal,
            h.actor.endpoint.watermark().0
        ),
        (0, 1)
    );
    // Close has not reached its accepted watermark: metadata/identity work is
    // still necessary. A stopped control plane remains an error at this point.
    h.actor.control.stop();
    assert!(matches!(
        h.actor.orders(RuntimeInstant::ZERO),
        Err(ActorError::Control(FailureReason::Closed))
    ));
    assert!(!h.actor.control_draining);
}

#[test]
fn unresolved_pending_records_count_toward_each_routing_visit_budget() {
    for budget in [1, 2, 7] {
        let (mut h, _) = pending_records(budget, 17, 0);
        let mut visited = 0;
        loop {
            let more = h.actor.route_pending(RuntimeInstant::ZERO).unwrap();
            visited += budget as usize;
            assert_eq!(h.actor.engine.pending_records().count(), 17);
            if visited < 17 {
                assert!(more);
                assert_eq!(h.actor.pending_cursor, Some(RecordToken(visited as u64)));
            } else {
                assert!(!more, "an unchanged unresolved sweep must park");
                assert_eq!(h.actor.pending_cursor, None);
                break;
            }
        }
    }
}

#[test]
fn metadata_and_credit_changes_behind_pending_cursor_restart_without_spinning() {
    for credit_change in [false, true] {
        let (mut h, first) = pending_records(1, 7, 1);
        let credit = if credit_change {
            resolve_first(&mut h, first);
            use crate::credit::{Claim, Resource};
            let credits = h.actor.engine.credits();
            let pool = credits.snapshot()[Resource::InputBytes as usize];
            Some(
                credits
                    .reserve(&[Claim {
                        resource: Resource::InputBytes,
                        amount: pool.limit - pool.guaranteed_per_lane,
                        lane: 1,
                    }])
                    .unwrap(),
            )
        } else {
            None
        };
        assert!(h.actor.route_pending(RuntimeInstant::ZERO).unwrap());
        assert_eq!(h.actor.pending_cursor, Some(RecordToken(1)));
        assert_eq!(h.actor.engine.pending_records().count(), 7);
        if credit_change {
            drop(credit);
        } else {
            resolve_first(&mut h, first);
        }
        for token in 2..=7 {
            assert!(
                h.actor.route_pending(RuntimeInstant::ZERO).unwrap(),
                "change behind cursor requires another sweep at token {token}"
            );
        }
        assert_eq!(h.actor.pending_cursor, None);
        assert!(h.actor.route_pending(RuntimeInstant::ZERO).unwrap());
        assert_eq!(h.actor.engine.pending_records().count(), 6);
        let mut parked = false;
        for _ in 0..20 {
            if !h.actor.route_pending(RuntimeInstant::ZERO).unwrap() {
                parked = true;
                break;
            }
        }
        assert!(parked, "unchanged blocked topics must eventually park");
    }
}

#[test]
fn failed_ingress_settles_accepted_bulk_without_calling_partition_policy() {
    struct Policy(std::sync::atomic::AtomicUsize);
    impl crate::routing::NativePartitioner for Policy {
        fn choose_partitions(
            &self,
            _: &[crate::routing::RecordMetadata],
            _: crate::routing::RoutingSnapshot<'_>,
            _: &mut [crate::routing::PartitionChoice],
        ) -> Result<(), crate::routing::RoutingError> {
            self.0.fetch_add(1, Ordering::Relaxed);
            panic!("failed actor must not call policy");
        }
    }
    let policy = Arc::new(Policy(std::sync::atomic::AtomicUsize::new(0)));
    let mut h = Harness::with_config(ProducerConfig {
        partitioner: crate::routing::PartitionerConfig::Native(policy.clone()),
        max_completions_per_poll: 1,
        ..Default::default()
    });
    let topic = h
        ._client
        .open_topic_at("first", RuntimeInstant::ZERO)
        .unwrap();
    h.actor
        .ingress(
            &mut Context::from_waker(Waker::noop()),
            RuntimeInstant::ZERO,
        )
        .unwrap();
    resolve_first(&mut h, topic);
    let records: Vec<_> = (0..3)
        .map(|user_token| RecordDescriptor {
            topic,
            partition_hint: None,
            lane_hint: None,
            key: None,
            value: Some(b"payload"),
            headers: &[],
            timestamp_ms: 0,
            user_token,
            delivery_timeout: None,
        })
        .collect();
    assert_eq!(
        h._client
            .submit_copy_at(RuntimeInstant::ZERO, &records)
            .accepted,
        3
    );
    h.actor.engine.fail_producer(FailureReason::RuntimeFailed);
    for _ in 0..4 {
        h.actor
            .ingress(
                &mut Context::from_waker(Waker::noop()),
                RuntimeInstant::ZERO,
            )
            .unwrap();
    }
    assert_eq!(policy.0.load(Ordering::Relaxed), 0);
    let mut delivered = Vec::new();
    while let Some(event) = h.actor.engine.pop_event() {
        if let Event::Delivery(delivery) = event.event {
            assert_eq!(delivery.outcome.kind, DeliveryKind::NotWritten);
            delivered.push(delivery.user_token);
        }
    }
    assert_eq!(delivered, [0, 1, 2]);
}
#[test]
fn pending_setup_visits_are_bounded_fair_and_eventually_park() {
    for budget in [1, 2, 7, 64] {
        let mut h = Harness::new();
        for key in 1..=97 {
            h.setup(key);
        }
        assert_eq!(
            h.actor.connector.network.status().connections,
            0,
            "setup construction is cold"
        );
        for phase in 0..256 {
            let before = h.actor.connector.visits.borrow().len();
            assert!(!h.phase(budget));
            let after = h.actor.connector.visits.borrow().len();
            assert!(
                after - before <= budget as usize,
                "budget={budget} phase={phase}"
            );
            h.assert_deadlines();
            if !h.continue_sweep() {
                break;
            }
        }
        assert!(
            !h.continue_sweep(),
            "Pending sweep must park budget={budget}"
        );
        assert_eq!(
            *h.actor.connector.visits.borrow(),
            (1..=97).collect::<Vec<_>>()
        );
        assert_eq!(h.actor.connector.network.status().pending_reads, 97);
    }
}
#[test]
fn two_phase_owner_parks_after_a_full_pending_sweep_for_every_quota_alignment() {
    use std::sync::atomic::AtomicUsize;
    #[derive(Default)]
    struct CountWake(AtomicUsize);
    impl Wake for CountWake {
        fn wake(self: Arc<Self>) {
            self.wake_by_ref();
        }
        fn wake_by_ref(self: &Arc<Self>) {
            self.0.fetch_add(1, Ordering::Relaxed);
        }
    }
    for budget in [1, 2, 7, 8] {
        for count in [8, 16, 97] {
            let mut h = Harness::with_config(ProducerConfig {
                max_completions_per_poll: budget,
                codec_contexts: 1,
                ..Default::default()
            });
            for key in 1..=count {
                h.setup(key);
            }
            let wake = Arc::new(CountWake::default());
            let waker = Waker::from(wake.clone());
            let mut parked = false;
            for _ in 0..count * 4 + 8 {
                wake.0.store(0, Ordering::Relaxed);
                let before = h.actor.connector.visits.borrow().len();
                assert!(h.poll_owner_once(&waker).is_pending());
                let visits = h.actor.connector.visits.borrow().len() - before;
                assert!(visits <= budget as usize * 2);
                h.assert_deadlines();
                if wake.0.load(Ordering::Relaxed) == 0 {
                    parked = true;
                    break;
                }
            }
            assert!(parked, "owner did not park: quota={budget}, count={count}");
            assert!(!h.continue_sweep());
            let visited: BTreeSet<_> = h.actor.connector.visits.borrow().iter().copied().collect();
            assert!((1..=count).all(|key| visited.contains(&key)));
            assert_eq!(
                h.actor.connector.network.status().pending_reads,
                count as usize + 1
            );
        }
    }
}
#[test]
fn new_work_behind_cursor_restarts_after_tail_without_resetting_it() {
    let mut h = Harness::new();
    for key in [10, 20, 30] {
        h.setup(key);
    }
    assert!(!h.phase(2));
    assert_eq!(*h.actor.connector.visits.borrow(), [10]);
    h.setup(5);
    assert!(!h.phase(2));
    assert_eq!(*h.actor.connector.visits.borrow(), [10, 20]);
    assert!(h.phase(2), "dirty completed sweep requires a restart");
    assert_eq!(*h.actor.connector.visits.borrow(), [10, 20, 30]);
    assert!(!h.phase(2));
    assert_eq!(*h.actor.connector.visits.borrow(), [10, 20, 30, 5]);
    for _ in 0..8 {
        h.phase(2);
        if !h.continue_sweep() {
            break;
        }
    }
    assert!(!h.continue_sweep());
    h.assert_deadlines();
}
#[test]
fn completion_behind_partial_cursor_is_observed_before_parking() {
    let mut h = Harness::new();
    for key in [10, 20, 30] {
        h.driver(key);
    }
    assert!(!h.phase(2));
    let mut write =
        h.actor.connector.peers.lock().unwrap()[&10].submit_write(WriteRequest { buffer: vec![0] });
    assert!(
        Pin::new(&mut write)
            .poll(&mut Context::from_waker(Waker::noop()))
            .is_ready()
    );
    assert!(!h.phase(2));
    assert!(h.phase(2), "wake behind cursor must request a fresh sweep");
    for _ in 0..8 {
        h.phase(2);
        if !h.continue_sweep() {
            break;
        }
    }
    assert!(!h.continue_sweep());
    assert_eq!(
        h.actor.connector.reads.lock().unwrap().get(&10),
        Some(&2),
        "first read consumed and the next prefix read admitted"
    );
    assert_eq!(h.actor.connector.network.status().pending_reads, 3);
    h.assert_deadlines();
}
#[test]
fn deadline_index_tracks_enqueue_retire_and_cold_admission() {
    let mut h = Harness::new();
    h.setup(100);
    h.driver(10);
    for (correlation, deadline) in [(1, 70), (2, 30), (3, 90)] {
        h.actor
            .enqueue_connection(
                ConnectionKey(10),
                correlation,
                OwnedSendPlan::from_frame(vec![0, 0, 0, 4, 0, 0, 0, 1], 128).unwrap(),
                RuntimeInstant::from_nanos(deadline),
                RuntimeInstant::ZERO,
            )
            .unwrap();
        h.assert_deadlines();
    }
    assert_eq!(
        h.actor.io_deadlines.first().unwrap().0,
        RuntimeInstant::from_nanos(30)
    );
    h.actor
        .retire_connection(ConnectionKey(10), RetireReason::Requested);
    h.assert_deadlines();
    // Use the real stalled setup operation; its expired retained deadline must
    // disappear once admitted instead of forcing a timer spin while it is Pending.
    h.actor
        .poll_connection(
            ConnectionKey(100),
            &mut Context::from_waker(Waker::noop()),
            RuntimeInstant::ZERO,
        )
        .unwrap();
    h.assert_deadlines();
    assert!(h.actor.io_deadlines.is_empty());
    assert!(h.actor.connecting[&ConnectionKey(100)].polled);
}

#[test]
fn mixed_active_and_connecting_frontiers_share_one_visit_budget() {
    let mut h = Harness::new();
    for key in 1..=47 {
        if key % 3 == 0 {
            h.driver(key);
        } else {
            h.setup(key);
        }
    }
    for phase in 0..100 {
        let before: usize = h.actor.connector.reads.lock().unwrap().values().sum();
        assert!(!h.phase(3));
        let after: usize = h.actor.connector.reads.lock().unwrap().values().sum();
        assert!(after - before <= 3, "phase={phase}");
        if !h.continue_sweep() {
            break;
        }
    }
    assert!(!h.continue_sweep());
    let reads = h.actor.connector.reads.lock().unwrap();
    assert_eq!(reads.len(), 47);
    assert!(reads.values().all(|count| *count == 1));
    assert_eq!(h.actor.connector.network.status().pending_reads, 47);
    h.assert_deadlines();
}

#[test]
fn frame_deadline_is_removed_even_when_engine_rejects_the_response() {
    let mut h = Harness::new();
    h.driver(10);
    h.actor
        .enqueue_connection(
            ConnectionKey(10),
            1,
            OwnedSendPlan::from_frame(vec![0, 0, 0, 4, 0, 0, 0, 1], 128).unwrap(),
            RuntimeInstant::from_nanos(100),
            RuntimeInstant::ZERO,
        )
        .unwrap();
    // The transport is real; this isolated traversal fixture deliberately has no
    // engine request. Wire admission/progress is ignored until a response arrives.
    for _ in 0..3 {
        h.actor
            .poll_connection(
                ConnectionKey(10),
                &mut Context::from_waker(Waker::noop()),
                RuntimeInstant::ZERO,
            )
            .unwrap();
    }
    let mut write = h.actor.connector.peers.lock().unwrap()[&10].submit_write(WriteRequest {
        buffer: vec![0, 0, 0, 4, 0, 0, 0, 1],
    });
    assert!(
        Pin::new(&mut write)
            .poll(&mut Context::from_waker(Waker::noop()))
            .is_ready()
    );
    let mut rejected = false;
    for _ in 0..4 {
        if h.actor
            .poll_connection(
                ConnectionKey(10),
                &mut Context::from_waker(Waker::noop()),
                RuntimeInstant::ZERO,
            )
            .is_err()
        {
            rejected = true;
            break;
        }
    }
    assert!(
        rejected,
        "the engine must reject a response without a matching engine request"
    );
    assert_eq!(
        h.actor.connections[&ConnectionKey(10)].pending_requests(),
        0
    );
    h.assert_deadlines();
    assert!(
        h.actor.io_deadlines.is_empty(),
        "an error must not strand an already-consumed response deadline"
    );
}

impl Harness {
    fn poll_owner_once(&mut self, waker: &Waker) -> Poll<EngineStatus> {
        let actor = &mut self.actor;
        self._runtime
            .block_on(async { actor.poll_step(&mut Context::from_waker(waker)) })
            .unwrap()
            .unwrap()
    }
}

#[test]
fn actor_close_releases_registry_references_under_its_poll_budget() {
    for budget in [1, 2, 7] {
        let mut h = Harness::with_config(ProducerConfig {
            max_completions_per_poll: budget,
            max_live_leases: 17,
            release_event_capacity: 17,
            codec_contexts: 1,
            ..Default::default()
        });
        let leases: Vec<_> = (0..17)
            .map(|_| h._client.acquire(16, 0).unwrap().commit(16).unwrap())
            .collect();
        h._client
            .close_at(
                RuntimeInstant::ZERO,
                RuntimeDuration::from_nanos(1_000_000_000),
            )
            .unwrap();
        h.actor
            .ingress(
                &mut Context::from_waker(Waker::noop()),
                RuntimeInstant::ZERO,
            )
            .unwrap();
        assert_eq!(h.actor.endpoint.inputs().status().committed, 17);
        let mut released = Vec::new();
        let mut closed = false;
        for poll in 0..32 {
            let before = h.actor.endpoint.inputs().status().committed;
            let result = h.poll_owner_once(Waker::noop());
            let after = h.actor.endpoint.inputs().status().committed;
            assert!(
                before - after <= budget as usize,
                "budget={budget} poll={poll}"
            );
            let mut events = [Event::Closed { unresolved: 0 }; 32];
            let count = h._client.poll_events(&mut events);
            for event in &events[..count] {
                match event {
                    Event::InputReleased { lease } => {
                        assert!(!closed);
                        released.push(*lease);
                    }
                    Event::Closed { unresolved: 0 } => closed = true,
                    _ => panic!("unexpected close event {event:?}"),
                }
            }
            if result.is_ready() {
                break;
            }
        }
        assert!(closed);
        assert_eq!(released, leases);
        assert_eq!(h._client.status().unwrap().inputs.live, 0);
        assert!(h._client.credits().is_empty());
    }
}

#[test]
fn actor_close_parks_on_provider_ownership_and_final_release_wakes_it() {
    use std::sync::atomic::AtomicUsize;
    #[derive(Default)]
    struct CountWake(AtomicUsize);
    impl Wake for CountWake {
        fn wake(self: Arc<Self>) {
            self.0.fetch_add(1, Ordering::Relaxed);
        }
    }
    let mut h = Harness::with_config(ProducerConfig {
        max_completions_per_poll: 1,
        codec_contexts: 1,
        ..Default::default()
    });
    let lease = h._client.acquire(16, 0).unwrap().commit(16).unwrap();
    let provider = h.actor.endpoint.inputs().snapshot(lease).unwrap();
    let wake = Arc::new(CountWake::default());
    let waker = Waker::from(wake.clone());
    h._client
        .close_at(
            RuntimeInstant::ZERO,
            RuntimeDuration::from_nanos(1_000_000_000),
        )
        .unwrap();
    h.actor
        .ingress(
            &mut Context::from_waker(Waker::noop()),
            RuntimeInstant::ZERO,
        )
        .unwrap();
    for _ in 0..4 {
        assert!(h.poll_owner_once(&waker).is_pending());
    }
    assert!(!h.actor.endpoint.inputs().has_close_work());
    let before = wake.0.load(Ordering::Relaxed);
    assert!(h.poll_owner_once(&waker).is_pending());
    assert_eq!(
        wake.0.load(Ordering::Relaxed),
        before,
        "a held allocation alone must not spin"
    );
    drop(provider);
    assert!(wake.0.load(Ordering::Relaxed) > before);
    for _ in 0..4 {
        if h.poll_owner_once(&waker).is_ready() {
            break;
        }
    }
    let mut events = [Event::Closed { unresolved: 99 }; 4];
    let count = h._client.poll_events(&mut events);
    assert_eq!(
        &events[..count],
        &[
            Event::InputReleased { lease },
            Event::Closed { unresolved: 0 }
        ]
    );
    assert!(h._client.credits().is_empty());
}

#[test]
fn aborted_actor_finishes_local_input_sweep_and_exposes_late_provider_release() {
    let h = Harness::with_config(ProducerConfig {
        max_completions_per_poll: 1,
        max_live_leases: 17,
        release_event_capacity: 17,
        codec_contexts: 1,
        ..Default::default()
    });
    let leases: Vec<_> = (0..17)
        .map(|_| h._client.acquire(16, 0).unwrap().commit(16).unwrap())
        .collect();
    let provider = h.actor.endpoint.inputs().snapshot(leases[0]).unwrap();
    let Harness {
        actor,
        _client: client,
        _runtime,
    } = h;
    assert_eq!(client.owner_status(), 0);
    drop(actor);
    assert_eq!(client.owner_status(), 2);
    let status = client.status().unwrap();
    assert_eq!(status.inputs.committed, 0);
    assert_eq!(status.inputs.live, 1);
    assert_eq!(status.inputs.allocation_bytes_by_lane, [16, 0, 0, 0]);
    let mut events = [Event::Closed { unresolved: 99 }; 32];
    let count = client.poll_events(&mut events);
    assert!(
        !events[..count]
            .iter()
            .any(|event| matches!(event, Event::Closed { .. }))
    );
    let released: Vec<_> = events[..count]
        .iter()
        .filter_map(|event| match event {
            Event::InputReleased { lease } => Some(*lease),
            Event::Fatal { .. } => None,
            other => panic!("unexpected abort event {other:?}"),
        })
        .collect();
    assert_eq!(released, leases[1..]);
    drop(provider);
    assert_eq!(client.poll_events(&mut events), 1);
    assert_eq!(events[0], Event::InputReleased { lease: leases[0] });
    assert_eq!(client.status().unwrap().inputs.live, 0);
    assert!(client.credits().is_empty());
}
