use crate::{
    Fault, RealizedFault, ReplayManifest,
    faults::{Effects, FaultEngine, Hook, Outcome, Phase},
    history::{Audit, DomainEvent, Request},
};
use kr_kafka_broker_model::{BrokerAction, BrokerModel, FaultPlan, ObservedResponse};
use kr_kafka_client::control::Negotiation;
use kr_kafka_producer::{
    connector::{ConnectError, ConnectTarget, Connected, Connector},
    control::{ControlCodec, Probe},
    transport::{ConnectionDriver, DriverEvent, OwnedSendPlan, RetireReason, SendRequest},
    types::{TopicId, TopicPartition},
};
use kr_kafka_protocol::{
    Request as KafkaRequest, errors as code,
    frame::decode_request,
    wire::{DecodeLimits, Records},
};
use kr_kafka_record::{BatchDecodeLimits, inspect_batch};
use kr_runtime::{CompletionResult, RandomHandle, RuntimeDuration, RuntimeHandle, RuntimeInstant};
use kr_runtime_io::{completion::CompletionGuard, network::*};
use kr_shared_bytes::SharedBytes;
use std::{
    cell::{Cell, RefCell},
    collections::{BTreeMap, BTreeSet},
    future::{Future, poll_fn},
    pin::Pin,
    rc::Rc,
    task::Poll,
};
type Ticket<T, E> = Pin<Box<dyn Future<Output = CompletionResult<T, E>>>>;
pub(crate) struct AuditedStream {
    inner: SimStream,
    audit: Rc<RefCell<Audit>>,
    handle: RuntimeHandle,
    connection: u64,
    codec: Rc<ControlCodec>,
    guard: Option<std::sync::Arc<dyn Send + Sync>>,
}
impl ByteStreamSubmit for AuditedStream {
    type ReadResponse = Ticket<ReadResult, NetworkFailure>;
    type WriteResponse = Ticket<WriteResult, NetworkFailure>;
    type ControlResponse = Ticket<(), NetworkFailure>;
    fn submit_read(&self, request: ReadRequest) -> Self::ReadResponse {
        let mut response = self.inner.submit_read(request);
        if let Some(guard) = &self.guard {
            response.attach_completion_guard(guard.clone());
        }
        let audit = self.audit.clone();
        let handle = self.handle.clone();
        let connection = self.connection;
        let codec = self.codec.clone();
        Box::pin(async move {
            let result = response.await;
            if let Ok(result) = &result {
                let checked = response_read(
                    &mut audit.borrow_mut(),
                    &codec,
                    connection,
                    handle.now().as_nanos(),
                    &result.buffer,
                );
                if let Err(error) = checked {
                    audit.borrow_mut().fail(error);
                }
            }
            result
        })
    }
    fn submit_write(&self, request: WriteRequest) -> Self::WriteResponse {
        let bytes = request.buffer.len();
        let audit = self.audit.clone();
        let handle = self.handle.clone();
        let operation =
            audit
                .borrow_mut()
                .operation(handle.now().as_nanos(), self.connection, bytes, &[]);
        let mut response = self.inner.submit_write(request);
        if let Some(guard) = &self.guard {
            response.attach_completion_guard(guard.clone());
        }
        Box::pin(async move {
            let result = response.await;
            let written = match &result {
                Ok(result) => result.bytes_written,
                Err(error) => error.error().bytes_transferred(),
            };
            audit.borrow_mut().complete(
                handle.now().as_nanos(),
                operation,
                bytes,
                written,
                format!(
                    "{:?}",
                    result.as_ref().map_or_else(
                        |error| error.certainty(),
                        |_| kr_runtime::CompletionCertainty::Applied
                    )
                ),
            );
            result
        })
    }
    fn submit_shutdown_write(&self) -> Self::ControlResponse {
        let mut response = self.inner.submit_shutdown_write();
        if let Some(guard) = &self.guard {
            response.attach_completion_guard(guard.clone());
        }
        Box::pin(response)
    }
    fn submit_close(&self) -> Self::ControlResponse {
        let mut response = self.inner.submit_close();
        if let Some(guard) = &self.guard {
            response.attach_completion_guard(guard.clone());
        }
        let audit = self.audit.clone();
        let connection = self.connection;
        Box::pin(async move {
            let result = response.await;
            audit
                .borrow_mut()
                .requests
                .retain(|(id, _), _| *id != connection);
            result
        })
    }
}
impl ByteStreamVectoredSubmit for AuditedStream {
    type WriteVectoredResponse = Ticket<VectoredWriteResult, VectoredWriteFailure>;
    fn max_segments(&self) -> usize {
        self.inner.max_segments()
    }
    fn submit_write_vectored(&self, request: VectoredWriteRequest) -> Self::WriteVectoredResponse {
        let bytes = request
            .segments
            .iter()
            .map(|segment| (segment.range.end - segment.range.start) as usize)
            .sum();
        let spans: Vec<SharedBytes> = request
            .segments
            .iter()
            .map(|segment| segment.bytes.clone())
            .collect();
        let audit = self.audit.clone();
        let handle = self.handle.clone();
        let operation =
            audit
                .borrow_mut()
                .operation(handle.now().as_nanos(), self.connection, bytes, &spans);
        let mut response = self.inner.submit_write_vectored(request);
        if let Some(guard) = &self.guard {
            response.attach_completion_guard(guard.clone());
        }
        Box::pin(async move {
            let result = response.await;
            let written = match &result {
                Ok(result) => result.bytes_written,
                Err(error) => error.error().bytes_transferred(),
            };
            audit.borrow_mut().complete(
                handle.now().as_nanos(),
                operation,
                bytes,
                written,
                format!(
                    "{:?}",
                    result.as_ref().map_or_else(
                        |error| error.certainty(),
                        |_| kr_runtime::CompletionCertainty::Applied
                    )
                ),
            );
            result
        })
    }
}
fn response_read(
    audit: &mut Audit,
    codec: &ControlCodec,
    connection: u64,
    now: u64,
    bytes: &[u8],
) -> Result<(), String> {
    if bytes.len() < 8 {
        return Ok(());
    }
    let length = i32::from_be_bytes(bytes[..4].try_into().expect("prefix"));
    if length < 0 || length as usize != bytes.len() - 4 {
        return Ok(());
    }
    let correlation = i32::from_be_bytes(bytes[4..8].try_into().expect("correlation"));
    let Some(request) = audit.requests.remove(&(connection, correlation)) else {
        return Ok(());
    };
    audit.record(
        now,
        DomainEvent::ResponseRead {
            connection,
            correlation,
        },
    );
    if request.api != 0 {
        return Ok(());
    }
    if request.version != 13 {
        return Err("producer used name-based Produce".into());
    }
    let mut expected: Vec<_> = request.tokens.iter().map(|(_, part, _)| *part).collect();
    expected.sort_unstable();
    expected.dedup();
    let response = codec
        .parse_produce13(bytes, correlation, &expected)
        .map_err(|e| e.to_string())?;
    for partition in response.partitions {
        let success = matches!(
            partition.error_code,
            code::NONE | code::DUPLICATE_SEQUENCE_NUMBER
        );
        if partition.error_code == code::DUPLICATE_SEQUENCE_NUMBER {
            audit.coverage.duplicate_sequences += 1;
        }
        for (token, key, delta) in &request.tokens {
            if *key != partition.partition {
                continue;
            }
            let witness = if success {
                let duplicate = partition.error_code == code::DUPLICATE_SEQUENCE_NUMBER;
                let offset = if duplicate {
                    None
                } else {
                    Some(
                        partition
                            .base_offset
                            .ok_or("successful Produce omitted offset")?
                            .checked_add(i64::from(*delta))
                            .ok_or("response record offset overflow")?,
                    )
                };
                let timestamp = if duplicate { None } else { partition.timestamp };
                let at = audit.record(
                    now,
                    DomainEvent::ProduceResponse {
                        connection,
                        correlation,
                        token: *token,
                        duplicate,
                        offset,
                        timestamp,
                    },
                );
                Some(if duplicate {
                    ObservedResponse::Duplicate { at }
                } else {
                    ObservedResponse::Success {
                        at,
                        offset: offset.expect("normal offset"),
                        timestamp,
                    }
                })
            } else {
                None
            };
            let proof = &mut audit
                .accepted
                .get_mut(token)
                .ok_or("response for unknown token")?
                .proof;
            if witness.is_some() {
                proof.response = witness;
            }
            proof.rejection |= matches!(
                code::classify(partition.error_code),
                code::ErrorClass::DefinitiveNotWritten | code::ErrorClass::SequenceRecovery
            );
        }
    }
    Ok(())
}
struct BrokerState {
    produce_index: u32,
    duplicate: BTreeSet<u64>,
    live: BTreeMap<u64, (i32, Rc<ColdStream<SimStream>>)>,
}
#[derive(Clone)]
struct FaultRuntime {
    engine: Rc<RefCell<FaultEngine>>,
    random: RandomHandle,
    handle: RuntimeHandle,
    audit: Rc<RefCell<Audit>>,
    start_ns: u64,
}
impl FaultRuntime {
    fn decide(
        &self,
        phase: Phase,
        broker: i32,
        connection: u64,
        frame: u64,
        api: Option<i16>,
        correlation: Option<i32>,
    ) -> Result<Effects, String> {
        let now = self.handle.now().as_nanos();
        let decision = self.engine.borrow_mut().decide(
            Hook {
                phase,
                now_ns: now
                    .checked_sub(self.start_ns)
                    .ok_or("fault clock precedes start")?,
                broker,
                connection,
                frame,
                api,
                correlation,
            },
            &mut || self.random.random_u64().map_err(|error| error.to_string()),
        )?;
        let effects = decision.effects;
        self.audit
            .borrow_mut()
            .record(now, DomainEvent::FaultDecision(decision));
        Ok(effects)
    }
    #[allow(clippy::too_many_arguments)]
    fn timing(
        &self,
        phase: Phase,
        broker: i32,
        connection: u64,
        frame: u64,
        api: i16,
        correlation: i32,
        arrived_ns: u64,
    ) {
        let now_ns = self.handle.now().as_nanos();
        self.audit.borrow_mut().record(
            now_ns,
            DomainEvent::BrokerTiming {
                connection,
                broker,
                api,
                correlation,
                frame,
                phase,
                arrived_ns,
                now_ns,
            },
        );
    }
}
struct LiveConnection {
    state: Rc<RefCell<BrokerState>>,
    connection: u64,
    audit: Rc<RefCell<Audit>>,
    handle: RuntimeHandle,
}
impl Drop for LiveConnection {
    fn drop(&mut self) {
        self.state.borrow_mut().live.remove(&self.connection);
        self.audit.borrow_mut().record(
            self.handle.now().as_nanos(),
            DomainEvent::ConnectionClosed {
                connection: self.connection,
                reason: "BrokerServiceEnded".into(),
            },
        );
    }
}
#[derive(Clone)]
pub(crate) struct ModelConnector {
    pub handle: RuntimeHandle,
    pub network: SimNetwork,
    pub audit: Rc<RefCell<Audit>>,
    pub model: Rc<RefCell<BrokerModel>>,
    pub manifest: Rc<ReplayManifest>,
    next_connection: Rc<Cell<u64>>,
    broker_state: Rc<RefCell<BrokerState>>,
    faults: FaultRuntime,
}
impl ModelConnector {
    pub(crate) fn new(
        handle: RuntimeHandle,
        network: SimNetwork,
        audit: Rc<RefCell<Audit>>,
        model: Rc<RefCell<BrokerModel>>,
        manifest: Rc<ReplayManifest>,
        engine: Rc<RefCell<FaultEngine>>,
        random: RandomHandle,
    ) -> Result<Self, String> {
        if random.stream() != kr_runtime::rng::RandomStream::Fault {
            return Err("broker faults require Fault RNG stream".into());
        }
        let faults = FaultRuntime {
            engine,
            random,
            handle: handle.clone(),
            audit: audit.clone(),
            start_ns: manifest.start_ns,
        };
        let connector = Self {
            handle,
            network,
            audit,
            model,
            manifest,
            next_connection: Rc::new(Cell::new(1)),
            broker_state: Rc::new(RefCell::new(BrokerState {
                produce_index: 0,
                duplicate: BTreeSet::new(),
                live: BTreeMap::new(),
            })),
            faults,
        };
        for link in &connector.manifest.faults.links {
            for (from, to) in [
                (NodeId(0), NodeId(link.broker as u64)),
                (NodeId(link.broker as u64), NodeId(0)),
            ] {
                connector
                    .network
                    .set_link(
                        LinkKey { from, to },
                        LinkConfig {
                            max_chunk_bytes: link.chunk_bytes,
                            latency: RuntimeDuration::from_nanos(
                                connector.manifest.driver.link_latency_ns,
                            ),
                            state: LinkState::Open,
                        },
                    )
                    .map_err(|e| e.to_string())?;
            }
        }
        connector.install_isolations()?;
        Ok(connector)
    }
    /// Raw setup for an externally driven classic client. The same network,
    /// setup hooks, crash fencing and broker service are used; capability
    /// negotiation belongs to the external client.
    pub(crate) async fn connect_external(
        &self,
        broker: i32,
        deadline: RuntimeInstant,
    ) -> Result<AuditedStream, ConnectError> {
        let connection = self.next_connection.get();
        self.next_connection.set(
            connection
                .checked_add(1)
                .ok_or(ConnectError::ResourceExhausted)?,
        );
        let started = self.handle.now().as_nanos();
        let effect = self
            .faults
            .decide(Phase::Setup, broker, connection, 0, None, None)
            .map_err(|e| {
                self.audit.borrow_mut().fail(e);
                ConnectError::InvalidConfiguration
            })?;
        if crate::experiment_link::setup_failed(
            &self.manifest.faults,
            broker,
            started - self.manifest.start_ns,
        ) || effect.outcome == Outcome::SetupFailure
        {
            setup_delay(&self.handle, effect.delay_ns, deadline).await?;
            return Err(ConnectError::Timeout);
        }
        if !self.manifest.brokers.iter().any(|b| b.id == broker) {
            return Err(ConnectError::InvalidConfiguration);
        }
        if self.broker_state.borrow().live.len() >= self.manifest.network.connections {
            return Err(ConnectError::ResourceExhausted);
        }
        let (forward, reverse) = crate::experiment_link::profiles(&self.manifest, broker);
        let (left, right) = self
            .network
            .connected_pair_with_propagation(NodeId(0), NodeId(broker as u64), forward, reverse)
            .map_err(ConnectError::Network)?;
        self.audit.borrow_mut().record(
            started,
            DomainEvent::ConnectionOpened {
                connection,
                broker,
                lane: 0,
            },
        );
        let right = Rc::new(ColdStream::new(right));
        self.broker_state
            .borrow_mut()
            .live
            .insert(connection, (broker, right.clone()));
        let registration = LiveConnection {
            state: self.broker_state.clone(),
            connection,
            audit: self.audit.clone(),
            handle: self.handle.clone(),
        };
        let codec = Rc::new(
            ControlCodec::from_config(&self.manifest.producer).map_err(ConnectError::Protocol)?,
        );
        self.handle
            .spawn(serve(
                right,
                connection,
                broker,
                self.handle.clone(),
                self.audit.clone(),
                self.model.clone(),
                self.manifest.clone(),
                self.broker_state.clone(),
                self.faults.clone(),
                registration,
            ))
            .map_err(|_| ConnectError::ResourceExhausted)?;
        let stream = AuditedStream {
            inner: left,
            audit: self.audit.clone(),
            handle: self.handle.clone(),
            connection,
            codec,
            guard: None,
        };
        if let Err(error) = setup_delay(&self.handle, effect.delay_ns, deadline).await {
            let _ = stream.submit_close().await;
            return Err(error);
        }
        Ok(stream)
    }
    fn install_isolations(&self) -> Result<(), String> {
        let windows = self.faults.engine.borrow().isolations().to_vec();
        for (index, window) in windows.into_iter().enumerate() {
            let faults = self.faults.clone();
            let state = self.broker_state.clone();
            let start = self
                .manifest
                .start_ns
                .checked_add(window.start_ns)
                .ok_or("isolation start overflow")?;
            let end = self
                .manifest
                .start_ns
                .checked_add(window.end_ns)
                .ok_or("isolation end overflow")?;
            self.handle
                .spawn(async move {
                    let result = async {
                        faults
                            .handle
                            .sleep_until(RuntimeInstant::from_nanos(start))
                            .await
                            .map_err(|e| e.to_string())?;
                        faults.decide(
                            Phase::IsolationStart,
                            window.broker,
                            0,
                            index as u64 + 1,
                            None,
                            None,
                        )?;
                        // Poll every cold close at this boundary before yielding. A
                        // delayed completion must not postpone isolation of the
                        // remaining existing connections.
                        let mut closes: Vec<_> = state
                            .borrow()
                            .live
                            .iter()
                            .filter(|(_, (broker, _))| *broker == window.broker)
                            .map(|(id, (_, stream))| (*id, Some(Box::pin(stream.close()))))
                            .collect();
                        let mut timer =
                            Box::pin(faults.handle.sleep_until(RuntimeInstant::from_nanos(end)));
                        let mut restored = false;
                        poll_fn(|cx| {
                            if !restored {
                                match timer.as_mut().poll(cx) {
                                    Poll::Ready(Ok(())) => {
                                        if let Err(error) = faults.decide(
                                            Phase::IsolationEnd,
                                            window.broker,
                                            0,
                                            index as u64 + 1,
                                            None,
                                            None,
                                        ) {
                                            return Poll::Ready(Err(error));
                                        }
                                        restored = true;
                                    }
                                    Poll::Ready(Err(error)) => {
                                        return Poll::Ready(Err(error.to_string()));
                                    }
                                    Poll::Pending => {}
                                }
                            }
                            for (connection, close) in &mut closes {
                                if let Some(response) = close {
                                    match response.as_mut().poll(cx) {
                                        Poll::Ready(Ok(())) => {
                                            if let Err(error) =
                                                faults.engine.borrow_mut().isolation_closed()
                                            {
                                                return Poll::Ready(Err(error));
                                            }
                                            faults.audit.borrow_mut().record(
                                                faults.handle.now().as_nanos(),
                                                DomainEvent::IsolationClosed {
                                                    broker: window.broker,
                                                    connection: *connection,
                                                },
                                            );
                                            *close = None;
                                        }
                                        Poll::Ready(Err(error)) => {
                                            return Poll::Ready(Err(format!(
                                                "isolation close failed: {error:?}"
                                            )));
                                        }
                                        Poll::Pending => {}
                                    }
                                }
                            }
                            if restored && closes.iter().all(|(_, response)| response.is_none()) {
                                Poll::Ready(Ok(()))
                            } else {
                                Poll::Pending
                            }
                        })
                        .await
                    }
                    .await;
                    if let Err(error) = result {
                        faults.audit.borrow_mut().fail(error);
                    }
                })
                .map_err(|error| error.to_string())?;
        }
        Ok(())
    }
}
impl Connector for ModelConnector {
    type Stream = AuditedStream;
    type ConnectFuture =
        Pin<Box<dyn Future<Output = Result<Connected<Self::Stream>, ConnectError>>>>;
    fn connect(&mut self, target: ConnectTarget) -> Self::ConnectFuture {
        let network = self.network.clone();
        let handle = self.handle.clone();
        let audit = self.audit.clone();
        let model = self.model.clone();
        let manifest = self.manifest.clone();
        let next = self.next_connection.clone();
        let broker_state = self.broker_state.clone();
        let faults = self.faults.clone();
        Box::pin(async move {
            let connection = next.get();
            next.set(
                connection
                    .checked_add(1)
                    .ok_or(ConnectError::ResourceExhausted)?,
            );
            let broker = resolve_target(&manifest, &target)?;
            let started_ns = handle.now().as_nanos();
            let effect = faults
                .decide(Phase::Setup, broker, connection, 0, None, None)
                .map_err(|error| {
                    audit.borrow_mut().fail(error);
                    ConnectError::InvalidConfiguration
                })?;
            let mut resolved_ns = None;
            let result = async {
                if crate::experiment_link::setup_failed(
                    &manifest.faults,
                    broker,
                    started_ns - manifest.start_ns,
                ) {
                    let window = manifest
                        .faults
                        .link_outages
                        .iter()
                        .find(|w| {
                            w.broker == broker
                                && w.mode == crate::OutageMode::FailFast
                                && w.start_ns <= started_ns - manifest.start_ns
                                && started_ns - manifest.start_ns < w.end_ns
                        })
                        .expect("active failure");
                    let link = if window.direction == crate::LinkDirection::FromBroker {
                        LinkKey {
                            from: NodeId(broker as u64),
                            to: NodeId(0),
                        }
                    } else {
                        LinkKey {
                            from: NodeId(0),
                            to: NodeId(broker as u64),
                        }
                    };
                    return Err(ConnectError::Network(NetworkError::Partitioned { link }));
                }
                if effect.outcome == Outcome::SetupFailure {
                    setup_delay(&handle, effect.delay_ns, target.deadline).await?;
                    return Err(ConnectError::Timeout);
                }
                if broker_state.borrow().live.len() >= manifest.network.connections {
                    return Err(ConnectError::ResourceExhausted);
                }
                let pair = if crate::experiment_link::enabled(&manifest.faults) {
                    let (forward, reverse) = crate::experiment_link::profiles(&manifest, broker);
                    network.connected_pair_with_propagation(
                        NodeId(0),
                        NodeId(broker as u64),
                        forward,
                        reverse,
                    )
                } else {
                    network.connected_pair(NodeId(0), NodeId(broker as u64))
                };
                let (left, right) = pair.map_err(ConnectError::Network)?;
                audit.borrow_mut().record(
                    handle.now().as_nanos(),
                    DomainEvent::ConnectionOpened {
                        connection,
                        broker,
                        lane: target.lane,
                    },
                );
                let right = Rc::new(ColdStream::new(right));
                broker_state
                    .borrow_mut()
                    .live
                    .insert(connection, (broker, right.clone()));
                let registration = LiveConnection {
                    state: broker_state.clone(),
                    connection,
                    audit: audit.clone(),
                    handle: handle.clone(),
                };
                let codec = Rc::new(
                    ControlCodec::from_config(&manifest.producer)
                        .map_err(ConnectError::Protocol)?,
                );
                let service = serve(
                    right,
                    connection,
                    broker,
                    handle.clone(),
                    audit.clone(),
                    model,
                    manifest.clone(),
                    broker_state,
                    faults,
                    registration,
                );
                handle
                    .spawn(service)
                    .map_err(|_| ConnectError::ResourceExhausted)?;
                let stream = AuditedStream {
                    inner: left,
                    audit: audit.clone(),
                    handle: handle.clone(),
                    connection,
                    codec: codec.clone(),
                    guard: target.lifetime_guard,
                };
                // The endpoint exists while modeled setup work runs, so an
                // isolation timer can close a setup that crosses its start edge.
                setup_delay(&handle, effect.delay_ns, target.deadline).await?;
                let mut driver = ConnectionDriver::new(stream, target.driver)
                    .map_err(|_| ConnectError::InvalidConfiguration)?;
                if let Some(capture) = &audit.borrow().request_capture {
                    driver
                        .set_request_observer(capture.connection(connection))
                        .map_err(|_| ConnectError::InvalidConfiguration)?;
                }
                let request = codec
                    .api_versions_request(-1, Probe::V3)
                    .map_err(ConnectError::Protocol)?;
                driver
                    .enqueue(SendRequest {
                        correlation: -1,
                        deadline: target.deadline,
                        plan: OwnedSendPlan::from_frame(
                            request,
                            manifest.producer.rx_bytes_per_connection as usize,
                        )
                        .map_err(|_| ConnectError::InvalidConfiguration)?,
                    })
                    .map_err(|_| ConnectError::ResourceExhausted)?;
                let mut setup_timer = Box::pin(handle.sleep_until(target.deadline));
                let negotiated = poll_fn(|cx| {
                    if setup_timer.as_mut().poll(cx).is_ready() {
                        return Poll::Ready(None);
                    }
                    match driver.poll_event(cx, handle.now()) {
                        Poll::Pending => Poll::Pending,
                        Poll::Ready(Some(DriverEvent::Frame { bytes, .. })) => Poll::Ready(Some(
                            codec.shared().parse_api_versions(bytes, -1, Probe::V3),
                        )),
                        Poll::Ready(None) | Poll::Ready(Some(DriverEvent::Released)) => {
                            Poll::Ready(None)
                        }
                        Poll::Ready(Some(_)) => {
                            cx.waker().wake_by_ref();
                            Poll::Pending
                        }
                    }
                })
                .await;
                resolved_ns = Some(handle.now().as_nanos());
                match negotiated {
                    Some(Ok(Negotiation::Ready(capabilities))) => {
                        if !capabilities.supports(0, 13) {
                            audit.borrow_mut().incompatible_produce_advertised = true;
                        }
                        Ok(Connected {
                            driver,
                            capabilities,
                        })
                    }
                    result => {
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
                        Err(match result {
                            Some(Err(error)) => ConnectError::Protocol(error),
                            _ => ConnectError::Timeout,
                        })
                    }
                }
            }
            .await;
            if crate::experiment_link::enabled(&manifest.faults) {
                audit.borrow_mut().record(
                    handle.now().as_nanos(),
                    DomainEvent::SetupFinished {
                        connection,
                        broker,
                        started_ns,
                        deadline_ns: target.deadline.as_nanos(),
                        resolved_ns: resolved_ns.unwrap_or_else(|| handle.now().as_nanos()),
                        elapsed_ns: handle.now().as_nanos() - started_ns,
                        result: result
                            .as_ref()
                            .map_or_else(|error| format!("{error:?}"), |_| "Ready".into()),
                    },
                );
            }
            result
        })
    }
}
fn resolve_target(manifest: &ReplayManifest, target: &ConnectTarget) -> Result<i32, ConnectError> {
    let broker = manifest
        .brokers
        .iter()
        .find(|broker| broker.host == target.endpoint.host && broker.port == target.endpoint.port)
        .ok_or(ConnectError::InvalidConfiguration)?;
    if target.broker_id.is_some_and(|id| id != broker.id) {
        return Err(ConnectError::InvalidConfiguration);
    }
    Ok(broker.id)
}

async fn setup_delay(
    handle: &RuntimeHandle,
    delay_ns: u64,
    deadline: RuntimeInstant,
) -> Result<(), ConnectError> {
    if delay_ns != 0 {
        let ready = handle
            .now()
            .as_nanos()
            .checked_add(delay_ns)
            .map(RuntimeInstant::from_nanos)
            .ok_or(ConnectError::Timeout)?;
        handle
            .sleep_until(ready.min(deadline))
            .await
            .map_err(|_| ConnectError::Timeout)?;
    }
    if handle.now() >= deadline {
        Err(ConnectError::Timeout)
    } else {
        Ok(())
    }
}
#[allow(clippy::too_many_arguments)]
async fn serve(
    stream: Rc<ColdStream<SimStream>>,
    connection: u64,
    broker: i32,
    handle: RuntimeHandle,
    audit: Rc<RefCell<Audit>>,
    model: Rc<RefCell<BrokerModel>>,
    manifest: Rc<ReplayManifest>,
    state: Rc<RefCell<BrokerState>>,
    faults: FaultRuntime,
    _registration: LiveConnection,
) {
    let mut frame_index = 0u64;
    loop {
        let mut bytes = Vec::new();
        let mut expected = 4;
        while bytes.len() < expected {
            let max_bytes = expected - bytes.len();
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
                let length = i32::from_be_bytes(bytes[..4].try_into().expect("prefix"));
                if length < 4
                    || length as usize > manifest.producer.rx_bytes_per_connection as usize - 4
                {
                    audit
                        .borrow_mut()
                        .fail("broker rejected invalid frame length".into());
                    let _ = stream.close().await;
                    return;
                }
                expected = length as usize + 4;
            }
        }
        let parsed = observe_request(
            &mut audit.borrow_mut(),
            connection,
            handle.now().as_nanos(),
            &bytes,
        );
        let (api, correlation, tokens) = match parsed {
            Ok(parsed) => parsed,
            Err(error) => {
                audit.borrow_mut().fail(error);
                let _ = stream.close().await;
                return;
            }
        };
        let arrived_ns = handle.now().as_nanos();
        frame_index = match frame_index.checked_add(1) {
            Some(index) => index,
            None => {
                audit
                    .borrow_mut()
                    .fail("broker frame ordinal overflow".into());
                return;
            }
        };
        if manifest.driver.service_delay_ns != 0 && !faults.engine.borrow().has_service(broker) {
            let _ = handle
                .sleep(RuntimeDuration::from_nanos(
                    manifest.driver.service_delay_ns,
                ))
                .await;
        }
        let mut plan = FaultPlan::default();
        if abandon_crashed_frame(
            &manifest,
            &audit,
            &handle,
            broker,
            connection,
            correlation,
            arrived_ns,
            &tokens,
        ) {
            let _ = stream.close().await;
            return;
        }
        let mut disconnect = false;
        if api == 0 {
            let index = {
                let mut state = state.borrow_mut();
                let index = state.produce_index;
                state.produce_index += 1;
                index
            };
            for rule in manifest
                .fault_plan
                .iter()
                .filter(|rule| rule.produce_index == index)
            {
                let realized = RealizedFault {
                    produce_index: index,
                    correlation,
                    rule: rule.clone(),
                };
                audit.borrow_mut().record(
                    handle.now().as_nanos(),
                    DomainEvent::Fault(realized.clone()),
                );
                audit.borrow_mut().realized.push(realized);
                let result = apply_fault(
                    &rule.fault,
                    &manifest,
                    &mut model.borrow_mut(),
                    &mut plan,
                    &mut disconnect,
                );
                if let Err(error) = result {
                    audit.borrow_mut().fail(error);
                }
                match rule.fault {
                    Fault::DuplicateSequence => {
                        state.borrow_mut().duplicate.extend(tokens.iter().copied());
                        plan.drop_after_commit = true;
                    }
                    Fault::LeaderMove { .. } => audit.borrow_mut().coverage.leader_moves += 1,
                    Fault::Recreate { topic, .. } => {
                        let mut audit = audit.borrow_mut();
                        audit.coverage.recreates += 1;
                        audit.recreated.insert(manifest.topics[topic as usize].id);
                    }
                    Fault::AddPartitions { .. } => audit.borrow_mut().coverage.expands += 1,
                    Fault::Throttle { .. } => audit.borrow_mut().coverage.throttles += 1,
                    _ => {}
                }
            }
            if !plan.drop_after_commit
                && tokens
                    .iter()
                    .any(|token| state.borrow().duplicate.contains(token))
            {
                plan.duplicate_sequence_error = true;
            }
        }
        if disconnect {
            for token in &tokens {
                if let Some(record) = audit.borrow_mut().accepted.get_mut(token) {
                    record.proof.ambiguous = true;
                }
            }
            let _ = stream.close().await;
            return;
        }
        let before = match phase_effect(
            &faults,
            Phase::BeforeAppend,
            broker,
            connection,
            frame_index,
            api,
            correlation,
        )
        .await
        {
            Ok(effect) => effect,
            Err(error) => {
                audit.borrow_mut().fail(error);
                let _ = stream.close().await;
                return;
            }
        };
        if before.outcome != Outcome::Continue {
            mark_ambiguous(&audit, &tokens);
            if before.outcome == Outcome::Disconnect {
                let _ = stream.close().await;
                return;
            }
            continue;
        }
        if let Some(error) = before.reject_error {
            plan.reject_before_commit = Some(error);
        }
        plan.throttle_time_ms = plan.throttle_time_ms.max(before.throttle_ms as i32);
        faults.timing(
            Phase::BeforeAppend,
            broker,
            connection,
            frame_index,
            api,
            correlation,
            arrived_ns,
        );
        if abandon_crashed_frame(
            &manifest,
            &audit,
            &handle,
            broker,
            connection,
            correlation,
            arrived_ns,
            &tokens,
        ) {
            let _ = stream.close().await;
            return;
        }
        let before_stats = model.borrow().stats();
        let action = model.borrow_mut().handle_frame(broker, &bytes, plan);
        let action = match action {
            Ok(action) => action,
            Err(error) => {
                audit.borrow_mut().fail(error.to_string());
                let _ = stream.close().await;
                return;
            }
        };
        let after_stats = model.borrow().stats();
        let committed = match after_stats
            .committed_batches
            .checked_sub(before_stats.committed_batches)
            .zip(
                after_stats
                    .committed_records
                    .checked_sub(before_stats.committed_records),
            )
            .and_then(|(batches, records)| {
                Some((u32::try_from(batches).ok()?, u32::try_from(records).ok()?))
            }) {
            Some((0, 0)) => false,
            Some((batches, records)) if batches != 0 && records != 0 => {
                audit.borrow_mut().record(
                    handle.now().as_nanos(),
                    DomainEvent::BrokerCommit {
                        connection,
                        correlation,
                        batches,
                        records,
                    },
                );
                true
            }
            _ => {
                audit
                    .borrow_mut()
                    .fail("invalid broker append counter transition".into());
                return;
            }
        };
        faults.timing(
            Phase::AfterAppend,
            broker,
            connection,
            frame_index,
            api,
            correlation,
            arrived_ns,
        );
        let after = match phase_effect(
            &faults,
            Phase::AfterAppend,
            broker,
            connection,
            frame_index,
            api,
            correlation,
        )
        .await
        {
            Ok(effect) => effect,
            Err(error) => {
                audit.borrow_mut().fail(error);
                let _ = stream.close().await;
                return;
            }
        };
        if after.outcome != Outcome::Continue {
            record_response_loss(&faults, committed);
            mark_ambiguous(&audit, &tokens);
            if after.outcome == Outcome::Disconnect {
                let _ = stream.close().await;
                return;
            }
            continue;
        }
        match action {
            BrokerAction::Reply(response) => {
                let reply = match phase_effect(
                    &faults,
                    Phase::BeforeResponse,
                    broker,
                    connection,
                    frame_index,
                    api,
                    correlation,
                )
                .await
                {
                    Ok(effect) => effect,
                    Err(error) => {
                        audit.borrow_mut().fail(error);
                        let _ = stream.close().await;
                        return;
                    }
                };
                if reply.outcome != Outcome::Continue {
                    record_response_loss(&faults, committed);
                    mark_ambiguous(&audit, &tokens);
                    if reply.outcome == Outcome::Disconnect {
                        let _ = stream.close().await;
                        return;
                    }
                    continue;
                }
                faults.timing(
                    Phase::BeforeResponse,
                    broker,
                    connection,
                    frame_index,
                    api,
                    correlation,
                    arrived_ns,
                );
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
                    if result.bytes_written == 0 {
                        audit.borrow_mut().fail("zero broker write progress".into());
                        return;
                    }
                    offset += result.bytes_written;
                }
            }
            BrokerAction::Disconnect { .. } => {
                record_response_loss(&faults, committed);
                for token in &tokens {
                    if let Some(record) = audit.borrow_mut().accepted.get_mut(token) {
                        record.proof.ambiguous = true;
                    }
                }
                let _ = stream.close().await;
                return;
            }
            BrokerAction::DropRequest | BrokerAction::DropResponse { .. } => {
                record_response_loss(&faults, committed);
                for token in &tokens {
                    if let Some(record) = audit.borrow_mut().accepted.get_mut(token) {
                        record.proof.ambiguous = true;
                    }
                }
            }
        }
    }
}
#[allow(clippy::too_many_arguments)]
async fn phase_effect(
    faults: &FaultRuntime,
    phase: Phase,
    broker: i32,
    connection: u64,
    frame: u64,
    api: i16,
    correlation: i32,
) -> Result<Effects, String> {
    let effect = faults.decide(
        phase,
        broker,
        connection,
        frame,
        Some(api),
        Some(correlation),
    )?;
    if effect.delay_ns != 0 {
        faults
            .handle
            .sleep(RuntimeDuration::from_nanos(effect.delay_ns))
            .await
            .map_err(|error| error.to_string())?;
    }
    Ok(effect)
}
fn record_response_loss(faults: &FaultRuntime, committed: bool) {
    if committed && let Err(error) = faults.engine.borrow_mut().committed_response_lost() {
        faults.audit.borrow_mut().fail(error);
    }
}
fn mark_ambiguous(audit: &Rc<RefCell<Audit>>, tokens: &[u64]) {
    let mut audit = audit.borrow_mut();
    for token in tokens {
        if let Some(record) = audit.accepted.get_mut(token) {
            record.proof.ambiguous = true;
        }
    }
}
fn apply_fault(
    fault: &Fault,
    manifest: &ReplayManifest,
    model: &mut BrokerModel,
    plan: &mut FaultPlan,
    disconnect: &mut bool,
) -> Result<(), String> {
    let topic = |index: u32| {
        manifest
            .topics
            .get(index as usize)
            .ok_or("fault topic index")
    };
    match fault {
        Fault::DropBeforeCommit => plan.drop_after_parse = true,
        Fault::DropAfterCommit => plan.drop_after_commit = true,
        Fault::DisconnectBeforeCommit => *disconnect = true,
        Fault::DisconnectAfterCommit => plan.disconnect_before_response = true,
        Fault::DuplicateSequence => {}
        Fault::RejectSequence => {
            plan.reject_before_commit = Some(code::OUT_OF_ORDER_SEQUENCE_NUMBER)
        }
        Fault::LeaderMove {
            topic: index,
            partition,
            broker,
        } => model
            .move_leader(topic(*index)?.id, *partition, *broker)
            .map_err(|e| e.to_string())?,
        Fault::Throttle { millis } => plan.throttle_time_ms = *millis,
        Fault::Delete { topic: index } => model
            .delete_topic(topic(*index)?.id)
            .map_err(|e| e.to_string())?,
        Fault::Recreate {
            topic: index,
            new_id,
        } => {
            let topic = topic(*index)?;
            model.delete_topic(topic.id).map_err(|e| e.to_string())?;
            model
                .create_topic_with_id(&topic.name, *new_id, &topic.leaders)
                .map_err(|e| e.to_string())?;
        }
        Fault::AddPartitions {
            topic: index,
            additional_leaders,
        } => model
            .add_partitions(topic(*index)?.id, additional_leaders)
            .map_err(|e| e.to_string())?,
    }
    Ok(())
}
fn observe_request(
    audit: &mut Audit,
    connection: u64,
    now: u64,
    bytes: &[u8],
) -> Result<(i16, i32, Vec<u64>), String> {
    if let Some((version, correlation)) = kr_kafka_broker_model::unsupported_api_versions(bytes) {
        audit.record(
            now,
            DomainEvent::BrokerRequest {
                connection,
                api: 18,
                version,
                correlation,
                records: Vec::new(),
            },
        );
        return Ok((18, correlation, Vec::new()));
    }
    let frame = decode_request(bytes, DecodeLimits::default()).map_err(|e| e.to_string())?;
    let mut tokens = Vec::new();
    if let KafkaRequest::ProduceRequest(kr_kafka_protocol::produce_request::View::V13(request)) =
        &frame.body
    {
        audit.coverage.produce13 += 1;
        for topic in request.topic_data.iter() {
            let topic = topic.map_err(|e| e.to_string())?;
            for partition in topic.partition_data.iter() {
                let partition = partition.map_err(|e| e.to_string())?;
                let Some(Records::Borrowed(bytes)) = partition.records else {
                    return Err("decoded produce records were not borrowed".into());
                };
                let batch = inspect_batch(bytes, BatchDecodeLimits::default())
                    .map_err(|e| e.to_string())?;
                for record in batch.records() {
                    let record = record.map_err(|e| e.to_string())?;
                    let headers: Vec<_> = record
                        .headers
                        .collect::<Result<_, _>>()
                        .map_err(|error| error.to_string())?;
                    let id = crate::manifest::record_id(
                        headers.iter().map(|header| (header.key, header.value)),
                    )?;
                    let token = *audit
                        .ids
                        .get(&id)
                        .ok_or("C1 broker received unaccepted workload ID")?;
                    let accepted = audit
                        .accepted
                        .get_mut(&token)
                        .ok_or("accepted token missing")?;
                    if accepted.proof.transmitted {
                        audit.coverage.retries += 1;
                    }
                    accepted.proof.transmitted = true;
                    accepted.proof.parsed_attempts = accepted
                        .proof
                        .parsed_attempts
                        .checked_add(1)
                        .ok_or("parsed attempt counter overflow")?;
                    tokens.push((
                        token,
                        TopicPartition {
                            topic: TopicId(topic.topic_id),
                            partition: partition.index,
                        },
                        record.offset_delta,
                    ));
                }
            }
        }
    } else if frame.api_key == 0 {
        return Err("producer used unsupported Produce layout".into());
    }
    let ids = tokens.iter().map(|(token, _, _)| *token).collect();
    audit.record(
        now,
        DomainEvent::BrokerRequest {
            connection,
            api: frame.api_key,
            version: frame.version,
            correlation: frame.correlation_id,
            records: tokens.iter().map(|(token, _, _)| *token).collect(),
        },
    );
    audit.requests.insert(
        (connection, frame.correlation_id),
        Request {
            api: frame.api_key,
            version: frame.version,
            tokens,
        },
    );
    Ok((frame.api_key, frame.correlation_id, ids))
}

#[cfg(test)]
mod target_tests {
    use super::*;
    #[test]
    fn explicit_broker_identity_must_match_its_endpoint() {
        let manifest = ReplayManifest::from_seed(0, crate::CampaignLimits::default()).unwrap();
        let mut target = ConnectTarget {
            endpoint: kr_kafka_client::config::BrokerEndpoint {
                host: "model".into(),
                port: 9093,
            },
            broker_id: None,
            lane: 0,
            deadline: RuntimeInstant::from_nanos(1),
            driver: kr_kafka_client::transport::DriverConfig::default(),
            lifetime_guard: None,
        };
        assert_eq!(resolve_target(&manifest, &target).unwrap(), 2);
        target.broker_id = Some(1);
        assert!(matches!(
            resolve_target(&manifest, &target),
            Err(ConnectError::InvalidConfiguration)
        ));
        target.broker_id = Some(2);
        assert_eq!(resolve_target(&manifest, &target).unwrap(), 2);
        target.endpoint.host = "missing".into();
        assert!(resolve_target(&manifest, &target).is_err());
    }
}

/// Check declared windows, not callback order. A frame received before a crash
/// cannot resume append after recovery even if its simulated service delay spans
/// the entire outage. The legacy socket-isolation path is unchanged.
#[allow(clippy::too_many_arguments)]
fn abandon_crashed_frame(
    manifest: &ReplayManifest,
    audit: &Rc<RefCell<Audit>>,
    handle: &RuntimeHandle,
    broker: i32,
    connection: u64,
    correlation: i32,
    arrived_ns: u64,
    tokens: &[u64],
) -> bool {
    if !manifest.faults.crash_on_isolation {
        return false;
    }
    let now = handle.now().as_nanos();
    let Some(index) = interrupted_window(
        &manifest.faults,
        broker,
        arrived_ns - manifest.start_ns,
        now - manifest.start_ns,
    ) else {
        return false;
    };
    mark_ambiguous(audit, tokens);
    audit.borrow_mut().record(
        now,
        DomainEvent::BrokerFrameAbandoned {
            connection,
            correlation,
            broker,
            window: index as u32,
        },
    );
    true
}

fn interrupted_window(
    config: &crate::faults::FaultConfig,
    broker: i32,
    arrived: u64,
    now: u64,
) -> Option<usize> {
    if !config.crash_on_isolation {
        return None;
    }
    config.isolations.iter().position(|window| {
        window.broker == broker && window.start_ns <= now && window.end_ns > arrived
    })
}
#[cfg(test)]
mod crash_service_tests {
    use super::*;
    #[test]
    fn crash_edges_are_half_open_and_service_cannot_survive_a_whole_outage() {
        let mut config = crate::faults::FaultConfig {
            crash_on_isolation: true,
            isolations: vec![crate::faults::IsolationWindow {
                broker: 1,
                start_ns: 20,
                end_ns: 50,
            }],
            ..Default::default()
        };
        for (arrived, now, expected) in [
            (19, 19, None),
            (19, 20, Some(0)),
            (20, 20, Some(0)),
            (49, 50, Some(0)),
            (50, 50, None),
            (19, 60, Some(0)),
            (50, 60, None),
        ] {
            assert_eq!(interrupted_window(&config, 1, arrived, now), expected);
        }
        assert_eq!(interrupted_window(&config, 2, 19, 60), None);
        config.crash_on_isolation = false;
        assert_eq!(interrupted_window(&config, 1, 19, 60), None);
    }
}
