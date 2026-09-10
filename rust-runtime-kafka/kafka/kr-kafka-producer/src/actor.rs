//! One portable owner task. Ingress is thread-safe; every Kafka transition and
//! every policy callback runs here on the runtime owner thread.
use crate::{
    admission::AdmittedRecord,
    client::{ClientClock, ClientEndpoint, ClientError, Command, ProducerClient},
    connector::{ConnectError, ConnectTarget, Connector},
    engine::{
        ConnectionEvent, ConnectionKey, EngineError, EngineOrder, EngineStatus, ProducerEngine,
    },
    transport::{
        ConnectionDriver, DriverConfig, DriverEvent, OwnedSendPlan, RetireReason, SendRequest,
        WriteMode,
    },
    types::*,
};
use kr_runtime::{RuntimeDuration, RuntimeHandle, RuntimeInstant, Sleep};
use std::{
    collections::{BTreeMap, BTreeSet, VecDeque},
    fmt,
    future::Future,
    pin::Pin,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    task::{Context, Poll, Wake, Waker},
};

mod control;
pub(crate) mod routing;
use control::{ControlEvent, ControlPlane};

#[derive(Clone, Copy, Debug)]
pub struct ActorConfig {
    pub write_mode: WriteMode,
    /// The host wrapper calibrates this before starting the runtime. Simulation
    /// uses its fixed scenario quota; this value never changes within a run.
    pub encode_bytes_per_poll: u32,
    /// Modeled elapsed compression work. A nonzero delay prevents an always
    /// ready encoder from starving virtual network and delivery deadlines.
    pub sim_encode_cost: RuntimeDuration,
}
impl Default for ActorConfig {
    fn default() -> Self {
        Self {
            write_mode: WriteMode::Staging,
            encode_bytes_per_poll: 64 * 1024,
            sim_encode_cost: RuntimeDuration::from_nanos(1_000),
        }
    }
}
#[derive(Debug)]
pub enum ActorError {
    Client(ClientError),
    Engine(EngineError),
    Control(FailureReason),
    InvalidConfig,
    RuntimeStopped,
}
impl fmt::Display for ActorError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "producer actor: {self:?}")
    }
}
impl std::error::Error for ActorError {}
impl From<ClientError> for ActorError {
    fn from(e: ClientError) -> Self {
        Self::Client(e)
    }
}
impl From<EngineError> for ActorError {
    fn from(e: EngineError) -> Self {
        Self::Engine(e)
    }
}
impl From<FailureReason> for ActorError {
    fn from(e: FailureReason) -> Self {
        Self::Control(e)
    }
}

/// Provider wakes are distinct from the actor's own bounded-work continuation.
/// A wake behind a partial cursor must request a fresh sweep. This cell owns no
/// actor, stream, setup future, or provider; retained abandoned wakers cannot
/// create a cycle through the actor's connection maps.
#[derive(Default)]
struct IoWake {
    changed: AtomicBool,
    parent: Mutex<Option<Arc<Waker>>>,
}
impl IoWake {
    fn register(&self, waker: &Waker) {
        if self
            .parent
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .as_ref()
            .is_some_and(|old| old.will_wake(waker))
        {
            return;
        }
        let next = Arc::new(waker.clone());
        let old = self
            .parent
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .replace(next);
        drop(old);
    }
    fn take_changed(&self) -> bool {
        self.changed.swap(false, Ordering::AcqRel)
    }
    fn clear(&self) {
        let old = self.parent.lock().unwrap_or_else(|p| p.into_inner()).take();
        drop(old);
    }
}
impl Wake for IoWake {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }
    fn wake_by_ref(self: &Arc<Self>) {
        self.changed.store(true, Ordering::Release);
        let parent = self
            .parent
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .clone();
        if let Some(parent) = parent {
            kr_runtime::contain_panic(|| parent.wake_by_ref());
        }
    }
}

struct Connecting<F> {
    future: Pin<Box<F>>,
    retire: Option<RetireReason>,
    deadline: RuntimeInstant,
    polled: bool,
}

/// Construction is cold. The returned client can reserve commands before this
/// future is spawned; no network operation starts until the actor is polled.
pub struct ProducerActor<C: Connector> {
    handle: RuntimeHandle,
    engine: ProducerEngine,
    connector: C,
    endpoint: ClientEndpoint,
    telemetry: std::sync::Arc<crate::telemetry::ProducerTelemetry>,
    settings: ActorConfig,
    driver: DriverConfig,
    control: ControlPlane<C>,
    connections: BTreeMap<ConnectionKey, ConnectionDriver<C::Stream>>,
    connecting: BTreeMap<ConnectionKey, Connecting<C::ConnectFuture>>,
    routing: routing::Routing,
    pending_submission: Option<std::vec::IntoIter<AdmittedRecord>>,
    // One Ready-topic slice keeps its original admission credits while advisory
    // collection yields; it never borrows the engine's unresolved-topic quota.
    routing_ingress: Vec<AdmittedRecord>,
    routing_pending: Vec<RecordToken>,
    routing_pending_more: bool,
    routing_deadline: Option<RuntimeInstant>,
    fences: VecDeque<Command>,
    cancels: BTreeSet<RecordToken>,
    sleep: Option<(RuntimeInstant, Pin<Box<Sleep>>)>,
    encode_at: Option<RuntimeInstant>,
    connection_cursor: Option<ConnectionKey>,
    io_sweep_active: bool,
    io_restart_required: bool,
    io_control_next: bool,
    io_deadlines: BTreeSet<(RuntimeInstant, ConnectionKey)>,
    io_wake: Arc<IoWake>,
    pending_cursor: Option<RecordToken>,
    pending_credit_state: Option<[crate::credit::PoolStatus; 3]>,
    pending_restart_required: bool,
    closing: bool,
    // Close's accepted watermark is terminal: only provider/control ownership
    // drain remains. Metadata timers can still emit obsolete engine orders
    // until that drain permits the engine's bounded final cleanup.
    control_draining: bool,
    done: bool,
    failure: Option<ActorError>,
}
// Associated futures are independently pinned in boxes. Moving this owner does
// not move a future that has been polled or a provider-owned byte allocation.
impl<C: Connector> Unpin for ProducerActor<C> {}

impl<C: Connector> ProducerActor<C> {
    pub fn new(
        handle: RuntimeHandle,
        engine: ProducerEngine,
        mut connector: C,
        clock: ClientClock,
        settings: ActorConfig,
    ) -> Result<(ProducerClient, Self), ActorError> {
        let simulation_runtime = match (&handle, &clock) {
            (RuntimeHandle::Sim(handle), ClientClock::Simulation) => Some(handle.identity()),
            (RuntimeHandle::Host(handle), ClientClock::Host(control))
                if control.belongs_to(handle) =>
            {
                None
            }
            _ => return Err(ActorError::InvalidConfig),
        };
        let initial = engine.status();
        if initial.accepted != 0
            || initial.closing
            || initial.failed
            || initial.closed
            || !engine.topics().is_empty()
        {
            return Err(ActorError::InvalidConfig);
        }
        if settings.encode_bytes_per_poll == 0
            || matches!(handle, RuntimeHandle::Sim(_))
                && settings.sim_encode_cost == RuntimeDuration::ZERO
        {
            return Err(ActorError::InvalidConfig);
        }
        if matches!(
            (&handle, &clock),
            (RuntimeHandle::Sim(_), ClientClock::Host(_))
                | (RuntimeHandle::Host(_), ClientClock::Simulation)
        ) {
            return Err(ActorError::InvalidConfig);
        }
        let config = engine.config();
        let driver = DriverConfig {
            mode: settings.write_mode,
            staging_bytes: config.staging_bytes_per_connection as usize,
            max_operation_bytes: config
                .request_hard_bytes
                .max(config.staging_bytes_per_connection)
                .max(config.rx_bytes_per_connection) as usize,
            max_inflight_requests: config.max_in_flight_per_connection as usize,
            rx_bytes: config.rx_bytes_per_connection as usize,
        };
        let control = ControlPlane::new(config, engine.credits(), driver)?;
        let (client, endpoint) =
            ProducerClient::channel(config.clone(), engine.credits(), clock, simulation_runtime)?;
        let routing = routing::Routing::new(config).map_err(|_| EngineError::AllocationFailed)?;
        let routing_limit = config.max_completions_per_poll as usize;
        let routing_ingress =
            crate::fixed::try_vec(routing_limit).map_err(|_| EngineError::AllocationFailed)?;
        let routing_pending =
            crate::fixed::try_vec(routing_limit).map_err(|_| EngineError::AllocationFailed)?;
        let telemetry = endpoint.telemetry();
        endpoint.attach_metrics(engine.metrics());
        connector.set_telemetry(telemetry.transport());
        Ok((
            client,
            Self {
                handle,
                engine,
                connector,
                endpoint,
                telemetry,
                settings,
                driver,
                control,
                connections: BTreeMap::new(),
                connecting: BTreeMap::new(),
                routing,
                pending_submission: None,
                routing_ingress,
                routing_pending,
                routing_pending_more: false,
                routing_deadline: None,
                fences: VecDeque::new(),
                cancels: BTreeSet::new(),
                sleep: None,
                encode_at: None,
                connection_cursor: None,
                io_sweep_active: false,
                io_restart_required: false,
                io_control_next: true,
                io_deadlines: BTreeSet::new(),
                io_wake: Arc::new(IoWake::default()),
                pending_cursor: None,
                pending_credit_state: None,
                pending_restart_required: false,
                closing: false,
                control_draining: false,
                done: false,
                failure: None,
            },
        ))
    }
    pub fn status(&self) -> EngineStatus {
        self.engine.status()
    }
    pub fn engine(&self) -> &ProducerEngine {
        &self.engine
    }

    fn fail(&mut self, reason: FailureReason) {
        self.endpoint.fence();
        self.engine.fail_producer(reason);
        if !self.closing {
            self.closing = true;
            let now = self.handle.now();
            let _ = self.engine.close(now, now, self.endpoint.watermark());
        }
        self.endpoint.inputs().close();
        self.control.stop();
    }
    fn apply_control(
        &mut self,
        event: ControlEvent,
        now: RuntimeInstant,
    ) -> Result<(), ActorError> {
        match event {
            ControlEvent::Metadata { handles, update } => {
                self.engine
                    .begin_metadata(now, handles, update, self.control.working_guard())?;
            }
            ControlEvent::Identity(identity) => self.engine.install_identity(identity)?,
            ControlEvent::MetadataFailed { handles } => {
                self.engine
                    .begin_metadata_failure(now, handles, self.control.working_guard())?;
            }
            ControlEvent::Fatal(reason) => self.fail(reason),
            ControlEvent::Progress | ControlEvent::Released => {}
        }
        Ok(())
    }
    fn metadata_notices(&mut self, maximum: u32) -> bool {
        let mut progress = false;
        for _ in 0..maximum {
            let Some(handle) = self.engine.take_metadata_notice() else {
                break;
            };
            self.pending_restart_required = true;
            if let Ok(topic) = self.engine.topics().get(handle) {
                self.endpoint.update_topic(topic);
            }
            progress = true;
        }
        progress
    }
    /// Merge the two ordered frontiers without materializing or sorting keys.
    fn next_io_key(&self) -> Option<ConnectionKey> {
        use std::ops::Bound::{Excluded, Unbounded};
        let next = |keys: &BTreeMap<ConnectionKey, _>| {
            match self.connection_cursor {
                Some(cursor) => keys.range((Excluded(cursor), Unbounded)).next(),
                None => keys.first_key_value(),
            }
            .map(|(key, _)| *key)
        };
        let connecting = match self.connection_cursor {
            Some(cursor) => self.connecting.range((Excluded(cursor), Unbounded)).next(),
            None => self.connecting.first_key_value(),
        }
        .map(|(key, _)| *key);
        next(&self.connections).into_iter().chain(connecting).min()
    }
    fn replace_io_deadline(
        &mut self,
        key: ConnectionKey,
        before: Option<RuntimeInstant>,
        after: Option<RuntimeInstant>,
    ) {
        if before == after {
            return;
        }
        if let Some(at) = before {
            self.io_deadlines.remove(&(at, key));
        }
        if let Some(at) = after {
            self.io_deadlines.insert((at, key));
        }
    }
    fn finish_io_sweep(&mut self) -> bool {
        self.io_restart_required |= self.io_wake.take_changed();
        self.connection_cursor = None;
        self.io_sweep_active = false;
        std::mem::take(&mut self.io_restart_required)
    }
    fn poll_io(
        &mut self,
        cx: &mut Context<'_>,
        now: RuntimeInstant,
        maximum: u32,
        allow_idle_start: bool,
    ) -> Result<bool, ActorError> {
        self.io_wake.register(cx.waker());
        self.io_restart_required |= self.io_wake.take_changed();
        // The two I/O phases share one resumable sweep. Once the first phase
        // finishes a Pending sweep, the second must stay parked unless a wake
        // or newly prepared work dirtied it. Starting another partial sweep
        // here would keep the owner runnable forever when the frontier is
        // larger than one phase's quota, preventing virtual timers advancing.
        if !allow_idle_start && !self.io_sweep_active && !self.io_restart_required {
            return Ok(false);
        }
        let waker = Waker::from(self.io_wake.clone());
        let mut context = Context::from_waker(&waker);
        let cx = &mut context;
        let mut progress = false;
        let mut control_polled = false;
        for _ in 0..maximum {
            if !self.io_sweep_active {
                self.io_sweep_active = true;
                self.io_restart_required = false;
            }
            let key = self.next_io_key();
            // One control visit per phase, alternating with data when the quota
            // is one. A Pending setup/read counts just as much as a completion.
            if !control_polled && (self.io_control_next || key.is_none()) {
                control_polled = true;
                self.io_control_next = false;
                if let Poll::Ready(event) =
                    self.control
                        .poll_event(cx, now, &mut self.connector, self.engine.topics())
                {
                    self.apply_control(event, now)?;
                    progress = true;
                }
            } else if let Some(key) = key {
                self.connection_cursor = Some(key);
                self.io_control_next = true;
                let ready = self.poll_connection(key, cx, now)?;
                self.io_restart_required |= ready;
                progress |= ready;
            } else {
                return Ok(self.finish_io_sweep() || progress);
            }
        }
        self.io_restart_required |= self.io_wake.take_changed();
        if self.next_io_key().is_none() {
            progress |= self.finish_io_sweep();
        }
        // A partial sweep needs continuation only if it is still partial after
        // the end-of-poll I/O phase. The caller checks io_sweep_active then.
        Ok(progress)
    }
    fn poll_connection(
        &mut self,
        key: ConnectionKey,
        cx: &mut Context<'_>,
        now: RuntimeInstant,
    ) -> Result<bool, ActorError> {
        // A frame handled earlier in this I/O sweep can quarantine a connection.
        // Its queued Retire order may not reach orders() before another sweep;
        // carry the engine fence into the driver before it can arm a younger write.
        let retiring = self.engine.is_failed() || self.engine.connection_is_retiring(key);
        if retiring && let Some(setup) = self.connecting.get_mut(&key) {
            setup.retire = Some(RetireReason::Requested);
        }
        if self
            .connecting
            .get(&key)
            .is_some_and(|setup| !setup.polled && (setup.retire.is_some() || now >= setup.deadline))
        {
            let setup = self.connecting.remove(&key).expect("cold setup exists");
            self.replace_io_deadline(key, Some(setup.deadline), None);
            self.engine.on_connection(
                key,
                now,
                ConnectionEvent::Retiring {
                    reason: RetireReason::Requested,
                },
            )?;
            self.engine
                .on_connection(key, now, ConnectionEvent::Released)?;
            return Ok(true);
        }
        if let Some(connecting) = self.connecting.get_mut(&key) {
            if !connecting.polled {
                self.io_deadlines.remove(&(connecting.deadline, key));
                connecting.polled = true;
            }
            let result = match connecting.future.as_mut().poll(cx) {
                Poll::Pending => return Ok(false),
                Poll::Ready(result) => result,
            };
            let retire = self
                .connecting
                .remove(&key)
                .expect("polled connecting slot")
                .retire;
            match result {
                Ok(mut connected) => {
                    if crate::control::Capabilities::from_advertised(
                        &connected.capabilities,
                        matches!(
                            self.engine.config().security,
                            crate::config::SecurityConfig::SaslTls { .. }
                        ),
                    )
                    .is_err()
                    {
                        connected.driver.retire(RetireReason::Requested);
                        self.fail(FailureReason::ProtocolViolation);
                    } else if let Some(reason) = retire {
                        connected.driver.retire(reason);
                    } else {
                        self.engine
                            .on_connection(key, now, ConnectionEvent::Active)?;
                    }
                    self.replace_io_deadline(key, None, connected.driver.next_deadline());
                    self.connections.insert(key, connected.driver);
                }
                Err(error) => {
                    self.engine.on_connection(
                        key,
                        now,
                        ConnectionEvent::Retiring {
                            reason: RetireReason::Requested,
                        },
                    )?;
                    self.engine
                        .on_connection(key, now, ConnectionEvent::Released)?;
                    if matches!(
                        error,
                        ConnectError::Authentication
                            | ConnectError::Protocol(_)
                            | ConnectError::InvalidConfiguration
                            | ConnectError::TransportUnavailable
                    ) {
                        self.fail(if matches!(error, ConnectError::Authentication) {
                            FailureReason::Authentication
                        } else {
                            FailureReason::ProtocolViolation
                        });
                    }
                }
            }
            return Ok(true);
        }
        let Some(driver) = self.connections.get_mut(&key) else {
            return Ok(false);
        };
        let before = driver.next_deadline();
        if retiring {
            driver.retire(RetireReason::Requested);
        }
        let copied_before = driver.staged_copies();
        let mut released = false;
        let mut progressed = false;
        let result: Result<(), ActorError> = (|| {
            match driver.poll_event(cx, now) {
                Poll::Pending => {}
                Poll::Ready(None) => {
                    progressed = true;
                    released = true;
                }
                Poll::Ready(Some(event)) => {
                    progressed = true;
                    match event {
                        DriverEvent::WriteAdmitted { correlation } => {
                            if let Some(request) = self.engine.request_for(key, correlation) {
                                self.engine.on_write_admitted(request)?;
                            }
                        }
                        DriverEvent::WriteProgress {
                            correlation,
                            confirmed,
                            certainty,
                            bytes,
                        } => {
                            self.telemetry.wire(bytes);
                            if self.engine.request_for(key, correlation).is_some() {
                                self.engine.on_write_at(
                                    now,
                                    key,
                                    correlation,
                                    confirmed,
                                    certainty,
                                )?;
                            }
                        }
                        DriverEvent::RequestRetired {
                            correlation,
                            confirmed,
                            certainty,
                        } => {
                            self.engine.on_request_retired(
                                key,
                                correlation,
                                now,
                                confirmed,
                                certainty,
                            )?;
                        }
                        DriverEvent::Frame { bytes, .. } => {
                            self.engine.on_frame(key, now, bytes)?;
                        }
                        DriverEvent::Retiring { reason } => {
                            self.engine.on_connection(
                                key,
                                now,
                                ConnectionEvent::Retiring { reason },
                            )?;
                        }
                        DriverEvent::Released => {
                            released = true;
                        }
                    }
                }
            }
            Ok(())
        })();
        let after = driver.next_deadline();
        self.telemetry
            .staging(driver.staged_copies().saturating_sub(copied_before));
        // Restore the exact deadline index even if processing the borrowed frame
        // reported an engine error and the actor is about to fence itself.
        self.replace_io_deadline(key, before, if released { None } else { after });
        result?;
        if released {
            self.connections.remove(&key);
            self.engine
                .on_connection(key, now, ConnectionEvent::Released)?;
        }
        Ok(progressed)
    }
    fn command(&mut self, command: Command, now: RuntimeInstant) -> Result<(), ActorError> {
        match command {
            Command::RefreshTopic { handle, at } => {
                self.engine.request_metadata_refresh(handle, at);
                self.endpoint.refreshed(handle);
            }
            Command::OpenTopic {
                handle,
                name,
                at,
                credit,
            } => {
                let result = self
                    .engine
                    .open_topic_reserved_credit(handle, &name, at, credit);
                if !(self.engine.is_failed() && matches!(result, Err(EngineError::Closed))) {
                    result?;
                }
            }
            Command::Submit(batch) => {
                debug_assert!(self.pending_submission.is_none());
                self.pending_submission = Some(batch.drain().into_iter());
            }
            Command::Cancel { token } => {
                if token.0 <= self.engine.status().accepted {
                    self.engine.cancel(now, token)?;
                } else if token <= self.endpoint.watermark() {
                    self.cancels.insert(token);
                }
            }
            Command::Close {
                at,
                deadline,
                watermark,
            } => {
                if !self.closing {
                    self.closing = true;
                    self.engine.close(at, deadline, watermark)?;
                    self.endpoint.inputs().close();
                }
            }
            fence => self.fences.push_back(fence),
        }
        Ok(())
    }
    fn ingress(&mut self, cx: &mut Context<'_>, now: RuntimeInstant) -> Result<bool, ActorError> {
        let maximum = self.engine.config().max_submissions_per_poll;
        let mut drained = 0;
        // Keep only one bulk off-mailbox at a time. Its descriptors still own
        // every input and completion credit while consumed over bounded polls.
        while self.pending_submission.is_none()
            && self.routing_ingress.is_empty()
            && drained < maximum
        {
            match self.endpoint.poll_command(cx) {
                Poll::Ready(Ok(Some(command))) => {
                    self.command(command, now)?;
                    drained += 1;
                }
                Poll::Ready(Ok(None)) | Poll::Pending => break,
                Poll::Ready(Err(error)) => return Err(error.into()),
            }
        }
        let mut immediate = drained == maximum;
        if self.routing_ingress.is_empty()
            && self.routing_pending.is_empty()
            && let Some(pending) = &mut self.pending_submission
        {
            let mut bytes = 0u64;
            let limit = self.engine.config().max_completions_per_poll as usize;
            while self.routing_ingress.len() < limit
                && bytes < u64::from(self.settings.encode_bytes_per_poll)
            {
                let Some(record) = pending.next() else {
                    break;
                };
                bytes += u64::from(record.standalone_encoded_bytes);
                self.routing_deadline = Some(
                    self.routing_deadline
                        .map_or(record.deadline, |at| at.min(record.deadline)),
                );
                self.routing_ingress.push(record);
            }
            if pending.len() == 0 {
                self.pending_submission = None;
            }
        }
        if !self.routing_ingress.is_empty() {
            if self
                .routing
                .choose(&self.handle, &self.engine, now, self.routing_ingress.iter())
            {
                self.engine.admit_records_buffered(
                    now,
                    &mut self.routing_ingress,
                    self.routing.choices(),
                )?;
                self.routing.reset();
                self.routing_deadline = None;
            }
            // Collection has finite local work, or a completed slice exposed a
            // ready mailbox entry. Outstanding metadata itself never self-wakes.
            immediate = true;
        }
        let accepted = RecordToken(self.engine.status().accepted);
        #[cfg(feature = "binding-test-hooks")]
        let metadata_ready = !self.engine.has_metadata_work();
        let ready = |command: &Command| match command {
            #[cfg(feature = "binding-test-hooks")]
            Command::TestMetadata { .. } => metadata_ready,
            Command::Flush { watermark, .. } | Command::CloseTopic { watermark, .. } => {
                *watermark <= accepted
            }
            _ => true,
        };
        for _ in 0..maximum {
            if !self.fences.front().is_some_and(ready) {
                break;
            }
            match self.fences.pop_front().expect("checked fence") {
                #[cfg(feature = "binding-test-hooks")]
                Command::TestMetadata { handle, at, update } => {
                    self.engine.apply_metadata(at, &[handle], update)?;
                    if let Ok(topic) = self.engine.topics().get(handle) {
                        self.endpoint.update_topic(topic);
                    }
                }
                Command::Flush {
                    token,
                    at,
                    watermark,
                    credit,
                } => self.engine.flush_reserved(token, watermark, at, credit)?,
                Command::CloseTopic { handle, at, .. } => {
                    self.engine.close_topic(handle, at)?;
                    self.endpoint.topic_closed(handle);
                    self.routing.forget(handle);
                }
                _ => unreachable!("only fences retained"),
            }
        }
        immediate |= self.fences.front().is_some_and(ready);
        let cancels: Vec<_> = self
            .cancels
            .range(..=accepted)
            .copied()
            .take(maximum as usize)
            .collect();
        for token in cancels {
            self.cancels.remove(&token);
            self.engine.cancel(now, token)?;
        }
        immediate |= self.cancels.first().is_some_and(|token| *token <= accepted);
        Ok(immediate)
    }
    fn pending_credits(&self) -> [crate::credit::PoolStatus; 3] {
        use crate::credit::Resource;
        let pools = self.engine.credits().snapshot();
        [
            Resource::InputBytes,
            Resource::Descriptors,
            Resource::DeliveryEvents,
        ]
        .map(|resource| pools[resource as usize])
    }
    fn route_pending(&mut self, now: RuntimeInstant) -> Result<bool, ActorError> {
        if self.engine.is_failed() {
            self.routing_pending.clear();
            self.pending_cursor = None;
            self.pending_credit_state = None;
            self.pending_restart_required = false;
            if self.routing_ingress.is_empty() {
                self.routing.reset();
            }
            return Ok(false);
        }
        if !self.routing_ingress.is_empty() {
            return Ok(false);
        }
        if self.routing_pending.is_empty() {
            if self.pending_cursor.is_none() {
                self.pending_credit_state = Some(self.pending_credits());
                self.pending_restart_required = false;
            }
            let maximum = self.engine.config().max_completions_per_poll as usize;
            let visited = self
                .engine
                .pending_records_after(self.pending_cursor)
                .take(maximum);
            for record in visited {
                self.pending_cursor = Some(record.token);
                if self
                    .engine
                    .topics()
                    .get(record.topic)
                    .is_ok_and(|topic| topic.state != crate::topic::TopicState::Resolving)
                {
                    self.routing_pending.push(record.token);
                }
            }
            self.routing_pending_more = self.pending_cursor.is_some()
                && self
                    .engine
                    .pending_records_after(self.pending_cursor)
                    .next()
                    .is_some();
        }
        // Deadline/cancel settlement may remove a token between collection polls.
        // Re-fetch only this bounded slice and never consume a vanished input's
        // native quota or RNG draw.
        self.routing_pending
            .retain(|&token| self.engine.pending_record(token).is_some());
        let records = self
            .routing_pending
            .iter()
            .filter_map(|&token| self.engine.pending_record(token));
        if !self
            .routing
            .choose(&self.handle, &self.engine, now, records)
        {
            return Ok(true);
        }
        for (index, &token) in self.routing_pending.iter().enumerate() {
            if self.engine.pending_record(token).is_some() {
                self.engine
                    .route_pending(now, token, self.routing.choices()[index])?;
            }
        }
        self.routing_pending.clear();
        self.routing.reset();
        if self.routing_pending_more {
            return Ok(true);
        }
        // A metadata or credit change behind the cursor requests another sweep.
        // Stable exhausted credits and unresolved topics park after one pass.
        let restart = self.pending_restart_required
            || self
                .pending_credit_state
                .is_some_and(|old| old != self.pending_credits());
        self.pending_cursor = None;
        self.pending_credit_state = None;
        self.pending_restart_required = false;
        Ok(restart && self.engine.pending_records().next().is_some())
    }
    fn queue_connection(
        &mut self,
        key: ConnectionKey,
        target: ConnectTarget,
    ) -> Result<(), ActorError> {
        if self.connecting.contains_key(&key) || self.connections.contains_key(&key) {
            return Err(EngineError::StaleConnection.into());
        }
        let deadline = target.deadline;
        self.io_restart_required = true;
        let future = Box::pin(self.connector.connect(target));
        self.replace_io_deadline(key, None, Some(deadline));
        self.connecting.insert(
            key,
            Connecting {
                future,
                retire: None,
                deadline,
                polled: false,
            },
        );
        Ok(())
    }
    fn retire_connection(&mut self, connection: ConnectionKey, reason: RetireReason) {
        self.io_restart_required = true;
        if let Some(driver) = self.connections.get_mut(&connection) {
            let before = driver.next_deadline();
            driver.retire(reason);
            let after = driver.next_deadline();
            self.replace_io_deadline(connection, before, after);
        } else if let Some(connecting) = self.connecting.get_mut(&connection) {
            connecting.retire = Some(reason);
            self.io_deadlines.remove(&(connecting.deadline, connection));
        }
    }
    fn enqueue_connection(
        &mut self,
        connection: ConnectionKey,
        correlation: i32,
        plan: OwnedSendPlan,
        deadline: RuntimeInstant,
        now: RuntimeInstant,
    ) -> Result<(), ActorError> {
        self.io_restart_required = true;
        self.telemetry.coalesced(plan.coalesced_bytes());
        let driver = self
            .connections
            .get_mut(&connection)
            .ok_or(EngineError::StaleConnection)?;
        let before = driver.next_deadline();
        if self.engine.is_failed() {
            driver.retire(RetireReason::Requested);
            let after = driver.next_deadline();
            self.replace_io_deadline(connection, before, after);
            if self.engine.request_for(connection, correlation).is_some() {
                self.engine.on_request_retired(
                    connection,
                    correlation,
                    now,
                    0,
                    kr_runtime::CompletionCertainty::NotApplied,
                )?;
            }
            return Ok(());
        }
        if let Err(rejected) = driver.enqueue(SendRequest {
            correlation,
            deadline,
            plan,
        }) {
            driver.retire(RetireReason::Transport(rejected.error));
        }
        let after = driver.next_deadline();
        self.replace_io_deadline(connection, before, after);
        Ok(())
    }
    fn orders(&mut self, now: RuntimeInstant) -> Result<bool, ActorError> {
        if !self.control_draining
            && self.closing
            && self.engine.status().terminal == self.endpoint.watermark().0
        {
            self.control_draining = true;
            // stop creates retirement work without a provider wake. Re-open a
            // completed first-phase sweep so the second phase admits close.
            self.io_restart_required = true;
            // Fence both work queued before close and orders emitted by this
            // poll's deadlines before handing anything to the control plane.
            // stop retains admitted setup/read/write/close operations until
            // their real terminal events release the working arena.
            self.control.stop();
        }
        let maximum = self.engine.config().max_completions_per_poll;
        let mut processed = false;
        for _ in 0..maximum {
            let Some(order) = self.engine.pop_order() else {
                return Ok(processed);
            };
            processed = true;
            self.io_restart_required = true;
            match order {
                EngineOrder::Connect {
                    key,
                    node,
                    lane,
                    lifetime_guard,
                } => {
                    let deadline = now
                        .checked_add(self.engine.config().request_timeout)
                        .unwrap_or(RuntimeInstant::MAX);
                    let target = ConnectTarget {
                        endpoint: crate::config::BrokerEndpoint {
                            host: node.host,
                            port: node.port,
                        },
                        broker_id: Some(node.id),
                        lane,
                        deadline,
                        driver: self.driver,
                        lifetime_guard: Some(lifetime_guard),
                    };
                    self.queue_connection(key, target)?;
                }
                EngineOrder::Metadata { handles } => {
                    if !self.engine.is_failed() && !self.control_draining {
                        self.control.queue_metadata(handles)?;
                    }
                }
                EngineOrder::InitProducerId { previous } => {
                    if !self.engine.is_failed() && !self.control_draining {
                        self.control.queue_identity(previous)?;
                    }
                }
                EngineOrder::Retire { connection, reason } => {
                    self.retire_connection(connection, reason);
                }
                EngineOrder::Dispatch {
                    connection,
                    correlation,
                    plan,
                    deadline,
                    ..
                } => {
                    self.enqueue_connection(connection, correlation, plan, deadline, now)?;
                }
            }
        }
        Ok(true)
    }
    fn events(&mut self, maximum: u32) -> Result<bool, ActorError> {
        for _ in 0..maximum {
            if let Some(event) = self.endpoint.inputs().pop_released() {
                self.engine
                    .publish_event(event)
                    .map_err(|_| EngineError::InvalidState("precharged release queue full"))?;
            }
            let Some(event) = self.engine.pop_event() else {
                return Ok(false);
            };
            self.endpoint.publish(event)?;
        }
        Ok(true)
    }
    /// Observed backing for bounded routing work. Ordered sticky/native indexes
    /// and allocator headers remain in the configuration report's explicit gaps.
    #[must_use]
    pub fn routing_storage(&self) -> crate::routing::RoutingStorage {
        crate::routing::RoutingStorage {
            fixed_capacity_bytes: self.routing.metadata_capacity_bytes()
                + self.routing_ingress.capacity() * size_of::<AdmittedRecord>()
                + self.routing_pending.capacity() * size_of::<RecordToken>(),
            callback_view_peak_bytes: self.routing.callback_view_peak_bytes(),
        }
    }
    fn obligations(&self) -> usize {
        self.connections.len()
            + self.connecting.len()
            + self.control.obligations()
            + self.endpoint.inputs().status().live
            + usize::from(self.pending_submission.is_some())
            + usize::from(!self.routing_ingress.is_empty())
            + usize::from(!self.routing_pending.is_empty())
            + self.fences.len()
            + self.cancels.len()
            + self.endpoint.queued_commands()
            + usize::from(self.endpoint.watermark().0 > self.engine.status().accepted)
    }
    fn poll_step(&mut self, cx: &mut Context<'_>) -> Result<Poll<EngineStatus>, ActorError> {
        self.engine.register_reclaim_waker(cx.waker());
        self.io_wake.register(cx.waker());
        self.endpoint
            .inputs()
            .register_release_waker(&Waker::from(self.io_wake.clone()));
        let now = self.handle.now();
        self.engine.observe_metrics_time(now);
        if self.endpoint.take_failure() {
            self.fail(FailureReason::RuntimeFailed);
        }
        let maximum = self.engine.config().max_completions_per_poll;
        let budget = WorkBudget {
            bytes: self.settings.encode_bytes_per_poll,
            items: maximum,
        };
        self.engine.set_external_obligations(self.obligations());
        let mut immediate = self.poll_io(cx, now, maximum, true)?;
        let dp = self.engine.on_deadline(now, budget);
        immediate |= dp.remaining_immediate;
        if let Some(error) = self.engine.take_metadata_error() {
            return Err(error.into());
        }
        immediate |= self.metadata_notices(maximum);
        immediate |= self.ingress(cx, now)?;
        immediate |= self
            .endpoint
            .inputs()
            .close_step(maximum as usize)
            .remaining;
        immediate |= self.route_pending(now)?;
        if self.encode_at.is_none_or(|at| now >= at) {
            let encode_start = match self.handle {
                RuntimeHandle::Host(_) => Some(self.handle.now()),
                RuntimeHandle::Sim(_) => None,
            };
            let completion_delay = if matches!(self.handle, RuntimeHandle::Sim(_)) {
                self.settings.sim_encode_cost
            } else {
                RuntimeDuration::ZERO
            };
            let progress = self
                .engine
                .encode_with_completion_delay(now, budget, completion_delay);
            let encoded = !self.engine.last_encode_work().is_empty();
            let elapsed = match encode_start {
                Some(start) => self
                    .handle
                    .now()
                    .checked_duration_since(start)
                    .unwrap_or(RuntimeDuration::ZERO),
                None => self.settings.sim_encode_cost,
            };
            self.engine.observe_encode_elapsed(elapsed)?;
            immediate |= self.engine.has_maintenance_work();
            self.telemetry.encode(progress.bytes);
            if matches!(self.handle, RuntimeHandle::Sim(_))
                && (encoded || progress.remaining_immediate)
            {
                self.encode_at = Some(
                    now.checked_add(self.settings.sim_encode_cost)
                        .unwrap_or(RuntimeInstant::MAX),
                );
            } else {
                self.encode_at = None;
                immediate |= progress.remaining_immediate;
            }
        }
        if let Ok(draw) = self.handle.random_u64() {
            self.engine.set_retry_jitter(draw);
        }
        let sp = self.engine.schedule(now, budget);
        immediate |= sp.remaining_immediate;
        immediate |= self.orders(now)?;
        if !self.engine.has_metadata_work() {
            let prepared =
                self.control
                    .prepare(now, &mut self.connector, self.engine.topics(), budget)?;
            self.io_restart_required |= prepared.items != 0;
            immediate |= prepared.remaining_immediate;
        }
        immediate |= self.events(maximum)?;
        // New cold setup/read/write/close futures first enter admission here.
        immediate |= self.poll_io(cx, now, maximum, false)?;
        immediate |= self.io_sweep_active || self.io_restart_required;
        self.engine.set_external_obligations(self.obligations());
        immediate |= self.events(maximum)?;
        immediate |= self.engine.has_terminal_work();
        self.engine.publish_metrics(now);
        immediate |= self.engine.has_metrics_work();
        if self.engine.status().closed && self.engine.status().queued_events == 0 {
            self.done = true;
            self.endpoint.finish();
            return Ok(Poll::Ready(self.engine.status()));
        }
        let deadline = self
            .engine
            .next_deadline()
            .into_iter()
            .chain(self.control.next_deadline())
            .chain(self.encode_at)
            .chain(self.routing_deadline)
            .chain(self.io_deadlines.first().map(|(at, _)| *at))
            .min();
        if self.sleep.as_ref().map(|(at, _)| *at) != deadline {
            self.sleep = deadline.map(|at| (at, Box::pin(self.handle.sleep_until(at))));
        }
        if let Some((_, sleep)) = &mut self.sleep {
            match sleep.as_mut().poll(cx) {
                Poll::Ready(Ok(())) => {
                    self.sleep = None;
                    immediate = true;
                }
                Poll::Ready(Err(_)) => return Err(ActorError::RuntimeStopped),
                Poll::Pending => {}
            }
        }
        if immediate {
            cx.waker().wake_by_ref();
        }
        Ok(Poll::Pending)
    }
}
impl<C: Connector> Future for ProducerActor<C> {
    type Output = Result<EngineStatus, ActorError>;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        let start = matches!(this.handle, RuntimeHandle::Host(_)).then(|| this.handle.now());
        let result = this.poll_step(cx);
        let nanos = start
            .and_then(|start| this.handle.now().checked_duration_since(start))
            .map_or(0, |duration| duration.as_nanos());
        this.telemetry.poll(nanos);
        match result {
            Ok(Poll::Ready(status)) => Poll::Ready(this.failure.take().map_or(Ok(status), Err)),
            Ok(Poll::Pending) => Poll::Pending,
            Err(error) => {
                let reason = match &error {
                    ActorError::Control(reason) => *reason,
                    _ => FailureReason::RuntimeFailed,
                };
                this.fail(reason);
                if matches!(error, ActorError::RuntimeStopped) {
                    return Poll::Ready(Err(error));
                }
                if this.failure.is_none() {
                    this.failure = Some(error);
                }
                // A protocol/control failure fences data immediately, but an
                // available runtime still drains admitted provider operations.
                cx.waker().wake_by_ref();
                Poll::Pending
            }
        }
    }
}

impl<C: Connector> Drop for ProducerActor<C> {
    fn drop(&mut self) {
        self.endpoint.inputs().clear_release_waker();
        self.io_wake.clear();
        if self.done {
            return;
        }
        // Emergency runtime failure abandons observation, never provider-owned
        // storage. Freeze caller reservation before collecting accepted mailbox
        // records, then publish exactly one failure for each remaining token.
        self.endpoint.fence();
        self.engine.set_external_obligations(1);
        let now = self.handle.now();
        self.engine
            .fail_producer_at(now, FailureReason::RuntimeFailed);
        if !self.routing_ingress.is_empty() {
            // This slice precedes the retained submission tail in dense token
            // order, including when runtime failure interrupts collection.
            self.routing.reject_records(self.routing_ingress.len());
            let _ = self.engine.admit_records_buffered(
                now,
                &mut self.routing_ingress,
                self.routing.choices(),
            );
        }
        if let Some(records) = self.pending_submission.take() {
            let records: Vec<_> = records.collect();
            let choices = vec![crate::routing::PartitionChoice::Partition(-1); records.len()];
            let _ = self.engine.admit_records(now, records, &choices);
        }
        while let Ok(Some(command)) = self.endpoint.pop_command() {
            match command {
                Command::Submit(batch) => {
                    let choices =
                        vec![crate::routing::PartitionChoice::Partition(-1); batch.records.len()];
                    let _ = self.engine.admit(now, batch, &choices);
                }
                Command::OpenTopic {
                    handle,
                    name,
                    at,
                    credit,
                } => {
                    let _ = self
                        .engine
                        .open_topic_reserved_credit(handle, &name, at, credit);
                }
                Command::Flush { .. } => self.fences.push_back(command),
                _ => {}
            }
        }
        while let Some(command) = self.fences.pop_front() {
            if let Command::Flush {
                token,
                at,
                watermark,
                credit,
            } = command
            {
                let _ = self.engine.flush_reserved(token, watermark, at, credit);
            }
        }
        self.endpoint.inputs().close();
        self.control.stop();
        // The runtime cannot schedule another cooperative poll after this Drop.
        // Finish only local failure/record ownership in bounded chunks here;
        // BatchAbort never waits for provider-held clones to retire. A None from
        // pop_event may mean payload cleanup made progress without a Delivery.
        let budget = WorkBudget {
            bytes: 1,
            items: self.engine.config().max_completions_per_poll.max(1),
        };
        loop {
            self.endpoint.inputs().close_step(budget.items as usize);
            if self.engine.has_maintenance_work() {
                self.engine.on_deadline(now, budget);
            }
            if self.engine.has_terminal_work() {
                self.engine.encode(now, budget);
            }
            while let Some(event) = self.engine.pop_event() {
                let _ = self.endpoint.publish(event);
            }
            if !self.engine.has_maintenance_work()
                && !self.engine.has_terminal_work()
                && !self.endpoint.inputs().has_close_work()
            {
                break;
            }
        }
        while let Some(event) = self.endpoint.inputs().pop_released() {
            let _ = self.endpoint.publish(event);
        }
        // No normal Closed event: admitted provider operations can still own
        // allocation guards. Late release notifications remain in Shared and
        // synchronous poll_events drains them after physical release.
        self.endpoint.aborted();
    }
}

#[cfg(test)]
mod io_tests;
#[cfg(test)]
mod routing_tests;
