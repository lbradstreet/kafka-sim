//! One bounded bootstrap/control connection owned by the producer actor.
//!
//! `prepare` creates cold setup futures and immutable requests; `poll_event`
//! observes at most one event. Setup and transport operations survive deadlines
//! and stop until their actual terminal points. The caller applies each owned
//! metadata snapshot before preparing more work.
//!
//! Queue admission visits only the supplied handles, using a preallocated AVL
//! membership index; the actor supplies singleton engine orders. Request assembly
//! charges each selected handle and each selector snapshot to `WorkBudget`.
//! Both zero quotas are inert. The actor re-polls unfinished preparation and
//! parks while setup, an admitted request, backoff, or throttle blocks it.
//!
//! Remaining control-frame quanta are explicit: final encoding and response
//! parsing are synchronous, bounded by `request_handles`, `request_bytes`, the
//! RX frame limit, and `ControlLimits` owned/partition/broker limits. Selector
//! validation uses sorted indexes in `ControlCodec`; terminal membership
//! release is O(request_handles * log(max_open_topics)). These are
//! not claims of item-budgeted parsing or codec preemption. No queue-sized
//! remainder copy, queue search, or per-handle queue allocation occurs here.
use crate::{
    config::{BrokerEndpoint, ProducerConfig, SecurityConfig},
    connector::{
        ConnectError, ConnectTarget, Connector, DATA_SETUP_BYTES, DATA_SETUP_RESERVED_BYTES,
    },
    control::{Capabilities, ControlCodec, ControlError, ControlLimits, MetadataUpdate},
    credit::{Claim, HeldCredits, Resource, SharedCredits},
    topic::{MetadataSelector, TopicCache, TopicState},
    transport::{
        ConnectionDriver, DriverConfig, DriverEvent, OwnedSendPlan, RetireReason, SendRequest,
        WriteMode,
    },
    types::{FailureReason, ProducerIdentity, Progress, TopicHandle, TopicId, WorkBudget},
};
use kr_kafka_protocol::errors as code;
use kr_runtime::{RuntimeDuration, RuntimeInstant};
use std::{
    future::Future,
    mem::size_of,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

mod queue;
use queue::{Queue, Queued};

type Result<T> = std::result::Result<T, FailureReason>;

#[derive(Debug)]
pub(super) enum ControlEvent {
    Metadata {
        handles: Vec<TopicHandle>,
        update: MetadataUpdate,
    },
    Identity(ProducerIdentity),
    MetadataFailed {
        handles: Vec<TopicHandle>,
    },
    Fatal(FailureReason),
    Progress,
    Released,
}
enum WorkKind {
    Metadata(Vec<TopicHandle>),
    Identity(Option<ProducerIdentity>),
}
enum Selector {
    Name(String),
    Id(TopicId),
}
impl Selector {
    fn borrowed(&self) -> MetadataSelector<'_> {
        match self {
            Self::Name(name) => MetadataSelector::Name(name),
            Self::Id(id) => MetadataSelector::Id(*id),
        }
    }
}
struct Work {
    kind: WorkKind,
    deadline: Option<RuntimeInstant>,
    attempts: u8,
    attempt_started: bool,
    selectors: Vec<Selector>,
    target_handles: usize,
    correlation: Option<i32>,
}
impl Work {
    fn new(kind: WorkKind) -> Self {
        Self {
            kind,
            deadline: None,
            attempts: 0,
            attempt_started: false,
            selectors: Vec::new(),
            target_handles: 0,
            correlation: None,
        }
    }
    fn reset_attempt(&mut self) {
        self.attempt_started = false;
        self.selectors.clear();
        self.correlation = None;
    }
}
struct Setup<F> {
    future: Pin<Box<F>>,
    deadline: RuntimeInstant,
    polled: bool,
    expired: bool,
}

pub(super) struct ControlPlane<C: Connector> {
    codec: ControlCodec,
    bootstrap: Vec<BrokerEndpoint>,
    bootstrap_index: usize,
    driver_config: DriverConfig,
    require_sasl: bool,
    request_timeout: RuntimeDuration,
    work_timeout: RuntimeDuration,
    backoff_min: RuntimeDuration,
    backoff_max: RuntimeDuration,
    max_attempts: u8,
    request_handles: usize,
    request_bytes: usize,
    queued: Queue,
    current: Option<Work>,
    connecting: Option<Setup<C::ConnectFuture>>,
    driver: Option<ConnectionDriver<C::Stream>>,
    next_correlation: i32,
    retry_at: Option<RuntimeInstant>,
    throttle_until: Option<RuntimeInstant>,
    retry_after_release: bool,
    pending: Option<ControlEvent>,
    stopping: bool,
    released: bool,
    // Last: all adapter buffers, requests and futures release before this arena.
    arena: Option<Arc<HeldCredits>>,
}

impl<C: Connector> ControlPlane<C> {
    /// Retained response work shares the arena until its final owned snapshot
    /// and engine application state have drained, including after control stop.
    pub(super) fn working_guard(&self) -> Option<Arc<HeldCredits>> {
        self.arena.clone()
    }
    pub(super) fn new(
        config: &ProducerConfig,
        credits: SharedCredits,
        mut driver: DriverConfig,
    ) -> Result<Self> {
        if config.bootstrap.is_empty()
            || config.bootstrap.len() > config.brokers_max as usize
            || config.max_open_topics == 0
            || config.max_attempts == 0
            || config.retry_backoff_min > config.retry_backoff_max
        {
            return Err(FailureReason::ResourceExhausted);
        }
        // The control connection does not borrow data RX, staging, request or
        // wire-window pools. Its complete working set occupies this reservation.
        let arena_bytes = config
            .control_reserve_bytes
            .checked_sub(DATA_SETUP_RESERVED_BYTES)
            .ok_or(FailureReason::ResourceExhausted)?;
        let arena = Arc::new(
            credits
                .reserve(&[Claim {
                    resource: Resource::ControlReserve,
                    amount: arena_bytes,
                    lane: 0,
                }])
                .map_err(|_| FailureReason::ResourceExhausted)?,
        );
        driver.max_inflight_requests = 1;
        let require_sasl = matches!(config.security, SecurityConfig::SaslTls { .. });
        let tls = if matches!(config.security, SecurityConfig::Plaintext) {
            0
        } else {
            (config.tls_plaintext_bytes as usize)
                .checked_add(config.tls_ciphertext_bytes as usize)
                .ok_or(FailureReason::ResourceExhausted)?
        };
        let queue_bytes = Queue::storage_bytes(config.max_open_topics as usize)
            .and_then(|n| {
                n.checked_add(
                    (config.max_open_topics as usize).checked_mul(4 * size_of::<TopicHandle>())?,
                )
            })
            .and_then(|n| n.checked_add(4 * size_of::<Work>() + 8192))
            .ok_or(FailureReason::ResourceExhausted)?;
        let endpoint_bytes = config
            .bootstrap
            .iter()
            .try_fold(config.client_id.len(), |sum, endpoint| {
                if endpoint.host.len() > 4096 {
                    return None;
                }
                sum.checked_add(endpoint.host.len() + size_of::<BrokerEndpoint>())
            })
            .ok_or(FailureReason::ResourceExhausted)?;
        let fixed = driver
            .rx_bytes
            .checked_add(if driver.mode == WriteMode::Staging {
                driver.staging_bytes
            } else {
                0
            })
            .and_then(|n| n.checked_add(tls))
            .and_then(|n| n.checked_add(queue_bytes))
            .and_then(|n| n.checked_add(endpoint_bytes))
            .and_then(|n| n.checked_add(DATA_SETUP_BYTES))
            .ok_or(FailureReason::ResourceExhausted)?;
        let working = arena_bytes
            .checked_sub(fixed)
            .filter(|n| *n >= 32 * 1024)
            .ok_or(FailureReason::ResourceExhausted)?;
        let request_bytes = working / 4;
        let request_handles = (request_bytes / 512)
            .max(1)
            .min(config.max_open_topics as usize);
        let mut limits = ControlLimits::from_config(config);
        limits.frame_bytes = driver.rx_bytes;
        limits.owned_bytes = working / 2;
        limits.topics = request_handles;
        limits.partitions = limits
            .partitions
            .min(limits.owned_bytes / size_of::<crate::control::MetadataPartition>());
        let codec = ControlCodec::new(config.client_id.clone(), require_sasl, limits)
            .map_err(|_| FailureReason::ResourceExhausted)?;
        let queued = Queue::new(config.max_open_topics as usize)?;
        Ok(Self {
            codec,
            bootstrap: config.bootstrap.clone(),
            bootstrap_index: 0,
            driver_config: driver,
            require_sasl,
            request_timeout: config.request_timeout,
            work_timeout: config.delivery_timeout,
            backoff_min: config.retry_backoff_min,
            backoff_max: config.retry_backoff_max,
            max_attempts: config.max_attempts,
            request_handles,
            request_bytes,
            queued,
            current: None,
            connecting: None,
            driver: None,
            next_correlation: 0,
            retry_at: None,
            throttle_until: None,
            retry_after_release: false,
            pending: None,
            stopping: false,
            released: false,
            arena: Some(arena),
        })
    }
    pub(super) fn queue_metadata(&mut self, handles: Vec<TopicHandle>) -> Result<()> {
        if self.stopping {
            return Err(FailureReason::Closed);
        }
        self.queued.push_metadata(handles)
    }
    pub(super) fn queue_identity(&mut self, previous: Option<ProducerIdentity>) -> Result<()> {
        if self.stopping {
            return Err(FailureReason::Closed);
        }
        if previous.is_some_and(|id| !id.is_valid()) {
            return Err(FailureReason::ProtocolViolation);
        }
        let current = self.current.as_ref().and_then(|work| match work.kind {
            WorkKind::Identity(identity) => Some(identity),
            WorkKind::Metadata(_) => None,
        });
        if let Some(old) = current.or_else(|| self.queued.identity()) {
            return if old == previous {
                Ok(())
            } else {
                Err(FailureReason::ProtocolViolation)
            };
        }
        self.queued.push_identity(previous)
    }
    /// Creates cold work under an explicit handle/selector visit quota. Each
    /// visited handle consumes an item, including a missing/failed topic. One
    /// selector may exceed a nonzero byte quantum by its bounded name length.
    /// Encoding the final request remains one control-frame quantum bounded by
    /// request_bytes/request_handles; it never copies the queued remainder.
    pub(super) fn prepare(
        &mut self,
        now: RuntimeInstant,
        connector: &mut C,
        topics: &TopicCache,
        budget: WorkBudget,
    ) -> Result<Progress> {
        let mut progress = Progress::default();
        while progress.items < budget.items
            && progress.bytes < budget.bytes
            && self.preparation_ready(now)
        {
            if self.current.is_none() {
                let Some(queued) = self.queued.take(self.request_handles) else {
                    break;
                };
                let mut work = match queued {
                    Queued::Metadata(count) => {
                        let mut handles = Vec::new();
                        handles
                            .try_reserve_exact(count)
                            .map_err(|_| FailureReason::ResourceExhausted)?;
                        let mut work = Work::new(WorkKind::Metadata(handles));
                        work.target_handles = count;
                        work.selectors
                            .try_reserve_exact(count)
                            .map_err(|_| FailureReason::ResourceExhausted)?;
                        work
                    }
                    Queued::Identity(previous) => Work::new(WorkKind::Identity(previous)),
                };
                work.deadline = Some(
                    now.checked_add(self.work_timeout)
                        .ok_or(FailureReason::RuntimeFailed)?,
                );
                self.current = Some(work);
                progress.items += 1;
                continue;
            }
            let work = self.current.as_mut().unwrap();
            if let WorkKind::Metadata(handles) = &mut work.kind
                && handles.len() < work.target_handles
            {
                let handle = self.queued.pop_handle();
                handles.push(handle);
                if let Ok(topic) = topics.get(handle)
                    && topic.state == TopicState::Resolving
                {
                    work.deadline = Some(work.deadline.unwrap().min(topic.resolution_deadline));
                }
                progress.items += 1;
                progress.bytes = progress
                    .bytes
                    .saturating_add(size_of::<TopicHandle>() as u32);
                continue;
            }
            if now >= work.deadline.unwrap()
                || work.attempts >= self.max_attempts && !work.attempt_started
            {
                self.exhaust(FailureReason::Deadline);
                progress.items += 1;
                break;
            }
            self.retry_at = None;
            self.throttle_until = None;
            if !work.attempt_started {
                work.attempts += 1;
                work.attempt_started = true;
            }
            let deadline = now
                .checked_add(self.request_timeout)
                .ok_or(FailureReason::RuntimeFailed)?
                .min(work.deadline.unwrap());
            if self.driver.is_none() {
                let target = ConnectTarget {
                    endpoint: self.bootstrap[self.bootstrap_index].clone(),
                    broker_id: None,
                    lane: 0,
                    deadline,
                    driver: self.driver_config,
                    lifetime_guard: self
                        .arena
                        .as_ref()
                        .map(|guard| guard.clone() as Arc<dyn Send + Sync>),
                };
                self.bootstrap_index = (self.bootstrap_index + 1) % self.bootstrap.len();
                self.connecting = Some(Setup {
                    future: Box::pin(connector.connect(target)),
                    deadline,
                    polled: false,
                    expired: false,
                });
                progress.items += 1;
                break;
            }
            if let WorkKind::Metadata(handles) = &work.kind
                && work.selectors.len() < handles.len()
            {
                let selector = match topics.selector(handles[work.selectors.len()]) {
                    Ok(MetadataSelector::Name(name)) => {
                        progress.bytes = progress.bytes.saturating_add(name.len() as u32);
                        Selector::Name(name.to_owned())
                    }
                    Ok(MetadataSelector::Id(id)) => {
                        progress.bytes = progress.bytes.saturating_add(16);
                        Selector::Id(id)
                    }
                    Err(_) => {
                        self.exhaust(FailureReason::TopicResolution);
                        progress.items += 1;
                        break;
                    }
                };
                work.selectors.push(selector);
                progress.items += 1;
                continue;
            }
            if self.next_correlation == i32::MAX {
                self.driver
                    .as_mut()
                    .unwrap()
                    .retire(RetireReason::Requested);
                self.retry_after_release = true;
                progress.items += 1;
                break;
            }
            let correlation = self.next_correlation;
            self.next_correlation += 1;
            let frame = match &work.kind {
                WorkKind::Identity(previous) => {
                    self.codec.init_producer_id_request(correlation, *previous)
                }
                WorkKind::Metadata(_) => {
                    let selectors: Vec<_> = work.selectors.iter().map(Selector::borrowed).collect();
                    self.codec.metadata_request(correlation, &selectors)
                }
            }
            .map_err(control_failure)?;
            progress.items += 1;
            progress.bytes = progress.bytes.saturating_add(frame.len() as u32);
            let mut plan = OwnedSendPlan::from_frame(frame, self.request_bytes)
                .map_err(|_| FailureReason::ResourceExhausted)?;
            plan.retain_metadata_guard(self.arena.as_ref().unwrap().clone());
            self.driver
                .as_mut()
                .unwrap()
                .enqueue(SendRequest {
                    correlation,
                    deadline,
                    plan,
                })
                .map_err(|_| FailureReason::ProtocolViolation)?;
            work.correlation = Some(correlation);
            break;
        }
        progress.remaining_immediate = self.preparation_ready(now);
        Ok(progress)
    }
    fn preparation_ready(&self, now: RuntimeInstant) -> bool {
        if self.stopping
            || self.pending.is_some()
            || self.connecting.is_some()
            || self
                .driver
                .as_ref()
                .is_some_and(ConnectionDriver::is_retiring)
        {
            return false;
        }
        let Some(work) = &self.current else {
            return !self.queued.is_empty();
        };
        if work.correlation.is_some() {
            return false;
        }
        // A partially selected FIFO group must finish its bounded handle visits
        // before it can report all associated failures, even if its deadline is due.
        if matches!(&work.kind, WorkKind::Metadata(handles) if handles.len() < work.target_handles)
            || now >= work.deadline.unwrap()
        {
            return true;
        }
        self.retry_at.is_none_or(|at| now >= at) && self.throttle_until.is_none_or(|at| now >= at)
    }
    pub(super) fn poll_event(
        &mut self,
        cx: &mut Context<'_>,
        now: RuntimeInstant,
        _connector: &mut C,
        _topics: &TopicCache,
    ) -> Poll<ControlEvent> {
        if let Some(event) = self.pending.take() {
            return Poll::Ready(event);
        }
        if self
            .connecting
            .as_ref()
            .is_some_and(|setup| !setup.polled && now >= setup.deadline)
        {
            // A setup future is cold until its first poll. Expiration before
            // that admission point needs no transport drain and must not start IO.
            self.connecting = None;
            self.retry(now);
            return Poll::Ready(self.pending.take().unwrap_or(ControlEvent::Progress));
        }
        if let Some(setup) = &mut self.connecting {
            setup.polled = true;
            match setup.future.as_mut().poll(cx) {
                Poll::Ready(result) => {
                    let setup = self.connecting.take().unwrap();
                    match result {
                        Ok(mut connected) => {
                            if Capabilities::from_advertised(
                                &connected.capabilities,
                                self.require_sasl,
                            )
                            .is_err()
                                || connected.driver.pending_requests() != 0
                            {
                                connected.driver.retire(RetireReason::Requested);
                                self.driver = Some(connected.driver);
                                self.fatal(FailureReason::ProtocolViolation);
                            } else if self.stopping || setup.expired || now >= setup.deadline {
                                connected.driver.retire(RetireReason::Requested);
                                self.driver = Some(connected.driver);
                                self.retry_after_release = !self.stopping;
                            } else {
                                self.driver = Some(connected.driver);
                                self.next_correlation = 0;
                            }
                        }
                        Err(error) => {
                            if !self.stopping {
                                match connect_failure(error) {
                                    Err(reason) => self.fatal(reason),
                                    Ok(_) => self.retry(now),
                                }
                            }
                        }
                    }
                    return Poll::Ready(self.pending.take().unwrap_or(ControlEvent::Progress));
                }
                Poll::Pending => {
                    if !setup.expired && now >= setup.deadline {
                        setup.expired = true;
                        return Poll::Ready(ControlEvent::Progress);
                    }
                    return Poll::Pending;
                }
            }
        }
        if let Some(driver) = &mut self.driver {
            let step = match driver.poll_event(cx, now) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(None) => DriverStep::Released,
                Poll::Ready(Some(DriverEvent::Released)) => DriverStep::Released,
                Poll::Ready(Some(DriverEvent::Retiring { reason })) => DriverStep::Retiring(reason),
                Poll::Ready(Some(DriverEvent::Frame { correlation, bytes })) => {
                    if self.stopping {
                        DriverStep::Progress
                    } else {
                        DriverStep::Frame(parse_frame(
                            &self.codec,
                            self.current.as_ref(),
                            correlation,
                            bytes,
                        ))
                    }
                }
                Poll::Ready(Some(
                    DriverEvent::WriteAdmitted { .. }
                    | DriverEvent::WriteProgress { .. }
                    | DriverEvent::RequestRetired { .. },
                )) => DriverStep::Progress,
            };
            match step {
                DriverStep::Frame(Ok(parsed)) => {
                    let throttle = match &parsed {
                        Parsed::Metadata(update) => update.throttle_ms,
                        Parsed::Identity(response) => response.throttle_ms,
                    };
                    self.throttle_until = (throttle != 0)
                        .then(|| {
                            now.checked_add(RuntimeDuration::from_nanos(
                                throttle as u64 * 1_000_000,
                            ))
                        })
                        .flatten();
                    match parsed {
                        Parsed::Metadata(update) => {
                            let WorkKind::Metadata(handles) = self.current.take().unwrap().kind
                            else {
                                unreachable!()
                            };
                            self.queued.complete(&handles);
                            return Poll::Ready(ControlEvent::Metadata { handles, update });
                        }
                        Parsed::Identity(response) => {
                            if let Some(identity) = response.identity {
                                self.current = None;
                                return Poll::Ready(ControlEvent::Identity(identity));
                            }
                            if retry_identity(response.error_code) {
                                self.retry(now);
                            } else {
                                self.fatal(
                                    if matches!(
                                        response.error_code,
                                        code::CLUSTER_AUTHORIZATION_FAILED
                                            | code::SASL_AUTHENTICATION_FAILED
                                    ) {
                                        FailureReason::Authentication
                                    } else if matches!(
                                        response.error_code,
                                        code::PRODUCER_FENCED | code::INVALID_PRODUCER_EPOCH
                                    ) {
                                        FailureReason::ProducerFenced
                                    } else {
                                        FailureReason::BrokerRejected
                                    },
                                );
                            }
                        }
                    }
                }
                DriverStep::Frame(Err(reason)) => self.fatal(reason),
                DriverStep::Retiring(reason) => {
                    if matches!(
                        reason,
                        RetireReason::Protocol(_) | RetireReason::Transport(_)
                    ) {
                        self.fatal(FailureReason::ProtocolViolation);
                    } else {
                        self.retry_after_release = !self.stopping;
                    }
                }
                DriverStep::Released => {
                    self.driver = None;
                    if self.retry_after_release && !self.stopping {
                        self.retry_after_release = false;
                        self.retry(now);
                    }
                }
                DriverStep::Progress => {}
            }
            return Poll::Ready(self.pending.take().unwrap_or(ControlEvent::Progress));
        }
        if self.stopping && !self.released {
            self.arena = None;
            self.released = true;
            return Poll::Ready(ControlEvent::Released);
        }
        Poll::Pending
    }
    fn retry(&mut self, now: RuntimeInstant) {
        let Some(work) = &mut self.current else {
            return;
        };
        work.reset_attempt();
        if now >= work.deadline.unwrap() || work.attempts >= self.max_attempts {
            self.exhaust(FailureReason::Deadline);
            return;
        }
        let multiplier = 1u64
            .checked_shl(work.attempts.saturating_sub(1) as u32)
            .unwrap_or(u64::MAX);
        let delay = self
            .backoff_min
            .as_nanos()
            .saturating_mul(multiplier)
            .min(self.backoff_max.as_nanos());
        self.retry_at = Some(
            now.checked_add(RuntimeDuration::from_nanos(delay))
                .unwrap_or(work.deadline.unwrap())
                .min(work.deadline.unwrap()),
        );
    }
    fn exhaust(&mut self, reason: FailureReason) {
        if let Some(work) = self.current.take() {
            match work.kind {
                WorkKind::Metadata(handles) => {
                    self.queued.complete(&handles);
                    self.pending = Some(ControlEvent::MetadataFailed { handles })
                }
                WorkKind::Identity(_) => self.fatal(reason),
            }
        }
    }
    fn fatal(&mut self, reason: FailureReason) {
        self.stop();
        self.pending = Some(ControlEvent::Fatal(reason));
    }
    pub(super) fn next_deadline(&self) -> Option<RuntimeInstant> {
        if self.stopping {
            return None;
        }
        if let Some(setup) = &self.connecting {
            return (!setup.expired).then_some(setup.deadline);
        }
        if let Some(driver) = &self.driver {
            if driver.is_retiring() {
                return None;
            }
            if let Some(deadline) = driver.next_deadline() {
                return Some(deadline);
            }
        }
        // An idle negotiated connection has no policy deadline; an old throttle
        // must not keep the actor runnable at the same virtual instant.
        let deadline = self.current.as_ref()?.deadline;
        let wake = self.retry_at.into_iter().chain(self.throttle_until).max();
        deadline.into_iter().chain(wake).min()
    }
    pub(super) fn stop(&mut self) {
        self.stopping = true;
        self.queued.clear();
        self.current = None;
        self.pending = None;
        self.retry_at = None;
        self.throttle_until = None;
        if self.connecting.as_ref().is_some_and(|setup| !setup.polled) {
            self.connecting = None;
        }
        if let Some(driver) = &mut self.driver {
            driver.retire(RetireReason::Requested);
        }
    }
    pub(super) fn obligations(&self) -> usize {
        self.queued.len()
            + usize::from(self.current.is_some())
            + usize::from(self.connecting.is_some())
            + usize::from(self.driver.is_some())
            + usize::from(self.arena.is_some())
            + usize::from(self.pending.is_some())
    }
}
enum Parsed {
    Metadata(MetadataUpdate),
    Identity(crate::control::IdentityResponse),
}
enum DriverStep {
    Frame(Result<Parsed>),
    Retiring(RetireReason),
    Released,
    Progress,
}
fn parse_frame(
    codec: &ControlCodec,
    work: Option<&Work>,
    correlation: i32,
    bytes: &[u8],
) -> Result<Parsed> {
    let work = work.ok_or(FailureReason::ProtocolViolation)?;
    if work.correlation != Some(correlation) {
        return Err(FailureReason::ProtocolViolation);
    }
    match &work.kind {
        WorkKind::Metadata(_) => {
            let selectors: Vec<_> = work.selectors.iter().map(Selector::borrowed).collect();
            codec
                .parse_metadata(bytes, correlation, &selectors)
                .map(Parsed::Metadata)
                .map_err(control_failure)
        }
        WorkKind::Identity(_) => codec
            .parse_identity(bytes, correlation)
            .map(Parsed::Identity)
            .map_err(control_failure),
    }
}
fn control_failure(error: ControlError) -> FailureReason {
    if matches!(error, ControlError::Limit(_)) {
        FailureReason::ResourceExhausted
    } else {
        FailureReason::ProtocolViolation
    }
}
fn connect_failure(error: ConnectError) -> std::result::Result<FailureReason, FailureReason> {
    match error {
        ConnectError::Authentication => Err(FailureReason::Authentication),
        ConnectError::Protocol(_) | ConnectError::InvalidConfiguration => {
            Err(FailureReason::ProtocolViolation)
        }
        ConnectError::WorkerFailed => Err(FailureReason::RuntimeFailed),
        ConnectError::TransportUnavailable => Err(FailureReason::Transport),
        ConnectError::Network(_) | ConnectError::Timeout => Ok(FailureReason::Transport),
        ConnectError::ResourceExhausted => Ok(FailureReason::ResourceExhausted),
        // New shared setup failures need an explicit producer policy.
        _ => Err(FailureReason::ProtocolViolation),
    }
}
fn retry_identity(error: i16) -> bool {
    matches!(
        error,
        code::COORDINATOR_LOAD_IN_PROGRESS
            | code::COORDINATOR_NOT_AVAILABLE
            | code::NOT_COORDINATOR
            | code::CONCURRENT_TRANSACTIONS
            | code::REQUEST_TIMED_OUT
            | code::NETWORK_EXCEPTION
    )
}

#[cfg(test)]
#[path = "control_tests.rs"]
mod tests;
