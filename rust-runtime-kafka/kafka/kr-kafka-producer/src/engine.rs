//! Passive producer state machine. All time, routing choices, random draws and
//! I/O outcomes arrive from its owner; this module creates no tasks or sockets.
use crate::{
    accumulator::{AppendError, ArrivalRate, Batch, BatchKey, BatchState},
    admission::{AdmittedRecord, RecordObligation, SubmissionBatch},
    config::{Compression, ConfigError, ProducerConfig, ValidatedConfig},
    control::{BrokerNode, ControlCodec, ControlError, MetadataUpdate},
    credit::{Claim, CreditError, HeldCredits, Resource, SharedCredits},
    lifecycle::{DeliveryTracker, EventEnvelope, EventQueue, LifecycleError},
    pool::{Pool, PoolError, Slot},
    routing::{PartitionChoice, PartitionSnapshot},
    sequence::{
        BrokerOutcome, LedgerChange, LedgerError, ProducerLedger, RecoveryState, TerminalBatch,
    },
    topic::{PartitionMetadata, TopicCache, TopicError, TopicState},
    transport::{OwnedSendPlan, PlanLimits, RetireReason, TransportError},
    types::*,
};
use kr_kafka_protocol::{
    errors as code,
    plan::EncodeLimits,
    wire::{Records, Sequence as WireSequence},
};
use kr_kafka_record::{CodecPool, OutputPool, ZstdConfig};
use kr_runtime::{CompletionCertainty, RuntimeDuration, RuntimeInstant};
use std::{
    collections::{BTreeMap, BTreeSet, VecDeque},
    fmt,
    sync::Arc,
};

mod admission;
mod deadlines;
mod dispatch;
mod dispatch_queue;
mod dispatch_schedule;
mod dispatch_size;
mod dispatch_work;
mod encoding;
mod encoding_queue;
mod estimation;
mod lifecycle;
pub(crate) mod memory;
mod metadata;
mod metrics;
mod partition_cleanup;
mod queue;
mod recovery;
mod request_gather;
mod retry;
mod terminal_order;
mod topic_settlement;
use queue::{BatchQueue, OrderQueue, RecordQueue};

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct ConnectionKey(pub u64);
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct RequestKey(pub u64);

#[derive(Clone, Debug)]
pub enum EngineError {
    Config(ConfigError),
    Credit(CreditError),
    Topic(TopicError),
    Ledger(LedgerError),
    Control(ControlError),
    Record(kr_kafka_record::Error),
    Transport(TransportError),
    Pool(PoolError),
    Lifecycle(LifecycleError),
    StaleConnection,
    StaleRequest,
    InvalidState(&'static str),
    Closed,
    AllocationFailed,
}
impl fmt::Display for EngineError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "producer engine: {self:?}")
    }
}
impl std::error::Error for EngineError {}
macro_rules! error_from {
    ($ty:ty,$variant:ident) => {
        impl From<$ty> for EngineError {
            fn from(error: $ty) -> Self {
                Self::$variant(error)
            }
        }
    };
}
error_from!(ConfigError, Config);
error_from!(CreditError, Credit);
error_from!(TopicError, Topic);
error_from!(LedgerError, Ledger);
error_from!(ControlError, Control);
error_from!(kr_kafka_record::Error, Record);
error_from!(TransportError, Transport);
error_from!(PoolError, Pool);
error_from!(LifecycleError, Lifecycle);
pub type Result<T> = std::result::Result<T, EngineError>;

#[derive(Debug)]
pub enum EngineOrder {
    Connect {
        key: ConnectionKey,
        node: BrokerNode,
        lane: u8,
        lifetime_guard: Arc<HeldCredits>,
    },
    Metadata {
        handles: Vec<TopicHandle>,
    },
    InitProducerId {
        previous: Option<ProducerIdentity>,
    },
    Retire {
        connection: ConnectionKey,
        reason: RetireReason,
    },
    Dispatch {
        connection: ConnectionKey,
        request: RequestKey,
        correlation: i32,
        plan: OwnedSendPlan,
        deadline: RuntimeInstant,
    },
}
#[derive(Clone, Debug)]
pub enum ConnectionEvent {
    Active,
    Retiring { reason: RetireReason },
    Released,
}
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct Admitted {
    pub records: u32,
    pub failed: u32,
    pub pending: u32,
}
pub use crate::accumulator::BatchSealStats;
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct EngineStatus {
    pub batch_seals: BatchSealStats,
    pub accepted: u64,
    pub terminal: u64,
    /// Lifetime Unknown deliveries, bounded by the u64 admission token space.
    pub unknown: u64,
    pub pending_records: usize,
    pub batches: usize,
    pub requests: usize,
    pub connections: usize,
    pub queued_events: usize,
    pub queued_orders: usize,
    pub deadlines: usize,
    pub closing: bool,
    pub closed: bool,
    pub failed: bool,
    pub identity: Option<ProducerIdentity>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ConnectionPhase {
    Connecting,
    Active,
    Retiring,
}
struct ConnectionState {
    retry_scheduled: bool,
    broker: i32,
    lane: u8,
    phase: ConnectionPhase,
    fifo: VecDeque<RequestKey>,
    pending_orders: [Option<usize>; 3],
    writing: Option<RequestKey>,
    bytes: usize,
    credit: Arc<HeldCredits>,
}
struct RequestState {
    connection: ConnectionKey,
    correlation: i32,
    batches: Vec<BatchKey>,
    partitions: Vec<TopicPartition>,
    deadline: RuntimeInstant,
    bytes: usize,
    confirmed: usize,
    sent_at: Option<RuntimeInstant>,
    admitted: bool,
    certainty: CompletionCertainty,
    attempt: u64,
    initial_write_unresolved: bool,
    _credit: HeldCredits,
}
struct PartitionQueue {
    compression_estimate: crate::batching::CompressionEstimate,
    metrics_scope: crate::telemetry::metrics::ScopeToken,
    lane: u8,
    records: RecordQueue,
    batches: BatchQueue,
    batch_bytes: u64,
    request_owners: usize,
    terminal_owners: usize,
    batch_ages: BTreeSet<(RuntimeInstant, BatchKey)>,
    arrival: ArrivalRate,
    deficit: usize,
    retry_at: RuntimeInstant,
    drain_bytes_per_second: u64,
    last_drain: Option<RuntimeInstant>,
}
struct BrokerState {
    metrics_scope: crate::telemetry::metrics::ScopeToken,
    node: BrokerNode,
    bytes: usize,
    requests: usize,
    throttle_until: RuntimeInstant,
    round_trip: crate::estimation::RoundTripTime,
}
struct FlushFence {
    token: FlushToken,
    watermark: RecordToken,
    credit: HeldCredits,
}
struct SealSweep {
    cursor: usize,
    end: usize,
    watermark: RecordToken,
    reason: SealReason,
}
struct CloseFence {
    deadline: RuntimeInstant,
    watermark: RecordToken,
}
struct TerminalRecords {
    payload: Option<crate::accumulator::TerminalPayload>,
    terminal: TerminalBatch,
    records: VecDeque<RecordObligation>,
    index: u32,
}
struct TopicRecords {
    records: BTreeMap<RecordToken, Option<TopicPartition>>,
    captured_id: Option<TopicId>,
    write_fence: crate::transport::WriteFence,
    reason: Option<FailureReason>,
    cursor: Option<RecordToken>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd)]
enum DeadlineKey {
    Batch(u64),
    Pending(RecordToken),
    Topic(TopicHandle),
    Request(RequestKey),
    Retry(TopicPartition),
    Broker(i32),
    Close,
}
#[derive(Default)]
struct Deadlines {
    ordered: BTreeSet<(RuntimeInstant, DeadlineKey)>,
    current: BTreeMap<DeadlineKey, RuntimeInstant>,
}
impl Deadlines {
    fn set(&mut self, key: DeadlineKey, at: RuntimeInstant) {
        self.remove(key);
        self.ordered.insert((at, key));
        self.current.insert(key, at);
    }
    fn remove(&mut self, key: DeadlineKey) {
        if let Some(at) = self.current.remove(&key) {
            self.ordered.remove(&(at, key));
        }
    }
    fn next(&self) -> Option<RuntimeInstant> {
        self.ordered.first().map(|(at, _)| *at)
    }
    fn due(&mut self, now: RuntimeInstant) -> Option<DeadlineKey> {
        let &(at, key) = self.ordered.first()?;
        if at > now {
            return None;
        }
        self.remove(key);
        Some(key)
    }
}

pub struct ProducerEngine {
    retry: retry::RetryWork,
    metrics: metrics::EngineMetrics,
    config: ProducerConfig,
    validated: ValidatedConfig,
    credits: SharedCredits,
    topics: TopicCache,
    topic_event_credits: BTreeMap<TopicHandle, HeldCredits>,
    metadata_pending: BTreeSet<TopicHandle>,
    metadata_work: Option<metadata::MetadataWork>,
    metadata_error: Option<EngineError>,
    brokers: BTreeMap<i32, BrokerState>,
    partitions: BTreeMap<TopicPartition, PartitionQueue>,
    pending: BTreeMap<RecordToken, AdmittedRecord>,
    pending_counts: BTreeMap<TopicHandle, usize>,
    pending_routes: BTreeMap<(TopicHandle, Option<i32>), BTreeSet<RecordToken>>,
    batched_locations: BTreeMap<RecordToken, BatchKey>,
    batch_requests: BTreeMap<BatchKey, RequestKey>,
    queued_locations: BTreeMap<RecordToken, TopicPartition>,
    topic_records: BTreeMap<TopicHandle, TopicRecords>,
    settling_topics: BTreeSet<TopicHandle>,
    settlement_cursor: Option<TopicHandle>,
    partition_cleanup: partition_cleanup::PartitionCleanup,
    attempt_counts: BTreeMap<u64, u32>,
    batches: Pool<Batch>,
    connections: Pool<ConnectionState>,
    requests: Pool<RequestState>,
    routes: BTreeMap<(i32, u8), ConnectionKey>,
    ledger: Option<ProducerLedger>,
    codecs: CodecPool,
    output: OutputPool,
    control: ControlCodec,
    events: EventQueue,
    orders: OrderQueue,
    tracker: DeliveryTracker,
    deadlines: Deadlines,
    flushes: VecDeque<FlushFence>,
    terminal_records: VecDeque<TerminalRecords>,
    terminal_order: terminal_order::TerminalOrder,
    next_flush: u64,
    next_correlation: i32,
    next_attempt: u64,
    closed_credit: Option<HeldCredits>,
    fatal_credit: Option<HeldCredits>,
    close: Option<CloseFence>,
    seal_sweep: Option<SealSweep>,
    fence_work: bool,
    close_retire_cursor: usize,
    close_retire_done: bool,
    closed: bool,
    failed: Option<FailureReason>,
    failure_work: bool,
    failure_batch_cursor: usize,
    failure_connection_cursor: usize,
    unknown: u64,
    seal_counter: crate::accumulator::SealCounter,
    identity_pending: bool,
    identity_refresh: Option<recovery::IdentityRefresh>,
    external_obligations: usize,
    retry_jitter: u64,
    encoder: encoding::Encoder,
    scheduler: dispatch_work::Scheduler,
    encoding_cost: crate::estimation::EncodingCost,
    last_encode_work: crate::estimation::EncodeWork,
    last_encode_aborted: bool,
    headroom_sweep: Option<estimation::HeadroomSweep>,
    headroom_first: bool,
}

impl ProducerEngine {
    pub fn new(config: ProducerConfig, identity: Option<ProducerIdentity>) -> Result<Self> {
        let validated = config.validate()?;
        let credits = SharedCredits::with_descriptor_policy(
            validated.credits,
            config.lanes,
            config.descriptor_admission_policy,
        )?;
        let closed_credit = credits.reserve(&[Claim {
            resource: Resource::ControlEvents,
            amount: 1,
            lane: 0,
        }])?;
        let fatal_credit = credits.reserve(&[Claim {
            resource: Resource::ControlEvents,
            amount: 1,
            lane: 0,
        }])?;
        let contexts = if matches!(config.compression, Compression::None) {
            0
        } else {
            config.codec_contexts as usize
        };
        let codecs = CodecPool::new(
            contexts,
            ZstdConfig {
                level: match config.compression {
                    Compression::None => 1,
                    Compression::Zstd { level } => level,
                },
                window_log: config.codec_window_log,
            },
        )?;
        if codecs.status().workspace_bytes > validated.memory.codec_workspace {
            return Err(EngineError::InvalidState(
                "codec workspace exceeds configured bound",
            ));
        }
        let output_slots = (config.max_batches as usize)
            .min(config.compressed_bytes / kr_kafka_record::BATCH_HEADER_BYTES);
        let cached_chunks = output_slots
            .saturating_mul(2)
            .min(config.compressed_bytes / kr_kafka_record::BATCH_HEADER_BYTES);
        let output = OutputPool::with_limits(config.compressed_bytes, output_slots, cached_chunks)?;
        let topics = TopicCache::new(
            config.max_open_topics as usize,
            config.max_batches as usize,
            config.topic_resolve_timeout,
            config.metadata_max_age,
        )?;
        let event_limit = validated.credits[Resource::DeliveryEvents as usize]
            .checked_add(validated.credits[Resource::ReleaseEvents as usize])
            .and_then(|n| n.checked_add(validated.credits[Resource::ControlEvents as usize]))
            .ok_or(EngineError::AllocationFailed)?;
        let order_limit = validated
            .max_connections
            .checked_mul(7)
            .and_then(|n| n.checked_add(config.max_open_topics as usize))
            .and_then(|n| n.checked_add(4))
            .ok_or(EngineError::AllocationFailed)?;
        let orders = OrderQueue::new(order_limit)?;
        let flushes = crate::fixed::try_deque(validated.credits[Resource::ControlEvents as usize])
            .map_err(|_| EngineError::AllocationFailed)?;
        let terminal_records = crate::fixed::try_deque(config.max_batches as usize)
            .map_err(|_| EngineError::AllocationFailed)?;
        let ledger = identity
            .map(|identity| {
                ProducerLedger::new(
                    identity,
                    config.max_batches as usize,
                    config.max_in_flight_per_connection as usize,
                )
            })
            .transpose()?;
        let mut engine = Self {
            retry: retry::RetryWork::default(),
            metrics: metrics::EngineMetrics::new(config.metrics)?,
            encoding_cost: crate::estimation::EncodingCost::new(config.delivery_timeout),
            last_encode_work: crate::estimation::EncodeWork::default(),
            last_encode_aborted: false,
            headroom_sweep: None,
            headroom_first: true,
            control: ControlCodec::from_config(&config)?,
            batches: Pool::new(config.max_batches as usize)?,
            connections: Pool::new(validated.max_connections)?,
            requests: Pool::new(validated.credits[Resource::RequestSlots as usize])?,
            tracker: DeliveryTracker::new(config.record_descriptors as usize),
            events: EventQueue::new(event_limit)?,
            config,
            validated,
            credits,
            topics,
            topic_event_credits: BTreeMap::new(),
            metadata_pending: BTreeSet::new(),
            metadata_work: None,
            metadata_error: None,
            brokers: BTreeMap::new(),
            partitions: BTreeMap::new(),
            pending: BTreeMap::new(),
            pending_counts: BTreeMap::new(),
            pending_routes: BTreeMap::new(),
            batched_locations: BTreeMap::new(),
            batch_requests: BTreeMap::new(),
            queued_locations: BTreeMap::new(),
            topic_records: BTreeMap::new(),
            settling_topics: BTreeSet::new(),
            settlement_cursor: None,
            partition_cleanup: partition_cleanup::PartitionCleanup::default(),
            attempt_counts: BTreeMap::new(),
            routes: BTreeMap::new(),
            ledger,
            codecs,
            output,
            orders,
            deadlines: Deadlines::default(),
            flushes,
            terminal_records,
            terminal_order: terminal_order::TerminalOrder::default(),
            next_flush: 1,
            next_correlation: 1,
            next_attempt: 1,
            closed_credit: Some(closed_credit),
            fatal_credit: Some(fatal_credit),
            close: None,
            seal_sweep: None,
            fence_work: false,
            close_retire_cursor: 0,
            close_retire_done: false,
            closed: false,
            failed: None,
            failure_work: false,
            failure_batch_cursor: 0,
            failure_connection_cursor: 0,
            unknown: 0,
            seal_counter: crate::accumulator::SealCounter::default(),
            identity_pending: identity.is_none(),
            identity_refresh: None,
            external_obligations: 0,
            retry_jitter: 0,
            encoder: encoding::Encoder::default(),
            scheduler: dispatch_work::Scheduler::default(),
        };
        if identity.is_none() {
            engine.order(EngineOrder::InitProducerId { previous: None })?;
        }
        Ok(engine)
    }
    pub fn config(&self) -> &ProducerConfig {
        &self.config
    }
    pub fn credits(&self) -> SharedCredits {
        self.credits.clone()
    }
    pub fn topics(&self) -> &TopicCache {
        &self.topics
    }
    pub fn validated_config(&self) -> ValidatedConfig {
        self.validated
    }
    pub fn pop_order(&mut self) -> Option<EngineOrder> {
        let (index, order) = self.orders.pop_front()?;
        if let Some((connection, kind)) = Self::order_connection(&order)
            && let Some(connection) = self.connections.get_mut(Slot::from_packed(connection.0))
            && connection.pending_orders[kind] == Some(index)
        {
            connection.pending_orders[kind] = None;
        }
        Some(order)
    }
    /// Whether bounded terminal byte/record cleanup still needs owner work,
    /// even if no public event is ready yet.
    #[must_use]
    pub fn has_terminal_work(&self) -> bool {
        !self.terminal_records.is_empty() || self.terminal_order.has_work()
    }
    #[must_use]
    pub fn has_maintenance_work(&self) -> bool {
        if self.has_metadata_work()
            || self.has_retry_work()
            || self.has_identity_work()
            || self.headroom_sweep.is_some()
        {
            return true;
        }
        self.seal_sweep.is_some()
            || self.failure_work
            || !self.settling_topics.is_empty()
            || self.partition_cleanup.has_work()
            || self.fence_work
            || self.output.has_reclaim_work()
    }
    pub fn pop_event(&mut self) -> Option<EventEnvelope> {
        if self.events.is_empty() {
            self.drain_terminal_records(1);
        }
        self.events.pop()
    }
    /// Publishes an event reserved against this engine's authority.
    /// # Errors
    /// Returns the original envelope on foreign authority or exhaustion. Its
    /// reservation remains held until the returned owner is consumed or dropped.
    #[allow(clippy::result_large_err)] // Returns the precharged envelope without allocating on exhaustion.
    pub fn publish_event(
        &mut self,
        event: EventEnvelope,
    ) -> std::result::Result<(), EventEnvelope> {
        if !event.belongs_to(&self.credits) {
            return Err(event);
        }
        self.events.push(event)
    }
    pub fn set_external_obligations(&mut self, count: usize) {
        self.external_obligations = count;
        self.complete_fences();
    }
    /// Supply a Workload-stream draw outside engine mutation; no policy callback
    /// or randomness source is invoked while changing the ledger.
    pub fn set_retry_jitter(&mut self, draw: u64) {
        self.retry_jitter = draw;
    }
    /// Register the owner before checking passive maintenance readiness. A
    /// provider's last payload release schedules this waker without polling I/O.
    pub fn register_reclaim_waker(&self, waker: &std::task::Waker) {
        self.output.register_reclaim_waker(waker);
    }
    pub fn next_deadline(&self) -> Option<RuntimeInstant> {
        if self.has_metadata_work()
            || self.has_retry_work()
            || self.has_identity_work()
            || self.headroom_sweep.is_some()
            || self.seal_sweep.is_some()
            || self.failure_work
            || !self.settling_topics.is_empty()
            || self.partition_cleanup.has_work()
            || self.fence_work
            || self.output.has_reclaim_work()
        {
            Some(RuntimeInstant::ZERO)
        } else {
            self.deadlines.next()
        }
    }
    #[must_use]
    pub fn is_failed(&self) -> bool {
        self.failed.is_some()
    }
    pub fn request_for(&self, connection: ConnectionKey, correlation: i32) -> Option<RequestKey> {
        self.connections
            .get(Slot::from_packed(connection.0))?
            .fifo
            .iter()
            .find(|key| {
                self.requests
                    .get(Slot::from_packed(key.0))
                    .is_some_and(|r| r.correlation == correlation)
            })
            .copied()
    }
    pub fn status(&self) -> EngineStatus {
        EngineStatus {
            batch_seals: self.seal_counter.snapshot(),
            accepted: self.tracker.accepted().0,
            terminal: self.tracker.accepted().0 - self.tracker.live(),
            unknown: self.unknown,
            pending_records: self.pending.len() + self.queued_locations.len(),
            batches: self.batches.len(),
            requests: self.requests.len(),
            connections: self.connections.len(),
            queued_events: self.events.len(),
            queued_orders: self.orders.len(),
            deadlines: self.deadlines.current.len(),
            closing: self.close.is_some(),
            closed: self.closed,
            failed: self.failed.is_some(),
            identity: self.ledger.as_ref().map(ProducerLedger::identity),
        }
    }
    pub fn is_quiescent(&self) -> bool {
        !self.has_metadata_work()
            && !self.failure_work
            && !self.has_retry_work()
            && !self.has_identity_work()
            && self.headroom_sweep.is_none()
            && self.topic_records.is_empty()
            && self.settling_topics.is_empty()
            && !self.partition_cleanup.has_work()
            && self.tracker.live() == 0
            && self.batches.is_empty()
            && self.pending.is_empty()
            && self.requests.is_empty()
            && self.connections.is_empty()
            && self.external_obligations == 0
            && self.output.status().reserved_bytes == 0
            && !self.output.has_reclaim_work()
            && {
                let pools = self.credits.snapshot();
                [
                    Resource::CompressedBytes,
                    Resource::RequestMetadata,
                    Resource::RxBytes,
                    Resource::StagingBytes,
                    Resource::TlsBytes,
                ]
                .iter()
                .all(|resource| pools[*resource as usize].held == 0)
            }
    }
    fn order_connection(order: &EngineOrder) -> Option<(ConnectionKey, usize)> {
        match order {
            EngineOrder::Connect { key, .. } => Some((*key, 0)),
            EngineOrder::Dispatch { connection, .. } => Some((*connection, 1)),
            EngineOrder::Retire { connection, .. } => Some((*connection, 2)),
            _ => None,
        }
    }
    fn remove_connection_order(&mut self, key: ConnectionKey, kind: usize) {
        if let Some(connection) = self.connections.get_mut(Slot::from_packed(key.0))
            && let Some(index) = connection.pending_orders[kind].take()
        {
            self.orders.remove(index);
        }
    }
    fn order(&mut self, order: EngineOrder) -> Result<()> {
        let owner = Self::order_connection(&order);
        if let Some((key, kind)) = owner
            && self
                .connections
                .get(Slot::from_packed(key.0))
                .is_some_and(|connection| connection.pending_orders[kind].is_some())
        {
            return Err(EngineError::InvalidState(
                "duplicate queued connection order",
            ));
        }
        let index = self.orders.push_back(order)?;
        if let Some((key, kind)) = owner
            && let Some(connection) = self.connections.get_mut(Slot::from_packed(key.0))
        {
            connection.pending_orders[kind] = Some(index);
        }
        Ok(())
    }
    fn event(&mut self, event: Event, credit: HeldCredits) {
        let envelope = EventEnvelope::new(event, credit).expect("one precharged event obligation");
        self.events
            .push(envelope)
            .expect("event queue covers every independently reserved event credit");
    }
    fn control_credit(&self) -> Result<HeldCredits> {
        Ok(self.credits.reserve(&[Claim {
            resource: Resource::ControlEvents,
            amount: 1,
            lane: 0,
        }])?)
    }
    fn deadline_after(now: RuntimeInstant, delay: RuntimeDuration) -> RuntimeInstant {
        now.checked_add(delay).unwrap_or(RuntimeInstant::MAX)
    }
    pub fn fail(&mut self, reason: FailureReason) {
        self.fail_all(reason);
    }
}
