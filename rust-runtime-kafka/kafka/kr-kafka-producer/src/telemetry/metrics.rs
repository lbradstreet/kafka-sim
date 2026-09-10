//! Bounded, passive HDR distributions and interval snapshots.
//!
//! Recording is owner-local and performs no allocation, clock read, wake, or
//! random draw. Three banks are allocated at startup. An explicitly requested
//! snapshot swaps an entire bank at an owner safe point; readers perform every
//! histogram scan and reset. A slow reader backpressures snapshots, never data.
//! Reader handles and snapshots own only metrics, not a producer or I/O owner.
use hdrhistogram::Histogram;
use kr_runtime::RuntimeInstant;
use std::sync::atomic::{AtomicBool, Ordering};
use std::{
    fmt,
    sync::{Arc, Mutex, MutexGuard, TryLockError, Weak},
};

mod config;
mod scope_index;
pub use config::{MetricsConfig, MetricsError, MetricsMemory};
use scope_index::ScopeIndex;

pub const METRICS_SCHEMA_VERSION: u32 = 1;

/// All latencies use supplied runtime/model nanoseconds. Depth distributions
/// are event-weighted, not time-weighted. Retries contribute distinct RTTs.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(usize)]
pub enum Metric {
    ProduceRttNanos,
    BatchFillNanos,
    BatchRawBytes,
    BatchWireBytes,
    RecordsPerBatch,
    QueueWaitNanos,
    DeliveryAckedNanos,
    DeliveryNotWrittenNanos,
    DeliveryUnknownNanos,
    InFlightRequests,
    InFlightWireBytes,
}
impl Metric {
    pub const ALL: [Self; 11] = [
        Self::ProduceRttNanos,
        Self::BatchFillNanos,
        Self::BatchRawBytes,
        Self::BatchWireBytes,
        Self::RecordsPerBatch,
        Self::QueueWaitNanos,
        Self::DeliveryAckedNanos,
        Self::DeliveryNotWrittenNanos,
        Self::DeliveryUnknownNanos,
        Self::InFlightRequests,
        Self::InFlightWireBytes,
    ];
    pub const COUNT: usize = Self::ALL.len();
    pub const fn unit(self) -> MetricUnit {
        match self {
            Self::BatchRawBytes | Self::BatchWireBytes | Self::InFlightWireBytes => {
                MetricUnit::Bytes
            }
            Self::RecordsPerBatch | Self::InFlightRequests => MetricUnit::Count,
            _ => MetricUnit::Nanoseconds,
        }
    }
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MetricUnit {
    Nanoseconds,
    Bytes,
    Count,
}

/// Topic identities are immutable UUIDs, so recreation cannot merge a new topic
/// into the old topic's distributions. Scopes never evict or reuse identities.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum Scope {
    Global,
    Broker(i32),
    Partition { topic_id: [u8; 16], partition: i32 },
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ScopeToken {
    index: Option<usize>,
    label: Scope,
}
impl ScopeToken {
    pub const GLOBAL: Self = Self {
        index: Some(0),
        label: Scope::Global,
    };
}

/// Inclusive equivalent-value range containing the requested quantile. HDR is
/// precise to its configured significant digits, not lossless for each input.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct QuantileRange {
    pub low: u64,
    pub high: u64,
}

/// Supplied runtime/model interval boundaries. Without supplied times these
/// remain None; a reader must not infer rate denominators from wall-clock time.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct IntervalBounds {
    pub start: Option<RuntimeInstant>,
    pub end: Option<RuntimeInstant>,
}

#[derive(Debug)]
pub struct Distribution {
    histogram: Histogram<u64>,
    high: u64,
    exact_max: u64,
    out_of_range: u64,
    count_overflow: u64,
    diagnostic_overflow: bool,
}
impl Distribution {
    fn new(config: MetricsConfig, metric: Metric) -> Result<Self, MetricsError> {
        let high = config.high(metric);
        let mut histogram = Histogram::new_with_bounds(1, high, config.significant_digits)
            .map_err(|_| MetricsError::InvalidBounds)?;
        histogram.auto(false);
        debug_assert_eq!(
            histogram.distinct_values(),
            config::bins(high, config.significant_digits)?
        );
        Ok(Self {
            histogram,
            high,
            exact_max: 0,
            out_of_range: 0,
            count_overflow: 0,
            diagnostic_overflow: false,
        })
    }
    fn record(&mut self, value: u64) {
        if value > self.high {
            increment(&mut self.out_of_range, &mut self.diagnostic_overflow);
        } else if self.histogram.len() == u64::MAX {
            // hdrhistogram saturates both bucket and total counters. Check the
            // total before mutation so successful samples retain exact counts.
            increment(&mut self.count_overflow, &mut self.diagnostic_overflow);
        } else {
            self.histogram
                .record(value)
                .expect("validated fixed HDR range");
            self.exact_max = self.exact_max.max(value);
        }
    }
    fn reset(&mut self) {
        self.histogram.reset();
        self.exact_max = 0;
        self.out_of_range = 0;
        self.count_overflow = 0;
        self.diagnostic_overflow = false;
    }
    pub fn count(&self) -> u64 {
        self.histogram.len()
    }
    /// Exact maximum of successfully recorded samples only. Rejected outliers
    /// and samples rejected after count exhaustion are excluded.
    pub fn exact_max(&self) -> Option<u64> {
        (!self.histogram.is_empty()).then_some(self.exact_max)
    }
    pub fn significant_digits(&self) -> u8 {
        self.histogram.sigfig()
    }
    pub fn highest_trackable(&self) -> u64 {
        self.high
    }
    pub fn out_of_range(&self) -> u64 {
        self.out_of_range
    }
    pub fn count_overflow(&self) -> u64 {
        self.count_overflow
    }
    pub fn diagnostic_overflow(&self) -> bool {
        self.diagnostic_overflow
    }
    /// Rank is ceil(count * millionths / 1_000_000), with rank one for zero.
    /// Iteration is reader-side; invalid quantiles and empty histograms return None.
    pub fn quantile(&self, millionths: u32) -> Option<QuantileRange> {
        if millionths > 1_000_000 || self.histogram.is_empty() {
            return None;
        }
        let rank = (u128::from(self.count()) * u128::from(millionths))
            .div_ceil(1_000_000)
            .max(1);
        let mut cumulative = 0u128;
        for bucket in self.histogram.iter_recorded() {
            cumulative += u128::from(bucket.count_at_value());
            if cumulative >= rank {
                let value = bucket.value_iterated_to();
                return Some(QuantileRange {
                    low: self.histogram.lowest_equivalent(value),
                    high: self.histogram.highest_equivalent(value),
                });
            }
        }
        None
    }
    /// Exporter-neutral sparse buckets. This scan belongs to the snapshot reader.
    pub fn buckets(&self) -> impl Iterator<Item = (QuantileRange, u64)> + '_ {
        self.histogram.iter_recorded().map(|bucket| {
            let value = bucket.value_iterated_to();
            (
                QuantileRange {
                    low: self.histogram.lowest_equivalent(value),
                    high: self.histogram.highest_equivalent(value),
                },
                bucket.count_at_value(),
            )
        })
    }
}
fn increment(value: &mut u64, overflow: &mut bool) {
    if let Some(next) = value.checked_add(1) {
        *value = next;
    } else {
        *overflow = true;
    }
}

#[derive(Debug)]
struct Bank {
    epoch: u64,
    bounds: IntervalBounds,
    labels: Vec<Option<Scope>>,
    distributions: Vec<Distribution>,
    omitted_scope_samples: u64,
    scope_capacity_rejections: u64,
    invalid_scope_samples: u64,
    invalid_time_samples: u64,
    missing_time_samples: u64,
    invalid_depth_samples: u64,
    diagnostic_overflow: bool,
}
impl Bank {
    fn new(config: MetricsConfig) -> Result<Self, MetricsError> {
        let slots = config.slots()?;
        let mut labels =
            crate::fixed::try_vec(slots).map_err(|_| MetricsError::AllocationFailed)?;
        labels.resize(slots, None);
        labels[0] = Some(Scope::Global);
        let mut distributions = crate::fixed::try_vec(
            slots
                .checked_mul(Metric::COUNT)
                .ok_or(MetricsError::Overflow)?,
        )
        .map_err(|_| MetricsError::AllocationFailed)?;
        for _ in 0..slots {
            for metric in Metric::ALL {
                distributions.push(Distribution::new(config, metric)?);
            }
        }
        Ok(Self {
            epoch: 0,
            bounds: IntervalBounds::default(),
            labels,
            distributions,
            omitted_scope_samples: 0,
            scope_capacity_rejections: 0,
            invalid_scope_samples: 0,
            invalid_time_samples: 0,
            missing_time_samples: 0,
            invalid_depth_samples: 0,
            diagnostic_overflow: false,
        })
    }
    fn reset(&mut self) {
        for distribution in &mut self.distributions {
            distribution.reset();
        }
        self.labels.fill(None);
        self.labels[0] = Some(Scope::Global);
        self.omitted_scope_samples = 0;
        self.scope_capacity_rejections = 0;
        self.invalid_scope_samples = 0;
        self.invalid_time_samples = 0;
        self.missing_time_samples = 0;
        self.invalid_depth_samples = 0;
        self.bounds = IntervalBounds::default();
        self.diagnostic_overflow = false;
    }
}
#[derive(Debug)]
struct Exchange {
    state: Mutex<ExchangeState>,
    pending: AtomicBool,
}
#[derive(Debug)]
struct ExchangeState {
    spare: [Option<Bank>; 2],
    published: Option<Bank>,
    terminal: Option<Bank>,
    requested: bool,
    closed: bool,
    epoch_exhausted: bool,
}
fn lock(exchange: &Exchange) -> MutexGuard<'_, ExchangeState> {
    exchange.state.lock().unwrap_or_else(|p| p.into_inner())
}

/// One owner records. Registration uses a preallocated AVL index with O(log N)
/// lookup/insertion over the configured scope cap; `record` is O(1).
#[derive(Debug)]
pub struct MetricsRecorder {
    config: MetricsConfig,
    active: Option<Bank>,
    exchange: Option<Arc<Exchange>>,
    scopes: ScopeIndex,
    brokers: usize,
    partitions: usize,
}
impl MetricsRecorder {
    pub fn new(config: MetricsConfig) -> Result<Self, MetricsError> {
        config.memory()?;
        if !config.enabled {
            return Ok(Self {
                config,
                active: None,
                exchange: None,
                scopes: ScopeIndex::default(),
                brokers: 0,
                partitions: 0,
            });
        }
        let mut active = Bank::new(config)?;
        active.epoch = 1;
        let exchange = Arc::new(Exchange {
            pending: AtomicBool::new(false),
            state: Mutex::new(ExchangeState {
                spare: [Some(Bank::new(config)?), Some(Bank::new(config)?)],
                published: None,
                terminal: None,
                requested: false,
                closed: false,
                epoch_exhausted: false,
            }),
        });
        // Some platforms lazily allocate std mutex backing on first use.
        // Establish it during construction, before any owner poll or reader
        // request participates in the allocation-free diagnostics contract.
        drop(lock(&exchange));
        let mut scopes = ScopeIndex::new(config.slots()?)?;
        assert_eq!(scopes.insert_new(Scope::Global), 0);
        Ok(Self {
            config,
            active: Some(active),
            exchange: Some(exchange),
            scopes,
            brokers: 0,
            partitions: 0,
        })
    }
    pub fn reader(&self) -> MetricsReader {
        MetricsReader {
            exchange: self.exchange.clone(),
        }
    }
    /// Advances the diagnostic interval using an existing runtime/model time.
    /// Backward samples are reported and cannot move a boundary backward.
    pub fn observe_time(&mut self, now: RuntimeInstant) {
        let Some(bank) = &mut self.active else {
            return;
        };
        if bank.bounds.end.is_some_and(|previous| now < previous) {
            increment(
                &mut bank.invalid_time_samples,
                &mut bank.diagnostic_overflow,
            );
        } else {
            bank.bounds.start.get_or_insert(now);
            bank.bounds.end = Some(now);
        }
    }
    pub fn current_time(&self) -> Option<RuntimeInstant> {
        self.active.as_ref().and_then(|bank| bank.bounds.end)
    }
    /// Makes absent timing evidence explicit instead of recording zero latency.
    pub fn missing_time(&mut self) {
        if let Some(bank) = &mut self.active {
            increment(
                &mut bank.missing_time_samples,
                &mut bank.diagnostic_overflow,
            );
        }
    }
    pub(crate) fn invalid_depth(&mut self) {
        if let Some(bank) = &mut self.active {
            increment(
                &mut bank.invalid_depth_samples,
                &mut bank.diagnostic_overflow,
            );
        }
    }
    /// Records an actual elapsed stage duration. Neither clock is read here.
    pub fn record_elapsed(
        &mut self,
        metric: Metric,
        scope: ScopeToken,
        start: RuntimeInstant,
        end: RuntimeInstant,
    ) {
        if let Some(elapsed) = end.checked_duration_since(start) {
            self.record(metric, scope, elapsed.as_nanos());
        } else if let Some(bank) = &mut self.active {
            increment(
                &mut bank.invalid_time_samples,
                &mut bank.diagnostic_overflow,
            );
        }
    }
    /// Supplies a safe-point timestamp before the constant-work handoff.
    pub fn publish_at(&mut self, now: RuntimeInstant) -> bool {
        self.observe_time(now);
        self.publish_if_requested()
    }
    /// Only explicitly requested diagnostic work sets this bit. After a
    /// contended handoff, an embedding may reschedule this bounded operation.
    pub fn snapshot_requested(&self) -> bool {
        self.exchange
            .as_ref()
            .is_some_and(|e| e.pending.load(Ordering::Acquire))
    }
    pub fn register_broker(&mut self, node: i32) -> ScopeToken {
        self.register(Scope::Broker(node))
    }
    pub fn register_partition(&mut self, topic_id: [u8; 16], partition: i32) -> ScopeToken {
        self.register(Scope::Partition {
            topic_id,
            partition,
        })
    }
    fn register(&mut self, scope: Scope) -> ScopeToken {
        if let Some(index) = self.scopes.find(scope) {
            return ScopeToken {
                index: Some(index),
                label: scope,
            };
        }
        let available = match scope {
            Scope::Global => return ScopeToken::GLOBAL,
            Scope::Broker(_) => self.brokers < usize::from(self.config.max_broker_scopes),
            Scope::Partition { .. } => {
                self.partitions < usize::from(self.config.max_partition_scopes)
            }
        };
        if !available || self.active.is_none() {
            if let Some(bank) = &mut self.active {
                increment(
                    &mut bank.scope_capacity_rejections,
                    &mut bank.diagnostic_overflow,
                );
            }
            return ScopeToken {
                index: None,
                label: scope,
            };
        }
        let index = self.scopes.insert_new(scope);
        match scope {
            Scope::Broker(_) => self.brokers += 1,
            Scope::Partition { .. } => self.partitions += 1,
            Scope::Global => {}
        }
        ScopeToken {
            index: Some(index),
            label: scope,
        }
    }
    /// Records globally and, if admitted, once in the supplied scope. Scope
    /// capacity exhaustion never drops the global sample or changes policy.
    pub fn record(&mut self, metric: Metric, scope: ScopeToken, value: u64) {
        let Some(bank) = &mut self.active else {
            return;
        };
        bank.distributions[metric as usize].record(value);
        self.record_scoped(metric, scope, value);
    }
    /// Records only the named non-global scope. Use with a separately recorded
    /// global total for depth metrics, whose broker value differs from global.
    /// A GLOBAL token is a no-op, preventing accidental duplicate global counts.
    pub fn record_scoped(&mut self, metric: Metric, scope: ScopeToken, value: u64) {
        let Some(bank) = &mut self.active else {
            return;
        };
        match scope.index {
            Some(0) if scope.label == Scope::Global => {}
            Some(index) if self.scopes.get(index) == Some(&scope.label) => {
                bank.labels[index] = Some(scope.label);
                bank.distributions[index * Metric::COUNT + metric as usize].record(value);
            }
            Some(_) => increment(
                &mut bank.invalid_scope_samples,
                &mut bank.diagnostic_overflow,
            ),
            None => increment(
                &mut bank.omitted_scope_samples,
                &mut bank.diagnostic_overflow,
            ),
        }
    }
    /// One nonblocking lock attempt and an O(1) bank swap. No reset, label scan,
    /// allocation or wake occurs here. A request remains pending on contention.
    pub fn publish_if_requested(&mut self) -> bool {
        let Some(exchange) = &self.exchange else {
            return false;
        };
        if !exchange.pending.load(Ordering::Acquire) {
            return false;
        }
        let mut state = match exchange.state.try_lock() {
            Ok(state) => state,
            Err(TryLockError::Poisoned(p)) => p.into_inner(),
            Err(TryLockError::WouldBlock) => return false,
        };
        if !state.requested || state.published.is_some() {
            return false;
        }
        let Some(epoch) = self
            .active
            .as_ref()
            .and_then(|bank| bank.epoch.checked_add(1))
        else {
            state.epoch_exhausted = true;
            state.requested = false;
            exchange.pending.store(false, Ordering::Release);
            return false;
        };
        let Some(mut next) = state.spare[0].take().or_else(|| state.spare[1].take()) else {
            return false;
        };
        next.epoch = epoch;
        let boundary = self.active.as_ref().and_then(|bank| bank.bounds.end);
        next.bounds = IntervalBounds {
            start: boundary,
            end: boundary,
        };
        state.published = self.active.replace(next);
        state.requested = false;
        exchange.pending.store(false, Ordering::Release);
        true
    }
}
impl Drop for MetricsRecorder {
    fn drop(&mut self) {
        if let Some(exchange) = &self.exchange {
            let mut state = lock(exchange);
            state.terminal = self.active.take();
            state.closed = true;
            state.requested = false;
            exchange.pending.store(false, Ordering::Release);
        }
    }
}

#[derive(Clone, Debug, Default)]
pub struct MetricsReader {
    exchange: Option<Arc<Exchange>>,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SnapshotError {
    Disabled,
    Busy,
    NoSpareBank,
    Closed,
    EpochExhausted,
}
impl fmt::Display for SnapshotError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "metrics snapshot: {self:?}")
    }
}
impl std::error::Error for SnapshotError {}
impl MetricsReader {
    /// Passive request. An embedding may explicitly wake its owner for this
    /// external action; recording itself never schedules runtime work.
    pub fn request_snapshot(&self) -> Result<(), SnapshotError> {
        let exchange = self.exchange.as_ref().ok_or(SnapshotError::Disabled)?;
        let mut state = lock(exchange);
        if state.closed {
            return Err(SnapshotError::Closed);
        }
        if state.epoch_exhausted {
            return Err(SnapshotError::EpochExhausted);
        }
        if state.requested || state.published.is_some() {
            return Err(SnapshotError::Busy);
        }
        if state.spare.iter().all(Option::is_none) {
            return Err(SnapshotError::NoSpareBank);
        }
        state.requested = true;
        exchange.pending.store(true, Ordering::Release);
        Ok(())
    }
    /// Takes the oldest completed interval, including the final interval after
    /// owner destruction. No scan/reset happens while the exchange lock is held.
    pub fn try_take_snapshot(&self) -> Option<MetricsSnapshot> {
        let exchange = self.exchange.as_ref()?;
        let bank = {
            let mut state = lock(exchange);
            state.published.take().or_else(|| state.terminal.take())?
        };
        Some(MetricsSnapshot {
            bank: Some(bank),
            exchange: Arc::downgrade(exchange),
        })
    }
    pub fn is_closed(&self) -> bool {
        self.exchange.as_ref().is_none_or(|e| lock(e).closed)
    }
}

/// Immutable interval lease. Dropping it resets its bank on the calling reader
/// thread, then returns it for reuse. Never drop a snapshot inside an actor poll.
#[derive(Debug)]
pub struct MetricsSnapshot {
    bank: Option<Bank>,
    exchange: Weak<Exchange>,
}
impl MetricsSnapshot {
    pub fn schema_version(&self) -> u32 {
        METRICS_SCHEMA_VERSION
    }
    fn bank(&self) -> &Bank {
        self.bank.as_ref().expect("live snapshot")
    }
    pub fn epoch(&self) -> u64 {
        self.bank().epoch
    }
    pub fn bounds(&self) -> IntervalBounds {
        self.bank().bounds
    }
    pub fn scopes(&self) -> impl Iterator<Item = Scope> + '_ {
        self.bank().labels.iter().flatten().copied()
    }
    pub fn distribution(&self, scope: Scope, metric: Metric) -> Option<&Distribution> {
        let index = self
            .bank()
            .labels
            .iter()
            .position(|label| *label == Some(scope))?;
        self.bank()
            .distributions
            .get(index * Metric::COUNT + metric as usize)
    }
    pub fn omitted_scope_samples(&self) -> u64 {
        self.bank().omitted_scope_samples
    }
    pub fn scope_capacity_rejections(&self) -> u64 {
        self.bank().scope_capacity_rejections
    }
    pub fn invalid_scope_samples(&self) -> u64 {
        self.bank().invalid_scope_samples
    }
    pub fn invalid_time_samples(&self) -> u64 {
        self.bank().invalid_time_samples
    }
    pub fn missing_time_samples(&self) -> u64 {
        self.bank().missing_time_samples
    }
    pub fn invalid_depth_samples(&self) -> u64 {
        self.bank().invalid_depth_samples
    }
    pub fn diagnostic_overflow(&self) -> bool {
        self.bank().diagnostic_overflow
    }
}
impl Drop for MetricsSnapshot {
    fn drop(&mut self) {
        let Some(mut bank) = self.bank.take() else {
            return;
        };
        // Reset outside the exchange lock and outside the owner. A weak handle
        // ensures the snapshot cannot keep the recorder/producer alive.
        bank.reset();
        if let Some(exchange) = self.exchange.upgrade() {
            let mut state = lock(&exchange);
            if !state.closed {
                if state.spare[0].is_none() {
                    state.spare[0] = Some(bank);
                } else {
                    debug_assert!(state.spare[1].is_none());
                    state.spare[1] = Some(bank);
                }
            }
        }
    }
}

#[cfg(test)]
mod tests;
