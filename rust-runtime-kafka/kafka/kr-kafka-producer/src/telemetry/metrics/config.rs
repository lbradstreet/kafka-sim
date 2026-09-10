use super::{Bank, Distribution, Exchange, Metric, MetricUnit, Scope, ScopeIndex};
use std::{fmt, mem::size_of};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct MetricsConfig {
    pub enabled: bool,
    /// One through five; the default preserves three significant digits.
    pub significant_digits: u8,
    pub highest_duration_nanos: u64,
    pub highest_bytes: u64,
    pub highest_count: u64,
    /// Additional scopes are opt-in because every scope owns eleven HDRs in
    /// each of three banks. Global distributions are always present if enabled.
    /// A zero cap intentionally omits those scoped samples and reports their
    /// number; global samples are retained without loss from scope exhaustion.
    pub max_broker_scopes: u16,
    pub max_partition_scopes: u16,
    /// Limit on requested histogram-bin and metadata backing. Private HDR
    /// reservation slack and shared-owner overhead are not bounded by this cap.
    pub max_storage_bytes: usize,
}
impl Default for MetricsConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            significant_digits: 3,
            highest_duration_nanos: 600_000_000_000,
            highest_bytes: 1 << 30,
            highest_count: 1 << 20,
            max_broker_scopes: 0,
            max_partition_scopes: 0,
            max_storage_bytes: 16 * 1024 * 1024,
        }
    }
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MetricsError {
    InvalidBounds,
    Overflow,
    StorageLimit { required: usize, limit: usize },
    AllocationFailed,
}
impl fmt::Display for MetricsError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "metrics configuration: {self:?}")
    }
}
impl std::error::Error for MetricsError {}

/// Checked requested backing, including all three banks and scope identities.
/// Label, distribution-descriptor and scope-index vector capacities are checked
/// exactly before publication. Allocator rounding is outside the core report.
/// Private HDR counter slack, Arc control-block overhead and platform mutex
/// backing remain explicit
/// unaccounted gaps; this is a requested-backing cap, not a full heap cap.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct MetricsMemory {
    /// Logical bin count times the counter layout. HDR exposes counts length,
    /// but not the private counter Vec's retained capacity; any excess remains
    /// in the collection-reservation-slack gap.
    pub histogram_counts: usize,
    pub fixed_metadata: usize,
    pub configured_bytes: usize,
}
impl MetricsConfig {
    pub(super) fn high(self, metric: Metric) -> u64 {
        match metric.unit() {
            MetricUnit::Nanoseconds => self.highest_duration_nanos,
            MetricUnit::Bytes => self.highest_bytes,
            MetricUnit::Count => self.highest_count,
        }
    }
    pub(super) fn slots(self) -> Result<usize, MetricsError> {
        usize::from(self.max_broker_scopes)
            .checked_add(usize::from(self.max_partition_scopes))
            .and_then(|v| v.checked_add(1))
            .ok_or(MetricsError::Overflow)
    }
    /// Allocation-free preflight using the pinned HDR 7.6.0 logical bin count.
    /// A constructor test compares this integer calculation with distinct_values
    /// (the counter length). It does not verify the private counter capacity.
    pub fn memory(self) -> Result<MetricsMemory, MetricsError> {
        if !self.enabled {
            return Ok(MetricsMemory::default());
        }
        if !(1..=5).contains(&self.significant_digits) {
            return Err(MetricsError::InvalidBounds);
        }
        let counts_per_scope = Metric::ALL.into_iter().try_fold(0usize, |sum, metric| {
            sum.checked_add(bins(self.high(metric), self.significant_digits)?)
                .ok_or(MetricsError::Overflow)
        })?;
        let slots = self.slots()?;
        let histogram_counts = counts_per_scope
            .checked_mul(slots)
            .and_then(|n| n.checked_mul(3))
            .and_then(|n| n.checked_mul(size_of::<u64>()))
            .ok_or(MetricsError::Overflow)?;
        // Exchange embeds the two spare and published/final Option<Bank>
        // descriptors. The active Bank is embedded in the engine recorder.
        let bank_metadata = size_of::<Distribution>()
            .checked_mul(Metric::COUNT)
            .and_then(|n| n.checked_add(size_of::<Option<Scope>>()))
            .and_then(|n| n.checked_mul(slots))
            .and_then(|n| n.checked_mul(3))
            .ok_or(MetricsError::Overflow)?;
        let fixed_metadata = ScopeIndex::configured_storage_bytes(slots)
            .and_then(|n| n.checked_add(bank_metadata))
            .and_then(|n| n.checked_add(size_of::<Exchange>()))
            .and_then(|n| n.checked_add(size_of::<Bank>()))
            .ok_or(MetricsError::Overflow)?;
        let configured_bytes = histogram_counts
            .checked_add(fixed_metadata)
            .ok_or(MetricsError::Overflow)?;
        if configured_bytes > self.max_storage_bytes {
            return Err(MetricsError::StorageLimit {
                required: configured_bytes,
                limit: self.max_storage_bytes,
            });
        }
        Ok(MetricsMemory {
            histogram_counts,
            fixed_metadata,
            configured_bytes,
        })
    }
}

pub(super) fn bins(high: u64, digits: u8) -> Result<usize, MetricsError> {
    if high < 2 || digits > 5 {
        return Err(MetricsError::InvalidBounds);
    }
    let sub_buckets = (2 * 10u32.pow(u32::from(digits))).next_power_of_two();
    let mut cover = u64::from(sub_buckets);
    let mut buckets = 1usize;
    while cover <= high {
        buckets += 1;
        let Some(next) = cover.checked_mul(2) else {
            break;
        };
        cover = next;
    }
    (buckets + 1)
        .checked_mul(sub_buckets as usize / 2)
        .ok_or(MetricsError::Overflow)
}
