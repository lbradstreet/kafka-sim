//! Provider-neutral, owned record-ring contracts and a deterministic memory provider.
//!
//! A ring has absolute, dense positions even though its retained storage is
//! bounded and reused. Writes and trims are first accepted by the current
//! process. [`RingWriter::sync`] atomically publishes both accepted bounds as
//! durable. Reads expose only the durable interval.

#![forbid(unsafe_code)]

use std::collections::VecDeque;
use std::fmt;
use std::future::{Future, Ready, ready};
use std::sync::{Arc, Mutex, MutexGuard};

use kr_runtime::{CompletionError, CompletionResult};

#[cfg(any(test, feature = "test-support"))]
pub mod conformance;
pub mod file;

/// The absolute position assigned to one accepted record.
///
/// Positions are dense and never change when physical ring storage wraps or is
/// reclaimed. An accepted-but-unsynced position can be reused after recovery
/// discards the unsynced suffix; only durable positions survive a crash.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct RingPosition(u64);

impl RingPosition {
    #[must_use]
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    #[must_use]
    pub const fn get(self) -> u64 {
        self.0
    }

    /// Returns the cursor immediately after this position, if representable.
    #[must_use]
    pub const fn next_cursor(self) -> Option<RingCursor> {
        match self.0.checked_add(1) {
            Some(next) => Some(RingCursor::new(next)),
            None => None,
        }
    }
}

/// The inclusive next absolute position for reads and conditional appends.
#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct RingCursor(u64);

impl RingCursor {
    pub const START: Self = Self(0);

    #[must_use]
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    #[must_use]
    pub const fn get(self) -> u64 {
        self.0
    }
}

/// A named fixed limit in [`RingLimits`].
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RingLimit {
    RecordBytes,
    LiveRecords,
    LivePayloadBytes,
    ReadRecords,
    ReadBytes,
    BatchRecords,
    BatchBytes,
}

impl fmt::Display for RingLimit {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::RecordBytes => formatter.write_str("max_record_bytes"),
            Self::LiveRecords => formatter.write_str("max_live_records"),
            Self::LivePayloadBytes => formatter.write_str("max_live_payload_bytes"),
            Self::ReadRecords => formatter.write_str("max_read_records"),
            Self::ReadBytes => formatter.write_str("max_read_bytes"),
            Self::BatchRecords => formatter.write_str("max_batch_records"),
            Self::BatchBytes => formatter.write_str("max_batch_bytes"),
        }
    }
}

/// Invalid provider-neutral ring bounds.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum RingLimitsError {
    ZeroLimit {
        limit: RingLimit,
    },
    /// `smaller` must not exceed `larger`.
    InconsistentLimits {
        smaller: RingLimit,
        smaller_value: usize,
        larger: RingLimit,
        larger_value: usize,
    },
    RecordIndexAllocationFailed {
        requested: usize,
    },
}

impl fmt::Display for RingLimitsError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ZeroLimit { limit } => write!(formatter, "ring limit {limit} must be non-zero"),
            Self::InconsistentLimits {
                smaller,
                smaller_value,
                larger,
                larger_value,
            } => write!(
                formatter,
                "ring limit {smaller} ({smaller_value}) exceeds {larger} ({larger_value})"
            ),
            Self::RecordIndexAllocationFailed { requested } => write!(
                formatter,
                "could not reserve the configured {requested}-record memory ring index"
            ),
        }
    }
}

impl std::error::Error for RingLimitsError {}

/// Provider-neutral fixed request and retained-resource bounds.
///
/// The live record and payload limits are charged against retained resources.
/// Records covered by an accepted trim remain charged until a successful sync
/// makes that trim durable and permits reclamation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RingLimits {
    pub max_record_bytes: usize,
    pub max_live_records: usize,
    pub max_live_payload_bytes: usize,
    pub max_read_records: usize,
    pub max_read_bytes: usize,
    pub max_batch_records: usize,
    pub max_batch_bytes: usize,
}

impl RingLimits {
    /// Checks that every configured record can be appended and read with the
    /// implementation's maximum batch and read budgets.
    pub fn validate(self) -> Result<(), RingLimitsError> {
        let limits = [
            (RingLimit::RecordBytes, self.max_record_bytes),
            (RingLimit::LiveRecords, self.max_live_records),
            (RingLimit::LivePayloadBytes, self.max_live_payload_bytes),
            (RingLimit::ReadRecords, self.max_read_records),
            (RingLimit::ReadBytes, self.max_read_bytes),
            (RingLimit::BatchRecords, self.max_batch_records),
            (RingLimit::BatchBytes, self.max_batch_bytes),
        ];
        for (limit, value) in limits {
            if value == 0 {
                return Err(RingLimitsError::ZeroLimit { limit });
            }
        }

        let relations = [
            (
                RingLimit::RecordBytes,
                self.max_record_bytes,
                RingLimit::LivePayloadBytes,
                self.max_live_payload_bytes,
            ),
            (
                RingLimit::RecordBytes,
                self.max_record_bytes,
                RingLimit::ReadBytes,
                self.max_read_bytes,
            ),
            (
                RingLimit::RecordBytes,
                self.max_record_bytes,
                RingLimit::BatchBytes,
                self.max_batch_bytes,
            ),
            (
                RingLimit::BatchRecords,
                self.max_batch_records,
                RingLimit::LiveRecords,
                self.max_live_records,
            ),
            (
                RingLimit::BatchBytes,
                self.max_batch_bytes,
                RingLimit::LivePayloadBytes,
                self.max_live_payload_bytes,
            ),
        ];
        for (smaller, smaller_value, larger, larger_value) in relations {
            if smaller_value > larger_value {
                return Err(RingLimitsError::InconsistentLimits {
                    smaller,
                    smaller_value,
                    larger,
                    larger_value,
                });
            }
        }
        Ok(())
    }
}

impl Default for RingLimits {
    fn default() -> Self {
        Self {
            max_record_bytes: 128 * 1_024,
            max_live_records: 4_096,
            max_live_payload_bytes: 16 * 1_024 * 1_024,
            max_read_records: 256,
            max_read_bytes: 1_024 * 1_024,
            max_batch_records: 64,
            max_batch_bytes: 1_024 * 1_024,
        }
    }
}

/// An owned, all-or-nothing bounded append batch.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AppendRequest {
    pub records: Vec<Vec<u8>>,
    /// If set, acceptance succeeds only when this equals the accepted tail.
    /// The comparison and whole-batch acceptance are one atomic ring action.
    pub expected_accepted_tail: Option<RingCursor>,
}

impl AppendRequest {
    #[must_use]
    pub fn new(records: Vec<Vec<u8>>) -> Self {
        Self {
            records,
            expected_accepted_tail: None,
        }
    }

    #[must_use]
    pub fn expecting(mut self, accepted_tail: RingCursor) -> Self {
        self.expected_accepted_tail = Some(accepted_tail);
        self
    }
}

/// A successful batch append. All input buffers return to the caller.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AppendSuccess {
    pub first_position: RingPosition,
    pub next_cursor: RingCursor,
    pub records: Vec<Vec<u8>>,
}

/// Dense logical positions assigned to one accepted append batch.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct AppendRange {
    pub first_position: RingPosition,
    pub next_cursor: RingCursor,
}

/// A failed batch append. All input buffers return to the caller.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AppendFailure {
    pub error: RingError,
    pub records: Vec<Vec<u8>>,
    /// Assigned positions when the failure certainty is `Applied`.
    ///
    /// This is `None` for `NotApplied`. A `MayHaveApplied` failure may carry a
    /// range when the provider knows the candidate positions but cannot prove
    /// whether the whole batch was accepted.
    pub accepted_range: Option<AppendRange>,
}

impl fmt::Display for AppendFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.error.fmt(formatter)
    }
}

impl std::error::Error for AppendFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.error)
    }
}

/// One bounded durable read request.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ReadRequest {
    pub cursor: RingCursor,
    pub max_records: usize,
    pub max_bytes: usize,
}

impl ReadRequest {
    #[must_use]
    pub const fn new(cursor: RingCursor, max_records: usize, max_bytes: usize) -> Self {
        Self {
            cursor,
            max_records,
            max_bytes,
        }
    }
}

/// One copied durable record.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RingRecord {
    pub position: RingPosition,
    pub buffer: Vec<u8>,
}

/// A bounded page of copied durable records.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReadPage {
    pub records: Vec<RingRecord>,
    /// Cursor immediately after the final returned record. It is unchanged
    /// when the page is empty.
    pub next_cursor: RingCursor,
    /// Whether another durable record exists at `next_cursor`.
    pub has_more: bool,
    pub payload_bytes: usize,
}

/// Successful accepted-head advancement.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TrimSuccess {
    pub accepted_head: RingCursor,
}

/// Successful durability fence and reclamation checkpoint.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SyncSuccess {
    pub durable_head: RingCursor,
    pub durable_tail: RingCursor,
    pub reclaimed_records: usize,
    pub reclaimed_payload_bytes: usize,
}

/// A failed durability fence with any checkpoint known to have applied.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SyncFailure {
    pub error: RingError,
    /// The atomic checkpoint installed by an `Applied` failure.
    ///
    /// This is `None` for `NotApplied`. A `MayHaveApplied` failure may carry
    /// the candidate checkpoint while requiring reopen to determine whether it
    /// or the prior checkpoint won.
    pub checkpoint: Option<SyncSuccess>,
}

impl fmt::Display for SyncFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.error.fmt(formatter)
    }
}

impl std::error::Error for SyncFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.error)
    }
}

/// Deterministic bounds and retained-resource diagnostics.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RingStatus {
    pub accepted_head: RingCursor,
    pub accepted_tail: RingCursor,
    pub durable_head: RingCursor,
    pub durable_tail: RingCursor,
    pub accepted_live_records: usize,
    pub accepted_live_payload_bytes: usize,
    pub retained_records: usize,
    pub retained_payload_bytes: usize,
    pub pending_reclaim_records: usize,
    pub pending_reclaim_payload_bytes: usize,
    pub max_live_records: usize,
    pub max_live_payload_bytes: usize,
    /// Provider-specific physical diagnostics. Memory-only providers report
    /// `None`; fixed-file providers report their circular allocation state.
    pub physical: Option<RingPhysicalStatus>,
}

/// Physical allocation and checkpoint diagnostics for a disk-backed ring.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RingPhysicalStatus {
    pub data_capacity_bytes: u64,
    /// Bytes protected from overwrite by the last durable head through the
    /// accepted allocator tail, including wrap padding.
    pub protected_bytes: u64,
    pub free_bytes: u64,
    pub durable_head_offset: u64,
    pub durable_tail_offset: u64,
    pub accepted_tail_offset: u64,
    pub metadata_generation: u64,
    pub recovery_required: bool,
}

/// Ring operation used in provider-level failures.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RingOperation {
    Read,
    Status,
    Append,
    Trim,
    Sync,
}

/// A rejected or failed ring operation.
#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum RingError {
    EmptyBatch,
    BatchRecordLimitExceeded {
        requested: usize,
        limit: usize,
    },
    BatchByteLimitExceeded {
        requested: usize,
        limit: usize,
    },
    RecordTooLarge {
        index: usize,
        size: usize,
        limit: usize,
    },
    RecordCountOverflow,
    PayloadSizeOverflow,
    RecordCapacityReached {
        retained: usize,
        requested: usize,
        limit: usize,
    },
    PayloadCapacityReached {
        retained: usize,
        requested: usize,
        limit: usize,
    },
    PhysicalCapacityReached {
        protected: u64,
        requested: u64,
        limit: u64,
    },
    PositionExhausted,
    MetadataGenerationExhausted,
    PositionConflict {
        expected: RingCursor,
        actual: RingCursor,
    },
    ZeroReadRecordLimit,
    ZeroReadByteLimit,
    ReadRecordLimitExceeded {
        requested: usize,
        limit: usize,
    },
    ReadByteLimitExceeded {
        requested: usize,
        limit: usize,
    },
    ReadBudgetTooSmall {
        needed: usize,
        available: usize,
    },
    CursorExpired {
        requested: RingCursor,
        oldest: RingCursor,
    },
    TrimPastDurableTail {
        requested: RingCursor,
        durable_tail: RingCursor,
    },
    Backpressure {
        limit: usize,
    },
    BackendFailure {
        operation: RingOperation,
        raw_os_error: Option<i32>,
        message: String,
    },
    CorruptStorage {
        offset: u64,
        message: String,
    },
    RecoveryRequired,
}

impl fmt::Display for RingError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::EmptyBatch => formatter.write_str("ring append batch must not be empty"),
            Self::BatchRecordLimitExceeded { requested, limit } => write!(
                formatter,
                "ring append batch has {requested} records, exceeding limit {limit}"
            ),
            Self::BatchByteLimitExceeded { requested, limit } => write!(
                formatter,
                "ring append batch has {requested} payload bytes, exceeding limit {limit}"
            ),
            Self::RecordTooLarge { index, size, limit } => write!(
                formatter,
                "ring record {index} has {size} bytes, exceeding limit {limit}"
            ),
            Self::RecordCountOverflow => {
                formatter.write_str("ring retained record count overflowed")
            }
            Self::PayloadSizeOverflow => {
                formatter.write_str("ring append payload byte count overflowed")
            }
            Self::RecordCapacityReached {
                retained,
                requested,
                limit,
            } => write!(
                formatter,
                "ring retains {retained} records and cannot reserve {requested} more within limit {limit}"
            ),
            Self::PayloadCapacityReached {
                retained,
                requested,
                limit,
            } => write!(
                formatter,
                "ring retains {retained} payload bytes and cannot reserve {requested} more within limit {limit}"
            ),
            Self::PhysicalCapacityReached {
                protected,
                requested,
                limit,
            } => write!(
                formatter,
                "ring protects {protected} physical bytes and cannot reserve {requested} more within limit {limit}"
            ),
            Self::PositionExhausted => formatter.write_str("ring position space exhausted"),
            Self::MetadataGenerationExhausted => {
                formatter.write_str("ring metadata generation space exhausted")
            }
            Self::PositionConflict { expected, actual } => write!(
                formatter,
                "ring accepted-tail conflict: expected {}, actual {}",
                expected.get(),
                actual.get()
            ),
            Self::ZeroReadRecordLimit => {
                formatter.write_str("ring read record limit must be non-zero")
            }
            Self::ZeroReadByteLimit => formatter.write_str("ring read byte limit must be non-zero"),
            Self::ReadRecordLimitExceeded { requested, limit } => write!(
                formatter,
                "ring read record limit {requested} exceeds fixed limit {limit}"
            ),
            Self::ReadByteLimitExceeded { requested, limit } => write!(
                formatter,
                "ring read byte limit {requested} exceeds fixed limit {limit}"
            ),
            Self::ReadBudgetTooSmall { needed, available } => write!(
                formatter,
                "next ring record needs {needed} bytes but the read budget is {available}"
            ),
            Self::CursorExpired { requested, oldest } => write!(
                formatter,
                "ring cursor {} expired; oldest durable cursor is {}",
                requested.get(),
                oldest.get()
            ),
            Self::TrimPastDurableTail {
                requested,
                durable_tail,
            } => write!(
                formatter,
                "ring trim cursor {} exceeds durable tail {}",
                requested.get(),
                durable_tail.get()
            ),
            Self::Backpressure { limit } => {
                write!(formatter, "ring in-flight capacity of {limit} was reached")
            }
            Self::BackendFailure {
                operation,
                raw_os_error,
                message,
            } => {
                write!(formatter, "ring {operation:?} backend failure")?;
                if let Some(code) = raw_os_error {
                    write!(formatter, " (OS error {code})")?;
                }
                write!(formatter, ": {message}")
            }
            Self::CorruptStorage { offset, message } => {
                write!(
                    formatter,
                    "corrupt ring storage at byte {offset}: {message}"
                )
            }
            Self::RecoveryRequired => formatter.write_str("ring requires close and recovery"),
        }
    }
}

impl std::error::Error for RingError {}

fn validate_append_request(
    request: &AppendRequest,
    limits: RingLimits,
) -> Result<usize, RingError> {
    if request.records.is_empty() {
        return Err(RingError::EmptyBatch);
    }
    if request.records.len() > limits.max_batch_records {
        return Err(RingError::BatchRecordLimitExceeded {
            requested: request.records.len(),
            limit: limits.max_batch_records,
        });
    }

    let mut payload_bytes = 0usize;
    for (index, record) in request.records.iter().enumerate() {
        if record.len() > limits.max_record_bytes {
            return Err(RingError::RecordTooLarge {
                index,
                size: record.len(),
                limit: limits.max_record_bytes,
            });
        }
        payload_bytes = payload_bytes
            .checked_add(record.len())
            .ok_or(RingError::PayloadSizeOverflow)?;
    }
    if payload_bytes > limits.max_batch_bytes {
        return Err(RingError::BatchByteLimitExceeded {
            requested: payload_bytes,
            limit: limits.max_batch_bytes,
        });
    }
    Ok(payload_bytes)
}

/// State transition granted by [`check_append_admission`].
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct AppendAdmission {
    /// Accepted tail after the whole batch, proven representable.
    next_cursor: RingCursor,
    /// Retained payload bytes after the whole batch, proven within limits.
    next_retained_payload_bytes: usize,
}

/// Checks one validated append batch against the shared logical admission
/// rules: optimistic-concurrency position conflict, retained record and
/// payload capacity, and position-space exhaustion.
///
/// Physical checks (circular allocation) remain provider-specific and run
/// after this logical admission.
fn check_append_admission(
    expected_accepted_tail: Option<RingCursor>,
    accepted_tail: RingCursor,
    records: &[Vec<u8>],
    payload_bytes: usize,
    retained_records: usize,
    retained_payload_bytes: usize,
    limits: RingLimits,
) -> Result<AppendAdmission, RingError> {
    if let Some(expected) = expected_accepted_tail
        && expected != accepted_tail
    {
        return Err(RingError::PositionConflict {
            expected,
            actual: accepted_tail,
        });
    }

    let next_retained = checked_record_count(retained_records, records.len())?;
    if next_retained > limits.max_live_records {
        return Err(RingError::RecordCapacityReached {
            retained: retained_records,
            requested: records.len(),
            limit: limits.max_live_records,
        });
    }
    let next_retained_payload_bytes = retained_payload_bytes
        .checked_add(payload_bytes)
        .ok_or(RingError::PayloadSizeOverflow)?;
    if next_retained_payload_bytes > limits.max_live_payload_bytes {
        return Err(RingError::PayloadCapacityReached {
            retained: retained_payload_bytes,
            requested: payload_bytes,
            limit: limits.max_live_payload_bytes,
        });
    }

    let batch_len = u64::try_from(records.len()).map_err(|_| RingError::PositionExhausted)?;
    let next_position = accepted_tail
        .get()
        .checked_add(batch_len)
        .ok_or(RingError::PositionExhausted)?;
    Ok(AppendAdmission {
        next_cursor: RingCursor::new(next_position),
        next_retained_payload_bytes,
    })
}

/// The canonical empty page for a cursor at or past the durable tail.
const fn empty_page(cursor: RingCursor) -> ReadPage {
    ReadPage {
        records: Vec::new(),
        next_cursor: cursor,
        has_more: false,
        payload_bytes: 0,
    }
}

/// Resolves a read cursor against the durable interval.
///
/// Returns the retained-index start and remaining durable record count, or
/// `None` when the cursor is at or past the durable tail and the read must
/// return [`empty_page`]. A cursor below the durable head is expired.
fn read_page_interval(
    cursor: RingCursor,
    durable_head: RingCursor,
    durable_tail: RingCursor,
) -> Result<Option<(usize, usize)>, RingError> {
    if cursor < durable_head {
        return Err(RingError::CursorExpired {
            requested: cursor,
            oldest: durable_head,
        });
    }
    if cursor >= durable_tail {
        return Ok(None);
    }
    let start = usize::try_from(cursor.get() - durable_head.get())
        .expect("durable ring cursor distance fits the bounded index");
    let durable_remaining = usize::try_from(durable_tail.get() - cursor.get())
        .expect("durable ring interval fits the bounded index");
    Ok(Some((start, durable_remaining)))
}

/// A budgeted read-page prefix planned by [`plan_read_page`].
struct ReadPagePlan {
    /// Number of leading records that fit both read budgets.
    take: usize,
    /// Exact payload bytes those records carry.
    payload_bytes: usize,
}

/// Applies the shared record-count and byte budgets to one durable interval.
///
/// `payload_lens` yields the payload length of each remaining durable record
/// in order. A first record that exceeds the byte budget is rejected so an
/// accepted read can never silently make no progress.
fn plan_read_page(
    payload_lens: impl Iterator<Item = usize>,
    max_records: usize,
    max_bytes: usize,
) -> Result<ReadPagePlan, RingError> {
    let mut take = 0usize;
    let mut payload_bytes = 0usize;
    for payload_len in payload_lens {
        if take == max_records {
            break;
        }
        let Some(next_payload_bytes) = payload_bytes.checked_add(payload_len) else {
            break;
        };
        if next_payload_bytes > max_bytes {
            if take == 0 {
                return Err(RingError::ReadBudgetTooSmall {
                    needed: payload_len,
                    available: max_bytes,
                });
            }
            break;
        }
        payload_bytes = next_payload_bytes;
        take += 1;
    }
    Ok(ReadPagePlan {
        take,
        payload_bytes,
    })
}

/// Reserves the exact output vector for one planned read page.
fn reserve_read_page(take: usize) -> Result<Vec<RingRecord>, RingError> {
    let mut records = Vec::new();
    records
        .try_reserve_exact(take)
        .map_err(|error| RingError::BackendFailure {
            operation: RingOperation::Read,
            raw_os_error: None,
            message: format!("could not reserve {take}-record read page: {error}"),
        })?;
    Ok(records)
}

/// Derives the shared next-cursor and has-more epilogue for one read page.
fn assemble_read_page(
    records: Vec<RingRecord>,
    cursor: RingCursor,
    durable_tail: RingCursor,
    payload_bytes: usize,
) -> ReadPage {
    let next_cursor = records
        .last()
        .map_or(cursor, |record| RingCursor::new(record.position.get() + 1));
    ReadPage {
        records,
        next_cursor,
        has_more: next_cursor < durable_tail,
        payload_bytes,
    }
}

/// Applies one accepted trim against the shared monotonic-head contract.
///
/// The head never moves backwards and never crosses the durable tail; a
/// too-far cursor is rejected without changing state.
fn apply_trim(
    before: RingCursor,
    durable_tail: RingCursor,
    accepted_head: &mut RingCursor,
) -> CompletionResult<TrimSuccess, RingError> {
    if before > durable_tail {
        return Err(CompletionError::not_applied(
            RingError::TrimPastDurableTail {
                requested: before,
                durable_tail,
            },
        ));
    }
    if before > *accepted_head {
        *accepted_head = before;
    }
    Ok(TrimSuccess {
        accepted_head: *accepted_head,
    })
}

fn validate_read_request(request: ReadRequest, limits: RingLimits) -> Result<(), RingError> {
    if request.max_records == 0 {
        return Err(RingError::ZeroReadRecordLimit);
    }
    if request.max_bytes == 0 {
        return Err(RingError::ZeroReadByteLimit);
    }
    if request.max_records > limits.max_read_records {
        return Err(RingError::ReadRecordLimitExceeded {
            requested: request.max_records,
            limit: limits.max_read_records,
        });
    }
    if request.max_bytes > limits.max_read_bytes {
        return Err(RingError::ReadByteLimitExceeded {
            requested: request.max_bytes,
            limit: limits.max_read_bytes,
        });
    }
    Ok(())
}

/// Owned, bounded durable record reads.
///
/// Every clone aliases one ring session. Calls successfully admitted through
/// any clone share one total order established at method invocation, not first
/// poll. For concurrent callers, successful queue insertion is the ordering
/// point; validation and backpressure rejections occur outside that order.
/// Returned futures own all response state and are `'static`. Dropping a future
/// abandons only its response; it is not a cancellation request for an admitted
/// operation. Dropping the last handle must drain admitted commands or complete
/// them with their documented certainty. Implementations with bounded admission
/// queues return `NotApplied` [`RingError::Backpressure`] when no slot is
/// available.
///
/// A [`RingCursor`] is scoped to one ring incarnation. Callers that persist a
/// cursor across destructive reformat or replacement must pair it with an
/// independently persisted incarnation identifier; sequence equality alone
/// does not identify the same ring.
pub trait RingReader: Clone + 'static {
    type ReadFuture: Future<Output = CompletionResult<ReadPage, RingError>> + 'static;
    type StatusFuture: Future<Output = CompletionResult<RingStatus, RingError>> + 'static;

    /// Reads a page from the durable interval only.
    ///
    /// A cursor below the durable head fails with `NotApplied`
    /// [`RingError::CursorExpired`]. A cursor at or above the durable tail
    /// returns an empty page; a past-tail cursor is not clamped backwards.
    fn read(&self, request: ReadRequest) -> Self::ReadFuture;

    /// Returns bounds and retained-resource use at this call's ordered turn.
    fn status(&self) -> Self::StatusFuture;
}

/// A [`RingReader`] handle and operation futures that may cross thread boundaries.
///
/// This companion trait is implemented automatically when the handle is
/// [`Send`] and [`Sync`] and both owned response futures are [`Send`]. Generic
/// multi-threaded runtime code should use this bound; deterministic and local
/// code can continue to use [`RingReader`] without requiring synchronization.
pub trait SendRingReader: RingReader<ReadFuture: Send, StatusFuture: Send> + Send + Sync {}

impl<T> SendRingReader for T where T: RingReader<ReadFuture: Send, StatusFuture: Send> + Send + Sync {}

/// Owned, bounded ring mutations with an explicit durability fence.
///
/// Append, trim, sync, read, and status share [`RingReader`]'s invocation
/// ordering. Thus a sync invoked after an append fences that append even if the
/// append future is never polled or is dropped.
pub trait RingWriter: RingReader {
    type AppendFuture: Future<Output = CompletionResult<AppendSuccess, AppendFailure>> + 'static;
    type TrimFuture: Future<Output = CompletionResult<TrimSuccess, RingError>> + 'static;
    type SyncFuture: Future<Output = CompletionResult<SyncSuccess, SyncFailure>> + 'static;

    /// Atomically accepts every record in one non-empty batch or none of them.
    ///
    /// Success and every failure return all input buffers. `NotApplied` means
    /// no record was accepted. An `Applied` failure carries its assigned range.
    /// `MayHaveApplied` requires recovery or application-level reconciliation
    /// before retrying and may carry the candidate range.
    fn append(&self, request: AppendRequest) -> Self::AppendFuture;

    /// Advances the accepted head to `before` without moving it backwards.
    ///
    /// The call is idempotent for cursors at or below the accepted head. It
    /// cannot trim beyond the durable tail observed at its ordered turn, so an
    /// unsynced record cannot be discarded by trim. Trimmed resources remain retained
    /// and unavailable to append until a later successful sync.
    fn trim(&self, before: RingCursor) -> Self::TrimFuture;

    /// Atomically makes the accepted head and tail durable.
    ///
    /// A successful fence also reclaims resources below the newly durable
    /// head. An `Applied` failure carries the installed checkpoint and the same
    /// reclamation guarantee as success. `NotApplied` leaves the accepted bounds
    /// pending for retry and the prior durable checkpoint recoverable.
    /// `MayHaveApplied` requires close and reopen and recovery must select either
    /// the prior or complete target checkpoint, never mixed bounds. Dropping the
    /// future does not roll back the fence.
    fn sync(&self) -> Self::SyncFuture;
}

/// A [`RingWriter`] whose handle and every operation future are sendable.
///
/// Implementations receive this marker automatically when they satisfy the
/// complete reader and writer contract. The trait adds no ordering or
/// concurrency guarantees beyond [`RingReader`] and [`RingWriter`]; it only
/// exposes thread-mobility properties to generic code.
pub trait SendRingWriter:
    RingWriter<AppendFuture: Send, TrimFuture: Send, SyncFuture: Send> + SendRingReader
{
}

impl<T> SendRingWriter for T where
    T: RingWriter<AppendFuture: Send, TrimFuture: Send, SyncFuture: Send> + SendRingReader
{
}

/// Result of simulating process loss and recovery in [`MemoryRing`].
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CrashResult {
    pub discarded_records: usize,
    pub discarded_payload_bytes: usize,
    pub reverted_trim_records: usize,
    pub reverted_trim_payload_bytes: usize,
}

struct MemoryRingState {
    config: RingLimits,
    records: VecDeque<RingRecord>,
    retained_payload_bytes: usize,
    accepted_head: RingCursor,
    accepted_tail: RingCursor,
    durable_head: RingCursor,
    durable_tail: RingCursor,
}

/// Cloneable, deterministic, thread-safe reference ring.
///
/// Clones share state through one mutex. Every operation completes immediately,
/// but its effect occurs during method invocation so the production
/// invocation-order and abandonment contract remains observable. Concurrent
/// invocation order is the mutex acquisition order and is intentionally
/// unspecified.
#[derive(Clone)]
pub struct MemoryRing {
    shared: Arc<Mutex<MemoryRingState>>,
}

impl MemoryRing {
    pub fn new(config: RingLimits) -> Result<Self, RingLimitsError> {
        config.validate()?;
        let mut records = VecDeque::new();
        records
            .try_reserve_exact(config.max_live_records)
            .map_err(|_| RingLimitsError::RecordIndexAllocationFailed {
                requested: config.max_live_records,
            })?;
        Ok(Self {
            shared: Arc::new(Mutex::new(MemoryRingState {
                config,
                records,
                retained_payload_bytes: 0,
                accepted_head: RingCursor::START,
                accepted_tail: RingCursor::START,
                durable_head: RingCursor::START,
                durable_tail: RingCursor::START,
            })),
        })
    }

    /// Drops the unsynced suffix and restores the last durable head.
    ///
    /// This models process loss followed by reopening the same durable ring.
    /// A pending trim is forgotten; durable records it covered become visible
    /// again. Positions assigned only to the discarded suffix may be reused.
    pub fn crash(&self) -> CrashResult {
        let mut state = lock_unpoisoned(&self.shared);
        let durable_head = state.durable_head;
        let durable_tail = state.durable_tail;
        let reverted_trim_end = state.accepted_head.min(durable_tail);
        let mut reverted_trim_records = 0;
        let mut reverted_trim_payload_bytes = 0;
        let mut discarded_records = 0;
        let mut discarded_payload_bytes = 0;

        for record in &state.records {
            let cursor = RingCursor::new(record.position.get());
            if cursor >= durable_head && cursor < reverted_trim_end {
                reverted_trim_records += 1;
                reverted_trim_payload_bytes += record.buffer.len();
            }
            if cursor >= durable_tail {
                discarded_records += 1;
                discarded_payload_bytes += record.buffer.len();
            }
        }
        while state
            .records
            .back()
            .is_some_and(|record| record.position.get() >= durable_tail.get())
        {
            let removed = state.records.pop_back().expect("back record exists");
            state.retained_payload_bytes -= removed.buffer.len();
        }
        state.accepted_head = durable_head;
        state.accepted_tail = durable_tail;

        CrashResult {
            discarded_records,
            discarded_payload_bytes,
            reverted_trim_records,
            reverted_trim_payload_bytes,
        }
    }

    fn append_now(&self, request: AppendRequest) -> CompletionResult<AppendSuccess, AppendFailure> {
        let mut state = lock_unpoisoned(&self.shared);
        let reject = |error, records| {
            CompletionError::not_applied(AppendFailure {
                error,
                records,
                accepted_range: None,
            })
        };
        let batch_payload_bytes = match validate_append_request(&request, state.config) {
            Ok(payload_bytes) => payload_bytes,
            Err(error) => return Err(reject(error, request.records)),
        };
        let AppendRequest {
            records,
            expected_accepted_tail,
        } = request;

        let actual = state.accepted_tail;
        let admission = match check_append_admission(
            expected_accepted_tail,
            actual,
            &records,
            batch_payload_bytes,
            state.records.len(),
            state.retained_payload_bytes,
            state.config,
        ) {
            Ok(admission) => admission,
            Err(error) => return Err(reject(error, records)),
        };

        let first_position = RingPosition::new(actual.get());
        for (index, record) in records.iter().enumerate() {
            let offset = u64::try_from(index).expect("admission checked the batch length");
            state.records.push_back(RingRecord {
                position: RingPosition::new(actual.get() + offset),
                buffer: record.clone(),
            });
        }
        state.retained_payload_bytes = admission.next_retained_payload_bytes;
        state.accepted_tail = admission.next_cursor;

        Ok(AppendSuccess {
            first_position,
            next_cursor: state.accepted_tail,
            records,
        })
    }

    fn read_now(&self, request: ReadRequest) -> CompletionResult<ReadPage, RingError> {
        let state = lock_unpoisoned(&self.shared);
        validate_read_request(request, state.config).map_err(CompletionError::not_applied)?;
        let interval = read_page_interval(request.cursor, state.durable_head, state.durable_tail)
            .map_err(CompletionError::not_applied)?;
        let Some((start, durable_remaining)) = interval else {
            return Ok(empty_page(request.cursor));
        };

        let plan = plan_read_page(
            state
                .records
                .iter()
                .skip(start)
                .take(durable_remaining)
                .map(|record| record.buffer.len()),
            request.max_records,
            request.max_bytes,
        )
        .map_err(CompletionError::not_applied)?;
        let mut records = reserve_read_page(plan.take).map_err(CompletionError::not_applied)?;
        records.extend(state.records.iter().skip(start).take(plan.take).cloned());

        Ok(assemble_read_page(
            records,
            request.cursor,
            state.durable_tail,
            plan.payload_bytes,
        ))
    }

    fn trim_now(&self, before: RingCursor) -> CompletionResult<TrimSuccess, RingError> {
        let mut state = lock_unpoisoned(&self.shared);
        let durable_tail = state.durable_tail;
        apply_trim(before, durable_tail, &mut state.accepted_head)
    }

    fn sync_now(&self) -> CompletionResult<SyncSuccess, SyncFailure> {
        let mut state = lock_unpoisoned(&self.shared);
        let mut reclaimed_records = 0;
        let mut reclaimed_payload_bytes = 0;
        while state
            .records
            .front()
            .is_some_and(|record| record.position.get() < state.accepted_head.get())
        {
            let removed = state.records.pop_front().expect("front record exists");
            reclaimed_records += 1;
            reclaimed_payload_bytes += removed.buffer.len();
            state.retained_payload_bytes -= removed.buffer.len();
        }
        state.durable_head = state.accepted_head;
        state.durable_tail = state.accepted_tail;

        Ok(SyncSuccess {
            durable_head: state.durable_head,
            durable_tail: state.durable_tail,
            reclaimed_records,
            reclaimed_payload_bytes,
        })
    }

    fn status_now(&self) -> CompletionResult<RingStatus, RingError> {
        let state = lock_unpoisoned(&self.shared);
        let mut accepted_live_records = 0;
        let mut accepted_live_payload_bytes = 0;
        let mut pending_reclaim_records = 0;
        let mut pending_reclaim_payload_bytes = 0;
        for record in &state.records {
            let position = record.position.get();
            if position >= state.accepted_head.get() && position < state.accepted_tail.get() {
                accepted_live_records += 1;
                accepted_live_payload_bytes += record.buffer.len();
            } else if position < state.accepted_head.get() {
                pending_reclaim_records += 1;
                pending_reclaim_payload_bytes += record.buffer.len();
            }
        }
        Ok(RingStatus {
            accepted_head: state.accepted_head,
            accepted_tail: state.accepted_tail,
            durable_head: state.durable_head,
            durable_tail: state.durable_tail,
            accepted_live_records,
            accepted_live_payload_bytes,
            retained_records: state.records.len(),
            retained_payload_bytes: state.retained_payload_bytes,
            pending_reclaim_records,
            pending_reclaim_payload_bytes,
            max_live_records: state.config.max_live_records,
            max_live_payload_bytes: state.config.max_live_payload_bytes,
            physical: None,
        })
    }
}

impl RingReader for MemoryRing {
    type ReadFuture = Ready<CompletionResult<ReadPage, RingError>>;
    type StatusFuture = Ready<CompletionResult<RingStatus, RingError>>;

    fn read(&self, request: ReadRequest) -> Self::ReadFuture {
        ready(self.read_now(request))
    }

    fn status(&self) -> Self::StatusFuture {
        ready(self.status_now())
    }
}

impl RingWriter for MemoryRing {
    type AppendFuture = Ready<CompletionResult<AppendSuccess, AppendFailure>>;
    type TrimFuture = Ready<CompletionResult<TrimSuccess, RingError>>;
    type SyncFuture = Ready<CompletionResult<SyncSuccess, SyncFailure>>;

    fn append(&self, request: AppendRequest) -> Self::AppendFuture {
        ready(self.append_now(request))
    }

    fn trim(&self, before: RingCursor) -> Self::TrimFuture {
        ready(self.trim_now(before))
    }

    fn sync(&self) -> Self::SyncFuture {
        ready(self.sync_now())
    }
}

fn lock_unpoisoned<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

fn checked_record_count(retained: usize, requested: usize) -> Result<usize, RingError> {
    retained
        .checked_add(requested)
        .ok_or(RingError::RecordCountOverflow)
}

#[cfg(test)]
mod tests {
    use std::task::{Context, Poll, Waker};

    use kr_runtime::CompletionCertainty;

    use super::*;

    fn complete<F: Future>(future: F) -> F::Output {
        let mut context = Context::from_waker(Waker::noop());
        let mut future = Box::pin(future);
        match future.as_mut().poll(&mut context) {
            Poll::Ready(output) => output,
            Poll::Pending => panic!("memory ring futures must be immediately ready"),
        }
    }

    fn limits() -> RingLimits {
        RingLimits {
            max_record_bytes: 5,
            max_live_records: 3,
            max_live_payload_bytes: 10,
            max_read_records: 2,
            max_read_bytes: 6,
            max_batch_records: 2,
            max_batch_bytes: 8,
        }
    }

    fn ring() -> MemoryRing {
        MemoryRing::new(limits()).unwrap()
    }

    fn append_one(ring: &MemoryRing, bytes: &[u8]) -> AppendSuccess {
        complete(ring.append(AppendRequest::new(vec![bytes.to_vec()]))).unwrap()
    }

    fn status(ring: &MemoryRing) -> RingStatus {
        complete(ring.status()).unwrap()
    }

    #[test]
    fn shared_limits_reject_zero_and_incoherent_bounds() {
        assert_eq!(RingLimits::default().validate(), Ok(()));

        let mut zero = limits();
        zero.max_record_bytes = 0;
        assert_eq!(
            zero.validate(),
            Err(RingLimitsError::ZeroLimit {
                limit: RingLimit::RecordBytes,
            })
        );

        let mut unreadable = limits();
        unreadable.max_read_bytes = 4;
        assert_eq!(
            unreadable.validate(),
            Err(RingLimitsError::InconsistentLimits {
                smaller: RingLimit::RecordBytes,
                smaller_value: 5,
                larger: RingLimit::ReadBytes,
                larger_value: 4,
            })
        );

        let mut oversized_batch = limits();
        oversized_batch.max_batch_records = 4;
        assert_eq!(
            MemoryRing::new(oversized_batch).err(),
            Some(RingLimitsError::InconsistentLimits {
                smaller: RingLimit::BatchRecords,
                smaller_value: 4,
                larger: RingLimit::LiveRecords,
                larger_value: 3,
            })
        );
    }

    #[test]
    fn positions_are_absolute_dense_and_checked() {
        assert_eq!(
            RingPosition::new(41).next_cursor(),
            Some(RingCursor::new(42))
        );
        assert_eq!(RingPosition::new(u64::MAX).next_cursor(), None);

        let ring = ring();
        let batch =
            complete(ring.append(AppendRequest::new(vec![b"a".to_vec(), b"bb".to_vec()]))).unwrap();
        assert_eq!(batch.first_position, RingPosition::new(0));
        assert_eq!(batch.next_cursor, RingCursor::new(2));
        let next = append_one(&ring, b"c");
        assert_eq!(next.first_position, RingPosition::new(2));
        assert_eq!(next.next_cursor, RingCursor::new(3));
    }

    #[test]
    fn atomic_batches_return_buffers_and_reads_expose_only_durable_pages() {
        let ring = ring();
        let buffers = vec![b"a".to_vec(), b"bb".to_vec()];
        let appended =
            complete(ring.append(AppendRequest::new(buffers.clone()).expecting(RingCursor::START)))
                .unwrap();
        assert_eq!(appended.records, buffers);
        append_one(&ring, b"ccc");

        let hidden = complete(ring.read(ReadRequest::new(RingCursor::START, 2, 6))).unwrap();
        assert!(hidden.records.is_empty());
        assert_eq!(hidden.next_cursor, RingCursor::START);
        assert!(!hidden.has_more);

        let synced = complete(ring.sync()).unwrap();
        assert_eq!(synced.durable_head, RingCursor::START);
        assert_eq!(synced.durable_tail, RingCursor::new(3));

        let first = complete(ring.read(ReadRequest::new(RingCursor::START, 2, 2))).unwrap();
        assert_eq!(
            first.records,
            vec![RingRecord {
                position: RingPosition::new(0),
                buffer: b"a".to_vec(),
            }]
        );
        assert_eq!(first.payload_bytes, 1);
        assert_eq!(first.next_cursor, RingCursor::new(1));
        assert!(first.has_more);

        let second = complete(ring.read(ReadRequest::new(first.next_cursor, 2, 6))).unwrap();
        assert_eq!(
            second.records,
            vec![
                RingRecord {
                    position: RingPosition::new(1),
                    buffer: b"bb".to_vec(),
                },
                RingRecord {
                    position: RingPosition::new(2),
                    buffer: b"ccc".to_vec(),
                },
            ]
        );
        assert_eq!(second.payload_bytes, 5);
        assert_eq!(second.next_cursor, RingCursor::new(3));
        assert!(!second.has_more);
    }

    #[test]
    fn append_validation_is_not_applied_atomic_and_returns_every_buffer() {
        let ring = ring();

        let empty = complete(ring.append(AppendRequest::new(Vec::new()))).unwrap_err();
        assert_eq!(empty.certainty(), CompletionCertainty::NotApplied);
        assert_eq!(empty.error().error, RingError::EmptyBatch);
        assert!(empty.error().records.is_empty());

        let too_many = vec![b"a".to_vec(), b"b".to_vec(), b"c".to_vec()];
        let error = complete(ring.append(AppendRequest::new(too_many.clone()))).unwrap_err();
        assert_eq!(error.certainty(), CompletionCertainty::NotApplied);
        assert_eq!(error.error().records, too_many);
        assert_eq!(
            error.error().error,
            RingError::BatchRecordLimitExceeded {
                requested: 3,
                limit: 2,
            }
        );

        let oversized = vec![b"ok".to_vec(), b"123456".to_vec()];
        let error = complete(ring.append(AppendRequest::new(oversized.clone()))).unwrap_err();
        assert_eq!(error.error().records, oversized);
        assert_eq!(
            error.error().error,
            RingError::RecordTooLarge {
                index: 1,
                size: 6,
                limit: 5,
            }
        );

        let too_many_bytes = vec![b"12345".to_vec(), b"1234".to_vec()];
        let error = complete(ring.append(AppendRequest::new(too_many_bytes.clone()))).unwrap_err();
        assert_eq!(error.error().records, too_many_bytes);
        assert_eq!(
            error.error().error,
            RingError::BatchByteLimitExceeded {
                requested: 9,
                limit: 8,
            }
        );

        append_one(&ring, b"x");
        let conflict_buffers = vec![b"y".to_vec(), b"z".to_vec()];
        let error = complete(
            ring.append(AppendRequest::new(conflict_buffers.clone()).expecting(RingCursor::START)),
        )
        .unwrap_err();
        assert_eq!(error.error().records, conflict_buffers);
        assert_eq!(
            error.error().error,
            RingError::PositionConflict {
                expected: RingCursor::START,
                actual: RingCursor::new(1),
            }
        );
        assert_eq!(status(&ring).accepted_tail, RingCursor::new(1));
    }

    #[test]
    fn accepted_trim_does_not_free_record_capacity_until_sync() {
        let ring = ring();
        complete(ring.append(AppendRequest::new(vec![b"a".to_vec(), b"b".to_vec()]))).unwrap();
        append_one(&ring, b"c");
        complete(ring.sync()).unwrap();
        complete(ring.trim(RingCursor::new(1))).unwrap();

        let pending = status(&ring);
        assert_eq!(pending.accepted_live_records, 2);
        assert_eq!(pending.retained_records, 3);
        assert_eq!(pending.pending_reclaim_records, 1);

        let buffer = vec![b"d".to_vec()];
        let full = complete(ring.append(AppendRequest::new(buffer.clone()))).unwrap_err();
        assert_eq!(full.certainty(), CompletionCertainty::NotApplied);
        assert_eq!(full.error().records, buffer);
        assert_eq!(
            full.error().error,
            RingError::RecordCapacityReached {
                retained: 3,
                requested: 1,
                limit: 3,
            }
        );

        let checkpoint = complete(ring.sync()).unwrap();
        assert_eq!(checkpoint.durable_head, RingCursor::new(1));
        assert_eq!(checkpoint.durable_tail, RingCursor::new(3));
        assert_eq!(checkpoint.reclaimed_records, 1);
        assert_eq!(checkpoint.reclaimed_payload_bytes, 1);

        let appended = append_one(&ring, b"d");
        assert_eq!(appended.first_position, RingPosition::new(3));
        assert_eq!(status(&ring).retained_records, 3);
    }

    #[test]
    fn accepted_trim_does_not_free_payload_capacity_until_sync() {
        let limits = RingLimits {
            max_record_bytes: 4,
            max_live_records: 5,
            max_live_payload_bytes: 6,
            max_read_records: 5,
            max_read_bytes: 6,
            max_batch_records: 2,
            max_batch_bytes: 6,
        };
        let ring = MemoryRing::new(limits).unwrap();
        complete(ring.append(AppendRequest::new(vec![b"1234".to_vec(), b"12".to_vec()]))).unwrap();
        complete(ring.sync()).unwrap();
        complete(ring.trim(RingCursor::new(1))).unwrap();

        let buffer = vec![b"x".to_vec()];
        let full = complete(ring.append(AppendRequest::new(buffer.clone()))).unwrap_err();
        assert_eq!(full.error().records, buffer);
        assert_eq!(
            full.error().error,
            RingError::PayloadCapacityReached {
                retained: 6,
                requested: 1,
                limit: 6,
            }
        );

        let sync = complete(ring.sync()).unwrap();
        assert_eq!(sync.reclaimed_payload_bytes, 4);
        append_one(&ring, b"x");
        assert_eq!(status(&ring).retained_payload_bytes, 3);
    }

    #[test]
    fn trim_is_durable_only_monotonic_and_bounded_by_durable_tail() {
        let ring = ring();
        complete(ring.append(AppendRequest::new(vec![b"a".to_vec(), b"b".to_vec()]))).unwrap();

        let unsynced = complete(ring.trim(RingCursor::new(1))).unwrap_err();
        assert_eq!(unsynced.certainty(), CompletionCertainty::NotApplied);
        assert_eq!(
            *unsynced.error(),
            RingError::TrimPastDurableTail {
                requested: RingCursor::new(1),
                durable_tail: RingCursor::START,
            }
        );

        complete(ring.sync()).unwrap();
        assert_eq!(
            complete(ring.trim(RingCursor::new(1))).unwrap(),
            TrimSuccess {
                accepted_head: RingCursor::new(1),
            }
        );
        assert_eq!(
            complete(ring.trim(RingCursor::START)).unwrap(),
            TrimSuccess {
                accepted_head: RingCursor::new(1),
            }
        );
        let past = complete(ring.trim(RingCursor::new(3))).unwrap_err();
        assert_eq!(past.certainty(), CompletionCertainty::NotApplied);
        assert_eq!(
            *past.error(),
            RingError::TrimPastDurableTail {
                requested: RingCursor::new(3),
                durable_tail: RingCursor::new(2),
            }
        );
    }

    #[test]
    fn reads_validate_both_bounds_report_stale_cursors_and_preserve_past_tail() {
        let ring = ring();
        append_one(&ring, b"12345");
        append_one(&ring, b"x");
        complete(ring.sync()).unwrap();

        let zero_records =
            complete(ring.read(ReadRequest::new(RingCursor::START, 0, 1))).unwrap_err();
        assert_eq!(zero_records.certainty(), CompletionCertainty::NotApplied);
        assert_eq!(*zero_records.error(), RingError::ZeroReadRecordLimit);
        let zero_bytes =
            complete(ring.read(ReadRequest::new(RingCursor::START, 1, 0))).unwrap_err();
        assert_eq!(*zero_bytes.error(), RingError::ZeroReadByteLimit);
        let too_many_records =
            complete(ring.read(ReadRequest::new(RingCursor::START, 3, 1))).unwrap_err();
        assert_eq!(
            *too_many_records.error(),
            RingError::ReadRecordLimitExceeded {
                requested: 3,
                limit: 2,
            }
        );
        let too_many_bytes =
            complete(ring.read(ReadRequest::new(RingCursor::START, 1, 7))).unwrap_err();
        assert_eq!(
            *too_many_bytes.error(),
            RingError::ReadByteLimitExceeded {
                requested: 7,
                limit: 6,
            }
        );

        let no_progress =
            complete(ring.read(ReadRequest::new(RingCursor::START, 2, 4))).unwrap_err();
        assert_eq!(no_progress.certainty(), CompletionCertainty::NotApplied);
        assert_eq!(
            *no_progress.error(),
            RingError::ReadBudgetTooSmall {
                needed: 5,
                available: 4,
            }
        );

        complete(ring.trim(RingCursor::new(1))).unwrap();
        complete(ring.sync()).unwrap();
        let stale = complete(ring.read(ReadRequest::new(RingCursor::START, 1, 6))).unwrap_err();
        assert_eq!(stale.certainty(), CompletionCertainty::NotApplied);
        assert_eq!(
            *stale.error(),
            RingError::CursorExpired {
                requested: RingCursor::START,
                oldest: RingCursor::new(1),
            }
        );

        let past = complete(ring.read(ReadRequest::new(RingCursor::new(99), 1, 6))).unwrap();
        assert!(past.records.is_empty());
        assert_eq!(past.next_cursor, RingCursor::new(99));
        assert!(!past.has_more);
    }

    #[test]
    fn crash_discards_unsynced_suffix_and_reverts_pending_trim() {
        let ring = ring();
        complete(ring.append(AppendRequest::new(vec![b"a".to_vec(), b"bb".to_vec()]))).unwrap();
        complete(ring.sync()).unwrap();
        complete(ring.trim(RingCursor::new(1))).unwrap();
        append_one(&ring, b"ccc");

        assert_eq!(
            ring.crash(),
            CrashResult {
                discarded_records: 1,
                discarded_payload_bytes: 3,
                reverted_trim_records: 1,
                reverted_trim_payload_bytes: 1,
            }
        );
        let recovered = status(&ring);
        assert_eq!(recovered.accepted_head, RingCursor::START);
        assert_eq!(recovered.accepted_tail, RingCursor::new(2));
        assert_eq!(recovered.durable_head, RingCursor::START);
        assert_eq!(recovered.durable_tail, RingCursor::new(2));
        assert_eq!(recovered.retained_records, 2);
        assert_eq!(recovered.retained_payload_bytes, 3);
        assert_eq!(recovered.pending_reclaim_records, 0);

        let page = complete(ring.read(ReadRequest::new(RingCursor::START, 2, 6))).unwrap();
        assert_eq!(page.records[0].buffer, b"a");
        assert_eq!(page.records[1].buffer, b"bb");
        let replacement = append_one(&ring, b"z");
        assert_eq!(replacement.first_position, RingPosition::new(2));
    }

    #[test]
    fn dropped_futures_abandon_responses_without_cancelling_ordered_effects() {
        let ring = ring();
        drop(ring.append(AppendRequest::new(vec![b"a".to_vec()])));
        drop(ring.sync());

        let durable = complete(ring.read(ReadRequest::new(RingCursor::START, 1, 6))).unwrap();
        assert_eq!(durable.records.len(), 1);
        assert_eq!(durable.records[0].buffer, b"a");

        drop(ring.trim(RingCursor::new(1)));
        let before_fence = status(&ring);
        assert_eq!(before_fence.accepted_head, RingCursor::new(1));
        assert_eq!(before_fence.durable_head, RingCursor::START);
        drop(ring.sync());

        let stale = complete(ring.read(ReadRequest::new(RingCursor::START, 1, 6))).unwrap_err();
        assert_eq!(
            *stale.error(),
            RingError::CursorExpired {
                requested: RingCursor::START,
                oldest: RingCursor::new(1),
            }
        );
    }

    #[test]
    fn status_accounts_for_live_retained_and_pending_reclaim_resources() {
        let ring = ring();
        complete(ring.append(AppendRequest::new(vec![b"aa".to_vec(), b"bbb".to_vec()]))).unwrap();
        complete(ring.sync()).unwrap();
        append_one(&ring, b"c");
        complete(ring.trim(RingCursor::new(1))).unwrap();

        assert_eq!(
            status(&ring),
            RingStatus {
                accepted_head: RingCursor::new(1),
                accepted_tail: RingCursor::new(3),
                durable_head: RingCursor::START,
                durable_tail: RingCursor::new(2),
                accepted_live_records: 2,
                accepted_live_payload_bytes: 4,
                retained_records: 3,
                retained_payload_bytes: 6,
                pending_reclaim_records: 1,
                pending_reclaim_payload_bytes: 2,
                max_live_records: 3,
                max_live_payload_bytes: 10,
                physical: None,
            }
        );
    }

    #[test]
    fn position_exhaustion_is_not_applied_and_preserves_buffers() {
        let ring = ring();
        {
            let mut state = lock_unpoisoned(&ring.shared);
            state.accepted_head = RingCursor::new(u64::MAX);
            state.accepted_tail = RingCursor::new(u64::MAX);
            state.durable_head = RingCursor::new(u64::MAX);
            state.durable_tail = RingCursor::new(u64::MAX);
        }
        let buffers = vec![b"x".to_vec()];
        let error = complete(ring.append(AppendRequest::new(buffers.clone()))).unwrap_err();
        assert_eq!(error.certainty(), CompletionCertainty::NotApplied);
        assert_eq!(error.error().error, RingError::PositionExhausted);
        assert_eq!(error.error().records, buffers);
        assert_eq!(status(&ring).accepted_tail, RingCursor::new(u64::MAX));
    }

    #[test]
    fn retained_record_count_overflow_has_its_own_error() {
        assert_eq!(
            checked_record_count(usize::MAX, 1),
            Err(RingError::RecordCountOverflow)
        );
        assert_eq!(
            RingError::RecordCountOverflow.to_string(),
            "ring retained record count overflowed"
        );
    }

    #[test]
    fn traits_and_futures_are_cloneable_owned_and_static() {
        fn assert_clone_static<T: Clone + 'static>() {}
        fn assert_future_static<F: Future + 'static>(_: F) {}
        fn assert_send_ring<T: SendRingWriter>() {}

        assert_clone_static::<MemoryRing>();
        assert_send_ring::<MemoryRing>();
        let ring = ring();
        assert_future_static(ring.status());
        assert_future_static(ring.read(ReadRequest::new(RingCursor::START, 1, 1)));
        assert_future_static(ring.append(AppendRequest::new(vec![b"x".to_vec()])));
        assert_future_static(ring.trim(RingCursor::START));
        assert_future_static(ring.sync());
    }

    #[derive(Clone)]
    struct WrongAppendCursorRing(MemoryRing);

    impl RingReader for WrongAppendCursorRing {
        type ReadFuture = Ready<CompletionResult<ReadPage, RingError>>;
        type StatusFuture = Ready<CompletionResult<RingStatus, RingError>>;

        fn read(&self, request: ReadRequest) -> Self::ReadFuture {
            ready(self.0.read_now(request))
        }

        fn status(&self) -> Self::StatusFuture {
            ready(self.0.status_now())
        }
    }

    impl RingWriter for WrongAppendCursorRing {
        type AppendFuture = Ready<CompletionResult<AppendSuccess, AppendFailure>>;
        type TrimFuture = Ready<CompletionResult<TrimSuccess, RingError>>;
        type SyncFuture = Ready<CompletionResult<SyncSuccess, SyncFailure>>;

        fn append(&self, request: AppendRequest) -> Self::AppendFuture {
            let mut result = self.0.append_now(request);
            if let Ok(success) = &mut result {
                success.next_cursor = RingCursor::new(success.next_cursor.get() + 1);
            }
            ready(result)
        }

        fn trim(&self, before: RingCursor) -> Self::TrimFuture {
            ready(self.0.trim_now(before))
        }

        fn sync(&self) -> Self::SyncFuture {
            ready(self.0.sync_now())
        }
    }

    fn conformance_limits() -> RingLimits {
        RingLimits {
            max_record_bytes: 16,
            max_live_records: 8,
            max_live_payload_bytes: 64,
            max_read_records: 4,
            max_read_bytes: 32,
            max_batch_records: 4,
            max_batch_bytes: 32,
        }
    }

    #[test]
    fn shared_contract_suite_rejects_a_wrong_append_cursor_mutant() {
        let mutant = WrongAppendCursorRing(MemoryRing::new(conformance_limits()).unwrap());

        let error = complete(conformance::check_ring_contract(&mutant))
            .expect_err("contract checker accepted a deliberately advanced append cursor");

        assert!(
            error.contains("first append returned"),
            "mutant tripped an unrelated contract assertion: {error}"
        );
    }

    #[test]
    fn shared_contract_suite_passes_memory_ring() {
        let ring = MemoryRing::new(conformance_limits()).unwrap();

        complete(conformance::check_ring_contract(&ring)).unwrap();
    }
}
