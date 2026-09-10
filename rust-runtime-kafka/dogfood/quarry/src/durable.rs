//! Durable submit and acknowledgement semantics over an owned record ring.

use std::collections::BTreeSet;
use std::fmt;

use kr_runtime::{CompletionCertainty, CompletionError, CompletionResult, SimDuration, SimInstant};
use kr_runtime_ring::{
    AppendFailure, AppendRequest, ReadRequest, RingCursor, RingError, RingPosition, RingStatus,
    RingWriter, SyncSuccess,
};

use crate::engine::{AckPlan, InMemoryQueue, SubmitPlan};
use crate::record::Record;
use crate::types::{
    AckOutcome, JobId, LeaseToken, LeasedJob, NackOutcome, QueueConfig, QueueError, QueueSnapshot,
    RenewOutcome, SubmitOutcome, SubmitRequest, WorkerId,
};

/// Default bound on records examined during one recovery.
pub const DEFAULT_MAX_REPLAY_RECORDS: usize = 65_536;

/// Default byte budget for one bounded recovery read.
pub const DEFAULT_RECOVERY_READ_BYTES: usize = 1_024 * 1_024;

/// Fixed work and memory bounds for one recovery attempt.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RecoveryConfig {
    /// Maximum durable records requested from one read. This must not exceed
    /// the ring's configured `max_read_records`.
    pub read_batch_records: usize,
    /// Maximum durable payload bytes requested from one read. This must not
    /// exceed the ring's configured `max_read_bytes` and must be large enough
    /// for every retained Quarry record.
    pub read_batch_bytes: usize,
    /// Maximum total durable records decoded during the attempt.
    pub max_records: usize,
}

impl RecoveryConfig {
    #[must_use]
    pub const fn new(
        read_batch_records: usize,
        read_batch_bytes: usize,
        max_records: usize,
    ) -> Self {
        Self {
            read_batch_records,
            read_batch_bytes,
            max_records,
        }
    }
}

impl Default for RecoveryConfig {
    fn default() -> Self {
        Self::new(256, DEFAULT_RECOVERY_READ_BYTES, DEFAULT_MAX_REPLAY_RECORDS)
    }
}

/// A durable queue operation or recovery failure.
#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum DurableQueueError {
    /// The in-memory queue contract rejected the operation.
    Queue(QueueError),
    /// The ring rejected or failed an operation.
    Ring(RingError),
    /// An in-memory queue whose last mutation was uncertain must be discarded
    /// and reconstructed from its ring.
    RecoveryRequired,
    /// A logical record could not be encoded before submission.
    RecordEncoding { message: String },
    /// A durable record failed framing, checksum, or field validation.
    CorruptRecord {
        position: RingPosition,
        message: String,
    },
    /// Individually valid records do not form a valid Quarry history.
    InvalidHistory {
        position: Option<RingPosition>,
        message: String,
    },
    /// Recovery must use the exact resource bounds that created the ring.
    ConfigurationMismatch {
        expected: QueueConfig,
        found: QueueConfig,
    },
    /// No fresh lease-token incarnation can be allocated.
    IncarnationExhausted,
    /// Recovery requires nonzero read-page and total-record bounds.
    ZeroRecoveryLimit { field: &'static str },
    /// The durable history exceeded this attempt's total replay bound.
    ReplayRecordLimitExceeded { limit: usize },
}

impl fmt::Display for DurableQueueError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Queue(error) => error.fmt(formatter),
            Self::Ring(error) => error.fmt(formatter),
            Self::RecoveryRequired => formatter.write_str("durable queue requires ring recovery"),
            Self::RecordEncoding { message } => {
                write!(
                    formatter,
                    "could not encode Quarry durable record: {message}"
                )
            }
            Self::CorruptRecord { position, message } => write!(
                formatter,
                "corrupt Quarry durable record at position {}: {message}",
                position.get()
            ),
            Self::InvalidHistory { position, message } => {
                if let Some(position) = position {
                    write!(
                        formatter,
                        "invalid Quarry durable history at position {}: {message}",
                        position.get()
                    )
                } else {
                    write!(formatter, "invalid Quarry durable history: {message}")
                }
            }
            Self::ConfigurationMismatch { expected, found } => write!(
                formatter,
                "queue configuration does not match ring: expected {expected:?}, found {found:?}"
            ),
            Self::IncarnationExhausted => {
                formatter.write_str("lease-token incarnation space exhausted")
            }
            Self::ZeroRecoveryLimit { field } => {
                write!(formatter, "recovery {field} must be non-zero")
            }
            Self::ReplayRecordLimitExceeded { limit } => {
                write!(formatter, "ring exceeds recovery record limit {limit}")
            }
        }
    }
}

impl std::error::Error for DurableQueueError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Queue(error) => Some(error),
            Self::Ring(error) => Some(error),
            Self::RecoveryRequired
            | Self::RecordEncoding { .. }
            | Self::CorruptRecord { .. }
            | Self::InvalidHistory { .. }
            | Self::ConfigurationMismatch { .. }
            | Self::IncarnationExhausted
            | Self::ZeroRecoveryLimit { .. }
            | Self::ReplayRecordLimitExceeded { .. } => None,
        }
    }
}

impl From<QueueError> for DurableQueueError {
    fn from(error: QueueError) -> Self {
        Self::Queue(error)
    }
}

impl From<RingError> for DurableQueueError {
    fn from(error: RingError) -> Self {
        Self::Ring(error)
    }
}

/// Quarry state whose submitted and acknowledged jobs are stored durably.
///
/// `submit` and `ack` do not update visible memory until their record is behind
/// a successful sync fence. Claims, renewals, lease expiry, and negative
/// acknowledgements are deliberately ephemeral: recovery makes every active
/// job unleased and restores its original `not_before`, but not a later nack
/// delay.
///
/// Callers must supply monotonically nondecreasing virtual instants across all
/// operations and recoveries of the same ring. Recovery does not read a host
/// clock or invent an instant.
///
/// One ring must have exactly one live writer/recovery owner. Conditional
/// recovery appends reject simultaneous attempts that observed the same tail,
/// but they do not implement a distributed leadership lease. A stale owner can
/// still perform ephemeral lease operations until it attempts a tail-fenced
/// durable mutation.
pub struct DurableQueue<R> {
    ring: R,
    engine: InMemoryQueue,
    tail: RingCursor,
    recovery_required: bool,
}

impl<R> DurableQueue<R> {
    /// Consumes the queue and returns its ring handle.
    #[must_use]
    pub fn into_ring(self) -> R {
        self.ring
    }

    /// Returns the incarnation that fences newly issued lease tokens.
    #[must_use]
    pub const fn incarnation(&self) -> u64 {
        self.engine.incarnation()
    }

    /// Reports whether an abandoned or uncertain mutation requires recovery.
    #[must_use]
    pub const fn recovery_required(&self) -> bool {
        self.recovery_required
    }

    /// Claims eligible jobs. Leases are not persisted.
    pub fn claim(
        &mut self,
        worker_id: WorkerId,
        max_jobs: usize,
        lease_for: SimDuration,
        now: SimInstant,
    ) -> Result<Vec<LeasedJob>, DurableQueueError> {
        self.ensure_healthy()?;
        self.engine
            .claim(worker_id, max_jobs, lease_for, now)
            .map_err(Into::into)
    }

    /// Renews an ephemeral lease.
    pub fn renew(
        &mut self,
        job_id: JobId,
        token: LeaseToken,
        lease_for: SimDuration,
        now: SimInstant,
    ) -> Result<RenewOutcome, DurableQueueError> {
        self.ensure_healthy()?;
        self.engine
            .renew(job_id, token, lease_for, now)
            .map_err(Into::into)
    }

    /// Releases an ephemeral lease and applies a non-durable retry delay.
    pub fn nack(
        &mut self,
        job_id: JobId,
        token: LeaseToken,
        retry_after: SimDuration,
        now: SimInstant,
    ) -> Result<NackOutcome, DurableQueueError> {
        self.ensure_healthy()?;
        self.engine
            .nack(job_id, token, retry_after, now)
            .map_err(Into::into)
    }

    /// Applies one ephemeral lease expiry if its token and deadline still match.
    pub fn expire(
        &mut self,
        job_id: JobId,
        token: LeaseToken,
        now: SimInstant,
    ) -> Result<bool, DurableQueueError> {
        self.ensure_healthy()?;
        Ok(self.engine.expire(job_id, token, now))
    }

    /// Returns semantic state after expiring due leases.
    pub fn snapshot(&mut self, now: SimInstant) -> Result<QueueSnapshot, DurableQueueError> {
        self.ensure_healthy()?;
        Ok(self.engine.snapshot(now))
    }

    fn ensure_healthy(&self) -> Result<(), DurableQueueError> {
        if self.recovery_required {
            Err(DurableQueueError::RecoveryRequired)
        } else {
            Ok(())
        }
    }
}

impl<R> DurableQueue<R>
where
    R: RingWriter,
{
    /// Reconstructs a queue from the durable ring and starts a fresh token
    /// incarnation.
    ///
    /// Recovery first syncs any accepted suffix left by an abandoned operation.
    /// It then reads bounded pages, validates the complete replay grammar,
    /// and conditionally appends a new incarnation marker at the observed tail.
    ///
    /// Quarry currently needs its complete history, so the ring head must still
    /// be at [`RingCursor::START`]. The queue never trims the ring.
    pub async fn recover(
        config: QueueConfig,
        ring: R,
        recovery: RecoveryConfig,
    ) -> CompletionResult<Self, DurableQueueError> {
        if recovery.read_batch_records == 0 {
            return Err(CompletionError::not_applied(
                DurableQueueError::ZeroRecoveryLimit {
                    field: "read_batch_records",
                },
            ));
        }
        if recovery.read_batch_bytes == 0 {
            return Err(CompletionError::not_applied(
                DurableQueueError::ZeroRecoveryLimit {
                    field: "read_batch_bytes",
                },
            ));
        }
        if recovery.max_records == 0 {
            return Err(CompletionError::not_applied(
                DurableQueueError::ZeroRecoveryLimit {
                    field: "max_records",
                },
            ));
        }

        let status = ring.status().await.map_err(|error| error.map(Into::into))?;
        ensure_untrimmed_status(status).map_err(CompletionError::not_applied)?;

        let fenced_tail = match ring.sync().await {
            Ok(checkpoint) => {
                untrimmed_checkpoint_tail(checkpoint).map_err(CompletionError::may_have_applied)?
            }
            Err(error) if error.certainty() == CompletionCertainty::Applied => {
                let (_, failure) = error.into_parts();
                let checkpoint = failure.checkpoint.ok_or_else(|| {
                    CompletionError::may_have_applied(invalid_history(
                        "applied recovery fence omitted its ring checkpoint",
                    ))
                })?;
                untrimmed_checkpoint_tail(checkpoint).map_err(CompletionError::may_have_applied)?
            }
            Err(error) => {
                return Err(error.map(|failure| DurableQueueError::Ring(failure.error)));
            }
        };

        let (mut replay, mut tail) = replay(&ring, config, recovery, fenced_tail)
            .await
            .map_err(recovery_error_after_fence)?;
        let prior_recovery_effect = true;

        if !replay.configured {
            let encoded = encode_for_recovery(Record::Configure { config }, prior_recovery_effect)?;
            tail = append_for_recovery(&ring, encoded, tail, prior_recovery_effect).await?;
        }

        let incarnation =
            next_incarnation(replay.incarnation).map_err(CompletionError::may_have_applied)?;
        let encoded = encode_for_recovery(
            Record::BeginIncarnation { id: incarnation },
            prior_recovery_effect,
        )?;
        tail = append_for_recovery(&ring, encoded, tail, prior_recovery_effect).await?;

        match ring.sync().await {
            Ok(checkpoint) => match checkpoint_coverage(checkpoint, tail) {
                CheckpointCoverage::Exact => {}
                CheckpointCoverage::IncludesExpected => {
                    return Err(CompletionError::applied(unexpected_checkpoint(
                        "recovery fence",
                        checkpoint,
                        tail,
                    )));
                }
                CheckpointCoverage::ExcludesExpected => {
                    return Err(CompletionError::may_have_applied(unexpected_checkpoint(
                        "recovery fence",
                        checkpoint,
                        tail,
                    )));
                }
            },
            Err(error) if error.certainty() == CompletionCertainty::Applied => {
                let (_, failure) = error.into_parts();
                let Some(checkpoint) = failure.checkpoint else {
                    return Err(CompletionError::may_have_applied(invalid_history(
                        "applied recovery fence omitted its ring checkpoint",
                    )));
                };
                return match checkpoint_coverage(checkpoint, tail) {
                    CheckpointCoverage::Exact => Err(CompletionError::applied(
                        DurableQueueError::Ring(failure.error),
                    )),
                    CheckpointCoverage::IncludesExpected => Err(CompletionError::applied(
                        unexpected_checkpoint("recovery fence", checkpoint, tail),
                    )),
                    CheckpointCoverage::ExcludesExpected => Err(CompletionError::may_have_applied(
                        unexpected_checkpoint("recovery fence", checkpoint, tail),
                    )),
                };
            }
            Err(error) => {
                let (_, failure) = error.into_parts();
                return Err(CompletionError::may_have_applied(DurableQueueError::Ring(
                    failure.error,
                )));
            }
        }

        replay.engine.begin_incarnation(incarnation);
        Ok(Self {
            ring,
            engine: replay.engine,
            tail,
            recovery_required: false,
        })
    }

    /// Durably submits or deduplicates a request.
    pub async fn submit(
        &mut self,
        request: SubmitRequest,
        now: SimInstant,
    ) -> CompletionResult<SubmitOutcome, DurableQueueError> {
        if let Err(error) = self.ensure_healthy() {
            return Err(CompletionError::not_applied(error));
        }

        let plan = match self.engine.plan_submit(request) {
            Ok(SubmitPlan::Immediate(outcome)) => {
                self.engine.expire_due(now);
                return Ok(outcome);
            }
            Ok(SubmitPlan::Insert(plan)) => plan,
            Err(error) => {
                self.engine.expire_due(now);
                return Err(CompletionError::not_applied(error.into()));
            }
        };
        let encoded = Record::encode_submit(
            plan.request_id(),
            plan.job_id(),
            plan.payload(),
            plan.not_before(),
        )
        .map_err(|error| {
            CompletionError::not_applied(DurableQueueError::RecordEncoding {
                message: error.to_string(),
            })
        })?;
        self.persist(encoded, (now, plan), |engine, (now, plan)| {
            engine.expire_due(now);
            engine.apply_submit(plan)
        })
        .await
    }

    /// Durably acknowledges the job fenced by `token`.
    pub async fn ack(
        &mut self,
        job_id: JobId,
        token: LeaseToken,
        now: SimInstant,
    ) -> CompletionResult<AckOutcome, DurableQueueError> {
        if let Err(error) = self.ensure_healthy() {
            return Err(CompletionError::not_applied(error));
        }

        let plan = match self.engine.plan_ack(job_id, token, now) {
            Ok(AckPlan::Immediate(outcome)) => {
                self.engine.expire_due(now);
                return Ok(outcome);
            }
            Ok(AckPlan::Complete(plan)) => plan,
            Err(error) => {
                self.engine.expire_due(now);
                return Err(CompletionError::not_applied(error.into()));
            }
        };
        let encoded = Record::Ack {
            job_id,
            lease_token: token,
        }
        .encode()
        .map_err(|error| {
            CompletionError::not_applied(DurableQueueError::RecordEncoding {
                message: error.to_string(),
            })
        })?;
        self.persist(encoded, (now, plan), |engine, (now, plan)| {
            engine.expire_due(now);
            engine.apply_ack(plan)
        })
        .await
    }

    async fn persist<M, T>(
        &mut self,
        encoded: Vec<u8>,
        mutation: M,
        apply: impl FnOnce(&mut InMemoryQueue, M) -> T,
    ) -> CompletionResult<T, DurableQueueError> {
        // This flag is set before the first side-effecting await. If this
        // future is abandoned at either I/O boundary, the queue remains fenced.
        self.recovery_required = true;
        let expected_tail = self.tail;
        let accepted_tail = match self
            .ring
            .append(AppendRequest::new(vec![encoded]).expecting(expected_tail))
            .await
        {
            Ok(success)
                if success.first_position.get() == expected_tail.get()
                    && Some(success.next_cursor.get()) == expected_tail.get().checked_add(1) =>
            {
                success.next_cursor
            }
            Ok(success) => {
                return Err(CompletionError::may_have_applied(invalid_at(
                    success.first_position,
                    format!(
                        "conditional one-record append at {} returned range {}..{}",
                        expected_tail.get(),
                        success.first_position.get(),
                        success.next_cursor.get()
                    ),
                )));
            }
            Err(error) => {
                let (
                    certainty,
                    AppendFailure {
                        error,
                        records: _,
                        accepted_range: _,
                    },
                ) = error.into_parts();
                if certainty == CompletionCertainty::NotApplied {
                    self.recovery_required = matches!(
                        &error,
                        RingError::PositionConflict { .. } | RingError::RecoveryRequired
                    );
                    return Err(CompletionError::not_applied(error.into()));
                }
                return Err(CompletionError::may_have_applied(error.into()));
            }
        };

        match self.ring.sync().await {
            Ok(checkpoint) => match checkpoint_coverage(checkpoint, accepted_tail) {
                CheckpointCoverage::Exact => {
                    let outcome = apply(&mut self.engine, mutation);
                    self.tail = accepted_tail;
                    self.recovery_required = false;
                    Ok(outcome)
                }
                CheckpointCoverage::IncludesExpected => {
                    let _ = apply(&mut self.engine, mutation);
                    Err(CompletionError::applied(unexpected_checkpoint(
                        "mutation fence",
                        checkpoint,
                        accepted_tail,
                    )))
                }
                CheckpointCoverage::ExcludesExpected => Err(CompletionError::may_have_applied(
                    unexpected_checkpoint("mutation fence", checkpoint, accepted_tail),
                )),
            },
            Err(error) if error.certainty() == CompletionCertainty::Applied => {
                let (_, failure) = error.into_parts();
                let Some(checkpoint) = failure.checkpoint else {
                    return Err(CompletionError::may_have_applied(invalid_history(
                        "applied mutation fence omitted its ring checkpoint",
                    )));
                };
                match checkpoint_coverage(checkpoint, accepted_tail) {
                    CheckpointCoverage::Exact => {
                        let _ = apply(&mut self.engine, mutation);
                        self.tail = accepted_tail;
                        self.recovery_required = false;
                        Err(CompletionError::applied(failure.error.into()))
                    }
                    CheckpointCoverage::IncludesExpected => {
                        let _ = apply(&mut self.engine, mutation);
                        Err(CompletionError::applied(unexpected_checkpoint(
                            "mutation fence",
                            checkpoint,
                            accepted_tail,
                        )))
                    }
                    CheckpointCoverage::ExcludesExpected => Err(CompletionError::may_have_applied(
                        unexpected_checkpoint("mutation fence", checkpoint, accepted_tail),
                    )),
                }
            }
            Err(error) => {
                let (_, failure) = error.into_parts();
                Err(CompletionError::may_have_applied(failure.error.into()))
            }
        }
    }
}

fn recovery_error_after_fence(
    error: CompletionError<DurableQueueError>,
) -> CompletionError<DurableQueueError> {
    CompletionError::may_have_applied(error.into_parts().1)
}

struct ReplayState {
    engine: InMemoryQueue,
    configured: bool,
    incarnation: Option<u64>,
    acknowledged_tokens: BTreeSet<LeaseToken>,
}

fn ensure_untrimmed_status(status: RingStatus) -> Result<(), DurableQueueError> {
    if status.accepted_head == RingCursor::START && status.durable_head == RingCursor::START {
        Ok(())
    } else {
        Err(invalid_history(format!(
            "queue recovery requires an untrimmed ring, found accepted head {} and durable head {}",
            status.accepted_head.get(),
            status.durable_head.get()
        )))
    }
}

fn untrimmed_checkpoint_tail(checkpoint: SyncSuccess) -> Result<RingCursor, DurableQueueError> {
    if checkpoint.durable_head == RingCursor::START {
        Ok(checkpoint.durable_tail)
    } else {
        Err(invalid_history(format!(
            "queue recovery requires an untrimmed ring, fence installed head {}",
            checkpoint.durable_head.get()
        )))
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum CheckpointCoverage {
    Exact,
    IncludesExpected,
    ExcludesExpected,
}

fn checkpoint_coverage(checkpoint: SyncSuccess, expected_tail: RingCursor) -> CheckpointCoverage {
    if checkpoint.durable_head == RingCursor::START {
        match checkpoint.durable_tail.cmp(&expected_tail) {
            std::cmp::Ordering::Equal => CheckpointCoverage::Exact,
            std::cmp::Ordering::Greater => CheckpointCoverage::IncludesExpected,
            std::cmp::Ordering::Less => CheckpointCoverage::ExcludesExpected,
        }
    } else {
        CheckpointCoverage::ExcludesExpected
    }
}

fn unexpected_checkpoint(
    operation: &str,
    checkpoint: SyncSuccess,
    expected_tail: RingCursor,
) -> DurableQueueError {
    invalid_history(format!(
        "{operation} installed ring interval {}..{}, expected 0..{}",
        checkpoint.durable_head.get(),
        checkpoint.durable_tail.get(),
        expected_tail.get()
    ))
}

impl ReplayState {
    fn new(config: QueueConfig) -> Self {
        Self {
            engine: InMemoryQueue::new(config),
            configured: false,
            incarnation: None,
            acknowledged_tokens: BTreeSet::new(),
        }
    }

    fn apply(
        &mut self,
        desired_config: QueueConfig,
        position: RingPosition,
        record: Record,
    ) -> Result<(), DurableQueueError> {
        match record {
            Record::Configure { config } => {
                if self.configured || position != RingPosition::new(0) {
                    return Err(invalid_at(position, "configuration record is not first"));
                }
                if config != desired_config {
                    return Err(DurableQueueError::ConfigurationMismatch {
                        expected: config,
                        found: desired_config,
                    });
                }
                self.configured = true;
            }
            Record::BeginIncarnation { id } => {
                self.require_configured(position)?;
                let expected = next_incarnation(self.incarnation)?;
                if id != expected {
                    return Err(invalid_at(
                        position,
                        format!("incarnation {id} does not follow {expected}"),
                    ));
                }
                self.incarnation = Some(id);
            }
            Record::Submit {
                request_id,
                job_id,
                payload,
                not_before,
            } => {
                self.require_incarnation(position)?;
                let outcome = self
                    .engine
                    .replay_submit(SubmitRequest {
                        request_id,
                        payload,
                        not_before,
                    })
                    .map_err(|error| {
                        invalid_at(position, format!("submit replay failed: {error}"))
                    })?;
                if outcome != (SubmitOutcome::Submitted { job_id }) {
                    return Err(invalid_at(
                        position,
                        format!(
                            "submit replay allocated job {}, record names {job_id}",
                            outcome.job_id()
                        ),
                    ));
                }
            }
            Record::Ack {
                job_id,
                lease_token,
            } => {
                let incarnation = self.require_incarnation(position)?;
                if lease_token.incarnation() != incarnation {
                    return Err(invalid_at(
                        position,
                        format!(
                            "ack token incarnation {} does not match active incarnation {incarnation}",
                            lease_token.incarnation()
                        ),
                    ));
                }
                if lease_token.sequence() == u64::MAX {
                    return Err(invalid_at(
                        position,
                        "ack token sequence could never have been issued",
                    ));
                }
                if self.acknowledged_tokens.contains(&lease_token) {
                    return Err(invalid_at(
                        position,
                        format!("ack token {lease_token} is reused"),
                    ));
                }
                let outcome = self
                    .engine
                    .replay_ack(job_id, lease_token)
                    .map_err(|error| invalid_at(position, format!("ack replay failed: {error}")))?;
                if outcome != AckOutcome::Completed {
                    return Err(invalid_at(position, "duplicate acknowledgement record"));
                }
                self.acknowledged_tokens.insert(lease_token);
            }
        }
        Ok(())
    }

    fn require_configured(&self, position: RingPosition) -> Result<(), DurableQueueError> {
        if self.configured {
            Ok(())
        } else {
            Err(invalid_at(
                position,
                "mutation appears before queue configuration",
            ))
        }
    }

    fn require_incarnation(&self, position: RingPosition) -> Result<u64, DurableQueueError> {
        self.require_configured(position)?;
        self.incarnation
            .ok_or_else(|| invalid_at(position, "mutation appears before an incarnation marker"))
    }
}

async fn replay<R>(
    ring: &R,
    config: QueueConfig,
    recovery: RecoveryConfig,
    fenced_tail: RingCursor,
) -> CompletionResult<(ReplayState, RingCursor), DurableQueueError>
where
    R: RingWriter,
{
    let mut state = ReplayState::new(config);
    let mut cursor = RingCursor::START;
    let mut replayed_records = 0_usize;
    while cursor < fenced_tail {
        let remaining = fenced_tail.get() - cursor.get();
        let remaining = usize::try_from(remaining).unwrap_or(usize::MAX);
        let read_batch_records = recovery.read_batch_records.min(remaining);
        let page = ring
            .read(ReadRequest::new(
                cursor,
                read_batch_records,
                recovery.read_batch_bytes,
            ))
            .await
            .map_err(|error| {
                CompletionError::not_applied(DurableQueueError::Ring(error.into_parts().1))
            })?;
        if page.records.len() > read_batch_records {
            return Err(CompletionError::not_applied(invalid_history(format!(
                "read returned {} records for limit {}",
                page.records.len(),
                read_batch_records
            ))));
        }
        let actual_payload_bytes = page.records.iter().try_fold(0_usize, |total, record| {
            total.checked_add(record.buffer.len()).ok_or_else(|| {
                CompletionError::not_applied(DurableQueueError::Ring(
                    RingError::PayloadSizeOverflow,
                ))
            })
        })?;
        if page.payload_bytes != actual_payload_bytes {
            return Err(CompletionError::not_applied(invalid_history(format!(
                "read reported {} payload bytes for {} bytes of records",
                page.payload_bytes, actual_payload_bytes
            ))));
        }
        if actual_payload_bytes > recovery.read_batch_bytes {
            return Err(CompletionError::not_applied(invalid_history(format!(
                "read returned {} payload bytes for limit {}",
                actual_payload_bytes, recovery.read_batch_bytes
            ))));
        }

        let old_cursor = cursor;
        for ring_record in page.records {
            replayed_records = replayed_records.checked_add(1).ok_or_else(|| {
                CompletionError::not_applied(DurableQueueError::ReplayRecordLimitExceeded {
                    limit: recovery.max_records,
                })
            })?;
            if replayed_records > recovery.max_records {
                return Err(CompletionError::not_applied(
                    DurableQueueError::ReplayRecordLimitExceeded {
                        limit: recovery.max_records,
                    },
                ));
            }
            if ring_record.position.get() != cursor.get() {
                return Err(CompletionError::not_applied(invalid_at(
                    ring_record.position,
                    format!("expected ring position {}", cursor.get()),
                )));
            }
            if ring_record.position.get() >= fenced_tail.get() {
                return Err(CompletionError::not_applied(invalid_at(
                    ring_record.position,
                    format!("record lies beyond recovery fence {}", fenced_tail.get()),
                )));
            }
            let record = Record::decode(&ring_record.buffer).map_err(|error| {
                CompletionError::not_applied(DurableQueueError::CorruptRecord {
                    position: ring_record.position,
                    message: error.to_string(),
                })
            })?;
            state
                .apply(config, ring_record.position, record)
                .map_err(CompletionError::not_applied)?;
            cursor = ring_record.position.next_cursor().ok_or_else(|| {
                CompletionError::not_applied(DurableQueueError::Ring(RingError::PositionExhausted))
            })?;
        }

        if page.next_cursor != cursor {
            return Err(CompletionError::not_applied(invalid_history(format!(
                "read continuation is {}, expected {}",
                page.next_cursor.get(),
                cursor.get()
            ))));
        }
        if cursor == old_cursor {
            return Err(CompletionError::not_applied(invalid_history(
                "read did not make progress before the recovery fence",
            )));
        }
        if cursor < fenced_tail && !page.has_more {
            return Err(CompletionError::not_applied(invalid_history(format!(
                "read ended at {} before recovery fence {}",
                cursor.get(),
                fenced_tail.get()
            ))));
        }
    }

    Ok((state, cursor))
}

fn encode_for_recovery(
    record: Record,
    prior_append: bool,
) -> CompletionResult<Vec<u8>, DurableQueueError> {
    record.encode().map_err(|error| {
        let error = DurableQueueError::RecordEncoding {
            message: error.to_string(),
        };
        if prior_append {
            CompletionError::may_have_applied(error)
        } else {
            CompletionError::not_applied(error)
        }
    })
}

async fn append_for_recovery<R>(
    ring: &R,
    encoded: Vec<u8>,
    expected_tail: RingCursor,
    prior_append: bool,
) -> CompletionResult<RingCursor, DurableQueueError>
where
    R: RingWriter,
{
    match ring
        .append(AppendRequest::new(vec![encoded]).expecting(expected_tail))
        .await
    {
        Ok(success) => {
            let expected_next = expected_tail.get().checked_add(1);
            if success.first_position.get() != expected_tail.get()
                || Some(success.next_cursor.get()) != expected_next
            {
                return Err(CompletionError::may_have_applied(invalid_at(
                    success.first_position,
                    format!(
                        "conditional one-record append at {} returned range {}..{}",
                        expected_tail.get(),
                        success.first_position.get(),
                        success.next_cursor.get()
                    ),
                )));
            }
            Ok(success.next_cursor)
        }
        Err(error) => {
            let (
                certainty,
                AppendFailure {
                    error,
                    records: _,
                    accepted_range: _,
                },
            ) = error.into_parts();
            let error = DurableQueueError::Ring(error);
            if !prior_append && certainty == CompletionCertainty::NotApplied {
                Err(CompletionError::not_applied(error))
            } else {
                Err(CompletionError::may_have_applied(error))
            }
        }
    }
}

fn next_incarnation(previous: Option<u64>) -> Result<u64, DurableQueueError> {
    match previous {
        None => Ok(1),
        Some(previous) => previous
            .checked_add(1)
            .ok_or(DurableQueueError::IncarnationExhausted),
    }
}

fn invalid_at(position: RingPosition, message: impl Into<String>) -> DurableQueueError {
    DurableQueueError::InvalidHistory {
        position: Some(position),
        message: message.into(),
    }
}

fn invalid_history(message: impl Into<String>) -> DurableQueueError {
    DurableQueueError::InvalidHistory {
        position: None,
        message: message.into(),
    }
}

#[cfg(test)]
mod tests {
    use std::cell::RefCell;
    use std::future::{Future, Ready, ready};
    use std::rc::Rc;
    use std::task::{Context, Poll, Waker};

    use kr_runtime::SimRuntime;
    use kr_runtime_ring::{MemoryRing, ReadPage, RingLimits, RingReader, SyncFailure, TrimSuccess};

    use super::*;
    use crate::types::RequestId;

    fn run<T>(future: impl Future<Output = T> + 'static) -> T
    where
        T: 'static,
    {
        SimRuntime::default().block_on(future).unwrap()
    }

    fn recovery_config(read_batch_records: usize) -> RecoveryConfig {
        RecoveryConfig::new(
            read_batch_records,
            DEFAULT_RECOVERY_READ_BYTES,
            DEFAULT_MAX_REPLAY_RECORDS,
        )
    }

    fn ring_with(records: Vec<Record>) -> MemoryRing {
        let ring = MemoryRing::new(RingLimits::default()).unwrap();
        let writer = ring.clone();
        run(async move {
            for record in records {
                writer
                    .append(AppendRequest::new(vec![record.encode().unwrap()]))
                    .await
                    .unwrap();
            }
            writer.sync().await.unwrap();
        });
        ring
    }

    fn expect_recovery_error(
        result: CompletionResult<DurableQueue<MemoryRing>, DurableQueueError>,
    ) -> CompletionError<DurableQueueError> {
        match result {
            Ok(_) => panic!("recovery unexpectedly succeeded"),
            Err(error) => error,
        }
    }

    enum FirstReadAction {
        AppendAndSync(Vec<u8>),
        MisreportPayloadBytes,
    }

    #[derive(Clone)]
    struct MutatingReadRing {
        inner: MemoryRing,
        first_read: Rc<RefCell<Option<FirstReadAction>>>,
    }

    impl MutatingReadRing {
        fn new(inner: MemoryRing, action: FirstReadAction) -> Self {
            Self {
                inner,
                first_read: Rc::new(RefCell::new(Some(action))),
            }
        }
    }

    impl RingReader for MutatingReadRing {
        type ReadFuture = Ready<CompletionResult<ReadPage, RingError>>;
        type StatusFuture = Ready<CompletionResult<RingStatus, RingError>>;

        fn read(&self, request: ReadRequest) -> Self::ReadFuture {
            match self.first_read.borrow_mut().take() {
                Some(FirstReadAction::AppendAndSync(record)) => {
                    complete_ready(self.inner.append(AppendRequest::new(vec![record])))
                        .expect("concurrent test append succeeds");
                    complete_ready(self.inner.sync()).expect("concurrent test sync succeeds");
                    self.inner.read(request)
                }
                Some(FirstReadAction::MisreportPayloadBytes) => {
                    let mut result = complete_ready(self.inner.read(request));
                    if let Ok(page) = &mut result {
                        page.payload_bytes = page
                            .payload_bytes
                            .checked_add(1)
                            .expect("test payload accounting fits");
                    }
                    ready(result)
                }
                None => self.inner.read(request),
            }
        }

        fn status(&self) -> Self::StatusFuture {
            self.inner.status()
        }
    }

    impl RingWriter for MutatingReadRing {
        type AppendFuture = Ready<CompletionResult<kr_runtime_ring::AppendSuccess, AppendFailure>>;
        type TrimFuture = Ready<CompletionResult<TrimSuccess, RingError>>;
        type SyncFuture = Ready<CompletionResult<SyncSuccess, SyncFailure>>;

        fn append(&self, request: AppendRequest) -> Self::AppendFuture {
            self.inner.append(request)
        }

        fn trim(&self, before: RingCursor) -> Self::TrimFuture {
            self.inner.trim(before)
        }

        fn sync(&self) -> Self::SyncFuture {
            self.inner.sync()
        }
    }

    fn complete_ready<T>(future: Ready<T>) -> T {
        let mut future = Box::pin(future);
        match Future::poll(future.as_mut(), &mut Context::from_waker(Waker::noop())) {
            Poll::Ready(output) => output,
            Poll::Pending => unreachable!("std::future::Ready must complete immediately"),
        }
    }

    #[derive(Clone)]
    struct RejectingAppendRing {
        inner: MemoryRing,
        error: RingError,
    }

    impl RingReader for RejectingAppendRing {
        type ReadFuture = Ready<CompletionResult<ReadPage, RingError>>;
        type StatusFuture = Ready<CompletionResult<RingStatus, RingError>>;

        fn read(&self, request: ReadRequest) -> Self::ReadFuture {
            self.inner.read(request)
        }

        fn status(&self) -> Self::StatusFuture {
            self.inner.status()
        }
    }

    impl RingWriter for RejectingAppendRing {
        type AppendFuture = Ready<CompletionResult<kr_runtime_ring::AppendSuccess, AppendFailure>>;
        type TrimFuture = Ready<CompletionResult<TrimSuccess, RingError>>;
        type SyncFuture = Ready<CompletionResult<SyncSuccess, SyncFailure>>;

        fn append(&self, request: AppendRequest) -> Self::AppendFuture {
            ready(Err(CompletionError::not_applied(AppendFailure {
                error: self.error.clone(),
                records: request.records,
                accepted_range: None,
            })))
        }

        fn trim(&self, before: RingCursor) -> Self::TrimFuture {
            self.inner.trim(before)
        }

        fn sync(&self) -> Self::SyncFuture {
            self.inner.sync()
        }
    }

    #[test]
    fn recovery_required_append_error_preserves_queue_poison() {
        let expected = RingError::RecoveryRequired;
        let (error, recovery_required, snapshot) = run(async move {
            let mut queue = DurableQueue {
                ring: RejectingAppendRing {
                    inner: MemoryRing::new(RingLimits::default()).unwrap(),
                    error: RingError::RecoveryRequired,
                },
                engine: InMemoryQueue::new(QueueConfig::default()),
                tail: RingCursor::START,
                recovery_required: false,
            };
            let error = queue
                .submit(
                    SubmitRequest {
                        request_id: RequestId::new(1),
                        payload: b"job".to_vec(),
                        not_before: SimInstant::ZERO,
                    },
                    SimInstant::ZERO,
                )
                .await
                .expect_err("terminal ring rejection must fail submit");
            let recovery_required = queue.recovery_required();
            let snapshot = queue.snapshot(SimInstant::ZERO);
            (error, recovery_required, snapshot)
        });

        assert_eq!(error.certainty(), CompletionCertainty::NotApplied);
        assert_eq!(error.error(), &DurableQueueError::Ring(expected));
        assert!(recovery_required);
        assert_eq!(snapshot, Err(DurableQueueError::RecoveryRequired));
    }

    #[test]
    fn leading_fence_can_promote_a_corrupt_suffix_before_replay_fails() {
        let ring = MemoryRing::new(RingLimits::default()).unwrap();
        let writer = ring.clone();
        run(async move {
            writer
                .append(AppendRequest::new(vec![b"not a record".to_vec()]))
                .await
                .unwrap();
        });

        let before = run(ring.status()).unwrap();
        assert_eq!(before.accepted_tail, RingCursor::new(1));
        assert_eq!(before.durable_tail, RingCursor::START);
        let error = expect_recovery_error(run(DurableQueue::recover(
            QueueConfig::default(),
            ring.clone(),
            recovery_config(1),
        )));
        assert_eq!(error.certainty(), CompletionCertainty::MayHaveApplied);
        assert!(matches!(
            error.error(),
            DurableQueueError::CorruptRecord { position, .. }
                if *position == RingPosition::new(0)
        ));
        let after = run(ring.status()).unwrap();
        assert_eq!(after.accepted_tail, before.accepted_tail);
        assert_eq!(after.durable_tail, RingCursor::new(1));
    }

    #[test]
    fn replay_is_pinned_to_the_initial_durable_tail() {
        let config = QueueConfig::default();
        let inner = ring_with(vec![
            Record::Configure { config },
            Record::BeginIncarnation { id: 1 },
        ]);
        let ring = MutatingReadRing::new(
            inner.clone(),
            FirstReadAction::AppendAndSync(
                Record::BeginIncarnation { id: 2 }
                    .encode()
                    .expect("valid concurrent record"),
            ),
        );

        let error = match run(DurableQueue::recover(config, ring, recovery_config(1))) {
            Ok(_) => panic!("recovery absorbed a record beyond its initial fence"),
            Err(error) => error,
        };

        assert_eq!(error.certainty(), CompletionCertainty::MayHaveApplied);
        assert!(matches!(
            error.error(),
            DurableQueueError::Ring(RingError::PositionConflict { expected, actual })
                if *expected == RingCursor::new(2) && *actual == RingCursor::new(3)
        ));
        assert_eq!(
            run(inner.status()).unwrap().durable_tail,
            RingCursor::new(3)
        );
    }

    #[test]
    fn replay_verifies_reported_payload_bytes_against_record_buffers() {
        let config = QueueConfig::default();
        let inner = ring_with(vec![
            Record::Configure { config },
            Record::BeginIncarnation { id: 1 },
        ]);
        let ring = MutatingReadRing::new(inner, FirstReadAction::MisreportPayloadBytes);

        let error = match run(DurableQueue::recover(config, ring, recovery_config(1))) {
            Ok(_) => panic!("recovery trusted inconsistent payload accounting"),
            Err(error) => error,
        };

        assert_eq!(error.certainty(), CompletionCertainty::MayHaveApplied);
        assert!(matches!(
            error.error(),
            DurableQueueError::InvalidHistory { message, .. }
                if message.contains("payload bytes")
        ));
    }

    #[test]
    fn replay_rejects_invalid_incarnations_job_ids_and_ack_tokens() {
        let config = QueueConfig::default();
        let request = Record::Submit {
            request_id: RequestId::new(1),
            job_id: JobId::new(0),
            payload: b"job".to_vec(),
            not_before: SimInstant::ZERO,
        };
        let cases = [
            vec![
                Record::Configure { config },
                Record::BeginIncarnation { id: 2 },
            ],
            vec![
                Record::Configure { config },
                Record::BeginIncarnation { id: 1 },
                Record::Submit {
                    request_id: RequestId::new(1),
                    job_id: JobId::new(9),
                    payload: b"job".to_vec(),
                    not_before: SimInstant::ZERO,
                },
            ],
            vec![
                Record::Configure { config },
                Record::BeginIncarnation { id: 1 },
                request,
                Record::Ack {
                    job_id: JobId::new(0),
                    lease_token: LeaseToken::from_parts(2, 0),
                },
            ],
            vec![
                Record::Configure { config },
                Record::BeginIncarnation { id: 1 },
                Record::Submit {
                    request_id: RequestId::new(1),
                    job_id: JobId::new(0),
                    payload: b"one".to_vec(),
                    not_before: SimInstant::ZERO,
                },
                Record::Submit {
                    request_id: RequestId::new(2),
                    job_id: JobId::new(1),
                    payload: b"two".to_vec(),
                    not_before: SimInstant::ZERO,
                },
                Record::Ack {
                    job_id: JobId::new(0),
                    lease_token: LeaseToken::from_parts(1, 0),
                },
                Record::Ack {
                    job_id: JobId::new(1),
                    lease_token: LeaseToken::from_parts(1, 0),
                },
            ],
            vec![
                Record::Configure { config },
                Record::BeginIncarnation { id: 1 },
                Record::Submit {
                    request_id: RequestId::new(1),
                    job_id: JobId::new(0),
                    payload: b"job".to_vec(),
                    not_before: SimInstant::ZERO,
                },
                Record::Ack {
                    job_id: JobId::new(0),
                    lease_token: LeaseToken::from_parts(1, u64::MAX),
                },
            ],
        ];

        for records in cases {
            let ring = ring_with(records);
            let error =
                expect_recovery_error(run(DurableQueue::recover(config, ring, recovery_config(1))));
            assert_eq!(error.certainty(), CompletionCertainty::MayHaveApplied);
            assert!(matches!(
                error.error(),
                DurableQueueError::InvalidHistory { .. }
            ));
        }
    }

    #[test]
    fn incarnation_exhaustion_is_typed() {
        assert_eq!(
            next_incarnation(Some(u64::MAX)),
            Err(DurableQueueError::IncarnationExhausted)
        );
    }
}
