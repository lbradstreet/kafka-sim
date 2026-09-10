//! Partition-local batch formation and bounded compression, with explicit input
//! consumption and a separate unsequenced seal/finalization boundary.
use crate::{
    admission::{AdmittedRecord, RecordObligation},
    batching::CompressionEstimate,
    config::{BatchTargetMode, Compression, ProducerConfig},
    credit::{Claim, HeldCredits, Resource, SharedCredits},
    estimation::{DeadlineHeadroom, EncodeWork, EncodingCost, RoundTripTime},
    pool::Slot,
    types::{Progress, SealReason, TopicPartition, WorkBudget},
};
use kr_kafka_record::{
    self as record, CodecPool, FinalizedBatch, OutputPool, RecordBatchBuilder, SealedBatch,
};
use kr_runtime::{RuntimeDuration, RuntimeInstant};
use std::{
    fmt,
    sync::{Arc, Mutex},
};

/// Actual nonempty batch sealing observations. Indices are Target, Linger,
/// Flush (including close), HardLimit, Deadline, ContextReclaimed, Sparse, and
/// RequestGather.
/// `raw_bytes` always counts encoded raw record bytes. `target_bytes` sums the
/// configured soft targets in the selected mode; their ratio is raw occupancy,
/// not compressed wire fill. Records and dispatch pressure may cross a target.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct BatchSealStats {
    pub by_reason: [u64; 8],
    pub raw_bytes: u64,
    pub target_bytes: u64,
    pub overflowed: bool,
}
impl BatchSealStats {
    fn observe(&mut self, reason: SealReason, raw: u32, target: u32) {
        let index = match reason {
            SealReason::Target => 0,
            SealReason::Linger => 1,
            SealReason::Flush => 2,
            SealReason::HardLimit => 3,
            SealReason::Deadline => 4,
            SealReason::ContextReclaimed => 5,
            SealReason::Sparse => 6,
            SealReason::RequestGather => 7,
        };
        for (counter, amount) in [
            (&mut self.by_reason[index], 1),
            (&mut self.raw_bytes, u64::from(raw)),
            (&mut self.target_bytes, u64::from(target)),
        ] {
            match counter.checked_add(amount) {
                Some(value) => *counter = value,
                None => {
                    *counter = u64::MAX;
                    self.overflowed = true;
                }
            }
        }
    }
}
#[derive(Clone, Default)]
pub(crate) struct SealCounter(Arc<Mutex<BatchSealStats>>);
impl SealCounter {
    pub(crate) fn snapshot(&self) -> BatchSealStats {
        *self
            .0
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }
    fn observe(&self, reason: SealReason, raw: u32, target: u32) {
        self.0
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .observe(reason, raw, target);
    }
}
pub type BatchKey = Slot<Batch>;
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BatchState {
    Open,
    Sealing,
    Sealed,
    Ready,
    InFlight,
    Failed,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AppendError {
    Closed,
    WrongPartition,
    HardLimit,
    InvalidRecord,
    AllocationFailed,
}
/// The rejected descriptor remains intact, including all admission obligations.
pub struct RejectedRecord {
    pub reason: AppendError,
    pub record: AdmittedRecord,
}
impl fmt::Debug for RejectedRecord {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RejectedRecord")
            .field("reason", &self.reason)
            .field("token", &self.record.token)
            .finish()
    }
}

enum Payload {
    Encoding(RecordBatchBuilder),
    Sealed(SealedBatch),
    Finalized(FinalizedBatch),
}
/// Retains failed/terminal byte owners while the actor releases a bounded number
/// of input records and output chunks. Credits outlive the bytes they cover.
pub(crate) struct TerminalPayload {
    payload: Option<record::BatchAbort>,
    transform: Option<HeldCredits>,
    output: Option<Arc<HeldCredits>>,
}
impl TerminalPayload {
    pub(crate) fn abort_step(&mut self, maximum: usize) -> record::AbortProgress {
        if maximum == 0 {
            return record::AbortProgress {
                records_released: 0,
                chunks_released: 0,
                done: self.payload.is_none(),
            };
        }
        let Some(payload) = &mut self.payload else {
            return record::AbortProgress {
                records_released: 0,
                chunks_released: 0,
                done: true,
            };
        };
        let mut progress = payload.abort_step(maximum, 0);
        if progress.records_released == 0 && !progress.done {
            progress = payload.abort_step(0, maximum);
        }
        if let Some(transform) = &mut self.transform {
            transform.release(Resource::CodecContexts);
        }
        if progress.done {
            self.payload = None;
            self.transform = None;
            self.output = None;
        }
        progress
    }
}
/// Output credit follows both the retry owner and all admitted writes. The
/// provider receives a clone of this guard with each shared output segment.
pub struct RetryPayload {
    batch: FinalizedBatch,
    credit: Arc<HeldCredits>,
}
impl RetryPayload {
    #[must_use]
    pub fn chunks(&self) -> &[kr_shared_bytes::SharedBytes] {
        self.batch.chunks()
    }
    #[must_use]
    pub fn wire_bytes(&self) -> usize {
        self.batch.wire_bytes()
    }
    #[must_use]
    pub fn identity(&self) -> record::Identity {
        self.batch.identity()
    }
    #[must_use]
    pub fn credit_guard(&self) -> Arc<HeldCredits> {
        self.credit.clone()
    }
}

/// A batch is tied to a UUID/partition/lane from its first accepted record.
/// Reconnect, metadata refresh and retries cannot change that assignment.
pub struct Batch {
    pub(crate) partition: TopicPartition,
    pub(crate) lane: u8,
    pub(crate) records: Vec<RecordObligation>,
    consumed: usize,
    payload: Option<Payload>,
    state: BatchState,
    seal_reason: Option<SealReason>,
    sealed_at: Option<RuntimeInstant>,
    seal_observed: bool,
    wire_observed: bool,
    seal_counter: Option<SealCounter>,
    first_accepted: Option<RuntimeInstant>,
    oldest_deadline: Option<RuntimeInstant>,
    linger_at: Option<RuntimeInstant>,
    deadline_headroom: DeadlineHeadroom,
    last_encode_work: EncodeWork,
    dispatch_ready_at: RuntimeInstant,
    pub(crate) gather_attempted: bool,
    target: u32,
    target_mode: BatchTargetMode,
    compression_estimate: CompressionEstimate,
    hard: u32,
    threshold: u32,
    envelope: u32,
    compression: Compression,
    credits: SharedCredits,
    transform: Option<HeldCredits>,
    output_guard: Option<Arc<HeldCredits>>,
    failure: Option<record::Error>,
    encode_wait: Option<EncodeWait>,
    raw_bytes: u32,
}

pub(crate) struct SealObservation {
    pub first_accepted: RuntimeInstant,
    pub sealed_at: Option<RuntimeInstant>,
    pub raw_bytes: u32,
    pub records: usize,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum EncodeWait {
    CompressedCredit,
    CodecCredit,
    Output,
    Other,
}
impl fmt::Debug for Batch {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Batch")
            .field("partition", &self.partition)
            .field("lane", &self.lane)
            .field("state", &self.state)
            .field("records", &self.records.len())
            .field("raw_bytes", &self.raw_bytes)
            .field("seal_reason", &self.seal_reason)
            .finish()
    }
}
impl Batch {
    /// Configuration and topic routing must be validated before creating a batch.
    /// # Errors
    /// Rejects invalid codec/chunk configuration before any input is moved.
    pub fn new(
        config: &ProducerConfig,
        effective_payload: u32,
        partition: TopicPartition,
        lane: u8,
        output: OutputPool,
        credits: SharedCredits,
    ) -> Result<Self, record::Error> {
        let batch = record::BatchConfig {
            raw_limit: effective_payload,
            output_limit: effective_payload,
            chunk_bytes: config.output_chunk_bytes,
            progressive_threshold: config.progressive_threshold.min(effective_payload),
        };
        let codec = match config.compression {
            Compression::None => record::Compression::None,
            Compression::Zstd { level } => record::Compression::Zstd { level },
        };
        let mut builder = RecordBatchBuilder::new(batch, codec, output)?;
        if config.batch_target_mode == BatchTargetMode::EstimatedWire {
            builder.enable_tail_compaction();
        }
        Ok(Self {
            partition,
            lane,
            records: Vec::new(),
            consumed: 0,
            payload: Some(Payload::Encoding(builder)),
            state: BatchState::Open,
            seal_reason: None,
            sealed_at: None,
            seal_observed: false,
            wire_observed: false,
            seal_counter: None,
            first_accepted: None,
            oldest_deadline: None,
            linger_at: None,
            deadline_headroom: DeadlineHeadroom::new(
                EncodingCost::new(config.delivery_timeout),
                RoundTripTime::default(),
                config.request_timeout,
                config.delivery_timeout,
            )
            .map_err(|_| record::Error::InvalidConfig)?,
            last_encode_work: EncodeWork::default(),
            dispatch_ready_at: RuntimeInstant::ZERO,
            gather_attempted: false,
            target: config.batch_target_bytes,
            target_mode: config.batch_target_mode,
            compression_estimate: CompressionEstimate::default(),
            hard: effective_payload,
            threshold: config.progressive_threshold,
            envelope: batch.envelope_bytes(),
            compression: config.compression,
            credits,
            transform: None,
            output_guard: None,
            failure: None,
            encode_wait: None,
            raw_bytes: 0,
        })
    }
    #[must_use]
    pub fn state(&self) -> BatchState {
        self.state
    }
    #[must_use]
    pub fn partition(&self) -> TopicPartition {
        self.partition
    }
    #[must_use]
    pub fn lane(&self) -> u8 {
        self.lane
    }
    #[must_use]
    pub fn record_count(&self) -> usize {
        self.records.len()
    }
    #[must_use]
    pub fn raw_bytes(&self) -> u32 {
        self.raw_bytes
    }
    #[must_use]
    pub fn target_bytes(&self) -> u32 {
        self.target
    }
    pub(crate) fn set_compression_estimate(&mut self, estimate: CompressionEstimate) {
        debug_assert!(self.records.is_empty());
        self.compression_estimate = estimate;
    }
    /// An estimate of the complete batch, never just the bytes the compressor
    /// has emitted so far (it may still buffer all consumed input).
    pub fn estimated_wire_bytes(&self) -> u64 {
        let predicted = match self.compression {
            Compression::None => u64::from(self.raw_bytes) + 61,
            Compression::Zstd { .. } => self.compression_estimate.wire_bytes(self.raw_bytes),
        };
        let emitted = match self.payload.as_ref() {
            Some(Payload::Encoding(builder)) => builder.output_bytes(),
            Some(Payload::Sealed(batch)) => batch.wire_bytes(),
            Some(Payload::Finalized(batch)) => batch.wire_bytes(),
            None => 0,
        };
        predicted.max(emitted as u64)
    }
    fn target_reached(&self) -> bool {
        match self.target_mode {
            BatchTargetMode::Raw => self.raw_bytes >= self.target,
            BatchTargetMode::EstimatedWire => self.estimated_wire_bytes() >= u64::from(self.target),
        }
    }
    #[must_use]
    pub fn oldest_deadline(&self) -> Option<RuntimeInstant> {
        self.oldest_deadline
    }
    #[must_use]
    pub fn first_accepted(&self) -> Option<RuntimeInstant> {
        self.first_accepted
    }
    #[must_use]
    pub fn seal_reason(&self) -> Option<SealReason> {
        self.seal_reason
    }
    /// One-shot observations do not affect batch policy or ownership.
    pub(crate) fn take_seal_observation(&mut self) -> Option<SealObservation> {
        if self.seal_reason.is_none() || self.seal_observed {
            return None;
        }
        self.seal_observed = true;
        Some(SealObservation {
            first_accepted: self.first_accepted?,
            sealed_at: self.sealed_at,
            raw_bytes: self.raw_bytes,
            records: self.records.len(),
        })
    }
    pub(crate) fn take_wire_observation(&mut self) -> Option<usize> {
        if self.wire_observed {
            return None;
        }
        let bytes = self.wire_bytes()?;
        self.wire_observed = true;
        Some(bytes)
    }
    #[must_use]
    pub fn failure(&self) -> Option<record::Error> {
        self.failure
    }
    #[must_use]
    pub fn wire_bytes(&self) -> Option<usize> {
        match self.payload.as_ref()? {
            Payload::Encoding(_) => None,
            Payload::Sealed(b) => Some(b.wire_bytes()),
            Payload::Finalized(b) => Some(b.wire_bytes()),
        }
    }
    /// Checks the precise timestamp/offset-dependent encoded length before
    /// consuming input. The hard limit can seal the old batch without losing the
    /// descriptor that must start the next one.
    /// # Errors
    /// Returns the original admitted record with its complete credit ownership.
    #[allow(clippy::result_large_err)] // Returning ownership must not allocate on exhaustion.
    pub fn try_append(
        &mut self,
        record: AdmittedRecord,
        linger: RuntimeDuration,
    ) -> Result<(), RejectedRecord> {
        self.try_append_inner(record, linger, None)
    }
    /// Timed owner variant; the supplied time observes a target/hard seal at
    /// its actual transition, independently of later encoder or dispatch work.
    #[allow(clippy::result_large_err)]
    pub fn try_append_at(
        &mut self,
        record: AdmittedRecord,
        linger: RuntimeDuration,
        now: RuntimeInstant,
    ) -> Result<(), RejectedRecord> {
        self.try_append_inner(record, linger, Some(now))
    }
    #[allow(clippy::result_large_err)]
    fn try_append_inner(
        &mut self,
        record: AdmittedRecord,
        linger: RuntimeDuration,
        now: Option<RuntimeInstant>,
    ) -> Result<(), RejectedRecord> {
        let reject = |record, reason| Err(RejectedRecord { reason, record });
        if self.state != BatchState::Open {
            return reject(record, AppendError::Closed);
        }
        if record.lane != self.lane
            || record
                .partition_hint
                .is_some_and(|p| p != self.partition.partition)
            || self
                .records
                .first()
                .is_some_and(|r| r.topic != record.topic)
        {
            return reject(record, AppendError::WrongPartition);
        }
        let Some(Payload::Encoding(builder)) = self.payload.as_mut() else {
            return reject(record, AppendError::Closed);
        };
        let size = match record.record.encoded_len(
            builder.base_timestamp().unwrap_or(record.record.timestamp),
            builder.record_count(),
        ) {
            Ok(n) => n,
            Err(_) => return reject(record, AppendError::InvalidRecord),
        };
        if self
            .raw_bytes
            .checked_add(size)
            .is_none_or(|n| n > self.hard)
        {
            self.seal_inner(SealReason::HardLimit, now);
            return reject(record, AppendError::HardLimit);
        }
        if self.records.try_reserve(1).is_err() {
            return reject(record, AppendError::AllocationFailed);
        }
        let first = self.first_accepted.unwrap_or(record.accepted_at);
        let linger_at = first
            .checked_add(linger)
            .unwrap_or(RuntimeInstant::from_nanos(u64::MAX));
        let deadline = self
            .oldest_deadline
            .map_or(record.deadline, |old| old.min(record.deadline));
        let (payload, obligation) = record.into_parts();
        // All fallible descriptor checks ran against exactly this builder above.
        let written = builder
            .push(payload)
            .expect("exact preflight of owner-local builder");
        self.records.push(obligation);
        self.raw_bytes += written;
        self.first_accepted = Some(first);
        self.linger_at = Some(linger_at);
        self.oldest_deadline = Some(deadline);
        if self.target_mode == BatchTargetMode::Raw && self.target_reached() {
            self.seal_inner(SealReason::Target, now);
        }
        Ok(())
    }
    pub(crate) fn observe_seals(&mut self, counter: SealCounter) {
        debug_assert!(self.seal_counter.is_none() && self.records.is_empty());
        self.seal_counter = Some(counter);
    }
    pub fn seal(&mut self, reason: SealReason) {
        self.seal_inner(reason, None);
    }
    pub fn seal_at(&mut self, reason: SealReason, now: RuntimeInstant) {
        self.seal_inner(reason, Some(now));
    }
    fn seal_inner(&mut self, reason: SealReason, now: Option<RuntimeInstant>) {
        if self.state != BatchState::Open || self.records.is_empty() {
            return;
        }
        if let Some(Payload::Encoding(builder)) = self.payload.as_mut() {
            builder.request_seal().expect("nonempty open batch");
            self.state = BatchState::Sealing;
            self.seal_reason = Some(reason);
            self.sealed_at = now;
            if let Some(counter) = &self.seal_counter {
                counter.observe(reason, self.raw_bytes, self.target);
            }
        }
    }
    /// Soft targets and linger wait for dispatch credits in EstimatedWire mode.
    /// Raw targets retain immediate sealing. Hard/flush/deadline settlement is
    /// always independent of dispatch pressure.
    pub fn seal_due(&mut self, now: RuntimeInstant, dispatch_credit: bool, sparse: bool) {
        if self.state != BatchState::Open {
            return;
        }
        if self.allowance_at().is_some_and(|at| now >= at) {
            self.seal_at(SealReason::Deadline, now);
        } else if dispatch_credit && self.target_reached() {
            self.seal_at(SealReason::Target, now);
        } else if dispatch_credit && sparse {
            self.seal_at(SealReason::Sparse, now);
        } else if dispatch_credit && self.linger_at.is_some_and(|at| now >= at) {
            self.seal_at(SealReason::Linger, now);
        }
    }
    /// Refreshes an open batch's passive cost snapshot. Growth uses the new
    /// per-byte estimate automatically; first acceptance and linger are fixed.
    /// Returns whether its current deadline allowance changed. Sealed retry
    /// bytes and their assigned identity are never modified by estimates.
    pub fn update_deadline_headroom(&mut self, estimate: DeadlineHeadroom) -> bool {
        if self.state != BatchState::Open {
            return false;
        }
        let previous = self.allowance_at();
        self.deadline_headroom = estimate;
        previous != self.allowance_at()
    }
    #[must_use]
    pub fn deadline_headroom(&self) -> RuntimeDuration {
        self.deadline_headroom.estimate(self.raw_bytes)
    }
    /// Actual codec work from the most recent encode call; empty after a wait,
    /// idle traversal, zero quota or an unsuccessful encoder invocation.
    #[must_use]
    pub const fn last_encode_work(&self) -> EncodeWork {
        self.last_encode_work
    }
    /// Modeled codec completion only controls first dispatch visibility. It
    /// never alters immutable retry bytes or a record's delivery deadline.
    #[must_use]
    pub const fn dispatch_ready_at(&self) -> RuntimeInstant {
        self.dispatch_ready_at
    }
    pub(crate) fn defer_dispatch_until(&mut self, at: RuntimeInstant) {
        debug_assert_eq!(self.state, BatchState::Sealed);
        self.dispatch_ready_at = at;
    }
    fn allowance_at(&self) -> Option<RuntimeInstant> {
        let deadline = self.oldest_deadline?;
        Some(
            RuntimeInstant::from_nanos(
                deadline
                    .as_nanos()
                    .saturating_sub(self.deadline_headroom().as_nanos()),
            )
            .max(self.first_accepted?),
        )
    }
    #[must_use]
    pub fn next_seal_deadline(&self, dispatch_credit: bool) -> Option<RuntimeInstant> {
        if self.state != BatchState::Open {
            return None;
        }
        let deadline = self.allowance_at()?;
        Some(if dispatch_credit {
            deadline.min(if self.target_reached() {
                self.first_accepted?
            } else {
                self.linger_at?
            })
        } else {
            deadline
        })
    }
    /// Whether an input or seal step can run without an external credit release.
    pub fn encoding_immediate(&self) -> bool {
        if !self.has_codec_work() {
            return false;
        }
        if self.transform.is_some() {
            return true;
        }
        let pools = self.credits.snapshot();
        let output = pools[Resource::CompressedBytes as usize];
        let codec = pools[Resource::CodecContexts as usize];
        output.limit - output.held >= self.envelope as usize
            && (matches!(self.compression, Compression::None) || codec.held < codec.limit)
    }
    pub(crate) fn has_codec_work(&self) -> bool {
        matches!(self.state, BatchState::Open | BatchState::Sealing)
            && (self.state == BatchState::Sealing
                || (self.raw_bytes >= self.threshold && self.consumed != self.records.len()))
    }
    /// Gathering never pulls more input or reserves a new transform envelope.
    /// Finalization uses the existing encoder budget and finish reservation.
    pub(crate) fn can_gather_open(&self) -> bool {
        self.state == BatchState::Open
            && !self.records.is_empty()
            && self.consumed == self.records.len()
            && self.transform.is_some()
            && self.encode_wait.is_none()
    }
    pub(crate) fn holds_codec_context(&self) -> bool {
        matches!(&self.payload, Some(Payload::Encoding(builder)) if builder.holds_codec_context())
    }
    pub(crate) fn encode_wait(&self) -> Option<EncodeWait> {
        self.encode_wait
    }
    /// One bounded compression quantum. Capacity starvation is a passive wait,
    /// while output overflow fails before any sequence can be assigned.
    pub fn encode(&mut self, codecs: &mut CodecPool, budget: WorkBudget) -> Progress {
        self.encode_inner(codecs, budget, false)
    }
    pub(crate) fn encode_retained(
        &mut self,
        codecs: &mut CodecPool,
        budget: WorkBudget,
    ) -> Progress {
        self.encode_inner(codecs, budget, true)
    }
    fn encode_inner(
        &mut self,
        codecs: &mut CodecPool,
        budget: WorkBudget,
        retain_failure: bool,
    ) -> Progress {
        self.last_encode_work = EncodeWork::default();
        self.encode_wait = None;
        let mut progress = Progress::default();
        if budget.bytes == 0
            || budget.items == 0
            || !matches!(self.state, BatchState::Open | BatchState::Sealing)
        {
            return progress;
        }
        if self.state == BatchState::Open && self.raw_bytes < self.threshold {
            return progress;
        }
        if self.transform.is_none() {
            let mut claims = vec![Claim {
                resource: Resource::CompressedBytes,
                amount: self.envelope as usize,
                lane: self.lane,
            }];
            if matches!(self.compression, Compression::Zstd { .. }) {
                claims.push(Claim {
                    resource: Resource::CodecContexts,
                    amount: 1,
                    lane: self.lane,
                });
            }
            match self.credits.reserve(&claims) {
                Ok(credits) => self.transform = Some(credits),
                Err(error) => {
                    self.encode_wait = Some(match error {
                        crate::credit::CreditError::ResourceExhausted { resource, .. }
                            if resource == Resource::CompressedBytes.name() =>
                        {
                            EncodeWait::CompressedCredit
                        }
                        crate::credit::CreditError::ResourceExhausted { resource, .. }
                            if resource == Resource::CodecContexts.name() =>
                        {
                            EncodeWait::CodecCredit
                        }
                        _ => EncodeWait::Other,
                    });
                    return progress;
                }
            }
        }
        let Some(Payload::Encoding(builder)) = self.payload.as_mut() else {
            return progress;
        };
        let encode_budget = record::EncodeBudget {
            input_bytes: budget.bytes as usize,
            codec_calls: budget.items as usize,
        };
        let result = if retain_failure {
            builder.progress_retained(codecs, encode_budget)
        } else {
            builder.progress(codecs, encode_budget)
        };
        match result {
            Ok(done) => {
                self.encode_wait = if done.waiting_for_context {
                    Some(EncodeWait::CodecCredit)
                } else if done.waiting_for_output {
                    Some(EncodeWait::Output)
                } else {
                    None
                };
                self.last_encode_work = EncodeWork {
                    raw_bytes: done.input_bytes as u32,
                    input_calls: (done.codec_calls - done.seal_calls) as u32,
                    seal_calls: done.seal_calls as u32,
                    seals_completed: u32::from(
                        done.sealed && matches!(self.compression, Compression::Zstd { .. }),
                    ),
                };
                progress.bytes = done.input_bytes as u32;
                progress.items = done.codec_calls as u32;
                for obligation in self
                    .records
                    .iter_mut()
                    .skip(self.consumed)
                    .take(done.records_released as usize)
                {
                    obligation.input_consumed();
                }
                self.consumed += done.records_released as usize;
                if done.sealed {
                    let allocated = builder.allocated_output_bytes();
                    let sealed = builder.take_sealed().expect("reported sealed encoder");
                    let mut credits = self.transform.take().expect("active transform credits");
                    credits.release(Resource::CodecContexts);
                    credits
                        .shrink(Resource::CompressedBytes, allocated)
                        .expect("sealed allocation is within envelope");
                    self.output_guard = Some(Arc::new(credits));
                    self.payload = Some(Payload::Sealed(sealed));
                    self.state = BatchState::Sealed;
                } else if done.waiting_for_context || done.waiting_for_output {
                    // A deferred builder owns no physical transform resource yet.
                    // Releasing the prospective credits cannot release an active codec.
                    if builder.state() == record::BatchState::Deferred
                        || (builder.allocated_output_bytes() == 0 && done.waiting_for_output)
                    {
                        self.transform = None;
                    }
                } else {
                    progress.remaining_immediate =
                        self.state == BatchState::Sealing || self.consumed < self.records.len();
                }
            }
            Err(error) => {
                self.failure = Some(error);
                self.state = BatchState::Failed;
                if !retain_failure {
                    self.payload = None;
                    self.transform = None;
                    for record in &mut self.records {
                        record.input_consumed();
                    }
                    self.consumed = self.records.len();
                }
            }
        }
        progress
    }
    /// Dispatch credits and the partition sequence window must be acquired first.
    /// # Errors
    /// Rejects non-sealed state and invalid identity without assigning a sequence.
    pub fn finalize(&mut self, identity: record::Identity) -> Result<(), record::Error> {
        if self.state != BatchState::Sealed {
            return Err(record::Error::Closed);
        }
        if identity.producer_id < 0 || identity.producer_epoch < 0 || identity.base_sequence < 0 {
            return Err(record::Error::InvalidIdentity);
        }
        let Some(Payload::Sealed(sealed)) = self.payload.take() else {
            return Err(record::Error::Closed);
        };
        match sealed.finalize(identity) {
            Ok(batch) => {
                self.payload = Some(Payload::Finalized(batch));
                self.state = BatchState::Ready;
                Ok(())
            }
            Err(error) => {
                self.failure = Some(error);
                self.state = BatchState::Failed;
                Err(error)
            }
        }
    }
    /// # Errors
    /// Only never-transmitted finalized batches may install the returned identity.
    pub fn refinalize(&mut self, identity: record::Identity) -> Result<(), record::Error> {
        match self.payload.as_mut() {
            Some(Payload::Finalized(batch)) => batch.refinalize(identity),
            _ => Err(record::Error::Closed),
        }
    }
    #[must_use]
    pub fn chunk_count(&self) -> Option<usize> {
        match self.payload.as_ref()? {
            Payload::Sealed(batch) => Some(batch.chunk_count()),
            Payload::Finalized(batch) => Some(batch.chunks().len()),
            Payload::Encoding(_) => None,
        }
    }
    #[must_use]
    pub fn chunks(&self) -> Option<&[kr_shared_bytes::SharedBytes]> {
        match self.payload.as_ref()? {
            Payload::Finalized(batch) => Some(batch.chunks()),
            _ => None,
        }
    }
    /// Provider-facing spans retain output credits even after the retry owner,
    /// request plan and observing future have all been dropped.
    /// # Errors
    /// Rejects non-finalized batches or a segment metadata allocation failure.
    pub fn retained_chunks(&self) -> Result<Vec<kr_shared_bytes::SharedBytes>, record::Error> {
        let chunks = self.chunks().ok_or(record::Error::Closed)?;
        let guard = self.output_guard.as_ref().ok_or(record::Error::Closed)?;
        let mut retained = Vec::new();
        retained
            .try_reserve_exact(chunks.len())
            .map_err(|_| record::Error::AllocationFailed)?;
        for chunk in chunks {
            retained.push(chunk.clone().retain_guard(guard.clone()));
        }
        Ok(retained)
    }
    #[must_use]
    pub fn output_credit_guard(&self) -> Option<Arc<HeldCredits>> {
        self.output_guard.clone()
    }
    pub fn mark_transmitted(&mut self) {
        if let Some(Payload::Finalized(batch)) = self.payload.as_mut() {
            batch.mark_transmitted();
        }
    }
    pub fn in_flight(&mut self) {
        if self.state == BatchState::Ready {
            self.state = BatchState::InFlight;
        }
    }
    pub fn retry(&mut self) {
        if self.state == BatchState::InFlight {
            self.state = BatchState::Ready;
        }
    }
    /// Terminal events may be published now. A payload returned here retains
    /// output capacity until the owner and all provider guards have released it.
    #[must_use]
    pub fn into_terminal(mut self) -> (Vec<RecordObligation>, Option<RetryPayload>) {
        for record in &mut self.records {
            record.input_consumed();
        }
        self.into_terminal_deferred()
    }
    pub(crate) fn into_terminal_staged(mut self) -> (Vec<RecordObligation>, TerminalPayload) {
        let payload = self.payload.take().map(|payload| match payload {
            Payload::Encoding(batch) => batch.into_abort(),
            Payload::Sealed(batch) => batch.into_abort(),
            Payload::Finalized(batch) => batch.into_abort(),
        });
        let retained = TerminalPayload {
            payload,
            transform: self.transform.take(),
            output: self.output_guard.take(),
        };
        (std::mem::take(&mut self.records), retained)
    }
    /// Moves terminal obligations to a bounded engine drain queue. Input credit
    /// release can be charged to each later event-drain work item instead of an
    /// unbounded loop in one response/deadline transition.
    #[must_use]
    pub fn into_terminal_deferred(mut self) -> (Vec<RecordObligation>, Option<RetryPayload>) {
        self.payload = match self.payload.take() {
            Some(Payload::Finalized(batch)) => Some(Payload::Finalized(batch)),
            _ => None,
        };
        let retained = match (self.payload.take(), self.output_guard.take()) {
            (Some(Payload::Finalized(batch)), Some(credit)) => Some(RetryPayload { batch, credit }),
            _ => None,
        };
        (std::mem::take(&mut self.records), retained)
    }
}

impl Drop for Batch {
    fn drop(&mut self) {
        // Input bytes held by the encoder must disappear before admission can
        // observe their returned credits on another submitting thread.
        self.payload = None;
        self.records.clear();
        self.transform = None;
        self.output_guard = None;
    }
}

/// Byte-driven routing is independent of this sparse-arrival estimate. Batch
/// sealing never resets either the traffic history or a routing lease.
#[derive(Clone, Copy, Debug, Default)]
pub struct ArrivalRate {
    last: Option<RuntimeInstant>,
    interval_nanos: Option<u64>,
}
impl ArrivalRate {
    pub fn observe(&mut self, now: RuntimeInstant) {
        if let Some(last) = self.last
            && let Some(gap) = now.checked_duration_since(last)
        {
            let gap = gap.as_nanos();
            self.interval_nanos = Some(
                self.interval_nanos
                    .map_or(gap, |old| old - old / 8 + gap / 8),
            );
        }
        self.last = Some(self.last.map_or(now, |old| old.max(now)));
    }
    #[must_use]
    pub fn below(&self, records_per_second: Option<u32>) -> bool {
        records_per_second
            .zip(self.interval_nanos)
            .is_some_and(|(rate, interval)| {
                rate > 0 && u128::from(interval) * u128::from(rate) > 1_000_000_000
            })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        admission::Admission,
        types::{RecordDescriptor, TopicHandle, TopicId},
    };
    fn setup() -> (
        ProducerConfig,
        SharedCredits,
        OutputPool,
        CodecPool,
        Admission,
    ) {
        let config = ProducerConfig {
            compression: Compression::None,
            batch_target_mode: BatchTargetMode::Raw,
            batch_target_bytes: 32,
            progressive_threshold: 16,
            ..Default::default()
        };
        let v = config.validate().unwrap();
        let credits = SharedCredits::new(v.credits, 1).unwrap();
        let output = OutputPool::new(config.compressed_bytes).unwrap();
        let codecs = CodecPool::new(0, record::ZstdConfig::default()).unwrap();
        let admission = Admission::new(&config, credits.clone(), v.effective_batch_payload_bytes);
        (config, credits, output, codecs, admission)
    }
    fn record(a: &mut Admission, value: &[u8], now: u64) -> AdmittedRecord {
        let input = RecordDescriptor {
            topic: TopicHandle(1),
            partition_hint: Some(0),
            lane_hint: None,
            key: None,
            value: Some(value),
            headers: &[],
            timestamp_ms: now as i64,
            user_token: now,
            delivery_timeout: None,
        };
        a.prepare_copy(RuntimeInstant::from_nanos(now), &[input], &[Ok(0)])
            .1
            .unwrap()
            .drain()
            .pop()
            .unwrap()
    }
    fn batch(config: &ProducerConfig, c: SharedCredits, o: OutputPool) -> Batch {
        Batch::new(
            config,
            config.validate().unwrap().effective_batch_payload_bytes,
            TopicPartition {
                topic: TopicId([1; 16]),
                partition: 0,
            },
            0,
            o,
            c,
        )
        .unwrap()
    }
    #[test]
    fn wire_target_waits_for_dispatch_but_hard_limits_and_deadlines_do_not() {
        let (mut config, c, o, _, mut a) = setup();
        config.batch_target_mode = BatchTargetMode::EstimatedWire;
        config.batch_target_bytes = 128;
        config.batch_hard_bytes = 512;
        config.output_chunk_bytes = 256;
        let mut b = batch(&config, c, o);
        b.try_append(record(&mut a, &[1; 80], 0), config.linger_max)
            .unwrap();
        assert_eq!(b.estimated_wire_bytes(), u64::from(b.raw_bytes()) + 61);
        assert!(b.estimated_wire_bytes() >= 128);
        let deadline = b.oldest_deadline();
        b.seal_due(RuntimeInstant::from_nanos(500_000), false, false);
        b.try_append(record(&mut a, &[2; 80], 1), config.linger_max)
            .unwrap();
        assert_eq!(b.state(), BatchState::Open);
        assert_eq!(b.oldest_deadline(), deadline);
        b.seal_due(RuntimeInstant::from_nanos(500_000), true, false);
        assert_eq!(b.seal_reason(), Some(SealReason::Target));

        for reason in [SealReason::HardLimit, SealReason::Deadline] {
            let (mut config, c, o, _, mut a) = setup();
            config.batch_target_mode = BatchTargetMode::EstimatedWire;
            config.batch_target_bytes = 64;
            config.batch_hard_bytes = 128;
            config.output_chunk_bytes = 128;
            let mut b = batch(&config, c, o);
            b.try_append(record(&mut a, &[1; 80], 0), config.linger_max)
                .unwrap();
            if reason == SealReason::HardLimit {
                let next = record(&mut a, &[2; 80], 1);
                let token = next.token;
                let rejected = b.try_append(next, config.linger_max).unwrap_err();
                assert_eq!(rejected.record.token, token);
                assert_eq!(b.record_count(), 1);
            } else {
                b.seal_due(b.allowance_at().unwrap(), false, false);
            }
            assert_eq!(b.seal_reason(), Some(reason));
        }
    }

    #[test]
    fn cold_compressor_with_no_emitted_output_cannot_hide_the_soft_target() {
        let (mut config, c, o, _, mut a) = setup();
        config.batch_target_mode = BatchTargetMode::EstimatedWire;
        config.compression = Compression::Zstd { level: 1 };
        config.batch_target_bytes = 128;
        let mut b = batch(&config, c, o);
        b.try_append(record(&mut a, &[1; 128], 0), config.linger_max)
            .unwrap();
        assert!(b.estimated_wire_bytes() >= 128);
        b.seal_due(RuntimeInstant::ZERO, true, false);
        assert_eq!(b.seal_reason(), Some(SealReason::Target));
    }
    #[test]
    fn actual_seal_transition_counts_once_and_survives_batch_drop() {
        let (config, c, o, _, mut a) = setup();
        let counter = SealCounter::default();
        let mut b = batch(&config, c, o);
        b.observe_seals(counter.clone());
        b.seal(SealReason::Flush);
        assert_eq!(counter.snapshot(), BatchSealStats::default());
        b.try_append(record(&mut a, b"a", 0), config.linger_max)
            .unwrap();
        let raw = u64::from(b.raw_bytes());
        b.seal(SealReason::Linger);
        b.seal(SealReason::Flush);
        let snapshot = counter.snapshot();
        assert_eq!(snapshot.by_reason, [0, 1, 0, 0, 0, 0, 0, 0]);
        assert_eq!(snapshot.raw_bytes, raw);
        assert_eq!(snapshot.target_bytes, u64::from(config.batch_target_bytes));
        drop(b);
        assert_eq!(counter.snapshot(), snapshot);
        let mut exhausted = BatchSealStats {
            by_reason: [u64::MAX; 8],
            ..BatchSealStats::default()
        };
        exhausted.observe(SealReason::Target, 1, 1);
        assert!(exhausted.overflowed);
        assert_eq!(exhausted.by_reason[0], u64::MAX);
    }
    #[test]
    fn linger_never_moves_later_and_waits_for_broker_credits() {
        let (config, c, o, _, mut a) = setup();
        let mut b = batch(&config, c.clone(), o);
        b.try_append(record(&mut a, b"a", 0), config.linger_max)
            .unwrap();
        b.try_append(record(&mut a, b"a", 100), config.linger_max)
            .unwrap();
        assert_eq!(
            b.next_seal_deadline(true),
            Some(RuntimeInstant::from_nanos(500_000))
        );
        b.seal_due(RuntimeInstant::from_nanos(500_000), false, false);
        assert_eq!(b.state(), BatchState::Open);
        b.seal_due(RuntimeInstant::from_nanos(500_000), true, false);
        assert_eq!(b.seal_reason(), Some(SealReason::Linger));
        drop(b);
        assert!(c.is_empty());
    }
    #[test]
    fn hard_limit_seals_before_append_and_returns_the_original_record() {
        let (mut config, c, o, _, _) = setup();
        config.batch_target_bytes = 100;
        config.batch_hard_bytes = 120;
        config.output_chunk_bytes = 120;
        let validated = config.validate().unwrap();
        let mut admission =
            Admission::new(&config, c.clone(), validated.effective_batch_payload_bytes);
        let mut b = batch(&config, c.clone(), o);
        b.try_append(record(&mut admission, &[1; 72], 0), config.linger_max)
            .unwrap();
        let first_bytes = b.raw_bytes();
        assert!(first_bytes < config.batch_target_bytes);
        let next = record(&mut admission, &[2; 72], 1);
        let token = next.token;
        let rejected = b.try_append(next, config.linger_max).unwrap_err();
        assert_eq!(rejected.reason, AppendError::HardLimit);
        assert_eq!(rejected.record.token, token);
        assert_eq!(b.record_count(), 1);
        assert_eq!(b.raw_bytes(), first_bytes);
        assert_eq!(
            b.oldest_deadline(),
            Some(RuntimeInstant::from_nanos(
                config.delivery_timeout.as_nanos()
            ))
        );
        assert_eq!(b.seal_reason(), Some(SealReason::HardLimit));
        drop(rejected);
        drop(b);
        assert!(c.is_empty());
    }
    #[test]
    fn sparse_seal_requires_dispatch_credit_and_deadline_has_precedence() {
        for dispatch_credit in [false, true] {
            for sparse in [false, true] {
                let (config, c, o, _, mut a) = setup();
                let mut b = batch(&config, c.clone(), o);
                b.try_append(record(&mut a, b"a", 0), config.linger_max)
                    .unwrap();
                b.seal_due(RuntimeInstant::from_nanos(1), dispatch_credit, sparse);
                let first_reason = (dispatch_credit && sparse).then_some(SealReason::Sparse);
                assert_eq!(b.seal_reason(), first_reason);
                let deadline = b.oldest_deadline().unwrap();
                b.seal_due(deadline, true, true);
                assert_eq!(
                    b.seal_reason(),
                    Some(first_reason.unwrap_or(SealReason::Deadline)),
                    "a deadline overrides sparse/linger only while the batch is open"
                );
                drop(b);
                assert!(c.is_empty());
            }
        }
    }
    #[test]
    fn learned_headroom_refreshes_existing_batches_without_restarting_linger() {
        let (config, c, o, _, mut a) = setup();
        let mut b = batch(&config, c, o);
        let mut first = record(&mut a, b"a", 100);
        first.deadline = RuntimeInstant::from_nanos(1_000);
        b.try_append(first, RuntimeDuration::from_nanos(200))
            .unwrap();
        let mut cost = EncodingCost::new(RuntimeDuration::from_nanos(1_000));
        cost.observe(
            EncodeWork {
                raw_bytes: 10,
                input_calls: 1,
                ..EncodeWork::default()
            },
            RuntimeDuration::from_nanos(20),
        )
        .unwrap();
        let headroom = |rtt_ns| {
            let mut rtt = RoundTripTime::default();
            rtt.observe(RuntimeDuration::from_nanos(rtt_ns));
            DeadlineHeadroom::new(
                cost,
                rtt,
                RuntimeDuration::from_nanos(100),
                RuntimeDuration::from_nanos(1_000),
            )
            .unwrap()
        };
        assert!(b.update_deadline_headroom(headroom(20)));
        assert_eq!(
            b.next_seal_deadline(true),
            Some(RuntimeInstant::from_nanos(300))
        );
        let first_cost = b.deadline_headroom().as_nanos();
        let mut second = record(&mut a, b"b", 150);
        second.deadline = RuntimeInstant::from_nanos(2_000);
        b.try_append(second, RuntimeDuration::from_nanos(200))
            .unwrap();
        assert!(
            b.deadline_headroom().as_nanos() > first_cost,
            "remaining encoding cost grows with retained raw input"
        );
        assert_eq!(b.first_accepted(), Some(RuntimeInstant::from_nanos(100)));
        assert_eq!(b.oldest_deadline(), Some(RuntimeInstant::from_nanos(1_000)));
        assert_eq!(
            b.next_seal_deadline(true),
            Some(RuntimeInstant::from_nanos(300))
        );
        b.update_deadline_headroom(headroom(800));
        let due = 1_000 - b.deadline_headroom().as_nanos();
        assert!(due < 300);
        b.seal_due(RuntimeInstant::from_nanos(due - 1), false, false);
        assert_eq!(b.state(), BatchState::Open);
        b.seal_due(RuntimeInstant::from_nanos(due), false, false);
        assert_eq!(b.seal_reason(), Some(SealReason::Deadline));
        assert!(
            !b.update_deadline_headroom(headroom(0)),
            "immutable sealed payload does not react to estimates"
        );
    }
    #[test]
    fn actual_zero_input_zstd_end_work_is_distinct_from_idle_and_plain_seals() {
        let (mut config, c, o, _, mut a) = setup();
        config.compression = Compression::Zstd { level: 1 };
        config.batch_target_bytes = 1024;
        let mut b = batch(&config, c, o);
        let mut codecs = CodecPool::new(1, record::ZstdConfig::default()).unwrap();
        b.try_append(record(&mut a, &[7; 64], 0), config.linger_max)
            .unwrap();
        for _ in 0..128 {
            b.encode(
                &mut codecs,
                WorkBudget {
                    bytes: 1024,
                    items: 1,
                },
            );
            assert_eq!(b.last_encode_work().seal_calls, 0);
            if b.consumed == b.records.len() {
                break;
            }
        }
        assert_eq!(b.consumed, b.records.len());
        b.seal(SealReason::Flush);
        b.encode(
            &mut codecs,
            WorkBudget {
                bytes: 1024,
                items: 0,
            },
        );
        assert!(b.last_encode_work().is_empty());
        let mut finish_calls = 0;
        for _ in 0..128 {
            b.encode(
                &mut codecs,
                WorkBudget {
                    bytes: 1024,
                    items: 1,
                },
            );
            let work = b.last_encode_work();
            assert_eq!(work.raw_bytes, 0);
            assert_eq!(work.input_calls, 0);
            finish_calls += work.seal_calls;
            if b.state() == BatchState::Sealed {
                assert_eq!(work.seals_completed, 1);
                break;
            }
        }
        assert!(finish_calls > 0);
        assert_eq!(b.state(), BatchState::Sealed);
        b.encode(
            &mut codecs,
            WorkBudget {
                bytes: 1024,
                items: 1,
            },
        );
        assert!(b.last_encode_work().is_empty());
    }
    #[test]
    fn target_seals_without_broker_credit_and_input_returns_before_delivery() {
        let (config, c, o, mut codecs, mut a) = setup();
        let mut b = batch(&config, c.clone(), o);
        b.try_append(record(&mut a, &[1; 32], 0), config.linger_max)
            .unwrap();
        assert_eq!(b.seal_reason(), Some(SealReason::Target));
        for _ in 0..1000 {
            let p = b.encode(&mut codecs, WorkBudget { bytes: 3, items: 2 });
            assert!(p.bytes <= 3 && p.items <= 2);
            if b.state() == BatchState::Sealed {
                break;
            }
        }
        assert_eq!(b.state(), BatchState::Sealed);
        assert_eq!(c.snapshot()[Resource::InputBytes as usize].held, 0);
        assert_eq!(c.snapshot()[Resource::Descriptors as usize].held, 1);
        b.finalize(record::Identity {
            producer_id: 1,
            producer_epoch: 0,
            base_sequence: 0,
        })
        .unwrap();
        let guard = b.output_credit_guard().unwrap();
        let (records, payload) = b.into_terminal();
        for r in records {
            drop(r.terminal());
        }
        drop(payload);
        assert!(c.snapshot()[Resource::CompressedBytes as usize].held > 0);
        drop(guard);
        assert!(c.is_empty());
    }
    #[test]
    fn output_exhaustion_defers_without_consuming_input() {
        let (config, c, _, mut codecs, mut a) = setup();
        let o = OutputPool::new(61).unwrap();
        let mut b = batch(&config, c.clone(), o);
        b.try_append(record(&mut a, b"input", 0), config.linger_max)
            .unwrap();
        b.seal(SealReason::Flush);
        let before = c.snapshot()[Resource::InputBytes as usize].held;
        b.encode(&mut codecs, WorkBudget::default());
        assert_eq!(c.snapshot()[Resource::InputBytes as usize].held, before);
        assert_eq!(c.snapshot()[Resource::CompressedBytes as usize].held, 0);
        drop(b);
        assert!(c.is_empty());
    }
}
