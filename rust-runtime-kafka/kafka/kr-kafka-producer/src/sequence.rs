//! Bounded idempotency ledger independent of request framing, sockets, and time.
//!
//! Sequence assignment consumes a partition window until FIFO terminal drain;
//! reconnects never create new window capacity. A rejected retry cannot erase
//! ambiguity from a prior attempt. Recovery bumps the epoch after old attempts
//! settle; epoch exhaustion installs a fresh broker-assigned producer identity.
use crate::types::{
    DeliveryKind, DeliveryOutcome, FailureReason, ProducerIdentity, Sequence, TopicPartition,
};
mod recovery;
pub use recovery::{IdentityChange, IdentityProgress};

use std::{
    collections::{BTreeMap, BTreeSet, VecDeque},
    fmt,
};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RecoveryState {
    Active,
    NeedsIdentity,
    RefreshingIdentity,
    FailedClosed,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LedgerError {
    InvalidConfig,
    InvalidIdentity,
    InvalidCount,
    UnknownPartition,
    UnknownBatch,
    DuplicateBatch,
    PartitionCapacity,
    WindowFull,
    RecoveryPending,
    FailedClosed,
    AttemptActive,
    AttemptMissing,
    StaleAttempt,
    AttemptExhausted,
    EarlierBatchReady,
    NotQuiescent,
    IdentityUnchanged,
    PartitionBusy,
}
impl fmt::Display for LedgerError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{self:?}")
    }
}
impl std::error::Error for LedgerError {}
pub type Result<T> = std::result::Result<T, LedgerError>;
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Assignment {
    pub partition: TopicPartition,
    pub batch: u64,
    pub identity: ProducerIdentity,
    pub base_sequence: Sequence,
    pub record_count: u32,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BrokerOutcome {
    Success {
        base_offset: Option<i64>,
        timestamp: Option<i64>,
    },
    Duplicate,
    /// Retryable responses preserve ambiguity under the producer outcome contract.
    Retry,
    SequenceError,
    /// Proves the current attempt did not commit. Earlier ambiguous attempts
    /// remain unresolved; only success/deduplication reconciles their outcome.
    DefinitiveRejection(FailureReason),
    Fatal(FailureReason),
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TerminalBatch {
    pub assignment: Assignment,
    pub outcome: DeliveryOutcome,
    pub base_offset: Option<i64>,
    pub timestamp: Option<i64>,
    pub attempts: u32,
    pub transmitted: bool,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LedgerChange {
    pub terminal: Vec<TerminalBatch>,
    pub recovery: RecoveryState,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Refinalization {
    pub before: Assignment,
    pub after: Assignment,
}
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct LedgerStats {
    pub partitions: usize,
    pub unresolved: usize,
    pub active_attempts: usize,
    pub ambiguous_batches: usize,
    /// Partitions with terminal Unknown outcomes awaiting an identity change.
    pub unresolved_partitions: usize,
}
#[derive(Clone, Copy, Debug)]
struct Pending {
    outcome: DeliveryOutcome,
    offset: Option<i64>,
    timestamp: Option<i64>,
    recovery: bool,
    // A non-head sequence rejection can be caused by an earlier missing
    // request. Keep its proof, assignment and window slot until the head is
    // reconciled; only then may the same batch be retried.
    retry_after_head: bool,
}
#[derive(Debug)]
struct Entry {
    assignment: Assignment,
    transmitted: bool,
    prior_ambiguity: bool,
    current_written: bool,
    current: Option<u64>,
    last_attempt: Option<u64>,
    attempts: u32,
    pending: Option<Pending>,
}
impl Entry {
    fn possible_transmission(&self) -> bool {
        self.transmitted || self.current.is_some()
    }
    fn terminal(self, pending: Pending) -> TerminalBatch {
        TerminalBatch {
            assignment: self.assignment,
            outcome: pending.outcome,
            base_offset: pending.offset,
            timestamp: pending.timestamp,
            attempts: self.attempts,
            transmitted: self.possible_transmission(),
        }
    }
    fn unresolved(&self, reason: FailureReason) -> Pending {
        Pending {
            outcome: DeliveryOutcome::unresolved(self.possible_transmission(), reason),
            offset: None,
            timestamp: None,
            recovery: true,
            retry_after_head: false,
        }
    }
}
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct EntryCounts {
    unresolved: usize,
    active_attempts: usize,
    ambiguous_batches: usize,
    refresh_blocked: usize,
}
impl EntryCounts {
    fn from_entries(entries: &VecDeque<Entry>) -> Self {
        let mut counts = Self::default();
        for entry in entries {
            counts.unresolved += 1;
            counts.active_attempts += usize::from(entry.current.is_some());
            counts.ambiguous_batches += usize::from(entry.prior_ambiguity);
            counts.refresh_blocked += usize::from(
                entry.transmitted || entry.current.is_some() || entry.pending.is_some(),
            );
        }
        counts
    }
    fn replace(&mut self, before: Self, after: Self) {
        self.unresolved = self.unresolved - before.unresolved + after.unresolved;
        self.active_attempts =
            self.active_attempts - before.active_attempts + after.active_attempts;
        self.ambiguous_batches =
            self.ambiguous_batches - before.ambiguous_batches + after.ambiguous_batches;
        self.refresh_blocked =
            self.refresh_blocked - before.refresh_blocked + after.refresh_blocked;
    }
}
#[derive(Debug)]
struct Partition {
    next: Sequence,
    sequence_identity: ProducerIdentity,
    entries: VecDeque<Entry>,
    counts: EntryCounts,
}
/// One producer-wide identity and recovery barrier, with independent ordered
/// partition windows. Batch IDs must not be reused during this ledger lifetime. Entries own
/// no input/output memory; the engine releases credits using terminal results.
#[derive(Debug)]
pub struct ProducerLedger {
    identity: ProducerIdentity,
    max_partitions: usize,
    max_in_flight: usize,
    partitions: BTreeMap<TopicPartition, Partition>,
    nonempty: BTreeSet<TopicPartition>,
    live_batches: BTreeSet<u64>,
    counts: EntryCounts,
    identity_install: Option<recovery::IdentityInstall>,
    failure_reason: Option<FailureReason>,
    recovery: RecoveryState,
    unresolved_partitions: BTreeSet<TopicPartition>,
}
impl ProducerLedger {
    pub fn new(
        identity: ProducerIdentity,
        max_partitions: usize,
        max_in_flight: usize,
    ) -> Result<Self> {
        if !identity.is_valid() {
            return Err(LedgerError::InvalidIdentity);
        }
        if max_partitions == 0 || !(1..=5).contains(&max_in_flight) {
            return Err(LedgerError::InvalidConfig);
        }
        Ok(Self {
            identity,
            max_partitions,
            max_in_flight,
            partitions: BTreeMap::new(),
            nonempty: BTreeSet::new(),
            live_batches: BTreeSet::new(),
            counts: EntryCounts::default(),
            identity_install: None,
            failure_reason: None,
            recovery: RecoveryState::Active,
            unresolved_partitions: BTreeSet::new(),
        })
    }
    pub fn identity(&self) -> ProducerIdentity {
        self.identity
    }
    pub fn recovery_state(&self) -> RecoveryState {
        self.recovery
    }
    /// Constant-time diagnostics. Every transition refreshes at most the five
    /// entries in its affected partition; no observer scans the topology.
    pub fn stats(&self) -> LedgerStats {
        LedgerStats {
            partitions: self.partitions.len(),
            unresolved: self.counts.unresolved,
            active_attempts: self.counts.active_attempts,
            ambiguous_batches: self.counts.ambiguous_batches,
            unresolved_partitions: self.unresolved_partitions.len(),
        }
    }
    fn refresh_counts(&mut self, partition: TopicPartition) {
        let partition = self
            .partitions
            .get_mut(&partition)
            .expect("validated partition");
        if partition.entries.is_empty() && self.recovery == RecoveryState::RefreshingIdentity {
            // A cancellation can empty a partition already visited by the
            // installer. It then leaves the live index, so invalidate its lazy
            // sequence marker instead of requiring an empty-topology sweep.
            partition.sequence_identity = self.identity;
        }
        let counts = EntryCounts::from_entries(&partition.entries);
        if partition.counts != counts
            && let Some(install) = &mut self.identity_install
        {
            // Only real entry changes can invalidate the dense layout. Repeated
            // cancellation of an already pending entry must not restart work.
            install.restart = true;
        }
        self.counts.replace(partition.counts, counts);
        partition.counts = counts;
    }
    pub fn register(&mut self, partition: TopicPartition) -> Result<()> {
        if self.recovery == RecoveryState::FailedClosed {
            return Err(LedgerError::FailedClosed);
        }
        if partition.partition < 0 || partition.topic.is_zero() {
            return Err(LedgerError::InvalidConfig);
        }
        if self.partitions.contains_key(&partition) {
            return Ok(());
        }
        if self.partitions.len() == self.max_partitions {
            return Err(LedgerError::PartitionCapacity);
        }
        self.partitions.insert(
            partition,
            Partition {
                next: Sequence::ZERO,
                sequence_identity: self.identity,
                entries: VecDeque::new(),
                counts: EntryCounts::default(),
            },
        );
        Ok(())
    }
    /// An empty partition can be forgotten only when re-registration would
    /// preserve its exact next sequence under the active producer identity.
    /// Old-identity history is already subject to lazy sequence reset.
    pub fn can_forget_partition(&self, partition: TopicPartition) -> Result<bool> {
        let state = self
            .partitions
            .get(&partition)
            .ok_or(LedgerError::UnknownPartition)?;
        Ok(state.entries.is_empty()
            && (state.sequence_identity != self.identity || state.next == Sequence::ZERO))
    }
    /// Removes only unused or stale-identity history. Closing a topic handle is
    /// not proof of broker deletion: reopening the same UUID under the same
    /// identity must continue its next sequence after acknowledged batches.
    pub fn remove(&mut self, partition: TopicPartition) -> Result<()> {
        if !self.can_forget_partition(partition)? {
            return Err(LedgerError::PartitionBusy);
        }
        self.partitions.remove(&partition);
        Ok(())
    }
    /// Requests an identity change to reclaim bounded historical state.
    /// Existing old-identity attempts retain their normal reconciliation path;
    /// new assignments remain fenced until the quiescent refresh completes.
    pub fn request_identity_refresh(&mut self) -> Result<()> {
        match self.recovery {
            RecoveryState::Active => self.recovery = RecoveryState::NeedsIdentity,
            RecoveryState::NeedsIdentity | RecoveryState::RefreshingIdentity => {}
            RecoveryState::FailedClosed => return Err(LedgerError::FailedClosed),
        }
        Ok(())
    }
    pub fn assignment(&self, partition: TopicPartition, batch: u64) -> Result<Assignment> {
        Ok(self.entry(partition, batch)?.assignment)
    }
    pub fn unresolved(&self, partition: TopicPartition) -> Result<usize> {
        Ok(self
            .partitions
            .get(&partition)
            .ok_or(LedgerError::UnknownPartition)?
            .entries
            .len())
    }
    pub fn transmitted(&self, partition: TopicPartition, batch: u64) -> Result<bool> {
        Ok(self.entry(partition, batch)?.transmitted)
    }
    /// A non-head sequence rejection awaiting an ordered retry of the same
    /// identity and sequence. This remains true after it reaches the head until
    /// its next attempt is admitted, expires, or is cancelled.
    pub fn sequence_retry_pending(&self, partition: TopicPartition, batch: u64) -> Result<bool> {
        Ok(self
            .entry(partition, batch)?
            .pending
            .is_some_and(|pending| pending.retry_after_head))
    }
    pub fn assign(
        &mut self,
        partition: TopicPartition,
        batch: u64,
        count: u32,
    ) -> Result<Assignment> {
        match self.recovery {
            RecoveryState::Active => {}
            RecoveryState::FailedClosed => return Err(LedgerError::FailedClosed),
            _ => return Err(LedgerError::RecoveryPending),
        }
        if count == 0 || count > i32::MAX as u32 {
            return Err(LedgerError::InvalidCount);
        }
        if self.live_batches.contains(&batch) {
            return Err(LedgerError::DuplicateBatch);
        }
        let p = self
            .partitions
            .get_mut(&partition)
            .ok_or(LedgerError::UnknownPartition)?;
        if p.entries
            .iter()
            .any(|e| e.pending.is_some_and(|r| r.recovery))
        {
            return Err(LedgerError::RecoveryPending);
        }
        if p.entries.len() == self.max_in_flight {
            return Err(LedgerError::WindowFull);
        }
        if p.sequence_identity != self.identity {
            debug_assert!(p.entries.is_empty());
            p.next = Sequence::ZERO;
            p.sequence_identity = self.identity;
        }
        let assignment = Assignment {
            partition,
            batch,
            identity: self.identity,
            base_sequence: p.next,
            record_count: count,
        };
        p.next = p.next.advance(count);
        self.nonempty.insert(partition);
        p.entries.push_back(Entry {
            assignment,
            transmitted: false,
            prior_ambiguity: false,
            current_written: false,
            current: None,
            last_attempt: None,
            attempts: 0,
            pending: None,
        });
        self.live_batches.insert(batch);
        self.refresh_counts(partition);
        Ok(assignment)
    }
    /// Whether earlier assignments permit this existing batch to start. A
    /// parsed sequence/recovery response remains a fence even while its batch
    /// waits for the FIFO head and still appears in flight to the accumulator.
    pub fn attempt_order_ready(&self, partition: TopicPartition, batch: u64) -> Result<bool> {
        let p = self
            .partitions
            .get(&partition)
            .ok_or(LedgerError::UnknownPartition)?;
        let position = p
            .entries
            .iter()
            .position(|entry| entry.assignment.batch == batch)
            .ok_or(LedgerError::UnknownBatch)?;
        if let Some(pending) = p.entries[position].pending {
            return Ok(pending.retry_after_head && position == 0);
        }
        Ok(!p.entries.iter().take(position).any(|entry| {
            (entry.current.is_none() && entry.pending.is_none())
                || entry.pending.is_some_and(|pending| pending.recovery)
        }))
    }
    /// Maximum admitted attempt count in this partition's unresolved window.
    /// Kafka's configured window bounds this scan to at most five entries;
    /// historical completed batches and accumulator backlog are not traversed.
    pub fn max_attempts(&self, partition: TopicPartition) -> Result<u32> {
        let partition = self
            .partitions
            .get(&partition)
            .ok_or(LedgerError::UnknownPartition)?;
        Ok(partition
            .entries
            .iter()
            .map(|entry| entry.attempts)
            .max()
            .unwrap_or(0))
    }
    /// Checks only the later entries in this partition's bounded window.
    pub fn later_requires_old_identity(
        &self,
        partition: TopicPartition,
        batch: u64,
    ) -> Result<bool> {
        let entries = &self
            .partitions
            .get(&partition)
            .ok_or(LedgerError::UnknownPartition)?
            .entries;
        let position = entries
            .iter()
            .position(|entry| entry.assignment.batch == batch)
            .ok_or(LedgerError::UnknownBatch)?;
        Ok(entries
            .iter()
            .skip(position + 1)
            .any(|entry| entry.transmitted || entry.current.is_some()))
    }
    /// Starts an admitted provider write attempt. A deadline before its terminal
    /// certainty is conservatively Unknown. Call only after actual admission;
    /// planning/credit reservation belongs before this method.
    pub fn start_attempt(
        &mut self,
        partition: TopicPartition,
        batch: u64,
        attempt: u64,
    ) -> Result<()> {
        if matches!(
            self.recovery,
            RecoveryState::FailedClosed | RecoveryState::RefreshingIdentity
        ) {
            return Err(if self.recovery == RecoveryState::FailedClosed {
                LedgerError::FailedClosed
            } else {
                LedgerError::RecoveryPending
            });
        }
        let p = self
            .partitions
            .get_mut(&partition)
            .ok_or(LedgerError::UnknownPartition)?;
        let position = p
            .entries
            .iter()
            .position(|e| e.assignment.batch == batch)
            .ok_or(LedgerError::UnknownBatch)?;
        if p.entries
            .iter()
            .take(position)
            .any(|e| e.current.is_none() && e.pending.is_none())
            || p.entries
                .iter()
                .take(position)
                .any(|e| e.pending.is_some_and(|r| r.recovery))
        {
            return Err(LedgerError::EarlierBatchReady);
        }
        let later_requires_old_identity = p
            .entries
            .iter()
            .skip(position + 1)
            .any(|entry| entry.transmitted || entry.current.is_some());
        let entry = &mut p.entries[position];
        if entry
            .pending
            .is_some_and(|pending| !pending.retry_after_head || position != 0)
        {
            return Err(LedgerError::UnknownBatch);
        }
        if entry.current.is_some() {
            return Err(LedgerError::AttemptActive);
        }
        if self.recovery == RecoveryState::NeedsIdentity
            && !entry.transmitted
            && !later_requires_old_identity
        {
            return Err(LedgerError::RecoveryPending);
        }
        if entry
            .last_attempt
            .is_some_and(|previous| attempt <= previous)
        {
            return Err(LedgerError::StaleAttempt);
        }
        entry.attempts = entry
            .attempts
            .checked_add(1)
            .ok_or(LedgerError::AttemptExhausted)?;
        entry.pending = None;
        entry.current = Some(attempt);
        entry.last_attempt = Some(attempt);
        entry.current_written = false;
        self.refresh_counts(partition);
        Ok(())
    }
    pub fn mark_transmitted(
        &mut self,
        partition: TopicPartition,
        batch: u64,
        attempt: u64,
    ) -> Result<()> {
        let e = self.active_entry(partition, batch, attempt)?;
        e.transmitted = true;
        e.current_written = true;
        self.refresh_counts(partition);
        Ok(())
    }
    /// Retires one failed/lost request attempt while retaining its exact sequence
    /// and window slot. NotApplied cannot erase bytes written in earlier stages.
    pub fn retire_attempt(
        &mut self,
        partition: TopicPartition,
        batch: u64,
        attempt: u64,
        may_have_written: bool,
    ) -> Result<()> {
        let e = self.active_entry(partition, batch, attempt)?;
        let ambiguous = may_have_written || e.current_written;
        e.transmitted |= ambiguous;
        e.prior_ambiguity |= ambiguous;
        e.current = None;
        e.current_written = false;
        self.refresh_counts(partition);
        Ok(())
    }
    pub fn response(
        &mut self,
        partition: TopicPartition,
        batch: u64,
        attempt: u64,
        outcome: BrokerOutcome,
    ) -> Result<LedgerChange> {
        self.response_mode(partition, batch, attempt, outcome, false)
    }
    /// Actor variant: a producer-wide failure is fenced immediately and leaves
    /// unrelated terminal entries for subsequent bounded drain_failed calls.
    pub fn response_deferred(
        &mut self,
        partition: TopicPartition,
        batch: u64,
        attempt: u64,
        outcome: BrokerOutcome,
    ) -> Result<LedgerChange> {
        self.response_mode(partition, batch, attempt, outcome, true)
    }
    fn response_mode(
        &mut self,
        partition: TopicPartition,
        batch: u64,
        attempt: u64,
        outcome: BrokerOutcome,
        deferred: bool,
    ) -> Result<LedgerChange> {
        let non_head = self
            .partitions
            .get(&partition)
            .ok_or(LedgerError::UnknownPartition)?
            .entries
            .front()
            .is_some_and(|entry| entry.assignment.batch != batch);
        let e = self.active_entry(partition, batch, attempt)?;
        e.current = None;
        e.current_written = false;
        e.transmitted = true;
        let rejected = |reason| {
            if e.prior_ambiguity {
                DeliveryOutcome::unknown(reason)
            } else {
                DeliveryOutcome::not_written(reason)
            }
        };
        e.pending = match outcome {
            BrokerOutcome::Success {
                base_offset,
                timestamp,
            } => Some(Pending {
                outcome: DeliveryOutcome::ACKED,
                offset: base_offset,
                timestamp,
                recovery: false,
                retry_after_head: false,
            }),
            BrokerOutcome::Duplicate => Some(Pending {
                outcome: DeliveryOutcome::ACKED,
                offset: None,
                timestamp: None,
                recovery: false,
                retry_after_head: false,
            }),
            BrokerOutcome::Retry => {
                e.prior_ambiguity = true;
                None
            }
            BrokerOutcome::SequenceError => Some(Pending {
                outcome: rejected(FailureReason::SequenceUnresolved),
                offset: None,
                timestamp: None,
                recovery: true,
                retry_after_head: non_head,
            }),
            BrokerOutcome::DefinitiveRejection(reason) => Some(Pending {
                outcome: rejected(reason),
                offset: None,
                timestamp: None,
                recovery: true,
                retry_after_head: false,
            }),
            BrokerOutcome::Fatal(reason) => Some(Pending {
                outcome: DeliveryOutcome::unknown(reason),
                offset: None,
                timestamp: None,
                recovery: true,
                retry_after_head: false,
            }),
        };
        self.refresh_counts(partition);
        let change = if let BrokerOutcome::Fatal(reason) = outcome {
            let mut terminal = Vec::new();
            if deferred {
                self.begin_failure(reason);
            } else {
                self.fail_all(reason, &mut terminal);
            }
            LedgerChange {
                terminal,
                recovery: self.recovery,
            }
        } else {
            self.drain(partition)
        };
        Ok(change)
    }
    /// Expiry/cancellation preserves a response already parsed while waiting for
    /// the partition FIFO head. Otherwise it terminates by cumulative certainty.
    pub fn expire(
        &mut self,
        partition: TopicPartition,
        batch: u64,
        reason: FailureReason,
    ) -> Result<LedgerChange> {
        self.expire_mode(partition, batch, reason, false)
    }
    pub fn expire_deferred(
        &mut self,
        partition: TopicPartition,
        batch: u64,
        reason: FailureReason,
    ) -> Result<LedgerChange> {
        self.expire_mode(partition, batch, reason, true)
    }
    fn expire_mode(
        &mut self,
        partition: TopicPartition,
        batch: u64,
        reason: FailureReason,
        _deferred: bool,
    ) -> Result<LedgerChange> {
        let e = self.entry_mut(partition, batch)?;
        if let Some(pending) = &mut e.pending {
            // Preserve the parsed rejection's certainty, but a terminal
            // deadline/cancellation must no longer wait for another retry.
            pending.retry_after_head = false;
        } else {
            e.pending = Some(e.unresolved(reason));
        }
        self.refresh_counts(partition);
        Ok(self.drain(partition))
    }
    pub fn cancel(&mut self, partition: TopicPartition, batch: u64) -> Result<LedgerChange> {
        self.expire(partition, batch, FailureReason::Cancelled)
    }
    pub fn fail_closed(&mut self, reason: FailureReason) -> LedgerChange {
        let mut terminal = Vec::new();
        self.fail_all(reason, &mut terminal);
        LedgerChange {
            terminal,
            recovery: self.recovery,
        }
    }
    /// A refresh is safe only once all active/transmitted entries are terminal.
    /// Never-transmitted assigned entries may remain; their canonical payload is
    /// re-finalized after the actor has retired every old request plan.
    pub fn begin_identity_refresh(&mut self) -> Result<ProducerIdentity> {
        if self.recovery == RecoveryState::FailedClosed {
            return Err(LedgerError::FailedClosed);
        }
        if self.recovery != RecoveryState::NeedsIdentity {
            return Err(LedgerError::RecoveryPending);
        }
        if self.counts.refresh_blocked != 0 {
            return Err(LedgerError::NotQuiescent);
        }
        self.recovery = RecoveryState::RefreshingIdentity;
        Ok(self.identity)
    }
    /// Synchronous convenience operation. Owners with a work quota use
    /// `begin_identity_install` and `install_identity_step` instead.
    pub fn install_identity(&mut self, identity: ProducerIdentity) -> Result<Vec<Refinalization>> {
        if !identity.is_valid() {
            return Err(LedgerError::InvalidIdentity);
        }
        if self.recovery != RecoveryState::RefreshingIdentity || self.identity_install.is_some() {
            return Err(LedgerError::RecoveryPending);
        }
        if self.counts.refresh_blocked != 0 {
            return Err(LedgerError::NotQuiescent);
        }
        self.begin_identity_install(identity)?;
        let mut changed = Vec::new();
        loop {
            let progress = self.install_identity_step()?;
            match progress.change {
                Some(IdentityChange::Refinalized(change)) => changed.push(change),
                Some(IdentityChange::Terminal(_)) => {
                    unreachable!("quiescent synchronous installation cannot create terminals")
                }
                None => {}
            }
            if progress.complete {
                return Ok(changed);
            }
        }
    }
    fn drain(&mut self, partition: TopicPartition) -> LedgerChange {
        let mut terminal = Vec::new();
        let p = self
            .partitions
            .get_mut(&partition)
            .expect("validated partition");
        let mut failed_head = false;
        while p.entries.front().is_some_and(|e| e.pending.is_some()) {
            if p.entries.front().unwrap().pending.unwrap().retry_after_head && !failed_head {
                break;
            }
            let e = p.entries.pop_front().unwrap();
            assert!(self.live_batches.remove(&e.assignment.batch));
            let pending = e.pending.unwrap();
            failed_head |= pending.recovery;
            if pending.recovery && self.recovery == RecoveryState::Active {
                self.recovery = RecoveryState::NeedsIdentity;
            }
            if pending.outcome.kind == DeliveryKind::Unknown {
                self.unresolved_partitions.insert(partition);
            }
            terminal.push(e.terminal(pending));
        }
        if p.entries.is_empty() {
            self.nonempty.remove(&partition);
        }
        self.refresh_counts(partition);
        LedgerChange {
            terminal,
            recovery: self.recovery,
        }
    }
    /// Establishes a sticky failure fence without visiting any partition.
    pub fn begin_failure(&mut self, reason: FailureReason) {
        self.identity_install = None;
        self.failure_reason.get_or_insert(reason);
        self.recovery = RecoveryState::FailedClosed;
    }
    pub fn failure_reason(&self) -> Option<FailureReason> {
        self.failure_reason
    }
    pub fn has_failed_entries(&self) -> bool {
        self.failure_reason.is_some() && !self.nonempty.is_empty()
    }
    /// Releases at most maximum sequence entries. Parsed terminal successes
    /// retain their proof; unresolved entries retain cumulative certainty.
    pub fn drain_failed(&mut self, maximum: usize) -> Vec<TerminalBatch> {
        let mut terminal = Vec::new();
        let Some(reason) = self.failure_reason else {
            return terminal;
        };
        for _ in 0..maximum {
            let Some(&partition) = self.nonempty.first() else {
                break;
            };
            let p = self
                .partitions
                .get_mut(&partition)
                .expect("indexed partition");
            let entry = p.entries.pop_front().expect("nonempty index");
            assert!(self.live_batches.remove(&entry.assignment.batch));
            if p.entries.is_empty() {
                self.nonempty.remove(&partition);
            }
            let pending = entry.pending.unwrap_or_else(|| entry.unresolved(reason));
            terminal.push(entry.terminal(pending));
            self.refresh_counts(partition);
        }
        terminal
    }
    fn fail_all(&mut self, reason: FailureReason, terminal: &mut Vec<TerminalBatch>) {
        self.begin_failure(reason);
        terminal.extend(self.drain_failed(usize::MAX));
    }
    fn entry(&self, partition: TopicPartition, batch: u64) -> Result<&Entry> {
        self.partitions
            .get(&partition)
            .ok_or(LedgerError::UnknownPartition)?
            .entries
            .iter()
            .find(|e| e.assignment.batch == batch)
            .ok_or(LedgerError::UnknownBatch)
    }
    fn entry_mut(&mut self, partition: TopicPartition, batch: u64) -> Result<&mut Entry> {
        self.partitions
            .get_mut(&partition)
            .ok_or(LedgerError::UnknownPartition)?
            .entries
            .iter_mut()
            .find(|e| e.assignment.batch == batch)
            .ok_or(LedgerError::UnknownBatch)
    }
    fn active_entry(
        &mut self,
        partition: TopicPartition,
        batch: u64,
        attempt: u64,
    ) -> Result<&mut Entry> {
        let e = self.entry_mut(partition, batch)?;
        match e.current {
            None => Err(LedgerError::AttemptMissing),
            Some(current) if current != attempt => Err(LedgerError::StaleAttempt),
            Some(_) => Ok(e),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::TopicId;
    fn identity() -> ProducerIdentity {
        ProducerIdentity {
            producer_id: 42,
            epoch: 0,
        }
    }
    fn partition(index: i32) -> TopicPartition {
        TopicPartition {
            topic: TopicId([1; 16]),
            partition: index,
        }
    }
    fn ledger() -> ProducerLedger {
        let mut l = ProducerLedger::new(identity(), 4, 5).unwrap();
        l.register(partition(0)).unwrap();
        l.register(partition(1)).unwrap();
        l
    }
    fn assigned(l: &mut ProducerLedger, p: i32, batch: u64) {
        l.assign(partition(p), batch, 1).unwrap();
    }
    fn start(l: &mut ProducerLedger, p: i32, batch: u64, attempt: u64) {
        l.start_attempt(partition(p), batch, attempt).unwrap();
    }
    fn ack(l: &mut ProducerLedger, p: i32, batch: u64, attempt: u64) -> LedgerChange {
        l.response(
            partition(p),
            batch,
            attempt,
            BrokerOutcome::Success {
                base_offset: Some(batch as i64),
                timestamp: None,
            },
        )
        .unwrap()
    }
    #[test]
    fn parsed_non_head_recovery_fences_younger_retry_until_the_head_resolves() {
        let mut l = ledger();
        for batch in 0..3 {
            assigned(&mut l, 0, batch);
            start(&mut l, 0, batch, batch + 1);
        }
        l.retire_attempt(partition(0), 0, 1, true).unwrap();
        l.response(partition(0), 1, 2, BrokerOutcome::SequenceError)
            .unwrap();
        l.retire_attempt(partition(0), 2, 3, true).unwrap();
        assert!(l.attempt_order_ready(partition(0), 0).unwrap());
        start(&mut l, 0, 0, 4);
        assert!(!l.attempt_order_ready(partition(0), 2).unwrap());
        assert_eq!(
            l.start_attempt(partition(0), 2, 5),
            Err(LedgerError::EarlierBatchReady)
        );
        ack(&mut l, 0, 0, 4);
        assert!(!l.attempt_order_ready(partition(0), 2).unwrap());
        assert!(l.attempt_order_ready(partition(0), 1).unwrap());
        start(&mut l, 0, 1, 5);
        assert!(l.attempt_order_ready(partition(0), 2).unwrap());
    }
    #[test]
    fn unresolved_window_survives_reconnect_and_partial_writes() {
        let mut l = ledger();
        for batch in 0..5 {
            assigned(&mut l, 0, batch);
            start(&mut l, 0, batch, batch);
            l.mark_transmitted(partition(0), batch, batch).unwrap();
        }
        for batch in 0..5 {
            l.retire_attempt(partition(0), batch, batch, false).unwrap();
        }
        assert_eq!(l.assign(partition(0), 6, 1), Err(LedgerError::WindowFull));
        assert_eq!(l.stats().unresolved, 5);
        assert_eq!(l.stats().ambiguous_batches, 5);
        for batch in 0..5 {
            let a = l.assignment(partition(0), batch).unwrap();
            assert_eq!(a.base_sequence.get(), batch as i32);
            start(&mut l, 0, batch, 10 + batch);
            let change = l
                .response(partition(0), batch, 10 + batch, BrokerOutcome::Duplicate)
                .unwrap();
            assert_eq!(change.terminal.len(), 1);
            assert_eq!(change.terminal[0].outcome.kind, DeliveryKind::Acked);
        }
        assert_eq!(l.stats().unresolved, 0);
        assert_eq!(l.assign(partition(0), 6, 1).unwrap().base_sequence.get(), 5);
    }
    #[test]
    fn non_head_sequence_error_waits_for_head_and_does_not_prevent_head_retry() {
        let mut l = ledger();
        assigned(&mut l, 0, 0);
        assigned(&mut l, 0, 1);
        start(&mut l, 0, 0, 1);
        start(&mut l, 0, 1, 2);
        l.retire_attempt(partition(0), 0, 1, true).unwrap();
        let held = l
            .response(partition(0), 1, 2, BrokerOutcome::SequenceError)
            .unwrap();
        assert!(held.terminal.is_empty());
        assert_eq!(held.recovery, RecoveryState::Active);
        start(&mut l, 0, 0, 3);
        let change = ack(&mut l, 0, 0, 3);
        assert_eq!(
            change
                .terminal
                .iter()
                .map(|b| b.assignment.batch)
                .collect::<Vec<_>>(),
            vec![0]
        );
        assert_eq!(change.terminal[0].outcome.kind, DeliveryKind::Acked);
        assert_eq!(change.recovery, RecoveryState::Active);
        assert_eq!(
            l.assignment(partition(0), 1).unwrap().base_sequence.get(),
            1
        );
        assert!(l.sequence_retry_pending(partition(0), 1).unwrap());
        start(&mut l, 0, 1, 4);
        let retried = ack(&mut l, 0, 1, 4);
        assert_eq!(retried.terminal.len(), 1);
        assert_eq!(retried.terminal[0].outcome.kind, DeliveryKind::Acked);
        assert_eq!(retried.terminal[0].attempts, 2);
        assert_eq!(retried.recovery, RecoveryState::Active);
        assert_eq!(l.identity(), identity());
    }
    #[test]
    fn global_refresh_still_allows_unsent_head_to_unblock_later_old_identity_attempt() {
        let mut l = ledger();
        assigned(&mut l, 0, 0);
        assigned(&mut l, 0, 1);
        assigned(&mut l, 1, 2);
        start(&mut l, 0, 0, 1);
        start(&mut l, 0, 1, 2);
        start(&mut l, 1, 2, 3);
        l.retire_attempt(partition(0), 0, 1, false).unwrap();
        l.response(partition(0), 1, 2, BrokerOutcome::SequenceError)
            .unwrap();
        l.response(partition(1), 2, 3, BrokerOutcome::SequenceError)
            .unwrap();
        assert_eq!(l.recovery_state(), RecoveryState::NeedsIdentity);
        assert_eq!(l.begin_identity_refresh(), Err(LedgerError::NotQuiescent));
        start(&mut l, 0, 0, 4);
        let result = ack(&mut l, 0, 0, 4);
        assert_eq!(result.terminal.len(), 1);
        assert_eq!(result.terminal[0].outcome.kind, DeliveryKind::Acked);
        assert_eq!(l.begin_identity_refresh(), Err(LedgerError::NotQuiescent));
        start(&mut l, 0, 1, 5);
        assert_eq!(ack(&mut l, 0, 1, 5).terminal.len(), 1);
        assert_eq!(l.begin_identity_refresh(), Ok(identity()));
    }
    #[test]
    fn held_sequence_retry_preserves_ambiguity_until_success_or_expiry() {
        for earlier_ambiguity in [false, true] {
            for expire in [false, true] {
                let mut l = ledger();
                assigned(&mut l, 0, 0);
                assigned(&mut l, 0, 1);
                let assignment = l.assignment(partition(0), 1).unwrap();
                start(&mut l, 0, 0, 1);
                start(&mut l, 0, 1, 2);
                if earlier_ambiguity {
                    l.retire_attempt(partition(0), 1, 2, true).unwrap();
                    start(&mut l, 0, 1, 3);
                }
                let held = l
                    .response(
                        partition(0),
                        1,
                        if earlier_ambiguity { 3 } else { 2 },
                        BrokerOutcome::SequenceError,
                    )
                    .unwrap();
                assert!(held.terminal.is_empty());
                assert!(!l.attempt_order_ready(partition(0), 1).unwrap());
                assert_eq!(l.stats().unresolved, 2);
                assert_eq!(ack(&mut l, 0, 0, 1).terminal.len(), 1);
                assert!(l.attempt_order_ready(partition(0), 1).unwrap());
                assert_eq!(l.assignment(partition(0), 1).unwrap(), assignment);
                if expire {
                    let change = l.expire(partition(0), 1, FailureReason::Deadline).unwrap();
                    assert_eq!(change.terminal.len(), 1);
                    assert_eq!(
                        change.terminal[0].outcome.kind,
                        if earlier_ambiguity {
                            DeliveryKind::Unknown
                        } else {
                            DeliveryKind::NotWritten
                        }
                    );
                    assert_eq!(change.recovery, RecoveryState::NeedsIdentity);
                } else {
                    start(&mut l, 0, 1, 4);
                    let change = ack(&mut l, 0, 1, 4);
                    assert_eq!(change.terminal.len(), 1);
                    assert_eq!(change.terminal[0].outcome.kind, DeliveryKind::Acked);
                    assert_eq!(change.recovery, RecoveryState::Active);
                    assert_eq!(l.stats().unresolved_partitions, 0);
                }
                assert_eq!(l.stats().unresolved, 0);
            }
        }
    }
    #[test]
    fn terminal_head_failure_does_not_retry_held_sequence_errors_across_a_gap() {
        let mut l = ledger();
        for batch in 0..5 {
            assigned(&mut l, 0, batch);
            start(&mut l, 0, batch, batch + 1);
        }
        for batch in 1..5 {
            assert!(
                l.response(partition(0), batch, batch + 1, BrokerOutcome::SequenceError)
                    .unwrap()
                    .terminal
                    .is_empty()
            );
        }
        assert_eq!(l.stats().unresolved, 5);
        assert_eq!(l.stats().active_attempts, 1);
        let change = l
            .response(
                partition(0),
                0,
                1,
                BrokerOutcome::DefinitiveRejection(FailureReason::BrokerRejected),
            )
            .unwrap();
        assert_eq!(change.terminal.len(), 5);
        assert!(
            change
                .terminal
                .iter()
                .all(|entry| entry.outcome.kind == DeliveryKind::NotWritten)
        );
        assert_eq!(change.recovery, RecoveryState::NeedsIdentity);
        assert_eq!(l.stats().unresolved, 0);
        assert_eq!(l.begin_identity_refresh(), Ok(identity()));
    }
    #[test]
    fn rejected_retry_never_erases_a_prior_possible_commit() {
        for rejection in [
            BrokerOutcome::SequenceError,
            BrokerOutcome::DefinitiveRejection(FailureReason::BrokerRejected),
        ] {
            let mut l = ledger();
            assigned(&mut l, 0, 0);
            start(&mut l, 0, 0, 1);
            l.retire_attempt(partition(0), 0, 1, true).unwrap();
            start(&mut l, 0, 0, 2);
            let result = l.response(partition(0), 0, 2, rejection).unwrap();
            assert_eq!(result.terminal[0].outcome.kind, DeliveryKind::Unknown);
            assert_eq!(result.recovery, RecoveryState::NeedsIdentity);
            assert_eq!(l.begin_identity_refresh(), Ok(identity()));
            let mut l = ledger();
            assigned(&mut l, 0, 0);
            start(&mut l, 0, 0, 1);
            let result = l.response(partition(0), 0, 1, rejection).unwrap();
            assert_eq!(result.terminal[0].outcome.kind, DeliveryKind::NotWritten);
            assert!(result.terminal[0].transmitted);
            assert_eq!(result.recovery, RecoveryState::NeedsIdentity);
        }
    }
    #[test]
    fn expiry_while_write_result_pending_is_unknown_but_notapplied_can_remain_unsent() {
        let mut l = ledger();
        assigned(&mut l, 0, 0);
        start(&mut l, 0, 0, 1);
        let result = l.expire(partition(0), 0, FailureReason::Deadline).unwrap();
        assert_eq!(result.terminal[0].outcome.kind, DeliveryKind::Unknown);
        assert!(result.terminal[0].transmitted);
        let mut l = ledger();
        assigned(&mut l, 0, 0);
        start(&mut l, 0, 0, 1);
        l.retire_attempt(partition(0), 0, 1, false).unwrap();
        let result = l.cancel(partition(0), 0).unwrap();
        assert_eq!(result.terminal[0].outcome.kind, DeliveryKind::NotWritten);
        assert!(!result.terminal[0].transmitted);
        assert_eq!(result.recovery, RecoveryState::NeedsIdentity);
    }
    #[test]
    fn refresh_is_global_quiescent_and_installs_actual_fresh_pid_zero_epoch() {
        let mut l = ledger();
        assigned(&mut l, 0, 0);
        assigned(&mut l, 0, 1);
        assigned(&mut l, 1, 2);
        start(&mut l, 0, 0, 1);
        start(&mut l, 1, 2, 2);
        let result = l
            .response(partition(0), 0, 1, BrokerOutcome::SequenceError)
            .unwrap();
        assert_eq!(result.recovery, RecoveryState::NeedsIdentity);
        assert_eq!(l.begin_identity_refresh(), Err(LedgerError::NotQuiescent));
        assert_eq!(
            l.start_attempt(partition(0), 1, 3),
            Err(LedgerError::RecoveryPending)
        );
        ack(&mut l, 1, 2, 2);
        l.begin_identity_refresh().unwrap();
        assert_eq!(
            l.assign(partition(1), 3, 1),
            Err(LedgerError::RecoveryPending)
        );
        let fresh = ProducerIdentity {
            producer_id: 99,
            epoch: 0,
        };
        let changed = l.install_identity(fresh).unwrap();
        assert_eq!(changed.len(), 1);
        assert_eq!(changed[0].before.base_sequence.get(), 1);
        assert_eq!(changed[0].after.base_sequence, Sequence::ZERO);
        assert_eq!(changed[0].after.identity, fresh);
        assert_eq!(l.identity(), fresh);
        assert_eq!(l.recovery_state(), RecoveryState::Active);
        assert_eq!(
            l.assign(partition(1), 3, 1).unwrap().base_sequence,
            Sequence::ZERO
        );
    }
    #[test]
    fn terminal_unknown_preserves_other_partitions_and_success_proofs_until_recovery() {
        let mut l = ledger();
        for (p, b) in [(0, 0), (0, 1), (1, 2), (1, 3)] {
            assigned(&mut l, p, b);
        }
        start(&mut l, 0, 0, 1);
        start(&mut l, 0, 1, 2);
        let held = ack(&mut l, 0, 1, 2);
        assert!(held.terminal.is_empty());
        start(&mut l, 1, 2, 3);
        let result = l.expire(partition(0), 0, FailureReason::Deadline).unwrap();
        assert_eq!(result.terminal.len(), 2);
        let kinds: BTreeMap<_, _> = result
            .terminal
            .iter()
            .map(|t| (t.assignment.batch, t.outcome.kind))
            .collect();
        assert_eq!(kinds[&0], DeliveryKind::Unknown);
        assert_eq!(kinds[&1], DeliveryKind::Acked);
        assert_eq!(l.stats().unresolved, 2);
        assert_eq!(l.stats().unresolved_partitions, 1);
        assert_eq!(
            l.assign(partition(0), 9, 1),
            Err(LedgerError::RecoveryPending)
        );
        assert_eq!(l.begin_identity_refresh(), Err(LedgerError::NotQuiescent));
        assert_eq!(
            ack(&mut l, 1, 2, 3).terminal[0].outcome.kind,
            DeliveryKind::Acked
        );
        l.begin_identity_refresh().unwrap();
        let next = identity().next_epoch().unwrap();
        let rewritten = l.install_identity(next).unwrap();
        assert_eq!(rewritten.len(), 1);
        assert_eq!(rewritten[0].after.batch, 3);
        assert_eq!(rewritten[0].after.base_sequence, Sequence::ZERO);
        assert_eq!(l.stats().unresolved_partitions, 0);
        assert_eq!(l.assign(partition(0), 9, 1).unwrap().identity, next);
    }
    #[test]
    fn stale_results_cannot_ack_a_new_attempt_or_reassigned_identity() {
        let mut l = ledger();
        assigned(&mut l, 0, 0);
        start(&mut l, 0, 0, 1);
        l.retire_attempt(partition(0), 0, 1, false).unwrap();
        start(&mut l, 0, 0, 2);
        assert_eq!(
            l.response(partition(0), 0, 1, BrokerOutcome::Duplicate),
            Err(LedgerError::StaleAttempt)
        );
        assert_eq!(
            l.mark_transmitted(partition(0), 0, 1),
            Err(LedgerError::StaleAttempt)
        );
        let result = ack(&mut l, 0, 0, 2);
        assert_eq!(result.terminal.len(), 1);
        assert_eq!(
            l.response(partition(0), 0, 2, BrokerOutcome::Duplicate),
            Err(LedgerError::UnknownBatch)
        );
    }
    #[test]
    fn sequence_count_wrap_and_rejection_hole_are_explicit() {
        let mut l = ledger();
        let a = l.assign(partition(0), 0, i32::MAX as u32).unwrap();
        assert_eq!(a.base_sequence, Sequence::ZERO);
        start(&mut l, 0, 0, 1);
        ack(&mut l, 0, 0, 1);
        let a = l.assign(partition(0), 1, 2).unwrap();
        assert_eq!(a.base_sequence.get(), i32::MAX);
        start(&mut l, 0, 1, 2);
        ack(&mut l, 0, 1, 2);
        assert_eq!(l.assign(partition(0), 2, 1).unwrap().base_sequence.get(), 1);
        l.cancel(partition(0), 2).unwrap();
        assert_eq!(
            l.assign(partition(0), 3, 1),
            Err(LedgerError::RecoveryPending)
        );
    }
    #[test]
    fn pending_success_is_not_downgraded_by_later_deadline() {
        let mut l = ledger();
        assigned(&mut l, 0, 0);
        assigned(&mut l, 0, 1);
        start(&mut l, 0, 0, 1);
        start(&mut l, 0, 1, 2);
        ack(&mut l, 0, 1, 2);
        assert!(
            l.expire(partition(0), 1, FailureReason::Deadline)
                .unwrap()
                .terminal
                .is_empty()
        );
        let result = ack(&mut l, 0, 0, 1);
        assert!(
            result
                .terminal
                .iter()
                .all(|t| t.outcome.kind == DeliveryKind::Acked)
        );
    }
    #[test]
    fn identity_install_rechecks_a_cancellation_that_arrived_during_refresh() {
        let mut l = ledger();
        assigned(&mut l, 0, 0);
        assigned(&mut l, 0, 1);
        assigned(&mut l, 1, 2);
        start(&mut l, 1, 2, 1);
        l.response(partition(1), 2, 1, BrokerOutcome::SequenceError)
            .unwrap();
        l.begin_identity_refresh().unwrap();
        assert!(
            l.expire_deferred(partition(0), 1, FailureReason::Cancelled)
                .unwrap()
                .terminal
                .is_empty()
        );
        let next = ProducerIdentity {
            producer_id: 43,
            epoch: 0,
        };
        assert_eq!(l.install_identity(next), Err(LedgerError::NotQuiescent));
        assert_eq!(l.identity(), identity());
        assert_eq!(l.assignment(partition(0), 0).unwrap().identity, identity());
        assert_eq!(l.assignment(partition(0), 1).unwrap().identity, identity());
        assert_eq!(l.counts.refresh_blocked, 1);
        assert_eq!(
            l.expire_deferred(partition(0), 0, FailureReason::Cancelled)
                .unwrap()
                .terminal
                .len(),
            2
        );
        let before = l.stats();
        assert!(l.install_identity(next).unwrap().is_empty());
        assert_eq!(l.stats(), before);
        assert_eq!(l.counts, EntryCounts::default());
        assert_eq!(l.identity(), next);
    }
    #[test]
    fn live_batch_index_and_counts_survive_cross_partition_failure_drain() {
        let mut l = ProducerLedger::new(identity(), 257, 5).unwrap();
        for index in 0..257 {
            l.register(partition(index)).unwrap();
            for offset in 0..5 {
                let batch = index as u64 * 5 + offset;
                l.assign(partition(index), batch, 1).unwrap();
                l.start_attempt(partition(index), batch, batch + 1).unwrap();
            }
            for offset in 0..5 {
                let batch = index as u64 * 5 + offset;
                l.retire_attempt(partition(index), batch, batch + 1, true)
                    .unwrap();
            }
        }
        assert_eq!(
            l.stats(),
            LedgerStats {
                partitions: 257,
                unresolved: 1285,
                active_attempts: 0,
                ambiguous_batches: 1285,
                unresolved_partitions: 0,
            }
        );
        let before = l.stats();
        assert_eq!(
            l.assign(partition(256), 0, 1),
            Err(LedgerError::DuplicateBatch)
        );
        assert_eq!(l.stats(), before);
        assert_eq!(l.counts.refresh_blocked, 1285);
        l.begin_failure(FailureReason::Closed);
        for remaining in (0..1285).rev() {
            let terminal = l.drain_failed(1);
            assert_eq!(terminal.len(), 1);
            assert_eq!(terminal[0].outcome.kind, DeliveryKind::Unknown);
            assert_eq!(l.stats().unresolved, remaining);
            assert_eq!(l.stats().ambiguous_batches, remaining);
            assert_eq!(l.counts.refresh_blocked, remaining);
            assert_eq!(l.live_batches.len(), remaining);
        }
        assert!(!l.has_failed_entries());
        assert_eq!(l.recovery_state(), RecoveryState::FailedClosed);
        assert_eq!(l.failure_reason(), Some(FailureReason::Closed));
        assert_eq!(l.counts, EntryCounts::default());
    }
    #[test]
    fn seeded_attempt_histories_match_independent_certainty_oracle() {
        #[derive(Default, Debug)]
        struct Model {
            active: bool,
            prior: bool,
            written: bool,
            ever: bool,
            attempt: u64,
            pending: Option<DeliveryKind>,
        }
        for seed in 1u64..=128 {
            let mut l = ledger();
            let mut model = BTreeMap::<u64, Model>::new();
            let mut random = seed;
            let mut next = 0u64;
            for step in 0..128 {
                random ^= random << 13;
                random ^= random >> 7;
                random ^= random << 17;
                let operation = random % 6;
                let mut change = None;
                if operation == 0 && next < 32 && l.assign(partition(0), next, 1).is_ok() {
                    model.insert(next, Model::default());
                    next += 1;
                } else if !model.is_empty() {
                    let key = *model
                        .keys()
                        .nth((random as usize / 8) % model.len())
                        .unwrap();
                    let m = model.get_mut(&key).unwrap();
                    match operation {
                        1 if !m.active && m.pending.is_none() => {
                            if l.start_attempt(partition(0), key, m.attempt + 1).is_ok() {
                                m.attempt += 1;
                                m.active = true;
                                m.written = false;
                            }
                        }
                        2 if m.active => {
                            let variant = (random >> 8) % 3;
                            let response = match variant {
                                0 => BrokerOutcome::Duplicate,
                                1 => BrokerOutcome::DefinitiveRejection(
                                    FailureReason::BrokerRejected,
                                ),
                                _ => BrokerOutcome::SequenceError,
                            };
                            m.active = false;
                            m.ever = true;
                            m.pending = Some(if variant == 0 {
                                DeliveryKind::Acked
                            } else if m.prior {
                                DeliveryKind::Unknown
                            } else {
                                DeliveryKind::NotWritten
                            });
                            change =
                                Some(l.response(partition(0), key, m.attempt, response).unwrap());
                        }
                        3 if m.active => {
                            let may = (random >> 12) & 1 != 0;
                            let ambiguous = may || m.written;
                            m.prior |= ambiguous;
                            m.ever |= ambiguous;
                            m.active = false;
                            l.retire_attempt(partition(0), key, m.attempt, may).unwrap();
                        }
                        4 if m.active => {
                            m.written = true;
                            m.ever = true;
                            l.mark_transmitted(partition(0), key, m.attempt).unwrap();
                        }
                        5 => {
                            if m.pending.is_none() {
                                m.pending = Some(if m.active || m.ever {
                                    DeliveryKind::Unknown
                                } else {
                                    DeliveryKind::NotWritten
                                });
                            }
                            change = Some(l.cancel(partition(0), key).unwrap());
                        }
                        _ => {}
                    }
                }
                if let Some(change) = change {
                    for terminal in change.terminal {
                        let m = model.remove(&terminal.assignment.batch).unwrap();
                        let expected = m.pending.unwrap_or(if m.active || m.ever {
                            DeliveryKind::Unknown
                        } else {
                            DeliveryKind::NotWritten
                        });
                        assert_eq!(
                            terminal.outcome.kind, expected,
                            "seed={seed} step={step} op={operation} model={m:?}"
                        );
                    }
                }
                assert_eq!(l.stats().unresolved, model.len(), "seed={seed} step={step}");
                assert_eq!(
                    l.stats().active_attempts,
                    model.values().filter(|m| m.active).count(),
                    "seed={seed} step={step}"
                );
                assert_eq!(
                    l.stats().ambiguous_batches,
                    model.values().filter(|m| m.prior).count(),
                    "seed={seed} step={step}"
                );
                assert_eq!(
                    l.counts.refresh_blocked,
                    model
                        .values()
                        .filter(|m| m.active || m.ever || m.pending.is_some())
                        .count(),
                    "seed={seed} step={step}"
                );
                assert_eq!(
                    l.live_batches,
                    model.keys().copied().collect(),
                    "seed={seed} step={step}"
                );
                assert!(model.len() <= 5);
                if l.recovery_state() == RecoveryState::FailedClosed {
                    assert!(model.is_empty());
                    break;
                }
                if l.recovery_state() == RecoveryState::NeedsIdentity
                    && l.begin_identity_refresh().is_ok()
                {
                    let next_id = ProducerIdentity {
                        producer_id: l.identity().producer_id + 1,
                        epoch: 0,
                    };
                    l.install_identity(next_id).unwrap();
                }
            }
            let terminal = l.fail_closed(FailureReason::Closed).terminal;
            assert_eq!(terminal.len(), model.len());
            for terminal in terminal {
                let m = model.remove(&terminal.assignment.batch).unwrap();
                let expected = m.pending.unwrap_or(if m.active || m.ever {
                    DeliveryKind::Unknown
                } else {
                    DeliveryKind::NotWritten
                });
                assert_eq!(terminal.outcome.kind, expected, "seed={seed}");
            }
            assert_eq!(l.stats().unresolved, 0);
        }
    }
}
