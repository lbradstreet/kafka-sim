//! Independent observations and delivery invariants, with no producer dependency.
use crate::{CommittedBatch, CommittedRecord, TopicId};
use alloc::{collections::BTreeMap, vec::Vec};
use kr_kafka_record::SharedBytes;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct OracleLimits {
    pub records: usize,
    pub leases: usize,
    pub flushes: usize,
    pub operations: usize,
    pub retained_bytes: usize,
    pub segments_per_operation: usize,
    pub credit_pools: usize,
}
impl Default for OracleLimits {
    fn default() -> Self {
        Self {
            records: 100_000,
            leases: 4096,
            flushes: 128,
            operations: 64,
            retained_bytes: 8 * 1024 * 1024,
            segments_per_operation: 64,
            credit_pools: 32,
        }
    }
}
/// `returned_at` is an observation ordinal, not wall/simulated time. The harness
/// assigns a distinct token per accepted record, even if application user tokens
/// repeat, and records its routing decision independently from delivery events.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct AcceptedRecord {
    pub token: u64,
    pub topic: TopicId,
    pub partition: i32,
    pub lease: Option<u64>,
    pub returned_at: u64,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ObservedOutcome {
    Acked,
    NotWritten,
    Unknown,
}
/// A successful, fully parsed response for the record's partition and sequence.
/// `at` is the independent observation ordinal. The success offset includes the
/// record's offset delta; its timestamp is response metadata, not CreateTime.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ObservedResponse {
    Success {
        at: u64,
        offset: i64,
        timestamp: Option<i64>,
    },
    /// Explicit DUPLICATE_SEQUENCE_NUMBER acknowledges the original append,
    /// but does not establish an offset or timestamp for the public delivery.
    Duplicate { at: u64 },
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ObservedDelivery {
    pub token: u64,
    pub topic: TopicId,
    pub partition: i32,
    pub outcome: ObservedOutcome,
    pub offset: Option<i64>,
    pub timestamp: Option<i64>,
    pub attempts: u32,
    /// Complete Produce requests observed independently at the broker. This is
    /// a lower bound: an admitted transport attempt may never form a full frame.
    pub parsed_attempts: u32,
    pub at: u64,
    pub transmitted: bool,
    pub definitive_broker_rejection: bool,
    /// A previous attempt could have committed and has not been reconciled by
    /// success/deduplication. Rejection of the current attempt cannot clear it.
    pub prior_ambiguous_attempt: bool,
    /// Socket completion alone never supplies a response witness.
    pub response: Option<ObservedResponse>,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CreditObservation {
    pub pool: u32,
    pub capacity: u64,
    pub reserved: u64,
    pub released: u64,
    pub held: u64,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Violation {
    pub invariant: &'static str,
    pub token: Option<u64>,
}
impl core::fmt::Display for Violation {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(f, "{} token={:?}", self.invariant, self.token)
    }
}
impl core::error::Error for Violation {}
type Result<T> = core::result::Result<T, Violation>;
fn fail(invariant: &'static str, token: Option<u64>) -> Violation {
    Violation { invariant, token }
}
#[derive(Clone)]
struct RecordState {
    accepted: AcceptedRecord,
    partition_unassigned: bool,
    input_consumed: Option<u64>,
    delivery: Option<ObservedDelivery>,
    topic_bound_at: Option<u64>,
}
#[derive(Clone)]
struct LeaseState {
    id: u64,
    released_at: Option<u64>,
}
#[derive(Clone)]
struct FlushState {
    id: u64,
    watermark: usize,
    at: u64,
    completed: bool,
}
#[derive(Clone)]
struct RetainedOperation {
    id: u64,
    segments: Vec<(SharedBytes, Vec<u8>)>,
}
/// The harness supplies accepted-prefix results, consumed-input observations,
/// parsed-response proofs, public events, credit snapshots and retained provider
/// spans. The oracle retains its own immutable history and validates the broker
/// log independently of producer ledger/state transitions.
#[derive(Clone)]
pub struct DeliveryOracle {
    limits: OracleLimits,
    records: Vec<RecordState>,
    token_index: BTreeMap<u64, usize>,
    leases: Vec<LeaseState>,
    flushes: Vec<FlushState>,
    operations: Vec<RetainedOperation>,
    credits: Vec<CreditObservation>,
    closed: bool,
}
impl DeliveryOracle {
    pub fn new(limits: OracleLimits) -> Self {
        Self {
            limits,
            records: Vec::new(),
            token_index: BTreeMap::new(),
            leases: Vec::new(),
            flushes: Vec::new(),
            operations: Vec::new(),
            credits: Vec::new(),
            closed: false,
        }
    }
    pub fn accept(&mut self, record: AcceptedRecord) -> Result<()> {
        self.accept_partition(record, false)
    }
    /// The caller supplied no explicit partition. `record.partition` is still
    /// the independent expected route if routing occurs. A terminal NotWritten
    /// with no transmission may instead preserve the producer's -1 sentinel.
    pub fn accept_unassigned_partition(&mut self, record: AcceptedRecord) -> Result<()> {
        self.accept_partition(record, true)
    }
    fn accept_partition(
        &mut self,
        record: AcceptedRecord,
        partition_unassigned: bool,
    ) -> Result<()> {
        if self.closed {
            return Err(fail("C10 admission after Closed", Some(record.token)));
        }
        if self.records.len() == self.limits.records {
            return Err(fail("oracle record capacity", Some(record.token)));
        }
        if self.token_index.contains_key(&record.token) {
            return Err(fail("C1 duplicate accepted token", Some(record.token)));
        }
        if let Some(lease) = record.lease {
            if let Some(state) = self.leases.iter().find(|l| l.id == lease) {
                if state.released_at.is_some() {
                    return Err(fail("C2 admission after lease release", Some(lease)));
                }
            } else {
                if self.leases.len() == self.limits.leases {
                    return Err(fail("oracle lease capacity", Some(lease)));
                }
                self.leases.push(LeaseState {
                    id: lease,
                    released_at: None,
                });
            }
        }
        self.token_index.insert(record.token, self.records.len());
        self.records.push(RecordState {
            accepted: record,
            partition_unassigned,
            input_consumed: None,
            delivery: None,
            topic_bound_at: None,
        });
        Ok(())
    }
    /// Resolve an admission whose topic UUID was unknown (`[0; 16]`). The
    /// harness obtains `topic` from an independent metadata observation, never
    /// from the delivery being checked. An admission binds exactly once.
    pub fn bind_topic(&mut self, token: u64, topic: TopicId, at: u64) -> Result<()> {
        let index = *self
            .token_index
            .get(&token)
            .ok_or(fail("C11 resolution of unknown token", Some(token)))?;
        let record = &mut self.records[index];
        if self.closed
            || record.delivery.is_some()
            || record.accepted.topic != [0; 16]
            || record.topic_bound_at.is_some()
            || topic == [0; 16]
            || at <= record.accepted.returned_at
        {
            return Err(fail(
                "C11 invalid or repeated topic resolution",
                Some(token),
            ));
        }
        record.accepted.topic = topic;
        record.topic_bound_at = Some(at);
        Ok(())
    }
    pub fn input_consumed(&mut self, token: u64, at: u64) -> Result<()> {
        let index = *self
            .token_index
            .get(&token)
            .ok_or(fail("C2 unknown input token", Some(token)))?;
        let record = &mut self.records[index];
        if at <= record.accepted.returned_at {
            return Err(fail(
                "C2 input consumed before admission returned",
                Some(token),
            ));
        }
        if record.input_consumed.is_some() {
            return Err(fail("C2 input consumed twice", Some(token)));
        }
        record.input_consumed = Some(at);
        Ok(())
    }
    pub fn delivery(&mut self, event: ObservedDelivery) -> Result<()> {
        let index = *self
            .token_index
            .get(&event.token)
            .ok_or(fail("C1 delivery of rejected record", Some(event.token)))?;
        let record = &mut self.records[index];
        if record.delivery.is_some() {
            return Err(fail("C1 duplicate delivery", Some(event.token)));
        }
        if event.at <= record.accepted.returned_at {
            return Err(fail(
                "C1 delivery before submission returned",
                Some(event.token),
            ));
        }
        let unrouted = record.partition_unassigned
            && event.partition == -1
            && event.outcome == ObservedOutcome::NotWritten
            && event.attempts == 0
            && event.parsed_attempts == 0
            && !event.transmitted
            && !event.prior_ambiguous_attempt
            && event.response.is_none();
        if event.topic != record.accepted.topic
            || (event.partition != record.accepted.partition && !unrouted)
        {
            return Err(fail(
                "C11 delivery routing identity changed",
                Some(event.token),
            ));
        }
        if record.topic_bound_at.is_some_and(|at| at >= event.at) {
            return Err(fail(
                "C11 delivery precedes topic resolution",
                Some(event.token),
            ));
        }
        if event.attempts < event.parsed_attempts
            || (event.transmitted && event.attempts == 0)
            || (event.parsed_attempts != 0 && !event.transmitted)
        {
            return Err(fail(
                "delivery attempts contradict observed transmission",
                Some(event.token),
            ));
        }
        if event.outcome == ObservedOutcome::Acked {
            let response = event
                .response
                .ok_or(fail("C3 Acked without parsed success", Some(event.token)))?;
            if event.parsed_attempts == 0 {
                return Err(fail(
                    "C3 response without observed request",
                    Some(event.token),
                ));
            }
            let response_at = match response {
                ObservedResponse::Success {
                    at,
                    offset,
                    timestamp,
                } => {
                    if offset < 0
                        || timestamp.is_some_and(|value| value < 0)
                        || event.offset != Some(offset)
                        || event.timestamp != timestamp
                    {
                        return Err(fail(
                            "C3 delivery metadata differs from parsed success",
                            Some(event.token),
                        ));
                    }
                    at
                }
                ObservedResponse::Duplicate { at } => {
                    if event.offset.is_some() || event.timestamp.is_some() {
                        return Err(fail(
                            "C3 duplicate response has no delivery offset or timestamp",
                            Some(event.token),
                        ));
                    }
                    at
                }
            };
            if response_at <= record.accepted.returned_at || response_at >= event.at {
                return Err(fail(
                    "C3 success outside admission/delivery interval",
                    Some(event.token),
                ));
            }
        } else if event.offset.is_some() || event.timestamp.is_some() {
            return Err(fail(
                "non-Acked delivery carries success metadata",
                Some(event.token),
            ));
        }
        if event.outcome == ObservedOutcome::NotWritten
            && (event.prior_ambiguous_attempt
                || (event.transmitted && !event.definitive_broker_rejection))
        {
            return Err(fail(
                "C4 ambiguous transmission reported NotWritten",
                Some(event.token),
            ));
        }
        record.delivery = Some(event);
        Ok(())
    }
    pub fn input_released(&mut self, lease: u64, at: u64) -> Result<()> {
        let state = self
            .leases
            .iter_mut()
            .find(|l| l.id == lease)
            .ok_or(fail("C2 unknown lease release", Some(lease)))?;
        if state.released_at.is_some() {
            return Err(fail("C2 duplicate lease release", Some(lease)));
        }
        for record in self
            .records
            .iter()
            .filter(|r| r.accepted.lease == Some(lease))
        {
            if record.input_consumed.is_none_or(|t| t > at)
                && record.delivery.is_none_or(|d| d.at > at)
            {
                return Err(fail(
                    "C2 release before last input consumed or failed",
                    Some(lease),
                ));
            }
        }
        state.released_at = Some(at);
        Ok(())
    }
    pub fn flush(&mut self, token: u64, at: u64) -> Result<()> {
        if self.flushes.len() == self.limits.flushes {
            return Err(fail("oracle flush capacity", Some(token)));
        }
        if self.flushes.iter().any(|f| f.id == token) {
            return Err(fail("C9 duplicate flush token", Some(token)));
        }
        if self.records.iter().any(|r| r.accepted.returned_at >= at) {
            return Err(fail("C9 invalid flush watermark ordinal", Some(token)));
        }
        self.flushes.push(FlushState {
            id: token,
            watermark: self.records.len(),
            at,
            completed: false,
        });
        Ok(())
    }
    pub fn flush_done(&mut self, token: u64, at: u64) -> Result<()> {
        let flush = self
            .flushes
            .iter_mut()
            .find(|f| f.id == token)
            .ok_or(fail("C9 unknown FlushDone", Some(token)))?;
        if flush.completed || at <= flush.at {
            return Err(fail("C9 duplicate or premature FlushDone", Some(token)));
        }
        if self.records[..flush.watermark]
            .iter()
            .any(|r| r.delivery.is_none_or(|d| d.at >= at))
        {
            return Err(fail(
                "C9 FlushDone precedes delivery watermark",
                Some(token),
            ));
        }
        flush.completed = true;
        Ok(())
    }
    pub fn credits(&mut self, observation: CreditObservation) -> Result<()> {
        if observation.reserved.checked_sub(observation.released) != Some(observation.held)
            || observation.held > observation.capacity
        {
            return Err(fail(
                "C7 credit conservation",
                Some(u64::from(observation.pool)),
            ));
        }
        if let Some(old) = self.credits.iter_mut().find(|c| c.pool == observation.pool) {
            if observation.reserved < old.reserved || observation.released < old.released {
                return Err(fail(
                    "C7 credit counters regressed",
                    Some(u64::from(observation.pool)),
                ));
            }
            *old = observation;
        } else {
            if self.credits.len() == self.limits.credit_pools {
                return Err(fail("oracle credit pool capacity", None));
            }
            self.credits.push(observation);
        }
        Ok(())
    }
    /// Observe retention after provider admission. Release this oracle reference
    /// alongside the provider's terminal completion before checking pool teardown.
    pub fn retain_operation(&mut self, id: u64, segments: &[SharedBytes]) -> Result<()> {
        if self.operations.len() == self.limits.operations
            || segments.is_empty()
            || segments.len() > self.limits.segments_per_operation
            || self.operations.iter().any(|o| o.id == id)
        {
            return Err(fail("C8 operation capacity or duplicate", Some(id)));
        }
        let held: usize = self
            .operations
            .iter()
            .flat_map(|o| &o.segments)
            .map(|(bytes, _)| bytes.allocation_len())
            .sum();
        let size = segments
            .iter()
            .try_fold(0usize, |n, s| n.checked_add(s.allocation_len()))
            .ok_or(fail("oracle retained byte overflow", Some(id)))?;
        if size > self.limits.retained_bytes - held {
            return Err(fail("oracle retained byte capacity", Some(id)));
        }
        self.operations.push(RetainedOperation {
            id,
            segments: segments
                .iter()
                .map(|s| (s.clone(), s.as_slice().to_vec()))
                .collect(),
        });
        Ok(())
    }
    pub fn check_retained_operations(&self) -> Result<()> {
        for operation in &self.operations {
            for (segment, original) in &operation.segments {
                if segment.as_slice() != original {
                    return Err(fail(
                        "C8 retained provider bytes were mutated",
                        Some(operation.id),
                    ));
                }
            }
        }
        Ok(())
    }
    pub fn release_operation(&mut self, id: u64) -> Result<()> {
        self.check_retained_operations()?;
        let i = self
            .operations
            .iter()
            .position(|o| o.id == id)
            .ok_or(fail("C8 unknown terminal operation", Some(id)))?;
        self.operations.swap_remove(i);
        Ok(())
    }
    pub fn closed(&mut self) -> Result<()> {
        if self.closed {
            return Err(fail("C10 duplicate Closed", None));
        }
        if self.records.iter().any(|r| r.delivery.is_none())
            || self.leases.iter().any(|l| l.released_at.is_none())
        {
            return Err(fail("C10 Closed with accepted obligations", None));
        }
        self.closed = true;
        Ok(())
    }
    /// Checks C1–C12 against the committed log. `token_of` independently extracts
    /// harness IDs from actual record bytes; `None` explicitly marks unrelated
    /// producer traffic. The oracle never trusts a producer-provided log.
    pub fn finish(
        &self,
        log: &[CommittedBatch],
        token_of: impl Fn(&CommittedRecord) -> Option<u64>,
    ) -> Result<OracleReport> {
        self.check_retained_operations()?;
        if !self.closed {
            return Err(fail("C10 missing Closed", None));
        }
        if self.flushes.iter().any(|f| !f.completed) {
            return Err(fail("C9 missing FlushDone", None));
        }
        if !self.operations.is_empty() || self.credits.iter().any(|c| c.held != 0) {
            return Err(fail("C7/C8 teardown retains resources", None));
        }
        let mut counts = alloc::vec![0usize;self.records.len()];
        let mut last_order = Vec::new();
        for batch in log {
            for record in &batch.records {
                let Some(token) = token_of(record) else {
                    continue;
                };
                let position = *self
                    .token_index
                    .get(&token)
                    .ok_or(fail("C1 committed unaccepted token", Some(token)))?;
                let expected = &self.records[position];
                if batch.topic != expected.accepted.topic
                    || batch.partition != expected.accepted.partition
                {
                    return Err(fail(
                        "C11/C12 committed to another topic ID or partition",
                        Some(token),
                    ));
                }
                if expected.delivery.is_some_and(|delivery| {
                    delivery.outcome == ObservedOutcome::Acked
                        && delivery
                            .offset
                            .is_some_and(|offset| offset != record.offset)
                }) {
                    return Err(fail(
                        "C3 acknowledged offset differs from committed log",
                        Some(token),
                    ));
                }
                if let Some((_, _, previous)) =
                    last_order.iter_mut().find(|(topic, partition, _)| {
                        *topic == batch.topic && *partition == batch.partition
                    })
                {
                    if position <= *previous {
                        return Err(fail("C6 committed duplicate or reorder", Some(token)));
                    }
                    *previous = position;
                } else {
                    last_order.push((batch.topic, batch.partition, position));
                }
                counts[position] += 1;
            }
        }
        let mut report = OracleReport {
            accepted: self.records.len(),
            ..Default::default()
        };
        for (state, count) in self.records.iter().zip(counts) {
            let delivery = state
                .delivery
                .ok_or(fail("C1 missing Delivery", Some(state.accepted.token)))?;
            match delivery.outcome {
                ObservedOutcome::Acked => {
                    if count != 1 {
                        return Err(fail(
                            "C3 Acked not committed exactly once",
                            Some(state.accepted.token),
                        ));
                    }
                    report.acked += 1;
                }
                ObservedOutcome::NotWritten => {
                    if count != 0 {
                        return Err(fail(
                            "C4 NotWritten appears in log",
                            Some(state.accepted.token),
                        ));
                    }
                    report.not_written += 1;
                }
                ObservedOutcome::Unknown => report.unknown += 1,
            }
        }
        Ok(report)
    }
}
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct OracleReport {
    pub accepted: usize,
    pub acked: usize,
    pub not_written: usize,
    pub unknown: usize,
}
