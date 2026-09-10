//! Atomic prefix admission. Rejected records are neither copied nor assigned IDs.
use crate::{
    config::{DescriptorAdmissionPolicy, ProducerConfig},
    credit::{Claim, CreditError, DescriptorClass, HeldCredits, Resource, SharedCredits},
    input::{InputError, InputLeases, LeasedRecordDescriptor, checked_slice},
    types::{LeaseId, RecordDescriptor, RecordToken, TopicHandle, TopicId},
};
use kr_kafka_record::{OwnedHeader, OwnedRecord};
use kr_runtime::{RuntimeDuration, RuntimeInstant};
use kr_shared_bytes::SharedBytes;
use std::fmt;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum AdmissionError {
    ForeignThread,
    ClockUnavailable,
    Credit(CreditError),
    InvalidRecord,
    InvalidLane,
    InvalidDeadline,
    RecordTooLarge,
    TokenExhausted,
    CopyCounterExhausted,
    AllocationFailed,
    BulkLimit,
    Closed,
    TopicClosed,
    PartitionFailed,
    InvalidLease,
}
impl fmt::Display for AdmissionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Credit(e) => e.fmt(f),
            _ => write!(f, "admission failed: {self:?}"),
        }
    }
}
impl std::error::Error for AdmissionError {}
impl From<CreditError> for AdmissionError {
    fn from(e: CreditError) -> Self {
        Self::Credit(e)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Submitted {
    pub accepted: u32,
    pub first_token: Option<RecordToken>,
    pub error: Option<AdmissionError>,
}
impl Submitted {
    #[must_use]
    pub fn token(&self, index: u32) -> Option<RecordToken> {
        if index < self.accepted {
            self.first_token
                .map(|t| RecordToken(t.0 + u64::from(index)))
        } else {
            None
        }
    }
}

/// Descriptor and delivery credits remain attached until batch terminal; input
/// credits move separately so compression can return them before delivery.
#[derive(Debug)]
pub struct AdmittedRecord {
    pub token: RecordToken,
    pub topic: TopicHandle,
    pub partition_hint: Option<i32>,
    pub lane: u8,
    pub user_token: u64,
    pub accepted_at: RuntimeInstant,
    pub deadline: RuntimeInstant,
    pub standalone_encoded_bytes: u32,
    pub record: OwnedRecord,
    pub(crate) input: HeldCredits,
    // Native payload capacity is charged once to its acquisition lane. Every
    // record additionally retains a view until its FIFO consumption, including
    // all-null records that otherwise own no payload spans.
    lease_input: Option<SharedBytes>,
    pub(crate) completion: HeldCredits,
}
impl AdmittedRecord {
    pub(crate) fn belongs_to(&self, authority: &SharedCredits) -> bool {
        self.input.belongs_to(authority) && self.completion.belongs_to(authority)
    }
    /// Relabels per-record input metadata and completion ownership together.
    /// Shared native allocation bytes retain their original acquisition lane.
    /// # Errors
    /// Destination-lane backpressure leaves the record and every guard unchanged.
    pub fn set_lane(&mut self, lane: u8) -> Result<(), CreditError> {
        HeldCredits::transfer_lane_group(&mut [&mut self.input, &mut self.completion], lane)?;
        self.lane = lane;
        Ok(())
    }
    /// Moves the input obligation to the batch's FIFO consumption ledger.
    pub fn into_parts(self) -> (OwnedRecord, RecordObligation) {
        (
            self.record,
            RecordObligation {
                token: self.token,
                topic: self.topic,
                partition_hint: self.partition_hint,
                lane: self.lane,
                user_token: self.user_token,
                accepted_at: self.accepted_at,
                deadline: self.deadline,
                standalone_encoded_bytes: self.standalone_encoded_bytes,
                input: self.input,
                lease_input: self.lease_input,
                completion: self.completion,
            },
        )
    }
}
#[derive(Debug)]
pub struct RecordObligation {
    pub token: RecordToken,
    pub topic: TopicHandle,
    pub partition_hint: Option<i32>,
    pub lane: u8,
    pub user_token: u64,
    pub accepted_at: RuntimeInstant,
    pub deadline: RuntimeInstant,
    pub standalone_encoded_bytes: u32,
    input: HeldCredits,
    lease_input: Option<SharedBytes>,
    completion: HeldCredits,
}
impl RecordObligation {
    /// Called only after the encoder reports this FIFO descriptor consumed.
    pub fn input_consumed(&mut self) {
        self.input.release(Resource::InputBytes);
        self.lease_input = None;
    }
    #[must_use]
    pub fn retained_input_bytes(&self) -> usize {
        self.input.amount(Resource::InputBytes)
    }
    /// Terminal record metadata is replaced by an event, which retains its event
    /// credit until application drain. Descriptor/input credits return here.
    #[must_use]
    pub fn terminal(mut self) -> HeldCredits {
        self.input_consumed();
        self.completion.release(Resource::Descriptors);
        self.completion.take(Resource::DeliveryEvents)
    }
}

#[derive(Debug)]
pub struct SubmissionBatch {
    pub records: Vec<AdmittedRecord>,
    mailbox: HeldCredits,
}

/// Copied under the client publication lock. No owner policy callback runs in
/// admission. Ready built-in keyed routing can be pinned to this identity.
#[derive(Clone, Copy, Debug)]
pub(crate) struct AdmissionRouting {
    pub lane: u8,
    pub ready: Option<(TopicId, u32)>,
    pub builtin: bool,
}
impl AdmissionRouting {
    fn unclassified(lane: u8) -> Self {
        Self {
            lane,
            ready: None,
            builtin: false,
        }
    }
    fn classify(self, hint: Option<i32>, key: Option<&[u8]>) -> (DescriptorClass, Option<i32>) {
        let Some((topic, count)) = self.ready.filter(|(_, count)| *count > 0) else {
            return (DescriptorClass::Unclassified, hint);
        };
        let partition = hint.or_else(|| {
            if self.builtin {
                key.map(|key| ((crate::routing::murmur2(key) & 0x7fff_ffff) % count) as i32)
            } else {
                None
            }
        });
        match partition {
            Some(partition) => (
                DescriptorClass::Partition { topic, partition },
                Some(partition),
            ),
            None => (DescriptorClass::Unclassified, None),
        }
    }
}
impl SubmissionBatch {
    /// Ownership enters the actor; the mailbox slot is now reusable.
    #[must_use]
    pub fn drain(mut self) -> Vec<AdmittedRecord> {
        self.mailbox.release(Resource::Mailbox);
        std::mem::take(&mut self.records)
    }
}

/// Caller-side state lives under the same admission lock as mailbox publication.
/// Routing lane choices and topic validity are supplied from the client snapshot;
/// no policy callback executes inside admission or engine mutation.
#[derive(Debug)]
pub struct Admission {
    credits: SharedCredits,
    next_token: u64,
    closed: bool,
    max_records: u32,
    max_headers: u32,
    max_record_bytes: u32,
    lanes: u8,
    delivery_timeout: RuntimeDuration,
    copied_bytes: u64,
    partition_pressure: bool,
}
impl Admission {
    #[must_use]
    pub fn new(config: &ProducerConfig, credits: SharedCredits, effective_payload: u32) -> Self {
        let partition_pressure =
            credits.descriptor_policy() == DescriptorAdmissionPolicy::PartitionPressure;
        Self {
            partition_pressure,
            credits,
            next_token: 1,
            closed: false,
            max_records: config.max_submission_records,
            max_headers: config.max_header_count,
            max_record_bytes: effective_payload,
            lanes: config.lanes,
            delivery_timeout: config.delivery_timeout,
            copied_bytes: 0,
        }
    }
    #[must_use]
    pub fn copied_bytes(&self) -> u64 {
        self.copied_bytes
    }
    #[must_use]
    pub fn last_token(&self) -> RecordToken {
        RecordToken(self.next_token - 1)
    }
    pub fn close(&mut self) {
        self.closed = true;
    }
    /// The longest valid prefix is charged and copied. `valid` contains the
    /// caller's topic/lane checks in record order; the first failure stops the
    /// prefix. A rejected suffix never acquires input or completion obligations.
    /// The returned batch must be published before releasing the outer client
    /// lock; dropping an unpublished batch cancels its reservations safely.
    pub fn prepare_copy(
        &mut self,
        now: RuntimeInstant,
        records: &[RecordDescriptor<'_>],
        valid: &[Result<u8, AdmissionError>],
    ) -> (Submitted, Option<SubmissionBatch>) {
        self.prepare_copy_inner(
            now,
            records,
            valid
                .iter()
                .map(|lane| lane.map(AdmissionRouting::unclassified)),
        )
    }
    pub(crate) fn prepare_copy_routed(
        &mut self,
        now: RuntimeInstant,
        records: &[RecordDescriptor<'_>],
        valid: &[Result<AdmissionRouting, AdmissionError>],
    ) -> (Submitted, Option<SubmissionBatch>) {
        self.prepare_copy_inner(now, records, valid.iter().copied())
    }
    fn prepare_copy_inner(
        &mut self,
        now: RuntimeInstant,
        records: &[RecordDescriptor<'_>],
        valid: impl ExactSizeIterator<Item = Result<AdmissionRouting, AdmissionError>>,
    ) -> (Submitted, Option<SubmissionBatch>) {
        let empty = |error| {
            (
                Submitted {
                    accepted: 0,
                    first_token: None,
                    error,
                },
                None,
            )
        };
        if self.closed {
            return empty(Some(AdmissionError::Closed));
        }
        if records.len() != valid.len() {
            return empty(Some(AdmissionError::InvalidRecord));
        }
        if records.is_empty() {
            return empty(None);
        }
        let mailbox = match self.credits.reserve(&[Claim {
            resource: Resource::Mailbox,
            amount: 1,
            lane: 0,
        }]) {
            Ok(c) => c,
            Err(e) => return empty(Some(e.into())),
        };
        let count = records.len().min(self.max_records as usize);
        let mut admitted = Vec::new();
        if admitted.try_reserve_exact(count).is_err() {
            return empty(Some(AdmissionError::AllocationFailed));
        }
        let first_token = RecordToken(self.next_token);
        let mut error = None;
        for (record, lane) in records.iter().zip(valid).take(count) {
            let result = (|| {
                let routing = lane?;
                let lane = routing.lane;
                if lane >= self.lanes {
                    return Err(AdmissionError::InvalidLane);
                }
                if record.lane_hint.is_some_and(|hint| hint != lane)
                    || record.partition_hint.is_some_and(|p| p < 0)
                    || record.headers.len() > self.max_headers as usize
                {
                    return Err(AdmissionError::InvalidRecord);
                }
                let timeout = record.delivery_timeout.unwrap_or(self.delivery_timeout);
                if timeout == RuntimeDuration::ZERO || timeout > self.delivery_timeout {
                    return Err(AdmissionError::InvalidDeadline);
                }
                let deadline = now
                    .checked_add(timeout)
                    .ok_or(AdmissionError::InvalidDeadline)?;
                let next = self
                    .next_token
                    .checked_add(1)
                    .ok_or(AdmissionError::TokenExhausted)?;
                let scratch_bytes = record
                    .headers
                    .len()
                    .checked_mul(std::mem::size_of::<kr_kafka_record::Header<'_>>())
                    .ok_or(AdmissionError::RecordTooLarge)?;
                // The borrowed-header preflight is transient physical metadata,
                // and must acquire byte credit before allocating its Vec.
                let _scratch_credit = reserve_input_metadata(&self.credits, scratch_bytes, lane)?;
                let mut headers = Vec::new();
                headers
                    .try_reserve_exact(record.headers.len())
                    .map_err(|_| AdmissionError::AllocationFailed)?;
                for header in record.headers {
                    headers.push(kr_kafka_record::Header {
                        key: header.key,
                        value: header.value,
                    });
                }
                let encoded = kr_kafka_record::encoded_len(
                    kr_kafka_record::Record {
                        timestamp: record.timestamp_ms,
                        key: record.key,
                        value: record.value,
                        headers: &headers,
                    },
                    record.timestamp_ms,
                    0,
                )
                .map_err(|_| AdmissionError::InvalidRecord)?;
                if encoded > self.max_record_bytes {
                    return Err(AdmissionError::RecordTooLarge);
                }
                let mut bytes = record
                    .key
                    .map_or(0, <[u8]>::len)
                    .checked_add(record.value.map_or(0, <[u8]>::len))
                    .ok_or(AdmissionError::RecordTooLarge)?;
                for header in record.headers {
                    bytes = bytes
                        .checked_add(header.key.len())
                        .and_then(|b| b.checked_add(header.value.map_or(0, <[u8]>::len)))
                        .ok_or(AdmissionError::RecordTooLarge)?;
                }
                let retained_bytes = bytes
                    .checked_add(header_metadata_bytes(record.headers.len())?)
                    .ok_or(AdmissionError::RecordTooLarge)?;
                let copied_bytes = self
                    .copied_bytes
                    .checked_add(bytes as u64)
                    .ok_or(AdmissionError::CopyCounterExhausted)?;
                let mut claims = vec![
                    Claim {
                        resource: Resource::Descriptors,
                        amount: 1,
                        lane,
                    },
                    Claim {
                        resource: Resource::DeliveryEvents,
                        amount: 1,
                        lane,
                    },
                ];
                if retained_bytes > 0 {
                    claims.push(Claim {
                        resource: Resource::InputBytes,
                        amount: retained_bytes,
                        lane,
                    });
                }
                let (class, partition_hint) = if self.partition_pressure {
                    routing.classify(record.partition_hint, record.key)
                } else {
                    (DescriptorClass::Unclassified, record.partition_hint)
                };
                let mut completion = self.credits.reserve_class(&claims, class)?;
                let mut slab = Vec::new();
                slab.try_reserve_exact(bytes)
                    .map_err(|_| AdmissionError::AllocationFailed)?;
                if let Some(key) = record.key {
                    slab.extend_from_slice(key);
                }
                if let Some(value) = record.value {
                    slab.extend_from_slice(value);
                }
                for header in record.headers {
                    slab.extend_from_slice(header.key.as_bytes());
                    if let Some(value) = header.value {
                        slab.extend_from_slice(value);
                    }
                }
                let slab = SharedBytes::from(slab);
                let mut offset = 0;
                let mut span = |bytes: Option<&[u8]>| {
                    bytes.map(|bytes| {
                        let start = offset;
                        offset += bytes.len();
                        slab.slice(start..offset)
                            .expect("checked copied input extent")
                    })
                };
                let key = span(record.key);
                let value = span(record.value);
                let mut owned_headers = Vec::new();
                owned_headers
                    .try_reserve_exact(record.headers.len())
                    .map_err(|_| AdmissionError::AllocationFailed)?;
                for header in record.headers {
                    owned_headers.push(OwnedHeader {
                        key: span(Some(header.key.as_bytes())).expect("non-null key"),
                        value: span(header.value),
                    });
                }
                let input = completion.take(Resource::InputBytes);
                let admitted = AdmittedRecord {
                    token: RecordToken(self.next_token),
                    topic: record.topic,
                    partition_hint,
                    lane,
                    user_token: record.user_token,
                    accepted_at: now,
                    deadline,
                    standalone_encoded_bytes: encoded,
                    record: OwnedRecord {
                        timestamp: record.timestamp_ms,
                        key,
                        value,
                        headers: owned_headers,
                    },
                    input,
                    lease_input: None,
                    completion,
                };
                self.next_token = next;
                self.copied_bytes = copied_bytes;
                Ok(admitted)
            })();
            match result {
                Ok(record) => admitted.push(record),
                Err(e) => {
                    error = Some(e);
                    break;
                }
            }
        }
        if error.is_none() && records.len() > count {
            error = Some(AdmissionError::BulkLimit);
        }
        let accepted = admitted.len() as u32;
        if accepted == 0 {
            return empty(error);
        }
        (
            Submitted {
                accepted,
                first_token: Some(first_token),
                error,
            },
            Some(SubmissionBatch {
                records: admitted,
                mailbox,
            }),
        )
    }

    /// Admits a prefix of ranges in one committed native input allocation.
    /// Payload bytes are never copied or charged a second time. The caller's
    /// outer admission lock serializes publication, routing and release; the
    /// registry lookup linearizes this operation against lease release.
    /// A registry from a different admission authority is rejected before any
    /// snapshot, token assignment or reservation, even if lease IDs coincide.
    pub fn prepare_leased(
        &mut self,
        now: RuntimeInstant,
        leases: &InputLeases,
        lease: LeaseId,
        records: &[LeasedRecordDescriptor<'_>],
        valid: &[Result<u8, AdmissionError>],
    ) -> (Submitted, Option<SubmissionBatch>) {
        self.prepare_leased_inner(
            now,
            leases,
            lease,
            records,
            valid
                .iter()
                .map(|lane| lane.map(AdmissionRouting::unclassified)),
        )
    }
    pub(crate) fn prepare_leased_routed(
        &mut self,
        now: RuntimeInstant,
        leases: &InputLeases,
        lease: LeaseId,
        records: &[LeasedRecordDescriptor<'_>],
        valid: &[Result<AdmissionRouting, AdmissionError>],
    ) -> (Submitted, Option<SubmissionBatch>) {
        self.prepare_leased_inner(now, leases, lease, records, valid.iter().copied())
    }
    fn prepare_leased_inner(
        &mut self,
        now: RuntimeInstant,
        leases: &InputLeases,
        lease: LeaseId,
        records: &[LeasedRecordDescriptor<'_>],
        valid: impl ExactSizeIterator<Item = Result<AdmissionRouting, AdmissionError>>,
    ) -> (Submitted, Option<SubmissionBatch>) {
        let empty = |error| {
            (
                Submitted {
                    accepted: 0,
                    first_token: None,
                    error,
                },
                None,
            )
        };
        if self.closed {
            return empty(Some(AdmissionError::Closed));
        }
        if records.len() != valid.len() {
            return empty(Some(AdmissionError::InvalidRecord));
        }
        if records.is_empty() {
            return empty(None);
        }
        if !leases.belongs_to(&self.credits) {
            return empty(Some(CreditError::ForeignCredit.into()));
        }
        let input = match leases.snapshot(lease) {
            Ok(input) => input,
            Err(error) => return empty(Some(input_error(error))),
        };
        let mailbox = match self.credits.reserve(&[Claim {
            resource: Resource::Mailbox,
            amount: 1,
            lane: 0,
        }]) {
            Ok(credit) => credit,
            Err(error) => return empty(Some(error.into())),
        };
        let count = records.len().min(self.max_records as usize);
        let mut admitted = Vec::new();
        if admitted.try_reserve_exact(count).is_err() {
            return empty(Some(AdmissionError::AllocationFailed));
        }
        let first_token = RecordToken(self.next_token);
        let mut error = None;
        for (descriptor, lane) in records.iter().zip(valid).take(count) {
            let result = (|| {
                let routing = lane?;
                let lane = routing.lane;
                if lane >= self.lanes {
                    return Err(AdmissionError::InvalidLane);
                }
                if descriptor.lane_hint.is_some_and(|hint| hint != lane)
                    || descriptor.partition_hint.is_some_and(|p| p < 0)
                    || descriptor.headers.len() > self.max_headers as usize
                {
                    return Err(AdmissionError::InvalidRecord);
                }
                let timeout = descriptor.delivery_timeout.unwrap_or(self.delivery_timeout);
                if timeout == RuntimeDuration::ZERO || timeout > self.delivery_timeout {
                    return Err(AdmissionError::InvalidDeadline);
                }
                let deadline = now
                    .checked_add(timeout)
                    .ok_or(AdmissionError::InvalidDeadline)?;
                let next = self
                    .next_token
                    .checked_add(1)
                    .ok_or(AdmissionError::TokenExhausted)?;
                let key = descriptor
                    .key
                    .clone()
                    .map(|range| checked_slice(&input, range))
                    .transpose()
                    .map_err(input_error)?;
                let value = descriptor
                    .value
                    .clone()
                    .map(|range| checked_slice(&input, range))
                    .transpose()
                    .map_err(input_error)?;
                let metadata_bytes = header_metadata_bytes(descriptor.headers.len())?;
                let mut claims = [
                    Claim {
                        resource: Resource::Descriptors,
                        amount: 1,
                        lane,
                    },
                    Claim {
                        resource: Resource::DeliveryEvents,
                        amount: 1,
                        lane,
                    },
                    Claim {
                        resource: Resource::InputBytes,
                        amount: metadata_bytes,
                        lane,
                    },
                ];
                let claims = if metadata_bytes == 0 {
                    &mut claims[..2]
                } else {
                    &mut claims[..]
                };
                let (class, partition_hint) = if self.partition_pressure {
                    routing.classify(
                        descriptor.partition_hint,
                        key.as_ref().map(SharedBytes::as_slice),
                    )
                } else {
                    (DescriptorClass::Unclassified, descriptor.partition_hint)
                };
                let mut completion = self.credits.reserve_class(claims, class)?;
                let mut headers = Vec::new();
                headers
                    .try_reserve_exact(descriptor.headers.len())
                    .map_err(|_| AdmissionError::AllocationFailed)?;
                for header in descriptor.headers {
                    let key = checked_slice(&input, header.key.clone()).map_err(input_error)?;
                    std::str::from_utf8(key.as_slice())
                        .map_err(|_| AdmissionError::InvalidRecord)?;
                    let value = header
                        .value
                        .clone()
                        .map(|range| checked_slice(&input, range))
                        .transpose()
                        .map_err(input_error)?;
                    headers.push(OwnedHeader { key, value });
                }
                let record = OwnedRecord {
                    timestamp: descriptor.timestamp_ms,
                    key,
                    value,
                    headers,
                };
                let encoded = record
                    .encoded_len(descriptor.timestamp_ms, 0)
                    .map_err(|_| AdmissionError::InvalidRecord)?;
                if encoded > self.max_record_bytes {
                    return Err(AdmissionError::RecordTooLarge);
                }
                let metadata = completion.take(Resource::InputBytes);
                let record = AdmittedRecord {
                    token: RecordToken(self.next_token),
                    topic: descriptor.topic,
                    partition_hint,
                    lane,
                    user_token: descriptor.user_token,
                    accepted_at: now,
                    deadline,
                    standalone_encoded_bytes: encoded,
                    record,
                    input: metadata,
                    lease_input: Some(input.clone()),
                    completion,
                };
                self.next_token = next;
                Ok(record)
            })();
            match result {
                Ok(record) => admitted.push(record),
                Err(failure) => {
                    error = Some(failure);
                    break;
                }
            }
        }
        if error.is_none() && records.len() > count {
            error = Some(AdmissionError::BulkLimit);
        }
        let accepted = admitted.len() as u32;
        if accepted == 0 {
            return empty(error);
        }
        (
            Submitted {
                accepted,
                first_token: Some(first_token),
                error,
            },
            Some(SubmissionBatch {
                records: admitted,
                mailbox,
            }),
        )
    }
}

fn header_metadata_bytes(count: usize) -> Result<usize, AdmissionError> {
    count
        .checked_mul(std::mem::size_of::<OwnedHeader>())
        .ok_or(AdmissionError::RecordTooLarge)
}

fn reserve_input_metadata(
    credits: &SharedCredits,
    bytes: usize,
    lane: u8,
) -> Result<HeldCredits, AdmissionError> {
    let claim = [Claim {
        resource: Resource::InputBytes,
        amount: bytes,
        lane,
    }];
    credits
        .reserve(if bytes == 0 { &[] } else { &claim })
        .map_err(AdmissionError::from)
}

fn input_error(error: InputError) -> AdmissionError {
    match error {
        InputError::Credit(error) => error.into(),
        InputError::Closed => AdmissionError::Closed,
        _ => AdmissionError::InvalidLease,
    }
}

#[cfg(test)]
mod pressure_tests;
#[cfg(test)]
mod tests {
    use super::*;
    fn record<'a>(value: &'a [u8]) -> RecordDescriptor<'a> {
        RecordDescriptor {
            topic: TopicHandle(1),
            partition_hint: None,
            lane_hint: None,
            key: None,
            value: Some(value),
            headers: &[],
            timestamp_ms: 0,
            user_token: 0,
            delivery_timeout: None,
        }
    }
    fn admission(limit: usize) -> (Admission, SharedCredits) {
        let config = ProducerConfig::default();
        let mut limits = config.validate().unwrap().credits;
        limits[Resource::InputBytes as usize] = limit;
        let credits = SharedCredits::new(limits, 1).unwrap();
        (Admission::new(&config, credits.clone(), 1024), credits)
    }
    #[test]
    fn prefix_is_atomic_across_pools_and_rejected_suffix_is_not_copied() {
        let (mut a, c) = admission(8);
        let records = [record(b"12345"), record(b"67890")];
        let (submitted, batch) = a.prepare_copy(RuntimeInstant::ZERO, &records, &[Ok(0), Ok(0)]);
        assert_eq!(submitted.accepted, 1);
        assert_eq!(a.copied_bytes(), 5);
        assert_eq!(a.last_token(), RecordToken(1));
        let status = c.snapshot();
        assert_eq!(status[Resource::Descriptors as usize].held, 1);
        assert_eq!(status[Resource::InputBytes as usize].held, 5);
        assert_eq!(status[Resource::DeliveryEvents as usize].held, 1);
        let record = batch.unwrap().drain().pop().unwrap();
        assert_eq!(c.snapshot()[Resource::Mailbox as usize].held, 0);
        let (_, mut obligation) = record.into_parts();
        obligation.input_consumed();
        assert_eq!(c.snapshot()[Resource::InputBytes as usize].held, 0);
        let event = obligation.terminal();
        assert_eq!(c.snapshot()[Resource::Descriptors as usize].held, 0);
        assert_eq!(c.snapshot()[Resource::DeliveryEvents as usize].held, 1);
        drop(event);
        assert!(c.is_empty());
    }
    #[test]
    fn invalid_record_never_consumes_a_token_or_event() {
        let (mut a, c) = admission(8);
        let mut invalid = record(b"a");
        invalid.delivery_timeout = Some(RuntimeDuration::ZERO);
        for validity in [Ok(0), Err(AdmissionError::TopicClosed)] {
            let (s, b) = a.prepare_copy(RuntimeInstant::ZERO, &[invalid], &[validity]);
            assert_eq!(s.accepted, 0);
            assert!(b.is_none());
            assert!(c.is_empty());
            assert_eq!(a.last_token(), RecordToken(0));
        }
        let (s, b) = a.prepare_copy(RuntimeInstant::ZERO, &[record(b"a")], &[Ok(0)]);
        assert_eq!(s.first_token, Some(RecordToken(1)));
        drop(b);
        assert!(c.is_empty());
    }
    #[test]
    fn every_prefix_boundary_preserves_conservation_and_payload_nullability() {
        for limit in 0..16 {
            let (mut a, c) = admission(limit);
            let records = [record(b"abc"); 8];
            let (s, b) = a.prepare_copy(RuntimeInstant::ZERO, &records, &[Ok(0); 8]);
            assert_eq!(s.accepted as usize, limit / 3);
            if let Some(b) = b {
                for record in &b.records {
                    assert!(record.record.key.is_none());
                    assert_eq!(record.record.value.as_ref().unwrap().as_slice(), b"abc");
                }
                drop(b);
            }
            assert!(c.is_empty());
        }
    }

    #[test]
    fn copy_counter_exhaustion_rejects_before_copy_or_token_advance() {
        let (mut admission, credits) = admission(8);
        admission.copied_bytes = u64::MAX;
        let (submitted, batch) =
            admission.prepare_copy(RuntimeInstant::ZERO, &[record(b"a")], &[Ok(0)]);
        assert_eq!(submitted.error, Some(AdmissionError::CopyCounterExhausted));
        assert_eq!(submitted.accepted, 0);
        assert!(batch.is_none());
        assert_eq!(admission.last_token(), RecordToken(0));
        assert_eq!(admission.copied_bytes(), u64::MAX);
        assert!(credits.is_empty());
    }
}
