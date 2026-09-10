//! Bounded metadata conversion. Copy payload borrows last only until admission;
//! leased payload addresses become checked offsets without dereferencing C data.
use crate::{
    abi::{KR_ERR_CLOSED, KR_ERR_EXHAUSTED, KR_ERR_INVALID, KrProducer},
    memory,
    types::*,
};
use kr_kafka_producer::{
    admission::AdmissionError,
    credit::HeldCredits,
    input::{LeasedHeader, LeasedRecordDescriptor},
    types::{Header, LeaseId, RecordDescriptor, TopicHandle},
};
use kr_runtime::RuntimeDuration;
use std::{mem::size_of, ops::Range};
struct Scratch<T> {
    values: Vec<T>,
    _credit: [HeldCredits; 2],
}
impl<T> Scratch<T> {
    fn new(producer: &KrProducer, count: usize) -> Result<Self, i32> {
        let bytes = count.checked_mul(size_of::<T>()).ok_or(KR_ERR_EXHAUSTED)?;
        let credit = producer.scratch(bytes)?;
        let mut values = Vec::new();
        values
            .try_reserve_exact(count)
            .map_err(|_| KR_ERR_EXHAUSTED)?;
        let actual = values
            .capacity()
            .checked_mul(size_of::<T>())
            .ok_or(KR_ERR_EXHAUSTED)?;
        let slack = producer.scratch(actual.checked_sub(bytes).ok_or(KR_ERR_EXHAUSTED)?)?;
        Ok(Self {
            values,
            _credit: [credit, slack],
        })
    }
}
struct Raw {
    records: Scratch<KrRecord>,
    headers: usize,
}
#[derive(Clone, Copy)]
enum InputPath {
    Copy,
    Leased,
}
impl InputPath {
    fn metadata(self, records: usize, headers: usize) -> Option<usize> {
        let (record, header) = match self {
            Self::Copy => (size_of::<RecordDescriptor<'_>>(), size_of::<Header<'_>>()),
            Self::Leased => (
                size_of::<LeasedRecordDescriptor<'_>>(),
                size_of::<LeasedHeader>(),
            ),
        };
        records
            .checked_mul(size_of::<KrRecord>() + size_of::<Range<usize>>() + record)?
            .checked_add(headers.checked_mul(header)?)
    }
    fn payload(self, key: KrSpan, value: KrSpan) -> Option<usize> {
        match self {
            Self::Copy => (key.len as usize).checked_add(value.len as usize),
            Self::Leased => Some(0),
        }
    }
}
unsafe fn snapshot(
    producer: &KrProducer,
    pointer: *const KrRecord,
    count: u32,
    path: InputPath,
) -> Result<Raw, i32> {
    let requested = count as usize;
    let count = requested
        .min(producer.config.max_submission_records as usize)
        .min(4096);
    if count < requested {
        producer.error(KR_ERR_EXHAUSTED);
    }
    memory::check(pointer, count)?;
    // Discover the feasible prefix before allocating metadata. A large rejected
    // suffix must not consume the byte budget needed by an earlier valid record.
    // Each probe includes all FFI vectors plus permanent input and the maximum
    // transient encoder-header preflight at that point in sequential admission.
    let mut accepted = 0usize;
    let mut headers = 0usize;
    let mut retained = 0usize;
    let mut admission_peak = 0usize;
    for index in 0..count {
        // SAFETY: caller supplies this versioned descriptor array for the call.
        let record = match unsafe { memory::versioned(pointer.wrapping_add(index)) } {
            Ok(record) => record,
            Err(error) => {
                producer.error(error);
                break;
            }
        };
        if record.header_count > producer.config.max_header_count
            || record.partition_hint < -1
            || record.lane_hint < -1
            || record.lane_hint >= i32::from(producer.config.lanes)
            || record.key_is_null > 1
            || record.value_is_null > 1
            || (record.key_is_null == 1 && record.key.len != 0)
            || (record.value_is_null == 1 && record.value.len != 0)
        {
            producer.error(KR_ERR_INVALID);
            break;
        }
        let candidate = (|| {
            let count = record.header_count as usize;
            let all_headers = headers.checked_add(count).ok_or(KR_ERR_EXHAUSTED)?;
            let metadata = path
                .metadata(accepted + 1, all_headers)
                .ok_or(KR_ERR_EXHAUSTED)?;
            let permanent = count
                .checked_mul(size_of::<kr_kafka_record::OwnedHeader>())
                .ok_or(KR_ERR_EXHAUSTED)?;
            let transient = count
                .checked_mul(size_of::<kr_kafka_record::Header<'_>>())
                .ok_or(KR_ERR_EXHAUSTED)?;
            let mut next_retained = retained
                .checked_add(permanent)
                .and_then(|n| n.checked_add(path.payload(record.key, record.value)?))
                .ok_or(KR_ERR_EXHAUSTED)?;
            let peak = admission_peak.max(
                next_retained
                    .checked_add(transient)
                    .ok_or(KR_ERR_EXHAUSTED)?,
            );
            // Check the header-count bound before touching its potentially large
            // array, then refine the payload requirement without allocating it.
            drop(producer.scratch(metadata.checked_add(peak).ok_or(KR_ERR_EXHAUSTED)?)?);
            memory::check(record.headers, count)?;
            for index in 0..count {
                // SAFETY: caller keeps all versioned headers immutable for the
                // call; preflight reads lengths and flags, never payload bytes.
                let header = unsafe { memory::versioned(record.headers.wrapping_add(index)) }?;
                if header.value_is_null > 1 || (header.value_is_null == 1 && header.value.len != 0)
                {
                    return Err(KR_ERR_INVALID);
                }
                next_retained = next_retained
                    .checked_add(
                        path.payload(header.key, header.value)
                            .ok_or(KR_ERR_EXHAUSTED)?,
                    )
                    .ok_or(KR_ERR_EXHAUSTED)?;
            }
            let peak = admission_peak.max(
                next_retained
                    .checked_add(transient)
                    .ok_or(KR_ERR_EXHAUSTED)?,
            );
            drop(producer.scratch(metadata.checked_add(peak).ok_or(KR_ERR_EXHAUSTED)?)?);
            Ok((all_headers, next_retained, peak))
        })();
        match candidate {
            Ok((all_headers, next_retained, peak)) => {
                accepted += 1;
                headers = all_headers;
                retained = next_retained;
                admission_peak = peak;
            }
            Err(error) => {
                producer.error(error);
                break;
            }
        }
    }
    let mut records = Scratch::new(producer, accepted)?;
    for index in 0..accepted {
        // SAFETY: the accepted metadata prefix was validated above and remains
        // immutable throughout the entire ABI call, including this second read.
        records
            .values
            .push(unsafe { memory::versioned(pointer.wrapping_add(index)) }?);
    }
    Ok(Raw { records, headers })
}
unsafe fn nullable<'a>(span: KrSpan, is_null: u32, limit: usize) -> Result<Option<&'a [u8]>, i32> {
    match is_null {
        1 if span.len == 0 => Ok(None),
        0 => {
            // SAFETY: caller keeps this immutable input alive through copy admission.
            Ok(Some(unsafe { memory::span(span, limit) }?))
        }
        _ => Err(KR_ERR_INVALID),
    }
}
fn range(span: KrSpan, is_null: u32, base: usize, used: u32) -> Result<Option<Range<u32>>, i32> {
    if is_null == 1 && span.len == 0 {
        return Ok(None);
    }
    if is_null != 0 {
        return Err(KR_ERR_INVALID);
    }
    if span.len == 0 && span.ptr.is_null() {
        return Ok(Some(0..0));
    }
    let offset = (span.ptr as usize)
        .checked_sub(base)
        .ok_or(KR_ERR_INVALID)?;
    let start = u32::try_from(offset).map_err(|_| KR_ERR_INVALID)?;
    let end = start
        .checked_add(span.len)
        .filter(|end| *end <= used)
        .ok_or(KR_ERR_INVALID)?;
    Ok(Some(start..end))
}
unsafe fn copy_headers<'a>(
    record: &KrRecord,
    headers: &mut Vec<Header<'a>>,
    limit: usize,
) -> Result<(), i32> {
    memory::check(record.headers, record.header_count as usize)?;
    for index in 0..record.header_count as usize {
        // SAFETY: the header array has this many initialized versioned elements.
        let header = unsafe { memory::versioned(record.headers.wrapping_add(index)) }?;
        // SAFETY: header key storage is immutable/readable for the ABI call.
        let key = std::str::from_utf8(unsafe { memory::span(header.key, limit) }?)
            .map_err(|_| KR_ERR_INVALID)?;
        // SAFETY: nullable value follows the same bounded input validity contract.
        let value = unsafe { nullable(header.value, header.value_is_null, limit) }?;
        headers.push(Header { key, value });
    }
    Ok(())
}
pub(crate) unsafe fn copy(
    producer: &KrProducer,
    pointer: *const KrRecord,
    count: u32,
) -> Result<u32, i32> {
    // SAFETY: forwarded versioned-array contract; only bounded metadata is copied.
    let raw = unsafe { snapshot(producer, pointer, count, InputPath::Copy) }?;
    let mut headers = Scratch::new(producer, raw.headers)?;
    let mut ranges = Scratch::new(producer, raw.records.values.len())?;
    let mut descriptors = Scratch::new(producer, raw.records.values.len())?;
    let limit = producer.config.batch_hard_bytes as usize;
    for record in &raw.records.values {
        let start = headers.values.len();
        // SAFETY: copied raw pointers still address the immutable caller input.
        if let Err(error) = unsafe { copy_headers(record, &mut headers.values, limit) } {
            headers.values.truncate(start);
            producer.error(error);
            break;
        }
        ranges.values.push(start..headers.values.len());
    }
    for (record, headers_range) in raw.records.values.iter().zip(&ranges.values) {
        // SAFETY: spans remain valid until the ensuing client copy returns.
        let key = match unsafe { nullable(record.key, record.key_is_null, limit) } {
            Ok(value) => value,
            Err(error) => {
                producer.error(error);
                break;
            }
        };
        // SAFETY: same caller validity window for the nullable value.
        let value = match unsafe { nullable(record.value, record.value_is_null, limit) } {
            Ok(value) => value,
            Err(error) => {
                producer.error(error);
                break;
            }
        };
        descriptors.values.push(RecordDescriptor {
            topic: TopicHandle(record.topic),
            partition_hint: (record.partition_hint >= 0).then_some(record.partition_hint),
            lane_hint: (record.lane_hint >= 0).then_some(record.lane_hint as u8),
            key,
            value,
            headers: &headers.values[headers_range.clone()],
            timestamp_ms: record.timestamp_ms,
            user_token: record.user_token,
            delivery_timeout: timeout(record.delivery_timeout_ns),
        });
    }
    let submitted = producer.client.submit_copy(&descriptors.values);
    if let Some(error) = submitted.error {
        producer.error(admission_error(error));
    }
    Ok(submitted.accepted)
}
unsafe fn leased_headers(
    record: &KrRecord,
    headers: &mut Vec<LeasedHeader>,
    base: usize,
    used: u32,
) -> Result<(), i32> {
    memory::check(record.headers, record.header_count as usize)?;
    for index in 0..record.header_count as usize {
        // SAFETY: only caller-owned header descriptors are copied; payload is not read.
        let header = unsafe { memory::versioned(record.headers.wrapping_add(index)) }?;
        let key = range(header.key, 0, base, used)?.ok_or(KR_ERR_INVALID)?;
        let value = range(header.value, header.value_is_null, base, used)?;
        headers.push(LeasedHeader { key, value });
    }
    Ok(())
}
pub(crate) unsafe fn leased(
    producer: &KrProducer,
    lease: LeaseId,
    pointer: *const KrRecord,
    count: u32,
) -> Result<u32, i32> {
    let (base, used) = {
        let slots = producer.leases.lock().unwrap_or_else(|p| p.into_inner());
        let lease = slots
            .iter()
            .flatten()
            .find(|entry| entry.id == lease && entry.writable.is_none())
            .ok_or(KR_ERR_INVALID)?;
        (lease.base, lease.used)
    };
    // SAFETY: caller-owned metadata is valid for this bounded conversion.
    let raw = unsafe { snapshot(producer, pointer, count, InputPath::Leased) }?;
    let mut headers = Scratch::new(producer, raw.headers)?;
    let mut ranges = Scratch::new(producer, raw.records.values.len())?;
    let mut descriptors = Scratch::new(producer, raw.records.values.len())?;
    for record in &raw.records.values {
        let start = headers.values.len();
        // SAFETY: header metadata is readable; native payload addresses are checked offsets.
        if let Err(error) = unsafe { leased_headers(record, &mut headers.values, base, used) } {
            headers.values.truncate(start);
            producer.error(error);
            break;
        }
        ranges.values.push(start..headers.values.len());
    }
    for (record, headers_range) in raw.records.values.iter().zip(&ranges.values) {
        let key = match range(record.key, record.key_is_null, base, used) {
            Ok(value) => value,
            Err(error) => {
                producer.error(error);
                break;
            }
        };
        let value = match range(record.value, record.value_is_null, base, used) {
            Ok(value) => value,
            Err(error) => {
                producer.error(error);
                break;
            }
        };
        descriptors.values.push(LeasedRecordDescriptor {
            topic: TopicHandle(record.topic),
            partition_hint: (record.partition_hint >= 0).then_some(record.partition_hint),
            lane_hint: (record.lane_hint >= 0).then_some(record.lane_hint as u8),
            key,
            value,
            headers: &headers.values[headers_range.clone()],
            timestamp_ms: record.timestamp_ms,
            user_token: record.user_token,
            delivery_timeout: timeout(record.delivery_timeout_ns),
        });
    }
    let submitted = producer.client.submit_leased(lease, &descriptors.values);
    if let Some(error) = submitted.error {
        producer.error(admission_error(error));
    }
    Ok(submitted.accepted)
}
fn timeout(nanos: u64) -> Option<RuntimeDuration> {
    if nanos == 0 {
        None
    } else {
        Some(RuntimeDuration::from_nanos(nanos))
    }
}
fn admission_error(error: AdmissionError) -> i32 {
    match error {
        AdmissionError::Credit(_) | AdmissionError::AllocationFailed => KR_ERR_EXHAUSTED,
        AdmissionError::Closed | AdmissionError::TopicClosed => KR_ERR_CLOSED,
        _ => KR_ERR_INVALID,
    }
}
