//! Bounded inspection of the producer's nontransactional magic-2 batch format.
use crate::{BATCH_HEADER_BYTES, Identity, crc32c};
use alloc::borrow::Cow;
#[cfg(feature = "zstd")]
use alloc::vec::Vec;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct BatchDecodeLimits {
    pub max_wire_bytes: usize,
    pub max_raw_bytes: usize,
    pub max_records: usize,
    pub max_headers: usize,
    pub max_field_bytes: usize,
    pub max_zstd_window_log: u32,
}
impl Default for BatchDecodeLimits {
    fn default() -> Self {
        Self {
            max_wire_bytes: 1024 * 1024 + 61,
            max_raw_bytes: 1024 * 1024,
            max_records: 131_072,
            max_headers: 131_072,
            max_field_bytes: 1024 * 1024,
            max_zstd_window_log: 23,
        }
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BatchDecodeError {
    Truncated,
    Length,
    Magic,
    Attributes,
    Checksum,
    Identity,
    Count,
    Varint,
    Timestamp,
    Offset,
    Utf8,
    Limit(&'static str),
    Compression,
    TrailingBytes,
    Allocation,
}
impl core::fmt::Display for BatchDecodeError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(f, "{self:?}")
    }
}
impl core::error::Error for BatchDecodeError {}
type Result<T> = core::result::Result<T, BatchDecodeError>;
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct BatchHeader {
    pub base_offset: i64,
    pub leader_epoch: i32,
    pub attributes: i16,
    pub last_offset_delta: i32,
    pub base_timestamp: i64,
    pub max_timestamp: i64,
    pub identity: Identity,
    pub record_count: i32,
}
/// Uncompressed input is borrowed. Zstd allocates at most `max_raw_bytes`, with
/// its decompressor window independently capped before parsing the frame.
#[derive(Debug)]
pub struct DecodedBatch<'a> {
    pub header: BatchHeader,
    payload: Cow<'a, [u8]>,
    limits: BatchDecodeLimits,
    header_count: usize,
}
impl DecodedBatch<'_> {
    pub fn records(&self) -> RecordIter<'_> {
        RecordIter {
            bytes: &self.payload,
            remaining: self.header.record_count as usize,
            index: 0,
            base: self.header.base_timestamp,
            limits: self.limits,
        }
    }
    pub fn raw_bytes(&self) -> &[u8] {
        &self.payload
    }
    /// Validated aggregate header count across all records in this batch.
    pub fn header_count(&self) -> usize {
        self.header_count
    }
}
#[derive(Clone, Copy, Debug)]
pub struct DecodedRecord<'a> {
    pub timestamp: i64,
    pub offset_delta: i32,
    pub key: Option<&'a [u8]>,
    pub value: Option<&'a [u8]>,
    pub headers: HeaderIter<'a>,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DecodedHeader<'a> {
    pub key: &'a str,
    pub value: Option<&'a [u8]>,
}
#[derive(Clone, Copy, Debug)]
pub struct HeaderIter<'a> {
    bytes: &'a [u8],
    remaining: usize,
    limit: usize,
}
impl<'a> Iterator for HeaderIter<'a> {
    type Item = Result<DecodedHeader<'a>>;
    fn next(&mut self) -> Option<Self::Item> {
        if self.remaining == 0 {
            return None;
        }
        self.remaining -= 1;
        let mut r = Cursor { bytes: self.bytes };
        let value = (|| {
            let key = r.optional(self.limit)?.ok_or(BatchDecodeError::Length)?;
            let key = core::str::from_utf8(key).map_err(|_| BatchDecodeError::Utf8)?;
            let value = r.optional(self.limit)?;
            Ok(DecodedHeader { key, value })
        })();
        self.bytes = r.bytes;
        Some(value)
    }
    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.remaining, Some(self.remaining))
    }
}
impl ExactSizeIterator for HeaderIter<'_> {}
pub struct RecordIter<'a> {
    bytes: &'a [u8],
    remaining: usize,
    index: i32,
    base: i64,
    limits: BatchDecodeLimits,
}
impl<'a> Iterator for RecordIter<'a> {
    type Item = Result<DecodedRecord<'a>>;
    fn next(&mut self) -> Option<Self::Item> {
        if self.remaining == 0 {
            return None;
        }
        self.remaining -= 1;
        let result = read_record(&mut self.bytes, self.base, self.index, self.limits);
        self.index += 1;
        Some(result)
    }
    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.remaining, Some(self.remaining))
    }
}
impl ExactSizeIterator for RecordIter<'_> {}

/// Validates exactly one batch, its checksum, every record/header and all
/// aggregate counts before exposing any record. Concatenated batches, zstd
/// frames, truncated streams and oversized expansion fail closed.
pub fn inspect_batch(bytes: &[u8], limits: BatchDecodeLimits) -> Result<DecodedBatch<'_>> {
    if bytes.len() > limits.max_wire_bytes {
        return Err(BatchDecodeError::Limit("wire bytes"));
    }
    if bytes.len() < BATCH_HEADER_BYTES {
        return Err(BatchDecodeError::Truncated);
    }
    let mut r = Cursor { bytes };
    let base_offset = r.i64()?;
    let length = r.i32()?;
    if length < 49 || usize::try_from(length).ok() != Some(bytes.len() - 12) {
        return Err(BatchDecodeError::Length);
    }
    let leader_epoch = r.i32()?;
    if r.take(1)?[0] != 2 {
        return Err(BatchDecodeError::Magic);
    }
    let crc = u32::from_be_bytes(r.take(4)?.try_into().unwrap());
    if crc32c(&bytes[21..]) != crc {
        return Err(BatchDecodeError::Checksum);
    }
    let attributes = r.i16()?;
    if attributes != 0 && attributes != 4 {
        return Err(BatchDecodeError::Attributes);
    }
    let last_offset_delta = r.i32()?;
    let base_timestamp = r.i64()?;
    let max_timestamp = r.i64()?;
    let identity = Identity {
        producer_id: r.i64()?,
        producer_epoch: r.i16()?,
        base_sequence: r.i32()?,
    };
    if identity.producer_id < 0 || identity.producer_epoch < 0 || identity.base_sequence < 0 {
        return Err(BatchDecodeError::Identity);
    }
    let record_count = r.i32()?;
    if record_count <= 0 || last_offset_delta != record_count - 1 {
        return Err(BatchDecodeError::Count);
    }
    if record_count as usize > limits.max_records {
        return Err(BatchDecodeError::Limit("records"));
    }
    let payload = if attributes == 0 {
        if r.bytes.len() > limits.max_raw_bytes {
            return Err(BatchDecodeError::Limit("raw bytes"));
        }
        Cow::Borrowed(r.bytes)
    } else {
        decompress(r.bytes, limits)?
    };
    let header = BatchHeader {
        base_offset,
        leader_epoch,
        attributes,
        last_offset_delta,
        base_timestamp,
        max_timestamp,
        identity,
        record_count,
    };
    let mut batch = DecodedBatch {
        header,
        payload,
        limits,
        header_count: 0,
    };
    let mut records = batch.records();
    let mut headers = 0usize;
    let mut maximum = i64::MIN;
    for record in &mut records {
        let record = record?;
        maximum = maximum.max(record.timestamp);
        headers = headers
            .checked_add(record.headers.len())
            .ok_or(BatchDecodeError::Count)?;
        if headers > limits.max_headers {
            return Err(BatchDecodeError::Limit("headers"));
        }
    }
    if !records.bytes.is_empty() {
        return Err(BatchDecodeError::TrailingBytes);
    }
    if maximum != max_timestamp {
        return Err(BatchDecodeError::Timestamp);
    }
    batch.header_count = headers;
    Ok(batch)
}
#[cfg(feature = "zstd")]
fn decompress(bytes: &[u8], limits: BatchDecodeLimits) -> Result<Cow<'_, [u8]>> {
    if !(10..=31).contains(&limits.max_zstd_window_log) {
        return Err(BatchDecodeError::Limit("zstd window log"));
    }
    if zstd_safe::find_frame_compressed_size(bytes).map_err(|_| BatchDecodeError::Compression)?
        != bytes.len()
    {
        return Err(BatchDecodeError::TrailingBytes);
    }
    if let Some(size) =
        zstd_safe::get_frame_content_size(bytes).map_err(|_| BatchDecodeError::Compression)?
        && size > limits.max_raw_bytes as u64
    {
        return Err(BatchDecodeError::Limit("raw bytes"));
    }
    let mut output = Vec::new();
    output
        .try_reserve_exact(limits.max_raw_bytes)
        .map_err(|_| BatchDecodeError::Allocation)?;
    output.resize(limits.max_raw_bytes, 0);
    let mut decoder = zstd_safe::DCtx::try_create().ok_or(BatchDecodeError::Allocation)?;
    decoder
        .set_parameter(zstd_safe::DParameter::WindowLogMax(
            limits.max_zstd_window_log,
        ))
        .map_err(|_| BatchDecodeError::Compression)?;
    // Use the streaming API: the one-shot API does not apply the window limit.
    let mut input = zstd_safe::InBuffer::around(bytes);
    let mut sink = zstd_safe::OutBuffer::around(&mut output[..]);
    loop {
        let before = (input.pos, sink.pos());
        let remaining = decoder
            .decompress_stream(&mut sink, &mut input)
            .map_err(|_| BatchDecodeError::Compression)?;
        if remaining == 0 {
            if input.pos != bytes.len() {
                return Err(BatchDecodeError::TrailingBytes);
            }
            break;
        }
        if sink.pos() == limits.max_raw_bytes {
            return Err(BatchDecodeError::Limit("raw bytes"));
        }
        if before == (input.pos, sink.pos()) {
            return Err(BatchDecodeError::Truncated);
        }
    }
    let length = sink.pos();
    output.truncate(length);
    Ok(Cow::Owned(output))
}
#[cfg(not(feature = "zstd"))]
fn decompress(_bytes: &[u8], _limits: BatchDecodeLimits) -> Result<Cow<'_, [u8]>> {
    Err(BatchDecodeError::Compression)
}

fn read_record<'a>(
    bytes: &mut &'a [u8],
    base: i64,
    index: i32,
    limits: BatchDecodeLimits,
) -> Result<DecodedRecord<'a>> {
    let mut r = Cursor { bytes };
    let length = r.var32()?;
    if length < 0 {
        return Err(BatchDecodeError::Length);
    }
    let mut body = Cursor {
        bytes: r.take(length as usize)?,
    };
    *bytes = r.bytes;
    if body.take(1)?[0] != 0 {
        return Err(BatchDecodeError::Attributes);
    }
    let timestamp = base
        .checked_add(body.var64()?)
        .ok_or(BatchDecodeError::Timestamp)?;
    let offset_delta = body.var32()?;
    if offset_delta != index {
        return Err(BatchDecodeError::Offset);
    }
    let key = body.optional(limits.max_field_bytes)?;
    let value = body.optional(limits.max_field_bytes)?;
    let count = body.var32()?;
    if count < 0 {
        return Err(BatchDecodeError::Count);
    }
    if count as usize > limits.max_headers {
        return Err(BatchDecodeError::Limit("headers"));
    }
    let headers = HeaderIter {
        bytes: body.bytes,
        remaining: count as usize,
        limit: limits.max_field_bytes,
    };
    for _ in 0..count {
        let key = body
            .optional(limits.max_field_bytes)?
            .ok_or(BatchDecodeError::Length)?;
        core::str::from_utf8(key).map_err(|_| BatchDecodeError::Utf8)?;
        body.optional(limits.max_field_bytes)?;
    }
    if !body.bytes.is_empty() {
        return Err(BatchDecodeError::TrailingBytes);
    }
    Ok(DecodedRecord {
        timestamp,
        offset_delta,
        key,
        value,
        headers,
    })
}
struct Cursor<'a> {
    bytes: &'a [u8],
}
impl<'a> Cursor<'a> {
    fn take(&mut self, n: usize) -> Result<&'a [u8]> {
        if n > self.bytes.len() {
            return Err(BatchDecodeError::Truncated);
        }
        let (out, rest) = self.bytes.split_at(n);
        self.bytes = rest;
        Ok(out)
    }
    fn i16(&mut self) -> Result<i16> {
        Ok(i16::from_be_bytes(self.take(2)?.try_into().unwrap()))
    }
    fn i32(&mut self) -> Result<i32> {
        Ok(i32::from_be_bytes(self.take(4)?.try_into().unwrap()))
    }
    fn i64(&mut self) -> Result<i64> {
        Ok(i64::from_be_bytes(self.take(8)?.try_into().unwrap()))
    }
    fn var64(&mut self) -> Result<i64> {
        let mut bits = 0u64;
        for i in 0..10 {
            let byte = self.take(1)?[0];
            if i == 9 && byte > 1 {
                return Err(BatchDecodeError::Varint);
            }
            bits |= u64::from(byte & 127) << (i * 7);
            if byte & 128 == 0 {
                if i > 0 && byte == 0 {
                    return Err(BatchDecodeError::Varint);
                }
                return Ok(((bits >> 1) as i64) ^ -((bits & 1) as i64));
            }
        }
        Err(BatchDecodeError::Varint)
    }
    fn var32(&mut self) -> Result<i32> {
        i32::try_from(self.var64()?).map_err(|_| BatchDecodeError::Varint)
    }
    fn optional(&mut self, limit: usize) -> Result<Option<&'a [u8]>> {
        let length = self.var32()?;
        if length == -1 {
            return Ok(None);
        }
        if length < 0 {
            return Err(BatchDecodeError::Length);
        }
        if length as usize > limit {
            return Err(BatchDecodeError::Limit("field bytes"));
        }
        Ok(Some(self.take(length as usize)?))
    }
}
