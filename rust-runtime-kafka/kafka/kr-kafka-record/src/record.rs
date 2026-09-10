use crate::{Error, Result, SharedBytes};
use alloc::vec::Vec;

#[derive(Clone, Copy, Debug)]
pub struct Header<'a> {
    pub key: &'a str,
    pub value: Option<&'a [u8]>,
}
#[derive(Clone, Copy, Debug)]
pub struct Record<'a> {
    pub timestamp: i64,
    pub key: Option<&'a [u8]>,
    pub value: Option<&'a [u8]>,
    pub headers: &'a [Header<'a>],
}
#[derive(Clone, Debug)]
pub struct OwnedHeader {
    pub key: SharedBytes,
    pub value: Option<SharedBytes>,
}
#[derive(Clone, Debug)]
pub struct OwnedRecord {
    pub timestamp: i64,
    pub key: Option<SharedBytes>,
    pub value: Option<SharedBytes>,
    pub headers: Vec<OwnedHeader>,
}
impl OwnedRecord {
    /// Exact encoded byte count without consuming the descriptor or payload.
    /// Use this to preflight admission into a batch before calling `push`.
    pub fn encoded_len(&self, base_timestamp: i64, offset: i32) -> Result<u32> {
        self.size(base_timestamp, offset).map(|(size, _)| size)
    }

    /// Copies payloads for callers without retained input spans. Streaming callers
    /// construct `OwnedRecord` directly and incur no payload copies here.
    pub fn copy_from(record: Record<'_>) -> Self {
        Self {
            timestamp: record.timestamp,
            key: record.key.map(shared),
            value: record.value.map(shared),
            headers: record
                .headers
                .iter()
                .map(|h| OwnedHeader {
                    key: shared(h.key.as_bytes()),
                    value: h.value.map(shared),
                })
                .collect(),
        }
    }
    pub(crate) fn size(&self, base_timestamp: i64, offset: i32) -> Result<(u32, i64)> {
        let delta = self
            .timestamp
            .checked_sub(base_timestamp)
            .ok_or(Error::LengthOverflow)?;
        for header in &self.headers {
            core::str::from_utf8(header.key.as_slice()).map_err(|_| Error::InvalidConfig)?;
        }
        let size = size_fields(
            delta,
            offset,
            self.key.as_ref().map(SharedBytes::len),
            self.value.as_ref().map(SharedBytes::len),
            self.headers.len(),
            self.headers
                .iter()
                .map(|h| (h.key.len(), h.value.as_ref().map(SharedBytes::len))),
        )?;
        Ok((size, delta))
    }
}
fn shared(bytes: &[u8]) -> SharedBytes {
    SharedBytes::from(alloc::sync::Arc::<[u8]>::from(bytes))
}

/// Full encoded record length (including its signed zigzag length prefix).
/// Payload contents are never scanned; only descriptor lengths are visited.
pub fn encoded_len(record: Record<'_>, base_timestamp: i64, offset: i32) -> Result<u32> {
    let delta = record
        .timestamp
        .checked_sub(base_timestamp)
        .ok_or(Error::LengthOverflow)?;
    size_fields(
        delta,
        offset,
        record.key.map(<[u8]>::len),
        record.value.map(<[u8]>::len),
        record.headers.len(),
        record
            .headers
            .iter()
            .map(|h| (h.key.len(), h.value.map(<[u8]>::len))),
    )
}
fn size_fields(
    delta: i64,
    offset: i32,
    key: Option<usize>,
    value: Option<usize>,
    headers: usize,
    fields: impl Iterator<Item = (usize, Option<usize>)>,
) -> Result<u32> {
    if offset < 0 {
        return Err(Error::LengthOverflow);
    }
    let mut size = 1u64 + varlen(delta) as u64 + varlen(i64::from(offset)) as u64;
    size += nullable_size(key)? + nullable_size(value)?;
    size += varlen(i64::from(
        i32::try_from(headers).map_err(|_| Error::LengthOverflow)?,
    )) as u64;
    for (key, value) in fields {
        size += nullable_size(Some(key))? + nullable_size(value)?;
    }
    let body = i32::try_from(size).map_err(|_| Error::LengthOverflow)?;
    u32::try_from(size + varlen(i64::from(body)) as u64).map_err(|_| Error::LengthOverflow)
}
fn nullable_size(length: Option<usize>) -> Result<u64> {
    let n = length.map_or(Ok(-1), |n| {
        i32::try_from(n).map_err(|_| Error::LengthOverflow)
    })?;
    Ok(varlen(i64::from(n)) as u64 + length.unwrap_or(0) as u64)
}
pub(crate) fn varlen(value: i64) -> usize {
    let zig = ((value as u64) << 1) ^ ((value >> 63) as u64);
    ((64 - zig.leading_zeros()).max(1) as usize).div_ceil(7)
}
pub(crate) fn varint(value: i64, out: &mut [u8]) -> usize {
    let mut zig = ((value as u64) << 1) ^ ((value >> 63) as u64);
    let mut n = 0;
    while zig >= 128 {
        out[n] = (zig as u8) | 128;
        zig >>= 7;
        n += 1;
    }
    out[n] = zig as u8;
    n + 1
}

// A cursor refers to one descriptor and produces a prefix in a fixed scratch or
// borrows the next payload span. Even a multi-megabyte value is consumed under
// the caller's byte quota, with no uncompressed aggregate image.
#[derive(Debug)]
pub(crate) struct RecordCursor {
    stage: u8,
    header: usize,
    pub position: usize,
    body: i64,
    delta: i64,
    offset: i32,
}
impl RecordCursor {
    pub(crate) fn new(total: u32, delta: i64, offset: i32) -> Self {
        // Find the unique body length such that body + zigzag_width(body) = total.
        let total = i64::from(total);
        let body = (1..=5)
            .map(|n| total - n)
            .find(|n| *n >= 0 && *n + varlen(*n) as i64 == total)
            .expect("validated record size");
        Self {
            stage: 0,
            header: 0,
            position: 0,
            body,
            delta,
            offset,
        }
    }
    pub(crate) fn segment<'a>(
        &self,
        record: &'a OwnedRecord,
        scratch: &'a mut [u8; 64],
    ) -> Option<&'a [u8]> {
        let prefix = |value, scratch: &'a mut [u8; 64]| {
            let n = varint(value, scratch);
            &scratch[..n]
        };
        match self.stage {
            0 => {
                let mut n = varint(self.body, scratch);
                scratch[n] = 0;
                n += 1;
                n += varint(self.delta, &mut scratch[n..]);
                n += varint(i64::from(self.offset), &mut scratch[n..]);
                n += varint(
                    record.key.as_ref().map_or(-1, |b| b.len() as i64),
                    &mut scratch[n..],
                );
                Some(&scratch[..n])
            }
            1 => Some(record.key.as_ref().map_or(&[], SharedBytes::as_slice)),
            2 => Some(prefix(
                record.value.as_ref().map_or(-1, |b| b.len() as i64),
                scratch,
            )),
            3 => Some(record.value.as_ref().map_or(&[], SharedBytes::as_slice)),
            4 => Some(prefix(record.headers.len() as i64, scratch)),
            5 => record
                .headers
                .get(self.header)
                .map(|h| prefix(h.key.len() as i64, scratch)),
            6 => Some(record.headers[self.header].key.as_slice()),
            7 => Some(prefix(
                record.headers[self.header]
                    .value
                    .as_ref()
                    .map_or(-1, |b| b.len() as i64),
                scratch,
            )),
            8 => Some(
                record.headers[self.header]
                    .value
                    .as_ref()
                    .map_or(&[], SharedBytes::as_slice),
            ),
            _ => unreachable!(),
        }
    }
    pub(crate) fn advance_segment(&mut self) {
        self.position = 0;
        if self.stage == 8 {
            self.stage = 5;
            self.header += 1;
        } else {
            self.stage += 1;
        }
    }
}

/// Castagnoli CRC-32C, compatible with Kafka's CRC over attributes..end.
pub fn crc32c(bytes: &[u8]) -> u32 {
    !crc_update(!0, bytes)
}
pub(crate) fn crc_update(mut crc: u32, bytes: &[u8]) -> u32 {
    for &byte in bytes {
        crc ^= u32::from(byte);
        for _ in 0..8 {
            crc = (crc >> 1) ^ (0x82f6_3b78 & 0u32.wrapping_sub(crc & 1));
        }
    }
    crc
}
