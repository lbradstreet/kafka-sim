//! Concatenated, committed magic-2 batches for bounded raw Fetch inspection.
use crate::{BatchDecodeError, BatchDecodeLimits, DecodedBatch, inspect_batch};

type Result<T> = core::result::Result<T, BatchDecodeError>;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RecordSetLimits {
    pub batch: BatchDecodeLimits,
    pub max_wire_bytes: usize,
    pub max_raw_bytes: usize,
    pub max_batches: usize,
    pub max_records: usize,
    pub max_headers: usize,
}
impl Default for RecordSetLimits {
    fn default() -> Self {
        Self {
            batch: BatchDecodeLimits::default(),
            max_wire_bytes: 4 * 1024 * 1024,
            max_raw_bytes: 4 * 1024 * 1024,
            max_batches: 64,
            max_records: 524_288,
            max_headers: 524_288,
        }
    }
}
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct RecordSetStats {
    pub batches: usize,
    pub wire_bytes: usize,
    pub raw_bytes: usize,
    pub records: usize,
    pub headers: usize,
    pub first_offset: Option<i64>,
    pub next_offset: Option<i64>,
}

/// Validates each complete batch before yielding it, including its CRC and all
/// records. Batches must have nonnegative, ordered, nonoverlapping offsets.
/// An empty record set is valid. Gaps between batches are allowed.
///
/// Validation is lazy: callers must consume every result, or call `finish`,
/// before committing a Fetch offset or exposing a response as validated. An
/// error fuses iteration and is retained for `finish`, even if it was observed
/// through `next`. Protocol response tags are validated by the framing codec.
///
/// The iterator owns no batch history. Each yielded zstd batch may retain up to
/// `batch.max_raw_bytes` of decoder output capacity; consumers retaining many
/// decoded batches must account for that capacity separately from the logical
/// aggregate raw-byte limit. Processing and dropping one batch at a time keeps
/// decompression storage bounded by one batch's limit.
pub struct RecordSetIter<'a> {
    bytes: &'a [u8],
    limits: RecordSetLimits,
    stats: RecordSetStats,
    error: Option<BatchDecodeError>,
}
impl<'a> RecordSetIter<'a> {
    pub fn new(bytes: &'a [u8], limits: RecordSetLimits) -> Result<Self> {
        if bytes.len() > limits.max_wire_bytes {
            return Err(BatchDecodeError::Limit("record-set wire bytes"));
        }
        Ok(Self {
            bytes,
            limits,
            stats: RecordSetStats::default(),
            error: None,
        })
    }
    /// Counts only the successfully validated prefix, never a rejected batch.
    pub fn stats(&self) -> RecordSetStats {
        self.stats
    }
    /// Fully validate any unvisited suffix. Never ignores an earlier error.
    pub fn finish(mut self) -> Result<RecordSetStats> {
        for batch in self.by_ref() {
            batch?;
        }
        self.error.map_or(Ok(self.stats), Err)
    }
    fn batch(&mut self) -> Result<DecodedBatch<'a>> {
        if self.stats.batches == self.limits.max_batches {
            return Err(BatchDecodeError::Limit("record-set batches"));
        }
        let length = self.bytes.get(8..12).ok_or(BatchDecodeError::Truncated)?;
        let length = i32::from_be_bytes(length.try_into().unwrap());
        if length < 49 {
            return Err(BatchDecodeError::Length);
        }
        let length = usize::try_from(length)
            .ok()
            .and_then(|length| length.checked_add(12))
            .ok_or(BatchDecodeError::Length)?;
        let bytes = self
            .bytes
            .get(..length)
            .ok_or(BatchDecodeError::Truncated)?;
        let mut limits = self.limits.batch;
        limits.max_raw_bytes = limits
            .max_raw_bytes
            .min(self.limits.max_raw_bytes - self.stats.raw_bytes);
        limits.max_records = limits
            .max_records
            .min(self.limits.max_records - self.stats.records);
        limits.max_headers = limits
            .max_headers
            .min(self.limits.max_headers - self.stats.headers);
        let batch = inspect_batch(bytes, limits)?;
        let base = batch.header.base_offset;
        if base < 0 || self.stats.next_offset.is_some_and(|next| base < next) {
            return Err(BatchDecodeError::Offset);
        }
        let next = base
            .checked_add(i64::from(batch.header.record_count))
            .ok_or(BatchDecodeError::Offset)?;
        self.stats.batches += 1;
        self.stats.wire_bytes += length;
        self.stats.raw_bytes += batch.raw_bytes().len();
        self.stats.records += batch.header.record_count as usize;
        self.stats.headers += batch.header_count();
        self.stats.first_offset.get_or_insert(base);
        self.stats.next_offset = Some(next);
        self.bytes = &self.bytes[length..];
        Ok(batch)
    }
}
impl<'a> Iterator for RecordSetIter<'a> {
    type Item = Result<DecodedBatch<'a>>;
    fn next(&mut self) -> Option<Self::Item> {
        if self.error.is_some() || self.bytes.is_empty() {
            return None;
        }
        let result = self.batch();
        if let Err(error) = result {
            self.error = Some(error);
        }
        Some(result)
    }
}
impl core::iter::FusedIterator for RecordSetIter<'_> {}
