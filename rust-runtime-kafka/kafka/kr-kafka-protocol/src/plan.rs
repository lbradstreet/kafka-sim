//! Checked, transport-independent scatter/gather send plans.
//!
//! Plans keep protocol metadata in one arena and retain record payloads as
//! borrowed slices or reference-counted spans. Building a plan performs no I/O.

use alloc::vec::Vec;
use core::ops::Range;

use crate::wire::{Error, Result};

/// Independent bounds on a single encoded message, including its frame prefix.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct EncodeLimits {
    pub max_bytes: usize,
    pub max_metadata_bytes: usize,
    pub max_segments: usize,
    pub max_array_elements: usize,
    pub max_depth: usize,
    pub max_tags: usize,
}

impl Default for EncodeLimits {
    fn default() -> Self {
        Self {
            max_bytes: 100 * 1024 * 1024,
            max_metadata_bytes: 16 * 1024 * 1024,
            max_segments: 65_536,
            max_array_elements: 1_000_000,
            max_depth: 64,
            max_tags: 65_536,
        }
    }
}

pub use kr_shared_bytes::SharedBytes;

/// Kafka record bytes are opaque to the protocol compiler.
#[derive(Clone, Copy, Debug)]
pub enum Records<'a> {
    Borrowed(&'a [u8]),
    Chunks(&'a [SharedBytes]),
    /// Copies an opaque fixed prefix into the protocol metadata arena, then
    /// retains the remaining payload spans without inspecting or copying them.
    /// A record encoder can use this for its independently mutable batch header.
    HeaderAndChunks {
        header: &'a [u8],
        chunks: &'a [SharedBytes],
    },
}

impl Default for Records<'_> {
    fn default() -> Self {
        Self::Borrowed(&[])
    }
}

impl<'a> Records<'a> {
    /// # Errors
    /// Returns `LengthOverflow` if the total cannot be represented.
    pub fn len(&self) -> Result<usize> {
        match self {
            Self::Borrowed(bytes) => Ok(bytes.len()),
            Self::HeaderAndChunks { header, chunks } => {
                chunks.iter().try_fold(header.len(), |len, chunk| {
                    len.checked_add(chunk.len()).ok_or(Error::LengthOverflow)
                })
            }
            Self::Chunks(chunks) => chunks.iter().try_fold(0usize, |len, chunk| {
                len.checked_add(chunk.len()).ok_or(Error::LengthOverflow)
            }),
        }
    }
    #[must_use]
    pub fn is_empty(&self) -> bool {
        match self {
            Self::Borrowed(bytes) => bytes.is_empty(),
            Self::Chunks(chunks) => chunks.iter().all(SharedBytes::is_empty),
            Self::HeaderAndChunks { header, chunks } => {
                header.is_empty() && chunks.iter().all(SharedBytes::is_empty)
            }
        }
    }
}

#[derive(Clone, Debug)]
enum Segment {
    Metadata(Range<usize>),
    Borrowed(usize),
    Shared(SharedBytes),
}

/// An immutable, fully checked sequence of metadata and external payload spans.
#[derive(Clone, Debug)]
pub struct SendPlan<'a> {
    metadata: Vec<u8>,
    segments: Vec<Segment>,
    borrowed: Vec<&'a [u8]>,
    len: usize,
    pub(crate) elements: usize,
    pub(crate) tags: usize,
    pub(crate) max_depth: usize,
}

impl<'a> SendPlan<'a> {
    pub(crate) fn empty() -> Self {
        Self {
            metadata: Vec::new(),
            segments: Vec::new(),
            borrowed: Vec::new(),
            len: 0,
            elements: 0,
            tags: 0,
            max_depth: 0,
        }
    }

    pub(crate) fn finish_frame(&mut self) -> Result<()> {
        if !matches!(self.segments.first(), Some(Segment::Metadata(range)) if range.start == 0 && range.end >= 4)
            || self.metadata.get(..4) != Some(&[0, 0, 0, 0])
        {
            return Err(Error::InvalidValue("frame must start with write_i32(0)"));
        }
        let len = self.len.checked_sub(4).ok_or(Error::LengthOverflow)?;
        let len = i32::try_from(len).map_err(|_| Error::LengthOverflow)?;
        self.metadata[..4].copy_from_slice(&len.to_be_bytes());
        Ok(())
    }

    #[must_use]
    pub const fn len(&self) -> usize {
        self.len
    }
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.len == 0
    }
    #[must_use]
    pub fn metadata_len(&self) -> usize {
        self.metadata.len()
    }
    /// Actual retained metadata allocation capacity, including unused backing.
    #[must_use]
    pub fn metadata_capacity(&self) -> usize {
        self.metadata.capacity()
    }
    #[must_use]
    pub fn segment_count(&self) -> usize {
        self.segments.len()
    }
    pub(crate) fn starts_with_metadata(&self) -> bool {
        matches!(self.segments.first(), Some(Segment::Metadata(_)))
    }
    pub(crate) fn ends_with_metadata(&self) -> bool {
        matches!(self.segments.last(), Some(Segment::Metadata(_)))
    }
    pub fn segments(&self) -> impl Iterator<Item = &[u8]> {
        self.segments
            .iter()
            .map(|segment| self.segment_bytes(segment))
    }
    pub fn shared_segments(&self) -> impl Iterator<Item = &SharedBytes> {
        self.segments.iter().filter_map(|segment| match segment {
            Segment::Shared(bytes) => Some(bytes),
            _ => None,
        })
    }

    /// Detaches a plan from request descriptors when all payload spans are
    /// reference-counted. Metadata and segment storage move unchanged; this
    /// operation allocates nothing and copies no bytes.
    ///
    /// # Errors
    /// Returns the original intact plan if any nonempty payload is borrowed.
    pub fn try_into_owned(self) -> core::result::Result<SendPlan<'static>, Self> {
        if !self.borrowed.is_empty() {
            return Err(self);
        }
        Ok(SendPlan {
            metadata: self.metadata,
            segments: self.segments,
            borrowed: Vec::new(),
            len: self.len,
            elements: self.elements,
            tags: self.tags,
            max_depth: self.max_depth,
        })
    }

    /// Moves a detached plan into independently owned transport spans. The
    /// metadata arena is converted once to shared storage; external payload
    /// allocations and their lifetime guards are moved without copying bytes.
    /// Metadata subviews retain the complete arena, so transport admission must
    /// account for allocation length rather than just each subview's length.
    ///
    /// # Errors
    /// Rejects borrowed payloads and descriptor allocation failure. Use
    /// `try_into_owned` first when rejection must return the original plan.
    pub fn into_shared_segments(self) -> Result<Vec<SharedBytes>> {
        if !self.borrowed.is_empty() {
            return Err(Error::InvalidValue("transport spans require an owned plan"));
        }
        let mut result = Vec::new();
        result
            .try_reserve_exact(self.segments.len())
            .map_err(|_| Error::AllocationFailed)?;
        let metadata = SharedBytes::from(self.metadata);
        for segment in self.segments {
            match segment {
                Segment::Metadata(range) => {
                    result.push(metadata.slice(range).map_err(|_| Error::InvalidRange)?)
                }
                Segment::Shared(bytes) => result.push(bytes),
                Segment::Borrowed(_) => {
                    return Err(Error::InvalidValue("transport spans require an owned plan"));
                }
            }
        }
        Ok(result)
    }

    /// Explicitly materializes the complete plan, useful for small messages.
    ///
    /// # Errors
    /// Returns `AllocationFailed` when the allocation cannot be reserved.
    pub fn to_vec(&self) -> Result<Vec<u8>> {
        let mut bytes = Vec::new();
        bytes
            .try_reserve_exact(self.len)
            .map_err(|_| Error::AllocationFailed)?;
        for segment in self.segments() {
            bytes.extend_from_slice(segment);
        }
        Ok(bytes)
    }

    #[must_use]
    pub fn cursor(&self) -> SendCursor<'_, 'a> {
        SendCursor {
            plan: self,
            confirmed: 0,
            segment: 0,
            offset: 0,
        }
    }

    fn segment_bytes<'p>(&'p self, segment: &'p Segment) -> &'p [u8] {
        match segment {
            Segment::Metadata(range) => &self.metadata[range.clone()],
            Segment::Borrowed(index) => self.borrowed[*index],
            Segment::Shared(bytes) => bytes.as_slice(),
        }
    }

    fn check_growth(
        &self,
        bytes: usize,
        metadata: usize,
        segments: usize,
        limits: EncodeLimits,
    ) -> Result<()> {
        check_bound(
            self.len.checked_add(bytes),
            limits.max_bytes,
            "encoded bytes",
        )?;
        check_bound(
            self.metadata.len().checked_add(metadata),
            limits.max_metadata_bytes,
            "metadata bytes",
        )?;
        check_bound(
            self.segments.len().checked_add(segments),
            limits.max_segments,
            "send segments",
        )
    }

    fn reserve_metadata(&mut self, additional: usize, limit: usize) -> Result<()> {
        let required = self
            .metadata
            .len()
            .checked_add(additional)
            .ok_or(Error::LengthOverflow)?;
        check_bound(Some(required), limit, "metadata bytes")?;
        if required > self.metadata.capacity() {
            // Retain amortized growth without reserving beyond the caller's
            // metadata budget. Tight bounds still accept their exact last byte.
            let target = required
                .max(self.metadata.capacity().saturating_mul(2))
                .min(limit);
            self.metadata
                .try_reserve_exact(target - self.metadata.len())
                .map_err(|_| Error::AllocationFailed)?;
        }
        Ok(())
    }

    pub(crate) fn metadata(&mut self, bytes: &[u8], limits: EncodeLimits) -> Result<()> {
        if bytes.is_empty() {
            return Ok(());
        }
        let merge = matches!(self.segments.last(), Some(Segment::Metadata(_)));
        self.check_growth(bytes.len(), bytes.len(), usize::from(!merge), limits)?;
        self.reserve_metadata(bytes.len(), limits.max_metadata_bytes)?;
        if !merge {
            self.segments
                .try_reserve(1)
                .map_err(|_| Error::AllocationFailed)?;
        }
        let start = self.metadata.len();
        self.metadata.extend_from_slice(bytes);
        if let Some(Segment::Metadata(range)) = self.segments.last_mut() {
            range.end = self.metadata.len();
        } else {
            self.segments
                .push(Segment::Metadata(start..self.metadata.len()));
        }
        self.len += bytes.len();
        Ok(())
    }

    pub(crate) fn borrowed(&mut self, bytes: &'a [u8], limits: EncodeLimits) -> Result<()> {
        if bytes.is_empty() {
            return Ok(());
        }
        self.check_growth(bytes.len(), 0, 1, limits)?;
        self.segments
            .try_reserve(1)
            .map_err(|_| Error::AllocationFailed)?;
        self.borrowed
            .try_reserve(1)
            .map_err(|_| Error::AllocationFailed)?;
        self.segments.push(Segment::Borrowed(self.borrowed.len()));
        self.borrowed.push(bytes);
        self.len += bytes.len();
        Ok(())
    }

    pub(crate) fn shared(&mut self, bytes: &SharedBytes, limits: EncodeLimits) -> Result<()> {
        if bytes.is_empty() {
            return Ok(());
        }
        self.check_growth(bytes.len(), 0, 1, limits)?;
        self.segments
            .try_reserve(1)
            .map_err(|_| Error::AllocationFailed)?;
        self.segments.push(Segment::Shared(bytes.clone()));
        self.len += bytes.len();
        Ok(())
    }

    pub(crate) fn append(&mut self, other: &Self, limits: EncodeLimits) -> Result<()> {
        // Preflight the whole append before changing the plan. The conservative
        // segment bound also accounts for all fragments before coalescing.
        let merge = matches!(
            (self.segments.last(), other.segments.first()),
            (Some(Segment::Metadata(_)), Some(Segment::Metadata(_)))
        );
        self.check_growth(
            other.len,
            other.metadata.len(),
            other.segments.len().saturating_sub(usize::from(merge)),
            limits,
        )?;
        self.reserve_metadata(other.metadata.len(), limits.max_metadata_bytes)?;
        self.segments
            .try_reserve(other.segments.len())
            .map_err(|_| Error::AllocationFailed)?;
        self.borrowed
            .try_reserve(other.borrowed.len())
            .map_err(|_| Error::AllocationFailed)?;
        for segment in &other.segments {
            match segment {
                Segment::Metadata(range) => {
                    self.metadata(&other.metadata[range.clone()], limits)?
                }
                Segment::Borrowed(index) => self.borrowed(other.borrowed[*index], limits)?,
                Segment::Shared(bytes) => self.shared(bytes, limits)?,
            }
        }
        Ok(())
    }
}

pub(crate) fn check_bound(
    value: Option<usize>,
    limit: usize,
    resource: &'static str,
) -> Result<()> {
    let value = value.ok_or(Error::LengthOverflow)?;
    if value > limit {
        return Err(Error::ResourceExhausted { resource, limit });
    }
    Ok(())
}

/// A cursor advances only after the transport reports a confirmed byte count.
#[derive(Debug)]
pub struct SendCursor<'p, 'a> {
    plan: &'p SendPlan<'a>,
    confirmed: usize,
    segment: usize,
    offset: usize,
}

impl<'p, 'a> SendCursor<'p, 'a> {
    #[must_use]
    pub const fn confirmed(&self) -> usize {
        self.confirmed
    }
    #[must_use]
    pub fn remaining(&self) -> usize {
        self.plan.len - self.confirmed
    }

    /// Builds the next bounded transport submission without advancing.
    ///
    /// # Errors
    /// Returns an error for zero bounds while data remains, or allocation failure.
    pub fn stage(&self, max_bytes: usize, max_segments: usize) -> Result<SendStage<'p, 'a>> {
        if self.remaining() != 0 && (max_bytes == 0 || max_segments == 0) {
            return Err(Error::InvalidStage);
        }
        let mut chunks = Vec::new();
        let mut skip = self.offset;
        let mut len = 0;
        for segment in &self.plan.segments[self.segment..] {
            if len == max_bytes || chunks.len() == max_segments {
                break;
            }
            let segment = self.plan.segment_bytes(segment);
            let bytes = &segment[skip..];
            skip = 0;
            let take = bytes.len().min(max_bytes - len);
            chunks.try_reserve(1).map_err(|_| Error::AllocationFailed)?;
            chunks.push(&bytes[..take]);
            len += take;
        }
        Ok(SendStage {
            plan: self.plan,
            offset: self.confirmed,
            chunks,
            len,
        })
    }

    /// Commits exactly the prefix the transport confirmed. A zero-byte or failed
    /// transfer leaves the cursor unchanged; callers may drop a stage to retry.
    ///
    /// # Errors
    /// Rejects stages from another plan, stale stages, or counts beyond the stage.
    pub fn confirm(&mut self, stage: SendStage<'p, 'a>, bytes: usize) -> Result<()> {
        if !core::ptr::eq(self.plan, stage.plan)
            || self.confirmed != stage.offset
            || bytes > stage.len
        {
            return Err(Error::InvalidStage);
        }
        let confirmed = self
            .confirmed
            .checked_add(bytes)
            .ok_or(Error::LengthOverflow)?;
        let mut segment = self.segment;
        let mut offset = self.offset;
        let mut remaining = bytes;
        while remaining != 0 {
            let available = self.plan.segment_bytes(&self.plan.segments[segment]).len() - offset;
            if remaining < available {
                offset = offset.checked_add(remaining).ok_or(Error::LengthOverflow)?;
                remaining = 0;
            } else {
                remaining -= available;
                segment = segment.checked_add(1).ok_or(Error::LengthOverflow)?;
                offset = 0;
            }
        }
        self.confirmed = confirmed;
        self.segment = segment;
        self.offset = offset;
        Ok(())
    }
}

/// A bounded borrowed submission; dropping it has no effect on its cursor.
#[derive(Debug)]
pub struct SendStage<'p, 'a> {
    plan: &'p SendPlan<'a>,
    offset: usize,
    chunks: Vec<&'p [u8]>,
    len: usize,
}

impl<'p> SendStage<'p, '_> {
    #[must_use]
    pub fn chunks(&self) -> &[&'p [u8]] {
        &self.chunks
    }
    #[must_use]
    pub const fn len(&self) -> usize {
        self.len
    }
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.len == 0
    }
}
