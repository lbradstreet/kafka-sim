//! Safe Kafka primitives and allocation-free validated borrowed message views.
//!
//! Readers validate canonical encodings and enforce aggregate element and tag
//! budgets. Writers are fallible builders: discard a writer after any error.

use core::{fmt, marker::PhantomData, str};

pub use crate::plan::{EncodeLimits, Records};
use crate::plan::{SendPlan, check_bound};

pub type Result<T> = core::result::Result<T, Error>;

#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum Error {
    UnexpectedEof {
        needed: usize,
        remaining: usize,
    },
    TrailingBytes {
        remaining: usize,
    },
    FrameLengthMismatch {
        declared: usize,
        actual: usize,
    },
    CorrelationMismatch {
        expected: i32,
        actual: i32,
    },
    InvalidLength {
        value: i64,
    },
    NullNotAllowed,
    InvalidUtf8,
    InvalidBoolean {
        value: u8,
    },
    InvalidVarint,
    InvalidTagOrder {
        previous: u32,
        tag: u32,
    },
    InvalidValue(&'static str),
    UnsupportedVersion {
        api_key: i16,
        version: i16,
    },
    ResourceExhausted {
        resource: &'static str,
        limit: usize,
    },
    LengthOverflow,
    AllocationFailed,
    InvalidRange,
    InvalidStage,
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnexpectedEof { needed, remaining } => {
                write!(f, "need {needed} bytes, have {remaining}")
            }
            Self::TrailingBytes { remaining } => write!(f, "{remaining} trailing bytes"),
            Self::FrameLengthMismatch { declared, actual } => {
                write!(f, "frame declares {declared} bytes, has {actual}")
            }
            Self::CorrelationMismatch { expected, actual } => {
                write!(f, "expected correlation {expected}, got {actual}")
            }
            Self::InvalidLength { value } => write!(f, "invalid Kafka length {value}"),
            Self::NullNotAllowed => f.write_str("null is not allowed for this field"),
            Self::InvalidUtf8 => f.write_str("invalid UTF-8 string"),
            Self::InvalidBoolean { value } => write!(f, "invalid boolean byte {value}"),
            Self::InvalidVarint => {
                f.write_str("overflowing, truncated, or noncanonical unsigned varint")
            }
            Self::InvalidTagOrder { previous, tag } => {
                write!(f, "tag {tag} does not follow tag {previous}")
            }
            Self::InvalidValue(reason) => f.write_str(reason),
            Self::UnsupportedVersion { api_key, version } => {
                write!(f, "unsupported version {version} for API {api_key}")
            }
            Self::ResourceExhausted { resource, limit } => {
                write!(f, "{resource} exceeds limit {limit}")
            }
            Self::LengthOverflow => f.write_str("length cannot be represented"),
            Self::AllocationFailed => f.write_str("allocation failed"),
            Self::InvalidRange => f.write_str("range is outside the byte view"),
            Self::InvalidStage => f.write_str("invalid or stale transport stage"),
        }
    }
}

impl core::error::Error for Error {}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DecodeLimits {
    pub max_bytes: usize,
    pub max_array_elements: usize,
    pub max_depth: usize,
    pub max_tags: usize,
}

impl Default for DecodeLimits {
    fn default() -> Self {
        Self {
            max_bytes: 100 * 1024 * 1024,
            max_array_elements: 1_000_000,
            max_depth: 64,
            max_tags: 65_536,
        }
    }
}

/// Decoder state. All views borrow the original immutable input.
#[derive(Clone, Debug)]
pub struct Reader<'a> {
    bytes: &'a [u8],
    position: usize,
    version: i16,
    flexible: bool,
    limits: DecodeLimits,
    elements: usize,
    tags: usize,
    depth: usize,
}

impl<'a> Reader<'a> {
    /// # Errors
    /// Rejects inputs larger than the configured message bound.
    pub fn new(
        bytes: &'a [u8],
        version: i16,
        flexible: bool,
        limits: DecodeLimits,
    ) -> Result<Self> {
        check_bound(Some(bytes.len()), limits.max_bytes, "decoded bytes")?;
        Ok(Self {
            bytes,
            position: 0,
            version,
            flexible,
            limits,
            elements: 0,
            tags: 0,
            depth: 0,
        })
    }

    #[must_use]
    pub const fn version(&self) -> i16 {
        self.version
    }
    #[must_use]
    pub const fn flexible(&self) -> bool {
        self.flexible
    }
    #[must_use]
    pub const fn position(&self) -> usize {
        self.position
    }
    #[must_use]
    pub fn remaining(&self) -> usize {
        self.bytes.len() - self.position
    }

    /// # Errors
    /// Rejects trailing bytes after decoding an exact message or tag payload.
    pub fn finish(&self) -> Result<()> {
        if self.remaining() != 0 {
            return Err(Error::TrailingBytes {
                remaining: self.remaining(),
            });
        }
        Ok(())
    }

    /// Temporarily overrides compact length encoding for a schema field.
    ///
    /// # Errors
    /// Propagates the field decoder's error and restores the prior mode.
    pub fn with_flexible<T>(
        &mut self,
        flexible: bool,
        f: impl FnOnce(&mut Self) -> Result<T>,
    ) -> Result<T> {
        let old = self.flexible;
        self.flexible = flexible;
        let result = f(self);
        self.flexible = old;
        result
    }

    /// Enters a bounded nested structure.
    ///
    /// # Errors
    /// Rejects excessive nesting or propagates the decoder's error.
    pub fn nested<T>(&mut self, f: impl FnOnce(&mut Self) -> Result<T>) -> Result<T> {
        check_bound(
            self.depth.checked_add(1),
            self.limits.max_depth,
            "decode depth",
        )?;
        self.depth += 1;
        let result = f(self);
        self.depth -= 1;
        result
    }

    /// # Errors
    /// Rejects excessive structure nesting or propagates the decoder's error.
    pub fn with_struct<T>(&mut self, f: impl FnOnce(&mut Self) -> Result<T>) -> Result<T> {
        self.nested(f)
    }

    /// Decodes an exact bounded tag payload with the parent's aggregate budget.
    ///
    /// # Errors
    /// Rejects excessive nesting, malformed payloads, or unconsumed payload bytes.
    pub fn with_subreader<T>(
        &mut self,
        bytes: &'a [u8],
        f: impl FnOnce(&mut Self) -> Result<T>,
    ) -> Result<T> {
        check_bound(
            self.depth.checked_add(1),
            self.limits.max_depth,
            "decode depth",
        )?;
        check_bound(Some(bytes.len()), self.limits.max_bytes, "decoded bytes")?;
        let mut child = self.clone();
        child.bytes = bytes;
        child.position = 0;
        child.depth += 1;
        let result = f(&mut child);
        self.elements = child.elements;
        self.tags = child.tags;
        let value = result?;
        child.finish()?;
        Ok(value)
    }

    /// # Errors
    /// Returns `UnexpectedEof` without advancing if fewer than `len` bytes remain.
    pub fn take(&mut self, len: usize) -> Result<&'a [u8]> {
        if len > self.remaining() {
            return Err(Error::UnexpectedEof {
                needed: len,
                remaining: self.remaining(),
            });
        }
        let result = &self.bytes[self.position..self.position + len];
        self.position += len;
        Ok(result)
    }

    /// # Errors
    /// Returns `UnexpectedEof` if the primitive is truncated.
    pub fn read_i8(&mut self) -> Result<i8> {
        Ok(self.take(1)?[0] as i8)
    }
    /// # Errors
    /// Returns `UnexpectedEof` if the primitive is truncated.
    pub fn read_u8(&mut self) -> Result<u8> {
        Ok(self.take(1)?[0])
    }
    /// # Errors
    /// Returns `UnexpectedEof` if the primitive is truncated.
    pub fn read_i16(&mut self) -> Result<i16> {
        Ok(i16::from_be_bytes(self.fixed()?))
    }
    /// # Errors
    /// Returns `UnexpectedEof` if the primitive is truncated.
    pub fn read_u16(&mut self) -> Result<u16> {
        Ok(u16::from_be_bytes(self.fixed()?))
    }
    /// # Errors
    /// Returns `UnexpectedEof` if the primitive is truncated.
    pub fn read_i32(&mut self) -> Result<i32> {
        Ok(i32::from_be_bytes(self.fixed()?))
    }
    /// # Errors
    /// Returns `UnexpectedEof` if the primitive is truncated.
    pub fn read_u32(&mut self) -> Result<u32> {
        Ok(u32::from_be_bytes(self.fixed()?))
    }
    /// # Errors
    /// Returns `UnexpectedEof` if the primitive is truncated.
    pub fn read_i64(&mut self) -> Result<i64> {
        Ok(i64::from_be_bytes(self.fixed()?))
    }
    /// # Errors
    /// Returns `UnexpectedEof` if the primitive is truncated.
    pub fn read_f64(&mut self) -> Result<f64> {
        Ok(f64::from_be_bytes(self.fixed()?))
    }
    /// # Errors
    /// Rejects truncated values and bytes other than zero or one.
    pub fn read_bool(&mut self) -> Result<bool> {
        match self.read_u8()? {
            0 => Ok(false),
            1 => Ok(true),
            value => Err(Error::InvalidBoolean { value }),
        }
    }
    /// # Errors
    /// Returns `UnexpectedEof` if the UUID is truncated.
    pub fn read_uuid(&mut self) -> Result<[u8; 16]> {
        self.fixed()
    }

    fn fixed<const N: usize>(&mut self) -> Result<[u8; N]> {
        let mut result = [0; N];
        result.copy_from_slice(self.take(N)?);
        Ok(result)
    }

    /// # Errors
    /// Rejects truncation, overflow, and nonminimal unsigned varints.
    pub fn read_uvarint(&mut self) -> Result<u32> {
        let mut value = 0;
        for index in 0..5 {
            let byte = self.read_u8()?;
            if index == 4 && byte > 0x0f {
                return Err(Error::InvalidVarint);
            }
            value |= u32::from(byte & 0x7f) << (index * 7);
            if byte & 0x80 == 0 {
                if index > 0 && byte == 0 {
                    return Err(Error::InvalidVarint);
                }
                return Ok(value);
            }
        }
        Err(Error::InvalidVarint)
    }

    fn length(&mut self, string: bool, nullable: bool) -> Result<Option<usize>> {
        let length = if self.flexible {
            i64::from(self.read_uvarint()?) - 1
        } else if string {
            i64::from(self.read_i16()?)
        } else {
            i64::from(self.read_i32()?)
        };
        // Compact encoding changes the length prefix, not Kafka's signed
        // STRING/BYTES/ARRAY domain. Java applies the same bounds in both modes.
        let maximum = if string {
            i64::from(i16::MAX)
        } else {
            i64::from(i32::MAX)
        };
        if length > maximum {
            return Err(Error::InvalidLength { value: length });
        }
        match length {
            -1 if nullable => Ok(None),
            -1 => Err(Error::NullNotAllowed),
            0.. => Ok(Some(
                usize::try_from(length).map_err(|_| Error::LengthOverflow)?,
            )),
            value => Err(Error::InvalidLength { value }),
        }
    }

    /// # Errors
    /// Rejects invalid lengths, null, truncation, and invalid UTF-8.
    pub fn read_string(&mut self) -> Result<&'a str> {
        self.read_string_inner(false)?.ok_or(Error::NullNotAllowed)
    }
    /// # Errors
    /// Rejects invalid lengths, truncation, and invalid UTF-8.
    pub fn read_nullable_string(&mut self) -> Result<Option<&'a str>> {
        self.read_string_inner(true)
    }
    fn read_string_inner(&mut self, nullable: bool) -> Result<Option<&'a str>> {
        self.length(true, nullable)?
            .map(|len| str::from_utf8(self.take(len)?).map_err(|_| Error::InvalidUtf8))
            .transpose()
    }

    /// # Errors
    /// Rejects invalid lengths, null, and truncation.
    pub fn read_bytes(&mut self) -> Result<&'a [u8]> {
        self.read_bytes_inner(false)?.ok_or(Error::NullNotAllowed)
    }
    /// # Errors
    /// Rejects invalid lengths and truncation.
    pub fn read_nullable_bytes(&mut self) -> Result<Option<&'a [u8]>> {
        self.read_bytes_inner(true)
    }
    fn read_bytes_inner(&mut self, nullable: bool) -> Result<Option<&'a [u8]>> {
        self.length(false, nullable)?
            .map(|len| self.take(len))
            .transpose()
    }
    /// # Errors
    /// Rejects invalid lengths, null, and truncation; record payloads stay opaque.
    pub fn read_records(&mut self) -> Result<Records<'a>> {
        self.read_bytes().map(Records::Borrowed)
    }
    /// # Errors
    /// Rejects invalid lengths and truncation; record payloads stay opaque.
    pub fn read_nullable_records(&mut self) -> Result<Option<Records<'a>>> {
        Ok(self.read_nullable_bytes()?.map(Records::Borrowed))
    }

    fn charge_elements(&mut self, len: usize) -> Result<()> {
        check_bound(
            self.elements.checked_add(len),
            self.limits.max_array_elements,
            "decoded array elements",
        )?;
        self.elements += len;
        Ok(())
    }
    fn charge_tags(&mut self, len: usize) -> Result<()> {
        check_bound(
            self.tags.checked_add(len),
            self.limits.max_tags,
            "decoded tags",
        )?;
        self.tags += len;
        Ok(())
    }
}

/// Generated codecs and primitive array elements share this small contract.
pub trait Wire<'a>: Sized {
    /// # Errors
    /// Rejects malformed values and exhausted decode limits.
    fn read(reader: &mut Reader<'a>) -> Result<Self>;
    /// # Errors
    /// Rejects invalid values, unrepresentable lengths, and exhausted encode limits.
    fn write(&self, writer: &mut Writer<'a>) -> Result<()>;
    fn is_default(&self) -> bool;
}

macro_rules! primitive_wire {
    ($ty:ty, $read:ident, $write:ident, $default:expr) => {
        impl<'a> Wire<'a> for $ty {
            fn read(reader: &mut Reader<'a>) -> Result<Self> {
                reader.$read()
            }
            fn write(&self, writer: &mut Writer<'a>) -> Result<()> {
                writer.$write(*self)
            }
            fn is_default(&self) -> bool {
                *self == $default
            }
        }
    };
}
primitive_wire!(i8, read_i8, write_i8, 0);
primitive_wire!(u8, read_u8, write_u8, 0);
primitive_wire!(i16, read_i16, write_i16, 0);
primitive_wire!(u16, read_u16, write_u16, 0);
primitive_wire!(i32, read_i32, write_i32, 0);
primitive_wire!(u32, read_u32, write_u32, 0);
primitive_wire!(i64, read_i64, write_i64, 0);
primitive_wire!(f64, read_f64, write_f64, 0.0);
primitive_wire!(bool, read_bool, write_bool, false);
primitive_wire!([u8; 16], read_uuid, write_uuid, [0; 16]);

impl<'a> Wire<'a> for &'a str {
    fn read(reader: &mut Reader<'a>) -> Result<Self> {
        reader.read_string()
    }
    fn write(&self, writer: &mut Writer<'a>) -> Result<()> {
        writer.write_string(self)
    }
    fn is_default(&self) -> bool {
        self.is_empty()
    }
}
impl<'a> Wire<'a> for &'a [u8] {
    fn read(reader: &mut Reader<'a>) -> Result<Self> {
        reader.read_bytes()
    }
    fn write(&self, writer: &mut Writer<'a>) -> Result<()> {
        writer.write_bytes(self)
    }
    fn is_default(&self) -> bool {
        self.is_empty()
    }
}
impl<'a> Wire<'a> for Records<'a> {
    fn read(reader: &mut Reader<'a>) -> Result<Self> {
        reader.read_records()
    }
    fn write(&self, writer: &mut Writer<'a>) -> Result<()> {
        writer.write_records(self)
    }
    fn is_default(&self) -> bool {
        self.is_empty()
    }
}

#[derive(Clone, Debug)]
enum SequenceSource<'a, T> {
    Slice(&'a [T]),
    Encoded { reader: Reader<'a>, count: usize },
}

/// An array builder borrows a slice; a decoded array borrows a validated wire
/// region. Decoding validates every element once without allocating a vector.
#[derive(Clone, Debug)]
pub struct Sequence<'a, T> {
    source: SequenceSource<'a, T>,
}

impl<T> Default for Sequence<'_, T> {
    fn default() -> Self {
        Self {
            source: SequenceSource::Slice(&[]),
        }
    }
}

impl<'a, T> Sequence<'a, T> {
    #[must_use]
    pub const fn new(values: &'a [T]) -> Self {
        Self {
            source: SequenceSource::Slice(values),
        }
    }
    #[must_use]
    pub const fn from_slice(values: &'a [T]) -> Self {
        Self::new(values)
    }
    #[must_use]
    pub fn len(&self) -> usize {
        match &self.source {
            SequenceSource::Slice(values) => values.len(),
            SequenceSource::Encoded { count, .. } => *count,
        }
    }
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl<'a, T> From<&'a [T]> for Sequence<'a, T> {
    fn from(value: &'a [T]) -> Self {
        Self::new(value)
    }
}

impl<'a, T: Wire<'a> + Clone> Sequence<'a, T> {
    /// # Errors
    /// Rejects null, malformed elements, or exhausted aggregate limits.
    pub fn read(reader: &mut Reader<'a>) -> Result<Self> {
        Self::read_inner(reader, false)?.ok_or(Error::NullNotAllowed)
    }
    /// # Errors
    /// Rejects malformed elements or exhausted aggregate limits.
    pub fn read_nullable(reader: &mut Reader<'a>) -> Result<Option<Self>> {
        Self::read_inner(reader, true)
    }

    fn read_inner(reader: &mut Reader<'a>, nullable: bool) -> Result<Option<Self>> {
        let Some(count) = reader.length(false, nullable)? else {
            return Ok(None);
        };
        reader.charge_elements(count)?;
        let mut snapshot = reader.clone();
        let start = reader.position;
        reader.nested(|reader| {
            for _ in 0..count {
                T::read(reader)?;
            }
            Ok(())
        })?;
        snapshot.bytes = &reader.bytes[start..reader.position];
        snapshot.position = 0;
        snapshot.depth += 1;
        Ok(Some(Self {
            source: SequenceSource::Encoded {
                reader: snapshot,
                count,
            },
        }))
    }

    #[must_use]
    pub fn iter(&self) -> SequenceIter<'_, 'a, T> {
        match &self.source {
            SequenceSource::Slice(values) => SequenceIter {
                source: IterSource::Slice(values.iter()),
            },
            SequenceSource::Encoded { reader, count } => SequenceIter {
                source: IterSource::Encoded {
                    reader: reader.clone(),
                    remaining: *count,
                    marker: PhantomData,
                },
            },
        }
    }

    /// # Errors
    /// Rejects invalid values or exhausted encode limits.
    pub fn write(&self, writer: &mut Writer<'a>) -> Result<()> {
        writer.charge_elements(self.len())?;
        writer.length(Some(self.len()), false)?;
        writer.nested(|writer| {
            for value in self.iter() {
                value?.write(writer)?;
            }
            Ok(())
        })
    }

    /// # Errors
    /// Rejects invalid values or exhausted encode limits.
    pub fn write_nullable(value: Option<&Self>, writer: &mut Writer<'a>) -> Result<()> {
        match value {
            Some(values) => values.write(writer),
            None => writer.length(None, false),
        }
    }
}

impl<'a, T: Wire<'a> + Clone> Wire<'a> for Sequence<'a, T> {
    fn read(reader: &mut Reader<'a>) -> Result<Self> {
        Self::read(reader)
    }
    fn write(&self, writer: &mut Writer<'a>) -> Result<()> {
        self.write(writer)
    }
    fn is_default(&self) -> bool {
        self.is_empty()
    }
}

enum IterSource<'s, 'a, T> {
    Slice(core::slice::Iter<'s, T>),
    Encoded {
        reader: Reader<'a>,
        remaining: usize,
        marker: PhantomData<T>,
    },
}

pub struct SequenceIter<'s, 'a, T> {
    source: IterSource<'s, 'a, T>,
}

impl<'a, T: Wire<'a> + Clone> Iterator for SequenceIter<'_, 'a, T> {
    type Item = Result<T>;
    fn next(&mut self) -> Option<Self::Item> {
        match &mut self.source {
            IterSource::Slice(values) => values.next().cloned().map(Ok),
            IterSource::Encoded {
                reader, remaining, ..
            } => {
                if *remaining == 0 {
                    return None;
                }
                *remaining -= 1;
                let result = T::read(reader);
                if result.is_err() {
                    *remaining = 0;
                }
                Some(result)
            }
        }
    }
    fn size_hint(&self) -> (usize, Option<usize>) {
        let len = match &self.source {
            IterSource::Slice(values) => values.len(),
            IterSource::Encoded { remaining, .. } => *remaining,
        };
        // User-written Wire implementations may fail even after validation.
        (usize::from(len != 0), Some(len))
    }
}

/// One validated flexible-version extension payload.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Tag<'a> {
    pub id: u32,
    pub payload: &'a [u8],
}

/// A validated borrowed tag block with generated known IDs filtered out.
#[derive(Clone, Copy, Debug)]
pub struct TaggedFields<'a> {
    bytes: &'a [u8],
    excluded: &'static [u32],
}

impl Default for TaggedFields<'_> {
    fn default() -> Self {
        Self {
            bytes: &[0],
            excluded: &[],
        }
    }
}

impl<'a> TaggedFields<'a> {
    /// # Errors
    /// Rejects truncation, duplicate/unsorted tags, and exhausted tag limits.
    pub fn read(reader: &mut Reader<'a>) -> Result<Self> {
        let start = reader.position;
        let count = reader.read_uvarint()? as usize;
        reader.charge_tags(count)?;
        let mut previous = None;
        for _ in 0..count {
            let tag = reader.read_uvarint()?;
            if let Some(previous) = previous
                && tag <= previous
            {
                return Err(Error::InvalidTagOrder { previous, tag });
            }
            previous = Some(tag);
            let len = reader.read_uvarint()? as usize;
            reader.take(len)?;
        }
        Ok(Self {
            bytes: &reader.bytes[start..reader.position],
            excluded: &[],
        })
    }

    #[must_use]
    pub fn excluding(mut self, known_ids: &'static [u32]) -> Self {
        self.excluded = known_ids;
        self
    }
    #[must_use]
    pub fn iter(&self) -> TagIter<'a> {
        TagIter {
            bytes: self.bytes,
            position: 0,
            remaining: None,
            excluded: self.excluded,
        }
    }
    #[must_use]
    pub fn len(&self) -> usize {
        self.iter().count()
    }
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.iter().next().is_none()
    }
}

pub struct TagIter<'a> {
    bytes: &'a [u8],
    position: usize,
    remaining: Option<u32>,
    excluded: &'static [u32],
}

impl TagIter<'_> {
    fn varint(&mut self) -> u32 {
        // The only constructor is TaggedFields::iter, after complete validation.
        let mut value = 0;
        for shift in (0..35).step_by(7) {
            let byte = self.bytes[self.position];
            self.position += 1;
            value |= u32::from(byte & 0x7f) << shift;
            if byte & 0x80 == 0 {
                break;
            }
        }
        value
    }
}

impl<'a> Iterator for TagIter<'a> {
    type Item = Tag<'a>;
    fn next(&mut self) -> Option<Self::Item> {
        let mut remaining = match self.remaining {
            Some(value) => value,
            None => self.varint(),
        };
        while remaining != 0 {
            remaining -= 1;
            self.remaining = Some(remaining);
            let id = self.varint();
            let len = self.varint() as usize;
            let payload = &self.bytes[self.position..self.position + len];
            self.position += len;
            if !self.excluded.contains(&id) {
                return Some(Tag { id, payload });
            }
        }
        self.remaining = Some(0);
        None
    }
}

/// A checked in-memory send-plan builder. After a failed write, discard it.
#[derive(Debug)]
pub struct Writer<'a> {
    plan: SendPlan<'a>,
    version: i16,
    flexible: bool,
    limits: EncodeLimits,
    elements: usize,
    tags: usize,
    depth: usize,
    max_depth: usize,
}

impl<'a> Writer<'a> {
    #[must_use]
    pub fn new(version: i16, flexible: bool, limits: EncodeLimits) -> Self {
        Self {
            plan: SendPlan::empty(),
            version,
            flexible,
            limits,
            elements: 0,
            tags: 0,
            depth: 0,
            max_depth: 0,
        }
    }
    #[must_use]
    pub const fn version(&self) -> i16 {
        self.version
    }
    #[must_use]
    pub const fn flexible(&self) -> bool {
        self.flexible
    }
    #[must_use]
    pub const fn limits(&self) -> EncodeLimits {
        self.limits
    }
    #[must_use]
    pub fn len(&self) -> usize {
        self.plan.len()
    }
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.plan.is_empty()
    }

    /// # Errors
    /// Reserved for future final validations; all writes are checked eagerly.
    pub fn finish(mut self) -> Result<SendPlan<'a>> {
        self.plan.elements = self.elements;
        self.plan.tags = self.tags;
        self.plan.max_depth = self.max_depth;
        Ok(self.plan)
    }

    /// Creates a bounded encoder for one known tag payload. Its resource usage
    /// is charged to this writer when the resulting plan is merged.
    #[must_use]
    pub fn child(&self) -> Self {
        let mut limits = self.limits;
        limits.max_array_elements = limits.max_array_elements.saturating_sub(self.elements);
        limits.max_tags = limits.max_tags.saturating_sub(self.tags);
        limits.max_depth = limits
            .max_depth
            .saturating_sub(self.depth)
            .saturating_sub(1);
        Self::new(self.version, self.flexible, limits)
    }

    /// Creates a known-tag payload writer after already encoded payloads.
    /// Parent and child arenas together cannot exceed the configured byte and
    /// metadata budgets. Tag envelope bytes are checked exactly on final merge;
    /// the temporary and final arenas together use at most twice that budget.
    ///
    /// # Errors
    /// Rejects exhausted aggregate byte, metadata, segment, element, tag, and
    /// nesting limits before another child arena can be allocated.
    pub fn child_after(&self, known: &[(u32, SendPlan<'a>)]) -> Result<Self> {
        let mut bytes = self.plan.len();
        let mut metadata = self.plan.metadata_len();
        let mut elements = self.elements;
        let mut tags = self
            .tags
            .checked_add(known.len())
            .and_then(|n| n.checked_add(1))
            .ok_or(Error::LengthOverflow)?;
        // The tag-count prefix is metadata. Consecutive metadata segments
        // coalesce, including the first segment of every generated child.
        let mut segments = self.plan.segment_count();
        if !self.plan.ends_with_metadata() {
            segments = segments.checked_add(1).ok_or(Error::LengthOverflow)?;
        }
        let mut ends_metadata = true;
        for (_, payload) in known {
            bytes = bytes
                .checked_add(payload.len())
                .ok_or(Error::LengthOverflow)?;
            metadata = metadata
                .checked_add(payload.metadata_len())
                .ok_or(Error::LengthOverflow)?;
            elements = elements
                .checked_add(payload.elements)
                .ok_or(Error::LengthOverflow)?;
            tags = tags
                .checked_add(payload.tags)
                .ok_or(Error::LengthOverflow)?;
            if !ends_metadata {
                segments = segments.checked_add(1).ok_or(Error::LengthOverflow)?;
            }
            ends_metadata = true;
            segments = segments
                .checked_add(payload.segment_count())
                .ok_or(Error::LengthOverflow)?;
            if payload.starts_with_metadata() {
                segments -= 1;
            }
            if !payload.is_empty() {
                ends_metadata = payload.ends_with_metadata();
            }
        }
        // The next tag's ID and size prefix precede its payload.
        if !ends_metadata {
            segments = segments.checked_add(1).ok_or(Error::LengthOverflow)?;
        }
        let depth = self.depth.checked_add(1).ok_or(Error::LengthOverflow)?;
        check_bound(Some(bytes), self.limits.max_bytes, "encoded bytes")?;
        check_bound(
            Some(metadata),
            self.limits.max_metadata_bytes,
            "metadata bytes",
        )?;
        check_bound(Some(segments), self.limits.max_segments, "send segments")?;
        check_bound(
            Some(elements),
            self.limits.max_array_elements,
            "encoded array elements",
        )?;
        check_bound(Some(tags), self.limits.max_tags, "encoded tags")?;
        check_bound(Some(depth), self.limits.max_depth, "encode depth")?;
        let mut limits = self.limits;
        limits.max_bytes -= bytes;
        limits.max_metadata_bytes -= metadata;
        limits.max_array_elements -= elements;
        limits.max_tags -= tags;
        limits.max_depth -= depth;
        // The child's leading metadata merges with its tag envelope. An empty
        // child has no segments; both cases satisfy this one-segment credit.
        limits.max_segments = (limits.max_segments - segments)
            .checked_add(1)
            .ok_or(Error::LengthOverflow)?;
        Ok(Self::new(self.version, self.flexible, limits))
    }

    /// Patches the initial `write_i32(0)` with the complete Kafka frame size.
    ///
    /// # Errors
    /// Rejects missing placeholders and frames beyond the signed 32-bit bound.
    pub fn finish_frame(mut self) -> Result<SendPlan<'a>> {
        self.plan.finish_frame()?;
        self.finish()
    }

    /// # Errors
    /// Propagates a field's error and restores the prior encoding mode.
    pub fn with_flexible<T>(
        &mut self,
        flexible: bool,
        f: impl FnOnce(&mut Self) -> Result<T>,
    ) -> Result<T> {
        let old = self.flexible;
        self.flexible = flexible;
        let result = f(self);
        self.flexible = old;
        result
    }

    /// # Errors
    /// Rejects excessive nesting or propagates a nested encoder's error.
    pub fn nested<T>(&mut self, f: impl FnOnce(&mut Self) -> Result<T>) -> Result<T> {
        check_bound(
            self.depth.checked_add(1),
            self.limits.max_depth,
            "encode depth",
        )?;
        self.depth += 1;
        self.max_depth = self.max_depth.max(self.depth);
        let result = f(self);
        self.depth -= 1;
        result
    }

    /// # Errors
    /// Rejects excessive structure nesting or propagates the encoder's error.
    pub fn with_struct<T>(&mut self, f: impl FnOnce(&mut Self) -> Result<T>) -> Result<T> {
        self.nested(f)
    }

    /// # Errors
    /// Rejects metadata, segment, and total byte limit exhaustion.
    pub fn write_raw(&mut self, bytes: &[u8]) -> Result<()> {
        self.plan.metadata(bytes, self.limits)
    }
    /// # Errors
    /// Rejects metadata, segment, and total byte limit exhaustion.
    pub fn write_i8(&mut self, value: i8) -> Result<()> {
        self.write_raw(&value.to_be_bytes())
    }
    /// # Errors
    /// Rejects metadata, segment, and total byte limit exhaustion.
    pub fn write_u8(&mut self, value: u8) -> Result<()> {
        self.write_raw(&[value])
    }
    /// # Errors
    /// Rejects metadata, segment, and total byte limit exhaustion.
    pub fn write_i16(&mut self, value: i16) -> Result<()> {
        self.write_raw(&value.to_be_bytes())
    }
    /// # Errors
    /// Rejects metadata, segment, and total byte limit exhaustion.
    pub fn write_u16(&mut self, value: u16) -> Result<()> {
        self.write_raw(&value.to_be_bytes())
    }
    /// # Errors
    /// Rejects metadata, segment, and total byte limit exhaustion.
    pub fn write_i32(&mut self, value: i32) -> Result<()> {
        self.write_raw(&value.to_be_bytes())
    }
    /// # Errors
    /// Rejects metadata, segment, and total byte limit exhaustion.
    pub fn write_u32(&mut self, value: u32) -> Result<()> {
        self.write_raw(&value.to_be_bytes())
    }
    /// # Errors
    /// Rejects metadata, segment, and total byte limit exhaustion.
    pub fn write_i64(&mut self, value: i64) -> Result<()> {
        self.write_raw(&value.to_be_bytes())
    }
    /// # Errors
    /// Rejects metadata, segment, and total byte limit exhaustion.
    pub fn write_f64(&mut self, value: f64) -> Result<()> {
        self.write_raw(&value.to_be_bytes())
    }
    /// # Errors
    /// Rejects metadata, segment, and total byte limit exhaustion.
    pub fn write_bool(&mut self, value: bool) -> Result<()> {
        self.write_u8(u8::from(value))
    }
    /// # Errors
    /// Rejects metadata, segment, and total byte limit exhaustion.
    pub fn write_uuid(&mut self, value: [u8; 16]) -> Result<()> {
        self.write_raw(&value)
    }

    /// # Errors
    /// Rejects metadata, segment, and total byte limit exhaustion.
    pub fn write_uvarint(&mut self, mut value: u32) -> Result<()> {
        let mut bytes = [0; 5];
        let mut len = 0;
        loop {
            bytes[len] = (value & 0x7f) as u8;
            value >>= 7;
            len += 1;
            if value == 0 {
                break;
            }
            bytes[len - 1] |= 0x80;
        }
        self.write_raw(&bytes[..len])
    }

    fn length(&mut self, len: Option<usize>, string: bool) -> Result<()> {
        let maximum = if string {
            i16::MAX as usize
        } else {
            i32::MAX as usize
        };
        if len.is_some_and(|len| len > maximum) {
            return Err(Error::LengthOverflow);
        }
        if self.flexible {
            let len = match len {
                None => 0,
                Some(len) => u32::try_from(len)
                    .ok()
                    .and_then(|len| len.checked_add(1))
                    .ok_or(Error::LengthOverflow)?,
            };
            self.write_uvarint(len)
        } else if string {
            self.write_i16(match len {
                None => -1,
                Some(len) => i16::try_from(len).map_err(|_| Error::LengthOverflow)?,
            })
        } else {
            self.write_i32(match len {
                None => -1,
                Some(len) => i32::try_from(len).map_err(|_| Error::LengthOverflow)?,
            })
        }
    }

    /// # Errors
    /// Rejects unrepresentable lengths and exhausted metadata/byte limits.
    pub fn write_string(&mut self, value: &str) -> Result<()> {
        self.length(Some(value.len()), true)?;
        self.write_raw(value.as_bytes())
    }
    /// # Errors
    /// Rejects unrepresentable lengths and exhausted metadata/byte limits.
    pub fn write_nullable_string(&mut self, value: Option<&str>) -> Result<()> {
        match value {
            Some(value) => self.write_string(value),
            None => self.length(None, true),
        }
    }
    /// # Errors
    /// Rejects unrepresentable lengths and exhausted metadata/byte limits.
    pub fn write_bytes(&mut self, value: &[u8]) -> Result<()> {
        self.length(Some(value.len()), false)?;
        self.write_raw(value)
    }
    /// # Errors
    /// Rejects unrepresentable lengths and exhausted metadata/byte limits.
    pub fn write_nullable_bytes(&mut self, value: Option<&[u8]>) -> Result<()> {
        match value {
            Some(value) => self.write_bytes(value),
            None => self.length(None, false),
        }
    }
    /// # Errors
    /// Rejects unrepresentable lengths and exhausted metadata/byte/segment limits.
    pub fn write_records(&mut self, value: &Records<'a>) -> Result<()> {
        self.length(Some(value.len()?), false)?;
        match value {
            Records::Borrowed(bytes) => self.plan.borrowed(bytes, self.limits),
            Records::HeaderAndChunks { header, chunks } => {
                self.plan.metadata(header, self.limits)?;
                for chunk in *chunks {
                    self.plan.shared(chunk, self.limits)?;
                }
                Ok(())
            }
            Records::Chunks(chunks) => {
                for chunk in *chunks {
                    self.plan.shared(chunk, self.limits)?;
                }
                Ok(())
            }
        }
    }
    /// # Errors
    /// Rejects unrepresentable lengths and exhausted metadata/byte/segment limits.
    pub fn write_nullable_records(&mut self, value: Option<&Records<'a>>) -> Result<()> {
        match value {
            Some(value) => self.write_records(value),
            None => self.length(None, false),
        }
    }

    fn charge_elements(&mut self, count: usize) -> Result<()> {
        check_bound(
            self.elements.checked_add(count),
            self.limits.max_array_elements,
            "encoded array elements",
        )?;
        self.elements += count;
        Ok(())
    }

    /// Merges generated known payload plans with validated unknown extensions.
    /// Known IDs must be sorted, unique, and excluded from the unknown block.
    ///
    /// # Errors
    /// Rejects duplicate/unsorted tags and exhausted tag, byte, or segment limits.
    pub fn write_tagged_fields(
        &mut self,
        unknown: &TaggedFields<'a>,
        known: &[(u32, SendPlan<'a>)],
    ) -> Result<()> {
        for pair in known.windows(2) {
            if pair[0].0 >= pair[1].0 {
                return Err(Error::InvalidTagOrder {
                    previous: pair[0].0,
                    tag: pair[1].0,
                });
            }
        }
        let count = unknown
            .len()
            .checked_add(known.len())
            .ok_or(Error::LengthOverflow)?;
        let mut tags = self.tags.checked_add(count).ok_or(Error::LengthOverflow)?;
        let mut elements = self.elements;
        let mut max_depth = self.max_depth;
        for (_, payload) in known {
            tags = tags
                .checked_add(payload.tags)
                .ok_or(Error::LengthOverflow)?;
            elements = elements
                .checked_add(payload.elements)
                .ok_or(Error::LengthOverflow)?;
            let depth = self
                .depth
                .checked_add(1)
                .and_then(|n| n.checked_add(payload.max_depth))
                .ok_or(Error::LengthOverflow)?;
            max_depth = max_depth.max(depth);
        }
        check_bound(Some(tags), self.limits.max_tags, "encoded tags")?;
        check_bound(
            Some(elements),
            self.limits.max_array_elements,
            "encoded array elements",
        )?;
        check_bound(Some(max_depth), self.limits.max_depth, "encode depth")?;
        let count32 = u32::try_from(count).map_err(|_| Error::LengthOverflow)?;
        let mut raw = unknown.iter().peekable();
        // Check collisions before appending any tag bytes.
        for (id, _) in known {
            while raw.peek().is_some_and(|tag| tag.id < *id) {
                raw.next();
            }
            if raw.peek().is_some_and(|tag| tag.id == *id) {
                return Err(Error::InvalidTagOrder {
                    previous: *id,
                    tag: *id,
                });
            }
        }
        self.tags = tags;
        self.elements = elements;
        self.max_depth = max_depth;
        self.write_uvarint(count32)?;
        let mut raw = unknown.iter().peekable();
        for (id, payload) in known {
            while raw.peek().is_some_and(|tag| tag.id < *id) {
                let tag = raw.next().expect("tag was just observed");
                self.write_unknown_tag(tag)?;
            }
            self.write_uvarint(*id)?;
            self.write_uvarint(u32::try_from(payload.len()).map_err(|_| Error::LengthOverflow)?)?;
            self.plan.append(payload, self.limits)?;
        }
        for tag in raw {
            self.write_unknown_tag(tag)?;
        }
        Ok(())
    }

    fn write_unknown_tag(&mut self, tag: Tag<'a>) -> Result<()> {
        self.write_uvarint(tag.id)?;
        self.write_uvarint(u32::try_from(tag.payload.len()).map_err(|_| Error::LengthOverflow)?)?;
        self.write_raw(tag.payload)
    }
}
