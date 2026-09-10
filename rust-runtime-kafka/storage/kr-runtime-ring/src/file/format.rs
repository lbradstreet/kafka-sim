//! Pure encoding and validation for version one of the disk-backed ring format.
//!
//! The file begins with two independently checksummed 4 KiB superblocks. The
//! circular data region begins at [`DATA_OFFSET`]. All integers are little
//! endian. Decoders reject non-canonical encodings so recovery never has to
//! guess whether a field is meaningful.

use std::fmt;

use crc32c::crc32c;

pub(super) const SUPERBLOCK_LEN: usize = 4_096;
pub(super) const SUPERBLOCK_COUNT: usize = 2;
pub(super) const DATA_OFFSET: u64 = (SUPERBLOCK_LEN * SUPERBLOCK_COUNT) as u64;

pub(super) const FRAME_HEADER_LEN: usize = 32;
pub(super) const FRAME_TRAILER_LEN: usize = 4;
pub(super) const MIN_DATA_FRAME_LEN: usize = FRAME_HEADER_LEN + FRAME_TRAILER_LEN;

const SUPERBLOCK_MAGIC: [u8; 8] = *b"DSTRING\0";
const FRAME_MAGIC: [u8; 4] = *b"DSTR";
const FORMAT_VERSION: u16 = 1;
const SUPERBLOCK_HEADER_LEN: u16 = 96;
const SUPERBLOCK_CHECKSUM_OFFSET: usize = SUPERBLOCK_LEN - 4;

const SUPERBLOCK_SLOT_OFFSET: usize = 12;
const SUPERBLOCK_FLAGS_OFFSET: usize = 13;
const SUPERBLOCK_SHORT_RESERVED_OFFSET: usize = 14;
const SUPERBLOCK_GENERATION_OFFSET: usize = 16;
const SUPERBLOCK_DATA_CAPACITY_OFFSET: usize = 24;
const SUPERBLOCK_MAX_RECORD_BYTES_OFFSET: usize = 32;
const SUPERBLOCK_MAX_LIVE_RECORDS_OFFSET: usize = 36;
const SUPERBLOCK_MAX_LIVE_PAYLOAD_BYTES_OFFSET: usize = 40;
const SUPERBLOCK_HEAD_SEQUENCE_OFFSET: usize = 48;
const SUPERBLOCK_TAIL_SEQUENCE_OFFSET: usize = 56;
const SUPERBLOCK_HEAD_OFFSET_OFFSET: usize = 64;
const SUPERBLOCK_TAIL_OFFSET_OFFSET: usize = 72;
const SUPERBLOCK_USED_BYTES_OFFSET: usize = 80;
const SUPERBLOCK_RETAINED_PAYLOAD_BYTES_OFFSET: usize = 88;
const SUPERBLOCK_RESERVED_OFFSET: usize = SUPERBLOCK_HEADER_LEN as usize;

const FRAME_PAYLOAD_LEN_OFFSET: usize = 8;
const FRAME_SEQUENCE_OFFSET: usize = 12;
const FRAME_AUX_OFFSET: usize = 20;
const FRAME_HEADER_CHECKSUM_OFFSET: usize = 28;
const DATA_KIND: u8 = 1;
const PADDING_KIND: u8 = 2;

/// Immutable file geometry recorded in every superblock.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct Geometry {
    pub(super) data_capacity: u64,
    pub(super) max_record_bytes: u32,
    pub(super) max_live_records: u32,
    pub(super) max_live_payload_bytes: u64,
}

impl Geometry {
    pub(super) fn validate(self) -> Result<(), FormatError> {
        if self.data_capacity == 0 {
            return Err(FormatError::ZeroGeometryField {
                field: "data_capacity",
            });
        }
        if self.max_record_bytes == 0 {
            return Err(FormatError::ZeroGeometryField {
                field: "max_record_bytes",
            });
        }
        if self.max_live_records == 0 {
            return Err(FormatError::ZeroGeometryField {
                field: "max_live_records",
            });
        }
        if self.max_live_payload_bytes == 0 {
            return Err(FormatError::ZeroGeometryField {
                field: "max_live_payload_bytes",
            });
        }
        if u64::from(self.max_record_bytes) > self.max_live_payload_bytes {
            return Err(FormatError::RecordLimitExceedsPayloadLimit {
                max_record_bytes: self.max_record_bytes,
                max_live_payload_bytes: self.max_live_payload_bytes,
            });
        }
        if self.max_live_payload_bytes > self.data_capacity {
            return Err(FormatError::PayloadLimitExceedsDataCapacity {
                max_live_payload_bytes: self.max_live_payload_bytes,
                data_capacity: self.data_capacity,
            });
        }
        let required = u64::from(self.max_record_bytes)
            .checked_add(MIN_DATA_FRAME_LEN as u64)
            .ok_or(FormatError::FrameLengthOverflow)?;
        if required > self.data_capacity {
            return Err(FormatError::RecordDoesNotFitDataCapacity {
                required,
                data_capacity: self.data_capacity,
            });
        }
        Ok(())
    }
}

/// Durable logical and physical bounds published by one superblock.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct Checkpoint {
    pub(super) head_seq: u64,
    pub(super) tail_seq: u64,
    pub(super) head_offset: u64,
    pub(super) tail_offset: u64,
    pub(super) used_bytes: u64,
    pub(super) retained_payload_bytes: u64,
}

impl Checkpoint {
    pub(super) fn validate(self, geometry: Geometry) -> Result<(), FormatError> {
        geometry.validate()?;
        if self.head_seq > self.tail_seq {
            return Err(FormatError::HeadAfterTail {
                head_seq: self.head_seq,
                tail_seq: self.tail_seq,
            });
        }
        let live_records = self.tail_seq - self.head_seq;
        if live_records > u64::from(geometry.max_live_records) {
            return Err(FormatError::LiveRecordLimitExceeded {
                live_records,
                max_live_records: geometry.max_live_records,
            });
        }
        validate_offset("head_offset", self.head_offset, geometry.data_capacity)?;
        validate_offset("tail_offset", self.tail_offset, geometry.data_capacity)?;
        if self.used_bytes > geometry.data_capacity {
            return Err(FormatError::UsedBytesExceedDataCapacity {
                used_bytes: self.used_bytes,
                data_capacity: geometry.data_capacity,
            });
        }
        if self.retained_payload_bytes > geometry.max_live_payload_bytes {
            return Err(FormatError::RetainedPayloadLimitExceeded {
                retained_payload_bytes: self.retained_payload_bytes,
                max_live_payload_bytes: geometry.max_live_payload_bytes,
            });
        }

        if live_records == 0 {
            if self.used_bytes != 0 || self.retained_payload_bytes != 0 {
                return Err(FormatError::EmptyCheckpointRetainsBytes {
                    used_bytes: self.used_bytes,
                    retained_payload_bytes: self.retained_payload_bytes,
                });
            }
            if self.head_offset != 0 || self.tail_offset != 0 {
                return Err(FormatError::EmptyCheckpointHasNonZeroOffsets {
                    head_offset: self.head_offset,
                    tail_offset: self.tail_offset,
                });
            }
        } else if self.used_bytes == 0 {
            return Err(FormatError::NonEmptyCheckpointHasNoUsedBytes { live_records });
        }

        let maximum_payload = live_records
            .checked_mul(u64::from(geometry.max_record_bytes))
            .ok_or(FormatError::CheckpointLengthOverflow)?;
        if self.retained_payload_bytes > maximum_payload {
            return Err(FormatError::RetainedPayloadExceedsRecordMaximum {
                retained_payload_bytes: self.retained_payload_bytes,
                maximum_payload,
            });
        }
        let minimum_used_bytes = live_records
            .checked_mul(MIN_DATA_FRAME_LEN as u64)
            .and_then(|overhead| overhead.checked_add(self.retained_payload_bytes))
            .ok_or(FormatError::CheckpointLengthOverflow)?;
        if self.used_bytes < minimum_used_bytes {
            return Err(FormatError::UsedBytesBelowFrameMinimum {
                used_bytes: self.used_bytes,
                minimum_used_bytes,
            });
        }

        let expected_tail = ((u128::from(self.head_offset) + u128::from(self.used_bytes))
            % u128::from(geometry.data_capacity)) as u64;
        if self.tail_offset != expected_tail {
            return Err(FormatError::TailOffsetMismatch {
                expected: expected_tail,
                found: self.tail_offset,
            });
        }
        Ok(())
    }
}

/// One of the two alternating durable checkpoints.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct Superblock {
    pub(super) physical_slot: u8,
    pub(super) generation: u64,
    pub(super) geometry: Geometry,
    pub(super) checkpoint: Checkpoint,
}

impl Superblock {
    pub(super) fn validate(self) -> Result<(), FormatError> {
        validate_physical_slot(self.physical_slot)?;
        if self.generation == 0 {
            return Err(FormatError::ZeroGeneration);
        }
        self.geometry.validate()?;
        self.checkpoint.validate(self.geometry)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum FrameKind {
    Data,
    Padding,
}

impl FrameKind {
    const fn tag(self) -> u8 {
        match self {
            Self::Data => DATA_KIND,
            Self::Padding => PADDING_KIND,
        }
    }
}

/// Validated fixed-width metadata for one physical data or padding frame.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct FrameHeader {
    pub(super) kind: FrameKind,
    pub(super) payload_len: u32,
    pub(super) sequence: u64,
    pub(super) aux: u64,
    encoded_len: usize,
}

impl FrameHeader {
    #[must_use]
    pub(super) const fn encoded_len(self) -> usize {
        self.encoded_len
    }

    /// Physical bytes charged to the ring. Padding stores only its header but
    /// consumes every byte through the end of the circular data region.
    #[must_use]
    #[cfg(test)]
    pub(super) const fn occupied_len(self) -> u64 {
        match self.kind {
            FrameKind::Data => self.encoded_len as u64,
            FrameKind::Padding => self.aux,
        }
    }
}

/// A structural or canonical-format violation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) enum FormatError {
    UnexpectedEnd {
        context: &'static str,
        needed: usize,
        actual: usize,
    },
    TrailingBytes {
        context: &'static str,
        expected: usize,
        actual: usize,
    },
    InvalidSuperblockMagic([u8; 8]),
    InvalidFrameMagic([u8; 4]),
    UnsupportedSuperblockVersion(u16),
    UnsupportedFrameVersion(u16),
    InvalidSuperblockHeaderLength(u16),
    InvalidPhysicalSlot(u8),
    InvalidExpectedPhysicalSlot(u8),
    UnexpectedPhysicalSlot {
        expected: u8,
        found: u8,
    },
    NonZeroSuperblockFlags(u8),
    NonZeroSuperblockReserved {
        offset: usize,
        value: u8,
    },
    NonZeroFrameReserved(u8),
    UnknownFrameKind(u8),
    ZeroGeometryField {
        field: &'static str,
    },
    RecordLimitExceedsPayloadLimit {
        max_record_bytes: u32,
        max_live_payload_bytes: u64,
    },
    PayloadLimitExceedsDataCapacity {
        max_live_payload_bytes: u64,
        data_capacity: u64,
    },
    RecordDoesNotFitDataCapacity {
        required: u64,
        data_capacity: u64,
    },
    HeadAfterTail {
        head_seq: u64,
        tail_seq: u64,
    },
    LiveRecordLimitExceeded {
        live_records: u64,
        max_live_records: u32,
    },
    OffsetOutOfRange {
        field: &'static str,
        offset: u64,
        data_capacity: u64,
    },
    UsedBytesExceedDataCapacity {
        used_bytes: u64,
        data_capacity: u64,
    },
    RetainedPayloadLimitExceeded {
        retained_payload_bytes: u64,
        max_live_payload_bytes: u64,
    },
    EmptyCheckpointRetainsBytes {
        used_bytes: u64,
        retained_payload_bytes: u64,
    },
    EmptyCheckpointHasNonZeroOffsets {
        head_offset: u64,
        tail_offset: u64,
    },
    NonEmptyCheckpointHasNoUsedBytes {
        live_records: u64,
    },
    RetainedPayloadExceedsRecordMaximum {
        retained_payload_bytes: u64,
        maximum_payload: u64,
    },
    UsedBytesBelowFrameMinimum {
        used_bytes: u64,
        minimum_used_bytes: u64,
    },
    TailOffsetMismatch {
        expected: u64,
        found: u64,
    },
    CheckpointLengthOverflow,
    ZeroGeneration,
    PayloadTooLarge {
        size: usize,
        limit: usize,
    },
    PayloadLengthNotRepresentable {
        size: usize,
    },
    FrameLengthOverflow,
    AllocationFailed {
        requested: usize,
    },
    DataAuxNotZero(u64),
    PaddingHasPayload(u32),
    PaddingTooShort {
        bytes_to_end: u64,
        minimum: u64,
    },
    PaddingLengthMismatch {
        expected: u64,
        found: u64,
    },
    WrongFrameKind {
        expected: FrameKind,
        found: FrameKind,
    },
    SuperblockChecksumMismatch {
        stored: u32,
        computed: u32,
    },
    FrameHeaderChecksumMismatch {
        stored: u32,
        computed: u32,
    },
    FrameChecksumMismatch {
        stored: u32,
        computed: u32,
    },
}

impl fmt::Display for FormatError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnexpectedEnd {
                context,
                needed,
                actual,
            } => write!(
                formatter,
                "{context} ended early: needed {needed} bytes, found {actual}"
            ),
            Self::TrailingBytes {
                context,
                expected,
                actual,
            } => write!(
                formatter,
                "{context} has trailing bytes: expected {expected}, found {actual}"
            ),
            Self::InvalidSuperblockMagic(found) => {
                write!(formatter, "invalid ring superblock magic {found:02x?}")
            }
            Self::InvalidFrameMagic(found) => {
                write!(formatter, "invalid ring frame magic {found:02x?}")
            }
            Self::UnsupportedSuperblockVersion(found) => {
                write!(formatter, "unsupported ring superblock version {found}")
            }
            Self::UnsupportedFrameVersion(found) => {
                write!(formatter, "unsupported ring frame version {found}")
            }
            Self::InvalidSuperblockHeaderLength(found) => write!(
                formatter,
                "ring superblock header length is {found}, expected {SUPERBLOCK_HEADER_LEN}"
            ),
            Self::InvalidPhysicalSlot(found) => {
                write!(formatter, "invalid ring superblock physical slot {found}")
            }
            Self::InvalidExpectedPhysicalSlot(found) => write!(
                formatter,
                "caller supplied invalid expected superblock slot {found}"
            ),
            Self::UnexpectedPhysicalSlot { expected, found } => write!(
                formatter,
                "ring superblock says physical slot {found}, expected {expected}"
            ),
            Self::NonZeroSuperblockFlags(found) => {
                write!(
                    formatter,
                    "ring superblock flags are {found:#04x}, expected zero"
                )
            }
            Self::NonZeroSuperblockReserved { offset, value } => write!(
                formatter,
                "ring superblock reserved byte at offset {offset} is {value:#04x}"
            ),
            Self::NonZeroFrameReserved(found) => {
                write!(formatter, "ring frame reserved byte is {found:#04x}")
            }
            Self::UnknownFrameKind(found) => write!(formatter, "unknown ring frame kind {found}"),
            Self::ZeroGeometryField { field } => {
                write!(formatter, "ring geometry field {field} must be nonzero")
            }
            Self::RecordLimitExceedsPayloadLimit {
                max_record_bytes,
                max_live_payload_bytes,
            } => write!(
                formatter,
                "maximum record size {max_record_bytes} exceeds live payload limit {max_live_payload_bytes}"
            ),
            Self::PayloadLimitExceedsDataCapacity {
                max_live_payload_bytes,
                data_capacity,
            } => write!(
                formatter,
                "live payload limit {max_live_payload_bytes} exceeds data capacity {data_capacity}"
            ),
            Self::RecordDoesNotFitDataCapacity {
                required,
                data_capacity,
            } => write!(
                formatter,
                "largest framed record needs {required} bytes but data capacity is {data_capacity}"
            ),
            Self::HeadAfterTail { head_seq, tail_seq } => write!(
                formatter,
                "checkpoint head sequence {head_seq} is after tail sequence {tail_seq}"
            ),
            Self::LiveRecordLimitExceeded {
                live_records,
                max_live_records,
            } => write!(
                formatter,
                "checkpoint retains {live_records} records, exceeding limit {max_live_records}"
            ),
            Self::OffsetOutOfRange {
                field,
                offset,
                data_capacity,
            } => write!(
                formatter,
                "checkpoint {field} {offset} is outside data capacity {data_capacity}"
            ),
            Self::UsedBytesExceedDataCapacity {
                used_bytes,
                data_capacity,
            } => write!(
                formatter,
                "checkpoint uses {used_bytes} bytes, exceeding data capacity {data_capacity}"
            ),
            Self::RetainedPayloadLimitExceeded {
                retained_payload_bytes,
                max_live_payload_bytes,
            } => write!(
                formatter,
                "checkpoint retains {retained_payload_bytes} payload bytes, exceeding limit {max_live_payload_bytes}"
            ),
            Self::EmptyCheckpointRetainsBytes {
                used_bytes,
                retained_payload_bytes,
            } => write!(
                formatter,
                "empty checkpoint retains {used_bytes} physical and {retained_payload_bytes} payload bytes"
            ),
            Self::EmptyCheckpointHasNonZeroOffsets {
                head_offset,
                tail_offset,
            } => write!(
                formatter,
                "empty checkpoint has nonzero head offset {head_offset} or tail offset {tail_offset}"
            ),
            Self::NonEmptyCheckpointHasNoUsedBytes { live_records } => write!(
                formatter,
                "checkpoint retains {live_records} records but uses zero physical bytes"
            ),
            Self::RetainedPayloadExceedsRecordMaximum {
                retained_payload_bytes,
                maximum_payload,
            } => write!(
                formatter,
                "checkpoint retains {retained_payload_bytes} payload bytes but its records can hold at most {maximum_payload}"
            ),
            Self::UsedBytesBelowFrameMinimum {
                used_bytes,
                minimum_used_bytes,
            } => write!(
                formatter,
                "checkpoint uses {used_bytes} bytes but its records require at least {minimum_used_bytes}"
            ),
            Self::TailOffsetMismatch { expected, found } => write!(
                formatter,
                "checkpoint tail offset is {found}, expected {expected} from head plus used bytes"
            ),
            Self::CheckpointLengthOverflow => {
                formatter.write_str("checkpoint byte accounting overflowed")
            }
            Self::ZeroGeneration => {
                formatter.write_str("ring superblock generation must be nonzero")
            }
            Self::PayloadTooLarge { size, limit } => {
                write!(formatter, "frame payload {size} exceeds limit {limit}")
            }
            Self::PayloadLengthNotRepresentable { size } => write!(
                formatter,
                "frame payload {size} cannot be represented by the version-one format"
            ),
            Self::FrameLengthOverflow => formatter.write_str("frame length overflowed"),
            Self::AllocationFailed { requested } => {
                write!(
                    formatter,
                    "could not allocate {requested} bytes for a ring frame"
                )
            }
            Self::DataAuxNotZero(found) => write!(
                formatter,
                "data frame auxiliary value is {found}, expected zero"
            ),
            Self::PaddingHasPayload(found) => {
                write!(formatter, "padding frame declares {found} payload bytes")
            }
            Self::PaddingTooShort {
                bytes_to_end,
                minimum,
            } => write!(
                formatter,
                "padding frame spans {bytes_to_end} bytes, fewer than its {minimum}-byte header"
            ),
            Self::PaddingLengthMismatch { expected, found } => write!(
                formatter,
                "padding frame spans {found} bytes, expected {expected} bytes to the data-region end"
            ),
            Self::WrongFrameKind { expected, found } => write!(
                formatter,
                "ring frame kind is {found:?}, expected {expected:?}"
            ),
            Self::SuperblockChecksumMismatch { stored, computed } => write!(
                formatter,
                "ring superblock CRC32C mismatch: stored {stored:#010x}, computed {computed:#010x}"
            ),
            Self::FrameHeaderChecksumMismatch { stored, computed } => write!(
                formatter,
                "ring frame header CRC32C mismatch: stored {stored:#010x}, computed {computed:#010x}"
            ),
            Self::FrameChecksumMismatch { stored, computed } => write!(
                formatter,
                "ring frame CRC32C mismatch: stored {stored:#010x}, computed {computed:#010x}"
            ),
        }
    }
}

impl std::error::Error for FormatError {}

pub(super) fn encode_superblock(
    superblock: &Superblock,
) -> Result<[u8; SUPERBLOCK_LEN], FormatError> {
    superblock.validate()?;
    let mut encoded = [0_u8; SUPERBLOCK_LEN];
    encoded[..8].copy_from_slice(&SUPERBLOCK_MAGIC);
    encoded[8..10].copy_from_slice(&FORMAT_VERSION.to_le_bytes());
    encoded[10..12].copy_from_slice(&SUPERBLOCK_HEADER_LEN.to_le_bytes());
    encoded[SUPERBLOCK_SLOT_OFFSET] = superblock.physical_slot;
    put_u64(
        &mut encoded,
        SUPERBLOCK_GENERATION_OFFSET,
        superblock.generation,
    );
    put_u64(
        &mut encoded,
        SUPERBLOCK_DATA_CAPACITY_OFFSET,
        superblock.geometry.data_capacity,
    );
    put_u32(
        &mut encoded,
        SUPERBLOCK_MAX_RECORD_BYTES_OFFSET,
        superblock.geometry.max_record_bytes,
    );
    put_u32(
        &mut encoded,
        SUPERBLOCK_MAX_LIVE_RECORDS_OFFSET,
        superblock.geometry.max_live_records,
    );
    put_u64(
        &mut encoded,
        SUPERBLOCK_MAX_LIVE_PAYLOAD_BYTES_OFFSET,
        superblock.geometry.max_live_payload_bytes,
    );
    put_u64(
        &mut encoded,
        SUPERBLOCK_HEAD_SEQUENCE_OFFSET,
        superblock.checkpoint.head_seq,
    );
    put_u64(
        &mut encoded,
        SUPERBLOCK_TAIL_SEQUENCE_OFFSET,
        superblock.checkpoint.tail_seq,
    );
    put_u64(
        &mut encoded,
        SUPERBLOCK_HEAD_OFFSET_OFFSET,
        superblock.checkpoint.head_offset,
    );
    put_u64(
        &mut encoded,
        SUPERBLOCK_TAIL_OFFSET_OFFSET,
        superblock.checkpoint.tail_offset,
    );
    put_u64(
        &mut encoded,
        SUPERBLOCK_USED_BYTES_OFFSET,
        superblock.checkpoint.used_bytes,
    );
    put_u64(
        &mut encoded,
        SUPERBLOCK_RETAINED_PAYLOAD_BYTES_OFFSET,
        superblock.checkpoint.retained_payload_bytes,
    );
    let checksum = crc32c(&encoded[..SUPERBLOCK_CHECKSUM_OFFSET]);
    put_u32(&mut encoded, SUPERBLOCK_CHECKSUM_OFFSET, checksum);
    Ok(encoded)
}

pub(super) fn decode_superblock(
    encoded: &[u8],
    expected_physical_slot: u8,
) -> Result<Superblock, FormatError> {
    require_exact(encoded.len(), SUPERBLOCK_LEN, "ring superblock")?;
    if expected_physical_slot >= SUPERBLOCK_COUNT as u8 {
        return Err(FormatError::InvalidExpectedPhysicalSlot(
            expected_physical_slot,
        ));
    }
    let magic = read_array::<8>(encoded, 0);
    if magic != SUPERBLOCK_MAGIC {
        return Err(FormatError::InvalidSuperblockMagic(magic));
    }
    let version = read_u16(encoded, 8);
    if version != FORMAT_VERSION {
        return Err(FormatError::UnsupportedSuperblockVersion(version));
    }
    let header_len = read_u16(encoded, 10);
    if header_len != SUPERBLOCK_HEADER_LEN {
        return Err(FormatError::InvalidSuperblockHeaderLength(header_len));
    }
    let stored = read_u32(encoded, SUPERBLOCK_CHECKSUM_OFFSET);
    let computed = crc32c(&encoded[..SUPERBLOCK_CHECKSUM_OFFSET]);
    if stored != computed {
        return Err(FormatError::SuperblockChecksumMismatch { stored, computed });
    }

    let physical_slot = encoded[SUPERBLOCK_SLOT_OFFSET];
    validate_physical_slot(physical_slot)?;
    if physical_slot != expected_physical_slot {
        return Err(FormatError::UnexpectedPhysicalSlot {
            expected: expected_physical_slot,
            found: physical_slot,
        });
    }
    let flags = encoded[SUPERBLOCK_FLAGS_OFFSET];
    if flags != 0 {
        return Err(FormatError::NonZeroSuperblockFlags(flags));
    }
    for (offset, value) in encoded[SUPERBLOCK_SHORT_RESERVED_OFFSET..SUPERBLOCK_GENERATION_OFFSET]
        .iter()
        .chain(encoded[SUPERBLOCK_RESERVED_OFFSET..SUPERBLOCK_CHECKSUM_OFFSET].iter())
        .enumerate()
    {
        if *value != 0 {
            let physical_offset = if offset < 2 {
                SUPERBLOCK_SHORT_RESERVED_OFFSET + offset
            } else {
                SUPERBLOCK_RESERVED_OFFSET + offset - 2
            };
            return Err(FormatError::NonZeroSuperblockReserved {
                offset: physical_offset,
                value: *value,
            });
        }
    }

    let superblock = Superblock {
        physical_slot,
        generation: read_u64(encoded, SUPERBLOCK_GENERATION_OFFSET),
        geometry: Geometry {
            data_capacity: read_u64(encoded, SUPERBLOCK_DATA_CAPACITY_OFFSET),
            max_record_bytes: read_u32(encoded, SUPERBLOCK_MAX_RECORD_BYTES_OFFSET),
            max_live_records: read_u32(encoded, SUPERBLOCK_MAX_LIVE_RECORDS_OFFSET),
            max_live_payload_bytes: read_u64(encoded, SUPERBLOCK_MAX_LIVE_PAYLOAD_BYTES_OFFSET),
        },
        checkpoint: Checkpoint {
            head_seq: read_u64(encoded, SUPERBLOCK_HEAD_SEQUENCE_OFFSET),
            tail_seq: read_u64(encoded, SUPERBLOCK_TAIL_SEQUENCE_OFFSET),
            head_offset: read_u64(encoded, SUPERBLOCK_HEAD_OFFSET_OFFSET),
            tail_offset: read_u64(encoded, SUPERBLOCK_TAIL_OFFSET_OFFSET),
            used_bytes: read_u64(encoded, SUPERBLOCK_USED_BYTES_OFFSET),
            retained_payload_bytes: read_u64(encoded, SUPERBLOCK_RETAINED_PAYLOAD_BYTES_OFFSET),
        },
    };
    superblock.validate()?;
    Ok(superblock)
}

pub(super) fn encode_data_frame(
    sequence: u64,
    payload: &[u8],
    max_payload_bytes: usize,
) -> Result<Vec<u8>, FormatError> {
    validate_payload(payload.len(), max_payload_bytes)?;
    let payload_len =
        u32::try_from(payload.len()).map_err(|_| FormatError::PayloadLengthNotRepresentable {
            size: payload.len(),
        })?;
    let encoded_len = data_frame_len(payload.len())?;
    let header = encode_frame_header(FrameKind::Data, payload_len, sequence, 0)?;
    let mut encoded = Vec::new();
    encoded
        .try_reserve_exact(encoded_len)
        .map_err(|_| FormatError::AllocationFailed {
            requested: encoded_len,
        })?;
    encoded.extend_from_slice(&header);
    encoded.extend_from_slice(payload);
    let checksum = crc32c(&encoded);
    encoded.extend_from_slice(&checksum.to_le_bytes());
    debug_assert_eq!(encoded.len(), encoded_len);
    Ok(encoded)
}

pub(super) fn encode_padding_frame(
    sequence: u64,
    bytes_to_end: u64,
) -> Result<[u8; FRAME_HEADER_LEN], FormatError> {
    validate_padding_len(bytes_to_end)?;
    encode_frame_header(FrameKind::Padding, 0, sequence, bytes_to_end)
}

pub(super) fn decode_frame_header(
    encoded: &[u8],
    max_payload_bytes: usize,
) -> Result<FrameHeader, FormatError> {
    require_min(encoded.len(), FRAME_HEADER_LEN, "ring frame header")?;
    let stored = read_u32(encoded, FRAME_HEADER_CHECKSUM_OFFSET);
    let computed = crc32c(&encoded[..FRAME_HEADER_CHECKSUM_OFFSET]);
    if stored != computed {
        return Err(FormatError::FrameHeaderChecksumMismatch { stored, computed });
    }
    let magic = read_array::<4>(encoded, 0);
    if magic != FRAME_MAGIC {
        return Err(FormatError::InvalidFrameMagic(magic));
    }
    let version = read_u16(encoded, 4);
    if version != FORMAT_VERSION {
        return Err(FormatError::UnsupportedFrameVersion(version));
    }
    let kind = match encoded[6] {
        DATA_KIND => FrameKind::Data,
        PADDING_KIND => FrameKind::Padding,
        found => return Err(FormatError::UnknownFrameKind(found)),
    };
    let reserved = encoded[7];
    if reserved != 0 {
        return Err(FormatError::NonZeroFrameReserved(reserved));
    }
    let payload_len = read_u32(encoded, FRAME_PAYLOAD_LEN_OFFSET);
    let payload_len_usize =
        usize::try_from(payload_len).map_err(|_| FormatError::PayloadTooLarge {
            size: usize::MAX,
            limit: max_payload_bytes,
        })?;
    validate_payload(payload_len_usize, max_payload_bytes)?;
    let sequence = read_u64(encoded, FRAME_SEQUENCE_OFFSET);
    let aux = read_u64(encoded, FRAME_AUX_OFFSET);
    let encoded_len = match kind {
        FrameKind::Data => {
            if aux != 0 {
                return Err(FormatError::DataAuxNotZero(aux));
            }
            data_frame_len(payload_len_usize)?
        }
        FrameKind::Padding => {
            if payload_len != 0 {
                return Err(FormatError::PaddingHasPayload(payload_len));
            }
            validate_padding_len(aux)?;
            FRAME_HEADER_LEN
        }
    };
    Ok(FrameHeader {
        kind,
        payload_len,
        sequence,
        aux,
        encoded_len,
    })
}

pub(super) fn decode_data_frame(
    encoded: &[u8],
    max_payload_bytes: usize,
) -> Result<(FrameHeader, Vec<u8>), FormatError> {
    let header = decode_frame_header(encoded, max_payload_bytes)?;
    if header.kind != FrameKind::Data {
        return Err(FormatError::WrongFrameKind {
            expected: FrameKind::Data,
            found: header.kind,
        });
    }
    require_exact(encoded.len(), header.encoded_len, "ring data frame")?;
    let checksum_offset = header.encoded_len - FRAME_TRAILER_LEN;
    let stored = read_u32(encoded, checksum_offset);
    let computed = crc32c(&encoded[..checksum_offset]);
    if stored != computed {
        return Err(FormatError::FrameChecksumMismatch { stored, computed });
    }
    let payload_len =
        usize::try_from(header.payload_len).map_err(|_| FormatError::PayloadTooLarge {
            size: usize::MAX,
            limit: max_payload_bytes,
        })?;
    let mut payload = Vec::new();
    payload
        .try_reserve_exact(payload_len)
        .map_err(|_| FormatError::AllocationFailed {
            requested: payload_len,
        })?;
    payload.extend_from_slice(&encoded[FRAME_HEADER_LEN..checksum_offset]);
    Ok((header, payload))
}

pub(super) fn decode_padding_frame(
    encoded: &[u8],
    max_payload_bytes: usize,
    expected_bytes_to_end: u64,
) -> Result<FrameHeader, FormatError> {
    let header = decode_frame_header(encoded, max_payload_bytes)?;
    if header.kind != FrameKind::Padding {
        return Err(FormatError::WrongFrameKind {
            expected: FrameKind::Padding,
            found: header.kind,
        });
    }
    require_exact(encoded.len(), FRAME_HEADER_LEN, "ring padding frame")?;
    if header.aux != expected_bytes_to_end {
        return Err(FormatError::PaddingLengthMismatch {
            expected: expected_bytes_to_end,
            found: header.aux,
        });
    }
    Ok(header)
}

pub(super) fn data_frame_len(payload_len: usize) -> Result<usize, FormatError> {
    u32::try_from(payload_len)
        .map_err(|_| FormatError::PayloadLengthNotRepresentable { size: payload_len })?;
    FRAME_HEADER_LEN
        .checked_add(payload_len)
        .and_then(|length| length.checked_add(FRAME_TRAILER_LEN))
        .ok_or(FormatError::FrameLengthOverflow)
}

fn encode_frame_header(
    kind: FrameKind,
    payload_len: u32,
    sequence: u64,
    aux: u64,
) -> Result<[u8; FRAME_HEADER_LEN], FormatError> {
    match kind {
        FrameKind::Data if aux != 0 => return Err(FormatError::DataAuxNotZero(aux)),
        FrameKind::Padding if payload_len != 0 => {
            return Err(FormatError::PaddingHasPayload(payload_len));
        }
        FrameKind::Padding => validate_padding_len(aux)?,
        FrameKind::Data => {}
    }
    let mut encoded = [0_u8; FRAME_HEADER_LEN];
    encoded[..4].copy_from_slice(&FRAME_MAGIC);
    encoded[4..6].copy_from_slice(&FORMAT_VERSION.to_le_bytes());
    encoded[6] = kind.tag();
    put_u32(&mut encoded, FRAME_PAYLOAD_LEN_OFFSET, payload_len);
    put_u64(&mut encoded, FRAME_SEQUENCE_OFFSET, sequence);
    put_u64(&mut encoded, FRAME_AUX_OFFSET, aux);
    let checksum = crc32c(&encoded[..FRAME_HEADER_CHECKSUM_OFFSET]);
    put_u32(&mut encoded, FRAME_HEADER_CHECKSUM_OFFSET, checksum);
    Ok(encoded)
}

fn validate_payload(size: usize, limit: usize) -> Result<(), FormatError> {
    if size > limit {
        Err(FormatError::PayloadTooLarge { size, limit })
    } else {
        Ok(())
    }
}

fn validate_padding_len(bytes_to_end: u64) -> Result<(), FormatError> {
    if bytes_to_end < FRAME_HEADER_LEN as u64 {
        Err(FormatError::PaddingTooShort {
            bytes_to_end,
            minimum: FRAME_HEADER_LEN as u64,
        })
    } else {
        Ok(())
    }
}

fn validate_physical_slot(slot: u8) -> Result<(), FormatError> {
    if slot < SUPERBLOCK_COUNT as u8 {
        Ok(())
    } else {
        Err(FormatError::InvalidPhysicalSlot(slot))
    }
}

fn validate_offset(
    field: &'static str,
    offset: u64,
    data_capacity: u64,
) -> Result<(), FormatError> {
    if offset < data_capacity {
        Ok(())
    } else {
        Err(FormatError::OffsetOutOfRange {
            field,
            offset,
            data_capacity,
        })
    }
}

fn require_min(actual: usize, needed: usize, context: &'static str) -> Result<(), FormatError> {
    if actual < needed {
        Err(FormatError::UnexpectedEnd {
            context,
            needed,
            actual,
        })
    } else {
        Ok(())
    }
}

fn require_exact(actual: usize, expected: usize, context: &'static str) -> Result<(), FormatError> {
    if actual < expected {
        Err(FormatError::UnexpectedEnd {
            context,
            needed: expected,
            actual,
        })
    } else if actual > expected {
        Err(FormatError::TrailingBytes {
            context,
            expected,
            actual,
        })
    } else {
        Ok(())
    }
}

fn read_array<const LENGTH: usize>(encoded: &[u8], offset: usize) -> [u8; LENGTH] {
    encoded[offset..offset + LENGTH]
        .try_into()
        .expect("fixed-width field was bounds checked")
}

fn read_u16(encoded: &[u8], offset: usize) -> u16 {
    u16::from_le_bytes(read_array(encoded, offset))
}

fn read_u32(encoded: &[u8], offset: usize) -> u32 {
    u32::from_le_bytes(read_array(encoded, offset))
}

fn read_u64(encoded: &[u8], offset: usize) -> u64 {
    u64::from_le_bytes(read_array(encoded, offset))
}

fn put_u32(encoded: &mut [u8], offset: usize, value: u32) {
    encoded[offset..offset + 4].copy_from_slice(&value.to_le_bytes());
}

fn put_u64(encoded: &mut [u8], offset: usize, value: u64) {
    encoded[offset..offset + 8].copy_from_slice(&value.to_le_bytes());
}

#[cfg(test)]
mod tests {
    use super::*;

    const SUPERBLOCK_GOLDEN_PREFIX: &[u8] = &[
        0x44, 0x53, 0x54, 0x52, 0x49, 0x4e, 0x47, 0x00, 0x01, 0x00, 0x60, 0x00, 0x01, 0x00, 0x00,
        0x00, 0x08, 0x07, 0x06, 0x05, 0x04, 0x03, 0x02, 0x01, 0x00, 0x04, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x40, 0x00, 0x00, 0x00, 0x0a, 0x00, 0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x07, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x09, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x00, 0x84, 0x03, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x4c, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x00, 0x00, 0xc8, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x14, 0x00,
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
    ];
    const DATA_GOLDEN: &[u8] = &[
        0x44, 0x53, 0x54, 0x52, 0x01, 0x00, 0x01, 0x00, 0x03, 0x00, 0x00, 0x00, 0x08, 0x07, 0x06,
        0x05, 0x04, 0x03, 0x02, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x7e, 0x28,
        0xa0, 0x98, 0xaa, 0xbb, 0xcc, 0xbd, 0xe0, 0xd3, 0x3a,
    ];
    const PADDING_GOLDEN: &[u8] = &[
        0x44, 0x53, 0x54, 0x52, 0x01, 0x00, 0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x08, 0x07, 0x06,
        0x05, 0x04, 0x03, 0x02, 0x01, 0x40, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x57, 0xc8,
        0x5a, 0x3f,
    ];

    fn sample_superblock(slot: u8) -> Superblock {
        Superblock {
            physical_slot: slot,
            generation: 0x0102_0304_0506_0708,
            geometry: Geometry {
                data_capacity: 1_024,
                max_record_bytes: 64,
                max_live_records: 10,
                max_live_payload_bytes: 512,
            },
            checkpoint: Checkpoint {
                head_seq: 7,
                tail_seq: 9,
                head_offset: 900,
                tail_offset: 76,
                used_bytes: 200,
                retained_payload_bytes: 20,
            },
        }
    }

    fn empty_superblock() -> Superblock {
        Superblock {
            physical_slot: 0,
            generation: 1,
            geometry: Geometry {
                data_capacity: 1_024,
                max_record_bytes: 64,
                max_live_records: 10,
                max_live_payload_bytes: 512,
            },
            checkpoint: Checkpoint {
                head_seq: 0,
                tail_seq: 0,
                head_offset: 0,
                tail_offset: 0,
                used_bytes: 0,
                retained_payload_bytes: 0,
            },
        }
    }

    fn rewrite_superblock_checksum(encoded: &mut [u8; SUPERBLOCK_LEN]) {
        let checksum = crc32c(&encoded[..SUPERBLOCK_CHECKSUM_OFFSET]);
        put_u32(encoded, SUPERBLOCK_CHECKSUM_OFFSET, checksum);
    }

    fn rewrite_frame_header_checksum(encoded: &mut [u8]) {
        let checksum = crc32c(&encoded[..FRAME_HEADER_CHECKSUM_OFFSET]);
        put_u32(encoded, FRAME_HEADER_CHECKSUM_OFFSET, checksum);
    }

    fn v1_reference_crc32c(bytes: &[u8]) -> u32 {
        const POLYNOMIAL: u32 = 0x82f6_3b78;
        let mut crc = u32::MAX;
        for byte in bytes {
            crc ^= u32::from(*byte);
            for _ in 0..8 {
                let mask = 0_u32.wrapping_sub(crc & 1);
                crc = (crc >> 1) ^ (POLYNOMIAL & mask);
            }
        }
        !crc
    }

    #[test]
    fn library_crc32c_matches_version_one_reference() {
        const LENGTHS: [usize; 19] = [
            0, 1, 7, 8, 9, 31, 32, 33, 255, 256, 257, 4_095, 4_096, 4_097, 16_383, 16_384, 16_385,
            65_535, 65_536,
        ];
        let bytes: Vec<u8> = (0..=LENGTHS[LENGTHS.len() - 1])
            .map(|index| (index.wrapping_mul(31).wrapping_add(index / 7)) as u8)
            .collect();

        for length in LENGTHS {
            let aligned = &bytes[..length];
            assert_eq!(crc32c(aligned), v1_reference_crc32c(aligned));

            let unaligned = &bytes[1..1 + length];
            assert_eq!(crc32c(unaligned), v1_reference_crc32c(unaligned));
        }
    }

    #[test]
    fn version_one_golden_vectors_are_stable() {
        assert_eq!(DATA_OFFSET, 8_192);
        let superblock = encode_superblock(&sample_superblock(1)).unwrap();
        assert_eq!(
            &superblock[..SUPERBLOCK_HEADER_LEN as usize],
            SUPERBLOCK_GOLDEN_PREFIX
        );
        assert!(
            superblock[SUPERBLOCK_RESERVED_OFFSET..SUPERBLOCK_CHECKSUM_OFFSET]
                .iter()
                .all(|byte| *byte == 0)
        );
        assert_eq!(
            read_u32(&superblock, SUPERBLOCK_CHECKSUM_OFFSET),
            0x36ef_65b0
        );

        let data = encode_data_frame(0x0102_0304_0506_0708, &[0xaa, 0xbb, 0xcc], 3).unwrap();
        assert_eq!(data, DATA_GOLDEN);
        let padding = encode_padding_frame(0x0102_0304_0506_0708, 64).unwrap();
        assert_eq!(padding, PADDING_GOLDEN);
    }

    #[test]
    fn superblock_data_and_padding_round_trip() {
        let expected = sample_superblock(1);
        let encoded = encode_superblock(&expected).unwrap();
        assert_eq!(decode_superblock(&encoded, 1).unwrap(), expected);

        let payload = b"round trip";
        let encoded = encode_data_frame(42, payload, payload.len()).unwrap();
        let (header, decoded) = decode_data_frame(&encoded, payload.len()).unwrap();
        assert_eq!(header.kind, FrameKind::Data);
        assert_eq!(header.sequence, 42);
        assert_eq!(header.aux, 0);
        assert_eq!(header.encoded_len(), encoded.len());
        assert_eq!(header.occupied_len(), encoded.len() as u64);
        assert_eq!(decoded, payload);

        let encoded = encode_padding_frame(43, 128).unwrap();
        let header = decode_padding_frame(&encoded, payload.len(), 128).unwrap();
        assert_eq!(header.kind, FrameKind::Padding);
        assert_eq!(header.sequence, 43);
        assert_eq!(header.payload_len, 0);
        assert_eq!(header.encoded_len(), FRAME_HEADER_LEN);
        assert_eq!(header.occupied_len(), 128);
    }

    #[test]
    fn every_superblock_truncation_and_bit_flip_fails_closed() {
        let encoded = encode_superblock(&sample_superblock(0)).unwrap();
        for length in 0..encoded.len() {
            assert!(
                decode_superblock(&encoded[..length], 0).is_err(),
                "truncation length={length}"
            );
        }
        for index in 0..encoded.len() {
            for bit in 0..8 {
                let mut corrupted = encoded;
                corrupted[index] ^= 1 << bit;
                assert!(
                    decode_superblock(&corrupted, 0).is_err(),
                    "bit flip index={index}, bit={bit}"
                );
            }
        }
    }

    #[test]
    fn every_data_frame_truncation_and_bit_flip_fails_closed() {
        let encoded = encode_data_frame(1, b"payload", 7).unwrap();
        for length in 0..encoded.len() {
            assert!(
                decode_data_frame(&encoded[..length], 7).is_err(),
                "truncation length={length}"
            );
        }
        for index in 0..encoded.len() {
            for bit in 0..8 {
                let mut corrupted = encoded.clone();
                corrupted[index] ^= 1 << bit;
                assert!(
                    decode_data_frame(&corrupted, 7).is_err(),
                    "bit flip index={index}, bit={bit}"
                );
            }
        }
    }

    #[test]
    fn every_padding_truncation_and_bit_flip_fails_closed() {
        let encoded = encode_padding_frame(1, 64).unwrap();
        for length in 0..encoded.len() {
            assert!(
                decode_padding_frame(&encoded[..length], 0, 64).is_err(),
                "truncation length={length}"
            );
        }
        for index in 0..encoded.len() {
            for bit in 0..8 {
                let mut corrupted = encoded;
                corrupted[index] ^= 1 << bit;
                assert!(
                    decode_padding_frame(&corrupted, 0, 64).is_err(),
                    "bit flip index={index}, bit={bit}"
                );
            }
        }
    }

    #[test]
    fn declared_payload_is_bounded_from_the_header_before_body_allocation() {
        let mut encoded = encode_data_frame(1, &[], 0).unwrap();
        put_u32(&mut encoded, FRAME_PAYLOAD_LEN_OFFSET, u32::MAX);
        rewrite_frame_header_checksum(&mut encoded);
        assert_eq!(
            decode_frame_header(&encoded, 1_024),
            Err(FormatError::PayloadTooLarge {
                size: u32::MAX as usize,
                limit: 1_024,
            })
        );
    }

    #[test]
    fn padding_is_header_only_and_must_span_exactly_to_the_region_end() {
        assert_eq!(
            encode_padding_frame(1, 31),
            Err(FormatError::PaddingTooShort {
                bytes_to_end: 31,
                minimum: 32,
            })
        );
        let encoded = encode_padding_frame(7, 64).unwrap();
        assert_eq!(encoded.len(), FRAME_HEADER_LEN);
        assert_eq!(
            decode_padding_frame(&encoded, 0, 63),
            Err(FormatError::PaddingLengthMismatch {
                expected: 63,
                found: 64,
            })
        );

        let mut noncanonical = encoded;
        put_u32(&mut noncanonical, FRAME_PAYLOAD_LEN_OFFSET, 1);
        rewrite_frame_header_checksum(&mut noncanonical);
        assert_eq!(
            decode_frame_header(&noncanonical, 1),
            Err(FormatError::PaddingHasPayload(1))
        );
    }

    #[test]
    fn frame_reserved_and_kind_specific_fields_are_canonical() {
        let mut reserved = encode_data_frame(1, &[], 0).unwrap();
        reserved[7] = 1;
        rewrite_frame_header_checksum(&mut reserved);
        assert_eq!(
            decode_frame_header(&reserved, 0),
            Err(FormatError::NonZeroFrameReserved(1))
        );

        let mut data_aux = encode_data_frame(1, &[], 0).unwrap();
        put_u64(&mut data_aux, FRAME_AUX_OFFSET, 7);
        rewrite_frame_header_checksum(&mut data_aux);
        assert_eq!(
            decode_frame_header(&data_aux, 0),
            Err(FormatError::DataAuxNotZero(7))
        );
    }

    #[test]
    fn superblock_rejects_noncanonical_metadata_and_reserved_bytes() {
        let encoded = encode_superblock(&sample_superblock(1)).unwrap();
        assert_eq!(
            decode_superblock(&encoded, 0),
            Err(FormatError::UnexpectedPhysicalSlot {
                expected: 0,
                found: 1,
            })
        );
        assert_eq!(
            decode_superblock(&encoded, 2),
            Err(FormatError::InvalidExpectedPhysicalSlot(2))
        );

        let mut flags = encoded;
        flags[SUPERBLOCK_FLAGS_OFFSET] = 1;
        rewrite_superblock_checksum(&mut flags);
        assert_eq!(
            decode_superblock(&flags, 1),
            Err(FormatError::NonZeroSuperblockFlags(1))
        );

        let mut short_reserved = encoded;
        short_reserved[SUPERBLOCK_SHORT_RESERVED_OFFSET] = 0x80;
        rewrite_superblock_checksum(&mut short_reserved);
        assert_eq!(
            decode_superblock(&short_reserved, 1),
            Err(FormatError::NonZeroSuperblockReserved {
                offset: SUPERBLOCK_SHORT_RESERVED_OFFSET,
                value: 0x80,
            })
        );

        let mut long_reserved = encoded;
        long_reserved[SUPERBLOCK_RESERVED_OFFSET + 17] = 0x40;
        rewrite_superblock_checksum(&mut long_reserved);
        assert_eq!(
            decode_superblock(&long_reserved, 1),
            Err(FormatError::NonZeroSuperblockReserved {
                offset: SUPERBLOCK_RESERVED_OFFSET + 17,
                value: 0x40,
            })
        );
    }

    #[test]
    fn superblock_validates_geometry_and_checkpoint_accounting() {
        let mut invalid = empty_superblock();
        invalid.generation = 0;
        assert_eq!(
            encode_superblock(&invalid),
            Err(FormatError::ZeroGeneration)
        );

        let mut invalid = empty_superblock();
        invalid.geometry.data_capacity = 0;
        assert_eq!(
            encode_superblock(&invalid),
            Err(FormatError::ZeroGeometryField {
                field: "data_capacity",
            })
        );

        let mut invalid = empty_superblock();
        invalid.checkpoint.tail_seq = 1;
        assert_eq!(
            encode_superblock(&invalid),
            Err(FormatError::NonEmptyCheckpointHasNoUsedBytes { live_records: 1 })
        );

        let mut invalid = empty_superblock();
        invalid.checkpoint.head_offset = 4;
        invalid.checkpoint.tail_offset = 4;
        assert_eq!(
            encode_superblock(&invalid),
            Err(FormatError::EmptyCheckpointHasNonZeroOffsets {
                head_offset: 4,
                tail_offset: 4,
            })
        );

        let mut invalid = sample_superblock(0);
        invalid.checkpoint.tail_offset = 77;
        assert_eq!(
            encode_superblock(&invalid),
            Err(FormatError::TailOffsetMismatch {
                expected: 76,
                found: 77,
            })
        );

        let mut invalid = sample_superblock(0);
        invalid.checkpoint.used_bytes = 90;
        invalid.checkpoint.tail_offset = 990;
        assert_eq!(
            encode_superblock(&invalid),
            Err(FormatError::UsedBytesBelowFrameMinimum {
                used_bytes: 90,
                minimum_used_bytes: 92,
            })
        );

        let mut encoded = encode_superblock(&sample_superblock(0)).unwrap();
        put_u16(&mut encoded, 10, 95);
        rewrite_superblock_checksum(&mut encoded);
        assert_eq!(
            decode_superblock(&encoded, 0),
            Err(FormatError::InvalidSuperblockHeaderLength(95))
        );
    }

    #[test]
    fn crc32c_matches_the_standard_check_value() {
        assert_eq!(crc32c(b"123456789"), 0xe306_9283);
    }

    fn put_u16(encoded: &mut [u8], offset: usize, value: u16) {
        encoded[offset..offset + 2].copy_from_slice(&value.to_le_bytes());
    }
}
