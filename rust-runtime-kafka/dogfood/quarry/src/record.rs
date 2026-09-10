//! Canonical encoding for Quarry's durable ring records.
//!
//! Every record is independently self-identifying:
//!
//! ```text
//! +------------+---------------+----------+--------------+-------------+
//! | magic (4B) | version (u16) | tag (u8) | variant body | CRC32C (u32)|
//! +------------+---------------+----------+--------------+-------------+
//! ```
//!
//! Integers use canonical little-endian encoding. Variable-length payloads are
//! prefixed by a little-endian `u32`. The CRC32C covers the header and variant
//! body. Decoding requires one complete record and rejects checksum mismatches
//! and trailing bytes so corrupt data or framing cannot be silently accepted.

use std::{fmt, mem::size_of};

use crc32c::crc32c;
use kr_runtime::SimInstant;

use crate::types::{JobId, LeaseToken, QueueConfig, RequestId};

const MAGIC: [u8; 4] = *b"QRYJ";
const VERSION: u16 = 1;

const BEGIN_INCARNATION_TAG: u8 = 1;
const SUBMIT_TAG: u8 = 2;
const ACK_TAG: u8 = 3;
const CONFIGURE_TAG: u8 = 4;

const HEADER_LEN: usize = MAGIC.len() + size_of::<u16>() + size_of::<u8>();
const CHECKSUM_LEN: usize = size_of::<u32>();
const BEGIN_INCARNATION_BODY_LEN: usize = size_of::<u64>();
const SUBMIT_FIXED_BODY_LEN: usize =
    size_of::<u64>() + size_of::<u64>() + size_of::<u32>() + size_of::<u64>();
const ACK_BODY_LEN: usize = size_of::<u64>() * 3;
const CONFIGURE_BODY_LEN: usize = size_of::<u64>() * 4;

/// One logical mutation in Quarry's durable history.
///
/// The encoding is deliberately owned: callers can retain the returned bytes
/// until their append operation completes without borrowing queue state.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum Record {
    /// Starts a new broker incarnation.
    BeginIncarnation { id: u64 },
    /// Adds one idempotent request and its assigned job identifier.
    Submit {
        request_id: RequestId,
        job_id: JobId,
        payload: Vec<u8>,
        not_before: SimInstant,
    },
    /// Permanently completes a job under its fencing token.
    Ack {
        job_id: JobId,
        lease_token: LeaseToken,
    },
    /// Persists the exact resource bounds required to recover the queue.
    Configure { config: QueueConfig },
}

impl Record {
    /// Encodes this record using durable-record format version 1.
    pub(crate) fn encode(&self) -> Result<Vec<u8>, RecordError> {
        match self {
            Self::BeginIncarnation { id } => {
                let mut encoded =
                    Vec::with_capacity(HEADER_LEN + BEGIN_INCARNATION_BODY_LEN + CHECKSUM_LEN);
                encode_header(&mut encoded, BEGIN_INCARNATION_TAG);
                encode_u64(&mut encoded, *id);
                encode_checksum(&mut encoded);
                Ok(encoded)
            }
            Self::Submit {
                request_id,
                job_id,
                payload,
                not_before,
            } => Self::encode_submit(*request_id, *job_id, payload, *not_before),
            Self::Ack {
                job_id,
                lease_token,
            } => {
                let mut encoded = Vec::with_capacity(HEADER_LEN + ACK_BODY_LEN + CHECKSUM_LEN);
                encode_header(&mut encoded, ACK_TAG);
                encode_u64(&mut encoded, job_id.get());
                encode_u64(&mut encoded, lease_token.incarnation());
                encode_u64(&mut encoded, lease_token.sequence());
                encode_checksum(&mut encoded);
                Ok(encoded)
            }
            Self::Configure { config } => {
                let active_capacity =
                    encode_config_value(ConfigField::ActiveCapacity, config.active_capacity)?;
                let max_payload_bytes =
                    encode_config_value(ConfigField::MaxPayloadBytes, config.max_payload_bytes)?;
                let max_claim_batch =
                    encode_config_value(ConfigField::MaxClaimBatch, config.max_claim_batch)?;
                let completed_history_capacity = encode_config_value(
                    ConfigField::CompletedHistoryCapacity,
                    config.completed_history_capacity,
                )?;

                let mut encoded =
                    Vec::with_capacity(HEADER_LEN + CONFIGURE_BODY_LEN + CHECKSUM_LEN);
                encode_header(&mut encoded, CONFIGURE_TAG);
                encode_u64(&mut encoded, active_capacity);
                encode_u64(&mut encoded, max_payload_bytes);
                encode_u64(&mut encoded, max_claim_batch);
                encode_u64(&mut encoded, completed_history_capacity);
                encode_checksum(&mut encoded);
                Ok(encoded)
            }
        }
    }

    /// Encodes a submit directly from a validated mutation plan without first
    /// cloning its payload into an owned [`Record`].
    pub(crate) fn encode_submit(
        request_id: RequestId,
        job_id: JobId,
        payload: &[u8],
        not_before: SimInstant,
    ) -> Result<Vec<u8>, RecordError> {
        let payload_len = checked_payload_len(payload.len())?;
        let capacity = HEADER_LEN
            .checked_add(SUBMIT_FIXED_BODY_LEN)
            .and_then(|size| size.checked_add(payload.len()))
            .and_then(|size| size.checked_add(CHECKSUM_LEN))
            .ok_or(RecordError::PayloadTooLarge {
                size: payload.len(),
                limit: u32::MAX,
            })?;
        let mut encoded = Vec::with_capacity(capacity);
        encode_header(&mut encoded, SUBMIT_TAG);
        encode_u64(&mut encoded, request_id.get());
        encode_u64(&mut encoded, job_id.get());
        encode_u32(&mut encoded, payload_len);
        encoded.extend_from_slice(payload);
        encode_u64(&mut encoded, not_before.as_nanos());
        encode_checksum(&mut encoded);
        Ok(encoded)
    }

    /// Decodes exactly one versioned durable record.
    pub(crate) fn decode(encoded: &[u8]) -> Result<Self, RecordError> {
        let mut decoder = Decoder::new(encoded);
        let magic = decoder.read_array::<4>()?;
        if magic != MAGIC {
            return Err(RecordError::InvalidMagic { found: magic });
        }

        let version = decoder.read_u16()?;
        if version != VERSION {
            return Err(RecordError::UnsupportedVersion { found: version });
        }

        let tag = decoder.read_u8()?;
        let record = match tag {
            BEGIN_INCARNATION_TAG => Self::BeginIncarnation {
                id: decoder.read_u64()?,
            },
            SUBMIT_TAG => {
                let request_id = RequestId::new(decoder.read_u64()?);
                let job_id = JobId::new(decoder.read_u64()?);
                let payload_len = decoder.read_u32()?;
                let payload = decoder.read_bytes(payload_len)?.to_vec();
                let not_before = SimInstant::from_nanos(decoder.read_u64()?);
                Self::Submit {
                    request_id,
                    job_id,
                    payload,
                    not_before,
                }
            }
            ACK_TAG => Self::Ack {
                job_id: JobId::new(decoder.read_u64()?),
                lease_token: LeaseToken::from_parts(decoder.read_u64()?, decoder.read_u64()?),
            },
            CONFIGURE_TAG => Self::Configure {
                config: QueueConfig {
                    active_capacity: decode_config_value(
                        ConfigField::ActiveCapacity,
                        decoder.read_u64()?,
                    )?,
                    max_payload_bytes: decode_config_value(
                        ConfigField::MaxPayloadBytes,
                        decoder.read_u64()?,
                    )?,
                    max_claim_batch: decode_config_value(
                        ConfigField::MaxClaimBatch,
                        decoder.read_u64()?,
                    )?,
                    completed_history_capacity: decode_config_value(
                        ConfigField::CompletedHistoryCapacity,
                        decoder.read_u64()?,
                    )?,
                },
            },
            _ => return Err(RecordError::UnknownTag { found: tag }),
        };

        let checksum_offset = decoder.offset();
        let stored = decoder.read_u32()?;
        decoder.finish()?;
        let computed = crc32c(&encoded[..checksum_offset]);
        if stored != computed {
            return Err(RecordError::ChecksumMismatch { stored, computed });
        }
        Ok(record)
    }
}

/// One persisted [`QueueConfig`] field.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ConfigField {
    ActiveCapacity,
    MaxPayloadBytes,
    MaxClaimBatch,
    CompletedHistoryCapacity,
}

impl fmt::Display for ConfigField {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::ActiveCapacity => "active_capacity",
            Self::MaxPayloadBytes => "max_payload_bytes",
            Self::MaxClaimBatch => "max_claim_batch",
            Self::CompletedHistoryCapacity => "completed_history_capacity",
        })
    }
}

/// A canonical record encoding or decoding failure.
#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub(crate) enum RecordError {
    /// An encoded payload cannot be represented by the format's `u32` length.
    PayloadTooLarge { size: usize, limit: u32 },
    /// The input ended before the current field was complete.
    UnexpectedEnd {
        offset: usize,
        needed: usize,
        remaining: usize,
    },
    /// The input does not identify a Quarry durable record.
    InvalidMagic { found: [u8; 4] },
    /// The record uses a durable-record version this implementation does not know.
    UnsupportedVersion { found: u16 },
    /// The record contains an unknown variant tag.
    UnknownTag { found: u8 },
    /// A host `usize` configuration field cannot be persisted as a `u64`.
    ConfigEncodeOutOfRange { field: ConfigField, value: usize },
    /// A persisted `u64` configuration field cannot be represented by this host.
    ConfigDecodeOutOfRange { field: ConfigField, value: u64 },
    /// The stored CRC32C does not cover the decoded header and body.
    ChecksumMismatch { stored: u32, computed: u32 },
    /// Bytes remained after the selected variant's complete body.
    TrailingBytes { count: usize },
}

impl fmt::Display for RecordError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::PayloadTooLarge { size, limit } => {
                write!(
                    formatter,
                    "durable-record payload size {size} exceeds limit {limit}"
                )
            }
            Self::UnexpectedEnd {
                offset,
                needed,
                remaining,
            } => write!(
                formatter,
                "durable record ended at offset {offset}: needed {needed} bytes, {remaining} remain"
            ),
            Self::InvalidMagic { found } => {
                write!(
                    formatter,
                    "invalid Quarry durable-record magic {found:02x?}"
                )
            }
            Self::UnsupportedVersion { found } => {
                write!(
                    formatter,
                    "unsupported Quarry durable-record version {found}"
                )
            }
            Self::UnknownTag { found } => {
                write!(formatter, "unknown Quarry durable-record tag {found}")
            }
            Self::ConfigEncodeOutOfRange { field, value } => write!(
                formatter,
                "queue configuration field {field} value {value} cannot be encoded as u64"
            ),
            Self::ConfigDecodeOutOfRange { field, value } => write!(
                formatter,
                "queue configuration field {field} value {value} cannot be represented as usize"
            ),
            Self::ChecksumMismatch { stored, computed } => write!(
                formatter,
                "Quarry durable-record CRC32C mismatch: stored {stored:#010x}, computed {computed:#010x}"
            ),
            Self::TrailingBytes { count } => {
                write!(
                    formatter,
                    "Quarry durable record has {count} trailing bytes"
                )
            }
        }
    }
}

impl std::error::Error for RecordError {}

fn checked_payload_len(size: usize) -> Result<u32, RecordError> {
    u32::try_from(size).map_err(|_| RecordError::PayloadTooLarge {
        size,
        limit: u32::MAX,
    })
}

fn encode_config_value(field: ConfigField, value: usize) -> Result<u64, RecordError> {
    u64::try_from(value).map_err(|_| RecordError::ConfigEncodeOutOfRange { field, value })
}

fn decode_config_value(field: ConfigField, value: u64) -> Result<usize, RecordError> {
    usize::try_from(value).map_err(|_| RecordError::ConfigDecodeOutOfRange { field, value })
}

fn encode_header(encoded: &mut Vec<u8>, tag: u8) {
    encoded.extend_from_slice(&MAGIC);
    encoded.extend_from_slice(&VERSION.to_le_bytes());
    encoded.push(tag);
}

fn encode_u32(encoded: &mut Vec<u8>, value: u32) {
    encoded.extend_from_slice(&value.to_le_bytes());
}

fn encode_u64(encoded: &mut Vec<u8>, value: u64) {
    encoded.extend_from_slice(&value.to_le_bytes());
}

fn encode_checksum(encoded: &mut Vec<u8>) {
    let checksum = crc32c(encoded);
    encode_u32(encoded, checksum);
}

struct Decoder<'a> {
    encoded: &'a [u8],
    offset: usize,
}

impl<'a> Decoder<'a> {
    const fn new(encoded: &'a [u8]) -> Self {
        Self { encoded, offset: 0 }
    }

    fn read_u8(&mut self) -> Result<u8, RecordError> {
        Ok(self.read_array::<1>()?[0])
    }

    fn read_u16(&mut self) -> Result<u16, RecordError> {
        Ok(u16::from_le_bytes(self.read_array()?))
    }

    fn read_u32(&mut self) -> Result<u32, RecordError> {
        Ok(u32::from_le_bytes(self.read_array()?))
    }

    fn read_u64(&mut self) -> Result<u64, RecordError> {
        Ok(u64::from_le_bytes(self.read_array()?))
    }

    fn read_array<const LENGTH: usize>(&mut self) -> Result<[u8; LENGTH], RecordError> {
        let bytes = self.read_bytes_usize(LENGTH)?;
        Ok(bytes
            .try_into()
            .expect("slice length is fixed by read_bytes_usize"))
    }

    fn read_bytes(&mut self, length: u32) -> Result<&'a [u8], RecordError> {
        let length = usize::try_from(length).map_err(|_| RecordError::UnexpectedEnd {
            offset: self.offset,
            needed: usize::MAX,
            remaining: self.remaining(),
        })?;
        self.read_bytes_usize(length)
    }

    fn read_bytes_usize(&mut self, length: usize) -> Result<&'a [u8], RecordError> {
        let remaining = self.remaining();
        if length > remaining {
            return Err(RecordError::UnexpectedEnd {
                offset: self.offset,
                needed: length,
                remaining,
            });
        }
        let end = self
            .offset
            .checked_add(length)
            .expect("length is bounded by the remaining input");
        let bytes = &self.encoded[self.offset..end];
        self.offset = end;
        Ok(bytes)
    }

    fn finish(self) -> Result<(), RecordError> {
        let count = self.remaining();
        if count == 0 {
            Ok(())
        } else {
            Err(RecordError::TrailingBytes { count })
        }
    }

    const fn offset(&self) -> usize {
        self.offset
    }

    fn remaining(&self) -> usize {
        self.encoded.len() - self.offset
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const BEGIN_GOLDEN: [u8; 19] = [
        0x51, 0x52, 0x59, 0x4a, // stable magic
        0x01, 0x00, // version 1
        0x01, // BeginIncarnation
        0x08, 0x07, 0x06, 0x05, 0x04, 0x03, 0x02, 0x01, // incarnation
        0xc3, 0x8a, 0xc4, 0x8d, // CRC32C
    ];

    const SUBMIT_GOLDEN: [u8; 42] = [
        0x51, 0x52, 0x59, 0x4a, // stable magic
        0x01, 0x00, // version 1
        0x02, // Submit
        0x08, 0x07, 0x06, 0x05, 0x04, 0x03, 0x02, 0x01, // request id
        0x18, 0x17, 0x16, 0x15, 0x14, 0x13, 0x12, 0x11, // job id
        0x03, 0x00, 0x00, 0x00, // payload length
        0xaa, 0xbb, 0xcc, // payload
        0x28, 0x27, 0x26, 0x25, 0x24, 0x23, 0x22, 0x21, // not-before
        0x2f, 0x20, 0x51, 0x26, // CRC32C
    ];

    const ACK_GOLDEN: [u8; 35] = [
        0x51, 0x52, 0x59, 0x4a, // stable magic
        0x01, 0x00, // version 1
        0x03, // Ack
        0x18, 0x17, 0x16, 0x15, 0x14, 0x13, 0x12, 0x11, // job id
        0x38, 0x37, 0x36, 0x35, 0x34, 0x33, 0x32, 0x31, // incarnation
        0x48, 0x47, 0x46, 0x45, 0x44, 0x43, 0x42, 0x41, // sequence
        0x3d, 0x9d, 0x6d, 0xe7, // CRC32C
    ];

    const CONFIGURE_GOLDEN: [u8; 43] = [
        0x51, 0x52, 0x59, 0x4a, // stable magic
        0x01, 0x00, // version 1
        0x04, // Configure
        0x04, 0x03, 0x02, 0x01, 0x00, 0x00, 0x00, 0x00, // active capacity
        0x14, 0x13, 0x12, 0x11, 0x00, 0x00, 0x00, 0x00, // max payload bytes
        0x24, 0x23, 0x22, 0x21, 0x00, 0x00, 0x00, 0x00, // max claim batch
        0x34, 0x33, 0x32, 0x31, 0x00, 0x00, 0x00, 0x00, // completed history
        0xb3, 0xb1, 0x6a, 0x12, // CRC32C
    ];

    fn golden_records() -> [(Record, &'static [u8]); 4] {
        [
            (
                Record::BeginIncarnation {
                    id: 0x0102_0304_0506_0708,
                },
                &BEGIN_GOLDEN,
            ),
            (
                Record::Submit {
                    request_id: RequestId::new(0x0102_0304_0506_0708),
                    job_id: JobId::new(0x1112_1314_1516_1718),
                    payload: vec![0xaa, 0xbb, 0xcc],
                    not_before: SimInstant::from_nanos(0x2122_2324_2526_2728),
                },
                &SUBMIT_GOLDEN,
            ),
            (
                Record::Ack {
                    job_id: JobId::new(0x1112_1314_1516_1718),
                    lease_token: LeaseToken::from_parts(
                        0x3132_3334_3536_3738,
                        0x4142_4344_4546_4748,
                    ),
                },
                &ACK_GOLDEN,
            ),
            (
                Record::Configure {
                    config: QueueConfig {
                        active_capacity: 0x0102_0304,
                        max_payload_bytes: 0x1112_1314,
                        max_claim_batch: 0x2122_2324,
                        completed_history_capacity: 0x3132_3334,
                    },
                },
                &CONFIGURE_GOLDEN,
            ),
        ]
    }

    fn refresh_checksum(encoded: &mut [u8]) {
        let checksum_offset = encoded.len() - CHECKSUM_LEN;
        let checksum = crc32c(&encoded[..checksum_offset]);
        encoded[checksum_offset..].copy_from_slice(&checksum.to_le_bytes());
    }

    #[test]
    fn version_one_encoding_matches_golden_bytes() {
        for (record, golden) in golden_records() {
            assert_eq!(record.encode().unwrap(), golden, "record={record:?}");
            assert_eq!(Record::decode(golden).unwrap(), record);
        }
    }

    #[test]
    fn borrowed_submit_encoding_matches_the_owned_record() {
        let record = Record::Submit {
            request_id: RequestId::new(9),
            job_id: JobId::new(4),
            payload: b"payload".to_vec(),
            not_before: SimInstant::from_nanos(17),
        };
        let Record::Submit {
            request_id,
            job_id,
            payload,
            not_before,
        } = &record
        else {
            unreachable!();
        };
        assert_eq!(
            Record::encode_submit(*request_id, *job_id, payload, *not_before),
            record.encode()
        );
    }

    #[test]
    fn every_truncated_golden_record_is_rejected() {
        for (_, golden) in golden_records() {
            for length in 0..golden.len() {
                assert!(
                    matches!(
                        Record::decode(&golden[..length]),
                        Err(RecordError::UnexpectedEnd { .. })
                    ),
                    "accepted or misclassified length {length} of {}: {:?}",
                    golden.len(),
                    Record::decode(&golden[..length])
                );
            }
        }
    }

    #[test]
    fn malformed_header_fields_are_explicit() {
        let mut invalid_magic = BEGIN_GOLDEN;
        invalid_magic[0] ^= 0xff;
        refresh_checksum(&mut invalid_magic);
        assert_eq!(
            Record::decode(&invalid_magic),
            Err(RecordError::InvalidMagic {
                found: [0xae, 0x52, 0x59, 0x4a]
            })
        );

        let mut unsupported_version = BEGIN_GOLDEN;
        unsupported_version[4..6].copy_from_slice(&2_u16.to_le_bytes());
        refresh_checksum(&mut unsupported_version);
        assert_eq!(
            Record::decode(&unsupported_version),
            Err(RecordError::UnsupportedVersion { found: 2 })
        );

        let mut unknown_tag = BEGIN_GOLDEN;
        unknown_tag[6] = 0xff;
        refresh_checksum(&mut unknown_tag);
        assert_eq!(
            Record::decode(&unknown_tag),
            Err(RecordError::UnknownTag { found: 0xff })
        );
    }

    #[test]
    fn every_variant_rejects_trailing_bytes() {
        for (_, golden) in golden_records() {
            let mut with_suffix = golden.to_vec();
            with_suffix.extend_from_slice(&[0xde, 0xad]);
            assert_eq!(
                Record::decode(&with_suffix),
                Err(RecordError::TrailingBytes { count: 2 })
            );
        }
    }

    #[test]
    fn shared_crc32c_matches_the_standard_check_value() {
        assert_eq!(crc32c(b"123456789"), 0xe306_9283);
    }

    #[test]
    fn every_single_bit_flip_fails_closed() {
        for (_, golden) in golden_records() {
            for offset in 0..golden.len() {
                for bit in 0..8 {
                    let mut corrupted = golden.to_vec();
                    corrupted[offset] ^= 1 << bit;
                    assert!(
                        Record::decode(&corrupted).is_err(),
                        "accepted bit {bit} flip at offset {offset} of {}",
                        golden.len()
                    );
                }
            }
        }
    }

    #[test]
    fn payload_corruption_reports_checksum_mismatch() {
        let payload_offset = HEADER_LEN + size_of::<u64>() * 2 + size_of::<u32>();
        let mut corrupted = SUBMIT_GOLDEN;
        corrupted[payload_offset] ^= 1;

        assert!(matches!(
            Record::decode(&corrupted),
            Err(RecordError::ChecksumMismatch { .. })
        ));
    }

    #[test]
    fn queue_config_round_trips_exactly() {
        let record = Record::Configure {
            config: QueueConfig {
                active_capacity: usize::MAX,
                max_payload_bytes: 0,
                max_claim_batch: 1,
                completed_history_capacity: usize::MAX / 2,
            },
        };

        assert_eq!(Record::decode(&record.encode().unwrap()).unwrap(), record);
    }

    #[cfg(target_pointer_width = "32")]
    #[test]
    fn oversized_persisted_queue_config_is_rejected_on_32_bit_hosts() {
        assert_eq!(
            decode_config_value(ConfigField::ActiveCapacity, u64::MAX),
            Err(RecordError::ConfigDecodeOutOfRange {
                field: ConfigField::ActiveCapacity,
                value: u64::MAX,
            })
        );
    }

    #[test]
    fn declared_payload_length_is_strictly_bounded_by_input() {
        let payload_length_offset = HEADER_LEN + size_of::<u64>() * 2;
        let mut truncated_payload =
            SUBMIT_GOLDEN[..payload_length_offset + size_of::<u32>()].to_vec();
        truncated_payload[payload_length_offset..][..size_of::<u32>()]
            .copy_from_slice(&u32::MAX.to_le_bytes());
        assert_eq!(
            Record::decode(&truncated_payload),
            Err(RecordError::UnexpectedEnd {
                offset: payload_length_offset + size_of::<u32>(),
                needed: usize::try_from(u32::MAX).unwrap(),
                remaining: 0,
            })
        );
    }

    #[test]
    fn payload_length_accepts_u32_max_and_rejects_the_next_value() {
        let maximum = usize::try_from(u32::MAX).unwrap();
        assert_eq!(checked_payload_len(maximum), Ok(u32::MAX));

        if let Some(too_large) = maximum.checked_add(1) {
            assert_eq!(
                checked_payload_len(too_large),
                Err(RecordError::PayloadTooLarge {
                    size: too_large,
                    limit: u32::MAX,
                })
            );
        }
    }
}
