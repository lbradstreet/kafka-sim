//! Bounded, owner-local streaming Kafka magic-2 record batches.
#![no_std]
#![forbid(unsafe_code)]

extern crate alloc;

mod batch;
mod codec;
mod decode;
mod output;
mod record;
mod record_set;

pub use batch::{
    AbortProgress, BatchAbort, BatchConfig, BatchState, EncodeBudget, EncodeProgress,
    FinalizedBatch, Identity, RecordBatchBuilder, SealedBatch,
};
pub use codec::{CodecPool, CodecPoolStatus, Compression, ZstdConfig};
pub use decode::{
    BatchDecodeError, BatchDecodeLimits, BatchHeader, DecodedBatch, DecodedHeader, DecodedRecord,
    HeaderIter, RecordIter, inspect_batch,
};
pub use kr_kafka_protocol::plan::SharedBytes;
pub use output::{OutputPool, OutputPoolStatus, OutputReclaimProgress};
pub use record::{Header, OwnedHeader, OwnedRecord, Record, crc32c, encoded_len};
pub use record_set::{RecordSetIter, RecordSetLimits, RecordSetStats};

/// Magic-2 fixed header size, including base offset and records count.
pub const BATCH_HEADER_BYTES: usize = 61;

/// Resource errors preserve a deferred builder; encoding errors latch it failed.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Error {
    InvalidConfig,
    UnsupportedCompression,
    CodecConfigurationMismatch,
    CodecFailure(usize),
    AllocationFailed,
    LengthOverflow,
    RawTooLarge,
    CompressedTooLarge,
    InvalidIdentity,
    EmptyBatch,
    Closed,
    Failed,
    Transmitted,
    SharedOutput,
    OutputExhausted,
}

impl core::fmt::Display for Error {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(f, "{self:?}")
    }
}
impl core::error::Error for Error {}
pub type Result<T> = core::result::Result<T, Error>;
