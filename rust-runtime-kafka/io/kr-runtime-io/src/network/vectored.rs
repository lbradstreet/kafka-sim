//! Shared owned scatter/gather contract and provider-independent validation.
use super::{ByteStreamSubmit, NetworkError, NetworkFailure, WriteRequest, WriteResult};
use kr_runtime::CompletionResult;
pub use kr_shared_bytes::SharedBytes;
use std::{collections::VecDeque, error::Error, fmt, future::Future, ops::Range};

/// Conservative portable bound used by the deterministic providers.
pub const MAX_WRITE_SEGMENTS: usize = 64;

/// Immutable shared backing storage and the byte range to transmit from its view.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WriteSegment {
    pub bytes: SharedBytes,
    pub range: Range<u32>,
}

/// Owned segments retained until terminal completion, even if the waiter is dropped.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct VectoredWriteRequest {
    pub segments: Vec<WriteSegment>,
}

/// Checked payload and retained allocation sizes for admission accounting.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct VectoredWriteSize {
    pub payload_bytes: usize,
    /// Full backing allocations, counting shared aliases only once per request.
    pub retained_bytes: usize,
}

impl VectoredWriteRequest {
    /// Validates every bound before admission, allocation, faults, or side effects.
    ///
    /// # Errors
    /// Returns `InvalidRequest` for empty payloads, invalid ranges, arithmetic
    /// overflow, segment storage above the cap, or bytes above the operation cap.
    /// The segment vector's capacity is bounded as well as its length. Subviews
    /// charge their entire allocation; repeated aliases charge it once.
    pub fn validate(
        &self,
        max_segments: usize,
        max_operation_bytes: usize,
    ) -> Result<VectoredWriteSize, NetworkError> {
        let invalid = |reason| NetworkError::InvalidRequest { reason };
        if self.segments.is_empty() || self.segments.capacity() > max_segments {
            return Err(invalid(
                "vectored segment storage is empty or exceeds max_segments",
            ));
        }
        let mut payload_bytes = 0usize;
        let mut retained_bytes = 0usize;
        for (index, segment) in self.segments.iter().enumerate() {
            let start = usize::try_from(segment.range.start)
                .map_err(|_| invalid("vectored range is not representable"))?;
            let end = usize::try_from(segment.range.end)
                .map_err(|_| invalid("vectored range is not representable"))?;
            if start >= end || end > segment.bytes.len() {
                return Err(invalid("vectored segment range is empty or out of bounds"));
            }
            payload_bytes = payload_bytes
                .checked_add(end - start)
                .ok_or_else(|| invalid("vectored payload length overflowed"))?;
            if !self.segments[..index]
                .iter()
                .any(|earlier| earlier.bytes.shares_allocation(&segment.bytes))
            {
                retained_bytes = retained_bytes
                    .checked_add(segment.bytes.allocation_len())
                    .ok_or_else(|| invalid("vectored retained allocation size overflowed"))?;
            }
            if payload_bytes > max_operation_bytes || retained_bytes > max_operation_bytes {
                return Err(invalid("vectored write exceeds max_operation_bytes"));
            }
        }
        Ok(VectoredWriteSize {
            payload_bytes,
            retained_bytes,
        })
    }
}

/// Terminal success returns the original segment vector and every shared allocation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct VectoredWriteResult {
    pub segments: Vec<WriteSegment>,
    /// Exact prefix bytes accepted in segment order; may end inside any segment.
    pub bytes_written: usize,
}

/// Terminal failure returns all ownership and reports exact known prefix progress.
/// Certainty is carried by the surrounding [`CompletionError`].
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct VectoredWriteFailure {
    pub error: NetworkError,
    pub segments: Vec<WriteSegment>,
    pub bytes_transferred: usize,
}

impl VectoredWriteFailure {
    #[must_use]
    pub fn new(error: NetworkError, segments: Vec<WriteSegment>, bytes_transferred: usize) -> Self {
        Self {
            error,
            segments,
            bytes_transferred,
        }
    }
    #[must_use]
    pub const fn error(&self) -> &NetworkError {
        &self.error
    }
    #[must_use]
    pub const fn bytes_transferred(&self) -> usize {
        self.bytes_transferred
    }
    #[must_use]
    pub fn into_segments(self) -> Vec<WriteSegment> {
        self.segments
    }
}
impl fmt::Display for VectoredWriteFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.error.fmt(formatter)
    }
}
impl Error for VectoredWriteFailure {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        Some(&self.error)
    }
}

/// Owned vectored extension with the same invocation-order admission as writes.
///
/// Rejection returns all segments with `NotApplied` certainty and zero progress.
/// After admission the provider retains backing storage until terminal completion;
/// abandoning observation cannot release a buffer still used by an operation.
/// Successful progress is an exact prefix of the concatenated ranges. Failures
/// carry known progress and certainty; uncertain progress must not be resumed on
/// the same connection. Read capacity remains independent of write byte capacity.
pub trait ByteStreamVectoredSubmit: ByteStreamSubmit {
    /// Owned response carrying all segment storage on either terminal outcome.
    type WriteVectoredResponse: Future<Output = CompletionResult<VectoredWriteResult, VectoredWriteFailure>>
        + 'static;
    /// Maximum retained segment-vector capacity for one admitted write.
    #[must_use]
    fn max_segments(&self) -> usize;
    /// Attempts admission during this call, before returning the completion ticket.
    fn submit_write_vectored(&self, request: VectoredWriteRequest) -> Self::WriteVectoredResponse;
}

/// A vectored stream whose handle and every owned response can cross threads.
pub trait SendByteStreamVectoredSubmit:
    ByteStreamVectoredSubmit<WriteVectoredResponse: Send> + super::SendByteStreamSubmit
{
}

impl<T> SendByteStreamVectoredSubmit for T where
    T: ByteStreamVectoredSubmit<WriteVectoredResponse: Send> + super::SendByteStreamSubmit
{
}

pub(super) enum WriteData {
    Contiguous(Vec<u8>),
    Vectored(VectoredWriteRequest),
}
impl From<WriteRequest> for WriteData {
    fn from(request: WriteRequest) -> Self {
        Self::Contiguous(request.buffer)
    }
}
impl From<VectoredWriteRequest> for WriteData {
    fn from(request: VectoredWriteRequest) -> Self {
        Self::Vectored(request)
    }
}
impl WriteData {
    pub(super) fn validate(&self, max_bytes: usize) -> Result<usize, NetworkError> {
        match self {
            Self::Contiguous(buffer) if buffer.capacity() <= max_bytes => Ok(buffer.capacity()),
            Self::Contiguous(_) => Err(NetworkError::InvalidRequest {
                reason: "write buffer exceeds max_operation_bytes",
            }),
            Self::Vectored(request) => request
                .validate(MAX_WRITE_SEGMENTS, max_bytes)
                .map(|size| size.retained_bytes),
        }
    }
    pub(super) fn len(&self) -> usize {
        match self {
            Self::Contiguous(buffer) => buffer.len(),
            // The sum was checked at admission, before queuing or execution.
            Self::Vectored(request) => request
                .segments
                .iter()
                .map(|s| (s.range.end - s.range.start) as usize)
                .sum(),
        }
    }
    pub(super) fn is_empty(&self) -> bool {
        self.len() == 0
    }
    pub(super) fn append_prefix(
        &self,
        destination: &mut VecDeque<u8>,
        length: usize,
    ) -> Result<(), NetworkError> {
        destination
            .try_reserve(length)
            .map_err(|_| NetworkError::ResourceExhausted {
                resource: "directional buffer allocation",
                limit: destination.len().saturating_add(length),
            })?;
        match self {
            Self::Contiguous(buffer) => destination.extend(buffer[..length].iter().copied()),
            Self::Vectored(request) => {
                let mut remaining = length;
                for segment in &request.segments {
                    let bytes = &segment.bytes.as_slice()
                        [segment.range.start as usize..segment.range.end as usize];
                    let count = bytes.len().min(remaining);
                    destination.extend(bytes[..count].iter().copied());
                    remaining -= count;
                    if remaining == 0 {
                        break;
                    }
                }
            }
        }
        Ok(())
    }
    pub(super) fn failure(self, error: NetworkError, bytes_transferred: usize) -> WriteFailure {
        WriteFailure {
            error,
            data: self,
            bytes_transferred,
        }
    }
    pub(super) fn success(self, bytes_written: usize) -> WriteSuccess {
        WriteSuccess {
            data: self,
            bytes_written,
        }
    }
}
pub(super) struct WriteSuccess {
    data: WriteData,
    bytes_written: usize,
}
pub(super) struct WriteFailure {
    error: NetworkError,
    data: WriteData,
    bytes_transferred: usize,
}
pub(super) type WriteOutput = CompletionResult<WriteSuccess, WriteFailure>;

pub(super) fn contiguous(output: WriteOutput) -> CompletionResult<WriteResult, NetworkFailure> {
    output
        .map(|success| {
            let WriteData::Contiguous(buffer) = success.data else {
                unreachable!("contiguous response owns contiguous data")
            };
            WriteResult {
                buffer,
                bytes_written: success.bytes_written,
            }
        })
        .map_err(|error| {
            error.map(|failure| {
                let WriteData::Contiguous(buffer) = failure.data else {
                    unreachable!("contiguous response owns contiguous data")
                };
                NetworkFailure::with_buffer(failure.error, buffer, failure.bytes_transferred)
            })
        })
}
pub(super) fn vectored(
    output: WriteOutput,
) -> CompletionResult<VectoredWriteResult, VectoredWriteFailure> {
    output
        .map(|success| {
            let WriteData::Vectored(request) = success.data else {
                unreachable!("vectored response owns vectored data")
            };
            VectoredWriteResult {
                segments: request.segments,
                bytes_written: success.bytes_written,
            }
        })
        .map_err(|error| {
            error.map(|failure| {
                let WriteData::Vectored(request) = failure.data else {
                    unreachable!("vectored response owns vectored data")
                };
                VectoredWriteFailure::new(
                    failure.error,
                    request.segments,
                    failure.bytes_transferred,
                )
            })
        })
}

/// Owned simulation vectored response, using the common completion primitive.
pub type SimVectoredWriteOperation =
    crate::completion::LocalOperation<CompletionResult<VectoredWriteResult, VectoredWriteFailure>>;
/// Owned thread-safe vectored response, using the common completion primitive.
pub type MemoryVectoredWriteOperation =
    crate::completion::SyncOperation<CompletionResult<VectoredWriteResult, VectoredWriteFailure>>;

pub(super) mod cells;

#[cfg(test)]
mod tests;
