//! Passive owned send cursors and a bounded, policy-free connection driver.
//!
//! Enqueueing requests performs no I/O. Only `poll_event` crosses the warm
//! provider admission boundary, in a fixed read/write/close order; each response
//! is then retained through terminal completion. This is the owned equivalent
//! of first-poll admission through `ColdStream`, without self-referential futures.
//! Socket progress is never a Kafka acknowledgement. The engine interprets
//! matching response frames and decides retry, sequence, and delivery policy.

#[cfg(feature = "request-observation")]
mod observation;
#[cfg(feature = "request-observation")]
pub use observation::{RequestFinish, RequestObservation, RequestObserver};

use kr_kafka_protocol::{plan::SendPlan, wire::Error as WireError};
use kr_runtime::{CompletionCertainty, RuntimeInstant};
use kr_runtime_io::network::{
    ByteStreamVectoredSubmit, NetworkError, ReadRequest, ReadResult, VectoredWriteRequest,
    WriteRequest, WriteSegment,
};
use kr_shared_bytes::SharedBytes;
use std::{
    collections::VecDeque,
    fmt,
    future::Future,
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    task::{Context, Poll},
};

/// Passive owner-set admission fence. Existing admitted operations retain their
/// actual completion evidence; closing this only prevents a new request start.
#[derive(Clone, Debug)]
pub struct WriteFence(Arc<AtomicBool>);
impl Default for WriteFence {
    fn default() -> Self {
        Self::new()
    }
}
impl WriteFence {
    pub fn new() -> Self {
        Self(Arc::new(AtomicBool::new(true)))
    }
    pub fn close(&self) {
        self.0.store(false, Ordering::Release);
    }
    fn is_open(&self) -> bool {
        self.0.load(Ordering::Acquire)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PlanLimits {
    pub max_bytes: usize,
    pub max_segments: usize,
    pub max_coalesced_bytes: usize,
    pub coalesce_below_bytes: usize,
}
impl Default for PlanLimits {
    fn default() -> Self {
        Self {
            max_bytes: 1024 * 1024,
            max_segments: 196,
            max_coalesced_bytes: 1024 * 1024,
            coalesce_below_bytes: 512,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum TransportError {
    InvalidConfig(&'static str),
    ResourceExhausted {
        resource: &'static str,
        limit: usize,
    },
    AllocationFailed,
    InvalidStage,
    DuplicateCorrelation(i32),
    Closed,
    Protocol(WireError),
}
impl fmt::Display for TransportError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidConfig(field) => write!(f, "invalid transport configuration: {field}"),
            Self::ResourceExhausted { resource, limit } => {
                write!(f, "transport {resource} exceeds {limit}")
            }
            Self::AllocationFailed => f.write_str("transport allocation failed"),
            Self::InvalidStage => f.write_str("invalid or overlapping transport stage"),
            Self::DuplicateCorrelation(value) => write!(f, "duplicate pending correlation {value}"),
            Self::Closed => f.write_str("connection is retiring or released"),
            Self::Protocol(error) => error.fmt(f),
        }
    }
}
impl std::error::Error for TransportError {}
impl From<WireError> for TransportError {
    fn from(error: WireError) -> Self {
        Self::Protocol(error)
    }
}

/// Fully owned normalized immutable spans. Retained payload allocation identity
/// and attached lifetime guards survive conversion and staging unchanged.
#[derive(Clone, Debug)]
pub struct OwnedSendPlan {
    segments: Vec<SharedBytes>,
    len: usize,
    coalesced_bytes: usize,
    metadata_bytes: usize,
    write_fences: Vec<WriteFence>,
}
impl OwnedSendPlan {
    /// Owns one complete control request without another payload copy.
    ///
    /// # Errors
    /// Rejects zero/oversized frames and inconsistent signed Kafka prefixes.
    pub fn from_frame(frame: Vec<u8>, max_bytes: usize) -> Result<Self, TransportError> {
        if frame.len() < 8 || frame.len() > max_bytes || frame.capacity() > max_bytes {
            return Err(TransportError::ResourceExhausted {
                resource: "control frame bytes",
                limit: max_bytes,
            });
        }
        let length = i32::from_be_bytes(frame[..4].try_into().expect("checked frame prefix"));
        if length < 0 || length as usize != frame.len() - 4 {
            return Err(TransportError::Protocol(WireError::InvalidValue(
                "control frame length",
            )));
        }
        let len = frame.len();
        let metadata_bytes = frame.capacity();
        let mut segments = Vec::new();
        segments
            .try_reserve_exact(1)
            .map_err(|_| TransportError::AllocationFailed)?;
        segments.push(SharedBytes::from(frame));
        Ok(Self {
            segments,
            len,
            coalesced_bytes: 0,
            metadata_bytes,
            write_fences: Vec::new(),
        })
    }
    /// # Errors
    /// Rejects oversized plans, borrowed payloads, coalescing overflow, or more
    /// than the explicit `max_segments` limit after normalization.
    pub fn from_protocol(
        plan: SendPlan<'static>,
        limits: PlanLimits,
    ) -> Result<Self, TransportError> {
        if limits.max_bytes == 0 {
            return Err(TransportError::InvalidConfig("max_bytes"));
        }
        if limits.max_segments == 0 {
            return Err(TransportError::InvalidConfig("max_segments"));
        }
        let max_segments = limits.max_segments;
        let len = plan.len();
        if len == 0 || len > limits.max_bytes || len > u32::MAX as usize {
            return Err(TransportError::ResourceExhausted {
                resource: "request bytes",
                limit: limits.max_bytes,
            });
        }
        let metadata_bytes = plan.metadata_capacity();
        let source = plan.into_shared_segments()?;
        let mut segments = Vec::new();
        segments
            .try_reserve_exact(source.len().min(max_segments))
            .map_err(|_| TransportError::AllocationFailed)?;
        let mut coalesced_bytes = 0usize;
        let mut source = source.into_iter().peekable();
        while let Some(span) = source.next() {
            if segments.len() == max_segments {
                return Err(TransportError::ResourceExhausted {
                    resource: "plan segments",
                    limit: max_segments,
                });
            }
            if span.len() < limits.coalesce_below_bytes
                && source
                    .peek()
                    .is_some_and(|next| next.len() < limits.coalesce_below_bytes)
            {
                let mut bytes = Vec::new();
                let mut append = |span: SharedBytes| -> Result<(), TransportError> {
                    coalesced_bytes = coalesced_bytes
                        .checked_add(span.len())
                        .ok_or(TransportError::InvalidStage)?;
                    if coalesced_bytes > limits.max_coalesced_bytes {
                        return Err(TransportError::ResourceExhausted {
                            resource: "coalesced bytes",
                            limit: limits.max_coalesced_bytes,
                        });
                    }
                    bytes
                        .try_reserve_exact(span.len())
                        .map_err(|_| TransportError::AllocationFailed)?;
                    bytes.extend_from_slice(span.as_slice());
                    Ok(())
                };
                append(span)?;
                while source
                    .peek()
                    .is_some_and(|next| next.len() < limits.coalesce_below_bytes)
                {
                    if let Some(next) = source.next() {
                        append(next)?;
                    }
                }
                segments.push(SharedBytes::from(bytes));
            } else {
                segments.push(span);
            }
        }
        Ok(Self {
            segments,
            len,
            coalesced_bytes,
            metadata_bytes,
            write_fences: Vec::new(),
        })
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
    pub fn segments(&self) -> &[SharedBytes] {
        &self.segments
    }
    #[must_use]
    pub const fn coalesced_bytes(&self) -> usize {
        self.coalesced_bytes
    }
    #[must_use]
    pub const fn metadata_bytes(&self) -> usize {
        self.metadata_bytes
    }
    /// Attaches the caller's bounded passive admission-fence set before enqueue.
    /// Fences never revoke an already admitted provider operation. Once a
    /// nonempty set is attached, it cannot be removed or replaced by another
    /// caller holding an owned plan or a clone of it. No allocation is performed.
    ///
    /// # Errors
    /// Returns the supplied vector unchanged when fences are already attached.
    pub fn retain_write_fences(&mut self, fences: Vec<WriteFence>) -> Result<(), Vec<WriteFence>> {
        if !self.write_fences.is_empty() {
            return Err(fences);
        }
        self.write_fences = fences;
        Ok(())
    }
    fn writes_allowed(&self) -> bool {
        self.write_fences.iter().all(WriteFence::is_open)
    }
    /// Retains the request's metadata reservation on every span that does not
    /// already carry a payload reservation. Existing guards are never replaced.
    pub fn retain_metadata_guard(&mut self, guard: std::sync::Arc<dyn Send + Sync>) {
        for span in &mut self.segments {
            *span = span
                .clone()
                .attach_guard(guard.clone())
                .unwrap_or_else(|original| original);
        }
    }
    #[must_use]
    pub fn into_cursor(self) -> PlanCursor {
        PlanCursor {
            plan: self,
            segment: 0,
            offset: 0,
            confirmed: 0,
            staged: None,
        }
    }
}

/// Owns its plan, so retained operations never borrow this cursor. A new stage
/// cannot be built until the previous stage receives an exact terminal count.
#[derive(Debug)]
pub struct PlanCursor {
    plan: OwnedSendPlan,
    segment: usize,
    offset: usize,
    confirmed: usize,
    staged: Option<usize>,
}
impl PlanCursor {
    #[must_use]
    pub const fn confirmed(&self) -> usize {
        self.confirmed
    }
    #[must_use]
    pub const fn remaining(&self) -> usize {
        self.plan.len - self.confirmed
    }
    #[must_use]
    pub const fn staged_len(&self) -> Option<usize> {
        self.staged
    }
    #[must_use]
    pub fn plan(&self) -> &OwnedSendPlan {
        &self.plan
    }

    /// Packs from the confirmed cursor and clears the reusable buffer first.
    /// Unwritten suffixes are repacked from the plan on the next call.
    ///
    /// # Errors
    /// Rejects zero bounds, a completed cursor, or an outstanding stage.
    pub fn stage_contiguous(
        &mut self,
        buffer: &mut Vec<u8>,
        maximum: usize,
    ) -> Result<usize, TransportError> {
        self.check_stage(maximum)?;
        let take = self.remaining().min(maximum);
        buffer.clear();
        if buffer.capacity() < take {
            buffer
                .try_reserve_exact(take)
                .map_err(|_| TransportError::AllocationFailed)?;
        }
        let mut offset = self.offset;
        for span in &self.plan.segments[self.segment..] {
            let count = (take - buffer.len()).min(span.len() - offset);
            buffer.extend_from_slice(&span.as_slice()[offset..offset + count]);
            offset = 0;
            if buffer.len() == take {
                break;
            }
        }
        self.staged = Some(take);
        Ok(take)
    }

    /// Builds owned shared subranges, charging complete distinct backing
    /// allocations against the same provider bound as payload bytes.
    ///
    /// # Errors
    /// Rejects impossible bounds or an allocation too large for one operation.
    pub fn stage_vectored(
        &mut self,
        maximum: usize,
        max_segments: usize,
    ) -> Result<VectoredWriteRequest, TransportError> {
        self.check_stage(maximum)?;
        if max_segments == 0 {
            return Err(TransportError::InvalidConfig("max_segments"));
        }
        let mut segments: Vec<WriteSegment> = Vec::new();
        segments
            .try_reserve_exact(max_segments.min(self.plan.segments.len() - self.segment))
            .map_err(|_| TransportError::AllocationFailed)?;
        let mut offset = self.offset;
        let mut len = 0;
        let mut retained = 0usize;
        for span in &self.plan.segments[self.segment..] {
            let new_allocation = !segments
                .iter()
                .any(|segment| segment.bytes.shares_allocation(span));
            let next_retained = retained
                .checked_add(if new_allocation {
                    span.allocation_len()
                } else {
                    0
                })
                .ok_or(TransportError::InvalidStage)?;
            if next_retained > maximum || segments.len() == max_segments || len == maximum {
                break;
            }
            let count = (maximum - len).min(span.len() - offset);
            segments.push(WriteSegment {
                bytes: span.clone(),
                range: u32::try_from(offset).map_err(|_| TransportError::InvalidStage)?
                    ..u32::try_from(offset + count).map_err(|_| TransportError::InvalidStage)?,
            });
            len += count;
            retained = next_retained;
            offset = 0;
        }
        if segments.is_empty() {
            return Err(TransportError::ResourceExhausted {
                resource: "retained write allocation",
                limit: maximum,
            });
        }
        self.staged = Some(len);
        Ok(VectoredWriteRequest { segments })
    }

    fn check_stage(&self, maximum: usize) -> Result<(), TransportError> {
        if maximum == 0 || self.remaining() == 0 || self.staged.is_some() {
            Err(TransportError::InvalidStage)
        } else {
            Ok(())
        }
    }
    /// # Errors
    /// Rejects a missing stage or progress beyond its submitted prefix.
    pub fn confirm(&mut self, bytes: usize) -> Result<(), TransportError> {
        let staged = self.staged.ok_or(TransportError::InvalidStage)?;
        if bytes > staged {
            return Err(TransportError::InvalidStage);
        }
        let mut remaining = bytes;
        while remaining != 0 {
            let available = self.plan.segments[self.segment].len() - self.offset;
            if remaining < available {
                self.offset += remaining;
                remaining = 0;
            } else {
                remaining -= available;
                self.segment += 1;
                self.offset = 0;
            }
        }
        self.confirmed += bytes;
        self.staged = None;
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum WriteMode {
    Staging,
    Vectored,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DriverConfig {
    pub mode: WriteMode,
    pub staging_bytes: usize,
    pub max_operation_bytes: usize,
    pub max_inflight_requests: usize,
    /// Total frame size including its four-byte length prefix.
    pub rx_bytes: usize,
}
impl Default for DriverConfig {
    fn default() -> Self {
        Self {
            mode: WriteMode::Staging,
            staging_bytes: 256 * 1024,
            max_operation_bytes: 1024 * 1024,
            max_inflight_requests: 5,
            rx_bytes: 1024 * 1024,
        }
    }
}

#[derive(Debug)]
pub struct SendRequest {
    pub correlation: i32,
    pub deadline: RuntimeInstant,
    pub plan: OwnedSendPlan,
}
#[derive(Debug)]
pub struct RejectedRequest {
    pub error: TransportError,
    pub request: SendRequest,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RetireReason {
    Requested,
    Deadline { correlation: i32 },
    EndOfStream,
    Network(NetworkError),
    Protocol(WireError),
    Transport(TransportError),
}
#[derive(Debug)]
pub enum DriverEvent<'a> {
    /// First write submission has been invoked. Its retained response may still
    /// prove NotApplied; until then a deadline cannot assume the request unsent.
    WriteAdmitted {
        correlation: i32,
    },
    WriteProgress {
        correlation: i32,
        bytes: usize,
        confirmed: usize,
        certainty: CompletionCertainty,
    },
    /// Exact complete Kafka frame, including length prefix. This borrow expires
    /// before the next driver poll reuses the sole RX allocation.
    Frame {
        correlation: i32,
        bytes: &'a [u8],
    },
    Retiring {
        reason: RetireReason,
    },
    RequestRetired {
        correlation: i32,
        confirmed: usize,
        certainty: CompletionCertainty,
    },
    Released,
}

struct Outbound {
    correlation: i32,
    deadline: RuntimeInstant,
    cursor: PlanCursor,
    certainty: CompletionCertainty,
    admitted: bool,
}
enum WriteOperation<S: ByteStreamVectoredSubmit> {
    Staging(Pin<Box<S::WriteResponse>>),
    Vectored(Pin<Box<S::WriteVectoredResponse>>),
}
struct PendingWrite<S: ByteStreamVectoredSubmit> {
    correlation: i32,
    operation: WriteOperation<S>,
}

/// Owner-driven connection transport. Dropping the driver is abortive handle
/// teardown, not cooperative completion: providers retain admitted ownership.
/// Call `retire` and poll through `Released` before releasing connection credits.
pub struct ConnectionDriver<S: ByteStreamVectoredSubmit> {
    #[cfg(feature = "request-observation")]
    observer: Option<Arc<dyn RequestObserver>>,
    stream: Option<S>,
    config: DriverConfig,
    requests: VecDeque<Outbound>,
    read: Option<Pin<Box<S::ReadResponse>>>,
    read_prefix: usize,
    read_max: usize,
    write: Option<PendingWrite<S>>,
    close: Option<Pin<Box<S::ControlResponse>>>,
    close_terminal: bool,
    rx: Option<Vec<u8>>,
    rx_expected: usize,
    frame_presented: bool,
    staging: Option<Vec<u8>>,
    staged_copies: u64,
    retiring: Option<RetireReason>,
    retiring_presented: bool,
    released: bool,
}

impl<S: ByteStreamVectoredSubmit> ConnectionDriver<S> {
    /// # Errors
    /// Rejects invalid retained-capacity bounds and allocation failure. No I/O
    /// is admitted during construction, including the initial read.
    pub fn new(stream: S, config: DriverConfig) -> Result<Self, TransportError> {
        if config.max_inflight_requests == 0 {
            return Err(TransportError::InvalidConfig(
                "max_inflight_requests must be nonzero",
            ));
        }
        if config.rx_bytes < 8 || config.rx_bytes > config.max_operation_bytes {
            return Err(TransportError::InvalidConfig(
                "rx_bytes must be 8..=max_operation_bytes",
            ));
        }
        if config.staging_bytes == 0 || config.staging_bytes > config.max_operation_bytes {
            return Err(TransportError::InvalidConfig(
                "staging_bytes must be 1..=max_operation_bytes",
            ));
        }
        if config.mode == WriteMode::Vectored && stream.max_segments() == 0 {
            return Err(TransportError::InvalidConfig("provider max_segments"));
        }
        let mut rx = Vec::new();
        rx.try_reserve_exact(config.rx_bytes)
            .map_err(|_| TransportError::AllocationFailed)?;
        let mut staging = Vec::new();
        if config.mode == WriteMode::Staging {
            staging
                .try_reserve_exact(config.staging_bytes)
                .map_err(|_| TransportError::AllocationFailed)?;
        }
        let mut requests = VecDeque::new();
        requests
            .try_reserve_exact(config.max_inflight_requests)
            .map_err(|_| TransportError::AllocationFailed)?;
        Ok(Self {
            #[cfg(feature = "request-observation")]
            observer: None,
            stream: Some(stream),
            config,
            requests,
            read: None,
            read_prefix: 0,
            read_max: 0,
            write: None,
            close: None,
            close_terminal: false,
            rx: Some(rx),
            rx_expected: 4,
            frame_presented: false,
            staging: Some(staging),
            staged_copies: 0,
            retiring: None,
            retiring_presented: false,
            released: false,
        })
    }

    /// Installs a passive sink before any requests or I/O are admitted.
    /// # Errors
    /// Rejects replacement or installation after the driver has started.
    #[cfg(feature = "request-observation")]
    pub fn set_request_observer(
        &mut self,
        observer: Arc<dyn RequestObserver>,
    ) -> Result<(), TransportError> {
        if self.observer.is_some()
            || !self.requests.is_empty()
            || self.read.is_some()
            || self.write.is_some()
            || self.retiring.is_some()
            || self.released
        {
            return Err(TransportError::InvalidStage);
        }
        self.observer = Some(observer);
        Ok(())
    }

    /// # Errors
    /// Returns the entire request on closed/full/duplicate admission rejection.
    pub fn enqueue(&mut self, request: SendRequest) -> Result<(), RejectedRequest> {
        let error = if self.retiring.is_some() || self.released {
            Some(TransportError::Closed)
        } else if self.requests.len() == self.config.max_inflight_requests {
            Some(TransportError::ResourceExhausted {
                resource: "inflight requests",
                limit: self.config.max_inflight_requests,
            })
        } else if self
            .requests
            .iter()
            .any(|pending| pending.correlation == request.correlation)
        {
            Some(TransportError::DuplicateCorrelation(request.correlation))
        } else if self.config.mode == WriteMode::Vectored
            && request
                .plan
                .segments
                .iter()
                .any(|span| span.allocation_len() > self.config.max_operation_bytes)
        {
            Some(TransportError::ResourceExhausted {
                resource: "retained write allocation",
                limit: self.config.max_operation_bytes,
            })
        } else {
            None
        };
        if let Some(error) = error {
            return Err(RejectedRequest { error, request });
        }
        self.requests.push_back(Outbound {
            correlation: request.correlation,
            deadline: request.deadline,
            cursor: request.plan.into_cursor(),
            certainty: CompletionCertainty::NotApplied,
            admitted: false,
        });
        Ok(())
    }

    /// Starts retirement without dropping or replacing any admitted operation.
    pub fn retire(&mut self, reason: RetireReason) {
        if self.retiring.is_none() {
            self.retiring = Some(reason);
        }
    }
    #[must_use]
    pub fn is_retiring(&self) -> bool {
        self.retiring.is_some()
    }
    #[must_use]
    pub const fn is_released(&self) -> bool {
        self.released
    }
    #[must_use]
    pub fn pending_requests(&self) -> usize {
        self.requests.len()
    }
    #[must_use]
    pub const fn staged_copies(&self) -> u64 {
        self.staged_copies
    }
    #[must_use]
    pub fn next_deadline(&self) -> Option<RuntimeInstant> {
        if self.retiring.is_some() {
            None
        } else {
            self.requests.iter().map(|request| request.deadline).min()
        }
    }

    /// Processes bounded completion work and returns at most one event. At most
    /// two read/write polls and one close poll occur per call. `Ready(None)` means Released
    /// was already delivered. The owner must poll again after an event; if its
    /// own quota expires it must arrange a self-wake. Deadlines use caller time.
    pub fn poll_event(
        &mut self,
        cx: &mut Context<'_>,
        now: RuntimeInstant,
    ) -> Poll<Option<DriverEvent<'_>>> {
        if self.released {
            return Poll::Ready(None);
        }
        if self.frame_presented {
            if let Some(rx) = &mut self.rx {
                rx.clear();
            }
            self.rx_expected = 4;
            self.frame_presented = false;
        }
        // Existing completions precede deadline decisions. Reads are retained
        // across every retirement path, including an idle read with no requests.
        self.poll_read(cx);
        if let Some(event) = self.poll_write(cx, now) {
            return Poll::Ready(Some(event));
        }
        self.poll_close(cx);
        if self.complete_frame_ready()
            && let Some(correlation) = self.take_frame(now)
        {
            return Poll::Ready(Some(DriverEvent::Frame {
                correlation,
                bytes: self
                    .rx
                    .as_ref()
                    .expect("validated frame owns RX buffer")
                    .as_slice(),
            }));
        }
        if self.retiring.is_none()
            && let Some(correlation) = self
                .requests
                .iter()
                .find(|request| now >= request.deadline)
                .map(|request| request.correlation)
        {
            self.retire(RetireReason::Deadline { correlation });
        }
        if self.retiring.is_some() {
            self.arm_close();
            if !self.retiring_presented {
                self.retiring_presented = true;
                return Poll::Ready(Some(DriverEvent::Retiring {
                    reason: self.retiring.clone().expect("retirement checked"),
                }));
            }
            if self.read.is_none() && self.write.is_none() && self.close_terminal {
                if let Some(request) = self.requests.pop_front() {
                    #[cfg(feature = "request-observation")]
                    if let Some(observer) = &self.observer {
                        observer.observe(
                            now,
                            RequestObservation::Finished {
                                correlation: request.correlation,
                                dispatched: request.admitted,
                                confirmed: request.cursor.confirmed(),
                                certainty: request.certainty,
                                result: RequestFinish::Retired(
                                    self.retiring.as_ref().expect("retiring driver"),
                                ),
                            },
                        );
                    }
                    return Poll::Ready(Some(DriverEvent::RequestRetired {
                        correlation: request.correlation,
                        confirmed: request.cursor.confirmed(),
                        certainty: request.certainty,
                    }));
                }
                self.rx = None;
                self.staging = None;
                self.released = true;
                return Poll::Ready(Some(DriverEvent::Released));
            }
            return Poll::Pending;
        }
        self.arm_read();
        if let Some(correlation) = self.arm_write(now) {
            return Poll::Ready(Some(DriverEvent::WriteAdmitted { correlation }));
        }
        // Explicit first-poll order for newly admitted operations. Each is
        // polled here so a cold owner never returns Pending with an unarmed wake.
        self.poll_read(cx);
        if let Some(event) = self.poll_write(cx, now) {
            return Poll::Ready(Some(event));
        }
        let frame_waits_for_write = self.complete_frame_ready()
            && self.requests.front().is_some_and(|request| {
                self.write
                    .as_ref()
                    .is_some_and(|write| write.correlation == request.correlation)
            });
        if (self.read.is_none() && !frame_waits_for_write) || self.retiring.is_some() {
            cx.waker().wake_by_ref();
        }
        Poll::Pending
    }

    fn arm_read(&mut self) {
        if self.read.is_some() || self.retiring.is_some() || self.complete_frame_ready() {
            return;
        }
        let Some(buffer) = self.rx.take() else {
            return;
        };
        let Some(stream) = &self.stream else {
            self.rx = Some(buffer);
            return;
        };
        self.read_prefix = buffer.len();
        self.read_max = self.rx_expected - buffer.len();
        self.read = Some(Box::pin(stream.submit_read(ReadRequest {
            buffer,
            max_bytes: self.read_max,
        })));
    }

    fn poll_read(&mut self, cx: &mut Context<'_>) {
        let Some(read) = &mut self.read else {
            return;
        };
        let Poll::Ready(result) = read.as_mut().poll(cx) else {
            return;
        };
        self.read = None;
        match result {
            Ok(ReadResult {
                buffer,
                bytes_read,
                end_of_stream,
            }) => {
                if bytes_read > self.read_max
                    || buffer.len() != self.read_prefix + bytes_read
                    || buffer.capacity() > self.config.rx_bytes
                    || (end_of_stream && bytes_read != 0)
                {
                    self.retire(RetireReason::Protocol(WireError::InvalidValue(
                        "invalid read completion",
                    )));
                } else if end_of_stream || bytes_read == 0 {
                    self.retire(RetireReason::EndOfStream);
                }
                self.rx = Some(buffer);
                self.read_frame_length();
            }
            Err(error) => {
                let (_, failure) = error.into_parts();
                self.retire(RetireReason::Network(failure.error().clone()));
                self.rx = failure.into_buffer();
            }
        }
    }

    fn read_frame_length(&mut self) {
        if self.rx_expected != 4 {
            return;
        }
        let Some(buffer) = &self.rx else {
            return;
        };
        if buffer.len() != 4 {
            return;
        }
        let declared = i32::from_be_bytes([buffer[0], buffer[1], buffer[2], buffer[3]]);
        if declared < 4 {
            self.retire(RetireReason::Protocol(WireError::InvalidLength {
                value: i64::from(declared),
            }));
        } else {
            let total = declared as usize + 4;
            if total > self.config.rx_bytes {
                self.retire(RetireReason::Protocol(WireError::ResourceExhausted {
                    resource: "response frame bytes",
                    limit: self.config.rx_bytes,
                }));
            } else {
                self.rx_expected = total;
            }
        }
    }

    fn complete_frame_ready(&self) -> bool {
        self.rx_expected >= 8
            && self
                .rx
                .as_ref()
                .is_some_and(|buffer| buffer.len() == self.rx_expected)
    }
    fn take_frame(&mut self, _now: RuntimeInstant) -> Option<i32> {
        let buffer = self.rx.as_ref()?;
        let actual = i32::from_be_bytes([buffer[4], buffer[5], buffer[6], buffer[7]]);
        let Some(request) = self.requests.front() else {
            self.retire(RetireReason::Protocol(WireError::InvalidValue(
                "response without pending request",
            )));
            return None;
        };
        if actual != request.correlation {
            let expected = request.correlation;
            for request in &mut self.requests {
                request.certainty = CompletionCertainty::MayHaveApplied;
            }
            self.retire(RetireReason::Protocol(WireError::CorrelationMismatch {
                expected,
                actual,
            }));
            return None;
        }
        // A completion notification can lag the peer's response. Keep the sole
        // frame arena until the head's write notification is terminal.
        if self
            .write
            .as_ref()
            .is_some_and(|write| write.correlation == actual)
        {
            return None;
        }
        if request.cursor.remaining() != 0 {
            self.retire(RetireReason::Protocol(WireError::InvalidValue(
                "response before complete request",
            )));
            return None;
        }
        #[cfg(feature = "request-observation")]
        if let Some(observer) = &self.observer {
            observer.observe(
                _now,
                RequestObservation::Finished {
                    correlation: actual,
                    dispatched: request.admitted,
                    confirmed: request.cursor.confirmed(),
                    certainty: request.certainty,
                    result: RequestFinish::Response,
                },
            );
        }
        self.requests.pop_front();
        self.frame_presented = true;
        Some(actual)
    }

    fn arm_write(&mut self, _now: RuntimeInstant) -> Option<i32> {
        if self.write.is_some() || self.retiring.is_some() {
            return None;
        }
        let request = self
            .requests
            .iter_mut()
            .find(|request| request.cursor.remaining() != 0)?;
        if !request.admitted && !request.cursor.plan().writes_allowed() {
            self.retire(RetireReason::Requested);
            return None;
        }
        let Some(stream) = &self.stream else {
            return None;
        };
        let operation = match self.config.mode {
            WriteMode::Staging => {
                let mut buffer = self.staging.take()?;
                match request
                    .cursor
                    .stage_contiguous(&mut buffer, self.config.staging_bytes)
                {
                    Ok(len) => {
                        let Some(copies) = self.staged_copies.checked_add(len as u64) else {
                            self.staging = Some(buffer);
                            self.retire(RetireReason::Transport(TransportError::InvalidStage));
                            return None;
                        };
                        self.staged_copies = copies;
                        #[cfg(feature = "request-observation")]
                        if !request.admitted
                            && let Some(observer) = &self.observer
                        {
                            observer.observe(
                                _now,
                                RequestObservation::Dispatched {
                                    correlation: request.correlation,
                                    plan: request.cursor.plan(),
                                },
                            );
                        }
                        WriteOperation::Staging(Box::pin(
                            stream.submit_write(WriteRequest { buffer }),
                        ))
                    }
                    Err(error) => {
                        self.staging = Some(buffer);
                        self.retire(RetireReason::Transport(error));
                        return None;
                    }
                }
            }
            WriteMode::Vectored => match request
                .cursor
                .stage_vectored(self.config.max_operation_bytes, stream.max_segments())
            {
                Ok(stage) => {
                    #[cfg(feature = "request-observation")]
                    if !request.admitted
                        && let Some(observer) = &self.observer
                    {
                        observer.observe(
                            _now,
                            RequestObservation::Dispatched {
                                correlation: request.correlation,
                                plan: request.cursor.plan(),
                            },
                        );
                    }
                    WriteOperation::Vectored(Box::pin(stream.submit_write_vectored(stage)))
                }
                Err(error) => {
                    self.retire(RetireReason::Transport(error));
                    return None;
                }
            },
        };
        self.write = Some(PendingWrite {
            correlation: request.correlation,
            operation,
        });
        let newly_admitted = !request.admitted;
        request.admitted = true;
        newly_admitted.then_some(request.correlation)
    }

    fn poll_write(
        &mut self,
        cx: &mut Context<'_>,
        _now: RuntimeInstant,
    ) -> Option<DriverEvent<'static>> {
        let pending = self.write.as_mut()?;
        let (bytes, certainty, error) = match &mut pending.operation {
            WriteOperation::Staging(future) => match future.as_mut().poll(cx) {
                Poll::Pending => return None,
                Poll::Ready(Ok(result)) => {
                    self.staging = Some(result.buffer);
                    (result.bytes_written, CompletionCertainty::Applied, None)
                }
                Poll::Ready(Err(error)) => {
                    let (certainty, failure) = error.into_parts();
                    let bytes = failure.bytes_transferred();
                    let error = failure.error().clone();
                    self.staging = failure.into_buffer();
                    (bytes, certainty, Some(error))
                }
            },
            WriteOperation::Vectored(future) => match future.as_mut().poll(cx) {
                Poll::Pending => return None,
                Poll::Ready(Ok(result)) => {
                    (result.bytes_written, CompletionCertainty::Applied, None)
                }
                Poll::Ready(Err(error)) => {
                    let (certainty, failure) = error.into_parts();
                    (failure.bytes_transferred, certainty, Some(failure.error))
                }
            },
        };
        let correlation = pending.correlation;
        self.write = None;
        let request = self
            .requests
            .iter_mut()
            .find(|request| request.correlation == correlation)?;
        request.certainty = merge_certainty(request.certainty, certainty);
        if bytes != 0 {
            request.certainty = merge_certainty(request.certainty, CompletionCertainty::Applied);
        }
        let valid = request.cursor.confirm(bytes).is_ok()
            && !(certainty == CompletionCertainty::NotApplied && bytes != 0);
        let confirmed = request.cursor.confirmed();
        let sticky = request.certainty;
        #[cfg(feature = "request-observation")]
        if valid
            && error.is_none()
            && request.cursor.remaining() == 0
            && let Some(observer) = &self.observer
        {
            observer.observe(_now, RequestObservation::WriteCompleted { correlation });
        }
        if !valid || (bytes == 0 && error.is_none()) {
            self.retire(RetireReason::Protocol(WireError::InvalidValue(
                "invalid write completion",
            )));
        } else if let Some(error) = error {
            self.retire(RetireReason::Network(error));
        }
        Some(DriverEvent::WriteProgress {
            correlation,
            bytes,
            confirmed,
            certainty: sticky,
        })
    }

    fn arm_close(&mut self) {
        if self.close.is_none()
            && !self.close_terminal
            && let Some(stream) = &self.stream
        {
            self.close = Some(Box::pin(stream.submit_close()));
        }
    }
    fn poll_close(&mut self, cx: &mut Context<'_>) {
        let Some(close) = &mut self.close else {
            return;
        };
        if close.as_mut().poll(cx).is_ready() {
            self.close = None;
            self.close_terminal = true;
            // An unsuccessful explicit close still gets the provider's normal
            // exclusive-handle drop teardown; admitted responses remain retained.
            self.stream = None;
        }
    }
}

fn merge_certainty(
    previous: CompletionCertainty,
    next: CompletionCertainty,
) -> CompletionCertainty {
    use CompletionCertainty::{Applied, MayHaveApplied, NotApplied};
    match (previous, next) {
        (MayHaveApplied, _) | (_, MayHaveApplied) => MayHaveApplied,
        (Applied, _) | (_, Applied) => Applied,
        _ => NotApplied,
    }
}

#[cfg(test)]
mod tests;
