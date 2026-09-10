//! Single-file owned asynchronous I/O and its deterministic simulation.

use std::cell::RefCell;
use std::collections::VecDeque;
use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::rc::Rc;
use std::task::{Context, Poll, Waker};

use crate::completion::{LocalAdmission, LocalPermitPool};
use crate::latency::{SimLatency, SimLatencyError, SimLatencyModel};
use kr_runtime::rng::RandomStream;
use kr_runtime::{
    CompletionCertainty, CompletionError, CompletionResult, Handle, RandomHandle, SimDuration,
    SpawnError,
};

mod cold;
mod memory;

pub use cold::ColdFile;
pub use memory::{MemoryFile, MemoryFileConfig, MemoryFileOpenError, MemoryFileStatus};

#[cfg(any(test, feature = "test-support"))]
pub mod test_support;

/// A positional file operation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StorageOperation {
    ReadAt,
    WriteAt,
    SetLen,
    Len,
    Sync,
}

impl StorageOperation {
    const COUNT: usize = 5;

    const fn index(self) -> usize {
        match self {
            Self::ReadAt => 0,
            Self::WriteAt => 1,
            Self::SetLen => 2,
            Self::Len => 3,
            Self::Sync => 4,
        }
    }
}

/// An I/O request failure.
#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum StorageError {
    RequestTooLarge {
        operation: StorageOperation,
        requested: usize,
        limit: usize,
    },
    FileTooLarge {
        requested: u64,
        limit: u64,
    },
    OffsetOverflow,
    /// A bounded resource cannot admit more work.
    ResourceExhausted {
        resource: &'static str,
        limit: usize,
    },
    Closed,
    Injected {
        operation: StorageOperation,
    },
    Backend {
        operation: StorageOperation,
        raw_os_error: Option<i32>,
        message: String,
    },
    DriverStopped,
    RecoveryRequired,
    RuntimeStopped,
}

impl fmt::Display for StorageError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::RequestTooLarge {
                operation,
                requested,
                limit,
            } => write!(
                formatter,
                "{operation:?} request of {requested} bytes exceeds limit {limit}"
            ),
            Self::FileTooLarge { requested, limit } => {
                write!(formatter, "file length {requested} exceeds limit {limit}")
            }
            Self::OffsetOverflow => formatter.write_str("file offset arithmetic overflowed"),
            Self::ResourceExhausted { resource, limit } => {
                write!(formatter, "{resource} limit of {limit} is exhausted")
            }
            Self::Closed => formatter.write_str("file session is closed"),
            Self::Injected { operation } => write!(formatter, "injected {operation:?} failure"),
            Self::Backend {
                operation,
                raw_os_error,
                message,
            } => match raw_os_error {
                Some(code) => write!(
                    formatter,
                    "{operation:?} backend failure (OS error {code}): {message}"
                ),
                None => write!(formatter, "{operation:?} backend failure: {message}"),
            },
            Self::DriverStopped => formatter.write_str("I/O driver stopped"),
            Self::RecoveryRequired => {
                formatter.write_str("I/O state is uncertain; reopen and recover before continuing")
            }
            Self::RuntimeStopped => formatter.write_str("simulation runtime stopped"),
        }
    }
}

impl std::error::Error for StorageError {}

/// An owned positional read request. `buffer.len()` is the maximum read size.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReadAtRequest {
    pub offset: u64,
    pub buffer: Vec<u8>,
}

impl ReadAtRequest {
    #[must_use]
    pub fn new(offset: u64, buffer: Vec<u8>) -> Self {
        Self { offset, buffer }
    }
}

/// A successful positional read. Only `bytes_read` bytes remain in `buffer`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReadAtSuccess {
    pub buffer: Vec<u8>,
    pub bytes_read: usize,
}

/// A failed positional read. Buffer ownership always returns to the caller.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReadAtFailure {
    pub error: StorageError,
    pub buffer: Vec<u8>,
    /// Bytes the provider confirms were transferred before the failure.
    ///
    /// This is a confirmed lower bound, not necessarily the operation's full
    /// effect. In particular, zero with `MayHaveApplied` certainty does not
    /// prove that no bytes were transferred.
    pub bytes_transferred: usize,
}

impl fmt::Display for ReadAtFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.error.fmt(formatter)
    }
}

impl std::error::Error for ReadAtFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.error)
    }
}

/// An owned positional write request.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WriteAtRequest {
    pub offset: u64,
    pub buffer: Vec<u8>,
}

impl WriteAtRequest {
    #[must_use]
    pub fn new(offset: u64, buffer: Vec<u8>) -> Self {
        Self { offset, buffer }
    }
}

/// A successful positional write.
///
/// `bytes_written` may be shorter than the request. Callers that require an
/// exact write must retry the unconsumed suffix at its advanced offset.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WriteAtSuccess {
    pub bytes_written: usize,
    pub buffer: Vec<u8>,
}

/// A failed positional write. Buffer ownership always returns to the caller.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WriteAtFailure {
    pub error: StorageError,
    pub buffer: Vec<u8>,
    /// Bytes the provider confirms were transferred before the failure.
    ///
    /// This is a confirmed lower bound, not necessarily the operation's full
    /// effect. In particular, zero with `MayHaveApplied` certainty does not
    /// prove that no bytes were transferred.
    pub bytes_transferred: usize,
}

impl fmt::Display for WriteAtFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.error.fmt(formatter)
    }
}

impl std::error::Error for WriteAtFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.error)
    }
}

/// The accepted file length observed at this operation's ordered turn.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct FileLength {
    pub len: u64,
}

/// A successful length change.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SetLenSuccess {
    pub len: u64,
}

/// A successful durability fence.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SyncSuccess {
    pub durable_len: u64,
}

/// Diagnostics for a simulated open-file session.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct FileIoStatus {
    pub accepted_len: u64,
    pub durable_len: u64,
    /// Version of the simulated fsync-gate semantics.
    pub fsync_gate_version: u32,
    /// Accepted bytes or length metadata currently excluded from the next sync.
    pub has_fsync_gated_data: bool,
    pub in_flight: usize,
    pub in_flight_limit: usize,
    /// Caller-owned bytes held by admitted operations.
    pub outstanding_bytes: usize,
    pub outstanding_bytes_limit: usize,
    /// Fault plans not yet assigned to an admitted operation.
    pub pending_faults: usize,
    /// Fault plans assigned to admitted operations so far.
    pub fault_hits: u64,
    /// Responses delivered while an earlier-admitted operation was still in
    /// flight. Always zero under [`SimPipelineModel::Serial`]; a campaign
    /// that enables overlap gates on this to prove reordering was exercised.
    pub reordered_completions: u64,
    pub closed: bool,
}

/// Deterministic policy used to resolve unsynced bytes during process loss.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum SimCrashModel {
    /// Discard every unsynced byte and length change.
    CleanRollback,
    /// FoundationDB-inspired sector survival and tearing policy, version 1.
    ///
    /// Dirty 512-byte sectors independently survive with 10% probability. A
    /// surviving sector may be torn and garbage-filled, and an unsynced length
    /// change survives independently with 50% probability. All choices consume
    /// the runtime's versioned `Fault` random stream.
    FoundationDbLikeV1,
}

/// Failure to apply a requested simulated crash policy.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum SimCrashError {
    /// The session was not opened with a dedicated fault random source.
    MissingFaultRandom,
}

impl fmt::Display for SimCrashError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MissingFaultRandom => {
                formatter.write_str("crash model requires a runtime Fault random source")
            }
        }
    }
}

impl std::error::Error for SimCrashError {}

/// Warm submission boundary for owned positional I/O on one open file.
///
/// This is the provider-facing eager contract: a `submit_*` call attempts
/// validation and bounded admission synchronously during the method
/// invocation, and the returned response future is a completion ticket for an
/// admission attempt that has already happened. Application code should use
/// [`ColdFile`], which defers admission to a future's first poll; this trait
/// is for provider implementations and systems code that intentionally needs
/// explicit eager submission, such as `kr-runtime-ring`.
///
/// Calls successfully admitted through one handle must have effects equivalent
/// to execution in method-invocation order. A provider may physically overlap
/// operations that commute, such as reads or non-overlapping positional writes,
/// and may complete overlapped operations out of admission order.
/// [`SimStorage`] exercises that freedom under
/// [`SimPipelineModel::CommutingOverlapV1`], so consumers can be driven
/// against reordered commuting completions deterministically.
/// `submit_sync` is still a fence for all earlier admitted writes and length
/// changes, even when their response futures are never polled or are dropped.
/// Every response future is owned and `'static`; dropping one abandons the
/// response, never the admitted operation.
///
/// [`SimStorage`] can additionally script the Linux writeback-error case where
/// an ambiguous failed sync removes old dirty pages from later fence attempts.
/// That exceptional eligibility state is explicit in [`SimFault`] and
/// [`FileIoStatus`]; production providers may instead poison the open session.
///
/// Backpressure and validation failures are returned with `NotApplied`
/// certainty. A failed operation with `MayHaveApplied` must be reconciled by a
/// higher layer before blindly retrying a non-idempotent action.
pub trait FileIoSubmit: Clone + 'static {
    type ReadAtResponse: Future<Output = CompletionResult<ReadAtSuccess, ReadAtFailure>> + 'static;
    type WriteAtResponse: Future<Output = CompletionResult<WriteAtSuccess, WriteAtFailure>>
        + 'static;
    type SetLenResponse: Future<Output = CompletionResult<SetLenSuccess, StorageError>> + 'static;
    type LenResponse: Future<Output = CompletionResult<FileLength, StorageError>> + 'static;
    type SyncResponse: Future<Output = CompletionResult<SyncSuccess, StorageError>> + 'static;

    fn submit_read_at(&self, request: ReadAtRequest) -> Self::ReadAtResponse;
    fn submit_write_at(&self, request: WriteAtRequest) -> Self::WriteAtResponse;
    fn submit_set_len(&self, len: u64) -> Self::SetLenResponse;
    fn submit_len(&self) -> Self::LenResponse;
    fn submit_sync(&self) -> Self::SyncResponse;
}

/// A [`FileIoSubmit`] handle that may be shared by tasks on a multi-threaded
/// runtime.
///
/// This companion trait is implemented automatically when the handle is
/// [`Send`] and [`Sync`] and every owned response future is [`Send`]. Generic
/// production code should use this bound when its enclosing future may move
/// between executor threads. Local and deterministic code can continue to use
/// [`FileIoSubmit`] without paying for cross-thread synchronization.
pub trait SendFileIoSubmit:
    FileIoSubmit<
        ReadAtResponse: Send,
        WriteAtResponse: Send,
        SetLenResponse: Send,
        LenResponse: Send,
        SyncResponse: Send,
    > + Send
    + Sync
{
}

impl<T> SendFileIoSubmit for T where
    T: FileIoSubmit<
            ReadAtResponse: Send,
            WriteAtResponse: Send,
            SetLenResponse: Send,
            LenResponse: Send,
            SyncResponse: Send,
        > + Send
        + Sync
{
}

type ReadResult = CompletionResult<ReadAtSuccess, ReadAtFailure>;
type WriteResult = CompletionResult<WriteAtSuccess, WriteAtFailure>;
type SetLenResult = CompletionResult<SetLenSuccess, StorageError>;
type LenResult = CompletionResult<FileLength, StorageError>;
type SyncResult = CompletionResult<SyncSuccess, StorageError>;

/// Future returned by the deterministic storage implementation.
///
/// Polling again after terminal completion panics.
pub type SimOperation<T> = crate::completion::LocalOperation<T>;

type Responder<T> = Rc<crate::completion::LocalCell<T>>;

/// The version of the simulated commuting-overlap completion policy.
///
/// Incrementing this is a replay-breaking change: a recorded run's completion
/// order depends on which operations the worker overlaps and on how their
/// latencies interleave.
pub const SIM_PIPELINE_MODEL_VERSION: u32 = 1;

/// How [`SimStorage`] schedules admitted operations onto its virtual device.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
#[non_exhaustive]
pub enum SimPipelineModel {
    /// One operation at a time, strictly in admission order.
    ///
    /// This is the deterministic reference behavior: each operation's latency
    /// begins when the previous operation completes, so completion order
    /// always equals admission order.
    #[default]
    Serial,
    /// Overlap the commuting prefix of the admitted queue, version 1.
    ///
    /// Mirrors the commuting classes of the Linux file pipelines: reads run
    /// concurrently with reads, writes with non-overlapping writes, and
    /// `set_len`, `len`, and `sync` are fences that wait for the pipeline to
    /// drain and run alone. Every operation in a batch starts when the batch
    /// starts and completes after its own latency, so commuting operations
    /// complete in latency order rather than admission order — under a
    /// perturbing [`SimLatencyModel`] that order becomes a seed-dependent,
    /// replayable choice. The next batch starts only after every response in
    /// the previous batch has resolved, matching the per-file uring actor.
    CommutingOverlapV1,
}

/// Resource and latency bounds for [`SimStorage`].
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SimStorageConfig {
    pub max_file_bytes: usize,
    pub max_read_bytes: usize,
    pub max_write_bytes: usize,
    /// Maximum bytes one successful simulated read transfers.
    pub max_read_chunk: usize,
    /// Maximum bytes one successful simulated write transfers.
    pub max_write_chunk: usize,
    pub max_in_flight: usize,
    /// Maximum caller-owned bytes held by admitted operations at once.
    ///
    /// [`Self::max_in_flight`] bounds how many operations are admitted;
    /// this bounds how much memory they hold between them, which their
    /// product otherwise leaves unbounded in practice. Read and write
    /// operations charge the allocation they pin — their buffer's capacity,
    /// not just its length, so a pooled buffer charges its full pool size —
    /// and metadata operations charge nothing. A charge is released when the
    /// response is consumed or its abandoned output is discarded.
    ///
    /// The default is `max_in_flight` times the larger of
    /// [`Self::max_read_bytes`] and [`Self::max_write_bytes`], which is the
    /// worst case the other limits already permit. Lower it to make this the
    /// binding constraint.
    pub max_outstanding_bytes: usize,
    pub max_scripted_faults: usize,
    /// Completion latency applied to an operation with no scripted fault.
    pub default_latency: SimDuration,
    /// How [`Self::default_latency`] is perturbed per operation.
    ///
    /// A perturbing model requires a [`RandomStream::Schedule`] source passed
    /// to [`SimStorage::open_with_random_sources`]. Jitter applies only to
    /// this default latency; a scripted fault's explicit delay stays exact.
    pub latency_model: SimLatencyModel,
    /// How admitted operations are scheduled onto the virtual device.
    ///
    /// [`SimPipelineModel::CommutingOverlapV1`] lets commuting operations
    /// complete out of admission order, which [`FileIoSubmit`] permits and
    /// the Linux pipelines exploit. Fencing is unchanged in both models:
    /// `sync` still fences every earlier admitted write. Scripted fault
    /// delays stay exact; in an overlapped batch each delay runs from the
    /// batch start rather than from the previous completion.
    pub pipeline_model: SimPipelineModel,
}

impl Default for SimStorageConfig {
    fn default() -> Self {
        Self {
            max_file_bytes: 16 * 1_024 * 1_024,
            max_read_bytes: 128 * 1_024,
            max_write_bytes: 128 * 1_024,
            max_read_chunk: 128 * 1_024,
            max_write_chunk: 128 * 1_024,
            max_in_flight: 64,
            // 64 operations each holding the 128 KiB read or write maximum.
            max_outstanding_bytes: 8 * 1_024 * 1_024,
            max_scripted_faults: 64,
            default_latency: SimDuration::ZERO,
            latency_model: SimLatencyModel::Fixed,
            pipeline_model: SimPipelineModel::Serial,
        }
    }
}

/// Deterministic random sources a simulated storage session may consume.
///
/// Each field is an independent domain-separated stream, so enabling one does
/// not shift another's draw positions.
#[derive(Clone, Default)]
pub struct SimRandomSources {
    /// The [`RandomStream::Fault`] source the crash model requires.
    pub fault: Option<RandomHandle>,
    /// The [`RandomStream::Schedule`] source completion jitter requires.
    pub schedule: Option<RandomHandle>,
}

impl fmt::Debug for SimRandomSources {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SimRandomSources")
            .field("fault", &self.fault.is_some())
            .field("schedule", &self.schedule.is_some())
            .finish()
    }
}

impl SimRandomSources {
    /// Sets the crash model's fault source.
    #[must_use]
    pub fn with_fault(mut self, random: RandomHandle) -> Self {
        self.fault = Some(random);
        self
    }

    /// Sets the completion-jitter schedule source.
    #[must_use]
    pub fn with_schedule(mut self, random: RandomHandle) -> Self {
        self.schedule = Some(random);
        self
    }
}

/// Whether an admitted scripted operation succeeds or fails around its effect.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SimOutcome {
    Success,
    FailBefore,
    FailAfter,
    MayHaveAppliedBefore,
    MayHaveAppliedAfter,
}

/// Current version of the simulated failed-fsync page-gating policy.
pub const SIM_FSYNC_GATE_VERSION: u32 = 1;

/// Page size used by the version-1 failed-fsync gating policy.
pub const SIM_FSYNC_PAGE_BYTES: usize = 4 * 1_024;

/// Explicit resolution of page eligibility after an ambiguous failed fsync.
///
/// Linux writeback errors can leave pages clean in cache even though their
/// contents did not reach durable storage. A later successful fsync then need
/// not retry those pages unless they were dirtied again. These versioned
/// choices make both legal simulator outcomes scriptable and replayable.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum SimFsyncFailure {
    /// Keep every dirty page eligible for a later sync.
    RetainDirtyPagesV1,
    /// Exclude pages dirty at this failed fsync until each page is dirtied again.
    ExcludeDirtyPagesV1,
}

impl SimOutcome {
    const fn applies(self) -> bool {
        matches!(
            self,
            Self::Success | Self::FailAfter | Self::MayHaveAppliedAfter
        )
    }

    const fn certainty(self) -> Option<CompletionCertainty> {
        match self {
            Self::Success => None,
            Self::FailBefore => Some(CompletionCertainty::NotApplied),
            Self::FailAfter => Some(CompletionCertainty::Applied),
            Self::MayHaveAppliedBefore | Self::MayHaveAppliedAfter => {
                Some(CompletionCertainty::MayHaveApplied)
            }
        }
    }
}

/// One deterministic operation plan, consumed FIFO for its operation kind.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SimFault {
    pub operation: StorageOperation,
    pub delay: SimDuration,
    pub outcome: SimOutcome,
    /// Optional per-operation transfer cap, including zero.
    pub max_bytes: Option<usize>,
    /// Page-eligibility resolution for an ambiguous fsync that did not apply.
    ///
    /// This must be present exactly when `operation` is [`StorageOperation::Sync`]
    /// and `outcome` is [`SimOutcome::MayHaveAppliedBefore`].
    pub fsync_failure: Option<SimFsyncFailure>,
}

impl SimFault {
    #[must_use]
    pub const fn new(operation: StorageOperation, delay: SimDuration, outcome: SimOutcome) -> Self {
        Self {
            operation,
            delay,
            outcome,
            max_bytes: None,
            fsync_failure: None,
        }
    }

    /// Restricts this read or write to at most `max_bytes` transferred bytes.
    #[must_use]
    pub const fn with_max_bytes(mut self, max_bytes: usize) -> Self {
        self.max_bytes = Some(max_bytes);
        self
    }

    /// Selects the page-eligibility result of an ambiguous failed fsync.
    #[must_use]
    pub const fn with_fsync_failure(mut self, failure: SimFsyncFailure) -> Self {
        self.fsync_failure = Some(failure);
        self
    }
}

/// A fault-script admission failure.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum SimFaultError {
    /// The bounded fault-script queue cannot admit more scripts.
    ResourceExhausted {
        limit: usize,
    },
    Closed,
    MissingFsyncFailure,
    UnexpectedFsyncFailure,
}

impl fmt::Display for SimFaultError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ResourceExhausted { limit } => {
                write!(formatter, "fault script limit of {limit} is exhausted")
            }
            Self::Closed => formatter.write_str("file session is closed"),
            Self::MissingFsyncFailure => formatter.write_str(
                "ambiguous non-applied sync fault requires an explicit fsync failure result",
            ),
            Self::UnexpectedFsyncFailure => formatter.write_str(
                "fsync failure result is only valid for an ambiguous non-applied sync fault",
            ),
        }
    }
}

impl std::error::Error for SimFaultError {}

#[derive(Default)]
struct DiskState {
    durable: Vec<u8>,
    open: bool,
}

/// Durable bytes shared by successive simulated open-file sessions.
#[derive(Clone, Default)]
pub struct SimDisk {
    inner: Rc<RefCell<DiskState>>,
}

impl SimDisk {
    #[must_use]
    pub fn from_durable_bytes(bytes: Vec<u8>) -> Self {
        Self {
            inner: Rc::new(RefCell::new(DiskState {
                durable: bytes,
                open: false,
            })),
        }
    }

    #[must_use]
    pub fn durable_bytes(&self) -> Vec<u8> {
        self.inner.borrow().durable.clone()
    }

    #[must_use]
    pub fn durable_len(&self) -> u64 {
        self.inner.borrow().durable.len() as u64
    }

    /// Opens a session over this disk's durable bytes.
    ///
    /// # Errors
    ///
    /// Returns the same errors as [`SimStorage::open`].
    pub fn open(
        &self,
        handle: Handle,
        config: SimStorageConfig,
    ) -> Result<SimStorage, SimOpenError> {
        SimStorage::open(handle, self.clone(), config)
    }

    /// Opens a session that can use [`SimCrashModel::FoundationDbLikeV1`].
    ///
    /// `crash_random` must be scoped to [`RandomStream::Fault`], keeping crash
    /// resolution independent from workload generation.
    ///
    /// # Errors
    ///
    /// Returns the same errors as [`SimStorage::open_with_crash_random`].
    pub fn open_with_crash_random(
        &self,
        handle: Handle,
        config: SimStorageConfig,
        crash_random: RandomHandle,
    ) -> Result<SimStorage, SimOpenError> {
        SimStorage::open_with_crash_random(handle, self.clone(), config, crash_random)
    }
}

/// Failure to create a simulated open-file session.
#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum SimOpenError {
    InvalidConfig(&'static str),
    AlreadyOpen,
    ExistingFileTooLarge { size: usize, limit: usize },
    Spawn(SpawnError),
}

impl fmt::Display for SimOpenError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidConfig(message) => write!(formatter, "invalid storage config: {message}"),
            Self::AlreadyOpen => formatter.write_str("simulated disk is already open"),
            Self::ExistingFileTooLarge { size, limit } => {
                write!(formatter, "existing file size {size} exceeds limit {limit}")
            }
            Self::Spawn(error) => write!(formatter, "failed to spawn storage driver: {error}"),
        }
    }
}

impl std::error::Error for SimOpenError {}

enum Command {
    Read {
        request: ReadAtRequest,
        plan: SimFault,
        response: Responder<ReadResult>,
    },
    Write {
        request: WriteAtRequest,
        plan: SimFault,
        response: Responder<WriteResult>,
    },
    SetLen {
        len: u64,
        plan: SimFault,
        response: Responder<SetLenResult>,
    },
    Len {
        plan: SimFault,
        response: Responder<LenResult>,
    },
    Sync {
        plan: SimFault,
        response: Responder<SyncResult>,
    },
}

impl Command {
    const fn operation(&self) -> StorageOperation {
        match self {
            Self::Read { .. } => StorageOperation::ReadAt,
            Self::Write { .. } => StorageOperation::WriteAt,
            Self::SetLen { .. } => StorageOperation::SetLen,
            Self::Len { .. } => StorageOperation::Len,
            Self::Sync { .. } => StorageOperation::Sync,
        }
    }

    const fn plan(&self) -> SimFault {
        match self {
            Self::Read { plan, .. }
            | Self::Write { plan, .. }
            | Self::SetLen { plan, .. }
            | Self::Len { plan, .. }
            | Self::Sync { plan, .. } => *plan,
        }
    }

    /// Completes the command as a `NotApplied` rejection carrying `error`,
    /// returning any owned request buffer with it.
    fn complete_with(self, error: StorageError) {
        match self {
            Self::Read {
                request, response, ..
            } => response.complete(Err(CompletionError::not_applied(ReadAtFailure {
                error,
                buffer: request.buffer,
                bytes_transferred: 0,
            }))),
            Self::Write {
                request, response, ..
            } => response.complete(Err(CompletionError::not_applied(WriteAtFailure {
                error,
                buffer: request.buffer,
                bytes_transferred: 0,
            }))),
            Self::SetLen { response, .. } => {
                response.complete(Err(CompletionError::not_applied(error)))
            }
            Self::Len { response, .. } => {
                response.complete(Err(CompletionError::not_applied(error)))
            }
            Self::Sync { response, .. } => {
                response.complete(Err(CompletionError::not_applied(error)))
            }
        }
    }
}

struct SessionState {
    disk: SimDisk,
    crash_random: Option<RandomHandle>,
    latency: SimLatency,
    config: SimStorageConfig,
    accepted: Vec<u8>,
    sync_candidate: Vec<u8>,
    sync_candidate_len: usize,
    queue: VecDeque<Command>,
    scripts: [VecDeque<SimFault>; StorageOperation::COUNT],
    scripted_faults: usize,
    fault_hits: u64,
    reordered_completions: u64,
    /// Bounds admitted operations. Each admission's permit rides in its
    /// response cell, so the reservation releases exactly when the output is
    /// consumed or abandoned — never by a hand-written release site.
    operation_permits: Rc<LocalPermitPool>,
    /// Bounds the caller bytes admitted operations hold, with the same
    /// permit-carried lifetime as `operation_permits`.
    byte_permits: Rc<LocalPermitPool>,
    external_handles: usize,
    closed: bool,
    worker_waker: Option<Waker>,
}

/// Deterministic single-file storage driven by [`kr_runtime::SimRuntime`].
///
/// Stopping the runtime closes the session, releases the underlying
/// [`SimDisk`], and terminalizes every admitted operation with
/// [`StorageError::RuntimeStopped`].
pub struct SimStorage {
    session: Rc<RefCell<SessionState>>,
}

impl Clone for SimStorage {
    fn clone(&self) -> Self {
        self.session.borrow_mut().external_handles += 1;
        Self {
            session: Rc::clone(&self.session),
        }
    }
}

impl Drop for SimStorage {
    fn drop(&mut self) {
        let is_last = {
            let mut state = self.session.borrow_mut();
            state.external_handles -= 1;
            state.external_handles == 0
        };
        if is_last {
            close_session(&self.session);
        }
    }
}

impl SimStorage {
    /// Opens a session over the disk's durable bytes.
    ///
    /// # Errors
    ///
    /// Returns [`SimOpenError::InvalidConfig`] when a config bound is zero or
    /// inconsistent, [`SimOpenError::AlreadyOpen`] when the disk already has an
    /// open session, [`SimOpenError::ExistingFileTooLarge`] when the durable
    /// image exceeds `config.max_file_bytes`, or [`SimOpenError::Spawn`] when
    /// the storage driver task cannot be spawned.
    pub fn open(
        handle: Handle,
        disk: SimDisk,
        config: SimStorageConfig,
    ) -> Result<Self, SimOpenError> {
        Self::open_inner(handle, disk, config, SimRandomSources::default())
    }

    /// Opens a session with a dedicated deterministic crash random source.
    ///
    /// # Errors
    ///
    /// Returns [`SimOpenError::InvalidConfig`] when `crash_random` is not
    /// scoped to [`RandomStream::Fault`], and otherwise the same errors as
    /// [`Self::open`].
    pub fn open_with_crash_random(
        handle: Handle,
        disk: SimDisk,
        config: SimStorageConfig,
        crash_random: RandomHandle,
    ) -> Result<Self, SimOpenError> {
        Self::open_with_random_sources(
            handle,
            disk,
            config,
            SimRandomSources::default().with_fault(crash_random),
        )
    }

    /// Opens a session with dedicated deterministic random sources.
    ///
    /// This is the constructor a campaign uses to give storage a perturbing
    /// [`SimLatencyModel`]: completion jitter needs a
    /// [`RandomStream::Schedule`] source, and the crash model needs a
    /// [`RandomStream::Fault`] source. Each source is independent, so adding
    /// latency diversity does not shift the fault stream's draw positions.
    ///
    /// # Errors
    ///
    /// Returns [`SimOpenError::InvalidConfig`] when a source is bound to the
    /// wrong stream, when `config.latency_model` perturbs without a schedule
    /// source, or when `config.default_latency` plus the model's maximum
    /// jitter overflows, and otherwise the same errors as [`Self::open`].
    pub fn open_with_random_sources(
        handle: Handle,
        disk: SimDisk,
        config: SimStorageConfig,
        sources: SimRandomSources,
    ) -> Result<Self, SimOpenError> {
        if let Some(fault) = &sources.fault
            && fault.stream() != RandomStream::Fault
        {
            return Err(SimOpenError::InvalidConfig(
                "crash_random must use the Fault stream",
            ));
        }
        Self::open_inner(handle, disk, config, sources)
    }

    fn open_inner(
        handle: Handle,
        disk: SimDisk,
        config: SimStorageConfig,
        sources: SimRandomSources,
    ) -> Result<Self, SimOpenError> {
        validate_config(config)?;
        let latency = SimLatency::new(
            config.latency_model,
            sources.schedule,
            config.default_latency,
        )
        .map_err(|error| match error {
            SimLatencyError::WrongStream { .. } => {
                SimOpenError::InvalidConfig("latency_random must use the Schedule stream")
            }
            SimLatencyError::MissingScheduleRandom => SimOpenError::InvalidConfig(
                "a perturbing latency_model requires a Schedule random source",
            ),
            SimLatencyError::LatencyOverflow { .. } => SimOpenError::InvalidConfig(
                "default_latency plus the latency model's maximum jitter overflows",
            ),
        })?;
        let crash_random = sources.fault;
        let accepted = {
            let mut disk_state = disk.inner.borrow_mut();
            if disk_state.open {
                return Err(SimOpenError::AlreadyOpen);
            }
            if disk_state.durable.len() > config.max_file_bytes {
                return Err(SimOpenError::ExistingFileTooLarge {
                    size: disk_state.durable.len(),
                    limit: config.max_file_bytes,
                });
            }
            disk_state.open = true;
            disk_state.durable.clone()
        };
        let session = Rc::new(RefCell::new(SessionState {
            disk,
            crash_random,
            latency,
            config,
            sync_candidate: accepted.clone(),
            sync_candidate_len: accepted.len(),
            accepted,
            queue: VecDeque::new(),
            scripts: std::array::from_fn(|_| VecDeque::new()),
            scripted_faults: 0,
            fault_hits: 0,
            reordered_completions: 0,
            operation_permits: Rc::new(LocalPermitPool::new(config.max_in_flight)),
            byte_permits: Rc::new(LocalPermitPool::new(config.max_outstanding_bytes)),
            external_handles: 1,
            closed: false,
            worker_waker: None,
        }));
        let worker = StorageWorker::new(Rc::clone(&session));
        if let Err(error) = handle.clone().spawn(run_worker(handle, worker)) {
            close_session(&session);
            return Err(SimOpenError::Spawn(error));
        }
        Ok(Self { session })
    }

    /// Scripts one matching operation. Scripts are assigned when an operation
    /// is admitted, FIFO within each operation kind.
    ///
    /// # Errors
    ///
    /// Returns [`SimFaultError::MissingFsyncFailure`] when an ambiguous sync
    /// fault lacks its required fsync resolution,
    /// [`SimFaultError::UnexpectedFsyncFailure`] when any other fault carries
    /// one, [`SimFaultError::Closed`] after the session has closed, or
    /// [`SimFaultError::ResourceExhausted`] when the bounded fault-script
    /// queue is full. A rejected script is not enqueued.
    pub fn inject(&self, fault: SimFault) -> Result<(), SimFaultError> {
        let needs_fsync_resolution = fault.operation == StorageOperation::Sync
            && fault.outcome == SimOutcome::MayHaveAppliedBefore;
        match (needs_fsync_resolution, fault.fsync_failure) {
            (true, None) => return Err(SimFaultError::MissingFsyncFailure),
            (false, Some(_)) => return Err(SimFaultError::UnexpectedFsyncFailure),
            _ => {}
        }
        let mut state = self.session.borrow_mut();
        if state.closed {
            return Err(SimFaultError::Closed);
        }
        if state.scripted_faults == state.config.max_scripted_faults {
            return Err(SimFaultError::ResourceExhausted {
                limit: state.config.max_scripted_faults,
            });
        }
        state.scripts[fault.operation.index()].push_back(fault);
        state.scripted_faults += 1;
        Ok(())
    }

    /// Simulates process loss: unsynced bytes are discarded, queued operations
    /// fail `NotApplied`, and the durable disk can immediately be reopened.
    pub fn crash(&self) {
        self.crash_with_model(SimCrashModel::CleanRollback)
            .expect("clean rollback does not require crash randomness");
    }

    /// Simulates process loss using an explicit, versioned durability policy.
    ///
    /// Queued operations fail `NotApplied`, and the durable disk can
    /// immediately be reopened.
    ///
    /// # Errors
    ///
    /// Returns [`SimCrashError::MissingFaultRandom`] when the model requires
    /// crash randomness but the session was not opened with
    /// [`Self::open_with_crash_random`]. The session stays open on failure.
    pub fn crash_with_model(&self, model: SimCrashModel) -> Result<(), SimCrashError> {
        resolve_crash_image(&self.session, model)?;
        close_session(&self.session);
        Ok(())
    }

    #[must_use]
    pub fn status(&self) -> FileIoStatus {
        let state = self.session.borrow();
        FileIoStatus {
            accepted_len: state.accepted.len() as u64,
            durable_len: state.disk.durable_len(),
            fsync_gate_version: SIM_FSYNC_GATE_VERSION,
            has_fsync_gated_data: has_fsync_gated_data(&state),
            in_flight: state.operation_permits.in_use(),
            in_flight_limit: state.config.max_in_flight,
            outstanding_bytes: state.byte_permits.in_use(),
            outstanding_bytes_limit: state.config.max_outstanding_bytes,
            pending_faults: state.scripted_faults,
            fault_hits: state.fault_hits,
            reordered_completions: state.reordered_completions,
            closed: state.closed,
        }
    }

    /// Plans one operation and reserves its admission.
    ///
    /// `charge` is the caller allocation the operation pins while admitted:
    /// reads and writes charge their buffer's capacity — which submission
    /// validated fits [`SimStorageConfig::max_outstanding_bytes`] at all,
    /// while [`SimStorageConfig::max_read_bytes`] and
    /// [`SimStorageConfig::max_write_bytes`] bound the transfer length —
    /// and metadata operations hold no caller allocation and charge nothing.
    /// The returned
    /// [`LocalAdmission`] releases both reservations when the response is
    /// consumed or abandoned, and a refused admission acquires nothing: a
    /// refused byte charge returns the already-held operation permit through
    /// the early exit itself.
    fn plan_and_admit(
        &self,
        operation: StorageOperation,
        charge: usize,
    ) -> Result<(SimFault, LocalAdmission), StorageError> {
        let mut state = self.session.borrow_mut();
        if state.closed {
            return Err(StorageError::Closed);
        }
        let operation_permit =
            state
                .operation_permits
                .acquire()
                .ok_or(StorageError::ResourceExhausted {
                    resource: "in-flight storage operations",
                    limit: state.config.max_in_flight,
                })?;
        let byte_permit =
            state
                .byte_permits
                .acquire_many(charge)
                .ok_or(StorageError::ResourceExhausted {
                    resource: "outstanding storage bytes",
                    limit: state.config.max_outstanding_bytes,
                })?;
        let admission = LocalAdmission::with_bytes(operation_permit, byte_permit);
        let default_latency = state.config.default_latency;
        let fault = state.scripts[operation.index()].pop_front();
        if fault.is_some() {
            state.scripted_faults -= 1;
            state.fault_hits = state.fault_hits.saturating_add(1);
        }
        if let Some(fault) = fault {
            // A scripted delay is an exact assertion about a virtual-time
            // deadline, so the latency model never perturbs it.
            return Ok((fault, admission));
        }
        // `open` validated that this sum is representable.
        let delay = default_latency
            .checked_add(state.latency.jitter())
            .expect("open validated default_latency plus the model's maximum jitter");
        Ok((
            SimFault::new(operation, delay, SimOutcome::Success),
            admission,
        ))
    }

    fn push(&self, command: Command) {
        let waker = {
            let mut state = self.session.borrow_mut();
            state.queue.push_back(command);
            state.worker_waker.take()
        };
        if let Some(waker) = waker {
            waker.wake();
        }
    }
}

impl FileIoSubmit for SimStorage {
    type ReadAtResponse = SimOperation<ReadResult>;
    type WriteAtResponse = SimOperation<WriteResult>;
    type SetLenResponse = SimOperation<SetLenResult>;
    type LenResponse = SimOperation<LenResult>;
    type SyncResponse = SimOperation<SyncResult>;

    fn submit_read_at(&self, request: ReadAtRequest) -> Self::ReadAtResponse {
        let config = self.session.borrow().config;
        if request.buffer.len() > config.max_read_bytes {
            return SimOperation::ready(Err(CompletionError::not_applied(ReadAtFailure {
                error: StorageError::RequestTooLarge {
                    operation: StorageOperation::ReadAt,
                    requested: request.buffer.len(),
                    limit: config.max_read_bytes,
                },
                buffer: request.buffer,
                bytes_transferred: 0,
            })));
        }
        // The budget charges the allocation a buffer pins, not the length it
        // transfers, so an allocation larger than the whole budget could
        // never be admitted however often it were retried. Reject it as
        // over-large up front instead of reporting a resumable exhaustion
        // that can never resolve.
        if request.buffer.capacity() > config.max_outstanding_bytes {
            return SimOperation::ready(Err(CompletionError::not_applied(ReadAtFailure {
                error: StorageError::RequestTooLarge {
                    operation: StorageOperation::ReadAt,
                    requested: request.buffer.capacity(),
                    limit: config.max_outstanding_bytes,
                },
                buffer: request.buffer,
                bytes_transferred: 0,
            })));
        }
        let (plan, admission) =
            match self.plan_and_admit(StorageOperation::ReadAt, request.buffer.capacity()) {
                Ok(admitted) => admitted,
                Err(error) => {
                    return SimOperation::ready(Err(CompletionError::not_applied(ReadAtFailure {
                        error,
                        buffer: request.buffer,
                        bytes_transferred: 0,
                    })));
                }
            };
        let (future, response) = SimOperation::pending(admission);
        self.push(Command::Read {
            request,
            plan,
            response,
        });
        future
    }

    fn submit_write_at(&self, request: WriteAtRequest) -> Self::WriteAtResponse {
        let config = self.session.borrow().config;
        if request.buffer.len() > config.max_write_bytes {
            return SimOperation::ready(Err(CompletionError::not_applied(WriteAtFailure {
                error: StorageError::RequestTooLarge {
                    operation: StorageOperation::WriteAt,
                    requested: request.buffer.len(),
                    limit: config.max_write_bytes,
                },
                buffer: request.buffer,
                bytes_transferred: 0,
            })));
        }
        // See `submit_read_at`: an allocation the whole budget cannot hold is
        // rejected as over-large rather than as resumable exhaustion.
        if request.buffer.capacity() > config.max_outstanding_bytes {
            return SimOperation::ready(Err(CompletionError::not_applied(WriteAtFailure {
                error: StorageError::RequestTooLarge {
                    operation: StorageOperation::WriteAt,
                    requested: request.buffer.capacity(),
                    limit: config.max_outstanding_bytes,
                },
                buffer: request.buffer,
                bytes_transferred: 0,
            })));
        }
        let end = request.offset.checked_add(request.buffer.len() as u64);
        let Some(end) = end else {
            return SimOperation::ready(Err(CompletionError::not_applied(WriteAtFailure {
                error: StorageError::OffsetOverflow,
                buffer: request.buffer,
                bytes_transferred: 0,
            })));
        };
        if end > config.max_file_bytes as u64 {
            return SimOperation::ready(Err(CompletionError::not_applied(WriteAtFailure {
                error: StorageError::FileTooLarge {
                    requested: end,
                    limit: config.max_file_bytes as u64,
                },
                buffer: request.buffer,
                bytes_transferred: 0,
            })));
        }
        let (plan, admission) = match self
            .plan_and_admit(StorageOperation::WriteAt, request.buffer.capacity())
        {
            Ok(admitted) => admitted,
            Err(error) => {
                return SimOperation::ready(Err(CompletionError::not_applied(WriteAtFailure {
                    error,
                    buffer: request.buffer,
                    bytes_transferred: 0,
                })));
            }
        };
        let (future, response) = SimOperation::pending(admission);
        self.push(Command::Write {
            request,
            plan,
            response,
        });
        future
    }

    fn submit_set_len(&self, len: u64) -> Self::SetLenResponse {
        let limit = self.session.borrow().config.max_file_bytes as u64;
        if len > limit {
            return SimOperation::ready(Err(CompletionError::not_applied(
                StorageError::FileTooLarge {
                    requested: len,
                    limit,
                },
            )));
        }
        let (plan, admission) = match self.plan_and_admit(StorageOperation::SetLen, 0) {
            Ok(admitted) => admitted,
            Err(error) => return SimOperation::ready(Err(CompletionError::not_applied(error))),
        };
        let (future, response) = SimOperation::pending(admission);
        self.push(Command::SetLen {
            len,
            plan,
            response,
        });
        future
    }

    fn submit_len(&self) -> Self::LenResponse {
        let (plan, admission) = match self.plan_and_admit(StorageOperation::Len, 0) {
            Ok(admitted) => admitted,
            Err(error) => return SimOperation::ready(Err(CompletionError::not_applied(error))),
        };
        let (future, response) = SimOperation::pending(admission);
        self.push(Command::Len { plan, response });
        future
    }

    fn submit_sync(&self) -> Self::SyncResponse {
        let (plan, admission) = match self.plan_and_admit(StorageOperation::Sync, 0) {
            Ok(admitted) => admitted,
            Err(error) => return SimOperation::ready(Err(CompletionError::not_applied(error))),
        };
        let (future, response) = SimOperation::pending(admission);
        self.push(Command::Sync { plan, response });
        future
    }
}

fn validate_config(config: SimStorageConfig) -> Result<(), SimOpenError> {
    if config.max_in_flight == 0 {
        return Err(SimOpenError::InvalidConfig("max_in_flight must be nonzero"));
    }
    if config.max_scripted_faults == 0 {
        return Err(SimOpenError::InvalidConfig(
            "max_scripted_faults must be nonzero",
        ));
    }
    // A budget below either per-operation maximum would reject a request the
    // size limits accept, so the two limits would disagree about what is
    // admissible. Refuse the configuration rather than resolve it at runtime.
    if config.max_outstanding_bytes < config.max_read_bytes {
        return Err(SimOpenError::InvalidConfig(
            "max_outstanding_bytes is below max_read_bytes",
        ));
    }
    if config.max_outstanding_bytes < config.max_write_bytes {
        return Err(SimOpenError::InvalidConfig(
            "max_outstanding_bytes is below max_write_bytes",
        ));
    }
    if config.max_read_bytes > config.max_file_bytes {
        return Err(SimOpenError::InvalidConfig(
            "max_read_bytes exceeds max_file_bytes",
        ));
    }
    if config.max_write_bytes > config.max_file_bytes {
        return Err(SimOpenError::InvalidConfig(
            "max_write_bytes exceeds max_file_bytes",
        ));
    }
    if config.max_read_chunk == 0 {
        return Err(SimOpenError::InvalidConfig(
            "max_read_chunk must be nonzero",
        ));
    }
    if config.max_write_chunk == 0 {
        return Err(SimOpenError::InvalidConfig(
            "max_write_chunk must be nonzero",
        ));
    }
    Ok(())
}

fn resolve_crash_image(
    session: &Rc<RefCell<SessionState>>,
    model: SimCrashModel,
) -> Result<(), SimCrashError> {
    if model == SimCrashModel::CleanRollback || session.borrow().closed {
        return Ok(());
    }
    let (sync_candidate, durable, random, disk) = {
        let state = session.borrow();
        (
            materialize_sync_candidate(&state),
            state.disk.durable_bytes(),
            state.crash_random.clone(),
            state.disk.clone(),
        )
    };
    let random = random.ok_or(SimCrashError::MissingFaultRandom)?;
    let resolved = foundationdb_like_crash_v1(&durable, &sync_candidate, &random);
    disk.inner.borrow_mut().durable = resolved;
    Ok(())
}

fn foundationdb_like_crash_v1(durable: &[u8], accepted: &[u8], random: &RandomHandle) -> Vec<u8> {
    const SECTOR_BYTES: usize = 512;
    const SURVIVAL_DENOMINATOR: u64 = 10;

    let image_len = durable.len().max(accepted.len());
    let mut resolved = durable.to_vec();
    resolved.resize(image_len, 0);

    for sector_start in (0..image_len).step_by(SECTOR_BYTES) {
        let sector_end = sector_start.saturating_add(SECTOR_BYTES).min(image_len);
        let dirty = (sector_start..sector_end).any(|index| {
            durable.get(index).copied().unwrap_or(0) != accepted.get(index).copied().unwrap_or(0)
        });
        if !dirty
            || random
                .random_below(SURVIVAL_DENOMINATOR)
                .expect("crash survival denominator is nonzero")
                != 0
        {
            continue;
        }

        let tear = random
            .random_below(4)
            .expect("crash tear choice bound is nonzero")
            != 0;
        let copied_end = if tear {
            sector_start
                + random
                    .random_below((sector_end - sector_start + 1) as u64)
                    .expect("sector cut bound is nonzero") as usize
        } else {
            sector_end
        };
        for (index, byte) in resolved
            .iter_mut()
            .enumerate()
            .take(copied_end)
            .skip(sector_start)
        {
            *byte = accepted.get(index).copied().unwrap_or(0);
        }
        if tear {
            let mut garbage = 0_u64;
            let mut remaining = 0_u8;
            for byte in resolved.iter_mut().take(sector_end).skip(copied_end) {
                if remaining == 0 {
                    garbage = random
                        .random_u64()
                        .expect("crash resolution runs before runtime shutdown");
                    remaining = 8;
                }
                *byte = garbage as u8;
                garbage >>= 8;
                remaining -= 1;
            }
        }
    }

    let final_len = if accepted.len() == durable.len()
        || random
            .random_bool_ratio(1, 2)
            .expect("crash length-survival ratio is valid")
    {
        accepted.len()
    } else {
        durable.len()
    };
    resolved.truncate(final_len);
    resolved
}

fn close_session(session: &Rc<RefCell<SessionState>>) {
    close_session_with(session, StorageError::Closed);
}

fn close_session_with(session: &Rc<RefCell<SessionState>>, error: StorageError) {
    let (commands, waker) = {
        let mut state = session.borrow_mut();
        if state.closed {
            return;
        }
        state.closed = true;
        let durable = state.disk.inner.borrow().durable.clone();
        state.accepted.clone_from(&durable);
        state.sync_candidate.clone_from(&durable);
        state.sync_candidate_len = durable.len();
        state.disk.inner.borrow_mut().open = false;
        // Draining the queue releases nothing here: each command's admission
        // rides in its response cell and returns to the pools when the
        // rejection below is consumed or abandoned.
        let commands: Vec<_> = state.queue.drain(..).collect();
        (commands, state.worker_waker.take())
    };
    for command in commands {
        command.complete_with(error.clone());
    }
    if let Some(waker) = waker {
        crate::completion::wake_contained(waker);
    }
}

struct StorageWorker {
    session: Rc<RefCell<SessionState>>,
    /// Commands the worker has taken from the queue, in admission order.
    /// Executed entries are `None`; every remaining command is terminalized
    /// when the worker drops.
    active: Vec<Option<Command>>,
}

impl StorageWorker {
    fn new(session: Rc<RefCell<SessionState>>) -> Self {
        Self {
            session,
            active: Vec::new(),
        }
    }

    fn activate(&mut self, command: Command) {
        debug_assert!(self.active.iter().all(Option::is_none));
        self.active.clear();
        self.active.push(Some(command));
    }

    /// Moves the queue's commuting prefix into the active batch.
    fn collect_commuting_batch(&mut self) {
        let mut state = self.session.borrow_mut();
        while let Some(next) = state.queue.front() {
            if !commutes_with_active(&self.active, next) {
                break;
            }
            let command = state
                .queue
                .pop_front()
                .expect("peeked queue command is present");
            self.active.push(Some(command));
        }
    }

    fn plan_at(&self, index: usize) -> SimFault {
        self.active[index]
            .as_ref()
            .expect("storage worker batch entry is still active")
            .plan()
    }

    fn take_at(&mut self, index: usize) -> Command {
        self.active[index]
            .take()
            .expect("storage worker batch entry is still active")
    }
}

impl Drop for StorageWorker {
    fn drop(&mut self) {
        let remaining: Vec<Command> = self.active.iter_mut().filter_map(Option::take).collect();
        close_session_with(&self.session, StorageError::RuntimeStopped);
        for command in remaining {
            command.complete_with(StorageError::RuntimeStopped);
        }
    }
}

/// Whether `next` may start while the active batch is outstanding.
///
/// This is the commuting classification the Linux file pipelines use: reads
/// run concurrently with reads, writes with non-overlapping writes, and
/// `set_len`, `len`, and `sync` are fences that run alone. Executed (`None`)
/// entries still hold their batch slot, so a fence never joins a batch that
/// has begun draining.
fn commutes_with_active(active: &[Option<Command>], next: &Command) -> bool {
    match next {
        Command::Read { .. } => active
            .iter()
            .flatten()
            .all(|command| matches!(command, Command::Read { .. })),
        Command::Write { request, .. } => active.iter().flatten().all(|command| match command {
            Command::Write {
                request: earlier, ..
            } => !write_ranges_overlap(earlier, request),
            _ => false,
        }),
        Command::SetLen { .. } | Command::Len { .. } | Command::Sync { .. } => false,
    }
}

fn write_ranges_overlap(first: &WriteAtRequest, second: &WriteAtRequest) -> bool {
    if first.buffer.is_empty() || second.buffer.is_empty() {
        return false;
    }
    // Submission validated offset + len against max_file_bytes, so these
    // additions cannot wrap.
    first.offset < second.offset + second.buffer.len() as u64
        && second.offset < first.offset + first.buffer.len() as u64
}

struct NextCommand {
    session: Rc<RefCell<SessionState>>,
}

impl Future for NextCommand {
    type Output = Option<Command>;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let mut state = self.session.borrow_mut();
        if let Some(command) = state.queue.pop_front() {
            Poll::Ready(Some(command))
        } else if state.closed {
            Poll::Ready(None)
        } else {
            state.worker_waker = Some(context.waker().clone());
            Poll::Pending
        }
    }
}

async fn run_worker(handle: Handle, mut worker: StorageWorker) {
    loop {
        let Some(command) = (NextCommand {
            session: Rc::clone(&worker.session),
        })
        .await
        else {
            return;
        };
        worker.activate(command);
        if worker.session.borrow().config.pipeline_model == SimPipelineModel::CommutingOverlapV1 {
            worker.collect_commuting_batch();
        }
        if !drive_active_batch(&handle, &mut worker).await {
            return;
        }
    }
}

/// Completes every active command at its own latency instant.
///
/// All commands in the batch start together, so each one completes at the
/// batch start plus its own delay: commuting operations admitted together
/// resolve in latency order, not admission order. A serial batch always holds
/// one command, which reduces to sleep-then-execute. Returns `false` when the
/// runtime is stopping mid-batch; the remaining commands stay active for the
/// worker's drop cleanup.
async fn drive_active_batch(handle: &Handle, worker: &mut StorageWorker) -> bool {
    let mut order: Vec<usize> = (0..worker.active.len()).collect();
    // The sort is stable, so equal delays complete in admission order.
    order.sort_by_key(|&index| worker.plan_at(index).delay);
    let mut elapsed = SimDuration::ZERO;
    for index in order {
        let delay = worker.plan_at(index).delay;
        if delay > elapsed {
            let remaining = SimDuration::from_nanos(delay.as_nanos() - elapsed.as_nanos());
            if handle.sleep(remaining).await.is_err() {
                return false;
            }
            elapsed = delay;
        }
        let overtakes_earlier_admission = worker.active[..index].iter().any(Option::is_some);
        let command = worker.take_at(index);
        if overtakes_earlier_admission {
            let mut state = worker.session.borrow_mut();
            state.reordered_completions = state.reordered_completions.saturating_add(1);
        }
        if worker.session.borrow().closed {
            command.complete_with(StorageError::Closed);
        } else {
            execute(command, &worker.session);
        }
    }
    worker.active.clear();
    true
}

fn execute(command: Command, session: &Rc<RefCell<SessionState>>) {
    let plan = command.plan();
    let operation = command.operation();
    match command {
        Command::Read {
            mut request,
            response,
            ..
        } => {
            if !plan.outcome.applies() {
                response.complete(Err(CompletionError::new(
                    plan.outcome.certainty().expect("failure has certainty"),
                    ReadAtFailure {
                        error: StorageError::Injected { operation },
                        buffer: request.buffer,
                        bytes_transferred: 0,
                    },
                )));
                return;
            }
            let state = session.borrow();
            let transfer_limit = plan
                .max_bytes
                .unwrap_or(state.config.max_read_chunk)
                .min(request.buffer.len());
            let start = usize::try_from(request.offset)
                .ok()
                .filter(|start| *start < state.accepted.len());
            let bytes_read = start.map_or(0, |start| {
                let count = transfer_limit.min(state.accepted.len() - start);
                request.buffer[..count].copy_from_slice(&state.accepted[start..start + count]);
                count
            });
            request.buffer.truncate(bytes_read);
            if let Some(certainty) = plan.outcome.certainty() {
                response.complete(Err(CompletionError::new(
                    certainty,
                    ReadAtFailure {
                        error: StorageError::Injected { operation },
                        buffer: request.buffer,
                        bytes_transferred: bytes_read,
                    },
                )));
            } else {
                response.complete(Ok(ReadAtSuccess {
                    buffer: request.buffer,
                    bytes_read,
                }));
            }
        }
        Command::Write {
            request, response, ..
        } => {
            let transfer_limit = {
                let state = session.borrow();
                plan.max_bytes
                    .unwrap_or(state.config.max_write_chunk)
                    .min(request.buffer.len())
            };
            if plan.outcome.applies() && transfer_limit != 0 {
                let end = request.offset as usize + transfer_limit;
                let mut state = session.borrow_mut();
                let previous_len = state.accepted.len();
                if state.accepted.len() < end {
                    state.accepted.resize(end, 0);
                }
                state.accepted[request.offset as usize..end]
                    .copy_from_slice(&request.buffer[..transfer_limit]);
                if end > previous_len {
                    state.sync_candidate_len = state.accepted.len();
                }
                mark_sync_candidate_page_dirty(&mut state, request.offset as usize, transfer_limit);
            }
            if let Some(certainty) = plan.outcome.certainty() {
                response.complete(Err(CompletionError::new(
                    certainty,
                    WriteAtFailure {
                        error: StorageError::Injected { operation },
                        buffer: request.buffer,
                        bytes_transferred: if plan.outcome.applies() {
                            transfer_limit
                        } else {
                            0
                        },
                    },
                )));
            } else {
                response.complete(Ok(WriteAtSuccess {
                    bytes_written: transfer_limit,
                    buffer: request.buffer,
                }));
            }
        }
        Command::SetLen { len, response, .. } => {
            if plan.outcome.applies() {
                let mut state = session.borrow_mut();
                state.accepted.resize(len as usize, 0);
                state.sync_candidate_len = len as usize;
                state.sync_candidate.resize(len as usize, 0);
            }
            complete_simple(response, plan, operation, SetLenSuccess { len });
        }
        Command::Len { response, .. } => {
            let len = session.borrow().accepted.len() as u64;
            complete_simple(response, plan, operation, FileLength { len });
        }
        Command::Sync { response, .. } => {
            if plan.outcome.applies() {
                let state = session.borrow();
                state.disk.inner.borrow_mut().durable = materialize_sync_candidate(&state);
            } else if plan.fsync_failure == Some(SimFsyncFailure::ExcludeDirtyPagesV1) {
                let mut state = session.borrow_mut();
                let durable = state.disk.inner.borrow().durable.clone();
                state.sync_candidate.clone_from(&durable);
                state.sync_candidate_len = durable.len();
            }
            let durable_len = session.borrow().disk.durable_len();
            complete_simple(response, plan, operation, SyncSuccess { durable_len });
        }
    }
}

fn mark_sync_candidate_page_dirty(state: &mut SessionState, offset: usize, len: usize) {
    debug_assert_ne!(len, 0);
    state
        .sync_candidate
        .resize(state.accepted.len().max(state.sync_candidate_len), 0);
    let first_page = offset / SIM_FSYNC_PAGE_BYTES;
    let last_page = (offset + len - 1) / SIM_FSYNC_PAGE_BYTES;
    for page in first_page..=last_page {
        let start = page * SIM_FSYNC_PAGE_BYTES;
        let end = start
            .saturating_add(SIM_FSYNC_PAGE_BYTES)
            .min(state.accepted.len());
        state.sync_candidate[start..end].copy_from_slice(&state.accepted[start..end]);
    }
}

fn materialize_sync_candidate(state: &SessionState) -> Vec<u8> {
    let mut image = state.sync_candidate.clone();
    image.resize(state.sync_candidate_len, 0);
    image
}

fn has_fsync_gated_data(state: &SessionState) -> bool {
    state.accepted.len() != state.sync_candidate_len
        || state.sync_candidate.len() < state.accepted.len()
        || state.accepted != state.sync_candidate[..state.accepted.len()]
}

fn complete_simple<T>(
    response: Responder<CompletionResult<T, StorageError>>,
    plan: SimFault,
    operation: StorageOperation,
    success: T,
) {
    if let Some(certainty) = plan.outcome.certainty() {
        response.complete(Err(CompletionError::new(
            certainty,
            StorageError::Injected { operation },
        )));
    } else {
        response.complete(Ok(success));
    }
}
