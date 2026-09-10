//! Owned byte-stream networking contracts and deterministic providers.
//!
//! [`SimNetwork`] performs no host I/O. Calls admit operations synchronously,
//! return owned `'static` futures, and use the supplied simulation runtime only
//! to deliver completion latency. Dropping a future abandons only its response:
//! the admitted operation remains queued and may still consume bytes or a
//! connection. This contract deliberately matches an io_uring submission whose
//! completion is no longer observed. A deliberately stalled operation has no
//! terminal completion and retains its permit and owned request until the
//! simulator state is torn down.
//!
//! [`MemoryNetwork`] implements the same owned contracts with thread-safe
//! handles and futures for portable actors, including on multi-threaded
//! executors. It has no fault or latency scripting; [`SimNetwork`] remains the
//! richer simulation provider.

use crate::completion::{LocalAdmission, LocalCell, LocalPermitPool};
use crate::latency::{SimLatency, SimLatencyError, SimLatencyModel};
use kr_runtime::{CompletionError, CompletionResult, Handle, RandomHandle, SimDuration};
use std::cell::RefCell;
use std::collections::{BTreeMap, VecDeque};
use std::error::Error;
use std::fmt;
use std::future::Future;
use std::rc::{Rc, Weak};

mod cold;
mod memory;
mod propagation;
pub use propagation::{PropagationProfile, PropagationWindow};
mod vectored;

use vectored::WriteData;
use vectored::cells::{LocalWriteCell, LocalWriteResponse};
pub use vectored::{
    ByteStreamVectoredSubmit, MAX_WRITE_SEGMENTS, MemoryVectoredWriteOperation,
    SendByteStreamVectoredSubmit, SharedBytes, SimVectoredWriteOperation, VectoredWriteFailure,
    VectoredWriteRequest, VectoredWriteResult, VectoredWriteSize, WriteSegment,
};

pub use cold::{ColdListener, ColdNetwork, ColdStream};
pub use memory::{
    MemoryListener, MemoryNetwork, MemoryNetworkConfig, MemoryNetworkStatus, MemoryOperation,
    MemoryStream,
};

#[cfg(any(test, feature = "test-support"))]
pub mod test_support;

/// A stable simulated machine or process-network identity.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct NodeId(pub u64);

/// A deterministic simulated network address.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct NetworkAddress {
    /// Simulated node hosting the endpoint.
    pub node: NodeId,
    /// Node-local service port. Port zero has no special meaning.
    pub port: u16,
}

/// One direction of a connected pair.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct LinkKey {
    /// Sender of bytes in this direction.
    pub from: NodeId,
    /// Receiver of bytes in this direction.
    pub to: NodeId,
}

/// Whether submissions may traverse a directional link.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LinkState {
    /// Operations and bytes may make progress.
    Open,
    /// Accepted operations remain pending until the link is opened.
    Clogged,
    /// New operations fail without being applied.
    ///
    /// Previously accepted operations retain the gate state captured when they
    /// were admitted.
    Partitioned,
}

/// Deterministic behavior of one directional link.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct LinkConfig {
    /// Completion latency captured when an operation is admitted.
    ///
    /// In [`SimNetwork`], latency delays the operation's local completion;
    /// admitted bytes may become peer-visible earlier. This is simulator timing,
    /// not a guarantee made by [`ByteStreamSubmit`] for every provider. A simulated
    /// connect models one traversal in each handshake direction and delays its
    /// client response by the checked sum of both admission-time latencies.
    pub latency: SimDuration,
    /// Maximum bytes transferred by one read or write completion.
    pub max_chunk_bytes: usize,
    /// Current progress state.
    pub state: LinkState,
}

impl Default for LinkConfig {
    fn default() -> Self {
        Self {
            latency: SimDuration::ZERO,
            max_chunk_bytes: 64 * 1024,
            state: LinkState::Open,
        }
    }
}

/// Bounded resource limits for [`SimNetwork`].
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct NetworkConfig {
    /// Maximum live bound listeners.
    pub max_listeners: usize,
    /// Maximum backlog accepted by one listener.
    pub max_listener_backlog: usize,
    /// Maximum live connected pairs.
    pub max_connections: usize,
    /// Maximum admitted operations that have not reached a consumed or
    /// abandoned terminal completion.
    pub max_inflight_operations: usize,
    /// Byte capacity of each direction of each connection.
    ///
    /// A valid nonempty simulated write waits when this capacity is full. Such
    /// writes retain an in-flight operation permit until capacity or a terminal
    /// close resolves them.
    pub directional_buffer_bytes: usize,
    /// Maximum caller-owned buffer accepted by one operation.
    pub max_operation_bytes: usize,
    /// Maximum caller-owned bytes held by admitted reads at once.
    ///
    /// A read charges the allocation it pins: its buffer's capacity, or the
    /// length the buffer may reach at completion if that is larger. A pooled
    /// buffer therefore charges its full pool size, not the portion this
    /// request will fill. The charge is released when the output is consumed
    /// or discarded.
    ///
    /// Reads draw on their own budget rather than sharing one with writes
    /// because a write can block on [`Self::directional_buffer_bytes`] while
    /// holding its charge, and only a read frees that capacity. Sharing one
    /// budget therefore admits a deadlock: blocked writes hold the bytes the
    /// unblocking read needs, and abandoning those writes does not release
    /// them, because a dropped response abandons delivery rather than the
    /// admitted operation. The split is the same reservation the io_uring
    /// provider makes for sustained receives, for the same reason.
    ///
    /// This is distinct from [`Self::directional_buffer_bytes`], which bounds
    /// bytes resident in one direction of one connection. This bounds bytes
    /// held by admitted reads across the whole provider.
    ///
    /// The default is `max_inflight_operations` times
    /// [`Self::max_operation_bytes`], the worst case the other limits already
    /// permit. Lower it to make this the binding constraint.
    pub max_outstanding_read_bytes: usize,
    /// Maximum caller-owned bytes held by admitted writes at once.
    ///
    /// A write charges the allocation it pins — its buffer's capacity, not
    /// just the payload length — released when its output is consumed or
    /// discarded. A write blocked on
    /// [`Self::directional_buffer_bytes`] keeps its charge until capacity or a
    /// terminal close resolves it. See [`Self::max_outstanding_read_bytes`]
    /// for why the two budgets are separate.
    ///
    /// The default matches [`Self::max_outstanding_read_bytes`].
    pub max_outstanding_write_bytes: usize,
    /// Maximum queued deterministic fault rules.
    pub max_scripted_faults: usize,
    /// Maximum directional link profiles that differ from [`Self::default_link`].
    pub max_link_overrides: usize,
    /// Maximum directional links with live operations waiting for a clogged
    /// admission-time gate to open.
    pub max_blocked_links: usize,
    /// Default behavior for directions without an override.
    pub default_link: LinkConfig,
    /// How each completion's link-derived latency is perturbed.
    ///
    /// A perturbing model requires a [`RandomStream::Schedule`](kr_runtime::rng::RandomStream::Schedule) source passed
    /// to [`SimNetwork::new_with_schedule_random`]. Jitter perturbs only the
    /// link-derived portion of a completion delay, and is drawn after every
    /// admission check, so a rejected operation consumes no draw. A scripted
    /// fault's `extra_latency` is added exactly on top, matching the way
    /// simulated storage leaves a scripted delay unperturbed.
    pub latency_model: SimLatencyModel,
}

impl Default for NetworkConfig {
    fn default() -> Self {
        Self {
            max_listeners: 1_024,
            max_listener_backlog: 1_024,
            max_connections: 1_024,
            max_inflight_operations: 4_096,
            directional_buffer_bytes: 256 * 1024,
            max_operation_bytes: 256 * 1024,
            // 4096 operations each holding the 256 KiB per-operation maximum,
            // per direction, so neither budget binds before the other limits do.
            max_outstanding_read_bytes: 4_096 * 256 * 1024,
            max_outstanding_write_bytes: 4_096 * 256 * 1024,
            max_scripted_faults: 1_024,
            max_link_overrides: 4_096,
            max_blocked_links: 4_096,
            default_link: LinkConfig::default(),
            latency_model: SimLatencyModel::Fixed,
        }
    }
}

/// A normalized network operation kind used by provider diagnostics.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum NetworkOperationKind {
    /// Bind a listener.
    Listen,
    /// Connect to a listener.
    Connect,
    /// Accept one connection.
    Accept,
    /// A stream read.
    Read,
    /// A stream write.
    Write,
    /// A write-half shutdown.
    ShutdownWrite,
    /// A full disconnect.
    Close,
}

impl NetworkOperationKind {
    const COUNT: usize = 7;

    const fn index(self) -> usize {
        match self {
            Self::Listen => 0,
            Self::Connect => 1,
            Self::Accept => 2,
            Self::Read => 3,
            Self::Write => 4,
            Self::ShutdownWrite => 5,
            Self::Close => 6,
        }
    }
}

/// Certainty reported by an injected error after an operation's effect.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AfterFaultCertainty {
    /// The modeled effect definitely occurred.
    Applied,
    /// The caller must reconcile whether the modeled effect occurred.
    MayHaveApplied,
}

impl AfterFaultCertainty {
    /// Wraps an after-effect failure with this certainty.
    pub(crate) const fn error<E>(self, failure: E) -> CompletionError<E> {
        match self {
            Self::Applied => CompletionError::applied(failure),
            Self::MayHaveApplied => CompletionError::may_have_applied(failure),
        }
    }
}

/// Terminal behavior of one scripted operation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FaultOutcome {
    /// Continue normally after applying the rule's delay and byte limit.
    Continue,
    /// Return an injected error before any effect.
    FailBefore { tag: u64 },
    /// Apply the effect, then report an injected error.
    FailAfter {
        tag: u64,
        certainty: AfterFaultCertainty,
    },
    /// Retain ownership and never complete or apply the operation.
    ///
    /// Dropping the response future does not release its operation permit or
    /// owned request. [`SimNetwork`] retains both until its state is torn down.
    StallBefore,
}

/// One deterministic fault rule, queued FIFO within its operation kind.
///
/// A rule is consumed by the next operation with the matching kind. Operations
/// of other kinds do not consume or skip it. The current simulator accepts
/// rules for `Read`, `Write`, `ShutdownWrite`, and `Close` only.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ScriptedFault {
    /// Operation kind to match.
    pub operation: NetworkOperationKind,
    /// Additional completion latency, added with overflow checking to the
    /// applicable admission-time link latency for every terminal outcome.
    pub extra_latency: SimDuration,
    /// Optional tighter partial-I/O bound.
    pub max_bytes: Option<usize>,
    /// Terminal behavior.
    pub outcome: FaultOutcome,
}

impl ScriptedFault {
    /// A before-effect injected failure.
    #[must_use]
    pub const fn fail_before(operation: NetworkOperationKind, tag: u64) -> Self {
        Self {
            operation,
            extra_latency: SimDuration::ZERO,
            max_bytes: None,
            outcome: FaultOutcome::FailBefore { tag },
        }
    }

    /// An after-effect injected failure.
    #[must_use]
    pub const fn fail_after(
        operation: NetworkOperationKind,
        tag: u64,
        certainty: AfterFaultCertainty,
    ) -> Self {
        Self {
            operation,
            extra_latency: SimDuration::ZERO,
            max_bytes: None,
            outcome: FaultOutcome::FailAfter { tag, certainty },
        }
    }

    /// A pending-forever operation that takes no effect.
    #[must_use]
    pub const fn stall(operation: NetworkOperationKind) -> Self {
        Self {
            operation,
            extra_latency: SimDuration::ZERO,
            max_bytes: None,
            outcome: FaultOutcome::StallBefore,
        }
    }

    /// A normal completion constrained to at most `max_bytes` bytes.
    #[must_use]
    pub const fn partial(operation: NetworkOperationKind, max_bytes: usize) -> Self {
        Self {
            operation,
            extra_latency: SimDuration::ZERO,
            max_bytes: Some(max_bytes),
            outcome: FaultOutcome::Continue,
        }
    }
}

/// Stable categories reported by the network contract.
#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum NetworkError {
    /// A simulator or link configuration is invalid.
    InvalidConfig { reason: &'static str },
    /// An operation request violates a documented bound.
    InvalidRequest { reason: &'static str },
    /// The directional byte buffer has no space.
    Backpressure { capacity: usize },
    /// The requested provider-native address is already in use.
    ///
    /// The address remains available in the caller's request and is omitted
    /// here so simulated and production providers share this category.
    AddressInUse,
    /// The connection was refused at the requested provider-native address.
    ///
    /// The address remains available in the caller's request and is omitted
    /// here so simulated and production providers share this category.
    ConnectionRefused,
    /// A listener handle no longer refers to a live binding.
    ListenerClosed,
    /// A deterministic listener cannot queue another server-side connection.
    ///
    /// Host socket providers whose backlog is kernel-managed need not surface
    /// this exact category for every overflow.
    BacklogFull { capacity: usize },
    /// The stream or its peer has disconnected.
    ConnectionClosed,
    /// The local write half was shut down.
    WriteClosed,
    /// The relevant direction is hard-partitioned.
    Partitioned { link: LinkKey },
    /// A bounded resource cannot admit more work.
    ResourceExhausted {
        resource: &'static str,
        limit: usize,
    },
    /// A deterministic fault rule fired.
    Injected { tag: u64 },
    /// The simulation runtime could not schedule a completion task.
    CompletionDriverUnavailable,
    /// A production driver stopped before accepting the operation.
    DriverStopped,
    /// A production backend reported an operating-system or reactor failure.
    Backend {
        operation: NetworkOperationKind,
        raw_os_error: Option<i32>,
        message: String,
    },
    /// A simulator identifier counter was exhausted.
    IdentifierExhausted,
}

impl fmt::Display for NetworkError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidConfig { reason } => write!(formatter, "invalid network config: {reason}"),
            Self::InvalidRequest { reason } => {
                write!(formatter, "invalid network request: {reason}")
            }
            Self::Backpressure { capacity } => {
                write!(
                    formatter,
                    "directional buffer capacity {capacity} is exhausted"
                )
            }
            Self::AddressInUse => formatter.write_str("address is already in use"),
            Self::ConnectionRefused => formatter.write_str("connection was refused"),
            Self::ListenerClosed => formatter.write_str("listener is closed"),
            Self::BacklogFull { capacity } => {
                write!(
                    formatter,
                    "listener backlog capacity {capacity} is exhausted"
                )
            }
            Self::ConnectionClosed => formatter.write_str("connection is closed"),
            Self::WriteClosed => formatter.write_str("local write half is closed"),
            Self::Partitioned { link } => {
                write!(
                    formatter,
                    "link {} -> {} is partitioned",
                    link.from.0, link.to.0
                )
            }
            Self::ResourceExhausted { resource, limit } => {
                write!(formatter, "{resource} limit of {limit} is exhausted")
            }
            Self::Injected { tag } => write!(formatter, "injected network fault {tag}"),
            Self::CompletionDriverUnavailable => {
                formatter.write_str("completion driver is unavailable")
            }
            Self::DriverStopped => formatter.write_str("network driver is stopped"),
            Self::Backend {
                operation,
                raw_os_error,
                message,
            } => {
                write!(formatter, "{operation:?} backend failure")?;
                if let Some(code) = raw_os_error {
                    write!(formatter, " (os error {code})")?;
                }
                write!(formatter, ": {message}")
            }
            Self::IdentifierExhausted => formatter.write_str("network identifier exhausted"),
        }
    }
}

impl Error for NetworkError {}

/// An operation failure that returns any caller-owned byte buffer.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NetworkFailure {
    error: NetworkError,
    buffer: Option<Vec<u8>>,
    bytes_transferred: usize,
}

impl NetworkFailure {
    /// Creates a failure for an operation without a byte buffer.
    #[must_use]
    pub fn without_buffer(error: NetworkError) -> Self {
        Self {
            error,
            buffer: None,
            bytes_transferred: 0,
        }
    }

    /// Creates a failure that returns the operation's byte buffer.
    #[must_use]
    pub fn with_buffer(error: NetworkError, buffer: Vec<u8>, bytes_transferred: usize) -> Self {
        Self {
            error,
            buffer: Some(buffer),
            bytes_transferred,
        }
    }

    /// Returns the stable error category.
    #[must_use]
    pub const fn error(&self) -> &NetworkError {
        &self.error
    }

    /// Returns the number of bytes consumed or admitted before failure.
    #[must_use]
    pub const fn bytes_transferred(&self) -> usize {
        self.bytes_transferred
    }

    /// Returns the caller-owned buffer, when this operation had one.
    #[must_use]
    pub fn into_buffer(self) -> Option<Vec<u8>> {
        self.buffer
    }
}

impl fmt::Display for NetworkFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.error.fmt(formatter)
    }
}

impl Error for NetworkFailure {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        Some(&self.error)
    }
}

/// An owned request for bytes from a stream.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReadRequest {
    /// Buffer whose existing prefix is preserved.
    pub buffer: Vec<u8>,
    /// Maximum bytes to append during this completion.
    pub max_bytes: usize,
}

/// A completed partial stream read.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReadResult {
    /// Original buffer with `bytes_read` bytes appended.
    pub buffer: Vec<u8>,
    /// Bytes appended by this completion.
    pub bytes_read: usize,
    /// Whether this nonzero-capacity read observed TCP-style EOF.
    ///
    /// EOF is reported only by a successful read that appends zero bytes after
    /// all buffered bytes have drained. A read returning the peer's final bytes
    /// reports `false`; a subsequent read reports EOF. A zero-capacity read
    /// never reports EOF.
    pub end_of_stream: bool,
}

/// An owned request to write bytes to a stream.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WriteRequest {
    /// Buffer retained by the driver until terminal completion.
    pub buffer: Vec<u8>,
}

/// A completed partial stream write.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WriteResult {
    /// Original caller-owned buffer.
    pub buffer: Vec<u8>,
    /// Prefix bytes admitted by this completion.
    pub bytes_written: usize,
}

/// An owned request to bind a listener.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ListenRequest<A = NetworkAddress> {
    /// Address to bind exclusively.
    pub address: A,
    /// Requested maximum server endpoints waiting to be accepted.
    ///
    /// [`SimNetwork`] enforces this exactly. Host socket providers may pass it
    /// to an operating-system backlog API whose effective bound can be capped
    /// or treated as a hint; portable callers must still enforce their own
    /// application admission limit when exactness matters.
    pub backlog: usize,
}

/// An owned request to connect two addresses.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ConnectRequest<A = NetworkAddress> {
    /// Client endpoint identity used for directional link selection.
    ///
    /// Providers reserve an exact non-ephemeral identity while its client
    /// stream is live. Provider-native addresses may define an ephemeral form
    /// (for example a socket address with port zero) with different reuse rules.
    pub local: A,
    /// Bound listener address.
    pub remote: A,
}

/// Bounded diagnostic state for a simulated network.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct NetworkStatus {
    /// Live bound listeners.
    pub listeners: usize,
    /// Live connected pairs.
    pub connections: usize,
    /// Operations holding an admission permit.
    pub inflight_operations: usize,
    /// Caller-owned bytes held by admitted reads.
    pub outstanding_read_bytes: usize,
    /// Caller-owned bytes held by admitted writes, including writes blocked on
    /// directional capacity.
    pub outstanding_write_bytes: usize,
    /// Fault rules not yet consumed.
    pub pending_faults: usize,
    /// Fault rules consumed so far.
    pub fault_hits: u64,
    /// Directional link profiles that differ from the configured default.
    pub link_overrides: usize,
    /// Directional links with live completions waiting for an admission-time
    /// clogged gate.
    pub blocked_links: usize,
}

/// Warm listener submission operations shared by simulated and real drivers.
pub trait NetworkListenerSubmit: 'static {
    /// Provider-native address, such as [`NetworkAddress`] or
    /// [`std::net::SocketAddr`].
    type Address: Clone + 'static;
    /// Stream returned by successful accepts.
    type Stream: ByteStreamSubmit;
    /// Owned accept future.
    type AcceptResponse: Future<Output = CompletionResult<Self::Stream, NetworkFailure>> + 'static;
    /// Owned listener-close future.
    type CloseResponse: Future<Output = CompletionResult<(), NetworkFailure>> + 'static;

    /// Returns the bound address without driving I/O.
    fn local_address(&self) -> Self::Address;

    /// Accepts one connection or waits in FIFO order.
    ///
    /// Submission is eager. Dropping the returned future abandons only the
    /// response; the admitted accept remains at its FIFO position and consumes
    /// one connection when it completes. No implicit cancellation is provided.
    fn submit_accept(&self) -> Self::AcceptResponse;

    /// Closes the binding and rejects pending accepts.
    ///
    /// Submission is eager, and dropping the returned future abandons only the
    /// response. Once close completes, later close calls succeed idempotently.
    fn submit_close(&self) -> Self::CloseResponse;
}

/// A [`NetworkListenerSubmit`] safe to share across executor threads.
///
/// Implementations are inferred from the listener, stream, address, and owned
/// future types. Requiring this trait therefore carries the complete portable
/// `Send` contract into generic code rather than relying on properties of one
/// concrete provider that its base trait does not advertise.
pub trait SendNetworkListenerSubmit:
    NetworkListenerSubmit<
        Address: Send,
        Stream: SendByteStreamSubmit,
        AcceptResponse: Send,
        CloseResponse: Send,
    > + Send
    + Sync
{
}

impl<T> SendNetworkListenerSubmit for T where
    T: NetworkListenerSubmit<
            Address: Send,
            Stream: SendByteStreamSubmit,
            AcceptResponse: Send,
            CloseResponse: Send,
        > + Send
        + Sync
{
}

/// Minimal warm connection-oriented networking control plane.
///
/// Application code should use [`ColdNetwork`], which defers admission to a
/// future's first poll; this trait is the provider-facing eager contract.
pub trait NetworkProviderSubmit: 'static {
    /// Provider-native address, such as [`NetworkAddress`] under simulation or
    /// [`std::net::SocketAddr`] for a real IP transport.
    type Address: Clone + 'static;
    /// Connected byte stream.
    type Stream: ByteStreamSubmit;
    /// Bound listener.
    type Listener: NetworkListenerSubmit<Address = Self::Address, Stream = Self::Stream>;
    /// Owned listen future.
    type ListenResponse: Future<Output = CompletionResult<Self::Listener, NetworkFailure>> + 'static;
    /// Owned connect future.
    type ConnectResponse: Future<Output = CompletionResult<Self::Stream, NetworkFailure>> + 'static;

    /// Binds one address with a bounded backlog.
    ///
    /// Submission is eager, and dropping the future abandons only its response.
    fn submit_listen(&self, request: ListenRequest<Self::Address>) -> Self::ListenResponse;

    /// Connects to a bound listener.
    ///
    /// Submission is eager, and dropping the future abandons only its response.
    fn submit_connect(&self, request: ConnectRequest<Self::Address>) -> Self::ConnectResponse;
}

/// A [`NetworkProviderSubmit`] safe to share across executor threads.
///
/// Its associated addresses, streams, listeners, and owned operation futures
/// are all `Send`. Provider, listener, and stream handles are also [`Sync`];
/// owned address values need not be because they cross task boundaries by
/// value rather than by shared reference.
pub trait SendNetworkProviderSubmit:
    NetworkProviderSubmit<
        Address: Send,
        Stream: SendByteStreamSubmit,
        Listener: SendNetworkListenerSubmit,
        ListenResponse: Send,
        ConnectResponse: Send,
    > + Send
    + Sync
{
}

impl<T> SendNetworkProviderSubmit for T where
    T: NetworkProviderSubmit<
            Address: Send,
            Stream: SendByteStreamSubmit,
            Listener: SendNetworkListenerSubmit,
            ListenResponse: Send,
            ConnectResponse: Send,
        > + Send
        + Sync
{
}

/// Warm owned byte-stream submission operations shared by simulated and real
/// drivers. Application code should use [`ColdStream`].
pub trait ByteStreamSubmit: 'static {
    /// Owned read future.
    type ReadResponse: Future<Output = CompletionResult<ReadResult, NetworkFailure>> + 'static;
    /// Owned write future.
    type WriteResponse: Future<Output = CompletionResult<WriteResult, NetworkFailure>> + 'static;
    /// Owned control-operation future.
    type ControlResponse: Future<Output = CompletionResult<(), NetworkFailure>> + 'static;

    /// Submits a read. The returned future does not borrow this stream.
    ///
    /// Submission is eager. The driver owns `request` until terminal completion.
    /// Dropping the future abandons only the response: the read may still consume
    /// bytes and retains its admission capacity until it completes. The API does
    /// not implicitly cancel it.
    fn submit_read(&self, request: ReadRequest) -> Self::ReadResponse;

    /// Submits a write. The returned future does not borrow this stream.
    ///
    /// Submission is eager. The driver owns `request` until terminal completion.
    /// Dropping the future abandons only the response and does not roll back or
    /// cancel the admitted write.
    fn submit_write(&self, request: WriteRequest) -> Self::WriteResponse;

    /// Closes only the local write half. Repeated calls are idempotent.
    /// Submission is eager; dropping the future does not cancel the shutdown.
    fn submit_shutdown_write(&self) -> Self::ControlResponse;

    /// Disconnects both local halves. Repeated calls are idempotent.
    /// Submission is eager; dropping the future does not cancel the close.
    fn submit_close(&self) -> Self::ControlResponse;
}

/// A [`ByteStreamSubmit`] safe to share across executor threads.
///
/// This marker is implemented automatically when both the stream handle and
/// all of its owned operation futures satisfy the multi-threaded runtime
/// contract. The separate trait preserves support for efficient `!Send`
/// simulation streams.
pub trait SendByteStreamSubmit:
    ByteStreamSubmit<ReadResponse: Send, WriteResponse: Send, ControlResponse: Send> + Send + Sync
{
}

impl<T> SendByteStreamSubmit for T where
    T: ByteStreamSubmit<ReadResponse: Send, WriteResponse: Send, ControlResponse: Send>
        + Send
        + Sync
{
}

/// A deterministic, bounded, host-I/O-free byte-stream network.
///
/// If runtime shutdown cancels a completion-delay task, the delay gate is
/// released and the operation's already-produced result remains observable.
#[derive(Clone)]
pub struct SimNetwork {
    state: Rc<RefCell<NetworkState>>,
}

impl SimNetwork {
    /// Creates a simulator driven by `handle`'s virtual time.
    ///
    /// # Errors
    ///
    /// Returns [`NetworkError::InvalidConfig`] for a zero resource or byte
    /// bound, or a zero default chunk size.
    pub fn new(handle: Handle, config: NetworkConfig) -> Result<Self, NetworkError> {
        Self::build(handle, config, None)
    }

    /// Creates a simulator whose completion latency is perturbed per operation.
    ///
    /// This is the constructor a campaign uses to explore completion orders:
    /// with a perturbing [`NetworkConfig::latency_model`], two operations
    /// admitted in one order can complete in either order, and every draw is
    /// visible in the run's determinism checkpoint.
    ///
    /// # Errors
    ///
    /// Returns [`NetworkError::InvalidConfig`] for the same config problems as
    /// [`Self::new`], when `random` is not scoped to
    /// [`RandomStream::Schedule`](kr_runtime::rng::RandomStream::Schedule), or when the model's maximum jitter is not
    /// representable.
    pub fn new_with_schedule_random(
        handle: Handle,
        config: NetworkConfig,
        random: RandomHandle,
    ) -> Result<Self, NetworkError> {
        Self::build(handle, config, Some(random))
    }

    fn build(
        handle: Handle,
        config: NetworkConfig,
        random: Option<RandomHandle>,
    ) -> Result<Self, NetworkError> {
        validate_config(config)?;
        // The base is dynamic here, so only the model's own bound is checked
        // now; a link sum that overflows with jitter is reported per operation
        // as a latency overflow.
        let latency =
            SimLatency::new(config.latency_model, random, SimDuration::ZERO).map_err(|error| {
                match error {
                    SimLatencyError::WrongStream { .. } => NetworkError::InvalidConfig {
                        reason: "latency random source must use the Schedule stream",
                    },
                    SimLatencyError::MissingScheduleRandom => NetworkError::InvalidConfig {
                        reason: "a perturbing latency_model requires a Schedule random source",
                    },
                    SimLatencyError::LatencyOverflow { .. } => NetworkError::InvalidConfig {
                        reason: "the latency model's maximum jitter is not representable",
                    },
                }
            })?;
        let permits = Rc::new(LocalPermitPool::new(config.max_inflight_operations));
        let read_byte_permits = Rc::new(LocalPermitPool::new(config.max_outstanding_read_bytes));
        let write_byte_permits = Rc::new(LocalPermitPool::new(config.max_outstanding_write_bytes));
        Ok(Self {
            state: Rc::new(RefCell::new(NetworkState {
                handle,
                config,
                latency,
                permits,
                read_byte_permits,
                write_byte_permits,
                next_connection_id: 0,
                next_listener_id: 0,
                connections: BTreeMap::new(),
                listeners: BTreeMap::new(),
                bindings: BTreeMap::new(),
                client_bindings: BTreeMap::new(),
                links: BTreeMap::new(),
                blocked: BTreeMap::new(),
                stalled_operations: Vec::new(),
                faults: std::array::from_fn(|_| VecDeque::new()),
                fault_hits: 0,
            })),
        })
    }

    /// Creates an in-memory connected pair between two simulated nodes.
    ///
    /// # Errors
    ///
    /// Returns a typed resource error when the connection bound is full.
    pub fn connected_pair(
        &self,
        left: NodeId,
        right: NodeId,
    ) -> Result<(SimStream, SimStream), NetworkError> {
        let id = self
            .state
            .borrow_mut()
            .create_connection(left, right, None)?;
        Ok((
            SimStream::new(Rc::clone(&self.state), id, Side::Left),
            SimStream::new(Rc::clone(&self.state), id, Side::Right),
        ))
    }

    /// Replaces one directional link profile.
    ///
    /// Opening a clogged or partitioned link wakes accepted operations in
    /// deterministic admission order and services queued reads.
    ///
    /// # Errors
    ///
    /// Returns [`NetworkError::InvalidConfig`] if `max_chunk_bytes` is zero, or
    /// [`NetworkError::ResourceExhausted`] when adding a nondefault profile
    /// would exceed [`NetworkConfig::max_link_overrides`].
    pub fn set_link(&self, link: LinkKey, config: LinkConfig) -> Result<(), NetworkError> {
        if config.max_chunk_bytes == 0 {
            return Err(NetworkError::InvalidConfig {
                reason: "link max_chunk_bytes must be nonzero",
            });
        }
        self.state.borrow_mut().set_link(link, config)
    }

    /// Queues a deterministic FIFO fault rule.
    ///
    /// # Errors
    ///
    /// Returns a typed resource error when the script bound is full.
    pub fn push_fault(&self, fault: ScriptedFault) -> Result<(), NetworkError> {
        if matches!(
            fault.operation,
            NetworkOperationKind::Listen
                | NetworkOperationKind::Connect
                | NetworkOperationKind::Accept
        ) {
            return Err(NetworkError::InvalidRequest {
                reason: "listener control-plane fault scripting is not supported",
            });
        }
        let mut state = self.state.borrow_mut();
        if state.pending_faults() >= state.config.max_scripted_faults {
            return Err(NetworkError::ResourceExhausted {
                resource: "scripted faults",
                limit: state.config.max_scripted_faults,
            });
        }
        state.faults[fault.operation.index()].push_back(fault);
        Ok(())
    }

    /// Returns bounded diagnostic state without driving operations.
    #[must_use]
    pub fn status(&self) -> NetworkStatus {
        let mut state = self.state.borrow_mut();
        state.purge_empty_blocked_links();
        NetworkStatus {
            listeners: state.listeners.len(),
            connections: state.connections.len(),
            inflight_operations: state.permits.in_use(),
            outstanding_read_bytes: state.read_byte_permits.in_use(),
            outstanding_write_bytes: state.write_byte_permits.in_use(),
            pending_faults: state.pending_faults(),
            fault_hits: state.fault_hits,
            link_overrides: state.links.len(),
            blocked_links: state.blocked.len(),
        }
    }
}

impl NetworkProviderSubmit for SimNetwork {
    type Address = NetworkAddress;
    type Stream = SimStream;
    type Listener = SimListener;
    type ListenResponse = SimOperation<CompletionResult<SimListener, NetworkFailure>>;
    type ConnectResponse = SimOperation<CompletionResult<SimStream, NetworkFailure>>;

    fn submit_listen(&self, request: ListenRequest) -> Self::ListenResponse {
        NetworkState::submit_listen(&self.state, request)
    }

    fn submit_connect(&self, request: ConnectRequest) -> Self::ConnectResponse {
        NetworkState::submit_connect(&self.state, request)
    }
}

/// One exclusive simulated address binding.
pub struct SimListener {
    state: Rc<RefCell<NetworkState>>,
    listener: u64,
    address: NetworkAddress,
}

impl fmt::Debug for SimListener {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SimListener")
            .field("listener", &self.listener)
            .field("address", &self.address)
            .finish_non_exhaustive()
    }
}

impl NetworkListenerSubmit for SimListener {
    type Address = NetworkAddress;
    type Stream = SimStream;
    type AcceptResponse = SimOperation<CompletionResult<SimStream, NetworkFailure>>;
    type CloseResponse = SimOperation<CompletionResult<(), NetworkFailure>>;

    fn local_address(&self) -> NetworkAddress {
        self.address
    }

    fn submit_accept(&self) -> Self::AcceptResponse {
        NetworkState::submit_accept(&self.state, self.listener)
    }

    fn submit_close(&self) -> Self::CloseResponse {
        NetworkState::submit_listener_close(&self.state, self.listener)
    }
}

impl Drop for SimListener {
    fn drop(&mut self) {
        self.state
            .borrow_mut()
            .close_listener_immediately(self.listener);
    }
}

/// One endpoint of a simulated connected pair.
pub struct SimStream {
    state: Rc<RefCell<NetworkState>>,
    connection: u64,
    side: Side,
}

impl SimStream {
    fn new(state: Rc<RefCell<NetworkState>>, connection: u64, side: Side) -> Self {
        Self {
            state,
            connection,
            side,
        }
    }
}

impl fmt::Debug for SimStream {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SimStream")
            .field("connection", &self.connection)
            .field("side", &self.side)
            .finish_non_exhaustive()
    }
}

impl ByteStreamSubmit for SimStream {
    type ReadResponse = SimOperation<CompletionResult<ReadResult, NetworkFailure>>;
    type WriteResponse = SimOperation<CompletionResult<WriteResult, NetworkFailure>>;
    type ControlResponse = SimOperation<CompletionResult<(), NetworkFailure>>;

    fn submit_read(&self, request: ReadRequest) -> Self::ReadResponse {
        NetworkState::submit_read(&self.state, self.connection, self.side, request)
    }

    fn submit_write(&self, request: WriteRequest) -> Self::WriteResponse {
        NetworkState::submit_write(&self.state, self.connection, self.side, request.into())
            .into_contiguous()
    }

    fn submit_shutdown_write(&self) -> Self::ControlResponse {
        NetworkState::submit_shutdown_write(&self.state, self.connection, self.side)
    }

    fn submit_close(&self) -> Self::ControlResponse {
        NetworkState::submit_close(&self.state, self.connection, self.side)
    }
}

impl ByteStreamVectoredSubmit for SimStream {
    type WriteVectoredResponse = SimVectoredWriteOperation;
    fn max_segments(&self) -> usize {
        MAX_WRITE_SEGMENTS
    }
    fn submit_write_vectored(&self, request: VectoredWriteRequest) -> Self::WriteVectoredResponse {
        NetworkState::submit_write(&self.state, self.connection, self.side, request.into())
            .into_vectored()
    }
}

impl Drop for SimStream {
    fn drop(&mut self) {
        self.state
            .borrow_mut()
            .close_side_immediately(self.connection, self.side);
    }
}

/// An owned simulator response. Polling after it returned `Ready` panics.
///
/// Dropping this value never cancels its admitted operation. A pending read or
/// accept remains queued, continues to hold an operation permit, and releases
/// that permit when its unobserved terminal response is discarded. An operation
/// scripted to stall has no terminal response and remains retained by the
/// simulator state even after this value is dropped.
pub type SimOperation<T> = crate::completion::LocalOperation<T>;

trait OpenGate {
    fn open(&self);
}

impl<T> OpenGate for LocalCell<T> {
    fn open(&self) {
        self.open_gate();
    }
}

// Data and control responses own caller buffers and guards, so a clogged
// completion needs a provider owner even after its observer disappears. Endpoint
// outputs contain NetworkState itself; retaining those here would form a cycle.
enum BlockedGate {
    Retained(Rc<dyn OpenGate>),
    Endpoint(Weak<dyn OpenGate>),
}

impl BlockedGate {
    fn live(&self) -> bool {
        match self {
            Self::Retained(_) => true,
            Self::Endpoint(gate) => gate.strong_count() != 0,
        }
    }

    fn open(self) {
        match self {
            Self::Retained(gate) => gate.open(),
            Self::Endpoint(gate) => {
                if let Some(gate) = gate.upgrade() {
                    gate.open();
                }
            }
        }
    }
}

trait StalledCompletion {}

impl<T> StalledCompletion for LocalCell<T> {}

struct NetworkState {
    handle: Handle,
    config: NetworkConfig,
    latency: SimLatency,
    permits: Rc<LocalPermitPool>,
    read_byte_permits: Rc<LocalPermitPool>,
    write_byte_permits: Rc<LocalPermitPool>,
    next_connection_id: u64,
    next_listener_id: u64,
    connections: BTreeMap<u64, Connection>,
    listeners: BTreeMap<u64, ListenerState>,
    bindings: BTreeMap<NetworkAddress, u64>,
    client_bindings: BTreeMap<NetworkAddress, u64>,
    links: BTreeMap<LinkKey, LinkConfig>,
    blocked: BTreeMap<LinkKey, Vec<BlockedGate>>,
    stalled_operations: Vec<Rc<dyn StalledCompletion>>,
    faults: [VecDeque<ScriptedFault>; NetworkOperationKind::COUNT],
    fault_hits: u64,
}

struct ListenerState {
    address: NetworkAddress,
    backlog: usize,
    queued_connections: VecDeque<u64>,
    pending_accepts: VecDeque<PendingAccept>,
}

struct PendingAccept {
    cell: Rc<LocalCell<CompletionResult<SimStream, NetworkFailure>>>,
}

struct Connection {
    left: Endpoint,
    right: Endpoint,
    client_binding: Option<NetworkAddress>,
    left_to_right: Pipe,
    right_to_left: Pipe,
}

#[derive(Clone, Copy)]
struct Endpoint {
    node: NodeId,
    alive: bool,
    read_open: bool,
    write_open: bool,
}

struct Pipe {
    bytes: VecDeque<u8>,
    capacity: usize,
    sender_closed: bool,
    receiver_open: bool,
    pending_reads: VecDeque<PendingRead>,
    pending_writes: VecDeque<PendingWrite>,
    propagation: Option<propagation::Propagation>,
}

struct PendingRead {
    request: ReadRequest,
    link_config: LinkConfig,
    wait_for_open: bool,
    fault: FaultPlan,
    cell: Rc<LocalCell<CompletionResult<ReadResult, NetworkFailure>>>,
}

struct PendingWrite {
    request: WriteData,
    link: LinkKey,
    link_config: LinkConfig,
    wait_for_open: bool,
    fault: FaultPlan,
    cell: LocalWriteCell,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Side {
    Left,
    Right,
}

#[derive(Clone, Copy)]
struct FaultPlan {
    extra_latency: SimDuration,
    max_bytes: Option<usize>,
    outcome: FaultOutcome,
}

impl Default for FaultPlan {
    fn default() -> Self {
        Self {
            extra_latency: SimDuration::ZERO,
            max_bytes: None,
            outcome: FaultOutcome::Continue,
        }
    }
}

impl NetworkState {
    fn retain_stalled<T: 'static>(&mut self, cell: &Rc<LocalCell<T>>) {
        // Every retained cell owns an operation permit, so admission bounds this
        // otherwise permanent holding set without a second capacity mechanism.
        debug_assert!(self.stalled_operations.len() < self.config.max_inflight_operations);
        let erased: Rc<dyn StalledCompletion> = cell.clone();
        self.stalled_operations.push(erased);
    }

    /// Parks a `StallBefore` operation: the teardown fallback output is
    /// committed behind a closed gate so the response never observes it while
    /// the simulator lives, and the cell is retained with its permit.
    fn stall_operation<S: 'static, E: 'static>(
        &mut self,
        cell: &Rc<LocalCell<CompletionResult<S, E>>>,
        failure: E,
    ) {
        cell.complete(Err(CompletionError::not_applied(failure)));
        cell.close_gate();
        cell.mark_delay_elapsed();
        self.retain_stalled(cell);
    }

    /// Completes a `FailBefore` operation: the injected error rides the
    /// ordinary completion schedule, and a scheduling failure replaces it
    /// immediately. `failure` wraps whichever error terminates the operation.
    fn fail_before<S: 'static, E: 'static>(
        &mut self,
        cell: &Rc<LocalCell<CompletionResult<S, E>>>,
        links: &[(LinkKey, LinkConfig)],
        extra_latency: SimDuration,
        tag: u64,
        failure: impl FnOnce(NetworkError) -> E,
    ) {
        match self.prepare_completion(cell, links, extra_latency) {
            Ok(()) => cell.complete(Err(CompletionError::not_applied(failure(
                NetworkError::Injected { tag },
            )))),
            Err(error) => {
                complete_immediately(cell, Err(CompletionError::not_applied(failure(error))));
            }
        }
    }

    fn submit_listen(
        state: &Rc<RefCell<Self>>,
        request: ListenRequest,
    ) -> SimOperation<CompletionResult<SimListener, NetworkFailure>> {
        let mut inner = state.borrow_mut();
        let Some(permit) = inner.permits.acquire() else {
            return SimOperation::ready(Err(CompletionError::not_applied(
                NetworkFailure::without_buffer(NetworkError::ResourceExhausted {
                    resource: "inflight operations",
                    limit: inner.config.max_inflight_operations,
                }),
            )));
        };
        let cell = Rc::new(LocalCell::with_delay(Some(permit)));
        let future = SimOperation::from_cell(Rc::clone(&cell));
        if request.backlog == 0 || request.backlog > inner.config.max_listener_backlog {
            complete_immediately(
                &cell,
                Err(CompletionError::not_applied(
                    NetworkFailure::without_buffer(NetworkError::InvalidRequest {
                        reason: "listener backlog must be within the configured nonzero bound",
                    }),
                )),
            );
            return future;
        }
        if inner.bindings.contains_key(&request.address)
            || inner.client_bindings.contains_key(&request.address)
        {
            complete_immediately(
                &cell,
                Err(CompletionError::not_applied(
                    NetworkFailure::without_buffer(NetworkError::AddressInUse),
                )),
            );
            return future;
        }
        if inner.listeners.len() >= inner.config.max_listeners {
            complete_immediately(
                &cell,
                Err(CompletionError::not_applied(
                    NetworkFailure::without_buffer(NetworkError::ResourceExhausted {
                        resource: "listeners",
                        limit: inner.config.max_listeners,
                    }),
                )),
            );
            return future;
        }
        let listener = inner.next_listener_id;
        let Some(next_listener) = listener.checked_add(1) else {
            complete_immediately(
                &cell,
                Err(CompletionError::not_applied(
                    NetworkFailure::without_buffer(NetworkError::IdentifierExhausted),
                )),
            );
            return future;
        };
        inner.next_listener_id = next_listener;
        inner.bindings.insert(request.address, listener);
        inner.listeners.insert(
            listener,
            ListenerState {
                address: request.address,
                backlog: request.backlog,
                queued_connections: VecDeque::new(),
                pending_accepts: VecDeque::new(),
            },
        );
        complete_immediately(
            &cell,
            Ok(SimListener {
                state: Rc::clone(state),
                listener,
                address: request.address,
            }),
        );
        future
    }

    fn submit_connect(
        state: &Rc<RefCell<Self>>,
        request: ConnectRequest,
    ) -> SimOperation<CompletionResult<SimStream, NetworkFailure>> {
        let mut inner = state.borrow_mut();
        let Some(permit) = inner.permits.acquire() else {
            return SimOperation::ready(Err(CompletionError::not_applied(
                NetworkFailure::without_buffer(NetworkError::ResourceExhausted {
                    resource: "inflight operations",
                    limit: inner.config.max_inflight_operations,
                }),
            )));
        };
        let cell = Rc::new(LocalCell::with_delay(Some(permit)));
        let future = SimOperation::from_cell(Rc::clone(&cell));
        if inner.bindings.contains_key(&request.local)
            || inner.client_bindings.contains_key(&request.local)
        {
            complete_immediately(
                &cell,
                Err(CompletionError::not_applied(
                    NetworkFailure::without_buffer(NetworkError::AddressInUse),
                )),
            );
            return future;
        }
        let forward_link = LinkKey {
            from: request.local.node,
            to: request.remote.node,
        };
        let reverse_link = LinkKey {
            from: request.remote.node,
            to: request.local.node,
        };
        // The simulator models one handshake traversal in each direction.
        // Both link profiles are captured at admission. Their latencies are
        // added, and the connect response remains gated until every direction
        // that was clogged at admission is opened. The server-side endpoint is
        // still enqueued eagerly, matching the simulator's general rule that
        // latency delays the local response rather than rolling back effects.
        let forward_config = inner.link_config(forward_link);
        let reverse_config = inner.link_config(reverse_link);
        let handshake_links = [
            (forward_link, forward_config),
            (reverse_link, reverse_config),
        ];
        let partitioned_link = handshake_links
            .iter()
            .find_map(|(link, config)| (config.state == LinkState::Partitioned).then_some(*link));
        if let Some(link) = partitioned_link {
            complete_immediately(
                &cell,
                Err(CompletionError::not_applied(
                    NetworkFailure::without_buffer(NetworkError::Partitioned { link }),
                )),
            );
            return future;
        }
        let Some(listener_id) = inner.bindings.get(&request.remote).copied() else {
            complete_immediately(
                &cell,
                Err(CompletionError::not_applied(
                    NetworkFailure::without_buffer(NetworkError::ConnectionRefused),
                )),
            );
            return future;
        };
        let Some(listener) = inner.listeners.get(&listener_id) else {
            complete_immediately(
                &cell,
                Err(CompletionError::not_applied(
                    NetworkFailure::without_buffer(NetworkError::ConnectionRefused),
                )),
            );
            return future;
        };
        if listener.pending_accepts.is_empty()
            && listener.queued_connections.len() >= listener.backlog
        {
            complete_immediately(
                &cell,
                Err(CompletionError::not_applied(
                    NetworkFailure::without_buffer(NetworkError::BacklogFull {
                        capacity: listener.backlog,
                    }),
                )),
            );
            return future;
        }
        if let Err(error) = inner.check_connection_capacity() {
            complete_immediately(
                &cell,
                Err(CompletionError::not_applied(
                    NetworkFailure::without_buffer(error),
                )),
            );
            return future;
        }
        if let Err(error) = inner.prepare_completion_with_retention(
            &cell,
            &handshake_links,
            SimDuration::ZERO,
            false,
        ) {
            complete_immediately(
                &cell,
                Err(CompletionError::not_applied(
                    NetworkFailure::without_buffer(error),
                )),
            );
            return future;
        }
        let connection = match inner.create_connection(
            request.local.node,
            request.remote.node,
            Some(request.local),
        ) {
            Ok(connection) => connection,
            Err(error) => {
                complete_immediately(
                    &cell,
                    Err(CompletionError::not_applied(
                        NetworkFailure::without_buffer(error),
                    )),
                );
                return future;
            }
        };
        let pending_accept = inner
            .listeners
            .get_mut(&listener_id)
            .expect("binding references a listener")
            .pending_accepts
            .pop_front();
        if let Some(accept) = pending_accept {
            // Complete outside the NetworkState borrow. If the accept response
            // was abandoned, dropping its unobserved SimStream closes that
            // endpoint and re-enters NetworkState.
            drop(inner);
            complete_immediately(
                &accept.cell,
                Ok(SimStream::new(Rc::clone(state), connection, Side::Right)),
            );
        } else {
            inner
                .listeners
                .get_mut(&listener_id)
                .expect("binding references a listener")
                .queued_connections
                .push_back(connection);
            drop(inner);
        }
        cell.complete(Ok(SimStream::new(Rc::clone(state), connection, Side::Left)));
        future
    }

    fn submit_accept(
        state: &Rc<RefCell<Self>>,
        listener_id: u64,
    ) -> SimOperation<CompletionResult<SimStream, NetworkFailure>> {
        let mut inner = state.borrow_mut();
        let Some(permit) = inner.permits.acquire() else {
            return SimOperation::ready(Err(CompletionError::not_applied(
                NetworkFailure::without_buffer(NetworkError::ResourceExhausted {
                    resource: "inflight operations",
                    limit: inner.config.max_inflight_operations,
                }),
            )));
        };
        let cell = Rc::new(LocalCell::with_delay(Some(permit)));
        let future = SimOperation::from_cell(Rc::clone(&cell));
        let Some(listener) = inner.listeners.get_mut(&listener_id) else {
            complete_immediately(
                &cell,
                Err(CompletionError::not_applied(
                    NetworkFailure::without_buffer(NetworkError::ListenerClosed),
                )),
            );
            return future;
        };
        if let Some(connection) = listener.queued_connections.pop_front() {
            complete_immediately(
                &cell,
                Ok(SimStream::new(Rc::clone(state), connection, Side::Right)),
            );
        } else {
            listener.pending_accepts.push_back(PendingAccept { cell });
        }
        future
    }

    fn submit_listener_close(
        state: &Rc<RefCell<Self>>,
        listener_id: u64,
    ) -> SimOperation<CompletionResult<(), NetworkFailure>> {
        let mut inner = state.borrow_mut();
        let Some(permit) = inner.permits.acquire() else {
            return ready_error(NetworkFailure::without_buffer(
                NetworkError::ResourceExhausted {
                    resource: "inflight operations",
                    limit: inner.config.max_inflight_operations,
                },
            ));
        };
        let cell = Rc::new(LocalCell::with_delay(Some(permit)));
        inner.close_listener_immediately(listener_id);
        complete_immediately(&cell, Ok(()));
        SimOperation::from_cell(cell)
    }

    fn close_listener_immediately(&mut self, listener_id: u64) {
        let Some(listener) = self.listeners.remove(&listener_id) else {
            return;
        };
        self.bindings.remove(&listener.address);
        for accept in listener.pending_accepts {
            complete_immediately(
                &accept.cell,
                Err(CompletionError::not_applied(
                    NetworkFailure::without_buffer(NetworkError::ListenerClosed),
                )),
            );
        }
        for connection in listener.queued_connections {
            self.close_side_immediately(connection, Side::Right);
        }
    }

    fn create_connection(
        &mut self,
        left: NodeId,
        right: NodeId,
        client_binding: Option<NetworkAddress>,
    ) -> Result<u64, NetworkError> {
        self.check_connection_capacity()?;
        let id = self.next_connection_id;
        self.next_connection_id = id.checked_add(1).ok_or(NetworkError::IdentifierExhausted)?;
        let pipe = || Pipe {
            bytes: VecDeque::new(),
            capacity: self.config.directional_buffer_bytes,
            sender_closed: false,
            receiver_open: true,
            pending_reads: VecDeque::new(),
            pending_writes: VecDeque::new(),
            propagation: None,
        };
        self.connections.insert(
            id,
            Connection {
                left: Endpoint {
                    node: left,
                    alive: true,
                    read_open: true,
                    write_open: true,
                },
                right: Endpoint {
                    node: right,
                    alive: true,
                    read_open: true,
                    write_open: true,
                },
                client_binding,
                left_to_right: pipe(),
                right_to_left: pipe(),
            },
        );
        if let Some(address) = client_binding {
            let previous = self.client_bindings.insert(address, id);
            debug_assert!(
                previous.is_none(),
                "client binding was checked before insertion"
            );
        }
        Ok(id)
    }

    fn check_connection_capacity(&self) -> Result<(), NetworkError> {
        if self.connections.len() >= self.config.max_connections {
            return Err(NetworkError::ResourceExhausted {
                resource: "connections",
                limit: self.config.max_connections,
            });
        }
        if self.next_connection_id == u64::MAX {
            return Err(NetworkError::IdentifierExhausted);
        }
        Ok(())
    }

    fn set_link(&mut self, link: LinkKey, config: LinkConfig) -> Result<(), NetworkError> {
        self.purge_empty_blocked_links();
        if config == self.config.default_link {
            self.links.remove(&link);
        } else {
            if !self.links.contains_key(&link) && self.links.len() >= self.config.max_link_overrides
            {
                return Err(NetworkError::ResourceExhausted {
                    resource: "link overrides",
                    limit: self.config.max_link_overrides,
                });
            }
            self.links.insert(link, config);
        }
        if config.state != LinkState::Open {
            return Ok(());
        }
        let connection_ids: Vec<u64> = self.connections.keys().copied().collect();
        for connection in connection_ids {
            self.service_direction(connection, Direction::LeftToRight, Some(link));
            self.service_direction(connection, Direction::RightToLeft, Some(link));
        }
        if let Some(blocked) = self.blocked.remove(&link) {
            for gate in blocked {
                gate.open();
            }
        }
        Ok(())
    }

    fn link_config(&self, key: LinkKey) -> LinkConfig {
        self.links
            .get(&key)
            .copied()
            .unwrap_or(self.config.default_link)
    }

    fn pending_faults(&self) -> usize {
        self.faults.iter().map(VecDeque::len).sum()
    }

    fn take_fault(&mut self, operation: NetworkOperationKind) -> FaultPlan {
        let Some(fault) = self.faults[operation.index()].pop_front() else {
            return FaultPlan::default();
        };
        debug_assert_eq!(fault.operation, operation);
        self.fault_hits = self.fault_hits.saturating_add(1);
        FaultPlan {
            extra_latency: fault.extra_latency,
            max_bytes: fault.max_bytes,
            outcome: fault.outcome,
        }
    }

    fn prepare_completion<T: 'static>(
        &mut self,
        cell: &Rc<LocalCell<T>>,
        links: &[(LinkKey, LinkConfig)],
        extra_latency: SimDuration,
    ) -> Result<(), NetworkError> {
        self.prepare_completion_with_retention(cell, links, extra_latency, true)
    }

    fn prepare_completion_with_retention<T: 'static>(
        &mut self,
        cell: &Rc<LocalCell<T>>,
        links: &[(LinkKey, LinkConfig)],
        extra_latency: SimDuration,
        retain: bool,
    ) -> Result<(), NetworkError> {
        self.purge_empty_blocked_links();
        let mut new_blocked_links = 0usize;
        for (index, (link, config)) in links.iter().enumerate() {
            if config.state != LinkState::Clogged
                || self.blocked.contains_key(link)
                || links[..index].iter().any(|(earlier, earlier_config)| {
                    earlier_config.state == LinkState::Clogged && earlier == link
                })
            {
                continue;
            }
            new_blocked_links += 1;
        }
        if self
            .blocked
            .len()
            .checked_add(new_blocked_links)
            .is_none_or(|needed| needed > self.config.max_blocked_links)
        {
            return Err(NetworkError::ResourceExhausted {
                resource: "blocked links",
                limit: self.config.max_blocked_links,
            });
        }
        let delay = links
            .iter()
            .try_fold(SimDuration::ZERO, |delay, (_, config)| {
                delay.checked_add(config.latency)
            })
            .and_then(|delay| delay.checked_add(self.latency.jitter()))
            .and_then(|delay| delay.checked_add(extra_latency));
        let Some(delay) = delay else {
            return Err(NetworkError::InvalidRequest {
                reason: "network completion latency overflowed",
            });
        };
        if !schedule_delay(&self.handle, Rc::clone(cell), delay) {
            return Err(NetworkError::CompletionDriverUnavailable);
        }
        for (link, config) in links {
            if config.state == LinkState::Clogged {
                cell.close_gate();
                self.register_blocked(*link, cell, retain);
            }
        }
        Ok(())
    }

    fn purge_empty_blocked_links(&mut self) {
        self.blocked.retain(|_, entries| {
            entries.retain(BlockedGate::live);
            !entries.is_empty()
        });
    }

    fn submit_read(
        state: &Rc<RefCell<Self>>,
        connection: u64,
        side: Side,
        request: ReadRequest,
    ) -> SimOperation<CompletionResult<ReadResult, NetworkFailure>> {
        let Some(result_len) = request.buffer.len().checked_add(request.max_bytes) else {
            return ready_error(NetworkFailure::with_buffer(
                NetworkError::InvalidRequest {
                    reason: "read result size overflowed",
                },
                request.buffer,
                0,
            ));
        };
        let mut inner = state.borrow_mut();
        inner.advance_propagation(connection);
        if request.buffer.capacity() > inner.config.max_operation_bytes
            || result_len > inner.config.max_operation_bytes
        {
            return ready_error(NetworkFailure::with_buffer(
                NetworkError::InvalidRequest {
                    reason: "read request exceeds max_operation_bytes",
                },
                request.buffer,
                0,
            ));
        }
        let Some(permit) = inner.permits.acquire() else {
            return ready_error(NetworkFailure::with_buffer(
                NetworkError::ResourceExhausted {
                    resource: "inflight operations",
                    limit: inner.config.max_inflight_operations,
                },
                request.buffer,
                0,
            ));
        };
        // A read charges the allocation it pins: the buffer's capacity, or the
        // length the buffer must grow to at completion if that is larger. The
        // budget bounds resident caller memory, so an over-provisioned pooled
        // buffer charges its full size, not the portion this request fills.
        // Returning here drops `permit`, so a refused byte reservation
        // releases the operation reservation it was paired with.
        let charge = result_len.max(request.buffer.capacity());
        let Some(byte_permit) = inner.read_byte_permits.acquire_many(charge) else {
            return ready_error(NetworkFailure::with_buffer(
                NetworkError::ResourceExhausted {
                    resource: "outstanding read bytes",
                    limit: inner.config.max_outstanding_read_bytes,
                },
                request.buffer,
                0,
            ));
        };
        let permit = LocalAdmission::with_bytes(permit, byte_permit);
        let Some(connection_state) = inner.connections.get(&connection) else {
            return ready_error(NetworkFailure::with_buffer(
                NetworkError::ConnectionClosed,
                request.buffer,
                0,
            ));
        };
        if !endpoint(connection_state, side).read_open {
            return ready_error(NetworkFailure::with_buffer(
                NetworkError::ConnectionClosed,
                request.buffer,
                0,
            ));
        }
        let link = LinkKey {
            from: endpoint(connection_state, side.other()).node,
            to: endpoint(connection_state, side).node,
        };
        let link_config = inner.link_config(link);
        if link_config.state == LinkState::Partitioned {
            return ready_error(NetworkFailure::with_buffer(
                NetworkError::Partitioned { link },
                request.buffer,
                0,
            ));
        }
        let fault = inner.take_fault(NetworkOperationKind::Read);
        let cell = Rc::new(LocalCell::with_delay(permit));
        let operation_future = SimOperation::from_cell(Rc::clone(&cell));
        match fault.outcome {
            FaultOutcome::FailBefore { tag } => {
                inner.fail_before(
                    &cell,
                    &[(link, link_config)],
                    fault.extra_latency,
                    tag,
                    move |error| NetworkFailure::with_buffer(error, request.buffer, 0),
                );
            }
            FaultOutcome::StallBefore => {
                inner.stall_operation(
                    &cell,
                    NetworkFailure::with_buffer(
                        NetworkError::CompletionDriverUnavailable,
                        request.buffer,
                        0,
                    ),
                );
            }
            FaultOutcome::Continue | FaultOutcome::FailAfter { .. } => {
                let connection_state = inner
                    .connections
                    .get_mut(&connection)
                    .expect("connection was checked above");
                incoming_pipe_mut(connection_state, side)
                    .pending_reads
                    .push_back(PendingRead {
                        request,
                        link_config,
                        wait_for_open: link_config.state == LinkState::Clogged,
                        fault,
                        cell: Rc::clone(&cell),
                    });
                drop(inner);
                state
                    .borrow_mut()
                    .service_direction(connection, incoming_direction(side), None);
            }
        }
        operation_future
    }

    fn prepare_write_completion(
        &mut self,
        cell: &LocalWriteCell,
        links: &[(LinkKey, LinkConfig)],
        latency: SimDuration,
    ) -> Result<(), NetworkError> {
        match cell {
            LocalWriteCell::Contiguous(cell) => self.prepare_completion(cell, links, latency),
            LocalWriteCell::Vectored(cell) => self.prepare_completion(cell, links, latency),
        }
    }

    fn submit_write(
        state: &Rc<RefCell<Self>>,
        connection: u64,
        side: Side,
        request: WriteData,
    ) -> LocalWriteResponse {
        let mut inner = state.borrow_mut();
        inner.advance_propagation(connection);
        let retained_bytes = match request.validate(inner.config.max_operation_bytes) {
            Ok(bytes) => bytes,
            Err(error) => {
                return LocalWriteResponse::ready(Err(CompletionError::not_applied(
                    request.failure(error, 0),
                )));
            }
        };
        let Some(permit) = inner.permits.acquire() else {
            return LocalWriteResponse::ready(Err(CompletionError::not_applied(request.failure(
                NetworkError::ResourceExhausted {
                    resource: "inflight operations",
                    limit: inner.config.max_inflight_operations,
                },
                0,
            ))));
        };
        // A write charges the allocation it pins — the buffer's capacity, not
        // just the payload length — so an over-provisioned pooled buffer
        // charges its full size. Returning here drops `permit`, so a refused
        // byte reservation releases the operation reservation it was paired
        // with.
        let Some(byte_permit) = inner.write_byte_permits.acquire_many(retained_bytes) else {
            return LocalWriteResponse::ready(Err(CompletionError::not_applied(request.failure(
                NetworkError::ResourceExhausted {
                    resource: "outstanding write bytes",
                    limit: inner.config.max_outstanding_write_bytes,
                },
                0,
            ))));
        };
        let permit = LocalAdmission::with_bytes(permit, byte_permit);
        let Some(connection_state) = inner.connections.get(&connection) else {
            return LocalWriteResponse::ready(Err(CompletionError::not_applied(
                request.failure(NetworkError::ConnectionClosed, 0),
            )));
        };
        let local = endpoint(connection_state, side);
        let peer = endpoint(connection_state, side.other());
        if !local.alive || !peer.alive || !peer.read_open {
            return LocalWriteResponse::ready(Err(CompletionError::not_applied(
                request.failure(NetworkError::ConnectionClosed, 0),
            )));
        }
        if !local.write_open {
            return LocalWriteResponse::ready(Err(CompletionError::not_applied(
                request.failure(NetworkError::WriteClosed, 0),
            )));
        }
        let link = LinkKey {
            from: local.node,
            to: peer.node,
        };
        let link_config = inner.link_config(link);
        if link_config.state == LinkState::Partitioned {
            return LocalWriteResponse::ready(Err(CompletionError::not_applied(
                request.failure(NetworkError::Partitioned { link }, 0),
            )));
        }
        // Reserve queue metadata before consuming a fault or scheduling effects.
        // The operation permit bounds queue growth even for abandoned responses.
        if outgoing_direction(side)
            .pipe_mut(
                inner
                    .connections
                    .get_mut(&connection)
                    .expect("connection was checked above"),
            )
            .pending_writes
            .try_reserve(1)
            .is_err()
        {
            return LocalWriteResponse::ready(Err(CompletionError::not_applied(request.failure(
                NetworkError::ResourceExhausted {
                    resource: "pending write allocation",
                    limit: inner.config.max_inflight_operations,
                },
                0,
            ))));
        }
        let (cell, future) = LocalWriteCell::with_delay(permit, request.is_vectored());
        let fault = inner.take_fault(NetworkOperationKind::Write);
        if matches!(
            fault.outcome,
            FaultOutcome::FailBefore { .. } | FaultOutcome::StallBefore
        ) {
            inner.execute_admitted_write(
                connection,
                outgoing_direction(side),
                request,
                link,
                link_config,
                fault,
                cell,
            );
            return future;
        }
        let direction = outgoing_direction(side);
        let pipe = direction.pipe(
            inner
                .connections
                .get(&connection)
                .expect("connection was checked above"),
        );
        let available = pipe.capacity.saturating_sub(pipe.bytes.len());
        if !request.is_empty() && available == 0 {
            direction
                .pipe_mut(
                    inner
                        .connections
                        .get_mut(&connection)
                        .expect("connection was checked above"),
                )
                .pending_writes
                .push_back(PendingWrite {
                    request,
                    link,
                    link_config,
                    wait_for_open: link_config.state == LinkState::Clogged,
                    fault,
                    cell,
                });
            return future;
        }
        inner.execute_admitted_write(
            connection,
            direction,
            request,
            link,
            link_config,
            fault,
            cell,
        );
        // Pending operations carry their own admission-time gate. Servicing
        // here lets an earlier Open-admitted read progress even if this write
        // was admitted after the link became clogged.
        inner.service_direction(connection, direction, None);
        future
    }

    #[allow(clippy::too_many_arguments)]
    fn execute_admitted_write(
        &mut self,
        connection: u64,
        direction: Direction,
        request: WriteData,
        link: LinkKey,
        link_config: LinkConfig,
        fault: FaultPlan,
        cell: LocalWriteCell,
    ) {
        if fault.outcome == FaultOutcome::StallBefore {
            cell.complete(Err(CompletionError::not_applied(
                request.failure(NetworkError::CompletionDriverUnavailable, 0),
            )));
            cell.close_gate();
            cell.mark_delay_elapsed();
            match &cell {
                LocalWriteCell::Contiguous(cell) => self.retain_stalled(cell),
                LocalWriteCell::Vectored(cell) => self.retain_stalled(cell),
            }
            return;
        }
        if let FaultOutcome::FailBefore { tag } = fault.outcome {
            match self.prepare_write_completion(&cell, &[(link, link_config)], fault.extra_latency)
            {
                Ok(()) => cell.complete(Err(CompletionError::not_applied(
                    request.failure(NetworkError::Injected { tag }, 0),
                ))),
                Err(error) => cell.complete_immediately(Err(CompletionError::not_applied(
                    request.failure(error, 0),
                ))),
            }
            return;
        }
        if let Err(error) =
            self.prepare_write_completion(&cell, &[(link, link_config)], fault.extra_latency)
        {
            cell.complete_immediately(Err(CompletionError::not_applied(request.failure(error, 0))));
            return;
        }
        let pipe = direction.pipe(
            self.connections
                .get(&connection)
                .expect("admitted write connection exists"),
        );
        let available = pipe.capacity.saturating_sub(pipe.bytes.len());
        let fault_limit = fault.max_bytes.unwrap_or(usize::MAX);
        let bytes_written = request
            .len()
            .min(available)
            .min(link_config.max_chunk_bytes)
            .min(fault_limit);
        if let Err(error) = self.prepare_propagation(connection, direction, bytes_written) {
            cell.complete(Err(CompletionError::not_applied(request.failure(error, 0))));
            return;
        }
        if let Err(error) = request.append_prefix(
            &mut direction
                .pipe_mut(
                    self.connections
                        .get_mut(&connection)
                        .expect("admitted write connection exists"),
                )
                .bytes,
            bytes_written,
        ) {
            self.rollback_propagation(connection, direction, bytes_written);
            cell.complete(Err(CompletionError::not_applied(request.failure(error, 0))));
            return;
        }
        let output = match fault.outcome {
            FaultOutcome::FailAfter { tag, certainty } => {
                Err(certainty.error(request.failure(NetworkError::Injected { tag }, bytes_written)))
            }
            FaultOutcome::Continue => Ok(request.success(bytes_written)),
            FaultOutcome::FailBefore { .. } | FaultOutcome::StallBefore => unreachable!(),
        };
        cell.complete(output);
    }

    fn submit_shutdown_write(
        state: &Rc<RefCell<Self>>,
        connection: u64,
        side: Side,
    ) -> SimOperation<CompletionResult<(), NetworkFailure>> {
        let mut inner = state.borrow_mut();
        inner.advance_propagation(connection);
        let Some(permit) = inner.permits.acquire() else {
            return ready_error(NetworkFailure::without_buffer(
                NetworkError::ResourceExhausted {
                    resource: "inflight operations",
                    limit: inner.config.max_inflight_operations,
                },
            ));
        };
        let Some(connection_state) = inner.connections.get(&connection) else {
            return ready_error(NetworkFailure::without_buffer(
                NetworkError::ConnectionClosed,
            ));
        };
        let link = outgoing_direction(side).link(connection_state);
        let link_config = inner.link_config(link);
        let fault = inner.take_fault(NetworkOperationKind::ShutdownWrite);
        let cell = Rc::new(LocalCell::with_delay(Some(permit)));
        let future = SimOperation::from_cell(Rc::clone(&cell));
        if let FaultOutcome::FailBefore { tag } = fault.outcome {
            inner.fail_before(
                &cell,
                &[(link, link_config)],
                fault.extra_latency,
                tag,
                NetworkFailure::without_buffer,
            );
            return future;
        }
        if fault.outcome == FaultOutcome::StallBefore {
            inner.stall_operation(
                &cell,
                NetworkFailure::without_buffer(NetworkError::CompletionDriverUnavailable),
            );
            return future;
        }
        if let Err(error) =
            inner.prepare_completion(&cell, &[(link, link_config)], fault.extra_latency)
        {
            complete_immediately(
                &cell,
                Err(CompletionError::not_applied(
                    NetworkFailure::without_buffer(error),
                )),
            );
            return future;
        }
        let pending_writes = {
            let connection_state = inner
                .connections
                .get_mut(&connection)
                .expect("connection was checked before fault assignment");
            endpoint_mut(connection_state, side).write_open = false;
            outgoing_pipe_mut(connection_state, side).sender_closed = true;
            std::mem::take(&mut outgoing_pipe_mut(connection_state, side).pending_writes)
        };
        complete_pending_writes(pending_writes, NetworkError::WriteClosed);
        let output = control_output(fault.outcome);
        cell.complete(output);
        inner.service_direction(connection, outgoing_direction(side), None);
        future
    }

    fn submit_close(
        state: &Rc<RefCell<Self>>,
        connection: u64,
        side: Side,
    ) -> SimOperation<CompletionResult<(), NetworkFailure>> {
        let mut inner = state.borrow_mut();
        let permit = inner.permits.acquire();
        if permit.is_none() {
            return ready_error(NetworkFailure::without_buffer(
                NetworkError::ResourceExhausted {
                    resource: "inflight operations",
                    limit: inner.config.max_inflight_operations,
                },
            ));
        }
        let cell = Rc::new(LocalCell::with_delay(permit));
        let future = SimOperation::from_cell(Rc::clone(&cell));
        let link_plan = inner.connections.get(&connection).map(|connection_state| {
            let link = outgoing_direction(side).link(connection_state);
            (link, inner.link_config(link))
        });
        let links = link_plan.as_slice();
        let fault = inner.take_fault(NetworkOperationKind::Close);
        if let FaultOutcome::FailBefore { tag } = fault.outcome {
            inner.fail_before(
                &cell,
                links,
                fault.extra_latency,
                tag,
                NetworkFailure::without_buffer,
            );
            return future;
        }
        if fault.outcome == FaultOutcome::StallBefore {
            inner.stall_operation(
                &cell,
                NetworkFailure::without_buffer(NetworkError::CompletionDriverUnavailable),
            );
            return future;
        }
        if let Err(error) = inner.prepare_completion(&cell, links, fault.extra_latency) {
            complete_immediately(
                &cell,
                Err(CompletionError::not_applied(
                    NetworkFailure::without_buffer(error),
                )),
            );
            return future;
        }
        inner.close_side_immediately(connection, side);
        cell.complete(control_output(fault.outcome));
        future
    }

    fn service_direction(
        &mut self,
        connection_id: u64,
        direction: Direction,
        only_link: Option<LinkKey>,
    ) {
        loop {
            self.advance_propagation(connection_id);
            let Some(link) = self
                .connections
                .get(&connection_id)
                .map(|connection| direction.link(connection))
            else {
                return;
            };
            if only_link.is_some_and(|expected| expected != link) {
                return;
            }
            let link_is_open = self.link_config(link).state == LinkState::Open;
            if link_is_open {
                let pipe = direction.pipe_mut(
                    self.connections
                        .get_mut(&connection_id)
                        .expect("connection still exists"),
                );
                for pending in &mut pipe.pending_reads {
                    if pending.wait_for_open {
                        pending.wait_for_open = false;
                        pending.link_config.state = LinkState::Open;
                    }
                }
                for pending in &mut pipe.pending_writes {
                    if pending.wait_for_open {
                        pending.wait_for_open = false;
                        pending.link_config.state = LinkState::Open;
                    }
                }
            }
            let (read_ready, write_ready, sender_closed, receiver_open) = {
                let connection = self
                    .connections
                    .get(&connection_id)
                    .expect("connection still exists");
                let pipe = direction.pipe(connection);
                let available = pipe.visible_bytes();
                (
                    pipe.pending_reads.front().is_some_and(|pending| {
                        !pending.wait_for_open
                            && (!pipe.receiver_open
                                || available != 0
                                || (pipe.sender_closed && pipe.bytes.is_empty())
                                || pending.request.max_bytes == 0)
                    }),
                    pipe.pending_writes.front().is_some_and(|pending| {
                        !pending.wait_for_open
                            && pipe.receiver_open
                            && !pipe.sender_closed
                            && pipe.bytes.len() < pipe.capacity
                    }),
                    pipe.sender_closed,
                    pipe.receiver_open,
                )
            };
            if read_ready {
                let mut pending = direction
                    .pipe_mut(
                        self.connections
                            .get_mut(&connection_id)
                            .expect("connection still exists"),
                    )
                    .pending_reads
                    .pop_front()
                    .expect("ready read was observed");
                if !receiver_open {
                    complete_immediately(
                        &pending.cell,
                        Err(CompletionError::not_applied(NetworkFailure::with_buffer(
                            NetworkError::ConnectionClosed,
                            pending.request.buffer,
                            0,
                        ))),
                    );
                    continue;
                }
                let available = direction
                    .pipe(
                        self.connections
                            .get(&connection_id)
                            .expect("connection still exists"),
                    )
                    .visible_bytes();
                let fault_limit = pending.fault.max_bytes.unwrap_or(usize::MAX);
                let bytes_read = available
                    .min(pending.request.max_bytes)
                    .min(pending.link_config.max_chunk_bytes)
                    .min(fault_limit);
                if let Err(error) = self.prepare_completion(
                    &pending.cell,
                    &[(link, pending.link_config)],
                    pending.fault.extra_latency,
                ) {
                    complete_immediately(
                        &pending.cell,
                        Err(CompletionError::not_applied(NetworkFailure::with_buffer(
                            error,
                            pending.request.buffer,
                            0,
                        ))),
                    );
                    continue;
                }
                if bytes_read > 0 {
                    let pipe = direction.pipe_mut(
                        self.connections
                            .get_mut(&connection_id)
                            .expect("connection still exists"),
                    );
                    pending
                        .request
                        .buffer
                        .extend(pipe.bytes.drain(..bytes_read));
                }
                let end_of_stream = {
                    let pipe = direction.pipe(
                        self.connections
                            .get(&connection_id)
                            .expect("connection still exists"),
                    );
                    pending.request.max_bytes != 0
                        && bytes_read == 0
                        && pipe.sender_closed
                        && pipe.bytes.is_empty()
                };
                let output = match pending.fault.outcome {
                    FaultOutcome::Continue => Ok(ReadResult {
                        buffer: pending.request.buffer,
                        bytes_read,
                        end_of_stream,
                    }),
                    FaultOutcome::FailAfter { tag, certainty } => {
                        Err(certainty.error(NetworkFailure::with_buffer(
                            NetworkError::Injected { tag },
                            pending.request.buffer,
                            bytes_read,
                        )))
                    }
                    FaultOutcome::FailBefore { .. } | FaultOutcome::StallBefore => unreachable!(),
                };
                pending.cell.complete(output);
                continue;
            }
            if write_ready {
                let pending = direction
                    .pipe_mut(
                        self.connections
                            .get_mut(&connection_id)
                            .expect("connection still exists"),
                    )
                    .pending_writes
                    .pop_front()
                    .expect("ready write was observed");
                debug_assert_eq!(pending.link, link);
                self.execute_admitted_write(
                    connection_id,
                    direction,
                    pending.request,
                    pending.link,
                    pending.link_config,
                    pending.fault,
                    pending.cell,
                );
                continue;
            }
            if !receiver_open || sender_closed {
                let Some(pending) = direction
                    .pipe_mut(
                        self.connections
                            .get_mut(&connection_id)
                            .expect("connection still exists"),
                    )
                    .pending_writes
                    .pop_front()
                else {
                    return;
                };
                let error = if receiver_open {
                    NetworkError::WriteClosed
                } else {
                    NetworkError::ConnectionClosed
                };
                pending
                    .cell
                    .complete_immediately(Err(CompletionError::not_applied(
                        pending.request.failure(error, 0),
                    )));
                continue;
            }
            return;
        }
    }

    fn register_blocked<T: 'static>(
        &mut self,
        link: LinkKey,
        cell: &Rc<LocalCell<T>>,
        retain: bool,
    ) {
        let entries = self.blocked.entry(link).or_default();
        entries.retain(BlockedGate::live);
        let erased: Rc<dyn OpenGate> = cell.clone();
        entries.push(if retain {
            BlockedGate::Retained(erased)
        } else {
            BlockedGate::Endpoint(Rc::downgrade(&erased))
        });
    }

    fn close_side_immediately(&mut self, connection_id: u64, side: Side) {
        if self.connections.get(&connection_id).is_some_and(|pair| {
            pair.left_to_right.propagation.is_some() || pair.right_to_left.propagation.is_some()
        }) {
            self.retire_propagating_pair(connection_id);
            return;
        }
        let Some(connection) = self.connections.get_mut(&connection_id) else {
            return;
        };
        let local = endpoint_mut(connection, side);
        if !local.alive {
            return;
        }
        local.alive = false;
        local.read_open = false;
        local.write_open = false;
        let released_client_binding = if side == Side::Left {
            connection.client_binding.take()
        } else {
            None
        };
        outgoing_pipe_mut(connection, side).sender_closed = true;
        incoming_pipe_mut(connection, side).receiver_open = false;
        incoming_pipe_mut(connection, side).bytes.clear();
        let outgoing_writes =
            std::mem::take(&mut outgoing_pipe_mut(connection, side).pending_writes);
        let incoming_writes =
            std::mem::take(&mut incoming_pipe_mut(connection, side).pending_writes);
        let pending = std::mem::take(&mut incoming_pipe_mut(connection, side).pending_reads);
        complete_pending_writes(outgoing_writes, NetworkError::ConnectionClosed);
        complete_pending_writes(incoming_writes, NetworkError::ConnectionClosed);
        for read in pending {
            complete_immediately(
                &read.cell,
                Err(CompletionError::not_applied(NetworkFailure::with_buffer(
                    NetworkError::ConnectionClosed,
                    read.request.buffer,
                    0,
                ))),
            );
        }
        if let Some(address) = released_client_binding {
            self.client_bindings.remove(&address);
        }
        self.service_direction(connection_id, outgoing_direction(side), None);
        let remove = self
            .connections
            .get(&connection_id)
            .is_some_and(|connection| !connection.left.alive && !connection.right.alive);
        if remove {
            self.connections.remove(&connection_id);
        }
    }
}

#[derive(Clone, Copy)]
enum Direction {
    LeftToRight,
    RightToLeft,
}

impl Direction {
    fn link(self, connection: &Connection) -> LinkKey {
        match self {
            Self::LeftToRight => LinkKey {
                from: connection.left.node,
                to: connection.right.node,
            },
            Self::RightToLeft => LinkKey {
                from: connection.right.node,
                to: connection.left.node,
            },
        }
    }

    fn pipe(self, connection: &Connection) -> &Pipe {
        match self {
            Self::LeftToRight => &connection.left_to_right,
            Self::RightToLeft => &connection.right_to_left,
        }
    }

    fn pipe_mut(self, connection: &mut Connection) -> &mut Pipe {
        match self {
            Self::LeftToRight => &mut connection.left_to_right,
            Self::RightToLeft => &mut connection.right_to_left,
        }
    }
}

impl Side {
    const fn other(self) -> Self {
        match self {
            Self::Left => Self::Right,
            Self::Right => Self::Left,
        }
    }
}

fn endpoint(connection: &Connection, side: Side) -> Endpoint {
    match side {
        Side::Left => connection.left,
        Side::Right => connection.right,
    }
}

fn endpoint_mut(connection: &mut Connection, side: Side) -> &mut Endpoint {
    match side {
        Side::Left => &mut connection.left,
        Side::Right => &mut connection.right,
    }
}

fn outgoing_direction(side: Side) -> Direction {
    match side {
        Side::Left => Direction::LeftToRight,
        Side::Right => Direction::RightToLeft,
    }
}

fn incoming_direction(side: Side) -> Direction {
    outgoing_direction(side.other())
}

fn outgoing_pipe_mut(connection: &mut Connection, side: Side) -> &mut Pipe {
    outgoing_direction(side).pipe_mut(connection)
}

fn incoming_pipe_mut(connection: &mut Connection, side: Side) -> &mut Pipe {
    incoming_direction(side).pipe_mut(connection)
}

fn complete_pending_writes(pending: VecDeque<PendingWrite>, error: NetworkError) {
    for write in pending {
        write
            .cell
            .complete_immediately(Err(CompletionError::not_applied(
                write.request.failure(error.clone(), 0),
            )));
    }
}

fn validate_config(config: NetworkConfig) -> Result<(), NetworkError> {
    if config.max_listeners == 0 {
        return Err(NetworkError::InvalidConfig {
            reason: "max_listeners must be nonzero",
        });
    }
    if config.max_listener_backlog == 0 {
        return Err(NetworkError::InvalidConfig {
            reason: "max_listener_backlog must be nonzero",
        });
    }
    if config.max_connections == 0 {
        return Err(NetworkError::InvalidConfig {
            reason: "max_connections must be nonzero",
        });
    }
    if config.max_inflight_operations == 0 {
        return Err(NetworkError::InvalidConfig {
            reason: "max_inflight_operations must be nonzero",
        });
    }
    if config.directional_buffer_bytes == 0 {
        return Err(NetworkError::InvalidConfig {
            reason: "directional_buffer_bytes must be nonzero",
        });
    }
    if config.max_operation_bytes == 0 {
        return Err(NetworkError::InvalidConfig {
            reason: "max_operation_bytes must be nonzero",
        });
    }
    // A budget below the per-operation maximum would reject a request that
    // max_operation_bytes accepts, so the two limits would disagree about what
    // is admissible. Refuse the configuration rather than resolve it at runtime.
    if config.max_outstanding_read_bytes < config.max_operation_bytes {
        return Err(NetworkError::InvalidConfig {
            reason: "max_outstanding_read_bytes is below max_operation_bytes",
        });
    }
    if config.max_outstanding_write_bytes < config.max_operation_bytes {
        return Err(NetworkError::InvalidConfig {
            reason: "max_outstanding_write_bytes is below max_operation_bytes",
        });
    }
    if config.max_scripted_faults == 0 {
        return Err(NetworkError::InvalidConfig {
            reason: "max_scripted_faults must be nonzero",
        });
    }
    if config.max_link_overrides == 0 {
        return Err(NetworkError::InvalidConfig {
            reason: "max_link_overrides must be nonzero",
        });
    }
    if config.max_blocked_links == 0 {
        return Err(NetworkError::InvalidConfig {
            reason: "max_blocked_links must be nonzero",
        });
    }
    if config.default_link.max_chunk_bytes == 0 {
        return Err(NetworkError::InvalidConfig {
            reason: "default link max_chunk_bytes must be nonzero",
        });
    }
    Ok(())
}

fn schedule_delay<T: 'static>(handle: &Handle, cell: Rc<LocalCell<T>>, delay: SimDuration) -> bool {
    if delay == SimDuration::ZERO {
        cell.mark_delay_elapsed();
        return true;
    }
    let sleep = handle.sleep(delay);
    let completion = DelayCompletion { cell };
    handle
        .spawn(async move {
            // The guard also runs if runtime shutdown drops this task before it
            // is first polled or while its timer is pending.
            let _completion = completion;
            let _timer_result = sleep.await;
        })
        .is_ok()
}

struct DelayCompletion<T> {
    cell: Rc<LocalCell<T>>,
}

impl<T> Drop for DelayCompletion<T> {
    fn drop(&mut self) {
        self.cell.mark_delay_elapsed();
    }
}

fn complete_immediately<T>(cell: &LocalCell<T>, output: T) {
    cell.complete(output);
    cell.mark_delay_elapsed();
}

fn control_output(outcome: FaultOutcome) -> CompletionResult<(), NetworkFailure> {
    match outcome {
        FaultOutcome::Continue => Ok(()),
        FaultOutcome::FailAfter { tag, certainty } => Err(certainty.error(
            NetworkFailure::without_buffer(NetworkError::Injected { tag }),
        )),
        FaultOutcome::FailBefore { .. } | FaultOutcome::StallBefore => unreachable!(),
    }
}

fn ready_error<T, E>(failure: E) -> SimOperation<CompletionResult<T, E>> {
    SimOperation::ready(Err(CompletionError::not_applied(failure)))
}
