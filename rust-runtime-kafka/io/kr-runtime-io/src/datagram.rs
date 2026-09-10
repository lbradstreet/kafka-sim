//! Owned atomic-datagram transport contracts.
//!
//! This module is parallel to [`crate::network::ByteStreamSubmit`]: a datagram
//! is one message with one source and one destination, not a partial byte
//! stream. Application code uses the cold [`ColdDatagramNetwork`] and
//! [`ColdDatagramSocket`] handles, where a never-polled operation has not
//! started. The warm `submit_*` traits below them admit operations eagerly
//! when a method is called and return owned `'static` futures. In both
//! layers, dropping a future after admission abandons only the response; it
//! does not cancel an admitted send or receive. In particular, a dropped
//! admitted receive may still consume one datagram.
//!
//! Every admitted operation is bounded by provider configuration. Providers
//! must reject work they cannot retain with [`DatagramError::ResourceExhausted`]
//! rather than grow an unbounded queue. [`DatagramSocketSubmit::submit_try_recv_from`]
//! lets callers drain already-available datagrams without waiting for a future
//! arrival. Batch operations are intentionally not part of this baseline
//! contract; a future batch extension must preserve per-datagram atomicity and
//! define its own bounded item count without implying atomicity for the batch.
//! A provider may bound concurrently admitted receives as tightly as one; a
//! receive rejected solely by that concurrency bound returns
//! [`DatagramError::ResourceExhausted`] with `NotApplied` certainty and its
//! owned buffer. Neither [`DatagramProviderSubmit`] nor [`DatagramSocketSubmit`] requires
//! `Send` or `Sync`. Providers advertise those capabilities on their concrete
//! types when they support cross-thread use.
//!
//! [`MemoryDatagramNetwork`] is the bounded, thread-safe, host-I/O-free
//! provider for portable actors. [`SimDatagramNetwork`] remains the richer
//! owner-thread provider for deterministic latency and fault campaigns.

use crate::network::NetworkAddress;
use kr_runtime::{CompletionResult, SimInstant};
use std::error::Error;
use std::fmt;
use std::future::Future;

mod cold;
mod memory;
mod sim;

pub use cold::{ColdDatagramNetwork, ColdDatagramSocket};
pub use memory::{
    MemoryDatagramConfig, MemoryDatagramNetwork, MemoryDatagramOperation, MemoryDatagramSocket,
    MemoryDatagramStatus,
};

pub use sim::{
    DatagramDirection, ScriptedDatagramFault, SimDatagramAfterEnqueueCertainty, SimDatagramConfig,
    SimDatagramCounters, SimDatagramFault, SimDatagramLinkConfig, SimDatagramNetwork,
    SimDatagramOperation, SimDatagramSocket, SimDatagramStatus,
};

/// An owned request to bind one datagram socket.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DatagramBindRequest<A> {
    /// Provider-native local address to bind exclusively.
    ///
    /// A provider may define an address such as IP port zero to request a
    /// provider-selected concrete address. Read the result with
    /// [`DatagramSocketSubmit::local_addr`].
    pub address: A,
}

/// An owned request to send one atomic datagram.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SendToRequest<A> {
    /// Payload retained by the provider until terminal completion.
    ///
    /// An empty payload is still a datagram and must not be optimized away.
    pub buffer: Vec<u8>,
    /// Provider-native destination address.
    pub destination: A,
}

/// A successful atomic datagram send.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SendToResult {
    /// Original caller-owned payload buffer.
    pub buffer: Vec<u8>,
    /// Number of bytes locally accepted.
    ///
    /// Datagram atomicity requires this to equal `buffer.len()` on success.
    pub bytes_sent: usize,
}

/// An owned request to receive one datagram.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RecvFromRequest {
    /// Buffer whose existing prefix is preserved.
    pub buffer: Vec<u8>,
    /// Maximum payload bytes to append during this completion.
    ///
    /// A zero value still consumes one available empty or nonempty datagram.
    /// A nonempty datagram then completes with
    /// [`DatagramTruncation::Truncated`].
    pub max_bytes: usize,
}

/// Whether a receive buffer held the complete datagram payload.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum DatagramTruncation {
    /// Every payload byte was appended to the returned buffer.
    Complete,
    /// Only a prefix fit; the remainder was discarded atomically.
    ///
    /// The discarded suffix can never be returned by a later receive.
    Truncated,
}

/// A successful receive of exactly one datagram.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RecvFromResult<A> {
    /// Original prefix followed by the payload bytes that fit.
    pub buffer: Vec<u8>,
    /// Payload bytes appended to `buffer` by this operation.
    pub bytes_received: usize,
    /// Full datagram payload length before receive-buffer truncation.
    ///
    /// This is greater than `bytes_received` exactly when `truncation` is
    /// [`DatagramTruncation::Truncated`].
    pub datagram_len: usize,
    /// Provider-native source address reported for this datagram.
    pub source: A,
    /// Explicit whole-datagram truncation status.
    pub truncation: DatagramTruncation,
}

/// Stable operation categories reported by datagram providers.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum DatagramOperationKind {
    /// Bind one socket.
    Bind,
    /// Send one datagram.
    SendTo,
    /// Receive one datagram, including deadline and nonblocking variants.
    RecvFrom,
    /// Close one socket.
    Close,
}

/// Stable error categories for datagram transports.
#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum DatagramError {
    /// A simulator or production provider configuration is invalid.
    InvalidConfig { reason: &'static str },
    /// An operation request violates a documented bound or invariant.
    InvalidRequest { reason: &'static str },
    /// A payload cannot be represented as one provider datagram.
    MessageTooLarge {
        /// Known provider maximum, or `None` when the backend reported only
        /// that the payload was too large (for example a path-dependent
        /// `EMSGSIZE` result).
        max_payload_bytes: Option<usize>,
    },
    /// The requested provider-native local address is already bound.
    AddressInUse,
    /// The bound socket and destination address families are incompatible.
    AddressFamilyMismatch,
    /// The socket has closed or no longer refers to a live binding.
    SocketClosed,
    /// A nonblocking receive found no datagram immediately available.
    WouldBlock,
    /// An absolute receive deadline elapsed before a datagram was consumed.
    DeadlineExceeded,
    /// A bounded resource cannot admit more work.
    ResourceExhausted {
        resource: &'static str,
        limit: usize,
    },
    /// A simulated directional partition rejected or dropped the operation.
    Partitioned,
    /// A deterministic fault rule fired.
    Injected { tag: u64 },
    /// A simulation runtime could not schedule required completion work.
    CompletionDriverUnavailable,
    /// A production driver stopped before terminalizing normally.
    DriverStopped,
    /// A production backend reported an operating-system or reactor failure.
    Backend {
        operation: DatagramOperationKind,
        raw_os_error: Option<i32>,
        message: String,
    },
}

impl fmt::Display for DatagramError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidConfig { reason } => {
                write!(formatter, "invalid datagram config: {reason}")
            }
            Self::InvalidRequest { reason } => {
                write!(formatter, "invalid datagram request: {reason}")
            }
            Self::MessageTooLarge {
                max_payload_bytes: Some(limit),
            } => write!(
                formatter,
                "datagram exceeds the payload limit of {limit} bytes"
            ),
            Self::MessageTooLarge {
                max_payload_bytes: None,
            } => formatter.write_str("datagram is too large for the transport"),
            Self::AddressInUse => formatter.write_str("datagram address is already in use"),
            Self::AddressFamilyMismatch => {
                formatter.write_str("datagram address families do not match")
            }
            Self::SocketClosed => formatter.write_str("datagram socket is closed"),
            Self::WouldBlock => formatter.write_str("no datagram is immediately available"),
            Self::DeadlineExceeded => formatter.write_str("datagram receive deadline elapsed"),
            Self::ResourceExhausted { resource, limit } => {
                write!(formatter, "{resource} limit of {limit} is exhausted")
            }
            Self::Partitioned => formatter.write_str("datagram direction is partitioned"),
            Self::Injected { tag } => write!(formatter, "injected datagram fault {tag}"),
            Self::CompletionDriverUnavailable => {
                formatter.write_str("datagram completion driver is unavailable")
            }
            Self::DriverStopped => formatter.write_str("datagram driver is stopped"),
            Self::Backend {
                operation,
                raw_os_error,
                message,
            } => {
                write!(formatter, "{operation:?} datagram backend failure")?;
                if let Some(code) = raw_os_error {
                    write!(formatter, " (os error {code})")?;
                }
                write!(formatter, ": {message}")
            }
        }
    }
}

impl Error for DatagramError {}

/// An operation failure that returns caller-owned state and effect diagnostics.
///
/// Send and receive failures return their exact allocation in `buffer`.
/// `bytes_transferred` is diagnostic: it describes bytes known to have
/// participated in the effect or a byte count reported by a backend whose
/// completion violated an invariant. Callers must use the enclosing
/// [`kr_runtime::CompletionError`]'s certainty, not this count alone, to decide
/// whether a send or receive took effect.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DatagramFailure {
    error: DatagramError,
    buffer: Option<Vec<u8>>,
    bytes_transferred: usize,
}

impl DatagramFailure {
    /// Creates a failure for an operation without a byte buffer.
    #[must_use]
    pub fn without_buffer(error: DatagramError) -> Self {
        Self {
            error,
            buffer: None,
            bytes_transferred: 0,
        }
    }

    /// Creates a send or receive failure and returns its owned buffer.
    #[must_use]
    pub fn with_buffer(error: DatagramError, buffer: Vec<u8>, bytes_transferred: usize) -> Self {
        Self {
            error,
            buffer: Some(buffer),
            bytes_transferred,
        }
    }

    /// Returns the stable error category.
    #[must_use]
    pub const fn error(&self) -> &DatagramError {
        &self.error
    }

    /// Returns the known or backend-reported byte count for this failure.
    #[must_use]
    pub const fn bytes_transferred(&self) -> usize {
        self.bytes_transferred
    }

    /// Returns the caller-owned buffer without consuming the failure.
    #[must_use]
    pub fn buffer(&self) -> Option<&[u8]> {
        self.buffer.as_deref()
    }

    /// Returns the caller-owned buffer, when this operation had one.
    #[must_use]
    pub fn into_buffer(self) -> Option<Vec<u8>> {
        self.buffer
    }
}

impl fmt::Display for DatagramFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.error.fmt(formatter)
    }
}

impl Error for DatagramFailure {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        Some(&self.error)
    }
}

/// Factory for independently bound atomic-datagram sockets.
pub trait DatagramProviderSubmit: 'static {
    /// Provider-native address, such as a simulated address or
    /// [`std::net::SocketAddr`].
    type Address: Clone + 'static;
    /// Provider-native absolute monotonic instant used for receive deadlines.
    ///
    /// Callers obtain values from the monotonic clock paired with this
    /// provider; the transport does not also act as a clock.
    type Instant: Clone + Ord + 'static;
    /// Bound socket returned on success.
    type Socket: DatagramSocketSubmit<Address = Self::Address, Instant = Self::Instant>;
    /// Owned bind future.
    type BindResponse: Future<Output = CompletionResult<Self::Socket, DatagramFailure>> + 'static;

    /// Exclusively binds one local address.
    ///
    /// Submission is eager. Dropping the future abandons only its response.
    /// A successful result owns the binding until its socket closes or drops.
    fn submit_bind(&self, request: DatagramBindRequest<Self::Address>) -> Self::BindResponse;
}

/// A [`DatagramProviderSubmit`] safe to share across executor threads.
///
/// The provider, its socket, provider-native address and instant, and its owned
/// bind future all advertise the bounds required by a multi-threaded runtime.
/// Address and instant values cross task boundaries by value and therefore
/// need not themselves be [`Sync`].
/// Implementations are inferred automatically; local simulation providers can
/// continue implementing only [`DatagramProviderSubmit`].
pub trait SendDatagramProviderSubmit:
    DatagramProviderSubmit<
        Address: Send,
        Instant: Send,
        Socket: SendDatagramSocketSubmit,
        BindResponse: Send,
    > + Send
    + Sync
{
}

impl<T> SendDatagramProviderSubmit for T where
    T: DatagramProviderSubmit<
            Address: Send,
            Instant: Send,
            Socket: SendDatagramSocketSubmit,
            BindResponse: Send,
        > + Send
        + Sync
{
}

/// Owned atomic-datagram operations on one bound socket.
///
/// One socket may send to and receive from many peers. The contract promises
/// no ordering between separate sends or between packets from different peers.
/// It also does not promise delivery, deduplication, integrity, or end-to-end
/// identity: those belong to the protocol above this transport.
///
/// When multiple receives are admitted concurrently, there is no portable
/// admission, completion, or packet-assignment order between them. Each
/// datagram is consumed by at most one operation, but a deadline or
/// nonblocking receive may terminalize before an earlier blocking receive.
/// Callers that require deterministic ownership of the next datagram must keep
/// only one receive outstanding on that socket.
pub trait DatagramSocketSubmit: 'static {
    /// Provider-native source and destination address.
    type Address: Clone + 'static;
    /// Provider-native absolute monotonic instant used by
    /// [`Self::submit_recv_from_until`].
    ///
    /// Callers obtain values from the monotonic clock paired with this socket's
    /// provider.
    type Instant: Clone + Ord + 'static;
    /// Owned send future.
    type SendResponse: Future<Output = CompletionResult<SendToResult, DatagramFailure>> + 'static;
    /// Owned receive future shared by blocking, nonblocking, and deadline
    /// receive modes.
    type RecvResponse: Future<Output = CompletionResult<RecvFromResult<Self::Address>, DatagramFailure>>
        + 'static;
    /// Owned close future.
    type ControlResponse: Future<Output = CompletionResult<(), DatagramFailure>> + 'static;

    /// Returns the concrete bound address without driving I/O.
    fn local_addr(&self) -> Self::Address;

    /// Sends exactly one atomic datagram.
    ///
    /// Submission is eager and transfers ownership of the payload until
    /// terminal completion. Success means the entire payload was accepted by
    /// the local transport; it does not guarantee remote delivery. Partial
    /// success is forbidden, including for nonempty payloads.
    ///
    /// On failure, the exact allocation is returned by [`DatagramFailure`]. A
    /// `NotApplied` failure means the datagram was not enqueued; `Applied`
    /// means the whole datagram was enqueued; `MayHaveApplied` means the whole
    /// datagram may have been enqueued. Retrying an ambiguous send requires a
    /// protocol-level deduplication identity.
    ///
    /// For a nonempty payload, any nonnegative backend completion count other
    /// than the full payload length violates this contract rather than forming
    /// a partial success. The provider must return a
    /// [`DatagramError::Backend`] failure with `MayHaveApplied` certainty, the
    /// unchanged buffer, and that completion count in
    /// [`DatagramFailure::bytes_transferred`]. The count does not prove that a
    /// prefix datagram was emitted. The affected I/O driver state must be
    /// retired rather than reused for later data operations because its
    /// completion semantics are no longer safe.
    fn submit_send_to(&self, request: SendToRequest<Self::Address>) -> Self::SendResponse;

    /// Receives and consumes exactly one datagram, waiting if necessary.
    ///
    /// Copied payload bytes are appended after the request buffer's existing
    /// prefix. If the payload does not fit, its suffix is discarded and the
    /// result explicitly reports [`DatagramTruncation::Truncated`]. Dropping
    /// the returned future does not cancel the receive.
    fn submit_recv_from(&self, request: RecvFromRequest) -> Self::RecvResponse;

    /// Attempts to consume one datagram without waiting for packet arrival.
    ///
    /// The operation may wait for bounded provider command admission and for
    /// earlier admitted commands to terminalize, but it must not wait for a
    /// datagram that was unavailable when this receive reached the transport.
    /// In that case it returns [`DatagramError::WouldBlock`] with `NotApplied`
    /// certainty and the unchanged request buffer. It need not complete in
    /// admission order relative to other receives.
    fn submit_try_recv_from(&self, request: RecvFromRequest) -> Self::RecvResponse;

    /// Receives one datagram before an absolute provider-native deadline.
    ///
    /// Queueing time after eager admission counts toward `deadline`. A provider
    /// that cannot track this operation's deadline independently of earlier
    /// receives must reject it at admission with
    /// [`DatagramError::ResourceExhausted`] and `NotApplied` certainty. Once
    /// admitted, the receive must terminalize when the provider's completion
    /// driver observes that the deadline has elapsed; it may not remain
    /// pending solely behind an earlier receive.
    ///
    /// A clean deadline completion is legal only when the provider establishes
    /// that no datagram was consumed. It returns
    /// [`DatagramError::DeadlineExceeded`] with `NotApplied` certainty and the
    /// unchanged request buffer. If an otherwise valid receive completion
    /// proves that a datagram was consumed, the operation succeeds even when
    /// the timeout completion is observed concurrently. If the provider cannot
    /// establish which effect won, or cannot return a valid receive result
    /// after consumption, it must return [`DatagramError::Backend`] with the
    /// appropriate `Applied` or `MayHaveApplied` certainty instead of claiming
    /// a clean deadline. Exact same-instant races follow the provider's
    /// documented event-order policy.
    fn submit_recv_from_until(
        &self,
        request: RecvFromRequest,
        deadline: Self::Instant,
    ) -> Self::RecvResponse;

    /// Releases the local binding and terminalizes the socket.
    ///
    /// Submission is eager, repeated calls are idempotent, and dropping the
    /// returned future does not cancel close. A successful close completion is
    /// a fence: every earlier admitted operation has terminalized internally,
    /// every provider or kernel reference to its owned request has ended, and
    /// the local binding has been released. An earlier send or receive canceled
    /// before its effect returns [`DatagramError::SocketClosed`] with
    /// `NotApplied` certainty and its unchanged buffer; operations whose effect
    /// raced with close follow their normal certainty rules. Operations
    /// submitted after close are rejected with
    /// [`DatagramError::SocketClosed`] and `NotApplied` certainty.
    fn submit_close(&self) -> Self::ControlResponse;
}

/// A [`DatagramSocketSubmit`] safe to share across executor threads.
///
/// Requiring this trait proves in generic code that the socket and every
/// operation future may cross executor threads, and that provider-native
/// addresses and instants may move between tasks. Owned address and instant
/// values need not be [`Sync`] because the socket API passes them by value.
pub trait SendDatagramSocketSubmit:
    DatagramSocketSubmit<
        Address: Send,
        Instant: Send,
        SendResponse: Send,
        RecvResponse: Send,
        ControlResponse: Send,
    > + Send
    + Sync
{
}

impl<T> SendDatagramSocketSubmit for T where
    T: DatagramSocketSubmit<
            Address: Send,
            Instant: Send,
            SendResponse: Send,
            RecvResponse: Send,
            ControlResponse: Send,
        > + Send
        + Sync
{
}

/// How a submitted receive waits for a matching datagram.
#[derive(Clone, Copy)]
pub(crate) enum ReceiveMode {
    Wait,
    Try,
    Deadline(SimInstant),
}

/// Completes one receive by appending a datagram payload to the request
/// buffer, reporting whole-datagram truncation explicitly.
pub(crate) fn receive_success(
    mut request: RecvFromRequest,
    source: NetworkAddress,
    payload: &[u8],
) -> CompletionResult<RecvFromResult<NetworkAddress>, DatagramFailure> {
    let datagram_len = payload.len();
    let bytes_received = request.max_bytes.min(datagram_len);
    request.buffer.extend_from_slice(&payload[..bytes_received]);
    Ok(RecvFromResult {
        buffer: request.buffer,
        bytes_received,
        datagram_len,
        source,
        truncation: if bytes_received == datagram_len {
            DatagramTruncation::Complete
        } else {
            DatagramTruncation::Truncated
        },
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::{IpAddr, Ipv4Addr, SocketAddr};

    #[test]
    fn request_and_result_types_accept_provider_native_addresses() {
        let address = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 4_000);
        let bind = DatagramBindRequest { address };
        let send = SendToRequest {
            buffer: b"packet".to_vec(),
            destination: address,
        };
        let receive = RecvFromResult {
            buffer: b"prefixpacket".to_vec(),
            bytes_received: 6,
            datagram_len: 6,
            source: address,
            truncation: DatagramTruncation::Complete,
        };

        assert_eq!(bind.address, address);
        assert_eq!(send.destination, address);
        assert_eq!(receive.source, address);
    }

    #[test]
    fn failure_returns_exact_buffer_and_diagnostics() {
        let buffer = vec![7, 8, 9];
        let failure = DatagramFailure::with_buffer(
            DatagramError::Injected { tag: 4 },
            buffer.clone(),
            buffer.len(),
        );

        assert_eq!(failure.error(), &DatagramError::Injected { tag: 4 });
        assert_eq!(failure.bytes_transferred(), 3);
        assert_eq!(failure.buffer(), Some(buffer.as_slice()));
        assert_eq!(failure.into_buffer(), Some(buffer));
    }

    #[test]
    fn error_chain_and_backend_display_are_stable() {
        let error = DatagramError::Backend {
            operation: DatagramOperationKind::RecvFrom,
            raw_os_error: Some(5),
            message: "reactor failed".to_owned(),
        };
        let failure = DatagramFailure::without_buffer(error);

        assert_eq!(
            failure.to_string(),
            "RecvFrom datagram backend failure (os error 5): reactor failed"
        );
        assert_eq!(
            failure.source().map(ToString::to_string),
            Some(failure.to_string())
        );
    }

    #[test]
    fn message_too_large_distinguishes_known_and_unknown_limits() {
        assert_eq!(
            DatagramError::MessageTooLarge {
                max_payload_bytes: Some(1_200),
            }
            .to_string(),
            "datagram exceeds the payload limit of 1200 bytes"
        );
        assert_eq!(
            DatagramError::MessageTooLarge {
                max_payload_bytes: None,
            }
            .to_string(),
            "datagram is too large for the transport"
        );
    }
}
