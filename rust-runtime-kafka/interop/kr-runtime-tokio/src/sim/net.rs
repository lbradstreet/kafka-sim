//! tokio::net-shaped TCP over kr-runtime-io's deterministic simulated network.
//!
//! [`TcpStream`] and [`TcpListener`] implement tokio's `AsyncRead`/`AsyncWrite`
//! contracts over [`SimNetwork`]'s owned-completion operations, so tokio's own
//! `io` combinators (`read_to_end`, `write_all`, `copy`, …) drive simulated
//! connections unchanged and deterministically.
//!
//! # The ambient network context
//!
//! tokio's API names no provider — `TcpStream::connect("10.0.0.1:80")` — so
//! the simulated provider and the caller's node identity are ambient, exactly
//! as the runtime is for `time` and `task`. A harness constructs one
//! [`SimNetContext`] per simulated node and either holds its
//! [`SimNetContext::install`] guard around driving (single-node runs, one
//! owner thread) or wraps `bind`/`connect` futures with
//! [`SimNetContext::scope`] (multi-node runs). Only `bind` and `connect`
//! consult the context; established streams and listeners never do.
//!
//! # Addressing
//!
//! Simulated addresses are `NetworkAddress { node: u64, port }`. IPv4 socket
//! addresses map losslessly: the IP's 32 bits are the node id. Node ids above
//! `u32::MAX` (minted by kr-runtime-native harness code) render as IPv6 addresses
//! carrying the node id in their low 64 bits under the `fd6b:6565:6c00::/64`
//! prefix. Name resolution is not simulated: string addresses must parse as
//! `ip:port`. Bind to explicit ports; simulated port zero has no
//! auto-assignment meaning.
//!
//! # Divergences from tokio
//!
//! The sim provider does not expose a peer identity on accepted connections,
//! so [`TcpListener::accept`] reports the unspecified address `0.0.0.0:0` and
//! [`TcpStream::peer_addr`] on an accepted stream returns
//! `io::ErrorKind::Unsupported` instead of fabricating an address; protocols
//! that need peer identity must carry it in their payload. Operation failures
//! map onto `io::ErrorKind` with the full [`NetworkFailure`] — including its
//! completion certainty — retained as the error source.

use kr_runtime::{CompletionError, CompletionResult};
use kr_runtime_io::network::{
    ByteStream as _, ConnectRequest, ListenRequest, NetworkAddress, NetworkError, NetworkFailure,
    NetworkListener as _, NetworkProvider as _, NodeId, ReadRequest, ReadResult, SimListener,
    SimNetwork, SimStream, WriteRequest, WriteResult,
};
use std::cell::{Cell, RefCell};
use std::future::Future;
use std::io;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::pin::Pin;
use std::rc::Rc;
use std::task::{Context, Poll};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

/// Bytes moved by one simulated read or write operation.
///
/// Matches the default `LinkConfig::max_chunk_bytes` and stays under the
/// default `NetworkConfig::max_operation_bytes`; larger caller buffers are
/// chunked across operations, which is how partial I/O behaves anyway.
const MAX_OPERATION_BYTES: usize = 64 * 1024;

/// Backlog requested by [`TcpListener::bind`]; matches the default
/// `NetworkConfig::max_listener_backlog`.
const DEFAULT_BACKLOG: usize = 1_024;

/// First ephemeral client port minted by a context, matching IANA's range.
const FIRST_EPHEMERAL_PORT: u16 = 49_152;

/// IPv6 prefix carrying simulated node ids that exceed the IPv4 space.
const WIDE_NODE_PREFIX: u128 = 0xfd6b_6565_6c00_0000_0000_0000_0000_0000;

thread_local! {
    static NET_CONTEXTS: RefCell<Vec<SimNetContext>> = const { RefCell::new(Vec::new()) };
}

/// The ambient provider and node identity used by [`TcpStream::connect`] and
/// [`TcpListener::bind`].
///
/// Cloning shares the ephemeral-port allocator, so every clone of one node's
/// context mints distinct client identities.
#[derive(Clone)]
pub struct SimNetContext {
    provider: SimNetwork,
    local_node: NodeId,
    next_ephemeral_port: Rc<Cell<u32>>,
}

impl SimNetContext {
    /// Creates a context for the node identified by `local_ip`.
    #[must_use]
    pub fn new(provider: SimNetwork, local_ip: Ipv4Addr) -> Self {
        Self {
            provider,
            local_node: NodeId(u64::from(u32::from(local_ip))),
            next_ephemeral_port: Rc::new(Cell::new(u32::from(FIRST_EPHEMERAL_PORT))),
        }
    }

    /// Installs this context on the current thread until the guard drops.
    ///
    /// Both kr-runtime executors poll every task on the owner thread, so a guard
    /// held around `block_on` or `run_until_stalled` makes this the ambient
    /// context for every task of a single-node simulation.
    #[must_use]
    pub fn install(&self) -> SimNetContextGuard {
        NET_CONTEXTS.with(|stack| stack.borrow_mut().push(self.clone()));
        SimNetContextGuard { _private: () }
    }

    /// Runs `future` with this context installed during each of its polls.
    ///
    /// Use this to give different nodes of one simulation different
    /// identities. Tasks spawned from inside the scope do not inherit it;
    /// wrap their futures too. Only `bind` and `connect` read the context,
    /// so wrapping just those futures is sufficient.
    pub async fn scope<F: Future>(&self, future: F) -> F::Output {
        Scoped {
            context: self.clone(),
            future: Box::pin(future),
        }
        .await
    }

    fn ephemeral_local(&self) -> io::Result<NetworkAddress> {
        let port = self.next_ephemeral_port.get();
        let Ok(port16) = u16::try_from(port) else {
            return Err(io::Error::new(
                io::ErrorKind::QuotaExceeded,
                "simulated ephemeral client ports are exhausted",
            ));
        };
        self.next_ephemeral_port.set(port + 1);
        Ok(NetworkAddress {
            node: self.local_node,
            port: port16,
        })
    }

    fn current(operation: &str) -> Self {
        NET_CONTEXTS
            .with(|stack| stack.borrow().last().cloned())
            .unwrap_or_else(|| {
                panic!(
                    "{operation} requires an ambient SimNetContext: install one \
                 around driving or wrap this future with SimNetContext::scope"
                )
            })
    }
}

/// Removes the context installed by [`SimNetContext::install`] on drop.
pub struct SimNetContextGuard {
    _private: (),
}

impl Drop for SimNetContextGuard {
    fn drop(&mut self) {
        NET_CONTEXTS.with(|stack| {
            stack.borrow_mut().pop();
        });
    }
}

/// The future returned by [`SimNetContext::scope`].
struct Scoped<F: Future> {
    context: SimNetContext,
    future: Pin<Box<F>>,
}

impl<F: Future> Future for Scoped<F> {
    type Output = F::Output;

    fn poll(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let _guard = self.context.install();
        self.future.as_mut().poll(context)
    }
}

/// Converts a tokio-shaped address argument into one simulated socket address.
///
/// This is the facade's counterpart of `tokio::net::ToSocketAddrs`. Name
/// resolution is not simulated, so string forms must parse as `ip:port`.
pub trait ToSocketAddrs {
    /// Returns the parsed socket address.
    ///
    /// # Errors
    ///
    /// Returns `io::ErrorKind::InvalidInput` when the value does not parse as
    /// an `ip:port` address.
    fn to_socket_addr(&self) -> io::Result<SocketAddr>;
}

impl ToSocketAddrs for SocketAddr {
    fn to_socket_addr(&self) -> io::Result<SocketAddr> {
        Ok(*self)
    }
}

impl ToSocketAddrs for (IpAddr, u16) {
    fn to_socket_addr(&self) -> io::Result<SocketAddr> {
        Ok(SocketAddr::new(self.0, self.1))
    }
}

impl ToSocketAddrs for (Ipv4Addr, u16) {
    fn to_socket_addr(&self) -> io::Result<SocketAddr> {
        Ok(SocketAddr::new(IpAddr::V4(self.0), self.1))
    }
}

impl ToSocketAddrs for str {
    fn to_socket_addr(&self) -> io::Result<SocketAddr> {
        self.parse().map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("cannot resolve {self:?}: simulated networking accepts only ip:port"),
            )
        })
    }
}

impl ToSocketAddrs for String {
    fn to_socket_addr(&self) -> io::Result<SocketAddr> {
        self.as_str().to_socket_addr()
    }
}

impl<T: ToSocketAddrs + ?Sized> ToSocketAddrs for &T {
    fn to_socket_addr(&self) -> io::Result<SocketAddr> {
        (**self).to_socket_addr()
    }
}

fn to_network(address: SocketAddr) -> io::Result<NetworkAddress> {
    match address.ip() {
        IpAddr::V4(ip) => Ok(NetworkAddress {
            node: NodeId(u64::from(u32::from(ip))),
            port: address.port(),
        }),
        IpAddr::V6(ip) => {
            let bits = u128::from(ip);
            if bits & !0xffff_ffff_ffff_ffff == WIDE_NODE_PREFIX {
                #[expect(clippy::cast_possible_truncation, reason = "masked to 64 bits")]
                let node = NodeId(bits as u64);
                Ok(NetworkAddress {
                    node,
                    port: address.port(),
                })
            } else {
                Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!("{ip} is not a simulated node address"),
                ))
            }
        }
    }
}

fn to_socket(address: NetworkAddress) -> SocketAddr {
    let ip = match u32::try_from(address.node.0) {
        Ok(v4) => IpAddr::V4(Ipv4Addr::from(v4)),
        Err(_) => IpAddr::V6(Ipv6Addr::from(
            WIDE_NODE_PREFIX | u128::from(address.node.0),
        )),
    };
    SocketAddr::new(ip, address.port)
}

fn failure_to_io(failure: CompletionError<NetworkFailure>) -> io::Error {
    let kind = match failure.error().error() {
        NetworkError::AddressInUse => io::ErrorKind::AddrInUse,
        NetworkError::ConnectionRefused | NetworkError::BacklogFull { .. } => {
            io::ErrorKind::ConnectionRefused
        }
        NetworkError::ConnectionClosed => io::ErrorKind::ConnectionReset,
        NetworkError::WriteClosed => io::ErrorKind::BrokenPipe,
        NetworkError::Partitioned { .. } => io::ErrorKind::HostUnreachable,
        NetworkError::ListenerClosed => io::ErrorKind::NotConnected,
        NetworkError::InvalidConfig { .. } | NetworkError::InvalidRequest { .. } => {
            io::ErrorKind::InvalidInput
        }
        NetworkError::Backpressure { .. }
        | NetworkError::ResourceExhausted { .. }
        | NetworkError::IdentifierExhausted => io::ErrorKind::QuotaExceeded,
        NetworkError::Backend {
            raw_os_error: Some(code),
            ..
        } => io::Error::from_raw_os_error(*code).kind(),
        _ => io::ErrorKind::Other,
    };
    io::Error::new(kind, failure)
}

type ControlOperation =
    kr_runtime_io::completion::LocalOperation<CompletionResult<(), NetworkFailure>>;
type ReadOperation =
    kr_runtime_io::completion::LocalOperation<CompletionResult<ReadResult, NetworkFailure>>;
type WriteOperation =
    kr_runtime_io::completion::LocalOperation<CompletionResult<WriteResult, NetworkFailure>>;

enum ReadState {
    Idle,
    Pending(ReadOperation),
    Buffered { data: Vec<u8>, offset: usize },
    Eof,
}

enum WriteState {
    Idle,
    Pending(WriteOperation),
}

enum ShutdownState {
    NotRequested,
    Pending(ControlOperation),
    Done,
}

/// A simulated TCP connection implementing tokio's `AsyncRead`/`AsyncWrite`.
///
/// Reads and writes are kr-runtime-io owned-completion operations submitted from
/// inside `poll_read`/`poll_write`, so tokio's lazy poll contract is
/// preserved at the poll boundary. Between a `poll_write` that returned
/// `Pending` and its completion the submitted prefix is already owned by the
/// driver; callers must present the same data on the next poll, which every
/// tokio `io` combinator already does.
pub struct TcpStream {
    stream: SimStream,
    local: SocketAddr,
    peer: Option<SocketAddr>,
    read: ReadState,
    write: WriteState,
    shutdown: ShutdownState,
}

impl std::fmt::Debug for TcpStream {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("TcpStream")
            .field("local", &self.local)
            .field("peer", &self.peer)
            .finish_non_exhaustive()
    }
}

impl TcpStream {
    fn new(stream: SimStream, local: SocketAddr, peer: Option<SocketAddr>) -> Self {
        Self {
            stream,
            local,
            peer,
            read: ReadState::Idle,
            write: WriteState::Idle,
            shutdown: ShutdownState::NotRequested,
        }
    }

    /// Connects to a bound simulated listener through the ambient context.
    ///
    /// # Errors
    ///
    /// Returns the provider's typed failure mapped onto `io::ErrorKind`
    /// (`ConnectionRefused`, `HostUnreachable` for a partitioned link, …).
    ///
    /// # Panics
    ///
    /// Panics when no [`SimNetContext`] is installed.
    pub async fn connect<A: ToSocketAddrs>(address: A) -> io::Result<Self> {
        let peer = address.to_socket_addr()?;
        let context = SimNetContext::current("TcpStream::connect");
        let local = context.ephemeral_local()?;
        let stream = context
            .provider
            .connect(ConnectRequest {
                local,
                remote: to_network(peer)?,
            })
            .await
            .map_err(failure_to_io)?;
        Ok(Self::new(stream, to_socket(local), Some(peer)))
    }

    /// Returns the local address of this stream.
    ///
    /// # Errors
    ///
    /// Never fails for simulated streams; the signature matches tokio.
    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        Ok(self.local)
    }

    /// Returns the peer address of a connected stream.
    ///
    /// # Errors
    ///
    /// Returns `io::ErrorKind::Unsupported` on an accepted stream: the sim
    /// provider does not expose peer identity, and this facade does not
    /// fabricate one.
    pub fn peer_addr(&self) -> io::Result<SocketAddr> {
        self.peer.ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::Unsupported,
                "the simulated provider does not expose peer identity on accepted \
                 connections; carry peer identity in the protocol payload",
            )
        })
    }
}

impl AsyncRead for TcpStream {
    fn poll_read(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        loop {
            match &mut self.read {
                ReadState::Eof => return Poll::Ready(Ok(())),
                ReadState::Buffered { data, offset } => {
                    let available = &data[*offset..];
                    let count = available.len().min(buf.remaining());
                    buf.put_slice(&available[..count]);
                    *offset += count;
                    if *offset == data.len() {
                        self.read = ReadState::Idle;
                    }
                    return Poll::Ready(Ok(()));
                }
                ReadState::Idle => {
                    if buf.remaining() == 0 {
                        return Poll::Ready(Ok(()));
                    }
                    let max_bytes = buf.remaining().min(MAX_OPERATION_BYTES);
                    let operation = self.stream.read(ReadRequest {
                        buffer: Vec::new(),
                        max_bytes,
                    });
                    self.read = ReadState::Pending(operation);
                }
                ReadState::Pending(operation) => match Pin::new(operation).poll(context) {
                    Poll::Pending => return Poll::Pending,
                    Poll::Ready(Ok(result)) => {
                        if result.end_of_stream {
                            self.read = ReadState::Eof;
                            return Poll::Ready(Ok(()));
                        }
                        if result.bytes_read == 0 {
                            // A nonzero-capacity read that appended nothing
                            // without observing EOF carries no information;
                            // resubmit rather than reporting a false EOF.
                            self.read = ReadState::Idle;
                            continue;
                        }
                        self.read = ReadState::Buffered {
                            data: result.buffer,
                            offset: 0,
                        };
                    }
                    Poll::Ready(Err(failure)) => {
                        self.read = ReadState::Idle;
                        return Poll::Ready(Err(failure_to_io(failure)));
                    }
                },
            }
        }
    }
}

impl AsyncWrite for TcpStream {
    fn poll_write(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        loop {
            match &mut self.write {
                WriteState::Idle => {
                    if buf.is_empty() {
                        return Poll::Ready(Ok(0));
                    }
                    let count = buf.len().min(MAX_OPERATION_BYTES);
                    let operation = self.stream.write(WriteRequest {
                        buffer: buf[..count].to_vec(),
                    });
                    self.write = WriteState::Pending(operation);
                }
                WriteState::Pending(operation) => match Pin::new(operation).poll(context) {
                    Poll::Pending => return Poll::Pending,
                    Poll::Ready(Ok(result)) => {
                        self.write = WriteState::Idle;
                        return Poll::Ready(Ok(result.bytes_written));
                    }
                    Poll::Ready(Err(failure)) => {
                        self.write = WriteState::Idle;
                        return Poll::Ready(Err(failure_to_io(failure)));
                    }
                },
            }
        }
    }

    fn poll_flush(self: Pin<&mut Self>, _context: &mut Context<'_>) -> Poll<io::Result<()>> {
        // Writes are submitted and individually awaited; the facade holds no
        // additional buffer to flush, matching tokio's TcpStream.
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<io::Result<()>> {
        loop {
            match &mut self.shutdown {
                ShutdownState::Done => return Poll::Ready(Ok(())),
                ShutdownState::NotRequested => {
                    self.shutdown = ShutdownState::Pending(self.stream.shutdown_write());
                }
                ShutdownState::Pending(operation) => match Pin::new(operation).poll(context) {
                    Poll::Pending => return Poll::Pending,
                    Poll::Ready(Ok(())) => {
                        self.shutdown = ShutdownState::Done;
                        return Poll::Ready(Ok(()));
                    }
                    Poll::Ready(Err(failure)) => {
                        self.shutdown = ShutdownState::Done;
                        return Poll::Ready(Err(failure_to_io(failure)));
                    }
                },
            }
        }
    }
}

/// A bound simulated listener.
pub struct TcpListener {
    listener: SimListener,
    local: SocketAddr,
}

impl std::fmt::Debug for TcpListener {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("TcpListener")
            .field("local", &self.local)
            .finish_non_exhaustive()
    }
}

impl TcpListener {
    /// Binds the address exclusively through the ambient context.
    ///
    /// The requested backlog is [`DEFAULT_BACKLOG`]; the simulated provider
    /// enforces it exactly. Bind explicit ports — simulated port zero is an
    /// ordinary port, not an auto-assignment request.
    ///
    /// # Errors
    ///
    /// Returns the provider's typed failure mapped onto `io::ErrorKind`
    /// (`AddrInUse`, `InvalidInput`, …).
    ///
    /// # Panics
    ///
    /// Panics when no [`SimNetContext`] is installed.
    pub async fn bind<A: ToSocketAddrs>(address: A) -> io::Result<Self> {
        let local = address.to_socket_addr()?;
        let context = SimNetContext::current("TcpListener::bind");
        let listener = context
            .provider
            .listen(ListenRequest {
                address: to_network(local)?,
                backlog: DEFAULT_BACKLOG,
            })
            .await
            .map_err(failure_to_io)?;
        Ok(Self { listener, local })
    }

    /// Accepts one connection in FIFO order.
    ///
    /// The returned address is always the unspecified `0.0.0.0:0`: the sim
    /// provider does not expose peer identity and this facade does not
    /// fabricate one.
    ///
    /// # Errors
    ///
    /// Returns the provider's typed failure mapped onto `io::ErrorKind`.
    pub async fn accept(&self) -> io::Result<(TcpStream, SocketAddr)> {
        let stream = self.listener.accept().await.map_err(failure_to_io)?;
        let unknown_peer = SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), 0);
        Ok((TcpStream::new(stream, self.local, None), unknown_peer))
    }

    /// Returns the bound address.
    ///
    /// # Errors
    ///
    /// Never fails for simulated listeners; the signature matches tokio.
    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        Ok(self.local)
    }
}
