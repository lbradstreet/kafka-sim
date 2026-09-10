//! Shared-ring parallel stream provider: many connections, two threads.
//!
//! This module is the network stage of the shared-budgeted-rings design
//! recorded in `DESIGN.md`, built as a parallel implementation beside the
//! per-stream actor/reactor fleet in [`crate::UringByteStream`]. Both are
//! usable; nothing selects one for you. The contract — [`ByteStreamSubmit`],
//! owned buffers, per-direction FIFO, certainty-tagged failures — is
//! identical, and both run the same conformance suite.
//!
//! Mechanics: a [`UringNetPool`] owns one io_uring ring (whose reactor is
//! the kernel-facing thread) and one coordinator thread. Submissions
//! validate on the caller thread and enqueue a command; the coordinator
//! runs every stream's read and write state machines, submits routed SQEs
//! to the shared ring, and completes responses when routed completions
//! arrive. A registered stream therefore costs one descriptor and no
//! threads, instead of the per-stream fleet's three of each.
//!
//! Both directions of stream I/O are peer-gated: an armed receive waits for
//! the peer to send, and a send on a full socket buffer waits for the peer
//! to read. Every registered stream reserves one sustained ring slot per
//! direction at pool construction, so a stalled peer consumes only its own
//! stream's reservation and no queue of waiters can form — one stream's
//! silence can never starve another's transfers. Each direction runs at
//! most one operation on the ring; later commands queue per direction in
//! FIFO order, bounded by `command_queue_capacity` behind the active
//! operation, exactly like the per-stream actors' queues.
//!
//! Close is an out-of-band control operation, mirroring
//! [`crate::UringByteStream`]: it shuts down the socket synchronously so it
//! can interrupt an armed transfer, after which the interrupted operation's
//! terminal CQE returns every owned buffer. The first close caches its
//! completion so concurrent and repeated callers observe the same result.
//!
//! The control plane runs on the same coordinator. `listen` binds inline
//! and registers an accept state machine; a listener with pending accepts
//! keeps one accept-plus-timeout pair armed on the ring, re-arming on
//! expiry, so listener close observes a bounded drain instead of needing a
//! cancellation path. `connect` submits a connect-plus-timeout pair.
//! Accepted and connected sockets register as ordinary pool streams, so
//! the whole provider — control plane included — runs on two threads.

use std::collections::{HashMap, VecDeque};
use std::fmt;
use std::io;
use std::net::{Shutdown, SocketAddr, TcpListener, TcpStream};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::mpsc::{self, Receiver, Sender};
use std::sync::{Arc, Mutex, Weak};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use kr_runtime::{CompletionError, CompletionResult};
use kr_runtime_io::network::{
    ByteStreamSubmit, ByteStreamVectoredSubmit, ConnectRequest, ListenRequest, MAX_WRITE_SEGMENTS,
    NetworkError, NetworkFailure, NetworkListenerSubmit, NetworkOperationKind,
    NetworkProviderSubmit, ReadRequest, ReadResult, VectoredWriteFailure, VectoredWriteRequest,
    VectoredWriteResult, WriteRequest, WriteResult, WriteSegment,
};

use crate::network::{
    ListenerControl, ListenerTerminal, accepted_then_closed, backend, bind_tcp_listener,
    control_error, create_bound_tcp_stream, is_retryable_accept_error, map_connect_error,
    map_listen_error, map_stream_error, no_buffer_error, not_applied_network, read_error,
    read_request_exceeds_bound, write_error, write_request_exceeds_bound,
};
use crate::operation::{
    DriverStoppedCommand, FailStopOnPanic, Responder, TerminalCommand, UringOperation, operation,
    ready,
};
use crate::ring::{
    Ring, RingCapacity, RoutedAcceptOutcome, RoutedAcceptStorage, RoutedCompletion,
    RoutedConnectStorage, RoutedResult, RoutedSubmission,
};
use crate::support::{
    Ingress, ResourcePermit, ResourcePool, WAKE_TOKEN, join_if_other_thread, lock_unpoisoned,
};

type ReadCompletion = CompletionResult<ReadResult, NetworkFailure>;
type WriteCompletion = CompletionResult<WriteResult, NetworkFailure>;
type VectoredWriteCompletion = CompletionResult<VectoredWriteResult, VectoredWriteFailure>;
type ControlCompletion = CompletionResult<(), NetworkFailure>;
type StreamCompletion = CompletionResult<PooledUringStream, NetworkFailure>;
type ListenCompletion = CompletionResult<PooledUringListener, NetworkFailure>;

/// How long one routed accept attempt stays armed before its linked timeout
/// expires and the coordinator re-arms it. Bounds listener close and pool
/// shutdown latency; between attempts, arriving connections wait in the
/// kernel backlog.
const ACCEPT_ARM_TIMEOUT: Duration = Duration::from_millis(50);

/// Fixed limits for one shared-ring stream pool.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct UringNetPoolConfig {
    /// Maximum registered streams alive at once.
    ///
    /// Each registered stream reserves two sustained ring slots — one armed
    /// receive, one armed send — and costs exactly one descriptor: its TCP
    /// socket. The pool's ring and eventfd are shared by every stream.
    pub max_streams: usize,
    /// Commands admitted behind the active operation in one direction.
    /// Also bounds one listener's pending accepts and close waiters.
    pub command_queue_capacity: usize,
    /// Submission-queue depth of the shared ring.
    pub ring_entries: u32,
    /// Maximum caller-owned buffer accepted by one operation.
    pub max_operation_bytes: usize,
    /// Maximum bytes submitted by one SQE and therefore one completion.
    pub max_io_chunk_bytes: usize,
    /// Maximum live bound listeners. A completed listener close releases
    /// its slot even while the closed handle remains live.
    pub max_listeners: usize,
    /// Maximum backlog accepted by one listen request.
    pub max_listener_backlog: usize,
    /// Maximum wall-clock time allowed for TCP connection establishment.
    ///
    /// The kernel Connect request is linked to this timeout, so a
    /// blackholed peer cannot hold a connect slot — or pool teardown, which
    /// awaits in-flight pairs — indefinitely.
    pub connect_timeout: Duration,
}

impl Default for UringNetPoolConfig {
    fn default() -> Self {
        Self {
            max_streams: 256,
            command_queue_capacity: 64,
            ring_entries: 8,
            max_operation_bytes: 256 * 1024,
            max_io_chunk_bytes: 64 * 1024,
            max_listeners: 256,
            max_listener_backlog: 1_024,
            connect_timeout: Duration::from_secs(10),
        }
    }
}

/// Failure to construct a stream pool or register a stream with it.
#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum UringNetPoolOpenError {
    /// A configuration field is outside its supported range.
    InvalidConfig { field: &'static str, reason: String },
    /// An operating-system interface failed.
    Io {
        action: &'static str,
        raw_os_error: Option<i32>,
        message: String,
    },
    /// A bounded pool resource is fully in use.
    ResourceExhausted {
        resource: &'static str,
        limit: usize,
    },
}

impl fmt::Display for UringNetPoolOpenError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidConfig { field, reason } => {
                write!(formatter, "invalid stream pool config {field}: {reason}")
            }
            Self::Io {
                action,
                raw_os_error,
                message,
            } => {
                write!(formatter, "could not {action}")?;
                if let Some(code) = raw_os_error {
                    write!(formatter, " (OS error {code})")?;
                }
                write!(formatter, ": {message}")
            }
            Self::ResourceExhausted { resource, limit } => {
                write!(formatter, "{resource} exhausted at limit {limit}")
            }
        }
    }
}

impl std::error::Error for UringNetPoolOpenError {}

fn open_io(action: &'static str, error: io::Error) -> UringNetPoolOpenError {
    UringNetPoolOpenError::Io {
        action,
        raw_os_error: error.raw_os_error(),
        message: error.to_string(),
    }
}

fn invalid_config(
    field: &'static str,
    reason: impl Into<String>,
) -> Result<(), UringNetPoolOpenError> {
    Err(UringNetPoolOpenError::InvalidConfig {
        field,
        reason: reason.into(),
    })
}

fn validate_config(config: UringNetPoolConfig) -> Result<(), UringNetPoolOpenError> {
    if config.max_streams == 0 {
        return invalid_config("max_streams", "must be nonzero");
    }
    if sustained_slots(config.max_streams).is_none() {
        return invalid_config("max_streams", "sustained slot reservation overflowed usize");
    }
    if config.command_queue_capacity == 0 {
        return invalid_config("command_queue_capacity", "must be nonzero");
    }
    if config.ring_entries < 4 || !config.ring_entries.is_power_of_two() {
        return invalid_config(
            "ring_entries",
            "must be a power of two and at least 4 for multiple queued I/O operations",
        );
    }
    if config.max_operation_bytes == 0 {
        return invalid_config("max_operation_bytes", "must be nonzero");
    }
    if config.max_io_chunk_bytes == 0 || config.max_io_chunk_bytes > u32::MAX as usize {
        return invalid_config("max_io_chunk_bytes", format!("must be in 1..={}", u32::MAX));
    }
    if config.max_listeners == 0 {
        return invalid_config("max_listeners", "must be nonzero");
    }
    if config.max_listener_backlog == 0 {
        return invalid_config("max_listener_backlog", "must be nonzero");
    }
    if config.connect_timeout.is_zero() {
        return invalid_config("connect_timeout", "must be nonzero");
    }
    if transient_slots(config).is_none() {
        return invalid_config(
            "max_listeners",
            "transient slot reservation overflowed usize",
        );
    }
    Ok(())
}

/// One sustained slot per direction for every registrable stream.
fn sustained_slots(max_streams: usize) -> Option<usize> {
    max_streams.checked_mul(2)
}

/// Transient capacity covering every control-plane pair that can be armed
/// at once: one accept-plus-timeout pair per listener, and one
/// connect-plus-timeout pair per in-flight connect, which is bounded by the
/// stream permits each connect holds. Sized to the maximum so a stalled
/// control pair can never defer a stream transfer behind it.
fn transient_slots(config: UringNetPoolConfig) -> Option<usize> {
    let accepts = config.max_listeners.checked_mul(2)?;
    let connects = config.max_streams.checked_mul(2)?;
    accepts.checked_add(connects)
}

enum StreamCommand {
    Read {
        request: ReadRequest,
        response: Responder<ReadCompletion>,
    },
    Write {
        request: WriteRequest,
        response: Responder<WriteCompletion>,
    },
    WriteVectored {
        request: VectoredWriteRequest,
        response: Responder<VectoredWriteCompletion>,
    },
    ShutdownWrite {
        response: Responder<ControlCompletion>,
    },
}

impl DriverStoppedCommand for StreamCommand {
    fn complete_driver_stopped(self) {
        match self {
            Self::Read { request, response } => {
                response.complete(read_error(NetworkError::DriverStopped, request.buffer));
            }
            Self::Write { request, response } => {
                response.complete(write_error(NetworkError::DriverStopped, request.buffer));
            }
            Self::WriteVectored { request, response } => {
                response.complete(vectored_error(
                    NetworkError::DriverStopped,
                    request.segments,
                ));
            }
            Self::ShutdownWrite { response } => {
                response.complete(control_error(NetworkError::DriverStopped));
            }
        }
    }
}

/// A control-plane command bound for the coordinator.
enum ControlCommand {
    Accept {
        listener: u64,
        stream_permit: ResourcePermit,
        response: Responder<StreamCompletion>,
    },
    Connect {
        socket: TcpStream,
        remote: SocketAddr,
        stream_permit: ResourcePermit,
        response: Responder<StreamCompletion>,
    },
}

impl DriverStoppedCommand for ControlCommand {
    fn complete_driver_stopped(self) {
        match self {
            Self::Accept { response, .. } | Self::Connect { response, .. } => {
                response.complete(no_buffer_error(NetworkError::DriverStopped));
            }
        }
    }
}

/// A freshly bound listener traveling to the coordinator, which owns its
/// accept state machine from then on.
struct ListenerRegistration {
    id: u64,
    listener: TcpListener,
    control: Arc<ListenerControl>,
    permit: ResourcePermit,
}

enum PoolMessage {
    Command {
        stream: Arc<StreamControl>,
        command: TerminalCommand<StreamCommand>,
    },
    Control(TerminalCommand<ControlCommand>),
    StreamClosed {
        stream: u64,
    },
    RegisterListener(ListenerRegistration),
    CloseListener {
        listener: u64,
    },
    Shutdown,
}

/// Everything a registered stream's commands need to reach the pool: kept
/// alive by every queued and active command, so the descriptor outlives all
/// kernel-visible references to it, and holding the stream permit so
/// sustained ring slots free only when the socket truly retires.
struct StreamControl {
    id: u64,
    socket: TcpStream,
    config: UringNetPoolConfig,
    /// Commands queued and not yet started, per direction, bounded by
    /// `command_queue_capacity`.
    read_admitted: AtomicUsize,
    write_admitted: AtomicUsize,
    /// Set by the out-of-band close before the socket shutdown, so every
    /// later command observes the close.
    closed: AtomicBool,
    ingress: Arc<Ingress<PoolMessage>>,
    _permit: ResourcePermit,
    lifetime_guard: Mutex<Option<Arc<dyn Send + Sync>>>,
    metrics: std::sync::OnceLock<Arc<kr_runtime_io::completion::CompletionMetrics>>,
}

impl StreamControl {
    fn retire_read(&self) {
        let previous = self.read_admitted.fetch_sub(1, Ordering::AcqRel);
        debug_assert!(previous > 0, "read command count underflowed");
    }

    fn retire_write(&self) {
        let previous = self.write_admitted.fetch_sub(1, Ordering::AcqRel);
        debug_assert!(previous > 0, "write command count underflowed");
    }

    fn is_closed(&self) -> bool {
        self.closed.load(Ordering::Acquire)
    }
}

/// Marks the stream closed for the coordinator when the last public handle
/// goes away, and serializes the out-of-band close.
struct HandleGuard {
    control: Arc<StreamControl>,
    /// The first close's cached completion; concurrent and repeated closes
    /// observe the same terminal result.
    close: Mutex<Option<ControlCompletion>>,
    /// Keeps the pool's shutdown ordered after this stream's close: the
    /// field drops after the close message is pushed.
    _shared: Arc<PoolShared>,
}

impl HandleGuard {
    fn close(&self) -> ControlCompletion {
        let mut state = lock_unpoisoned(&self.close);
        if let Some(completion) = &*state {
            return completion.clone();
        }
        self.control.closed.store(true, Ordering::Release);
        // Shutting the socket down interrupts an armed transfer: its
        // terminal CQE arrives and returns the owned buffer, which is how
        // close bypasses both direction FIFOs without an io_uring cancel.
        let completion = match self.control.socket.shutdown(Shutdown::Both) {
            Ok(()) => Ok(()),
            Err(error) if error.kind() == io::ErrorKind::NotConnected => Ok(()),
            Err(error) => Err(CompletionError::may_have_applied(
                NetworkFailure::without_buffer(backend(
                    NetworkOperationKind::Close,
                    error.raw_os_error(),
                    error.to_string(),
                )),
            )),
        };
        self.control.ingress.push(PoolMessage::StreamClosed {
            stream: self.control.id,
        });
        *state = Some(completion.clone());
        completion
    }
}

impl Drop for HandleGuard {
    fn drop(&mut self) {
        let _ = self.close();
        // The close is cached, so a handle dropped after an explicit close
        // must still tell the coordinator to drain any state a late command
        // recreated.
        self.control.ingress.push(PoolMessage::StreamClosed {
            stream: self.control.id,
        });
    }
}

struct PoolShared {
    config: UringNetPoolConfig,
    ingress: Arc<Ingress<PoolMessage>>,
    coordinator: Mutex<Option<JoinHandle<()>>>,
    streams: Arc<ResourcePool>,
    listeners: Arc<ResourcePool>,
    next_stream: AtomicU64,
    next_listener: AtomicU64,
}

impl PoolShared {
    /// Wraps a connected socket in a pool stream session carrying its
    /// already-acquired registration permit.
    fn register_with_permit(
        self: &Arc<Self>,
        socket: TcpStream,
        permit: ResourcePermit,
    ) -> PooledUringStream {
        let id = self.next_stream.fetch_add(1, Ordering::Relaxed);
        let control = Arc::new(StreamControl {
            id,
            socket,
            config: self.config,
            read_admitted: AtomicUsize::new(0),
            write_admitted: AtomicUsize::new(0),
            closed: AtomicBool::new(false),
            ingress: Arc::clone(&self.ingress),
            _permit: permit,
            lifetime_guard: Mutex::new(None),
            metrics: std::sync::OnceLock::new(),
        });
        PooledUringStream {
            handle: Arc::new(HandleGuard {
                control,
                close: Mutex::new(None),
                _shared: Arc::clone(self),
            }),
        }
    }

    fn acquire_stream_permit(&self) -> Result<ResourcePermit, NetworkError> {
        self.streams
            .acquire()
            .ok_or(NetworkError::ResourceExhausted {
                resource: "io_uring pooled network streams",
                limit: self.config.max_streams,
            })
    }
}

impl Drop for PoolShared {
    fn drop(&mut self) {
        // Every public handle is gone, so only queued and in-flight work
        // remains. The coordinator drains it — every response is completed,
        // normally or with DriverStopped — before the thread is joined.
        self.ingress.push(PoolMessage::Shutdown);
        join_if_other_thread(lock_unpoisoned(&self.coordinator).take());
    }
}

/// A shared-ring stream pool: one ring, one coordinator, no per-stream
/// threads.
///
/// Streams registered with the pool implement the same [`ByteStreamSubmit`]
/// contract as [`crate::UringByteStream`], with the same per-direction FIFO
/// and close semantics, while every stream shares the pool's two threads
/// instead of owning three of its own.
#[derive(Clone)]
pub struct UringNetPool {
    shared: Arc<PoolShared>,
}

/// Passive pool pressure. Commands are coordinator ingress, not kernel SQEs;
/// stream/listener counts include resources retained by abandoned operations.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct UringNetPoolStatus {
    pub streams: usize,
    pub listeners: usize,
    pub queued_commands: usize,
}
#[derive(Clone)]
pub struct UringNetPoolObserver {
    shared: Weak<PoolShared>,
}
impl UringNetPoolObserver {
    pub fn status(&self) -> Option<UringNetPoolStatus> {
        self.shared.upgrade().map(|s| UringNetPoolStatus {
            streams: s.streams.in_use(),
            listeners: s.listeners.in_use(),
            queued_commands: s.ingress.len(),
        })
    }
}
impl UringNetPool {
    /// Observes pressure without retaining the provider or postponing shutdown.
    pub fn observer(&self) -> UringNetPoolObserver {
        UringNetPoolObserver {
            shared: Arc::downgrade(&self.shared),
        }
    }

    /// Builds the pool: the shared ring and its reactor, and the
    /// coordinator thread.
    ///
    /// # Errors
    ///
    /// Returns [`UringNetPoolOpenError`] when the configuration is invalid
    /// or the ring or coordinator thread cannot be created.
    pub fn new(config: UringNetPoolConfig) -> Result<Self, UringNetPoolOpenError> {
        validate_config(config)?;
        let sustained = sustained_slots(config.max_streams)
            .expect("validated sustained reservation cannot overflow");
        let transient =
            transient_slots(config).expect("validated transient reservation cannot overflow");
        let ring = Ring::for_stream_pool(
            RingCapacity {
                entries: config.ring_entries,
                transient,
                sustained,
            },
            config.max_io_chunk_bytes,
        )
        .map_err(|error| open_io("create pooled stream io_uring", error))?;
        let (routed, completions) = mpsc::channel();
        let ingress = Arc::new(Ingress::new(routed.clone()));
        let shared = Arc::new(PoolShared {
            config,
            ingress: Arc::clone(&ingress),
            coordinator: Mutex::new(None),
            streams: Arc::new(ResourcePool::new(config.max_streams)),
            listeners: Arc::new(ResourcePool::new(config.max_listeners)),
            next_stream: AtomicU64::new(1),
            next_listener: AtomicU64::new(1),
        });

        // The coordinator registers accepted and connected sockets as pool
        // streams, so it needs the shared state — weakly, because holding
        // it strongly would keep the pool alive from its own thread and the
        // shutdown message could never be sent.
        let coordinator_shared = Arc::downgrade(&shared);
        let spawn_result = thread::Builder::new()
            .name("kr-runtime-io-uring-net-pool".to_owned())
            .spawn(move || {
                Coordinator {
                    ring,
                    config,
                    routed,
                    completions,
                    ingress,
                    shared: coordinator_shared,
                    streams: HashMap::new(),
                    listeners: HashMap::new(),
                    connects: HashMap::new(),
                    tokens: HashMap::new(),
                    next_token: WAKE_TOKEN + 1,
                    outstanding_ops: 0,
                    shutting_down: false,
                }
                .run();
            });
        let coordinator = match spawn_result {
            Ok(join) => join,
            Err(error) => return Err(open_io("spawn stream pool coordinator", error)),
        };
        *lock_unpoisoned(&shared.coordinator) = Some(coordinator);

        Ok(Self { shared })
    }

    /// Registers an already-connected TCP stream with the pool.
    ///
    /// The stream reserves its two sustained ring slots for as long as it is
    /// registered; the reservation frees once the last handle clone is
    /// dropped and every admitted command has terminalized.
    ///
    /// # Errors
    ///
    /// Returns [`UringNetPoolOpenError::ResourceExhausted`] when
    /// `max_streams` streams are already registered.
    pub fn register_stream(
        &self,
        socket: TcpStream,
    ) -> Result<PooledUringStream, UringNetPoolOpenError> {
        let Some(permit) = self.shared.streams.acquire() else {
            return Err(UringNetPoolOpenError::ResourceExhausted {
                resource: "io_uring pooled network streams",
                limit: self.shared.config.max_streams,
            });
        };
        Ok(self.shared.register_with_permit(socket, permit))
    }
}

impl NetworkProviderSubmit for UringNetPool {
    type Address = SocketAddr;
    type Stream = PooledUringStream;
    type Listener = PooledUringListener;
    type ListenResponse = UringOperation<ListenCompletion>;
    type ConnectResponse = UringOperation<StreamCompletion>;

    fn submit_listen(&self, request: ListenRequest<SocketAddr>) -> Self::ListenResponse {
        if request.backlog == 0 || request.backlog > self.shared.config.max_listener_backlog {
            return ready(no_buffer_error(NetworkError::InvalidRequest {
                reason: "listener backlog is outside the configured bound",
            }));
        }
        let Some(permit) = self.shared.listeners.acquire() else {
            return ready(no_buffer_error(NetworkError::ResourceExhausted {
                resource: "io_uring pooled listeners",
                limit: self.shared.config.max_listeners,
            }));
        };
        let listener = match bind_tcp_listener(request.address, request.backlog) {
            Ok(listener) => listener,
            Err(error) => return ready(Err(not_applied_network(map_listen_error(error)))),
        };
        let address = match listener.local_addr() {
            Ok(address) => address,
            Err(error) => {
                return ready(Err(CompletionError::may_have_applied(
                    NetworkFailure::without_buffer(backend(
                        NetworkOperationKind::Listen,
                        error.raw_os_error(),
                        error.to_string(),
                    )),
                )));
            }
        };
        let id = self.shared.next_listener.fetch_add(1, Ordering::Relaxed);
        let control = Arc::new(ListenerControl::new(
            self.shared.config.command_queue_capacity,
        ));
        self.shared
            .ingress
            .push(PoolMessage::RegisterListener(ListenerRegistration {
                id,
                listener,
                control: Arc::clone(&control),
                permit,
            }));
        ready(Ok(PooledUringListener {
            id,
            address,
            control,
            shared: Arc::clone(&self.shared),
        }))
    }

    fn submit_connect(&self, request: ConnectRequest<SocketAddr>) -> Self::ConnectResponse {
        let stream_permit = match self.shared.acquire_stream_permit() {
            Ok(permit) => permit,
            Err(error) => return ready(no_buffer_error(error)),
        };
        let socket = match create_bound_tcp_stream(request.local) {
            Ok(socket) => socket,
            Err(error) => return ready(Err(not_applied_network(map_connect_error(error)))),
        };
        let (future, response) = operation();
        self.shared
            .ingress
            .push(PoolMessage::Control(TerminalCommand::new(
                ControlCommand::Connect {
                    socket,
                    remote: request.remote,
                    stream_permit,
                    response,
                },
            )));
        future
    }
}

/// One exclusively bound TCP listener whose accepts run on the pool
/// coordinator.
///
/// Dropping the listener closes it: pending accepts are rejected and the
/// binding is released once the in-flight accept attempt drains.
pub struct PooledUringListener {
    id: u64,
    address: SocketAddr,
    control: Arc<ListenerControl>,
    shared: Arc<PoolShared>,
}

impl NetworkListenerSubmit for PooledUringListener {
    type Address = SocketAddr;
    type Stream = PooledUringStream;
    type AcceptResponse = UringOperation<StreamCompletion>;
    type CloseResponse = UringOperation<ControlCompletion>;

    fn local_address(&self) -> SocketAddr {
        self.address
    }

    fn submit_accept(&self) -> Self::AcceptResponse {
        if let Some(error) = self.control.admission_error() {
            return ready(no_buffer_error(error));
        }
        let stream_permit = match self.shared.acquire_stream_permit() {
            Ok(permit) => permit,
            Err(error) => return ready(no_buffer_error(error)),
        };
        let (future, response) = operation();
        self.shared
            .ingress
            .push(PoolMessage::Control(TerminalCommand::new(
                ControlCommand::Accept {
                    listener: self.id,
                    stream_permit,
                    response,
                },
            )));
        future
    }

    fn submit_close(&self) -> Self::CloseResponse {
        let future = self.control.submit_close();
        self.shared
            .ingress
            .push(PoolMessage::CloseListener { listener: self.id });
        future
    }
}

impl Drop for PooledUringListener {
    fn drop(&mut self) {
        self.control.request_close_without_response();
        self.shared
            .ingress
            .push(PoolMessage::CloseListener { listener: self.id });
    }
}

/// One pooled connected stream session implementing [`ByteStreamSubmit`] and
/// [`ByteStreamVectoredSubmit`].
///
/// Clones share the session. Dropping the last clone closes the stream;
/// dropping a response future abandons only its response, never the
/// admitted operation or its buffer.
#[derive(Clone)]
pub struct PooledUringStream {
    handle: Arc<HandleGuard>,
}

impl PooledUringStream {
    /// Enables native completion timing for subsequent stream I/O. Call once
    /// before submission to measure every operation, including immediate errors.
    pub fn attach_completion_metrics(
        &self,
        metrics: Arc<kr_runtime_io::completion::CompletionMetrics>,
    ) -> Result<(), NetworkError> {
        self.handle
            .control
            .metrics
            .set(metrics)
            .map_err(|_| NetworkError::InvalidRequest {
                reason: "completion metrics already attached",
            })
    }
    /// Attaches one passive allocation-budget obligation before stream I/O.
    /// It follows admitted buffers through unconsumed terminal responses.
    pub fn attach_lifetime_guard(&self, guard: Arc<dyn Send + Sync>) -> Result<(), NetworkError> {
        let mut slot = lock_unpoisoned(&self.handle.control.lifetime_guard);
        if slot.is_some()
            || self.handle.control.read_admitted.load(Ordering::Acquire) != 0
            || self.handle.control.write_admitted.load(Ordering::Acquire) != 0
        {
            return Err(NetworkError::InvalidRequest {
                reason: "lifetime guard must precede I/O",
            });
        }
        *slot = Some(guard);
        Ok(())
    }
    fn guarded_operation<T>(&self) -> (UringOperation<T>, Responder<T>) {
        kr_runtime_io::completion::SyncOperation::channel_with_guard_and_metrics(
            lock_unpoisoned(&self.handle.control.lifetime_guard).clone(),
            self.handle.control.metrics.get().cloned(),
        )
    }
    fn guarded_ready<T>(&self, output: T) -> UringOperation<T> {
        let (future, responder) = self.guarded_operation();
        responder.complete(output);
        future
    }
    fn try_admit(&self, admitted: &AtomicUsize) -> Result<(), NetworkError> {
        let capacity = self.handle.control.config.command_queue_capacity;
        let mut current = admitted.load(Ordering::Acquire);
        loop {
            if current >= capacity {
                return Err(NetworkError::ResourceExhausted {
                    resource: "io_uring pooled network command queue",
                    limit: capacity,
                });
            }
            match admitted.compare_exchange_weak(
                current,
                current + 1,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return Ok(()),
                Err(observed) => current = observed,
            }
        }
    }

    fn send(&self, command: StreamCommand) {
        self.handle.control.ingress.push(PoolMessage::Command {
            stream: Arc::clone(&self.handle.control),
            command: TerminalCommand::new(command),
        });
    }
}

impl ByteStreamSubmit for PooledUringStream {
    type ReadResponse = UringOperation<ReadCompletion>;
    type WriteResponse = UringOperation<WriteCompletion>;
    type ControlResponse = UringOperation<ControlCompletion>;

    fn submit_read(&self, request: ReadRequest) -> Self::ReadResponse {
        let control = &self.handle.control;
        if read_request_exceeds_bound(&request, control.config.max_operation_bytes) {
            return self.guarded_ready(read_error(
                NetworkError::InvalidRequest {
                    reason: "read request exceeds max_operation_bytes",
                },
                request.buffer,
            ));
        }
        if let Err(error) = self.try_admit(&control.read_admitted) {
            return self.guarded_ready(read_error(error, request.buffer));
        }
        let (future, response) = self.guarded_operation();
        self.send(StreamCommand::Read { request, response });
        future
    }

    fn submit_write(&self, request: WriteRequest) -> Self::WriteResponse {
        let control = &self.handle.control;
        if write_request_exceeds_bound(&request, control.config.max_operation_bytes) {
            return self.guarded_ready(write_error(
                NetworkError::InvalidRequest {
                    reason: "write request exceeds max_operation_bytes",
                },
                request.buffer,
            ));
        }
        if let Err(error) = self.try_admit(&control.write_admitted) {
            return self.guarded_ready(write_error(error, request.buffer));
        }
        let (future, response) = self.guarded_operation();
        self.send(StreamCommand::Write { request, response });
        future
    }

    fn submit_shutdown_write(&self) -> Self::ControlResponse {
        if let Err(error) = self.try_admit(&self.handle.control.write_admitted) {
            return self.guarded_ready(control_error(error));
        }
        let (future, response) = self.guarded_operation();
        self.send(StreamCommand::ShutdownWrite { response });
        future
    }

    fn submit_close(&self) -> Self::ControlResponse {
        self.guarded_ready(self.handle.close())
    }
}

impl ByteStreamVectoredSubmit for PooledUringStream {
    type WriteVectoredResponse = UringOperation<VectoredWriteCompletion>;

    fn max_segments(&self) -> usize {
        MAX_WRITE_SEGMENTS
    }

    fn submit_write_vectored(&self, request: VectoredWriteRequest) -> Self::WriteVectoredResponse {
        let control = &self.handle.control;
        if let Err(error) = request.validate(MAX_WRITE_SEGMENTS, control.config.max_operation_bytes)
        {
            return self.guarded_ready(vectored_error(error, request.segments));
        }
        if let Err(error) = self.try_admit(&control.write_admitted) {
            return self.guarded_ready(vectored_error(error, request.segments));
        }
        let (future, response) = self.guarded_operation();
        self.send(StreamCommand::WriteVectored { request, response });
        future
    }
}

fn vectored_error(error: NetworkError, segments: Vec<WriteSegment>) -> VectoredWriteCompletion {
    Err(CompletionError::not_applied(VectoredWriteFailure::new(
        error, segments, 0,
    )))
}

fn vectored_backend_error(error: &io::Error) -> NetworkError {
    if error.kind() == io::ErrorKind::OutOfMemory {
        NetworkError::ResourceExhausted {
            resource: "vectored send metadata",
            limit: MAX_WRITE_SEGMENTS,
        }
    } else {
        map_stream_error(NetworkOperationKind::Write, error)
    }
}

/// A read armed on the ring: at most one per stream at a time.
struct ActiveRead {
    token: u64,
    /// The caller's buffer, grown by the admitted chunk; the kernel writes
    /// through a pointer into it, so it must stay untouched until the
    /// completion for `token` arrives.
    buffer: Vec<u8>,
    original_len: usize,
    requested: usize,
    response: Responder<ReadCompletion>,
}

/// A send armed on the ring: at most one per stream at a time.
struct ActiveWrite {
    token: u64,
    requested: usize,
    payload: ActiveWritePayload,
}

enum ActiveWritePayload {
    /// The coordinator retains contiguous storage under the original contract.
    Contiguous {
        buffer: Vec<u8>,
        response: Responder<WriteCompletion>,
    },
    /// The ring owns every segment and native pointer target. Its terminal
    /// completion returns the original segment vector to this responder.
    Vectored {
        response: Responder<VectoredWriteCompletion>,
    },
}

struct StreamState {
    control: Arc<StreamControl>,
    read_queue: VecDeque<StreamCommand>,
    write_queue: VecDeque<StreamCommand>,
    active_read: Option<ActiveRead>,
    active_write: Option<ActiveWrite>,
    /// The local write half was shut down; later writes fail `WriteClosed`.
    write_closed: bool,
    /// The last public handle is gone; the state is removed once drained.
    closing: bool,
}

impl StreamState {
    fn new(control: Arc<StreamControl>) -> Self {
        Self {
            control,
            read_queue: VecDeque::new(),
            write_queue: VecDeque::new(),
            active_read: None,
            active_write: None,
            write_closed: false,
            closing: false,
        }
    }

    fn is_drained(&self) -> bool {
        self.read_queue.is_empty()
            && self.write_queue.is_empty()
            && self.active_read.is_none()
            && self.active_write.is_none()
    }
}

/// Which direction a routed completion resolved.
#[derive(Clone, Copy)]
enum Direction {
    Read,
    Write,
}

/// Which state machine a routed token belongs to.
#[derive(Clone, Copy)]
enum TokenOwner {
    Stream(u64),
    Listener(u64),
    /// Keyed by the pair's connect token.
    Connect(u64),
}

/// One accept admitted by a listener handle, waiting FIFO for a connection.
struct PendingAccept {
    stream_permit: ResourcePermit,
    response: Responder<StreamCompletion>,
}

/// One accept-plus-timeout pair armed on the ring, joining its two routed
/// completions.
struct ArmedAccept {
    accept_token: u64,
    accept_result: Option<RoutedResult>,
    timeout_result: Option<RoutedResult>,
    /// The kernel reads the linked timeout through this storage until both
    /// completions arrive.
    _storage: RoutedAcceptStorage,
}

struct ListenerState {
    control: Arc<ListenerControl>,
    listener: TcpListener,
    accepts: VecDeque<PendingAccept>,
    armed: Option<ArmedAccept>,
    _permit: ResourcePermit,
}

/// One connect-plus-timeout pair in flight, joining its two routed
/// completions. The socket must outlive the pair's terminal CQEs because
/// the kernel holds its descriptor.
struct PendingConnect {
    socket: TcpStream,
    stream_permit: ResourcePermit,
    response: Responder<StreamCompletion>,
    connect_result: Option<RoutedResult>,
    timeout_result: Option<RoutedResult>,
    /// The kernel reads the encoded address and linked timeout through this
    /// storage until both completions arrive.
    _storage: RoutedConnectStorage,
}

struct Coordinator {
    ring: Ring,
    config: UringNetPoolConfig,
    /// The submission-side sender for routed completions; cloned into every
    /// routed SQE. The paired receiver below is the coordinator's single
    /// blocking point.
    routed: Sender<RoutedCompletion>,
    completions: Receiver<RoutedCompletion>,
    ingress: Arc<Ingress<PoolMessage>>,
    /// The pool state accepted and connected sockets register through —
    /// weak, so the coordinator's own thread never keeps the pool alive.
    shared: Weak<PoolShared>,
    streams: HashMap<u64, StreamState>,
    listeners: HashMap<u64, ListenerState>,
    /// In-flight connect pairs, keyed by their connect token.
    connects: HashMap<u64, PendingConnect>,
    /// Routed token to its owning state machine, one entry per in-flight
    /// SQE.
    tokens: HashMap<u64, TokenOwner>,
    next_token: u64,
    /// Ring completions still owed — one per in-flight SQE, so a linked
    /// pair counts two. The shutdown gate waits for zero.
    outstanding_ops: usize,
    shutting_down: bool,
}

impl Coordinator {
    fn run(mut self) {
        // The coordinator owns kernel-visible buffers in its active slots,
        // so an internal panic cannot honestly unwind past them.
        let _fail_stop = FailStopOnPanic;
        loop {
            if self.shutting_down && self.outstanding_ops == 0 {
                return;
            }
            let Ok(completion) = self.completions.recv() else {
                // Every sender is gone: no handle, no reactor, no ingress
                // producer. Nothing can arrive, so nothing is owed.
                return;
            };
            if completion.token == WAKE_TOKEN {
                self.drain_ingress();
            } else {
                self.finish_ring(completion);
            }
        }
    }

    fn allocate_token(&mut self) -> u64 {
        let token = self.next_token;
        self.next_token = token
            .checked_add(1)
            .expect("routed token space is practically inexhaustible");
        token
    }

    fn drain_ingress(&mut self) {
        loop {
            let message = self.ingress.pop();
            match message {
                None => return,
                Some(PoolMessage::Command { stream, command }) => {
                    let command = command.into_inner();
                    if stream.is_closed() {
                        // A closed stream's state must not be resurrected:
                        // completing here keeps the map free of entries no
                        // close message will ever drain again.
                        complete_closed(&stream, command);
                        continue;
                    }
                    let id = stream.id;
                    let is_read = matches!(command, StreamCommand::Read { .. });
                    let state = self
                        .streams
                        .entry(id)
                        .or_insert_with(|| StreamState::new(Arc::clone(&stream)));
                    if is_read {
                        state.read_queue.push_back(command);
                        self.pump_read(id);
                    } else {
                        state.write_queue.push_back(command);
                        self.pump_write(id);
                    }
                }
                Some(PoolMessage::Control(command)) => {
                    let command = command.into_inner();
                    match command {
                        ControlCommand::Accept {
                            listener,
                            stream_permit,
                            response,
                        } => match self.listeners.get_mut(&listener) {
                            Some(state) if !state.control.is_closing() => {
                                state.accepts.push_back(PendingAccept {
                                    stream_permit,
                                    response,
                                });
                                self.pump_listener(listener);
                            }
                            // The close won the race to the coordinator or
                            // already removed the state; the accept observes
                            // it instead of hanging.
                            _ => response.complete(no_buffer_error(NetworkError::ListenerClosed)),
                        },
                        ControlCommand::Connect {
                            socket,
                            remote,
                            stream_permit,
                            response,
                        } => {
                            self.start_connect(socket, remote, stream_permit, response);
                        }
                    }
                }
                Some(PoolMessage::RegisterListener(registration)) => {
                    let ListenerRegistration {
                        id,
                        listener,
                        control,
                        permit,
                    } = registration;
                    self.listeners.insert(
                        id,
                        ListenerState {
                            control,
                            listener,
                            accepts: VecDeque::new(),
                            armed: None,
                            _permit: permit,
                        },
                    );
                }
                Some(PoolMessage::CloseListener { listener }) => {
                    self.pump_listener(listener);
                }
                Some(PoolMessage::StreamClosed { stream }) => {
                    if let Some(state) = self.streams.get_mut(&stream) {
                        state.closing = true;
                    }
                    // The pumps observe the close and drain both queues;
                    // armed operations terminalize through the socket
                    // shutdown's CQEs.
                    self.pump_read(stream);
                    self.pump_write(stream);
                    self.remove_if_drained(stream);
                }
                Some(PoolMessage::Shutdown) => {
                    self.shutting_down = true;
                    // Queued commands are refused with DriverStopped;
                    // armed operations are interrupted so their terminal
                    // completions release the exit gate.
                    let ids: Vec<u64> = self.streams.keys().copied().collect();
                    for id in ids {
                        if let Some(state) = self.streams.get_mut(&id) {
                            let control = Arc::clone(&state.control);
                            while let Some(command) = state.read_queue.pop_front() {
                                control.retire_read();
                                command.complete_driver_stopped();
                            }
                            while let Some(command) = state.write_queue.pop_front() {
                                control.retire_write();
                                command.complete_driver_stopped();
                            }
                            if state.active_read.is_some() || state.active_write.is_some() {
                                let _ = control.socket.shutdown(Shutdown::Both);
                            }
                        }
                        self.remove_if_drained(id);
                    }
                    // Listeners drain through their close path: queued
                    // accepts reject, and an armed attempt's linked timeout
                    // bounds how long its completions take. In-flight
                    // connect pairs likewise terminalize within the connect
                    // timeout, which the exit gate awaits.
                    let listener_ids: Vec<u64> = self.listeners.keys().copied().collect();
                    for id in listener_ids {
                        if let Some(state) = self.listeners.get_mut(&id) {
                            state.control.request_close_without_response();
                        }
                        self.pump_listener(id);
                    }
                }
            }
        }
    }

    fn remove_if_drained(&mut self, id: u64) {
        if let Some(state) = self.streams.get(&id)
            && (state.closing || self.shutting_down)
            && state.is_drained()
        {
            self.streams.remove(&id);
        }
    }

    /// Starts the next queued read unless one is already armed.
    ///
    /// Each iteration pops the head command, completes it immediately when
    /// the stream is closed or the request needs no ring transfer, and
    /// otherwise arms exactly one routed receive — the stream's reserved
    /// sustained slot — and returns until its completion pumps again.
    fn pump_read(&mut self, id: u64) {
        loop {
            let Some(state) = self.streams.get_mut(&id) else {
                return;
            };
            if state.active_read.is_some() {
                return;
            }
            let Some(command) = state.read_queue.pop_front() else {
                self.remove_if_drained(id);
                return;
            };
            let control = Arc::clone(&state.control);
            let closed = control.is_closed() || state.closing;
            control.retire_read();
            let StreamCommand::Read { request, response } = command else {
                unreachable!("write command queued on the read direction")
            };
            if closed {
                response.complete(read_error(NetworkError::ConnectionClosed, request.buffer));
                continue;
            }
            let original_len = request.buffer.len();
            let requested = request.max_bytes.min(control.config.max_io_chunk_bytes);
            let Some(target_len) = original_len.checked_add(requested) else {
                response.complete(read_error(
                    NetworkError::InvalidRequest {
                        reason: "read result length overflowed usize",
                    },
                    request.buffer,
                ));
                continue;
            };
            let mut buffer = request.buffer;
            if requested == 0 {
                // A zero-capacity read never reports EOF and needs no CQE.
                response.complete(Ok(ReadResult {
                    buffer,
                    bytes_read: 0,
                    end_of_stream: false,
                }));
                continue;
            }
            if let Err(error) = buffer.try_reserve_exact(requested) {
                response.complete(read_error(
                    backend(NetworkOperationKind::Read, None, error.to_string()),
                    buffer,
                ));
                continue;
            }
            buffer.resize(target_len, 0);
            let token = self.allocate_token();
            match self.ring.start_routed_recv(
                &control.socket,
                &mut buffer,
                original_len,
                requested,
                &self.routed,
                token,
            ) {
                Ok(RoutedSubmission::Submitted { requested }) => {
                    self.tokens.insert(token, TokenOwner::Stream(id));
                    self.outstanding_ops += 1;
                    self.streams
                        .get_mut(&id)
                        .expect("pumped stream state disappeared")
                        .active_read = Some(ActiveRead {
                        token,
                        buffer,
                        original_len,
                        requested,
                        response,
                    });
                    return;
                }
                Ok(RoutedSubmission::Empty) => {
                    // `requested` was checked nonzero, so the validated
                    // chunk cannot be empty; complete honestly regardless.
                    buffer.truncate(original_len);
                    response.complete(Ok(ReadResult {
                        buffer,
                        bytes_read: 0,
                        end_of_stream: false,
                    }));
                }
                Err(error) => {
                    buffer.truncate(original_len);
                    let error = if control.is_closed() {
                        NetworkError::ConnectionClosed
                    } else {
                        map_stream_error(NetworkOperationKind::Read, &error)
                    };
                    response.complete(read_error(error, buffer));
                }
            }
        }
    }

    /// Starts the next queued write or shutdown unless a send is armed.
    ///
    /// Mirrors [`Self::pump_read`]: one armed routed send at a time on the
    /// stream's reserved sustained slot, with `shutdown_write` executed at
    /// its FIFO turn as a direct syscall — it cannot block, so it needs no
    /// ring slot.
    fn pump_write(&mut self, id: u64) {
        loop {
            let Some(state) = self.streams.get_mut(&id) else {
                return;
            };
            if state.active_write.is_some() {
                return;
            }
            let Some(command) = state.write_queue.pop_front() else {
                self.remove_if_drained(id);
                return;
            };
            let control = Arc::clone(&state.control);
            let closed = control.is_closed() || state.closing;
            let write_closed = state.write_closed;
            control.retire_write();
            match command {
                StreamCommand::ShutdownWrite { response } => {
                    if closed || write_closed {
                        response.complete(Ok(()));
                        continue;
                    }
                    match control.socket.shutdown(Shutdown::Write) {
                        Ok(()) => {
                            self.streams
                                .get_mut(&id)
                                .expect("pumped stream state disappeared")
                                .write_closed = true;
                            response.complete(Ok(()));
                        }
                        Err(error) => {
                            response.complete(Err(CompletionError::may_have_applied(
                                NetworkFailure::without_buffer(backend(
                                    NetworkOperationKind::ShutdownWrite,
                                    error.raw_os_error(),
                                    error.to_string(),
                                )),
                            )));
                        }
                    }
                }
                StreamCommand::Write { request, response } => {
                    if closed {
                        response
                            .complete(write_error(NetworkError::ConnectionClosed, request.buffer));
                        continue;
                    }
                    if write_closed {
                        response.complete(write_error(NetworkError::WriteClosed, request.buffer));
                        continue;
                    }
                    let buffer = request.buffer;
                    let len = buffer.len();
                    if len == 0 {
                        response.complete(Ok(WriteResult {
                            buffer,
                            bytes_written: 0,
                        }));
                        continue;
                    }
                    let token = self.allocate_token();
                    match self.ring.start_routed_send(
                        &control.socket,
                        &buffer,
                        0,
                        len,
                        &self.routed,
                        token,
                    ) {
                        Ok(RoutedSubmission::Submitted { requested }) => {
                            self.tokens.insert(token, TokenOwner::Stream(id));
                            self.outstanding_ops += 1;
                            self.streams
                                .get_mut(&id)
                                .expect("pumped stream state disappeared")
                                .active_write = Some(ActiveWrite {
                                token,
                                requested,
                                payload: ActiveWritePayload::Contiguous { buffer, response },
                            });
                            return;
                        }
                        Ok(RoutedSubmission::Empty) => {
                            // `len` was checked nonzero, so the validated
                            // chunk cannot be empty; complete honestly.
                            response.complete(Ok(WriteResult {
                                buffer,
                                bytes_written: 0,
                            }));
                        }
                        Err(error) => {
                            let error = if control.is_closed() {
                                NetworkError::ConnectionClosed
                            } else {
                                map_stream_error(NetworkOperationKind::Write, &error)
                            };
                            response.complete(write_error(error, buffer));
                        }
                    }
                }
                StreamCommand::WriteVectored { request, response } => {
                    if closed {
                        response.complete(vectored_error(
                            NetworkError::ConnectionClosed,
                            request.segments,
                        ));
                        continue;
                    }
                    if write_closed {
                        response
                            .complete(vectored_error(NetworkError::WriteClosed, request.segments));
                        continue;
                    }
                    let token = self.allocate_token();
                    match self.ring.start_routed_send_vectored(
                        &control.socket,
                        request,
                        &self.routed,
                        token,
                    ) {
                        Ok(requested) => {
                            self.tokens.insert(token, TokenOwner::Stream(id));
                            self.outstanding_ops += 1;
                            self.streams
                                .get_mut(&id)
                                .expect("pumped stream state disappeared")
                                .active_write = Some(ActiveWrite {
                                token,
                                requested,
                                payload: ActiveWritePayload::Vectored { response },
                            });
                            return;
                        }
                        Err(failure) => {
                            let error = if control.is_closed() {
                                NetworkError::ConnectionClosed
                            } else {
                                vectored_backend_error(&failure.error)
                            };
                            response.complete(vectored_error(error, failure.segments));
                        }
                    }
                }
                StreamCommand::Read { .. } => {
                    unreachable!("read command queued on the write direction")
                }
            }
        }
    }

    /// Rejects, arms, or retires a listener as its state directs.
    ///
    /// A closing listener rejects every queued accept, and once its armed
    /// attempt has drained it is removed and every close waiter observes
    /// the terminal. An open listener with pending accepts keeps exactly
    /// one accept-plus-timeout pair armed; a failed arm attempt fails the
    /// head accept and retries for the next.
    fn pump_listener(&mut self, id: u64) {
        loop {
            {
                let Some(state) = self.listeners.get_mut(&id) else {
                    return;
                };
                if state.control.is_closing() {
                    while let Some(pending) = state.accepts.pop_front() {
                        pending
                            .response
                            .complete(no_buffer_error(NetworkError::ListenerClosed));
                    }
                    if state.armed.is_none() {
                        let state = self
                            .listeners
                            .remove(&id)
                            .expect("drained listener state disappeared");
                        // The binding closes before the terminal is
                        // published, so a completed close means the address
                        // is releasable.
                        drop(state.listener);
                        state.control.finish(ListenerTerminal::Closed);
                    }
                    return;
                }
                if state.armed.is_some() || state.accepts.is_empty() {
                    return;
                }
            }
            let accept_token = self.allocate_token();
            let timeout_token = self.allocate_token();
            let state = self
                .listeners
                .get_mut(&id)
                .expect("pumped listener state disappeared");
            match self.ring.start_routed_accept(
                &state.listener,
                ACCEPT_ARM_TIMEOUT,
                &self.routed,
                accept_token,
                timeout_token,
            ) {
                Ok(storage) => {
                    state.armed = Some(ArmedAccept {
                        accept_token,
                        accept_result: None,
                        timeout_result: None,
                        _storage: storage,
                    });
                    self.tokens.insert(accept_token, TokenOwner::Listener(id));
                    self.tokens.insert(timeout_token, TokenOwner::Listener(id));
                    self.outstanding_ops += 2;
                    return;
                }
                Err(error) => {
                    // Nothing was staged, so the head accept fails closed
                    // and the next waiter gets its own attempt.
                    let pending = state
                        .accepts
                        .pop_front()
                        .expect("armed attempt had a waiter");
                    pending.response.complete(Err(not_applied_network(backend(
                        NetworkOperationKind::Accept,
                        error.raw_os_error(),
                        error.to_string(),
                    ))));
                }
            }
        }
    }

    fn finish_listener(&mut self, id: u64, completion: RoutedCompletion) {
        let state = self
            .listeners
            .get_mut(&id)
            .expect("completed listener state disappeared");
        let armed = state
            .armed
            .as_mut()
            .expect("listener completion arrived without an armed attempt");
        if completion.token == armed.accept_token {
            armed.accept_result = Some(completion.result);
        } else {
            armed.timeout_result = Some(completion.result);
        }
        if armed.accept_result.is_none() || armed.timeout_result.is_none() {
            return;
        }
        let ArmedAccept {
            accept_result,
            timeout_result,
            ..
        } = state.armed.take().expect("joined attempt is present");
        let accept = accept_result.expect("accept result is present");
        let timeout = timeout_result.expect("timeout result is present");
        let closing = state.control.is_closing();
        match self.ring.finish_routed_accept(accept, timeout) {
            Ok(RoutedAcceptOutcome::Accepted(socket)) => {
                match state.accepts.pop_front() {
                    // Close already rejected the queue; the dequeued
                    // connection has no waiter and closes by RAII.
                    None => drop(socket),
                    Some(pending) if closing => {
                        drop(socket);
                        pending.response.complete(Err(accepted_then_closed()));
                    }
                    Some(pending) => match self.shared.upgrade() {
                        Some(shared) => {
                            let stream = shared.register_with_permit(socket, pending.stream_permit);
                            pending.response.complete(Ok(stream));
                        }
                        // The pool is tearing down; the accepted connection
                        // cannot register, so it closes and the response is
                        // honest about the consumed backlog entry.
                        None => {
                            drop(socket);
                            pending.response.complete(Err(accepted_then_closed()));
                        }
                    },
                }
            }
            // The attempt's timeout expired; re-arming below picks the
            // waiter back up.
            Ok(RoutedAcceptOutcome::NoConnection) => {}
            Ok(RoutedAcceptOutcome::Failed(error)) => {
                if !is_retryable_accept_error(&error)
                    && let Some(pending) = state.accepts.pop_front()
                {
                    pending.response.complete(if closing {
                        Err(not_applied_network(NetworkError::ListenerClosed))
                    } else {
                        Err(CompletionError::may_have_applied(
                            NetworkFailure::without_buffer(backend(
                                NetworkOperationKind::Accept,
                                error.raw_os_error(),
                                error.to_string(),
                            )),
                        ))
                    });
                }
            }
            // The pair's completions were inconsistent and the ring
            // poisoned; the head accept cannot be presumed effect-free.
            Err(error) => {
                if let Some(pending) = state.accepts.pop_front() {
                    pending
                        .response
                        .complete(Err(CompletionError::may_have_applied(
                            NetworkFailure::without_buffer(backend(
                                NetworkOperationKind::Accept,
                                error.raw_os_error(),
                                error.to_string(),
                            )),
                        )));
                }
            }
        }
        self.pump_listener(id);
    }

    fn start_connect(
        &mut self,
        socket: TcpStream,
        remote: SocketAddr,
        stream_permit: ResourcePermit,
        response: Responder<StreamCompletion>,
    ) {
        let connect_token = self.allocate_token();
        let timeout_token = self.allocate_token();
        match self.ring.start_routed_connect(
            &socket,
            remote,
            self.config.connect_timeout,
            &self.routed,
            connect_token,
            timeout_token,
        ) {
            Ok(storage) => {
                self.tokens
                    .insert(connect_token, TokenOwner::Connect(connect_token));
                self.tokens
                    .insert(timeout_token, TokenOwner::Connect(connect_token));
                self.outstanding_ops += 2;
                self.connects.insert(
                    connect_token,
                    PendingConnect {
                        socket,
                        stream_permit,
                        response,
                        connect_result: None,
                        timeout_result: None,
                        _storage: storage,
                    },
                );
            }
            Err(error) => {
                response.complete(Err(not_applied_network(map_connect_error(error))));
            }
        }
    }

    fn finish_connect(&mut self, key: u64, completion: RoutedCompletion) {
        let pending = self
            .connects
            .get_mut(&key)
            .expect("completed connect state disappeared");
        if completion.token == key {
            pending.connect_result = Some(completion.result);
        } else {
            pending.timeout_result = Some(completion.result);
        }
        if pending.connect_result.is_none() || pending.timeout_result.is_none() {
            return;
        }
        let PendingConnect {
            socket,
            stream_permit,
            response,
            connect_result,
            timeout_result,
            ..
        } = self
            .connects
            .remove(&key)
            .expect("joined connect state is present");
        let connect = connect_result.expect("connect result is present");
        let timeout = timeout_result.expect("timeout result is present");
        match self.ring.finish_routed_connect(connect, timeout) {
            Ok(()) => match self.shared.upgrade() {
                Some(shared) => {
                    let stream = shared.register_with_permit(socket, stream_permit);
                    response.complete(Ok(stream));
                }
                // The pool is tearing down: the TCP connection exists but
                // no stream can register, so it closes and the response
                // reports the applied effect.
                None => {
                    drop(socket);
                    response.complete(Err(CompletionError::applied(
                        NetworkFailure::without_buffer(NetworkError::DriverStopped),
                    )));
                }
            },
            Err(failure) => {
                let (error, may_have_applied) = failure.into_parts();
                let output = NetworkFailure::without_buffer(map_connect_error(error));
                response.complete(Err(if may_have_applied {
                    CompletionError::may_have_applied(output)
                } else {
                    CompletionError::not_applied(output)
                }));
            }
        }
    }

    fn finish_ring(&mut self, completion: RoutedCompletion) {
        let owner = self
            .tokens
            .remove(&completion.token)
            .expect("routed completion carried an unknown token");
        self.outstanding_ops = self
            .outstanding_ops
            .checked_sub(1)
            .expect("outstanding completion count underflowed");
        match owner {
            TokenOwner::Stream(id) => self.finish_stream(id, completion),
            TokenOwner::Listener(id) => self.finish_listener(id, completion),
            TokenOwner::Connect(key) => self.finish_connect(key, completion),
        }
    }

    fn finish_stream(&mut self, id: u64, completion: RoutedCompletion) {
        let state = self
            .streams
            .get_mut(&id)
            .expect("completed stream state disappeared");
        let closed = state.control.is_closed();
        let direction = if state
            .active_read
            .as_ref()
            .is_some_and(|active| active.token == completion.token)
        {
            Direction::Read
        } else if state
            .active_write
            .as_ref()
            .is_some_and(|active| active.token == completion.token)
        {
            Direction::Write
        } else {
            unreachable!("routed completion matched no active stream operation")
        };
        match direction {
            Direction::Read => {
                let ActiveRead {
                    mut buffer,
                    original_len,
                    requested,
                    response,
                    ..
                } = state
                    .active_read
                    .take()
                    .expect("checked active read is present");
                match self
                    .ring
                    .finish_routed_transfer(requested, completion.result)
                {
                    Ok(transferred) => {
                        buffer.truncate(original_len + transferred);
                        response.complete(Ok(ReadResult {
                            buffer,
                            bytes_read: transferred,
                            end_of_stream: requested > 0 && transferred == 0,
                        }));
                    }
                    Err(failure) => {
                        buffer.truncate(original_len);
                        let error = if closed {
                            NetworkError::ConnectionClosed
                        } else {
                            map_stream_error(NetworkOperationKind::Read, &failure.error)
                        };
                        let output = NetworkFailure::with_buffer(error, buffer, 0);
                        response.complete(Err(if failure.may_have_applied {
                            CompletionError::may_have_applied(output)
                        } else {
                            CompletionError::not_applied(output)
                        }));
                    }
                }
            }
            Direction::Write => {
                let ActiveWrite {
                    requested, payload, ..
                } = state
                    .active_write
                    .take()
                    .expect("checked active write is present");
                let result = self
                    .ring
                    .finish_routed_transfer(requested, completion.result);
                match payload {
                    ActiveWritePayload::Contiguous { buffer, response } => match result {
                        Ok(transferred) => response.complete(Ok(WriteResult {
                            buffer,
                            bytes_written: transferred,
                        })),
                        Err(failure) => {
                            let error = if closed {
                                NetworkError::ConnectionClosed
                            } else {
                                map_stream_error(NetworkOperationKind::Write, &failure.error)
                            };
                            let output = NetworkFailure::with_buffer(error, buffer, 0);
                            response.complete(Err(if failure.may_have_applied {
                                CompletionError::may_have_applied(output)
                            } else {
                                CompletionError::not_applied(output)
                            }));
                        }
                    },
                    ActiveWritePayload::Vectored { response } => {
                        let segments = completion
                            .segments
                            .expect("terminal vectored completion returns its original segments");
                        match result {
                            Ok(transferred) => response.complete(Ok(VectoredWriteResult {
                                segments,
                                bytes_written: transferred,
                            })),
                            Err(failure) => {
                                let error = if closed {
                                    NetworkError::ConnectionClosed
                                } else {
                                    vectored_backend_error(&failure.error)
                                };
                                let output = VectoredWriteFailure::new(error, segments, 0);
                                response.complete(Err(if failure.may_have_applied {
                                    CompletionError::may_have_applied(output)
                                } else {
                                    CompletionError::not_applied(output)
                                }));
                            }
                        }
                    }
                }
            }
        }
        match direction {
            Direction::Read => self.pump_read(id),
            Direction::Write => self.pump_write(id),
        }
        self.remove_if_drained(id);
    }
}

/// Completes a command for a stream whose close preceded it, without
/// touching coordinator state.
fn complete_closed(stream: &StreamControl, command: StreamCommand) {
    match command {
        StreamCommand::Read { request, response } => {
            stream.retire_read();
            response.complete(read_error(NetworkError::ConnectionClosed, request.buffer));
        }
        StreamCommand::Write { request, response } => {
            stream.retire_write();
            response.complete(write_error(NetworkError::ConnectionClosed, request.buffer));
        }
        StreamCommand::WriteVectored { request, response } => {
            stream.retire_write();
            response.complete(vectored_error(
                NetworkError::ConnectionClosed,
                request.segments,
            ));
        }
        StreamCommand::ShutdownWrite { response } => {
            stream.retire_write();
            response.complete(Ok(()));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::block_on;
    use kr_runtime::CompletionCertainty;
    use std::net::Ipv4Addr;
    use std::time::Duration;

    /// A coordinator whose thread is the test itself, mirroring the file
    /// pool's white-box harness: commands and completions are delivered one
    /// event at a time so poison behavior is asserted deterministically.
    struct Harness {
        coordinator: Coordinator,
        ingress: Arc<Ingress<PoolMessage>>,
        permits: Arc<ResourcePool>,
        config: UringNetPoolConfig,
    }

    impl Harness {
        fn new() -> Self {
            let config = UringNetPoolConfig {
                max_streams: 4,
                command_queue_capacity: 8,
                ring_entries: 8,
                max_operation_bytes: 64 * 1024,
                max_io_chunk_bytes: 64 * 1024,
                max_listeners: 4,
                max_listener_backlog: 64,
                connect_timeout: Duration::from_secs(10),
            };
            let ring = Ring::for_stream_pool(
                RingCapacity {
                    entries: config.ring_entries,
                    transient: transient_slots(config).expect("test transient reservation fits"),
                    sustained: sustained_slots(config.max_streams)
                        .expect("test sustained reservation fits"),
                },
                config.max_io_chunk_bytes,
            )
            .expect("create test ring");
            let (routed, completions) = mpsc::channel();
            let ingress = Arc::new(Ingress::new(routed.clone()));
            Self {
                coordinator: Coordinator {
                    ring,
                    config,
                    routed,
                    completions,
                    ingress: Arc::clone(&ingress),
                    // No accepts or connects register through this harness,
                    // so a dangling pool reference is sufficient.
                    shared: Weak::new(),
                    streams: HashMap::new(),
                    listeners: HashMap::new(),
                    connects: HashMap::new(),
                    tokens: HashMap::new(),
                    next_token: WAKE_TOKEN + 1,
                    outstanding_ops: 0,
                    shutting_down: false,
                },
                ingress,
                permits: Arc::new(ResourcePool::new(4)),
                config,
            }
        }

        /// Registers one loopback stream, returning its control and the
        /// peer socket, which stays silent unless the test speaks.
        fn register(&self, id: u64) -> (Arc<StreamControl>, TcpStream) {
            let listener = TcpListener::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)))
                .expect("bind loopback listener");
            let address = listener.local_addr().expect("read listener address");
            let socket = TcpStream::connect(address).expect("connect loopback client");
            let (peer, _) = listener.accept().expect("accept loopback client");
            let control = Arc::new(StreamControl {
                id,
                socket,
                config: self.config,
                read_admitted: AtomicUsize::new(0),
                write_admitted: AtomicUsize::new(0),
                closed: AtomicBool::new(false),
                ingress: Arc::clone(&self.ingress),
                _permit: self.permits.acquire().expect("stream permit is available"),
                lifetime_guard: Mutex::new(None),
                metrics: std::sync::OnceLock::new(),
            });
            (control, peer)
        }

        fn submit(&mut self, control: &Arc<StreamControl>, command: StreamCommand) {
            match &command {
                StreamCommand::Read { .. } => {
                    control.read_admitted.fetch_add(1, Ordering::AcqRel);
                }
                StreamCommand::Write { .. }
                | StreamCommand::WriteVectored { .. }
                | StreamCommand::ShutdownWrite { .. } => {
                    control.write_admitted.fetch_add(1, Ordering::AcqRel);
                }
            }
            self.ingress.push(PoolMessage::Command {
                stream: Arc::clone(control),
                command: TerminalCommand::new(command),
            });
            self.coordinator.drain_ingress();
        }

        /// Feeds the next real ring completion to the coordinator, skipping
        /// the wake sentinels `Ingress::push` interleaves on the channel.
        fn deliver_ring_completion(&mut self) {
            loop {
                let completion = self
                    .coordinator
                    .completions
                    .recv_timeout(Duration::from_secs(10))
                    .expect("ring completion arrives");
                if completion.token == WAKE_TOKEN {
                    continue;
                }
                self.coordinator.finish_ring(completion);
                return;
            }
        }
    }

    #[test]
    fn a_poisoned_ring_terminalizes_armed_transfers_and_fails_later_ones_closed() {
        let mut harness = Harness::new();
        let (first, _first_peer) = harness.register(1);

        // Arm a receive against a silent peer: only the poison drain's
        // cancel can complete it, so the interruption is deterministic.
        let (armed_read, armed_response) = operation();
        harness.submit(
            &first,
            StreamCommand::Read {
                request: ReadRequest {
                    buffer: Vec::new(),
                    max_bytes: 8,
                },
                response: armed_response,
            },
        );
        assert_eq!(harness.coordinator.outstanding_ops, 1);

        harness.coordinator.ring.poison_for_test();
        harness.deliver_ring_completion();
        let interrupted = block_on(armed_read).expect_err("armed receive terminalizes");
        assert_eq!(interrupted.certainty(), CompletionCertainty::NotApplied);
        let (_, interrupted) = interrupted.into_parts();
        assert!(matches!(interrupted.error(), NetworkError::Backend { .. }));
        assert!(
            interrupted.into_buffer().is_some(),
            "the armed receive returned its buffer"
        );

        // Later submissions on the same stream fail closed before any
        // effect.
        let (rejected_read, rejected_response) = operation();
        harness.submit(
            &first,
            StreamCommand::Read {
                request: ReadRequest {
                    buffer: Vec::new(),
                    max_bytes: 8,
                },
                response: rejected_response,
            },
        );
        let rejected = block_on(rejected_read).expect_err("poisoned submission fails");
        assert_eq!(rejected.certainty(), CompletionCertainty::NotApplied);
        assert!(matches!(
            rejected.error().error(),
            NetworkError::Backend { .. }
        ));

        // The blast radius is the whole provider: a second stream's first
        // write observes the poison identically, with its buffer returned.
        let (second, _second_peer) = harness.register(2);
        let (rejected_write, write_response) = operation();
        harness.submit(
            &second,
            StreamCommand::Write {
                request: WriteRequest { buffer: vec![9; 8] },
                response: write_response,
            },
        );
        let refused = block_on(rejected_write).expect_err("second stream is inside the radius");
        assert_eq!(refused.certainty(), CompletionCertainty::NotApplied);
        let (_, refused) = refused.into_parts();
        assert!(matches!(refused.error(), NetworkError::Backend { .. }));
        assert_eq!(
            refused.into_buffer().map(|buffer| buffer.len()),
            Some(8),
            "the write returned its buffer"
        );

        // Nothing armed remains and nothing is owed: teardown is clean.
        assert_eq!(harness.coordinator.outstanding_ops, 0);
        assert!(harness.coordinator.tokens.is_empty());
    }

    #[test]
    fn invalid_native_vectored_progress_returns_ownership_and_fences_the_pool() {
        let mut harness = Harness::new();
        let (control, _peer) = harness.register(1);
        let owner = kr_runtime_io::SharedBytes::from(vec![1; 8]);
        let segments = vec![WriteSegment {
            bytes: owner.clone(),
            range: 0..8,
        }];
        let pointer = segments.as_ptr();
        let (future, response) = operation();
        harness.submit(
            &control,
            StreamCommand::WriteVectored {
                request: VectoredWriteRequest { segments },
                response,
            },
        );
        // Wait for the real terminal CQE before corrupting its reported count.
        // Native metadata is already released, so the injected malformed result
        // cannot manufacture an early lifetime boundary for kernel pointers.
        let mut completion = loop {
            let completion = harness
                .coordinator
                .completions
                .recv_timeout(Duration::from_secs(10))
                .expect("native vectored CQE arrives");
            if completion.token != WAKE_TOKEN {
                break completion;
            }
        };
        assert_eq!(
            completion
                .segments
                .as_ref()
                .expect("vectored ownership")
                .as_ptr(),
            pointer
        );
        assert_eq!(
            completion.result.as_ref().expect("native send succeeded"),
            &8
        );
        completion.result = Ok(9);
        harness.coordinator.finish_ring(completion);
        let failure = block_on(future).expect_err("impossible positive progress is uncertain");
        assert_eq!(failure.certainty(), CompletionCertainty::MayHaveApplied);
        assert_eq!(failure.error().segments.as_ptr(), pointer);
        assert_eq!(failure.error().bytes_transferred(), 0);
        drop(failure);
        assert_eq!(owner.strong_count(), 1);

        let (later, response) = operation();
        harness.submit(
            &control,
            StreamCommand::WriteVectored {
                request: VectoredWriteRequest {
                    segments: vec![WriteSegment {
                        bytes: owner.clone(),
                        range: 0..8,
                    }],
                },
                response,
            },
        );
        let failure = block_on(later).expect_err("poisoned pool rejects later vectored sends");
        assert_eq!(failure.certainty(), CompletionCertainty::NotApplied);
        assert!(matches!(
            failure.error().error(),
            NetworkError::Backend { .. }
        ));
        drop(failure);
        assert_eq!(owner.strong_count(), 1);
        assert_eq!(harness.coordinator.outstanding_ops, 0);
        assert!(harness.coordinator.tokens.is_empty());
    }

    #[test]
    fn config_rejects_invalid_ring_shape() {
        let config = UringNetPoolConfig {
            ring_entries: 3,
            ..UringNetPoolConfig::default()
        };
        assert!(matches!(
            validate_config(config),
            Err(UringNetPoolOpenError::InvalidConfig {
                field: "ring_entries",
                ..
            })
        ));
    }

    #[test]
    fn config_rejects_zero_stream_and_queue_bounds() {
        for (config, field) in [
            (
                UringNetPoolConfig {
                    max_streams: 0,
                    ..UringNetPoolConfig::default()
                },
                "max_streams",
            ),
            (
                UringNetPoolConfig {
                    command_queue_capacity: 0,
                    ..UringNetPoolConfig::default()
                },
                "command_queue_capacity",
            ),
            (
                UringNetPoolConfig {
                    max_operation_bytes: 0,
                    ..UringNetPoolConfig::default()
                },
                "max_operation_bytes",
            ),
            (
                UringNetPoolConfig {
                    max_io_chunk_bytes: 0,
                    ..UringNetPoolConfig::default()
                },
                "max_io_chunk_bytes",
            ),
        ] {
            let result = validate_config(config);
            match result {
                Err(UringNetPoolOpenError::InvalidConfig { field: actual, .. }) => {
                    assert_eq!(actual, field);
                }
                other => panic!("{field} validation returned {other:?}"),
            }
        }
    }

    #[test]
    fn sustained_reservation_covers_both_directions_and_checks_overflow() {
        assert_eq!(sustained_slots(256), Some(512));
        assert_eq!(sustained_slots(usize::MAX), None);
        assert!(matches!(
            validate_config(UringNetPoolConfig {
                max_streams: usize::MAX,
                ..UringNetPoolConfig::default()
            }),
            Err(UringNetPoolOpenError::InvalidConfig {
                field: "max_streams",
                ..
            })
        ));
    }
}
