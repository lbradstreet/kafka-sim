use crate::driver::{self, Command, Resource, ResourceKind};
use kr_runtime::{CompletionError, CompletionResult};
use kr_runtime_io::completion::{SyncOperation, SyncResponder};
use kr_runtime_io::network::{
    ByteStreamSubmit, ByteStreamVectoredSubmit, ConnectRequest, ListenRequest, NetworkError,
    NetworkFailure, NetworkListenerSubmit, NetworkOperationKind, NetworkProviderSubmit,
    ReadRequest, ReadResult, VectoredWriteFailure, VectoredWriteRequest, VectoredWriteResult,
    WriteRequest, WriteResult,
};
use rustix::{
    event::{EventfdFlags, epoll, eventfd},
    fd::OwnedFd,
};
use std::{
    collections::VecDeque,
    net::SocketAddr,
    sync::{
        Arc, Mutex, MutexGuard, OnceLock, Weak,
        atomic::{AtomicBool, Ordering},
    },
    thread::JoinHandle,
    time::{Duration, Instant},
};

pub(crate) type Response<T> = SyncResponder<CompletionResult<T, NetworkFailure>>;
pub(crate) type Operation<T> = SyncOperation<CompletionResult<T, NetworkFailure>>;
pub(crate) type VectoredResponse =
    SyncResponder<CompletionResult<VectoredWriteResult, VectoredWriteFailure>>;

/// All retained operation and socket resources are bounded independently.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ReadinessConfig {
    /// Maximum time from connect admission until socket establishment.
    pub connect_timeout: Duration,
    pub max_streams: usize,
    pub max_listeners: usize,
    pub max_listener_backlog: usize,
    pub max_control_operations: usize,
    pub max_read_operations: usize,
    pub max_write_operations: usize,
    pub max_operation_bytes: usize,
    pub max_outstanding_read_bytes: usize,
    pub max_outstanding_write_bytes: usize,
    pub max_segments: usize,
    /// Maximum bytes returned by one transfer; also bounds one reactor quantum.
    pub max_chunk_bytes: usize,
    /// Requested kernel socket buffer size. The kernel may round or double it.
    pub socket_buffer_bytes: usize,
}
impl Default for ReadinessConfig {
    fn default() -> Self {
        Self {
            connect_timeout: Duration::from_secs(10),
            max_streams: 1024,
            max_listeners: 128,
            max_listener_backlog: 128,
            max_control_operations: 512,
            max_read_operations: 2048,
            max_write_operations: 2048,
            max_operation_bytes: 1024 * 1024,
            max_outstanding_read_bytes: 64 * 1024 * 1024,
            max_outstanding_write_bytes: 64 * 1024 * 1024,
            max_segments: 64,
            max_chunk_bytes: 256 * 1024,
            socket_buffer_bytes: 256 * 1024,
        }
    }
}
impl ReadinessConfig {
    pub(crate) fn validate(self) -> Result<usize, NetworkError> {
        if self.connect_timeout.is_zero() || self.connect_timeout > Duration::from_secs(86_400) {
            return Err(NetworkError::InvalidConfig {
                reason: "connect timeout must be nonzero and at most one day",
            });
        }
        if [
            self.max_streams,
            self.max_listeners,
            self.max_listener_backlog,
            self.max_control_operations,
            self.max_read_operations,
            self.max_write_operations,
            self.max_operation_bytes,
            self.max_outstanding_read_bytes,
            self.max_outstanding_write_bytes,
            self.max_segments,
            self.max_chunk_bytes,
            self.socket_buffer_bytes,
        ]
        .contains(&0)
        {
            return Err(NetworkError::InvalidConfig {
                reason: "readiness limits must be nonzero",
            });
        }
        if self.max_segments > 1024 || self.max_listener_backlog > i32::MAX as usize {
            return Err(NetworkError::InvalidConfig {
                reason: "segment or listen backlog limit exceeds Linux bound",
            });
        }
        self.max_streams
            .checked_add(self.max_listeners)
            .and_then(|objects| objects.checked_mul(2))
            .and_then(|n| n.checked_add(self.max_control_operations))
            .and_then(|n| n.checked_add(self.max_read_operations))
            .and_then(|n| n.checked_add(self.max_write_operations))
            .ok_or(NetworkError::InvalidConfig {
                reason: "readiness queue bounds overflow",
            })
    }
}

/// Passive provider accounting, including completed responses not yet consumed.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ReadinessStatus {
    pub streams: usize,
    pub listeners: usize,
    pub read_operations: usize,
    pub write_operations: usize,
    pub control_operations: usize,
    pub close_operations: usize,
    pub outstanding_read_bytes: usize,
    pub outstanding_write_bytes: usize,
    pub queued_commands: usize,
    pub stopped: bool,
}

/// Cloneable native TCP provider backed by one bounded epoll thread.
#[derive(Clone)]
pub struct ReadinessNet {
    pub(crate) owner: Arc<Owner>,
}

impl ReadinessNet {
    /// Starts an empty provider. No executor is created or driven.
    ///
    /// # Errors
    /// Rejects invalid bounds and reports epoll/eventfd/thread/allocation failure.
    pub fn new(config: ReadinessConfig) -> Result<Self, NetworkError> {
        let capacity = config.validate()?;
        let epoll = epoll::create(epoll::CreateFlags::CLOEXEC)
            .map_err(|e| backend(NetworkOperationKind::Connect, e))?;
        let wake = Arc::new(
            eventfd(0, EventfdFlags::CLOEXEC | EventfdFlags::NONBLOCK)
                .map_err(|e| backend(NetworkOperationKind::Connect, e))?,
        );
        epoll::add(
            &epoll,
            &*wake,
            epoll::EventData::new_u64(0),
            epoll::EventFlags::IN,
        )
        .map_err(|e| backend(NetworkOperationKind::Connect, e))?;
        let mut commands = VecDeque::new();
        commands
            .try_reserve_exact(capacity)
            .map_err(|_| exhausted("readiness command allocation", capacity))?;
        let shared = Arc::new(Shared {
            config,
            wake,
            stopped: AtomicBool::new(false),
            #[cfg(test)]
            vectored_eagain: Mutex::new(None),
            queue: Mutex::new(Queue {
                commands,
                next_id: 1,
                operations: [0; 4],
                read_bytes: 0,
                write_bytes: 0,
                streams: 0,
                listeners: 0,
            }),
        });
        let owner = Arc::new(Owner {
            shared: shared.clone(),
            thread: Mutex::new(None),
        });
        let thread = std::thread::Builder::new()
            .name("kr-epoll".into())
            .spawn(move || driver::run(epoll, shared))
            .map_err(|error| NetworkError::Backend {
                operation: NetworkOperationKind::Connect,
                raw_os_error: error.raw_os_error(),
                message: error.to_string(),
            })?;
        *lock(&owner.thread) = Some(thread);
        Ok(Self { owner })
    }

    #[must_use]
    pub fn status(&self) -> ReadinessStatus {
        self.owner.shared.status()
    }

    /// A passive weak observer does not postpone provider shutdown.
    pub fn observer(&self) -> ReadinessObserver {
        ReadinessObserver {
            shared: Arc::downgrade(&self.owner.shared),
        }
    }
}

#[derive(Clone)]
pub struct ReadinessObserver {
    shared: Weak<Shared>,
}
impl ReadinessObserver {
    pub fn status(&self) -> Option<ReadinessStatus> {
        self.shared.upgrade().map(|shared| shared.status())
    }
}
impl Shared {
    fn status(&self) -> ReadinessStatus {
        let q = lock(&self.queue);
        ReadinessStatus {
            streams: q.streams,
            listeners: q.listeners,
            read_operations: q.operations[Kind::Read as usize],
            write_operations: q.operations[Kind::Write as usize],
            control_operations: q.operations[Kind::Control as usize],
            close_operations: q.operations[Kind::Close as usize],
            outstanding_read_bytes: q.read_bytes,
            outstanding_write_bytes: q.write_bytes,
            queued_commands: q.commands.len(),
            stopped: self.stopped.load(Ordering::Acquire),
        }
    }
}

pub(crate) struct Owner {
    pub(crate) shared: Arc<Shared>,
    thread: Mutex<Option<JoinHandle<()>>>,
}
impl Drop for Owner {
    fn drop(&mut self) {
        self.shared.stopped.store(true, Ordering::Release);
        self.shared.wake();
        if let Some(thread) = lock(&self.thread).take() {
            // Completing an abandoned final response may destroy the last owner
            // on the reactor itself. It must exit naturally instead of self-join.
            if thread.thread().id() != std::thread::current().id() {
                let _ = thread.join();
            }
        }
    }
}

pub(crate) struct Shared {
    pub(crate) config: ReadinessConfig,
    pub(crate) wake: Arc<OwnedFd>,
    pub(crate) stopped: AtomicBool,
    pub(crate) queue: Mutex<Queue>,
    /// One-shot observation of a real sendmsg EAGAIN, private to native tests.
    #[cfg(test)]
    vectored_eagain: Mutex<Option<std::sync::mpsc::SyncSender<()>>>,
}
pub(crate) struct Queue {
    pub(crate) commands: VecDeque<Command>,
    next_id: u64,
    operations: [usize; 4],
    read_bytes: usize,
    write_bytes: usize,
    pub(crate) streams: usize,
    pub(crate) listeners: usize,
}
impl Shared {
    #[cfg(test)]
    pub(crate) fn vectored_eagain_for_test(&self) {
        let observer = lock(&self.vectored_eagain).take();
        if let Some(observer) = observer {
            let _ = observer.try_send(());
        }
    }
    pub(crate) fn wake(&self) {
        match rustix::io::write(&*self.wake, &1u64.to_ne_bytes()) {
            Ok(_) | Err(rustix::io::Errno::AGAIN) => {}
            Err(_) => {
                self.stopped.store(true, Ordering::Release);
            }
        }
    }
    pub(crate) fn pop(&self) -> Option<Command> {
        lock(&self.queue).commands.pop_front()
    }
    pub(crate) fn enqueue(&self, command: Command) {
        let mut command = Some(command);
        {
            let mut q = lock(&self.queue);
            if !self.stopped.load(Ordering::Acquire) {
                // Startup reserved space for all admitted operations plus one
                // destructor-close command per live socket. No growth occurs.
                debug_assert!(q.commands.len() < q.commands.capacity());
                q.commands
                    .push_back(command.take().expect("command was just present"));
            }
        }
        if let Some(command) = command {
            command.fail(NetworkError::DriverStopped);
        } else {
            self.wake();
        }
    }
}

#[derive(Clone, Copy)]
pub(crate) enum Kind {
    Read,
    Write,
    Control,
    Close,
}
struct Guard {
    owner: Arc<Owner>,
    kind: Kind,
    bytes: usize,
}
impl Drop for Guard {
    fn drop(&mut self) {
        let mut q = lock(&self.owner.shared.queue);
        q.operations[self.kind as usize] -= 1;
        match self.kind {
            Kind::Read => q.read_bytes -= self.bytes,
            Kind::Write => q.write_bytes -= self.bytes,
            _ => {}
        }
    }
}
impl Owner {
    fn reserve(self: &Arc<Self>, kind: Kind, bytes: usize) -> Result<Guard, NetworkError> {
        let config = self.shared.config;
        let (limit, resource) = match kind {
            Kind::Read => (config.max_read_operations, "read operations"),
            Kind::Write => (config.max_write_operations, "write operations"),
            Kind::Control => (config.max_control_operations, "control operations"),
            Kind::Close => (
                config.max_streams + config.max_listeners,
                "close operations",
            ),
        };
        let mut q = lock(&self.shared.queue);
        if self.shared.stopped.load(Ordering::Acquire) {
            return Err(NetworkError::DriverStopped);
        }
        if q.operations[kind as usize] >= limit {
            return Err(exhausted(resource, limit));
        }
        let (held, max, name) = match kind {
            Kind::Read => (
                q.read_bytes,
                config.max_outstanding_read_bytes,
                "outstanding read bytes",
            ),
            Kind::Write => (
                q.write_bytes,
                config.max_outstanding_write_bytes,
                "outstanding write bytes",
            ),
            _ => (0, 0, "control bytes"),
        };
        let next = held
            .checked_add(bytes)
            .filter(|n| *n <= max)
            .ok_or_else(|| exhausted(name, max))?;
        q.operations[kind as usize] += 1;
        match kind {
            Kind::Read => q.read_bytes = next,
            Kind::Write => q.write_bytes = next,
            _ => {}
        }
        Ok(Guard {
            owner: self.clone(),
            kind,
            bytes,
        })
    }
    pub(crate) fn resource(self: &Arc<Self>, kind: ResourceKind) -> Result<Resource, NetworkError> {
        let mut q = lock(&self.shared.queue);
        let (held, limit, name) = match kind {
            ResourceKind::Stream => (q.streams, self.shared.config.max_streams, "streams"),
            ResourceKind::Listener => (q.listeners, self.shared.config.max_listeners, "listeners"),
        };
        if held == limit {
            return Err(exhausted(name, limit));
        }
        let id = q.next_id;
        q.next_id = q
            .next_id
            .checked_add(1)
            .ok_or(NetworkError::IdentifierExhausted)?;
        match kind {
            ResourceKind::Stream => q.streams += 1,
            ResourceKind::Listener => q.listeners += 1,
        }
        Ok(Resource {
            shared: Arc::downgrade(&self.shared),
            kind,
            id,
        })
    }
    fn channel<T>(
        self: &Arc<Self>,
        kind: Kind,
        bytes: usize,
    ) -> Result<(Operation<T>, Response<T>), NetworkError> {
        Ok(SyncOperation::channel_with_guard(
            self.reserve(kind, bytes)?,
        ))
    }
}

pub(crate) struct Flags {
    metrics: OnceLock<Arc<kr_runtime_io::completion::CompletionMetrics>>,
    lifetime_guard: Mutex<Option<Arc<dyn Send + Sync>>>,
    pub(crate) closed: AtomicBool,
    pub(crate) write_closed: AtomicBool,
}
impl Flags {
    pub(crate) fn new() -> Arc<Self> {
        Arc::new(Self {
            closed: AtomicBool::new(false),
            write_closed: AtomicBool::new(false),
            lifetime_guard: Mutex::new(None),
            metrics: OnceLock::new(),
        })
    }
}

/// Exclusive listener handle. Dropping it requests an owned asynchronous close.
pub struct ReadinessListener {
    pub(crate) owner: Arc<Owner>,
    pub(crate) id: u64,
    pub(crate) flags: Arc<Flags>,
    pub(crate) address: SocketAddr,
}
impl std::fmt::Debug for ReadinessListener {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ReadinessListener")
            .field("id", &self.id)
            .field("address", &self.address)
            .finish()
    }
}
/// TCP stream handle. Every operation owns its buffers independently of this handle.
pub struct ReadinessStream {
    pub(crate) owner: Arc<Owner>,
    pub(crate) id: u64,
    pub(crate) flags: Arc<Flags>,
}
impl std::fmt::Debug for ReadinessStream {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ReadinessStream")
            .field("id", &self.id)
            .finish()
    }
}
impl Drop for ReadinessStream {
    fn drop(&mut self) {
        if !self.flags.closed.load(Ordering::Acquire) {
            self.owner.shared.enqueue(Command::Close {
                id: self.id,
                responder: None,
            });
        }
    }
}
impl Drop for ReadinessListener {
    fn drop(&mut self) {
        if !self.flags.closed.load(Ordering::Acquire) {
            self.owner.shared.enqueue(Command::Close {
                id: self.id,
                responder: None,
            });
        }
    }
}

impl NetworkProviderSubmit for ReadinessNet {
    type Address = SocketAddr;
    type Stream = ReadinessStream;
    type Listener = ReadinessListener;
    type ListenResponse = Operation<ReadinessListener>;
    type ConnectResponse = Operation<ReadinessStream>;
    fn submit_listen(&self, request: ListenRequest<SocketAddr>) -> Self::ListenResponse {
        if request.backlog == 0 || request.backlog > self.owner.shared.config.max_listener_backlog {
            return failed(NetworkError::InvalidRequest {
                reason: "listen backlog exceeds configured bound",
            });
        }
        let resource = match self.owner.resource(ResourceKind::Listener) {
            Ok(resource) => resource,
            Err(error) => return failed(error),
        };
        let (future, responder) = match self.owner.channel(Kind::Control, 0) {
            Ok(pair) => pair,
            Err(e) => return failed(e),
        };
        self.owner.shared.enqueue(Command::Listen {
            request,
            resource,
            owner: self.owner.clone(),
            responder,
        });
        future
    }
    fn submit_connect(&self, request: ConnectRequest<SocketAddr>) -> Self::ConnectResponse {
        if request.local.is_ipv4() != request.remote.is_ipv4() {
            return failed(NetworkError::InvalidRequest {
                reason: "local and remote address families differ",
            });
        }
        let Some(deadline) = Instant::now().checked_add(self.owner.shared.config.connect_timeout)
        else {
            return failed(NetworkError::InvalidRequest {
                reason: "connect deadline overflow",
            });
        };
        let resource = match self.owner.resource(ResourceKind::Stream) {
            Ok(resource) => resource,
            Err(error) => return failed(error),
        };
        let (future, responder) = match self.owner.channel(Kind::Control, 0) {
            Ok(pair) => pair,
            Err(e) => return failed(e),
        };
        self.owner.shared.enqueue(Command::Connect {
            request,
            deadline,
            resource,
            owner: self.owner.clone(),
            responder,
        });
        future
    }
}
impl NetworkListenerSubmit for ReadinessListener {
    type Address = SocketAddr;
    type Stream = ReadinessStream;
    type AcceptResponse = Operation<ReadinessStream>;
    type CloseResponse = Operation<()>;
    fn local_address(&self) -> SocketAddr {
        self.address
    }
    fn submit_accept(&self) -> Self::AcceptResponse {
        if self.flags.closed.load(Ordering::Acquire) {
            return failed(NetworkError::ListenerClosed);
        }
        let resource = match self.owner.resource(ResourceKind::Stream) {
            Ok(resource) => resource,
            Err(error) => return failed(error),
        };
        let (future, responder) = match self.owner.channel(Kind::Control, 0) {
            Ok(pair) => pair,
            Err(e) => return failed(e),
        };
        self.owner.shared.enqueue(Command::Accept {
            id: self.id,
            resource,
            owner: self.owner.clone(),
            responder,
        });
        future
    }
    fn submit_close(&self) -> Self::CloseResponse {
        close(&self.owner, self.id, &self.flags)
    }
}
impl ReadinessStream {
    /// Enables native completion timing for subsequent stream I/O. Call once,
    /// before submission, to include every operation in the measurement.
    pub fn attach_completion_metrics(
        &self,
        metrics: Arc<kr_runtime_io::completion::CompletionMetrics>,
    ) -> Result<(), NetworkError> {
        self.flags
            .metrics
            .set(metrics)
            .map_err(|_| NetworkError::InvalidRequest {
                reason: "completion metrics already attached",
            })
    }
    /// Attaches one passive allocation-budget obligation before stream I/O.
    /// Each operation pins the flags and obligation through terminal observation.
    pub fn attach_lifetime_guard(&self, guard: Arc<dyn Send + Sync>) -> Result<(), NetworkError> {
        let mut slot = lock(&self.flags.lifetime_guard);
        if slot.is_some() {
            return Err(NetworkError::InvalidRequest {
                reason: "stream already has a lifetime guard",
            });
        }
        *slot = Some(guard);
        Ok(())
    }
    fn failed_buffer<T>(&self, error: NetworkError, buffer: Vec<u8>) -> Operation<T> {
        let (future, responder) = SyncOperation::channel_with_guard(self.flags.clone());
        responder.complete(Err(CompletionError::not_applied(
            NetworkFailure::with_buffer(error, buffer, 0),
        )));
        future
    }
    fn channel<T>(
        &self,
        kind: Kind,
        bytes: usize,
    ) -> Result<(Operation<T>, Response<T>), NetworkError> {
        Ok(SyncOperation::channel_with_guard_and_metrics(
            (self.owner.reserve(kind, bytes)?, self.flags.clone()),
            self.flags.metrics.get().cloned(),
        ))
    }
}
impl ByteStreamSubmit for ReadinessStream {
    type ReadResponse = Operation<ReadResult>;
    type WriteResponse = Operation<WriteResult>;
    type ControlResponse = Operation<()>;
    fn submit_read(&self, request: ReadRequest) -> Self::ReadResponse {
        let length = match request.buffer.len().checked_add(request.max_bytes) {
            Some(len) => len.max(request.buffer.capacity()),
            None => {
                return self.failed_buffer(
                    NetworkError::InvalidRequest {
                        reason: "read length overflow",
                    },
                    request.buffer,
                );
            }
        };
        if length > self.owner.shared.config.max_operation_bytes {
            return self.failed_buffer(
                NetworkError::InvalidRequest {
                    reason: "read buffer exceeds max_operation_bytes",
                },
                request.buffer,
            );
        }
        if self.flags.closed.load(Ordering::Acquire) {
            return self.failed_buffer(NetworkError::ConnectionClosed, request.buffer);
        }
        let (future, responder) = match self.channel(Kind::Read, length) {
            Ok(pair) => pair,
            Err(e) => return self.failed_buffer(e, request.buffer),
        };
        self.owner.shared.enqueue(Command::Read {
            id: self.id,
            request,
            responder,
        });
        future
    }
    fn submit_write(&self, request: WriteRequest) -> Self::WriteResponse {
        if request.buffer.capacity() > self.owner.shared.config.max_operation_bytes {
            return self.failed_buffer(
                NetworkError::InvalidRequest {
                    reason: "write buffer exceeds max_operation_bytes",
                },
                request.buffer,
            );
        }
        if self.flags.closed.load(Ordering::Acquire) {
            return self.failed_buffer(NetworkError::ConnectionClosed, request.buffer);
        }
        if self.flags.write_closed.load(Ordering::Acquire) {
            return self.failed_buffer(NetworkError::WriteClosed, request.buffer);
        }
        let (future, responder) = match self.channel(Kind::Write, request.buffer.capacity()) {
            Ok(pair) => pair,
            Err(e) => return self.failed_buffer(e, request.buffer),
        };
        self.owner.shared.enqueue(Command::Write {
            id: self.id,
            request,
            responder,
        });
        future
    }
    fn submit_shutdown_write(&self) -> Self::ControlResponse {
        if self.flags.closed.load(Ordering::Acquire) {
            return failed(NetworkError::ConnectionClosed);
        }
        let (future, responder) = match self.channel(Kind::Control, 0) {
            Ok(pair) => pair,
            Err(e) => return failed(e),
        };
        self.flags.write_closed.store(true, Ordering::Release);
        self.owner.shared.enqueue(Command::Shutdown {
            id: self.id,
            responder,
        });
        future
    }
    fn submit_close(&self) -> Self::ControlResponse {
        close(&self.owner, self.id, &self.flags)
    }
}
impl ByteStreamVectoredSubmit for ReadinessStream {
    type WriteVectoredResponse =
        SyncOperation<CompletionResult<VectoredWriteResult, VectoredWriteFailure>>;
    fn max_segments(&self) -> usize {
        self.owner.shared.config.max_segments
    }
    fn submit_write_vectored(&self, request: VectoredWriteRequest) -> Self::WriteVectoredResponse {
        let admission = (|| {
            let size = request.validate(
                self.max_segments(),
                self.owner.shared.config.max_operation_bytes,
            )?;
            if self.flags.closed.load(Ordering::Acquire) {
                return Err(NetworkError::ConnectionClosed);
            }
            if self.flags.write_closed.load(Ordering::Acquire) {
                return Err(NetworkError::WriteClosed);
            }
            self.owner.reserve(Kind::Write, size.retained_bytes)
        })();
        let guard = match admission {
            Ok(guard) => guard,
            Err(error) => {
                let (future, responder) = SyncOperation::channel_with_guard(self.flags.clone());
                responder.complete(Err(CompletionError::not_applied(
                    VectoredWriteFailure::new(error, request.segments, 0),
                )));
                return future;
            }
        };
        let (future, responder) = SyncOperation::channel_with_guard_and_metrics(
            (guard, self.flags.clone()),
            self.flags.metrics.get().cloned(),
        );
        self.owner.shared.enqueue(Command::Vectored {
            id: self.id,
            request,
            responder,
        });
        future
    }
}
fn close(owner: &Arc<Owner>, id: u64, flags: &Arc<Flags>) -> Operation<()> {
    if flags.closed.load(Ordering::Acquire) {
        return SyncOperation::ready(Ok(()));
    }
    let (future, responder) = match owner.reserve(Kind::Close, 0) {
        Ok(guard) => SyncOperation::channel_with_guard_and_metrics(
            (guard, flags.clone()),
            flags.metrics.get().cloned(),
        ),
        Err(e) => return failed(e),
    };
    owner.shared.enqueue(Command::Close {
        id,
        responder: Some(responder),
    });
    future
}
pub(crate) fn failed<T>(error: NetworkError) -> Operation<T> {
    SyncOperation::ready(Err(CompletionError::not_applied(
        NetworkFailure::without_buffer(error),
    )))
}
pub(crate) fn backend(operation: NetworkOperationKind, error: rustix::io::Errno) -> NetworkError {
    match error {
        rustix::io::Errno::ADDRINUSE => NetworkError::AddressInUse,
        rustix::io::Errno::CONNREFUSED => NetworkError::ConnectionRefused,
        rustix::io::Errno::PIPE | rustix::io::Errno::CONNRESET | rustix::io::Errno::NOTCONN => {
            NetworkError::ConnectionClosed
        }
        _ => NetworkError::Backend {
            operation,
            raw_os_error: Some(error.raw_os_error()),
            message: error.to_string(),
        },
    }
}
pub(crate) fn exhausted(resource: &'static str, limit: usize) -> NetworkError {
    NetworkError::ResourceExhausted { resource, limit }
}
pub(crate) fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

#[cfg(test)]
#[path = "blocked_tests.rs"]
mod blocked_tests;
