//! Connected TCP byte streams backed by a bounded io_uring actor.

use std::fmt;
use std::io;
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{self, Receiver, RecvTimeoutError, SyncSender};
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use kr_runtime::{CompletionError, CompletionResult, contain_panic};
use kr_runtime_io::network::{
    ByteStreamSubmit, ConnectRequest, ListenRequest, NetworkError, NetworkFailure,
    NetworkListenerSubmit, NetworkOperationKind, NetworkProviderSubmit, ReadRequest, ReadResult,
    WriteRequest, WriteResult,
};

use crate::operation::{
    ActiveResponder, DriverStoppedCommand, FailStopOnPanic, Responder, TerminalCommand,
    UringOperation, operation, ready,
};
use crate::ring::{EncodedSocketAddr, OwnedTransferFailure, Ring};
use crate::support::{
    Rejection, ResourcePermit, ResourcePool, finish_actor_start, join_if_other_thread,
    lock_unpoisoned, try_send_command,
};

type ReadCompletion = CompletionResult<ReadResult, NetworkFailure>;
type WriteCompletion = CompletionResult<WriteResult, NetworkFailure>;
type ControlCompletion = CompletionResult<(), NetworkFailure>;
type ListenCompletion = CompletionResult<UringListener, NetworkFailure>;
type ConnectCompletion = CompletionResult<UringByteStream, NetworkFailure>;

const ACCEPT_RETRY_PAUSE: Duration = Duration::from_millis(1);

/// Open descriptors one live connected stream holds: its TCP socket, the
/// io_uring backing its private reactor, and that reactor's eventfd.
///
/// Published because it is the term that makes
/// [`UringNetworkProviderConfig::max_streams`] chargeable against
/// `RLIMIT_NOFILE`: a stream ceiling is only reachable if the process may open
/// this many descriptors per stream.
pub const DESCRIPTORS_PER_STREAM: usize = 3;

/// Fixed resource limits for one connected TCP stream actor.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct UringNetworkConfig {
    /// Commands admitted behind the active operation in one directional actor.
    pub command_queue_capacity: usize,
    /// Maximum user SQEs kept in flight by each network reactor.
    /// Command notification uses a separate internal poll operation.
    pub ring_entries: u32,
    /// Maximum caller-owned buffer accepted by one operation.
    pub max_operation_bytes: usize,
    /// Maximum bytes requested by one SQE and therefore one completion.
    pub max_io_chunk_bytes: usize,
    /// Maximum wall-clock time allowed for TCP connection establishment.
    ///
    /// The kernel Connect request is linked to this timeout, so a blackholed
    /// peer cannot hold a provider actor or its destructor indefinitely.
    pub connect_timeout: Duration,
}

impl Default for UringNetworkConfig {
    fn default() -> Self {
        Self {
            command_queue_capacity: 64,
            ring_entries: 8,
            max_operation_bytes: 256 * 1024,
            max_io_chunk_bytes: 64 * 1024,
            connect_timeout: Duration::from_secs(10),
        }
    }
}

/// Fixed resource limits for an io_uring TCP provider.
///
/// `stream.command_queue_capacity` also bounds the provider control actor and
/// each listener accept actor. `stream.ring_entries` bounds the reactor shared
/// by provider control and listeners, plus the per-stream reactor shared by its
/// read and write actors. `max_streams` counts connected stream handles, not TCP
/// connection pairs: a client and its accepted server endpoint consume two
/// permits while both are live.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct UringNetworkProviderConfig {
    /// Per-connected-stream limits and shared control-actor sizing.
    pub stream: UringNetworkConfig,
    /// Maximum live bound listeners. A completed listener close releases its
    /// permit even while the closed handle remains live.
    pub max_listeners: usize,
    /// Maximum live connected stream handles.
    ///
    /// Each live stream owns a private reactor, so it costs
    /// [`DESCRIPTORS_PER_STREAM`] descriptors — TCP socket, io_uring, eventfd —
    /// and three threads: the reactor plus its read and write actors. That
    /// isolation is what lets a blocked read on one stream leave every other
    /// stream's queue depth untouched, but it makes the ceiling expensive, and
    /// a value far above what `RLIMIT_NOFILE` allows is not a ceiling so much
    /// as a promise the host cannot keep. Raise it deliberately, alongside the
    /// descriptor and thread limits it implies.
    pub max_streams: usize,
    /// Maximum backlog accepted by one listen request.
    pub max_listener_backlog: usize,
}

impl Default for UringNetworkProviderConfig {
    fn default() -> Self {
        Self {
            stream: UringNetworkConfig::default(),
            max_listeners: 1_024,
            // 256 streams need 768 descriptors, which fits the common 1,024
            // soft `RLIMIT_NOFILE` with room for listeners and the provider's
            // own reactor. The previous 4,096 needed 12,288 and could not be
            // reached on such a host: the provider would admit permits it had
            // no descriptors to honor, and accepts would fail with EMFILE well
            // before the advertised bound.
            max_streams: 256,
            max_listener_backlog: 1_024,
        }
    }
}

/// Failure to create an io_uring network provider, listener, or stream actor.
#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum UringNetworkOpenError {
    /// A fixed resource bound is invalid.
    InvalidConfig {
        field: &'static str,
        message: String,
    },
    /// TCP setup or io_uring construction failed.
    Io {
        action: &'static str,
        raw_os_error: Option<i32>,
        message: String,
    },
    /// The actor stopped before its readiness handshake.
    DriverStopped,
}

impl fmt::Display for UringNetworkOpenError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidConfig { field, message } => {
                write!(formatter, "invalid io_uring network {field}: {message}")
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
            Self::DriverStopped => formatter.write_str("io_uring network driver stopped"),
        }
    }
}

impl std::error::Error for UringNetworkOpenError {}

enum ReadCommand {
    Read {
        request: ReadRequest,
        response: Responder<ReadCompletion>,
    },
    #[cfg(test)]
    Panic {
        entered: SyncSender<()>,
        release: Receiver<()>,
    },
}

enum WriteCommand {
    Write {
        request: WriteRequest,
        response: Responder<WriteCompletion>,
    },
    ShutdownWrite {
        response: Responder<ControlCompletion>,
    },
    #[cfg(test)]
    Panic {
        entered: SyncSender<()>,
        release: Receiver<()>,
    },
}

enum ProviderCommand {
    Listen {
        request: ListenRequest<SocketAddr>,
        listener_permit: ResourcePermit,
        response: Responder<ListenCompletion>,
    },
    Connect {
        request: ConnectRequest<SocketAddr>,
        stream_permit: ResourcePermit,
        response: Responder<ConnectCompletion>,
    },
    #[cfg(test)]
    Panic {
        entered: SyncSender<()>,
        release: Receiver<()>,
    },
}

enum ListenerCommand {
    Accept {
        stream_permit: ResourcePermit,
        response: Responder<ConnectCompletion>,
    },
    #[cfg(test)]
    Panic {
        entered: SyncSender<()>,
        release: Receiver<()>,
    },
}

type ReadActorMessage = TerminalCommand<ReadCommand>;
type WriteActorMessage = TerminalCommand<WriteCommand>;
type ProviderActorMessage = TerminalCommand<ProviderCommand>;
type ListenerActorMessage = TerminalCommand<ListenerCommand>;

impl DriverStoppedCommand for ReadCommand {
    fn complete_driver_stopped(self) {
        match self {
            Self::Read { request, response } => {
                response.complete(read_error(NetworkError::DriverStopped, request.buffer));
            }
            #[cfg(test)]
            Self::Panic { .. } => {}
        }
    }
}

impl DriverStoppedCommand for WriteCommand {
    fn complete_driver_stopped(self) {
        match self {
            Self::Write { request, response } => {
                response.complete(write_error(NetworkError::DriverStopped, request.buffer));
            }
            Self::ShutdownWrite { response } => {
                response.complete(control_error(NetworkError::DriverStopped));
            }
            #[cfg(test)]
            Self::Panic { .. } => {}
        }
    }
}

impl DriverStoppedCommand for ProviderCommand {
    fn complete_driver_stopped(self) {
        match self {
            Self::Listen { response, .. } => {
                response.complete(no_buffer_error(NetworkError::DriverStopped));
            }
            Self::Connect { response, .. } => {
                response.complete(no_buffer_error(NetworkError::DriverStopped));
            }
            #[cfg(test)]
            Self::Panic { .. } => {}
        }
    }
}

impl DriverStoppedCommand for ListenerCommand {
    fn complete_driver_stopped(self) {
        match self {
            Self::Accept { response, .. } => {
                response.complete(no_buffer_error(NetworkError::DriverStopped));
            }
            #[cfg(test)]
            Self::Panic { .. } => {}
        }
    }
}

/// A bounded TCP control-plane provider driven by io_uring connect and accept.
///
/// Calls reserve queue and live-resource capacity synchronously. Commands that
/// are admitted execute in FIFO order even when their response future is
/// abandoned. Socket creation, bind, and listen are ordinary Linux lifecycle
/// calls; connection establishment and acceptance use io_uring CQEs.
pub struct UringNetwork {
    sender: Option<SyncSender<ProviderActorMessage>>,
    join: Option<JoinHandle<()>>,
    listener_pool: Arc<ResourcePool>,
    stream_pool: Arc<ResourcePool>,
    config: UringNetworkProviderConfig,
}

impl UringNetwork {
    /// Starts the provider control actor with fixed queue, listener, stream,
    /// backlog, and ring bounds.
    ///
    /// # Errors
    ///
    /// Returns [`UringNetworkOpenError::InvalidConfig`] when a resource bound
    /// is invalid, [`UringNetworkOpenError::Io`] when io_uring or actor-thread
    /// construction fails, or [`UringNetworkOpenError::DriverStopped`] when
    /// the control actor stops before its readiness handshake.
    pub fn new(config: UringNetworkProviderConfig) -> Result<Self, UringNetworkOpenError> {
        validate_provider_config(config)?;
        let ring = Ring::for_network_provider(
            config.stream.ring_entries,
            config.stream.max_io_chunk_bytes,
        )
        .map_err(|error| open_io("create network reactor", error))?;
        let listener_pool = Arc::new(ResourcePool::new(config.max_listeners));
        let stream_pool = Arc::new(ResourcePool::new(config.max_streams));
        let (sender, receiver) = mpsc::sync_channel(config.stream.command_queue_capacity);
        let (ready_sender, ready_receiver) = mpsc::sync_channel(1);
        let actor_stream_pool = Arc::clone(&stream_pool);
        let join = thread::Builder::new()
            .name("kr-runtime-io-uring-net-control".to_owned())
            .spawn(move || {
                if ready_sender.send(Ok(())).is_ok() {
                    ProviderActor {
                        ring,
                        config,
                        stream_pool: actor_stream_pool,
                    }
                    .run(receiver);
                }
            })
            .map_err(|error| open_io("spawn network control actor", error))?;
        let (sender, join) = finish_actor_start(
            sender,
            join,
            ready_receiver,
            UringNetworkOpenError::DriverStopped,
        )?;
        Ok(Self {
            sender: Some(sender),
            join: Some(join),
            listener_pool,
            stream_pool,
            config,
        })
    }

    fn try_send(&self, command: ProviderCommand) -> Result<(), (ProviderCommand, Rejection)> {
        try_send_command(self.sender.as_ref(), command)
    }
}

impl NetworkProviderSubmit for UringNetwork {
    type Address = SocketAddr;
    type Stream = UringByteStream;
    type Listener = UringListener;
    type ListenResponse = UringOperation<ListenCompletion>;
    type ConnectResponse = UringOperation<ConnectCompletion>;

    fn submit_listen(&self, request: ListenRequest<SocketAddr>) -> Self::ListenResponse {
        if request.backlog == 0 || request.backlog > self.config.max_listener_backlog {
            return ready(no_buffer_error(NetworkError::InvalidRequest {
                reason: "listener backlog is outside the configured bound",
            }));
        }
        let Some(listener_permit) = self.listener_pool.acquire() else {
            return ready(no_buffer_error(NetworkError::ResourceExhausted {
                resource: "io_uring listeners",
                limit: self.config.max_listeners,
            }));
        };
        let (future, response) = operation();
        match self.try_send(ProviderCommand::Listen {
            request,
            listener_permit,
            response,
        }) {
            Ok(()) => future,
            Err((ProviderCommand::Listen { response, .. }, rejection)) => {
                response.complete(no_buffer_error(provider_rejection_error(
                    rejection,
                    self.config.stream.command_queue_capacity,
                )));
                future
            }
            Err(_) => unreachable!("try_send changed a provider command variant"),
        }
    }

    fn submit_connect(&self, request: ConnectRequest<SocketAddr>) -> Self::ConnectResponse {
        let Some(stream_permit) = self.stream_pool.acquire() else {
            return ready(no_buffer_error(NetworkError::ResourceExhausted {
                resource: "io_uring connected streams",
                limit: self.config.max_streams,
            }));
        };
        let (future, response) = operation();
        match self.try_send(ProviderCommand::Connect {
            request,
            stream_permit,
            response,
        }) {
            Ok(()) => future,
            Err((ProviderCommand::Connect { response, .. }, rejection)) => {
                response.complete(no_buffer_error(provider_rejection_error(
                    rejection,
                    self.config.stream.command_queue_capacity,
                )));
                future
            }
            Err(_) => unreachable!("try_send changed a provider command variant"),
        }
    }
}

impl Drop for UringNetwork {
    fn drop(&mut self) {
        self.sender.take();
        join_if_other_thread(self.join.take());
    }
}

/// One exclusively bound TCP listener with a bounded io_uring accept actor.
pub struct UringListener {
    sender: Option<SyncSender<ListenerActorMessage>>,
    join: Option<JoinHandle<()>>,
    control: Arc<ListenerControl>,
    stream_pool: Arc<ResourcePool>,
    address: SocketAddr,
    config: UringNetworkProviderConfig,
}

impl UringListener {
    fn start(
        listener: TcpListener,
        address: SocketAddr,
        ring: Ring,
        config: UringNetworkProviderConfig,
        listener_permit: ResourcePermit,
        stream_pool: Arc<ResourcePool>,
    ) -> Result<Self, UringNetworkOpenError> {
        let (sender, receiver) = mpsc::sync_channel(config.stream.command_queue_capacity);
        let (ready_sender, ready_receiver) = mpsc::sync_channel(1);
        let control = Arc::new(ListenerControl::new(config.stream.command_queue_capacity));
        let actor_control = Arc::clone(&control);
        let join = thread::Builder::new()
            .name("kr-runtime-io-uring-net-accept".to_owned())
            .spawn(move || {
                actor_control.set_thread(thread::current());
                if ready_sender.send(Ok(())).is_ok() {
                    ListenerActor {
                        ring,
                        listener,
                        config,
                        control: Arc::clone(&actor_control),
                        _listener_permit: listener_permit,
                    }
                    .run(receiver);
                }
            })
            .map_err(|error| open_io("spawn listener accept actor", error))?;
        let (sender, join) = finish_actor_start(
            sender,
            join,
            ready_receiver,
            UringNetworkOpenError::DriverStopped,
        )?;
        Ok(Self {
            sender: Some(sender),
            join: Some(join),
            control,
            stream_pool,
            address,
            config,
        })
    }

    fn try_send(
        &self,
        command: ListenerCommand,
    ) -> Result<(), (ListenerCommand, ListenerRejection)> {
        self.control.try_send_accept(self.sender.as_ref(), command)
    }
}

impl NetworkListenerSubmit for UringListener {
    type Address = SocketAddr;
    type Stream = UringByteStream;
    type AcceptResponse = UringOperation<ConnectCompletion>;
    type CloseResponse = UringOperation<ControlCompletion>;

    fn local_address(&self) -> SocketAddr {
        self.address
    }

    fn submit_accept(&self) -> Self::AcceptResponse {
        if let Some(error) = self.control.admission_error() {
            return ready(no_buffer_error(error));
        }
        let Some(stream_permit) = self.stream_pool.acquire() else {
            return ready(no_buffer_error(NetworkError::ResourceExhausted {
                resource: "io_uring connected streams",
                limit: self.config.max_streams,
            }));
        };
        let (future, response) = operation();
        match self.try_send(ListenerCommand::Accept {
            stream_permit,
            response,
        }) {
            Ok(()) => future,
            Err((ListenerCommand::Accept { response, .. }, rejection)) => {
                let error = match rejection {
                    ListenerRejection::Closing => NetworkError::ListenerClosed,
                    ListenerRejection::Full => NetworkError::ResourceExhausted {
                        resource: "io_uring listener accept command queue",
                        limit: self.config.stream.command_queue_capacity,
                    },
                    ListenerRejection::Stopped => NetworkError::DriverStopped,
                };
                response.complete(no_buffer_error(error));
                future
            }
            #[cfg(test)]
            Err(_) => unreachable!("try_send changed a listener command variant"),
        }
    }

    fn submit_close(&self) -> Self::CloseResponse {
        self.control.submit_close()
    }
}

impl Drop for UringListener {
    fn drop(&mut self) {
        self.control.request_close_without_response();
        self.sender.take();
        self.control.wake_actor();
        join_if_other_thread(self.join.take());
    }
}

struct ProviderActor {
    ring: Ring,
    config: UringNetworkProviderConfig,
    stream_pool: Arc<ResourcePool>,
}

impl ProviderActor {
    fn run(&mut self, receiver: Receiver<ProviderActorMessage>) {
        while let Ok(mut command) = receiver.recv() {
            let command = command.take();
            match command {
                ProviderCommand::Listen {
                    request,
                    listener_permit,
                    response,
                } => {
                    let active = ActiveResponder::new(response, active_network_driver_stopped);
                    active.complete(self.listen(request, listener_permit));
                }
                ProviderCommand::Connect {
                    request,
                    stream_permit,
                    response,
                } => {
                    let output = {
                        let _fail_stop = FailStopOnPanic;
                        self.connect(request, stream_permit)
                    };
                    response.complete(output);
                }
                #[cfg(test)]
                ProviderCommand::Panic { entered, release } => {
                    let _ = entered.send(());
                    let _ = release.recv();
                    panic!("injected provider actor panic");
                }
            }
        }
    }

    fn listen(
        &mut self,
        request: ListenRequest<SocketAddr>,
        listener_permit: ResourcePermit,
    ) -> ListenCompletion {
        let listener = match bind_tcp_listener(request.address, request.backlog) {
            Ok(listener) => listener,
            Err(error) => return Err(not_applied_network(map_listen_error(error))),
        };
        let address = match listener.local_addr() {
            Ok(address) => address,
            Err(error) => {
                return Err(CompletionError::may_have_applied(
                    NetworkFailure::without_buffer(backend(
                        NetworkOperationKind::Listen,
                        error.raw_os_error(),
                        error.to_string(),
                    )),
                ));
            }
        };
        match UringListener::start(
            listener,
            address,
            self.ring.clone(),
            self.config,
            listener_permit,
            Arc::clone(&self.stream_pool),
        ) {
            Ok(listener) => Ok(listener),
            Err(error) => Err(CompletionError::may_have_applied(
                NetworkFailure::without_buffer(open_error(NetworkOperationKind::Listen, error)),
            )),
        }
    }

    fn connect(
        &mut self,
        request: ConnectRequest<SocketAddr>,
        stream_permit: ResourcePermit,
    ) -> ConnectCompletion {
        let stream = match create_bound_tcp_stream(request.local) {
            Ok(stream) => stream,
            Err(error) => return Err(not_applied_network(map_connect_error(error))),
        };
        if let Err(failure) =
            self.ring
                .connect(&stream, request.remote, self.config.stream.connect_timeout)
        {
            let (error, may_have_applied) = failure.into_parts();
            let mapped = map_connect_error(error);
            let output = NetworkFailure::without_buffer(mapped);
            return Err(if may_have_applied {
                CompletionError::may_have_applied(output)
            } else {
                CompletionError::not_applied(output)
            });
        }
        UringByteStream::from_tcp_stream_with_permit(
            stream,
            self.config.stream,
            Some(stream_permit),
        )
        .map_err(|error| {
            CompletionError::applied(NetworkFailure::without_buffer(open_error(
                NetworkOperationKind::Connect,
                error,
            )))
        })
    }
}

struct ListenerActor {
    ring: Ring,
    listener: TcpListener,
    config: UringNetworkProviderConfig,
    control: Arc<ListenerControl>,
    _listener_permit: ResourcePermit,
}

impl ListenerActor {
    fn run(mut self, receiver: Receiver<ListenerActorMessage>) {
        let exit = ListenerExitGuard::new(Arc::clone(&self.control));
        while !self.control.is_closing() {
            match receiver.recv_timeout(ACCEPT_RETRY_PAUSE) {
                Ok(mut command) => match command.take() {
                    ListenerCommand::Accept {
                        stream_permit,
                        response,
                    } => {
                        let output = {
                            let _fail_stop = FailStopOnPanic;
                            self.accept(stream_permit)
                        };
                        response.complete(output);
                    }
                    #[cfg(test)]
                    ListenerCommand::Panic { entered, release } => {
                        let _ = entered.send(());
                        let _ = release.recv();
                        panic!("injected listener actor panic");
                    }
                },
                Err(RecvTimeoutError::Timeout) => {}
                Err(RecvTimeoutError::Disconnected) => break,
            }
        }
        while let Ok(mut command) = receiver.try_recv() {
            match command.take() {
                ListenerCommand::Accept { response, .. } => {
                    response.complete(no_buffer_error(NetworkError::ListenerClosed));
                }
                #[cfg(test)]
                ListenerCommand::Panic { .. } => {}
            }
        }
        drop(self);
        exit.finish_closed();
    }

    fn accept(&mut self, stream_permit: ResourcePermit) -> ConnectCompletion {
        loop {
            if self.control.is_closing() {
                return Err(not_applied_network(NetworkError::ListenerClosed));
            }
            match self.ring.accept(&self.listener) {
                Ok((stream, _peer)) => {
                    if self.control.is_closing() {
                        drop(stream);
                        return Err(accepted_then_closed());
                    }
                    return UringByteStream::from_tcp_stream_with_permit(
                        stream,
                        self.config.stream,
                        Some(stream_permit),
                    )
                    .map_err(|error| {
                        CompletionError::applied(NetworkFailure::without_buffer(open_error(
                            NetworkOperationKind::Accept,
                            error,
                        )))
                    });
                }
                Err(error) if is_retryable_accept_error(&error) => {
                    thread::park_timeout(ACCEPT_RETRY_PAUSE);
                }
                Err(_error) if self.control.is_closing() => {
                    return Err(not_applied_network(NetworkError::ListenerClosed));
                }
                Err(error) => {
                    return Err(CompletionError::may_have_applied(
                        NetworkFailure::without_buffer(backend(
                            NetworkOperationKind::Accept,
                            error.raw_os_error(),
                            error.to_string(),
                        )),
                    ));
                }
            }
        }
    }
}

pub(crate) struct ListenerControl {
    state: Mutex<ListenerCloseState>,
    actor_thread: Mutex<Option<thread::Thread>>,
    waiter_capacity: usize,
}

struct ListenerCloseState {
    requested: bool,
    terminal: Option<ListenerTerminal>,
    waiters: Vec<Responder<ControlCompletion>>,
}

#[derive(Clone, Copy)]
pub(crate) enum ListenerTerminal {
    Closed,
    DriverStopped,
}

impl ListenerControl {
    pub(crate) fn new(waiter_capacity: usize) -> Self {
        Self {
            state: Mutex::new(ListenerCloseState {
                requested: false,
                terminal: None,
                waiters: Vec::new(),
            }),
            actor_thread: Mutex::new(None),
            waiter_capacity,
        }
    }

    fn set_thread(&self, actor_thread: thread::Thread) {
        *lock_unpoisoned(&self.actor_thread) = Some(actor_thread);
    }

    pub(crate) fn is_closing(&self) -> bool {
        let state = lock_unpoisoned(&self.state);
        state.requested || state.terminal.is_some()
    }

    pub(crate) fn admission_error(&self) -> Option<NetworkError> {
        let state = lock_unpoisoned(&self.state);
        match state.terminal {
            Some(ListenerTerminal::DriverStopped) => Some(NetworkError::DriverStopped),
            Some(ListenerTerminal::Closed) => Some(NetworkError::ListenerClosed),
            None if state.requested => Some(NetworkError::ListenerClosed),
            None => None,
        }
    }

    /// Admits an accept under the same lock that closes the admission gate.
    /// Once `submit_close` sets `requested`, no command can race behind the
    /// actor's final queue drain and lose its terminal response.
    fn try_send_accept(
        &self,
        sender: Option<&SyncSender<ListenerActorMessage>>,
        command: ListenerCommand,
    ) -> Result<(), (ListenerCommand, ListenerRejection)> {
        let state = lock_unpoisoned(&self.state);
        if matches!(state.terminal, Some(ListenerTerminal::DriverStopped)) {
            return Err((command, ListenerRejection::Stopped));
        }
        if state.requested || state.terminal.is_some() {
            return Err((command, ListenerRejection::Closing));
        }
        try_send_command(sender, command)
            .map_err(|(command, rejection)| (command, rejection.into()))
    }

    pub(crate) fn submit_close(&self) -> UringOperation<ControlCompletion> {
        let (future, response) = operation();
        let mut response = Some(response);
        let immediate = {
            let mut state = lock_unpoisoned(&self.state);
            if let Some(terminal) = state.terminal {
                Some(listener_terminal_completion(terminal))
            } else if state.waiters.len() >= self.waiter_capacity {
                Some(no_buffer_error(NetworkError::ResourceExhausted {
                    resource: "io_uring listener close waiters",
                    limit: self.waiter_capacity,
                }))
            } else {
                state.requested = true;
                state
                    .waiters
                    .push(response.take().expect("close response is available"));
                None
            }
        };
        if let Some(output) = immediate {
            response
                .take()
                .expect("immediate close retained its response")
                .complete(output);
        }
        self.wake_actor();
        future
    }

    pub(crate) fn request_close_without_response(&self) {
        let mut state = lock_unpoisoned(&self.state);
        if state.terminal.is_none() {
            state.requested = true;
        }
    }

    pub(crate) fn finish(&self, terminal: ListenerTerminal) {
        let waiters = {
            let mut state = lock_unpoisoned(&self.state);
            state.requested = true;
            state.terminal = Some(terminal);
            std::mem::take(&mut state.waiters)
        };
        for waiter in waiters {
            contain_panic(|| {
                waiter.complete(listener_terminal_completion(terminal));
            });
        }
    }

    fn wake_actor(&self) {
        if let Some(actor_thread) = lock_unpoisoned(&self.actor_thread).as_ref() {
            actor_thread.unpark();
        }
    }
}

pub(crate) fn listener_terminal_completion(terminal: ListenerTerminal) -> ControlCompletion {
    match terminal {
        ListenerTerminal::Closed => Ok(()),
        ListenerTerminal::DriverStopped => control_error(NetworkError::DriverStopped),
    }
}

struct ListenerExitGuard {
    control: Arc<ListenerControl>,
    finished: bool,
}

impl ListenerExitGuard {
    fn new(control: Arc<ListenerControl>) -> Self {
        Self {
            control,
            finished: false,
        }
    }

    fn finish_closed(mut self) {
        self.control.finish(ListenerTerminal::Closed);
        self.finished = true;
    }
}

impl Drop for ListenerExitGuard {
    fn drop(&mut self) {
        if !self.finished {
            self.control.finish(ListenerTerminal::DriverStopped);
        }
    }
}

/// An owned, connected TCP byte stream driven by Linux io_uring.
///
/// Calls admit commands immediately. Independent bounded read and write/control
/// actors preserve FIFO order within each direction without letting a pending
/// receive block local writes. The returned futures do not borrow this handle,
/// and abandoning one does not remove its command from actor order. Full close
/// is an out-of-band control operation: it shuts down the shared socket
/// synchronously so it can interrupt an active send or receive, after which the
/// actors return every owned buffer from the corresponding terminal CQE. The
/// first close serializes that shutdown and caches its completion so concurrent
/// and repeated callers observe the same terminal result.
pub struct UringByteStream {
    read_sender: Option<SyncSender<ReadActorMessage>>,
    read_join: Option<JoinHandle<()>>,
    write_sender: Option<SyncSender<WriteActorMessage>>,
    write_join: Option<JoinHandle<()>>,
    close: StreamClose,
    closed: Arc<AtomicBool>,
    config: UringNetworkConfig,
    _stream_permit: Option<ResourcePermit>,
}

impl UringByteStream {
    /// Takes ownership of an already-connected TCP stream.
    ///
    /// # Errors
    ///
    /// Returns [`UringNetworkOpenError::InvalidConfig`] when a config bound is
    /// invalid, or [`UringNetworkOpenError::Io`] when creating the stream
    /// reactor, cloning the TCP stream, or starting the read/write actors
    /// fails. The stream is dropped (closed) on failure.
    pub fn from_tcp_stream(
        stream: TcpStream,
        config: UringNetworkConfig,
    ) -> Result<Self, UringNetworkOpenError> {
        Self::from_tcp_stream_with_permit(stream, config, None)
    }

    fn from_tcp_stream_with_permit(
        stream: TcpStream,
        config: UringNetworkConfig,
        stream_permit: Option<ResourcePermit>,
    ) -> Result<Self, UringNetworkOpenError> {
        validate_config(config)?;
        // One reactor is shared by this stream's read and write actors. A
        // blocked receive therefore cannot consume the queue depth needed by
        // unrelated streams or by provider control operations.
        let ring = Ring::for_network(config.ring_entries, config.max_io_chunk_bytes)
            .map_err(|error| open_io("create stream reactor", error))?;
        let write_stream = stream
            .try_clone()
            .map_err(|error| open_io("clone TCP stream for write actor", error))?;
        let interrupt = stream
            .try_clone()
            .map_err(|error| open_io("clone TCP stream for close interruption", error))?;
        let closed = Arc::new(AtomicBool::new(false));
        let (read_sender, read_join) =
            start_read_actor(stream, ring.clone(), config, Arc::clone(&closed))?;
        let (write_sender, write_join) =
            match start_write_actor(write_stream, ring, config, Arc::clone(&closed)) {
                Ok(actor) => actor,
                Err(error) => {
                    let _ = interrupt.shutdown(std::net::Shutdown::Both);
                    drop(read_sender);
                    let _ = read_join.join();
                    return Err(error);
                }
            };
        Ok(Self {
            read_sender: Some(read_sender),
            read_join: Some(read_join),
            write_sender: Some(write_sender),
            write_join: Some(write_join),
            close: StreamClose::new(interrupt),
            closed,
            config,
            _stream_permit: stream_permit,
        })
    }

    fn try_send_read(&self, command: ReadCommand) -> Result<(), (ReadCommand, Rejection)> {
        try_send_command(self.read_sender.as_ref(), command)
    }

    fn try_send_write(&self, command: WriteCommand) -> Result<(), (WriteCommand, Rejection)> {
        try_send_command(self.write_sender.as_ref(), command)
    }

    fn close_actors(&mut self) {
        let _ = self.close.close(&self.closed);
        self.read_sender.take();
        self.write_sender.take();
        join_if_other_thread(self.read_join.take());
        join_if_other_thread(self.write_join.take());
    }
}

fn start_read_actor(
    stream: TcpStream,
    ring: Ring,
    config: UringNetworkConfig,
    closed: Arc<AtomicBool>,
) -> Result<(SyncSender<ReadActorMessage>, JoinHandle<()>), UringNetworkOpenError> {
    let (sender, receiver) = mpsc::sync_channel(config.command_queue_capacity);
    let (ready_sender, ready_receiver) = mpsc::sync_channel(1);
    let join = thread::Builder::new()
        .name("kr-runtime-io-uring-net-read".to_owned())
        .spawn(move || {
            if ready_sender.send(Ok(())).is_ok() {
                ReadActor::new(ring, stream, config, closed).run(receiver);
            }
        })
        .map_err(|error| open_io("spawn network read actor", error))?;
    finish_actor_start(
        sender,
        join,
        ready_receiver,
        UringNetworkOpenError::DriverStopped,
    )
}

fn start_write_actor(
    stream: TcpStream,
    ring: Ring,
    config: UringNetworkConfig,
    closed: Arc<AtomicBool>,
) -> Result<(SyncSender<WriteActorMessage>, JoinHandle<()>), UringNetworkOpenError> {
    let (sender, receiver) = mpsc::sync_channel(config.command_queue_capacity);
    let (ready_sender, ready_receiver) = mpsc::sync_channel(1);
    let join = thread::Builder::new()
        .name("kr-runtime-io-uring-net-write".to_owned())
        .spawn(move || {
            if ready_sender.send(Ok(())).is_ok() {
                WriteActor::new(ring, stream, closed).run(receiver);
            }
        })
        .map_err(|error| open_io("spawn network write actor", error))?;
    finish_actor_start(
        sender,
        join,
        ready_receiver,
        UringNetworkOpenError::DriverStopped,
    )
}

impl Drop for UringByteStream {
    fn drop(&mut self) {
        self.close_actors();
    }
}

impl ByteStreamSubmit for UringByteStream {
    type ReadResponse = UringOperation<ReadCompletion>;
    type WriteResponse = UringOperation<WriteCompletion>;
    type ControlResponse = UringOperation<ControlCompletion>;

    fn submit_read(&self, request: ReadRequest) -> Self::ReadResponse {
        if read_request_exceeds_bound(&request, self.config.max_operation_bytes) {
            return ready(read_error(
                NetworkError::InvalidRequest {
                    reason: "read request exceeds max_operation_bytes",
                },
                request.buffer,
            ));
        }
        let (future, response) = operation();
        match self.try_send_read(ReadCommand::Read { request, response }) {
            Ok(()) => future,
            Err((ReadCommand::Read { request, response }, rejection)) => {
                response.complete(read_error(
                    stream_rejection_error(rejection, self.config),
                    request.buffer,
                ));
                future
            }
            #[cfg(test)]
            Err(_) => unreachable!("try_send changed a read command variant"),
        }
    }

    fn submit_write(&self, request: WriteRequest) -> Self::WriteResponse {
        if write_request_exceeds_bound(&request, self.config.max_operation_bytes) {
            return ready(write_error(
                NetworkError::InvalidRequest {
                    reason: "write request exceeds max_operation_bytes",
                },
                request.buffer,
            ));
        }
        let (future, response) = operation();
        match self.try_send_write(WriteCommand::Write { request, response }) {
            Ok(()) => future,
            Err((WriteCommand::Write { request, response }, rejection)) => {
                response.complete(write_error(
                    stream_rejection_error(rejection, self.config),
                    request.buffer,
                ));
                future
            }
            Err(_) => unreachable!("try_send changed a write command variant"),
        }
    }

    fn submit_shutdown_write(&self) -> Self::ControlResponse {
        let (future, response) = operation();
        match self.try_send_write(WriteCommand::ShutdownWrite { response }) {
            Ok(()) => future,
            Err((WriteCommand::ShutdownWrite { response }, rejection)) => {
                response.complete(control_error(stream_rejection_error(
                    rejection,
                    self.config,
                )));
                future
            }
            Err(_) => unreachable!("try_send changed a shutdown command variant"),
        }
    }

    fn submit_close(&self) -> Self::ControlResponse {
        ready(self.close.close(&self.closed))
    }
}

struct StreamClose {
    state: Mutex<StreamCloseState>,
}

enum StreamCloseState {
    Open(TcpStream),
    Closed(ControlCompletion),
}

impl StreamClose {
    fn new(interrupt: TcpStream) -> Self {
        Self {
            state: Mutex::new(StreamCloseState::Open(interrupt)),
        }
    }

    fn close(&self, closed: &AtomicBool) -> ControlCompletion {
        self.close_with(closed, |interrupt| {
            match interrupt.shutdown(std::net::Shutdown::Both) {
                Ok(()) => Ok(()),
                Err(error) if error.kind() == io::ErrorKind::NotConnected => Ok(()),
                Err(error) => Err(CompletionError::may_have_applied(
                    NetworkFailure::without_buffer(backend(
                        NetworkOperationKind::Close,
                        error.raw_os_error(),
                        error.to_string(),
                    )),
                )),
            }
        })
    }

    fn close_with(
        &self,
        closed: &AtomicBool,
        shutdown: impl FnOnce(&TcpStream) -> ControlCompletion,
    ) -> ControlCompletion {
        let mut state = lock_unpoisoned(&self.state);
        match &*state {
            StreamCloseState::Closed(completion) => completion.clone(),
            StreamCloseState::Open(interrupt) => {
                closed.store(true, Ordering::Release);
                let completion = shutdown(interrupt);
                *state = StreamCloseState::Closed(completion.clone());
                completion
            }
        }
    }
}

pub(crate) fn read_request_exceeds_bound(request: &ReadRequest, limit: usize) -> bool {
    request.buffer.capacity() > limit
        || request
            .buffer
            .len()
            .checked_add(request.max_bytes)
            .is_none_or(|result_len| result_len > limit)
}

pub(crate) fn write_request_exceeds_bound(request: &WriteRequest, limit: usize) -> bool {
    request.buffer.capacity() > limit
}

#[derive(Clone, Copy)]
enum ListenerRejection {
    Closing,
    Full,
    Stopped,
}

impl From<Rejection> for ListenerRejection {
    fn from(rejection: Rejection) -> Self {
        match rejection {
            Rejection::Full => Self::Full,
            Rejection::Stopped => Self::Stopped,
        }
    }
}

fn stream_rejection_error(rejection: Rejection, config: UringNetworkConfig) -> NetworkError {
    match rejection {
        Rejection::Full => NetworkError::ResourceExhausted {
            resource: "io_uring network command queue",
            limit: config.command_queue_capacity,
        },
        Rejection::Stopped => NetworkError::DriverStopped,
    }
}

fn provider_rejection_error(rejection: Rejection, capacity: usize) -> NetworkError {
    match rejection {
        Rejection::Full => NetworkError::ResourceExhausted {
            resource: "io_uring network provider command queue",
            limit: capacity,
        },
        Rejection::Stopped => NetworkError::DriverStopped,
    }
}

struct ReadActor {
    ring: Ring,
    stream: TcpStream,
    config: UringNetworkConfig,
    closed: Arc<AtomicBool>,
}

impl ReadActor {
    fn new(
        ring: Ring,
        stream: TcpStream,
        config: UringNetworkConfig,
        closed: Arc<AtomicBool>,
    ) -> Self {
        Self {
            ring,
            stream,
            config,
            closed,
        }
    }

    fn run(&mut self, receiver: Receiver<ReadActorMessage>) {
        while let Ok(mut command) = receiver.recv() {
            match command.take() {
                ReadCommand::Read { request, response } => {
                    let output = {
                        let _fail_stop = FailStopOnPanic;
                        self.read(request)
                    };
                    response.complete(output);
                }
                #[cfg(test)]
                ReadCommand::Panic { entered, release } => {
                    let _ = entered.send(());
                    let _ = release.recv();
                    panic!("injected read actor panic");
                }
            }
        }
    }

    fn read(&mut self, request: ReadRequest) -> ReadCompletion {
        if self.closed.load(Ordering::Acquire) {
            return read_error(NetworkError::ConnectionClosed, request.buffer);
        }
        let original_len = request.buffer.len();
        let requested = request.max_bytes.min(self.config.max_io_chunk_bytes);
        let Some(target_len) = original_len.checked_add(requested) else {
            return read_error(
                NetworkError::InvalidRequest {
                    reason: "read result length overflowed usize",
                },
                request.buffer,
            );
        };
        let mut buffer = request.buffer;
        if let Err(error) = buffer.try_reserve_exact(requested) {
            return read_error(
                backend(NetworkOperationKind::Read, None, error.to_string()),
                buffer,
            );
        }
        buffer.resize(target_len, 0);
        match self
            .ring
            .recv(&self.stream, buffer, original_len, requested)
        {
            Ok(mut transfer) => {
                transfer
                    .buffer
                    .truncate(original_len + transfer.transferred);
                Ok(ReadResult {
                    buffer: transfer.buffer,
                    bytes_read: transfer.transferred,
                    end_of_stream: requested > 0 && transfer.transferred == 0,
                })
            }
            Err(mut failure) => {
                failure.buffer.truncate(original_len);
                let output = NetworkFailure::with_buffer(
                    if self.closed.load(Ordering::Acquire) {
                        NetworkError::ConnectionClosed
                    } else {
                        map_stream_error(NetworkOperationKind::Read, &failure.error)
                    },
                    failure.buffer,
                    0,
                );
                Err(if failure.may_have_applied {
                    CompletionError::may_have_applied(output)
                } else {
                    CompletionError::not_applied(output)
                })
            }
        }
    }
}

struct WriteActor {
    ring: Ring,
    stream: TcpStream,
    closed: Arc<AtomicBool>,
    write_closed: bool,
}

impl WriteActor {
    fn new(ring: Ring, stream: TcpStream, closed: Arc<AtomicBool>) -> Self {
        Self {
            ring,
            stream,
            closed,
            write_closed: false,
        }
    }

    fn run(&mut self, receiver: Receiver<WriteActorMessage>) {
        while let Ok(mut command) = receiver.recv() {
            let command = command.take();
            match command {
                WriteCommand::Write { request, response } => {
                    let output = {
                        let _fail_stop = FailStopOnPanic;
                        self.write(request)
                    };
                    response.complete(output);
                }
                WriteCommand::ShutdownWrite { response } => {
                    let active = ActiveResponder::new(response, active_network_driver_stopped);
                    active.complete(self.shutdown_write());
                }
                #[cfg(test)]
                WriteCommand::Panic { entered, release } => {
                    let _ = entered.send(());
                    let _ = release.recv();
                    panic!("injected write actor panic");
                }
            }
        }
    }

    fn write(&mut self, request: WriteRequest) -> WriteCompletion {
        if self.closed.load(Ordering::Acquire) {
            return write_error(NetworkError::ConnectionClosed, request.buffer);
        }
        if self.write_closed {
            return write_error(NetworkError::WriteClosed, request.buffer);
        }
        let len = request.buffer.len();
        match self.ring.send(&self.stream, request.buffer, 0, len) {
            Ok(transfer) => Ok(WriteResult {
                buffer: transfer.buffer,
                bytes_written: transfer.transferred,
            }),
            Err(failure) if self.closed.load(Ordering::Acquire) => {
                Err(map_closed_transfer_failure(failure))
            }
            Err(failure) => Err(map_transfer_failure(NetworkOperationKind::Write, failure)),
        }
    }

    fn shutdown_write(&mut self) -> ControlCompletion {
        if self.write_closed || self.closed.load(Ordering::Acquire) {
            return Ok(());
        }
        match self.ring.shutdown(&self.stream, libc::SHUT_WR) {
            Ok(()) => {
                self.write_closed = true;
                Ok(())
            }
            Err(error) => Err(CompletionError::may_have_applied(
                NetworkFailure::without_buffer(backend(
                    NetworkOperationKind::ShutdownWrite,
                    error.raw_os_error(),
                    error.to_string(),
                )),
            )),
        }
    }
}

pub(crate) fn read_error(error: NetworkError, buffer: Vec<u8>) -> ReadCompletion {
    Err(CompletionError::not_applied(NetworkFailure::with_buffer(
        error, buffer, 0,
    )))
}

pub(crate) fn write_error(error: NetworkError, buffer: Vec<u8>) -> WriteCompletion {
    Err(CompletionError::not_applied(NetworkFailure::with_buffer(
        error, buffer, 0,
    )))
}

pub(crate) fn control_error(error: NetworkError) -> ControlCompletion {
    Err(CompletionError::not_applied(
        NetworkFailure::without_buffer(error),
    ))
}

fn active_network_driver_stopped<T>() -> CompletionResult<T, NetworkFailure> {
    Err(CompletionError::may_have_applied(
        NetworkFailure::without_buffer(NetworkError::DriverStopped),
    ))
}

fn map_transfer_failure(
    operation: NetworkOperationKind,
    failure: OwnedTransferFailure,
) -> CompletionError<NetworkFailure> {
    let output = NetworkFailure::with_buffer(
        map_stream_error(operation, &failure.error),
        failure.buffer,
        0,
    );
    if failure.may_have_applied {
        CompletionError::may_have_applied(output)
    } else {
        CompletionError::not_applied(output)
    }
}

fn map_closed_transfer_failure(failure: OwnedTransferFailure) -> CompletionError<NetworkFailure> {
    let output = NetworkFailure::with_buffer(NetworkError::ConnectionClosed, failure.buffer, 0);
    if failure.may_have_applied {
        CompletionError::may_have_applied(output)
    } else {
        CompletionError::not_applied(output)
    }
}

pub(crate) fn backend(
    operation: NetworkOperationKind,
    raw_os_error: Option<i32>,
    message: String,
) -> NetworkError {
    NetworkError::Backend {
        operation,
        raw_os_error,
        message,
    }
}

pub(crate) fn map_stream_error(operation: NetworkOperationKind, error: &io::Error) -> NetworkError {
    match error.raw_os_error() {
        Some(
            libc::EPIPE | libc::ECONNRESET | libc::ENOTCONN | libc::ECONNABORTED | libc::ESHUTDOWN,
        ) => NetworkError::ConnectionClosed,
        _ => backend(operation, error.raw_os_error(), error.to_string()),
    }
}

pub(crate) fn no_buffer_error<T>(error: NetworkError) -> CompletionResult<T, NetworkFailure> {
    Err(not_applied_network(error))
}

pub(crate) fn not_applied_network(error: NetworkError) -> CompletionError<NetworkFailure> {
    CompletionError::not_applied(NetworkFailure::without_buffer(error))
}

pub(crate) fn accepted_then_closed() -> CompletionError<NetworkFailure> {
    // The accept CQE already consumed the kernel backlog entry and produced an
    // owned stream. Closing that stream because listener shutdown won the
    // following control-plane check does not undo the accepted connection.
    CompletionError::applied(NetworkFailure::without_buffer(NetworkError::ListenerClosed))
}

pub(crate) fn map_listen_error(error: io::Error) -> NetworkError {
    if error.raw_os_error() == Some(libc::EADDRINUSE) {
        NetworkError::AddressInUse
    } else {
        backend(
            NetworkOperationKind::Listen,
            error.raw_os_error(),
            error.to_string(),
        )
    }
}

pub(crate) fn map_connect_error(error: io::Error) -> NetworkError {
    match error.raw_os_error() {
        Some(libc::EADDRINUSE) => NetworkError::AddressInUse,
        Some(libc::ECONNREFUSED) => NetworkError::ConnectionRefused,
        _ => backend(
            NetworkOperationKind::Connect,
            error.raw_os_error(),
            error.to_string(),
        ),
    }
}

fn open_error(operation: NetworkOperationKind, error: UringNetworkOpenError) -> NetworkError {
    match error {
        UringNetworkOpenError::Io {
            raw_os_error,
            message,
            ..
        } => backend(operation, raw_os_error, message),
        UringNetworkOpenError::InvalidConfig { field, message } => {
            backend(operation, None, format!("invalid {field}: {message}"))
        }
        UringNetworkOpenError::DriverStopped => NetworkError::DriverStopped,
    }
}

pub(crate) fn create_bound_tcp_stream(local: SocketAddr) -> io::Result<TcpStream> {
    let socket = create_tcp_socket(local)?;
    bind_socket(&socket, local)?;
    Ok(TcpStream::from(socket))
}

pub(crate) fn bind_tcp_listener(address: SocketAddr, backlog: usize) -> io::Result<TcpListener> {
    let socket = create_tcp_socket(address)?;
    set_reuse_address(&socket)?;
    bind_socket(&socket, address)?;
    let backlog = i32::try_from(backlog).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("listener backlog {backlog} exceeds i32::MAX"),
        )
    })?;
    // SAFETY: `socket` owns a live TCP socket descriptor and `backlog` is a
    // validated nonnegative i32. No pointer arguments are involved.
    if unsafe { libc::listen(socket.as_raw_fd(), backlog) } < 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(TcpListener::from(socket))
}

fn set_reuse_address(socket: &OwnedFd) -> io::Result<()> {
    let enabled: libc::c_int = 1;
    // SAFETY: `socket` owns a live descriptor and the option pointer refers to
    // an initialized `c_int` for the exact duration reported to setsockopt.
    let result = unsafe {
        libc::setsockopt(
            socket.as_raw_fd(),
            libc::SOL_SOCKET,
            libc::SO_REUSEADDR,
            std::ptr::from_ref(&enabled).cast(),
            std::mem::size_of_val(&enabled) as libc::socklen_t,
        )
    };
    if result < 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(())
}

pub(crate) fn is_retryable_accept_error(error: &io::Error) -> bool {
    matches!(
        error.kind(),
        io::ErrorKind::WouldBlock | io::ErrorKind::Interrupted
    ) || error.raw_os_error() == Some(libc::ECONNABORTED)
}

fn create_tcp_socket(address: SocketAddr) -> io::Result<OwnedFd> {
    let domain = match address {
        SocketAddr::V4(_) => libc::AF_INET,
        SocketAddr::V6(_) => libc::AF_INET6,
    };
    // SAFETY: these constants request a standard TCP socket and return either
    // a fresh owned descriptor or -1 without transferring any Rust resource.
    let raw_fd = unsafe {
        libc::socket(
            domain,
            libc::SOCK_STREAM | libc::SOCK_CLOEXEC,
            libc::IPPROTO_TCP,
        )
    };
    if raw_fd < 0 {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: successful socket(2) returned a new descriptor whose sole owner
    // is transferred into this `OwnedFd` immediately.
    Ok(unsafe { OwnedFd::from_raw_fd(raw_fd) })
}

fn bind_socket(socket: &OwnedFd, address: SocketAddr) -> io::Result<()> {
    let encoded = EncodedSocketAddr::new(address);
    let (pointer, len) = encoded.as_ptr_len();
    // SAFETY: the encoded sockaddr pointer and its exact length refer to a
    // live stack value for the duration of bind(2).
    let result = unsafe { libc::bind(socket.as_raw_fd(), pointer, len) };
    if result < 0 {
        Err(io::Error::last_os_error())
    } else {
        Ok(())
    }
}

fn validate_config(config: UringNetworkConfig) -> Result<(), UringNetworkOpenError> {
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
    if config.max_io_chunk_bytes > config.max_operation_bytes {
        return invalid_config("max_io_chunk_bytes", "must not exceed max_operation_bytes");
    }
    if config.connect_timeout.is_zero() {
        return invalid_config("connect_timeout", "must be nonzero");
    }
    Ok(())
}

fn validate_provider_config(
    config: UringNetworkProviderConfig,
) -> Result<(), UringNetworkOpenError> {
    validate_config(config.stream)?;
    if config.max_listeners == 0 {
        return invalid_config("max_listeners", "must be nonzero");
    }
    if config.max_streams == 0 {
        return invalid_config("max_streams", "must be nonzero");
    }
    // Not checked against the live `RLIMIT_NOFILE`: unlike the datagram
    // provider's `max_sockets`, this ceiling reserves nothing at construction,
    // so rejecting a config whose worst case exceeds the current limit would
    // fail providers that never approach it. Exhaustion still fails closed —
    // creating a stream reactor without descriptors returns a typed
    // `UringNetworkOpenError::Io` — so only the arithmetic is checked here.
    if config
        .max_streams
        .checked_mul(DESCRIPTORS_PER_STREAM)
        .is_none()
    {
        return invalid_config("max_streams", "descriptor requirement overflowed");
    }
    if config.max_listener_backlog == 0 {
        return invalid_config("max_listener_backlog", "must be nonzero");
    }
    if config.max_listener_backlog > i32::MAX as usize {
        return invalid_config("max_listener_backlog", "must not exceed i32::MAX");
    }
    Ok(())
}

fn invalid_config<T>(
    field: &'static str,
    message: impl Into<String>,
) -> Result<T, UringNetworkOpenError> {
    Err(UringNetworkOpenError::InvalidConfig {
        field,
        message: message.into(),
    })
}

fn open_io(action: &'static str, error: io::Error) -> UringNetworkOpenError {
    UringNetworkOpenError::Io {
        action,
        raw_os_error: error.raw_os_error(),
        message: error.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use std::net::Ipv4Addr;
    use std::sync::Barrier;
    use std::sync::atomic::AtomicUsize;

    use super::*;
    use crate::test_support::block_on;

    fn test_config() -> UringNetworkConfig {
        UringNetworkConfig {
            command_queue_capacity: 8,
            ring_entries: 4,
            max_operation_bytes: 256,
            max_io_chunk_bytes: 64,
            connect_timeout: Duration::from_millis(100),
        }
    }

    fn test_provider_config() -> UringNetworkProviderConfig {
        UringNetworkProviderConfig {
            stream: test_config(),
            max_listeners: 8,
            max_streams: 16,
            max_listener_backlog: 8,
        }
    }

    fn inject_panic<C: DriverStoppedCommand>(
        sender: &SyncSender<TerminalCommand<C>>,
        make: impl FnOnce(SyncSender<()>, Receiver<()>) -> C,
    ) -> (Receiver<()>, SyncSender<()>) {
        let (entered_sender, entered_receiver) = mpsc::sync_channel(1);
        let (release_sender, release_receiver) = mpsc::sync_channel(1);
        assert!(
            sender
                .send(TerminalCommand::new(make(entered_sender, release_receiver)))
                .is_ok()
        );
        (entered_receiver, release_sender)
    }

    #[test]
    fn config_rejects_unbounded_or_incoherent_values() {
        let config = UringNetworkConfig {
            max_io_chunk_bytes: UringNetworkConfig::default().max_operation_bytes + 1,
            ..UringNetworkConfig::default()
        };
        assert!(matches!(
            validate_config(config),
            Err(UringNetworkOpenError::InvalidConfig {
                field: "max_io_chunk_bytes",
                ..
            })
        ));

        let config = UringNetworkConfig {
            connect_timeout: Duration::ZERO,
            ..UringNetworkConfig::default()
        };
        assert!(matches!(
            validate_config(config),
            Err(UringNetworkOpenError::InvalidConfig {
                field: "connect_timeout",
                ..
            })
        ));

        let provider = UringNetworkProviderConfig {
            stream: UringNetworkConfig {
                ring_entries: 1,
                ..UringNetworkConfig::default()
            },
            ..UringNetworkProviderConfig::default()
        };
        assert!(matches!(
            validate_provider_config(provider),
            Err(UringNetworkOpenError::InvalidConfig {
                field: "ring_entries",
                ..
            })
        ));
    }

    #[test]
    fn read_bound_includes_the_preserved_buffer_prefix() {
        assert!(!read_request_exceeds_bound(
            &ReadRequest {
                buffer: vec![0; 2],
                max_bytes: 2,
            },
            4
        ));
        assert!(read_request_exceeds_bound(
            &ReadRequest {
                buffer: vec![0; 3],
                max_bytes: 2,
            },
            4
        ));
    }

    #[test]
    fn stream_bounds_include_retained_buffer_capacity() {
        let mut read_buffer = Vec::with_capacity(5);
        read_buffer.push(b'r');
        assert!(read_request_exceeds_bound(
            &ReadRequest {
                buffer: read_buffer,
                max_bytes: 1,
            },
            4
        ));

        let mut write_buffer = Vec::with_capacity(5);
        write_buffer.push(b'w');
        assert!(write_request_exceeds_bound(
            &WriteRequest {
                buffer: write_buffer,
            },
            4
        ));
    }

    #[test]
    fn concurrent_stream_closes_publish_one_shared_terminal_result() {
        let listener = TcpListener::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)))
            .expect("bind loopback listener");
        let address = listener.local_addr().expect("listener address");
        let client = TcpStream::connect(address).expect("connect loopback client");
        let (_server, _) = listener.accept().expect("accept loopback client");
        let close = Arc::new(StreamClose::new(client));
        let closed = Arc::new(AtomicBool::new(false));
        let shutdowns = Arc::new(AtomicUsize::new(0));
        let callers = 8;
        let start = Arc::new(Barrier::new(callers));

        let threads = (0..callers)
            .map(|_| {
                let close = Arc::clone(&close);
                let closed = Arc::clone(&closed);
                let shutdowns = Arc::clone(&shutdowns);
                let start = Arc::clone(&start);
                thread::spawn(move || {
                    start.wait();
                    close.close_with(&closed, |_| {
                        shutdowns.fetch_add(1, Ordering::SeqCst);
                        Err(CompletionError::may_have_applied(
                            NetworkFailure::without_buffer(NetworkError::Injected { tag: 91 }),
                        ))
                    })
                })
            })
            .collect::<Vec<_>>();

        let expected = Err(CompletionError::may_have_applied(
            NetworkFailure::without_buffer(NetworkError::Injected { tag: 91 }),
        ));
        for thread in threads {
            assert_eq!(thread.join().expect("close caller joins"), expected);
        }
        assert!(closed.load(Ordering::Acquire));
        assert_eq!(shutdowns.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn peer_disconnect_errnos_share_the_provider_neutral_category() {
        for errno in [
            libc::EPIPE,
            libc::ECONNRESET,
            libc::ENOTCONN,
            libc::ECONNABORTED,
            libc::ESHUTDOWN,
        ] {
            assert_eq!(
                map_stream_error(
                    NetworkOperationKind::Write,
                    &io::Error::from_raw_os_error(errno),
                ),
                NetworkError::ConnectionClosed
            );
        }

        assert!(matches!(
            map_stream_error(
                NetworkOperationKind::Read,
                &io::Error::from_raw_os_error(libc::EIO),
            ),
            NetworkError::Backend {
                operation: NetworkOperationKind::Read,
                raw_os_error: Some(libc::EIO),
                ..
            }
        ));
    }

    #[test]
    fn aborted_accept_is_retryable() {
        assert!(is_retryable_accept_error(&io::Error::from_raw_os_error(
            libc::ECONNABORTED
        )));
        assert!(!is_retryable_accept_error(&io::Error::from_raw_os_error(
            libc::EIO
        )));
    }

    #[test]
    fn listener_close_after_accept_reports_applied() {
        let error = accepted_then_closed();
        assert_eq!(error.certainty(), kr_runtime::CompletionCertainty::Applied);
        assert_eq!(error.error().error(), &NetworkError::ListenerClosed);
    }

    #[test]
    fn listener_socket_enables_address_reuse() {
        let listener = bind_tcp_listener(SocketAddr::from((std::net::Ipv4Addr::LOCALHOST, 0)), 1)
            .expect("bind listener");
        let mut enabled: libc::c_int = 0;
        let mut length = std::mem::size_of_val(&enabled) as libc::socklen_t;
        // SAFETY: all pointers refer to initialized storage with the exact
        // lengths supplied, and the listener descriptor remains live.
        let result = unsafe {
            libc::getsockopt(
                listener.as_raw_fd(),
                libc::SOL_SOCKET,
                libc::SO_REUSEADDR,
                std::ptr::from_mut(&mut enabled).cast(),
                &mut length,
            )
        };
        assert_eq!(
            result,
            0,
            "inspect SO_REUSEADDR: {}",
            io::Error::last_os_error()
        );
        assert_eq!(length as usize, std::mem::size_of_val(&enabled));
        assert_eq!(enabled, 1);
    }

    #[test]
    fn panicked_stream_actors_terminalize_queued_and_later_buffers() {
        let listener = TcpListener::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)))
            .expect("bind loopback listener");
        let address = listener.local_addr().expect("listener address");
        let client = TcpStream::connect(address).expect("connect loopback client");
        let (_server, _) = listener.accept().expect("accept loopback client");
        let stream =
            UringByteStream::from_tcp_stream(client, test_config()).expect("start stream actors");

        let (entered, release) = inject_panic(
            stream.read_sender.as_ref().expect("read actor sender"),
            |entered, release| ReadCommand::Panic { entered, release },
        );
        entered.recv().expect("read actor reaches panic gate");
        let mut read_buffer = Vec::with_capacity(32);
        read_buffer.extend_from_slice(b"prefix");
        let read_pointer = read_buffer.as_ptr();
        let read = stream.submit_read(ReadRequest {
            buffer: read_buffer,
            max_bytes: 1,
        });
        release.send(()).expect("release read actor panic");
        let read = block_on(read).expect_err("queued read must fail");
        let (_, read) = read.into_parts();
        assert_eq!(read.error(), &NetworkError::DriverStopped);
        let read_buffer = read.into_buffer().expect("read buffer");
        assert_eq!(read_buffer.as_ptr(), read_pointer);

        let mut later_read_buffer = Vec::with_capacity(32);
        later_read_buffer.extend_from_slice(b"later");
        let later_read_pointer = later_read_buffer.as_ptr();
        let later_read = block_on(stream.submit_read(ReadRequest {
            buffer: later_read_buffer,
            max_bytes: 1,
        }))
        .expect_err("stopped read actor rejects later work");
        let (_, later_read) = later_read.into_parts();
        assert_eq!(later_read.error(), &NetworkError::DriverStopped);
        let later_read_buffer = later_read.into_buffer().expect("later read buffer");
        assert_eq!(later_read_buffer.as_ptr(), later_read_pointer);

        let (entered, release) = inject_panic(
            stream.write_sender.as_ref().expect("write actor sender"),
            |entered, release| WriteCommand::Panic { entered, release },
        );
        entered.recv().expect("write actor reaches panic gate");
        let mut write_buffer = Vec::with_capacity(32);
        write_buffer.extend_from_slice(b"data");
        let write_pointer = write_buffer.as_ptr();
        let write = stream.submit_write(WriteRequest {
            buffer: write_buffer,
        });
        let shutdown = stream.submit_shutdown_write();
        let close = stream.submit_close();
        release.send(()).expect("release write actor panic");
        let write = block_on(write).expect_err("queued write must fail");
        let (_, write) = write.into_parts();
        assert_eq!(write.error(), &NetworkError::DriverStopped);
        let write_buffer = write.into_buffer().expect("write buffer");
        assert_eq!(write_buffer.as_ptr(), write_pointer);
        assert_eq!(
            block_on(shutdown)
                .expect_err("queued shutdown must fail")
                .error()
                .error(),
            &NetworkError::DriverStopped
        );
        block_on(close).expect("out-of-band close does not depend on the write actor");

        let mut later_write_buffer = Vec::with_capacity(32);
        later_write_buffer.extend_from_slice(b"later");
        let later_write_pointer = later_write_buffer.as_ptr();
        let later_write = block_on(stream.submit_write(WriteRequest {
            buffer: later_write_buffer,
        }))
        .expect_err("stopped write actor rejects later work");
        let (_, later_write) = later_write.into_parts();
        assert_eq!(later_write.error(), &NetworkError::DriverStopped);
        let later_write_buffer = later_write.into_buffer().expect("later write buffer");
        assert_eq!(later_write_buffer.as_ptr(), later_write_pointer);
    }

    #[test]
    fn panicked_listener_and_provider_terminalize_waiters_and_later_admission() {
        let provider = UringNetwork::new(test_provider_config()).expect("start provider actor");
        let listener = block_on(provider.submit_listen(ListenRequest {
            address: SocketAddr::from((Ipv4Addr::LOCALHOST, 0)),
            backlog: 2,
        }))
        .expect("start listener actor");

        let (entered, release) = inject_panic(
            listener.sender.as_ref().expect("listener actor sender"),
            |entered, release| ListenerCommand::Panic { entered, release },
        );
        entered.recv().expect("listener reaches panic gate");
        let accept = listener.submit_accept();
        let close = listener.submit_close();
        release.send(()).expect("release listener panic");
        assert_eq!(
            block_on(accept)
                .err()
                .expect("queued accept must fail")
                .error()
                .error(),
            &NetworkError::DriverStopped
        );
        assert_eq!(
            block_on(close)
                .expect_err("close waiter must fail")
                .error()
                .error(),
            &NetworkError::DriverStopped
        );
        assert_eq!(
            block_on(listener.submit_accept())
                .err()
                .expect("stopped listener rejects later accept")
                .error()
                .error(),
            &NetworkError::DriverStopped
        );

        let (entered, release) = inject_panic(
            provider.sender.as_ref().expect("provider actor sender"),
            |entered, release| ProviderCommand::Panic { entered, release },
        );
        entered.recv().expect("provider reaches panic gate");
        let listen = provider.submit_listen(ListenRequest {
            address: SocketAddr::from((Ipv4Addr::LOCALHOST, 0)),
            backlog: 2,
        });
        let connect = provider.submit_connect(ConnectRequest {
            local: SocketAddr::from((Ipv4Addr::LOCALHOST, 0)),
            remote: SocketAddr::from((Ipv4Addr::LOCALHOST, 9)),
        });
        release.send(()).expect("release provider panic");
        assert_eq!(
            block_on(listen)
                .err()
                .expect("queued listen must fail")
                .error()
                .error(),
            &NetworkError::DriverStopped
        );
        assert_eq!(
            block_on(connect)
                .err()
                .expect("queued connect must fail")
                .error()
                .error(),
            &NetworkError::DriverStopped
        );
        assert_eq!(
            block_on(provider.submit_listen(ListenRequest {
                address: SocketAddr::from((Ipv4Addr::LOCALHOST, 0)),
                backlog: 2,
            }))
            .err()
            .expect("stopped provider rejects later listen")
            .error()
            .error(),
            &NetworkError::DriverStopped
        );
    }
}
