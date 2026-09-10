//! Atomic UDP datagrams backed by bounded io_uring actors.
//!
//! Socket creation, bind, and address discovery are lifecycle syscalls. Every
//! packet send and receive is submitted as `IORING_OP_SENDMSG` or
//! `IORING_OP_RECVMSG`. A socket has independent send and receive actors so a
//! waiting receive cannot prevent local send progress. The receive side admits
//! exactly one operation at a time; a blocking receive issues one kernel
//! RecvMsg and is retired by `IORING_OP_ASYNC_CANCEL`, so an out-of-band close
//! interrupts it without relying on `shutdown(2)`, which does not cancel a
//! receive on an unconnected UDP socket. Cancellation replaces an earlier
//! design that sampled for packets between interruptible actor parks; that cost
//! a wakeup per socket per interval whether or not any traffic arrived.

use std::fmt;
use std::io;
use std::net::{SocketAddr, UdpSocket};
use std::sync::mpsc::{self, Receiver, RecvError, SyncSender, TrySendError};
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};
use std::time::Instant;

use kr_runtime::{CompletionError, CompletionResult};
use kr_runtime_io::datagram::{
    DatagramBindRequest, DatagramError, DatagramFailure, DatagramOperationKind,
    DatagramProviderSubmit, DatagramSocketSubmit, DatagramTruncation, RecvFromRequest,
    RecvFromResult, SendToRequest, SendToResult,
};

use crate::operation::{
    DriverStoppedCommand, FailStopOnPanic, Responder, TerminalCommand, UringOperation, operation,
    ready,
};
use crate::ring::{
    CancelToken, DatagramEffect, DatagramReceiveAttempt, OwnedDatagramFailure,
    OwnedDatagramReceive, Ring, RingCapacity,
};
use crate::support::{
    ResourcePermit, ResourcePool, finish_actor_start, join_if_other_thread, lock_unpoisoned,
};

type BindCompletion = CompletionResult<UringDatagramSocket, DatagramFailure>;
type SendCompletion = CompletionResult<SendToResult, DatagramFailure>;
type RecvCompletion = CompletionResult<RecvFromResult<SocketAddr>, DatagramFailure>;
type ControlCompletion = CompletionResult<(), DatagramFailure>;

const RECEIVE_LIMIT: usize = 1;

/// Largest total in-flight completion capacity one reactor can be built with.
///
/// Mirrors `IORING_MAX_CQ_ENTRIES`. Checked here so an impossible pairing of
/// `max_sockets` and `ring_entries` is a typed config error at construction
/// instead of an io_uring build failure or a saturated ring at runtime.
const MAX_RING_IN_FLIGHT: usize = 65_536;

/// Fixed resource limits for the Linux io_uring UDP provider.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct UringDatagramConfig {
    /// Commands admitted behind the active send operation for one socket.
    ///
    /// The same value bounds concurrent close waiters. The receive actor has a
    /// stricter portable admission limit of one operation.
    pub command_queue_capacity: usize,
    /// Maximum user SQEs kept in flight by the provider's shared reactor.
    /// Command notification uses a separate internal poll operation.
    pub ring_entries: u32,
    /// Maximum number of live bound sockets.
    pub max_sockets: usize,
    /// Maximum UDP payload accepted by one send.
    pub max_datagram_bytes: usize,
    /// Maximum retained caller `Vec` capacity and total logical receive result
    /// bytes for one admitted operation.
    pub max_operation_bytes: usize,
}

impl Default for UringDatagramConfig {
    fn default() -> Self {
        Self {
            command_queue_capacity: 64,
            ring_entries: 8,
            // Every bound socket may hold one armed receive, so this is what
            // the reactor must reserve completion capacity for: 128 sockets
            // need a 256-entry completion queue where 1,024 needed 2,048. The
            // larger bound was an import-time default no workload asked for,
            // and it is only a default — raise it deliberately and
            // `validate_config` will confirm the ring can carry it.
            max_sockets: 128,
            max_datagram_bytes: 65_507,
            max_operation_bytes: 256 * 1_024,
        }
    }
}

/// Failure to construct the provider or one of its bounded socket actors.
#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum UringDatagramOpenError {
    /// A fixed resource bound is invalid.
    InvalidConfig {
        field: &'static str,
        message: String,
    },
    /// UDP lifecycle setup or io_uring construction failed.
    Io {
        action: &'static str,
        raw_os_error: Option<i32>,
        message: String,
    },
    /// A socket actor stopped before its readiness handshake.
    DriverStopped,
}

impl fmt::Display for UringDatagramOpenError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidConfig { field, message } => {
                write!(formatter, "invalid io_uring datagram {field}: {message}")
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
            Self::DriverStopped => formatter.write_str("io_uring datagram driver stopped"),
        }
    }
}

impl std::error::Error for UringDatagramOpenError {}

/// Linux UDP provider whose packet data plane is driven by io_uring.
pub struct UringDatagram {
    config: UringDatagramConfig,
    ring: Ring,
    socket_pool: Arc<ResourcePool>,
}

impl UringDatagram {
    /// Validates all bounds and proves the running kernel supports the required
    /// datagram opcodes.
    ///
    /// # Errors
    ///
    /// Returns [`UringDatagramOpenError::InvalidConfig`] when a config bound
    /// is invalid, or [`UringDatagramOpenError::Io`] when the io_uring reactor
    /// cannot be created or the kernel lacks a required opcode.
    pub fn new(config: UringDatagramConfig) -> Result<Self, UringDatagramOpenError> {
        validate_config(config)?;
        // `RECEIVE_LIMIT` admits one receive per socket, so `max_sockets` is
        // exactly the number that can be armed at once. Reserving that many
        // slots is what keeps an idle socket from ever holding capacity a send
        // on an unrelated socket needs.
        let ring = Ring::for_datagram(RingCapacity {
            entries: config.ring_entries,
            transient: config.ring_entries as usize,
            sustained: config.max_sockets * RECEIVE_LIMIT,
        })
        .map_err(|error| open_io("create datagram reactor", error))?;
        Ok(Self {
            config,
            ring,
            socket_pool: Arc::new(ResourcePool::new(config.max_sockets)),
        })
    }
}

impl DatagramProviderSubmit for UringDatagram {
    type Address = SocketAddr;
    type Instant = Instant;
    type Socket = UringDatagramSocket;
    type BindResponse = UringOperation<BindCompletion>;

    fn submit_bind(&self, request: DatagramBindRequest<SocketAddr>) -> Self::BindResponse {
        let Some(socket_permit) = self.socket_pool.acquire() else {
            return ready(no_buffer_error(DatagramError::ResourceExhausted {
                resource: "io_uring datagram sockets",
                limit: self.config.max_sockets,
            }));
        };

        let socket = match UdpSocket::bind(request.address) {
            Ok(socket) => socket,
            Err(error) => {
                return ready(no_buffer_error(map_bind_error(&error)));
            }
        };
        let address = match socket.local_addr() {
            Ok(address) => address,
            Err(error) => {
                return ready(no_buffer_error(backend(
                    DatagramOperationKind::Bind,
                    &error,
                )));
            }
        };
        match UringDatagramSocket::start(
            socket,
            address,
            self.ring.clone(),
            self.config,
            socket_permit,
        ) {
            Ok(socket) => ready(Ok(socket)),
            Err(error) => ready(no_buffer_error(DatagramError::Backend {
                operation: DatagramOperationKind::Bind,
                raw_os_error: open_raw_os_error(&error),
                message: error.to_string(),
            })),
        }
    }
}

struct SendCommand {
    request: SendToRequest<SocketAddr>,
    response: Responder<SendCompletion>,
}

enum ReceiveMode {
    Blocking,
    Nonblocking,
    Deadline(Instant),
}

struct ReceiveCommand {
    request: RecvFromRequest,
    mode: ReceiveMode,
    response: Responder<RecvCompletion>,
    permit: Option<ReceivePermit>,
}

type SendActorMessage = TerminalCommand<SendCommand>;
type ReceiveActorMessage = TerminalCommand<ReceiveCommand>;

impl DriverStoppedCommand for SendCommand {
    fn complete_driver_stopped(self) {
        let Self { request, response } = self;
        response.complete(buffer_error(
            DatagramError::DriverStopped,
            request.buffer,
            0,
            DatagramEffect::NotApplied,
        ));
    }
}

impl DriverStoppedCommand for ReceiveCommand {
    fn complete_driver_stopped(self) {
        let Self {
            request,
            response,
            permit,
            ..
        } = self;
        drop(permit);
        response.complete(buffer_error(
            DatagramError::DriverStopped,
            request.buffer,
            0,
            DatagramEffect::NotApplied,
        ));
    }
}

/// One bound UDP socket with independent bounded io_uring actors.
///
/// Exactly one receive may be admitted at a time, and a blocking or deadline
/// receive arms a single cancellable recvmsg rather than polling. A deadline
/// is served by cancelling that armed operation when it expires: a valid
/// receive CQE that races the cancel wins and is delivered, while a cancel
/// that wins returns a clean `DeadlineExceeded` completion. Either way the
/// terminal CQE is consumed before the call returns. Each admitted command
/// owns its response channel and caller buffer until it terminalizes.
pub struct UringDatagramSocket {
    send_join: Mutex<Option<JoinHandle<()>>>,
    receive_join: Mutex<Option<JoinHandle<()>>>,
    control: Arc<SocketControl>,
    address: SocketAddr,
    config: UringDatagramConfig,
}

impl UringDatagramSocket {
    fn start(
        socket: UdpSocket,
        address: SocketAddr,
        ring: Ring,
        config: UringDatagramConfig,
        socket_permit: ResourcePermit,
    ) -> Result<Self, UringDatagramOpenError> {
        let send_socket = socket
            .try_clone()
            .map_err(|error| open_io("clone UDP socket for send actor", error))?;
        let control = Arc::new(SocketControl::new(
            config.command_queue_capacity,
            2,
            socket_permit,
            ring.clone(),
        ));
        let (send_sender, send_join) =
            match start_send_actor(send_socket, ring.clone(), config, Arc::clone(&control)) {
                Ok(actor) => actor,
                Err(error) => {
                    control.actor_never_started();
                    return Err(error);
                }
            };
        // Register before the receive actor can fail: the failure path below
        // closes, and closing is what drops this sender to wake the send actor.
        // An unregistered sender would leave that join waiting forever.
        control.register_send_sender(send_sender);
        let (receive_sender, receive_join) =
            match start_receive_actor(socket, ring, Arc::clone(&control)) {
                Ok(actor) => actor,
                Err(error) => {
                    control.request_close_without_response();
                    let _ = send_join.join();
                    return Err(error);
                }
            };
        control.register_receive_sender(receive_sender);
        Ok(Self {
            send_join: Mutex::new(Some(send_join)),
            receive_join: Mutex::new(Some(receive_join)),
            control,
            address,
            config,
        })
    }

    fn receive(
        &self,
        request: RecvFromRequest,
        mode: ReceiveMode,
    ) -> UringOperation<RecvCompletion> {
        if request
            .buffer
            .len()
            .checked_add(request.max_bytes)
            .is_none_or(|len| len > self.config.max_operation_bytes)
            || request.buffer.capacity() > self.config.max_operation_bytes
        {
            return ready(buffer_error(
                DatagramError::ResourceExhausted {
                    resource: "io_uring datagram receive operation bytes",
                    limit: self.config.max_operation_bytes,
                },
                request.buffer,
                0,
                DatagramEffect::NotApplied,
            ));
        }

        let (future, response) = operation();
        let command = ReceiveCommand {
            request,
            mode,
            response,
            permit: None,
        };
        match self.control.try_admit_receive(command) {
            Ok(()) => future,
            Err((command, error)) => {
                let ReceiveCommand {
                    request,
                    response,
                    permit,
                    ..
                } = command;
                drop(permit);
                response.complete(buffer_error(
                    error,
                    request.buffer,
                    0,
                    DatagramEffect::NotApplied,
                ));
                future
            }
        }
    }

    fn join_actors(&self) {
        join_actor(&self.send_join);
        join_actor(&self.receive_join);
    }
}

impl DatagramSocketSubmit for UringDatagramSocket {
    type Address = SocketAddr;
    type Instant = Instant;
    type SendResponse = UringOperation<SendCompletion>;
    type RecvResponse = UringOperation<RecvCompletion>;
    type ControlResponse = UringOperation<ControlCompletion>;

    fn local_addr(&self) -> SocketAddr {
        self.address
    }

    fn submit_send_to(&self, request: SendToRequest<SocketAddr>) -> Self::SendResponse {
        if request.buffer.len() > self.config.max_datagram_bytes {
            return ready(buffer_error(
                DatagramError::MessageTooLarge {
                    max_payload_bytes: Some(self.config.max_datagram_bytes),
                },
                request.buffer,
                0,
                DatagramEffect::NotApplied,
            ));
        }
        if request.buffer.capacity() > self.config.max_operation_bytes {
            return ready(buffer_error(
                DatagramError::ResourceExhausted {
                    resource: "io_uring datagram send operation bytes",
                    limit: self.config.max_operation_bytes,
                },
                request.buffer,
                0,
                DatagramEffect::NotApplied,
            ));
        }
        if !same_address_family(self.address, request.destination) {
            return ready(buffer_error(
                DatagramError::AddressFamilyMismatch,
                request.buffer,
                0,
                DatagramEffect::NotApplied,
            ));
        }

        let (future, response) = operation();
        let command = SendCommand { request, response };
        match self.control.try_admit_send(command) {
            Ok(()) => future,
            Err((command, error)) => {
                let SendCommand { request, response } = command;
                response.complete(buffer_error(
                    error,
                    request.buffer,
                    0,
                    DatagramEffect::NotApplied,
                ));
                future
            }
        }
    }

    fn submit_recv_from(&self, request: RecvFromRequest) -> Self::RecvResponse {
        self.receive(request, ReceiveMode::Blocking)
    }

    fn submit_try_recv_from(&self, request: RecvFromRequest) -> Self::RecvResponse {
        self.receive(request, ReceiveMode::Nonblocking)
    }

    fn submit_recv_from_until(
        &self,
        request: RecvFromRequest,
        deadline: Instant,
    ) -> Self::RecvResponse {
        self.receive(request, ReceiveMode::Deadline(deadline))
    }

    fn submit_close(&self) -> Self::ControlResponse {
        let (future, response) = operation();
        self.control.request_close(response);
        future
    }
}

impl Drop for UringDatagramSocket {
    fn drop(&mut self) {
        self.control.request_close_without_response();
        self.join_actors();
    }
}

fn start_send_actor(
    socket: UdpSocket,
    ring: Ring,
    config: UringDatagramConfig,
    control: Arc<SocketControl>,
) -> Result<(SyncSender<SendActorMessage>, JoinHandle<()>), UringDatagramOpenError> {
    let (sender, receiver) = mpsc::sync_channel(config.command_queue_capacity);
    let (ready_sender, ready_receiver) = mpsc::sync_channel(1);
    let actor_control = Arc::clone(&control);
    let join = match thread::Builder::new()
        .name("kr-runtime-io-uring-udp-send".to_owned())
        .spawn(move || {
            let exit = DatagramActorExit::new(Arc::clone(&actor_control));
            if ready_sender.send(Ok(())).is_err() {
                drop(socket);
                exit.finish(None);
                return;
            }
            let mut actor = SendActor {
                ring,
                socket,
                receiver,
                control: Arc::clone(&actor_control),
            };
            let error = actor.run();
            drop(actor);
            exit.finish(error);
        }) {
        Ok(join) => join,
        Err(error) => {
            control.actor_never_started();
            return Err(open_io("spawn datagram send actor", error));
        }
    };
    finish_actor_start(
        sender,
        join,
        ready_receiver,
        UringDatagramOpenError::DriverStopped,
    )
}

fn start_receive_actor(
    socket: UdpSocket,
    ring: Ring,
    control: Arc<SocketControl>,
) -> Result<(SyncSender<ReceiveActorMessage>, JoinHandle<()>), UringDatagramOpenError> {
    let (sender, receiver) = mpsc::sync_channel(RECEIVE_LIMIT);
    let (ready_sender, ready_receiver) = mpsc::sync_channel(1);
    let actor_control = Arc::clone(&control);
    let join = match thread::Builder::new()
        .name("kr-runtime-io-uring-udp-recv".to_owned())
        .spawn(move || {
            let exit = DatagramActorExit::new(Arc::clone(&actor_control));
            if ready_sender.send(Ok(())).is_err() {
                drop(socket);
                exit.finish(None);
                return;
            }
            let mut actor = ReceiveActor {
                ring,
                socket,
                receiver,
                control: Arc::clone(&actor_control),
            };
            let error = actor.run();
            drop(actor);
            exit.finish(error);
        }) {
        Ok(join) => join,
        Err(error) => {
            control.actor_never_started();
            return Err(open_io("spawn datagram receive actor", error));
        }
    };
    finish_actor_start(
        sender,
        join,
        ready_receiver,
        UringDatagramOpenError::DriverStopped,
    )
}

struct SendActor {
    ring: Ring,
    socket: UdpSocket,
    receiver: Receiver<SendActorMessage>,
    control: Arc<SocketControl>,
}

impl SendActor {
    fn run(&mut self) -> Option<DatagramError> {
        loop {
            if self.control.is_closing() {
                self.drain_closed();
                return None;
            }
            match self.receiver.recv() {
                Ok(mut command) => {
                    let SendCommand { request, response } = command.take();
                    if self.control.is_closing() {
                        response.complete(buffer_error(
                            DatagramError::SocketClosed,
                            request.buffer,
                            0,
                            DatagramEffect::NotApplied,
                        ));
                        continue;
                    }
                    let (output, retire) = {
                        let _fail_stop = FailStopOnPanic;
                        self.send(request)
                    };
                    response.complete(output);
                    if retire {
                        return Some(DatagramError::DriverStopped);
                    }
                }
                // Blocking here costs nothing while idle. Admission wakes this
                // by sending; close and actor exit wake it by dropping the
                // sender, which is the only way this loop ends.
                Err(RecvError) => {
                    return if self.control.is_closing() {
                        None
                    } else {
                        Some(DatagramError::DriverStopped)
                    };
                }
            }
        }
    }

    fn send(&mut self, request: SendToRequest<SocketAddr>) -> (SendCompletion, bool) {
        let result = self
            .ring
            .send_datagram(&self.socket, request.buffer, request.destination);
        let retire = self.ring.is_poisoned();
        match result {
            Ok(sent) => (
                Ok(SendToResult {
                    bytes_sent: sent.transferred,
                    buffer: sent.buffer,
                }),
                false,
            ),
            Err(failure) => {
                let error = map_data_error(
                    DatagramOperationKind::SendTo,
                    &failure.error,
                    self.control.is_closing(),
                    false,
                );
                (
                    buffer_error(
                        error,
                        failure.buffer,
                        failure.bytes_transferred,
                        failure.effect,
                    ),
                    retire,
                )
            }
        }
    }

    fn drain_closed(&mut self) {
        while let Ok(command) = self.receiver.try_recv() {
            let SendCommand { request, response } = command.into_inner();
            response.complete(buffer_error(
                DatagramError::SocketClosed,
                request.buffer,
                0,
                DatagramEffect::NotApplied,
            ));
        }
    }
}

struct ReceiveActor {
    ring: Ring,
    socket: UdpSocket,
    receiver: Receiver<ReceiveActorMessage>,
    control: Arc<SocketControl>,
}

impl ReceiveActor {
    fn run(&mut self) -> Option<DatagramError> {
        loop {
            if self.control.is_closing() {
                self.drain_closed();
                return None;
            }
            match self.receiver.recv() {
                Ok(mut command) => {
                    let ReceiveCommand {
                        request,
                        mode,
                        response,
                        permit,
                    } = command.take();
                    let (output, retire) = if self.control.is_closing() {
                        (
                            buffer_error(
                                DatagramError::SocketClosed,
                                request.buffer,
                                0,
                                DatagramEffect::NotApplied,
                            ),
                            false,
                        )
                    } else {
                        let _fail_stop = FailStopOnPanic;
                        self.receive(request, mode)
                    };
                    drop(permit);
                    response.complete(output);
                    if retire {
                        return Some(DatagramError::DriverStopped);
                    }
                }
                // Blocking here costs nothing while idle. Admission wakes this
                // by sending; close and actor exit wake it by dropping the
                // sender, which is the only way this loop ends.
                Err(RecvError) => {
                    return if self.control.is_closing() {
                        None
                    } else {
                        Some(DatagramError::DriverStopped)
                    };
                }
            }
        }
    }

    fn receive(&mut self, request: RecvFromRequest, mode: ReceiveMode) -> (RecvCompletion, bool) {
        let original_len = request.buffer.len();
        match mode {
            ReceiveMode::Nonblocking => {
                let result = self.ring.try_recv_datagram(
                    &self.socket,
                    request.buffer,
                    original_len,
                    request.max_bytes,
                );
                let retire = self.ring.is_poisoned();
                self.finish_receive(original_len, result, true, retire)
            }
            ReceiveMode::Blocking | ReceiveMode::Deadline(_) => {
                let buffer = request.buffer;
                if self.control.is_closing() {
                    return (
                        buffer_error(
                            DatagramError::SocketClosed,
                            buffer,
                            0,
                            DatagramEffect::NotApplied,
                        ),
                        false,
                    );
                }
                let deadline = match mode {
                    ReceiveMode::Deadline(deadline) => {
                        if Instant::now() >= deadline {
                            return (
                                buffer_error(
                                    DatagramError::DeadlineExceeded,
                                    buffer,
                                    0,
                                    DatagramEffect::NotApplied,
                                ),
                                false,
                            );
                        }
                        Some(deadline)
                    }
                    ReceiveMode::Blocking => None,
                    ReceiveMode::Nonblocking => unreachable!("handled above"),
                };
                // One kernel-blocking receive replaces the former attempt-park
                // loop. Close no longer has to be sampled: it retires this
                // operation through the armed cancel token.
                let control = Arc::clone(&self.control);
                let attempt = self.ring.recv_datagram_blocking(
                    &self.socket,
                    buffer,
                    original_len,
                    request.max_bytes,
                    deadline,
                    |token| control.arm_receive_cancel(token),
                );
                self.control.disarm_receive_cancel();
                match attempt {
                    Ok(DatagramReceiveAttempt::Received(received)) => {
                        // A datagram that reached here already left the
                        // kernel's socket buffer and cannot be put back, so it
                        // is delivered even when a close won the race. Turning
                        // it into SocketClosed would silently drop a packet the
                        // peer believes was delivered.
                        let retire = self.ring.is_poisoned();
                        self.finish_receive(original_len, Ok(received), false, retire)
                    }
                    Ok(DatagramReceiveAttempt::Cancelled { buffer }) => {
                        // Nothing was dequeued, so both outcomes are NotApplied.
                        // A cancel issued by close outranks the deadline: the
                        // socket is gone either way, and SocketClosed is the
                        // more specific reason.
                        let error = if self.control.is_closing() {
                            DatagramError::SocketClosed
                        } else {
                            DatagramError::DeadlineExceeded
                        };
                        (
                            buffer_error(error, buffer, 0, DatagramEffect::NotApplied),
                            false,
                        )
                    }
                    Ok(DatagramReceiveAttempt::WouldBlock { buffer, .. }) => {
                        unreachable!(
                            "a blocking receive reported WouldBlock with {} buffer bytes",
                            buffer.len()
                        )
                    }
                    Err(failure) => {
                        let retire = self.ring.is_poisoned();
                        self.finish_receive(original_len, Err(failure), false, retire)
                    }
                }
            }
        }
    }

    fn finish_receive(
        &self,
        original_len: usize,
        result: Result<OwnedDatagramReceive, OwnedDatagramFailure>,
        nonblocking: bool,
        retire: bool,
    ) -> (RecvCompletion, bool) {
        match result {
            Ok(received) => {
                let truncation = if received.transferred == received.datagram_len {
                    DatagramTruncation::Complete
                } else {
                    DatagramTruncation::Truncated
                };
                (
                    Ok(RecvFromResult {
                        buffer: received.buffer,
                        bytes_received: received.transferred,
                        datagram_len: received.datagram_len,
                        source: received.source,
                        truncation,
                    }),
                    retire,
                )
            }
            Err(mut failure) => {
                failure.buffer.truncate(original_len);
                let error = map_data_error(
                    DatagramOperationKind::RecvFrom,
                    &failure.error,
                    self.control.is_closing(),
                    nonblocking,
                );
                (
                    buffer_error(
                        error,
                        failure.buffer,
                        failure.bytes_transferred,
                        failure.effect,
                    ),
                    retire,
                )
            }
        }
    }

    fn drain_closed(&mut self) {
        while let Ok(command) = self.receiver.try_recv() {
            let ReceiveCommand {
                request,
                response,
                permit,
                mode: _,
            } = command.into_inner();
            drop(permit);
            response.complete(buffer_error(
                DatagramError::SocketClosed,
                request.buffer,
                0,
                DatagramEffect::NotApplied,
            ));
        }
    }
}

struct SocketControl {
    state: Mutex<SocketControlState>,
    close_waiter_limit: usize,
    /// Used only to retire an armed receive. Cancelling is what replaces the
    /// old close-observation park, so the close path needs its own way to
    /// reach the reactor.
    ring: Ring,
}

struct SocketControlState {
    phase: SocketPhase,
    terminal_error: Option<DatagramError>,
    receive_in_use: bool,
    /// The in-flight receive a close must retire, if one is armed.
    receive_cancel: Option<CancelToken>,
    actors_remaining: usize,
    /// Command channels held here rather than on the socket handle so that
    /// close can disconnect them. Dropping a sender is what wakes an actor
    /// blocked on its command queue; nothing parks waiting to be told.
    send_sender: Option<SyncSender<SendActorMessage>>,
    receive_sender: Option<SyncSender<ReceiveActorMessage>>,
    close_waiters: Vec<Responder<ControlCompletion>>,
    socket_permit: Option<ResourcePermit>,
}

#[derive(Clone, Copy, Eq, PartialEq)]
enum SocketPhase {
    Open,
    Closing,
    Closed,
}

impl SocketControl {
    fn new(
        close_waiter_limit: usize,
        actors: usize,
        socket_permit: ResourcePermit,
        ring: Ring,
    ) -> Self {
        Self {
            ring,
            state: Mutex::new(SocketControlState {
                phase: SocketPhase::Open,
                terminal_error: None,
                receive_in_use: false,
                receive_cancel: None,
                actors_remaining: actors,
                send_sender: None,
                receive_sender: None,
                close_waiters: Vec::new(),
                socket_permit: Some(socket_permit),
            }),
            close_waiter_limit,
        }
    }

    fn register_send_sender(&self, sender: SyncSender<SendActorMessage>) {
        lock_unpoisoned(&self.state).send_sender = Some(sender);
    }

    fn register_receive_sender(&self, sender: SyncSender<ReceiveActorMessage>) {
        lock_unpoisoned(&self.state).receive_sender = Some(sender);
    }

    /// Drops both command channels so any actor blocked on one wakes.
    ///
    /// The senders are dropped by the caller outside the state lock. An actor
    /// that wakes takes that lock immediately to read its phase, so releasing
    /// it first keeps the wake from contending with the close that caused it.
    fn take_senders(
        state: &mut SocketControlState,
    ) -> (
        Option<SyncSender<SendActorMessage>>,
        Option<SyncSender<ReceiveActorMessage>>,
    ) {
        (state.send_sender.take(), state.receive_sender.take())
    }

    fn try_admit_send(&self, command: SendCommand) -> Result<(), (SendCommand, DatagramError)> {
        let state = lock_unpoisoned(&self.state);
        if state.phase != SocketPhase::Open {
            return Err((command, terminal_admission_error(&state)));
        }
        let Some(sender) = state.send_sender.as_ref() else {
            return Err((command, terminal_admission_error(&state)));
        };
        // The send itself wakes an actor blocked on the queue; there is no
        // separate notification to deliver.
        match sender.try_send(TerminalCommand::new(command)) {
            Ok(()) => Ok(()),
            Err(TrySendError::Full(command)) => Err((
                command.into_inner(),
                DatagramError::ResourceExhausted {
                    resource: "io_uring datagram send command queue",
                    limit: self.close_waiter_limit,
                },
            )),
            Err(TrySendError::Disconnected(command)) => {
                Err((command.into_inner(), DatagramError::DriverStopped))
            }
        }
    }

    fn try_admit_receive(
        self: &Arc<Self>,
        mut command: ReceiveCommand,
    ) -> Result<(), (ReceiveCommand, DatagramError)> {
        let mut state = lock_unpoisoned(&self.state);
        if state.phase != SocketPhase::Open {
            return Err((command, terminal_admission_error(&state)));
        }
        if state.receive_in_use {
            return Err((
                command,
                DatagramError::ResourceExhausted {
                    resource: "io_uring datagram concurrent receives",
                    limit: RECEIVE_LIMIT,
                },
            ));
        }
        let Some(sender) = state.receive_sender.clone() else {
            return Err((command, terminal_admission_error(&state)));
        };
        state.receive_in_use = true;
        command.permit = Some(ReceivePermit {
            control: Arc::clone(self),
        });
        match sender.try_send(TerminalCommand::new(command)) {
            Ok(()) => Ok(()),
            Err(TrySendError::Full(command)) => Err((
                command.into_inner(),
                DatagramError::ResourceExhausted {
                    resource: "io_uring datagram receive command queue",
                    limit: RECEIVE_LIMIT,
                },
            )),
            Err(TrySendError::Disconnected(command)) => {
                Err((command.into_inner(), DatagramError::DriverStopped))
            }
        }
    }

    fn release_receive(&self) {
        let mut state = lock_unpoisoned(&self.state);
        debug_assert!(state.receive_in_use, "datagram receive permit underflowed");
        state.receive_in_use = false;
    }

    /// Publishes an in-flight receive so a later close can retire it.
    ///
    /// Returns `false` when the socket already left `Open`, which means no
    /// close will ever observe this token and the caller must cancel the
    /// receive itself. Deciding that under the same lock that `request_close`
    /// takes is what makes the two paths exclusive: a close either sees the
    /// armed token or is seen here, never neither.
    fn arm_receive_cancel(&self, token: CancelToken) -> bool {
        let mut state = lock_unpoisoned(&self.state);
        if state.phase != SocketPhase::Open {
            return false;
        }
        debug_assert!(
            state.receive_cancel.is_none(),
            "a second receive armed while one was already in flight"
        );
        state.receive_cancel = Some(token);
        true
    }

    fn disarm_receive_cancel(&self) {
        lock_unpoisoned(&self.state).receive_cancel = None;
    }

    /// Retires an armed receive, if any, outside the state lock.
    ///
    /// The cancel is queued rather than awaited. Close reaches here from
    /// `submit_close`, which must hand back a future without performing I/O:
    /// blocking here for the cancel's CQE made a caller unable to select on or
    /// time out the very operation it had just been handed, and blocked the
    /// cold facade's executor during first-poll submission. Nothing is lost by
    /// not waiting — the cancel result reports only whether the kernel found
    /// the target, while close waiters are released by the receive's own
    /// terminal CQE.
    ///
    /// Still taken outside the state lock: completing an operation can run a
    /// caller waker, which must never happen under the mutex that admission
    /// and completion also take.
    fn cancel_armed_receive(&self) {
        let token = lock_unpoisoned(&self.state).receive_cancel.take();
        if let Some(token) = token {
            self.ring.request_cancel(token);
        }
    }

    fn request_close(&self, response: Responder<ControlCompletion>) {
        let mut response = Some(response);
        let mut immediate = None;
        let senders = {
            let mut state = lock_unpoisoned(&self.state);
            match state.phase {
                SocketPhase::Closed => {
                    immediate = Some(close_completion(state.terminal_error.clone()));
                    (None, None)
                }
                SocketPhase::Open | SocketPhase::Closing
                    if state.close_waiters.len() >= self.close_waiter_limit =>
                {
                    immediate = Some(no_buffer_error(DatagramError::ResourceExhausted {
                        resource: "io_uring datagram close waiters",
                        limit: self.close_waiter_limit,
                    }));
                    (None, None)
                }
                SocketPhase::Open | SocketPhase::Closing => {
                    state.phase = SocketPhase::Closing;
                    state
                        .close_waiters
                        .push(response.take().expect("close response is available"));
                    Self::take_senders(&mut state)
                }
            }
        };
        if let Some(completion) = immediate {
            response
                .take()
                .expect("immediate close response is available")
                .complete(completion);
        }
        // The phase is already Closing, so a receive arming from here on cancels
        // itself and this only has to retire one that armed earlier.
        self.cancel_armed_receive();
        drop(senders);
    }

    fn request_close_without_response(&self) {
        let senders = {
            let mut state = lock_unpoisoned(&self.state);
            if state.phase == SocketPhase::Open {
                state.phase = SocketPhase::Closing;
            }
            Self::take_senders(&mut state)
        };
        self.cancel_armed_receive();
        drop(senders);
    }

    fn is_closing(&self) -> bool {
        lock_unpoisoned(&self.state).phase != SocketPhase::Open
    }

    fn actor_never_started(&self) {
        self.actor_exited(Some(DatagramError::DriverStopped));
    }

    fn actor_exited(&self, error: Option<DatagramError>) {
        let (senders, completions) = {
            let mut state = lock_unpoisoned(&self.state);
            if let Some(error) = error {
                state.terminal_error.get_or_insert(error);
            }
            if state.phase == SocketPhase::Open {
                state.phase = SocketPhase::Closing;
                state
                    .terminal_error
                    .get_or_insert(DatagramError::DriverStopped);
            }
            debug_assert!(
                state.actors_remaining > 0,
                "datagram actor count underflowed"
            );
            state.actors_remaining = state.actors_remaining.saturating_sub(1);
            // This actor is leaving, so no further command it would have served
            // can be admitted. Disconnecting the queues is what tells a sibling
            // blocked on one; previously that relied on the sibling waking on
            // its own to notice.
            let senders = Self::take_senders(&mut state);
            let completions = if state.actors_remaining == 0 {
                state.phase = SocketPhase::Closed;
                let error = state.terminal_error.clone();
                let waiters = std::mem::take(&mut state.close_waiters);
                let socket_permit = state.socket_permit.take();
                Some((waiters, error, socket_permit))
            } else {
                None
            };
            (senders, completions)
        };
        drop(senders);
        // Disconnecting the queues wakes a sibling blocked on one, but a
        // sibling blocked in the ring on an armed receive is not waiting on a
        // queue: it is waiting for a peer. A send actor that exits — panicked,
        // or failed out by a poisoned ring — would otherwise leave that receive
        // armed for good, and every close waiter behind it.
        self.cancel_armed_receive();
        if let Some((waiters, error, socket_permit)) = completions {
            drop(socket_permit);
            for response in waiters {
                response.complete(close_completion(error.clone()));
            }
        }
    }
}

fn terminal_admission_error(state: &SocketControlState) -> DatagramError {
    state
        .terminal_error
        .clone()
        .unwrap_or(DatagramError::SocketClosed)
}

struct ReceivePermit {
    control: Arc<SocketControl>,
}

impl Drop for ReceivePermit {
    fn drop(&mut self) {
        self.control.release_receive();
    }
}

struct DatagramActorExit {
    control: Option<Arc<SocketControl>>,
}

impl DatagramActorExit {
    fn new(control: Arc<SocketControl>) -> Self {
        Self {
            control: Some(control),
        }
    }

    fn finish(mut self, error: Option<DatagramError>) {
        self.control
            .take()
            .expect("datagram actor exit guard is armed")
            .actor_exited(error);
    }
}

impl Drop for DatagramActorExit {
    fn drop(&mut self) {
        if let Some(control) = self.control.take() {
            control.actor_exited(Some(DatagramError::DriverStopped));
        }
    }
}

fn buffer_error<T>(
    error: DatagramError,
    buffer: Vec<u8>,
    bytes_transferred: usize,
    effect: DatagramEffect,
) -> CompletionResult<T, DatagramFailure> {
    let failure = DatagramFailure::with_buffer(error, buffer, bytes_transferred);
    Err(match effect {
        DatagramEffect::NotApplied => CompletionError::not_applied(failure),
        DatagramEffect::Applied => CompletionError::applied(failure),
        DatagramEffect::MayHaveApplied => CompletionError::may_have_applied(failure),
    })
}

fn no_buffer_error<T>(error: DatagramError) -> CompletionResult<T, DatagramFailure> {
    Err(CompletionError::not_applied(
        DatagramFailure::without_buffer(error),
    ))
}

fn close_completion(error: Option<DatagramError>) -> ControlCompletion {
    match error {
        None => Ok(()),
        Some(error) => Err(CompletionError::applied(DatagramFailure::without_buffer(
            error,
        ))),
    }
}

fn map_bind_error(error: &io::Error) -> DatagramError {
    match error.raw_os_error() {
        Some(libc::EADDRINUSE) => DatagramError::AddressInUse,
        Some(libc::EAFNOSUPPORT) => DatagramError::AddressFamilyMismatch,
        _ => backend(DatagramOperationKind::Bind, error),
    }
}

fn map_data_error(
    operation: DatagramOperationKind,
    error: &io::Error,
    closing: bool,
    nonblocking_receive: bool,
) -> DatagramError {
    if closing && matches!(error.raw_os_error(), Some(libc::EBADF | libc::ECANCELED)) {
        return DatagramError::SocketClosed;
    }
    match error.raw_os_error() {
        Some(libc::EMSGSIZE) => DatagramError::MessageTooLarge {
            max_payload_bytes: None,
        },
        Some(libc::EAFNOSUPPORT) => DatagramError::AddressFamilyMismatch,
        Some(code) if nonblocking_receive && code == libc::EAGAIN => DatagramError::WouldBlock,
        _ => backend(operation, error),
    }
}

fn backend(operation: DatagramOperationKind, error: &io::Error) -> DatagramError {
    DatagramError::Backend {
        operation,
        raw_os_error: error.raw_os_error(),
        message: error.to_string(),
    }
}

fn same_address_family(first: SocketAddr, second: SocketAddr) -> bool {
    matches!(
        (first, second),
        (SocketAddr::V4(_), SocketAddr::V4(_)) | (SocketAddr::V6(_), SocketAddr::V6(_))
    )
}

fn validate_config(config: UringDatagramConfig) -> Result<(), UringDatagramOpenError> {
    if config.command_queue_capacity == 0 {
        return invalid_config("command_queue_capacity", "must be nonzero");
    }
    if config.ring_entries < 4 || !config.ring_entries.is_power_of_two() {
        return invalid_config(
            "ring_entries",
            "must be a power of two and at least 4 for multiple queued I/O operations",
        );
    }
    if config.max_sockets == 0 {
        return invalid_config("max_sockets", "must be nonzero");
    }
    // Every bound socket may hold one armed receive, and each armed receive
    // owes a CQE. A provider whose sockets cannot all be armed at once is not
    // merely slower: the receives that find no slot wedge behind the ones that
    // did, and nothing completes to release them. Reject that shape here rather
    // than let it become a runtime hang.
    let max_armed_receives = config
        .max_sockets
        .checked_mul(RECEIVE_LIMIT)
        .and_then(|armed| armed.checked_add(config.ring_entries as usize));
    match max_armed_receives {
        Some(total) if total <= MAX_RING_IN_FLIGHT => {}
        Some(total) => {
            return invalid_config(
                "max_sockets",
                format!(
                    "{} sockets need {total} in-flight io_uring completions, over the \
                     {MAX_RING_IN_FLIGHT} one reactor can carry; lower max_sockets or run \
                     more providers",
                    config.max_sockets
                ),
            );
        }
        None => {
            return invalid_config("max_sockets", "in-flight completion capacity overflowed");
        }
    }
    if config.max_datagram_bytes == 0 || config.max_datagram_bytes > u32::MAX as usize {
        return invalid_config("max_datagram_bytes", format!("must be in 1..={}", u32::MAX));
    }
    if config.max_operation_bytes == 0 {
        return invalid_config("max_operation_bytes", "must be nonzero");
    }
    if config.max_datagram_bytes > config.max_operation_bytes {
        return invalid_config("max_datagram_bytes", "must not exceed max_operation_bytes");
    }
    Ok(())
}

fn invalid_config<T>(
    field: &'static str,
    message: impl Into<String>,
) -> Result<T, UringDatagramOpenError> {
    Err(UringDatagramOpenError::InvalidConfig {
        field,
        message: message.into(),
    })
}

fn open_io(action: &'static str, error: io::Error) -> UringDatagramOpenError {
    UringDatagramOpenError::Io {
        action,
        raw_os_error: error.raw_os_error(),
        message: error.to_string(),
    }
}

fn open_raw_os_error(error: &UringDatagramOpenError) -> Option<i32> {
    match error {
        UringDatagramOpenError::Io { raw_os_error, .. } => *raw_os_error,
        UringDatagramOpenError::InvalidConfig { .. } | UringDatagramOpenError::DriverStopped => {
            None
        }
    }
}

fn join_actor(join: &Mutex<Option<JoinHandle<()>>>) {
    join_if_other_thread(lock_unpoisoned(join).take());
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn config_rejects_unbounded_or_uninterruptible_values() {
        for config in [
            UringDatagramConfig {
                command_queue_capacity: 0,
                ..UringDatagramConfig::default()
            },
            UringDatagramConfig {
                ring_entries: 3,
                ..UringDatagramConfig::default()
            },
            UringDatagramConfig {
                max_sockets: 0,
                ..UringDatagramConfig::default()
            },
            UringDatagramConfig {
                max_datagram_bytes: 0,
                ..UringDatagramConfig::default()
            },
            UringDatagramConfig {
                max_operation_bytes: 0,
                ..UringDatagramConfig::default()
            },
            UringDatagramConfig {
                max_operation_bytes: UringDatagramConfig::default().max_datagram_bytes - 1,
                ..UringDatagramConfig::default()
            },
        ] {
            assert!(matches!(
                validate_config(config),
                Err(UringDatagramOpenError::InvalidConfig { .. })
            ));
        }
    }
}
