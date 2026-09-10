//! Thread-safe, deterministic, host-I/O-free byte-stream networking.

use std::collections::{BTreeMap, VecDeque};
use std::fmt;
use std::sync::{Arc, Mutex};

use super::vectored::cells::{SyncWriteCell, SyncWriteResponse};
use super::vectored::{WriteData, WriteOutput, contiguous, vectored};
use crate::completion::{SafeWaker, SyncCell, SyncPermit, SyncPermitPool, lock_unpoisoned};
use kr_runtime::{CompletionError, CompletionResult};

use super::{
    ByteStreamSubmit, ByteStreamVectoredSubmit, ConnectRequest, Direction, ListenRequest,
    MAX_WRITE_SEGMENTS, MemoryVectoredWriteOperation, NetworkAddress, NetworkError, NetworkFailure,
    NetworkListenerSubmit, NetworkProviderSubmit, ReadRequest, ReadResult, Side,
    VectoredWriteRequest, WriteRequest, WriteResult, incoming_direction, outgoing_direction,
};

/// Bounds for the thread-safe deterministic in-memory network.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct MemoryNetworkConfig {
    /// Maximum simultaneously bound listeners.
    pub max_listeners: usize,
    /// Maximum backlog accepted by one listener.
    pub max_listener_backlog: usize,
    /// Maximum live connected pairs.
    pub max_connections: usize,
    /// Maximum admitted operations without a consumed or abandoned terminal response.
    pub max_inflight_operations: usize,
    /// Byte capacity of each direction of each connection.
    pub directional_buffer_bytes: usize,
    /// Maximum caller-owned read result or write buffer admitted by one operation.
    pub max_operation_bytes: usize,
    /// Maximum bytes transferred by one successful read or write completion.
    pub max_chunk_bytes: usize,
}

impl Default for MemoryNetworkConfig {
    fn default() -> Self {
        Self {
            max_listeners: 1_024,
            max_listener_backlog: 1_024,
            max_connections: 1_024,
            max_inflight_operations: 4_096,
            directional_buffer_bytes: 256 * 1_024,
            max_operation_bytes: 256 * 1_024,
            max_chunk_bytes: 64 * 1_024,
        }
    }
}

/// Passive bounded diagnostic state for [`MemoryNetwork`].
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct MemoryNetworkStatus {
    /// Live bound listeners.
    pub listeners: usize,
    /// Live connected pairs, including half-closed pairs.
    pub connections: usize,
    /// Operations awaiting terminalization or consumption of their response.
    pub inflight_operations: usize,
    /// Server-side connections waiting in listener backlogs.
    pub queued_connections: usize,
    /// Accept operations waiting for a connection.
    pub pending_accepts: usize,
    /// Bytes retained across both directions of all connections.
    pub buffered_bytes: usize,
    /// Read operations waiting for bytes, EOF, or close.
    pub pending_reads: usize,
    /// Write operations waiting for directional buffer capacity.
    pub pending_writes: usize,
}

/// A cloneable deterministic provider whose handles and futures are `Send`.
///
/// The provider performs no host I/O and starts no worker threads. Each method
/// applies or queues its operation before returning. Sequential call traces are
/// fully deterministic; concurrent calls are linearized in mutex-acquisition
/// order. Per-direction bytes, listener backlogs, connections, listeners, and
/// all pending or unconsumed operation responses are bounded by configuration.
#[derive(Clone)]
pub struct MemoryNetwork {
    state: Arc<Mutex<State>>,
}

impl MemoryNetwork {
    /// Creates an empty provider.
    ///
    /// # Errors
    ///
    /// Returns [`NetworkError::InvalidConfig`] when any required bound is zero.
    pub fn new(config: MemoryNetworkConfig) -> Result<Self, NetworkError> {
        validate_config(config)?;
        Ok(Self {
            state: Arc::new(Mutex::new(State {
                config,
                permits: Arc::new(SyncPermitPool::new(config.max_inflight_operations)),
                next_connection_id: 0,
                next_listener_id: 0,
                connections: BTreeMap::new(),
                listeners: BTreeMap::new(),
                bindings: BTreeMap::new(),
                client_bindings: BTreeMap::new(),
            })),
        })
    }

    /// Creates a connected in-memory pair without binding a listener.
    ///
    /// # Errors
    ///
    /// Returns a typed resource or identifier error when the pair cannot be
    /// admitted.
    pub fn connected_pair(&self) -> Result<(MemoryStream, MemoryStream), NetworkError> {
        transact(&self.state, |state, _actions| {
            let connection = state.create_connection(None)?;
            Ok((
                MemoryStream::new(Arc::clone(&self.state), connection, Side::Left),
                MemoryStream::new(Arc::clone(&self.state), connection, Side::Right),
            ))
        })
    }

    /// Returns a passive, bounded state snapshot without driving operations.
    #[must_use]
    pub fn status(&self) -> MemoryNetworkStatus {
        let state = lock_unpoisoned(&self.state);
        state.status()
    }
}

impl NetworkProviderSubmit for MemoryNetwork {
    type Address = NetworkAddress;
    type Stream = MemoryStream;
    type Listener = MemoryListener;
    type ListenResponse = MemoryOperation<CompletionResult<MemoryListener, NetworkFailure>>;
    type ConnectResponse = MemoryOperation<CompletionResult<MemoryStream, NetworkFailure>>;

    fn submit_listen(&self, request: ListenRequest) -> Self::ListenResponse {
        State::submit_listen(&self.state, request)
    }

    fn submit_connect(&self, request: ConnectRequest) -> Self::ConnectResponse {
        State::submit_connect(&self.state, request)
    }
}

/// One exclusive in-memory address binding.
pub struct MemoryListener {
    state: Arc<Mutex<State>>,
    listener: u64,
    address: NetworkAddress,
}

impl fmt::Debug for MemoryListener {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("MemoryListener")
            .field("listener", &self.listener)
            .field("address", &self.address)
            .finish_non_exhaustive()
    }
}

impl NetworkListenerSubmit for MemoryListener {
    type Address = NetworkAddress;
    type Stream = MemoryStream;
    type AcceptResponse = MemoryOperation<CompletionResult<MemoryStream, NetworkFailure>>;
    type CloseResponse = MemoryOperation<CompletionResult<(), NetworkFailure>>;

    fn local_address(&self) -> NetworkAddress {
        self.address
    }

    fn submit_accept(&self) -> Self::AcceptResponse {
        State::submit_accept(&self.state, self.listener)
    }

    fn submit_close(&self) -> Self::CloseResponse {
        State::submit_listener_close(&self.state, self.listener)
    }
}

impl Drop for MemoryListener {
    fn drop(&mut self) {
        transact(&self.state, |state, actions| {
            state.close_listener_immediately(self.listener, actions);
        });
    }
}

/// One endpoint of an in-memory connected pair.
pub struct MemoryStream {
    state: Arc<Mutex<State>>,
    connection: u64,
    side: Side,
}

impl MemoryStream {
    fn new(state: Arc<Mutex<State>>, connection: u64, side: Side) -> Self {
        Self {
            state,
            connection,
            side,
        }
    }
}

impl fmt::Debug for MemoryStream {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("MemoryStream")
            .field("connection", &self.connection)
            .field("side", &self.side)
            .finish_non_exhaustive()
    }
}

impl ByteStreamSubmit for MemoryStream {
    type ReadResponse = MemoryOperation<CompletionResult<ReadResult, NetworkFailure>>;
    type WriteResponse = MemoryOperation<CompletionResult<WriteResult, NetworkFailure>>;
    type ControlResponse = MemoryOperation<CompletionResult<(), NetworkFailure>>;

    fn submit_read(&self, request: ReadRequest) -> Self::ReadResponse {
        State::submit_read(&self.state, self.connection, self.side, request)
    }

    fn submit_write(&self, request: WriteRequest) -> Self::WriteResponse {
        State::submit_write(&self.state, self.connection, self.side, request.into())
            .into_contiguous()
    }

    fn submit_shutdown_write(&self) -> Self::ControlResponse {
        State::submit_shutdown_write(&self.state, self.connection, self.side)
    }

    fn submit_close(&self) -> Self::ControlResponse {
        State::submit_close(&self.state, self.connection, self.side)
    }
}

impl ByteStreamVectoredSubmit for MemoryStream {
    type WriteVectoredResponse = MemoryVectoredWriteOperation;
    fn max_segments(&self) -> usize {
        MAX_WRITE_SEGMENTS
    }
    fn submit_write_vectored(&self, request: VectoredWriteRequest) -> Self::WriteVectoredResponse {
        State::submit_write(&self.state, self.connection, self.side, request.into()).into_vectored()
    }
}

impl Drop for MemoryStream {
    fn drop(&mut self) {
        transact(&self.state, |state, actions| {
            state.close_side_immediately(self.connection, self.side, actions);
        });
    }
}

/// An owned in-memory operation response.
///
/// Polling after completion panics. Dropping a pending response never cancels
/// its admitted operation; provider state retains it until terminal completion.
pub type MemoryOperation<T> = crate::completion::SyncOperation<T>;

trait KeepAlive: Send + Sync {}

impl<T> KeepAlive for T where T: Send + Sync {}

#[derive(Default)]
struct Actions {
    wakers: Vec<SafeWaker>,
    keep_alive: Vec<Arc<dyn KeepAlive>>,
}

impl Actions {
    fn complete<T>(&mut self, cell: &Arc<SyncCell<T>>, output: T)
    where
        T: Send + 'static,
    {
        if let Some(waker) = cell.set_output(output) {
            self.wakers.push(waker);
        }
        let erased: Arc<dyn KeepAlive> = cell.clone();
        self.keep_alive.push(erased);
    }

    fn complete_write(&mut self, cell: &SyncWriteCell, output: WriteOutput) {
        match cell {
            SyncWriteCell::Contiguous(cell) => self.complete(cell, contiguous(output)),
            SyncWriteCell::Vectored(cell) => self.complete(cell, vectored(output)),
        }
    }

    fn run(mut self) {
        for waker in self.wakers.drain(..) {
            waker.wake();
        }
        drop(self.keep_alive);
    }
}

fn transact<R>(
    state: &Arc<Mutex<State>>,
    operation: impl FnOnce(&mut State, &mut Actions) -> R,
) -> R {
    let mut actions = Actions::default();
    let result = {
        let mut state = lock_unpoisoned(state);
        operation(&mut state, &mut actions)
    };
    actions.run();
    result
}

struct State {
    config: MemoryNetworkConfig,
    permits: Arc<SyncPermitPool>,
    next_connection_id: u64,
    next_listener_id: u64,
    connections: BTreeMap<u64, Connection>,
    listeners: BTreeMap<u64, ListenerState>,
    bindings: BTreeMap<NetworkAddress, u64>,
    client_bindings: BTreeMap<NetworkAddress, u64>,
}

struct ListenerState {
    address: NetworkAddress,
    backlog: usize,
    queued_connections: VecDeque<u64>,
    pending_accepts: VecDeque<PendingAccept>,
}

struct PendingAccept {
    cell: Arc<SyncCell<CompletionResult<MemoryStream, NetworkFailure>>>,
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
}

struct PendingRead {
    request: ReadRequest,
    cell: Arc<SyncCell<CompletionResult<ReadResult, NetworkFailure>>>,
}

struct PendingWrite {
    request: WriteData,
    cell: SyncWriteCell,
}

impl State {
    fn status(&self) -> MemoryNetworkStatus {
        let mut queued_connections = 0;
        let mut pending_accepts = 0;
        for listener in self.listeners.values() {
            queued_connections += listener.queued_connections.len();
            pending_accepts += listener.pending_accepts.len();
        }
        let mut buffered_bytes = 0;
        let mut pending_reads = 0;
        let mut pending_writes = 0;
        for connection in self.connections.values() {
            for pipe in [&connection.left_to_right, &connection.right_to_left] {
                buffered_bytes += pipe.bytes.len();
                pending_reads += pipe.pending_reads.len();
                pending_writes += pipe.pending_writes.len();
            }
        }
        MemoryNetworkStatus {
            listeners: self.listeners.len(),
            connections: self.connections.len(),
            inflight_operations: self.permits.in_use(),
            queued_connections,
            pending_accepts,
            buffered_bytes,
            pending_reads,
            pending_writes,
        }
    }

    fn acquire(&self) -> Result<SyncPermit, NetworkError> {
        self.permits
            .acquire()
            .ok_or(NetworkError::ResourceExhausted {
                resource: "inflight operations",
                limit: self.config.max_inflight_operations,
            })
    }

    fn create_connection(
        &mut self,
        client_binding: Option<NetworkAddress>,
    ) -> Result<u64, NetworkError> {
        if self.connections.len() >= self.config.max_connections {
            return Err(NetworkError::ResourceExhausted {
                resource: "connections",
                limit: self.config.max_connections,
            });
        }
        let connection = self.next_connection_id;
        self.next_connection_id = connection
            .checked_add(1)
            .ok_or(NetworkError::IdentifierExhausted)?;
        self.connections.insert(
            connection,
            Connection {
                left: Endpoint {
                    alive: true,
                    read_open: true,
                    write_open: true,
                },
                right: Endpoint {
                    alive: true,
                    read_open: true,
                    write_open: true,
                },
                client_binding,
                left_to_right: Pipe::new(self.config.directional_buffer_bytes),
                right_to_left: Pipe::new(self.config.directional_buffer_bytes),
            },
        );
        Ok(connection)
    }

    fn submit_listen(
        state: &Arc<Mutex<Self>>,
        request: ListenRequest,
    ) -> MemoryOperation<CompletionResult<MemoryListener, NetworkFailure>> {
        transact(state, |inner, actions| {
            if request.backlog == 0 || request.backlog > inner.config.max_listener_backlog {
                return MemoryOperation::ready(Err(CompletionError::not_applied(
                    NetworkFailure::without_buffer(NetworkError::InvalidRequest {
                        reason: "listener backlog must be within the configured nonzero bound",
                    }),
                )));
            }
            let permit = match inner.acquire() {
                Ok(permit) => permit,
                Err(error) => return MemoryOperation::ready(operation_failure(error)),
            };
            let (future, cell) = MemoryOperation::pending(Some(permit));
            let output = if inner.bindings.contains_key(&request.address)
                || inner.client_bindings.contains_key(&request.address)
            {
                Err(CompletionError::not_applied(
                    NetworkFailure::without_buffer(NetworkError::AddressInUse),
                ))
            } else if inner.listeners.len() >= inner.config.max_listeners {
                Err(CompletionError::not_applied(
                    NetworkFailure::without_buffer(NetworkError::ResourceExhausted {
                        resource: "listeners",
                        limit: inner.config.max_listeners,
                    }),
                ))
            } else {
                let listener = inner.next_listener_id;
                let Some(next) = listener.checked_add(1) else {
                    actions.complete(
                        &cell,
                        Err(CompletionError::not_applied(
                            NetworkFailure::without_buffer(NetworkError::IdentifierExhausted),
                        )),
                    );
                    return future;
                };
                inner.next_listener_id = next;
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
                Ok(MemoryListener {
                    state: Arc::clone(state),
                    listener,
                    address: request.address,
                })
            };
            actions.complete(&cell, output);
            future
        })
    }

    fn submit_connect(
        state: &Arc<Mutex<Self>>,
        request: ConnectRequest,
    ) -> MemoryOperation<CompletionResult<MemoryStream, NetworkFailure>> {
        transact(state, |inner, actions| {
            let permit = match inner.acquire() {
                Ok(permit) => permit,
                Err(error) => return MemoryOperation::ready(operation_failure(error)),
            };
            let (future, cell) = MemoryOperation::pending(Some(permit));
            let rejected = if inner.bindings.contains_key(&request.local)
                || inner.client_bindings.contains_key(&request.local)
            {
                Some(NetworkError::AddressInUse)
            } else {
                let Some(listener_id) = inner.bindings.get(&request.remote).copied() else {
                    actions.complete(
                        &cell,
                        Err(CompletionError::not_applied(
                            NetworkFailure::without_buffer(NetworkError::ConnectionRefused),
                        )),
                    );
                    return future;
                };
                let listener = inner
                    .listeners
                    .get(&listener_id)
                    .expect("binding references a live listener");
                if listener.pending_accepts.is_empty()
                    && listener.queued_connections.len() >= listener.backlog
                {
                    Some(NetworkError::BacklogFull {
                        capacity: listener.backlog,
                    })
                } else if inner.connections.len() >= inner.config.max_connections {
                    Some(NetworkError::ResourceExhausted {
                        resource: "connections",
                        limit: inner.config.max_connections,
                    })
                } else {
                    None
                }
            };
            if let Some(error) = rejected {
                actions.complete(
                    &cell,
                    Err(CompletionError::not_applied(
                        NetworkFailure::without_buffer(error),
                    )),
                );
                return future;
            }

            let listener_id = inner.bindings[&request.remote];
            let connection = match inner.create_connection(Some(request.local)) {
                Ok(connection) => connection,
                Err(error) => {
                    actions.complete(
                        &cell,
                        Err(CompletionError::not_applied(
                            NetworkFailure::without_buffer(error),
                        )),
                    );
                    return future;
                }
            };
            inner.client_bindings.insert(request.local, connection);
            let pending_accept = inner
                .listeners
                .get_mut(&listener_id)
                .expect("binding references a live listener")
                .pending_accepts
                .pop_front();
            if let Some(accept) = pending_accept {
                actions.complete(
                    &accept.cell,
                    Ok(MemoryStream::new(
                        Arc::clone(state),
                        connection,
                        Side::Right,
                    )),
                );
            } else {
                inner
                    .listeners
                    .get_mut(&listener_id)
                    .expect("binding references a live listener")
                    .queued_connections
                    .push_back(connection);
            }
            actions.complete(
                &cell,
                Ok(MemoryStream::new(Arc::clone(state), connection, Side::Left)),
            );
            future
        })
    }

    fn submit_accept(
        state: &Arc<Mutex<Self>>,
        listener: u64,
    ) -> MemoryOperation<CompletionResult<MemoryStream, NetworkFailure>> {
        transact(state, |inner, actions| {
            let permit = match inner.acquire() {
                Ok(permit) => permit,
                Err(error) => return MemoryOperation::ready(operation_failure(error)),
            };
            let (future, cell) = MemoryOperation::pending(Some(permit));
            let Some(listener_state) = inner.listeners.get_mut(&listener) else {
                actions.complete(
                    &cell,
                    Err(CompletionError::not_applied(
                        NetworkFailure::without_buffer(NetworkError::ListenerClosed),
                    )),
                );
                return future;
            };
            if let Some(connection) = listener_state.queued_connections.pop_front() {
                actions.complete(
                    &cell,
                    Ok(MemoryStream::new(
                        Arc::clone(state),
                        connection,
                        Side::Right,
                    )),
                );
            } else {
                listener_state.pending_accepts.push_back(PendingAccept {
                    cell: Arc::clone(&cell),
                });
            }
            future
        })
    }

    fn submit_listener_close(
        state: &Arc<Mutex<Self>>,
        listener: u64,
    ) -> MemoryOperation<CompletionResult<(), NetworkFailure>> {
        transact(state, |inner, actions| {
            let permit = match inner.acquire() {
                Ok(permit) => permit,
                Err(error) => return MemoryOperation::ready(operation_failure(error)),
            };
            let (future, cell) = MemoryOperation::pending(Some(permit));
            inner.close_listener_immediately(listener, actions);
            actions.complete(&cell, Ok(()));
            future
        })
    }

    fn submit_read(
        state: &Arc<Mutex<Self>>,
        connection: u64,
        side: Side,
        request: ReadRequest,
    ) -> MemoryOperation<CompletionResult<ReadResult, NetworkFailure>> {
        let Some(result_len) = request.buffer.len().checked_add(request.max_bytes) else {
            return MemoryOperation::ready(transfer_failure(
                NetworkError::InvalidRequest {
                    reason: "read result size overflowed",
                },
                request.buffer,
            ));
        };
        transact(state, |inner, actions| {
            if request.buffer.capacity() > inner.config.max_operation_bytes
                || result_len > inner.config.max_operation_bytes
            {
                return MemoryOperation::ready(transfer_failure(
                    NetworkError::InvalidRequest {
                        reason: "read request exceeds max_operation_bytes",
                    },
                    request.buffer,
                ));
            }
            let permit = match inner.acquire() {
                Ok(permit) => permit,
                Err(error) => {
                    return MemoryOperation::ready(transfer_failure(error, request.buffer));
                }
            };
            let Some(connection_state) = inner.connections.get(&connection) else {
                return MemoryOperation::ready(transfer_failure(
                    NetworkError::ConnectionClosed,
                    request.buffer,
                ));
            };
            if !endpoint(connection_state, side).read_open {
                return MemoryOperation::ready(transfer_failure(
                    NetworkError::ConnectionClosed,
                    request.buffer,
                ));
            }
            let (future, cell) = MemoryOperation::pending(Some(permit));
            inner
                .connections
                .get_mut(&connection)
                .expect("connection was checked above")
                .pipe_mut(incoming_direction(side))
                .pending_reads
                .push_back(PendingRead {
                    request,
                    cell: Arc::clone(&cell),
                });
            inner.service_direction(connection, incoming_direction(side), actions);
            future
        })
    }

    fn submit_write(
        state: &Arc<Mutex<Self>>,
        connection: u64,
        side: Side,
        request: WriteData,
    ) -> SyncWriteResponse {
        transact(state, |inner, actions| {
            if let Err(error) = request.validate(inner.config.max_operation_bytes) {
                return SyncWriteResponse::ready(Err(CompletionError::not_applied(
                    request.failure(error, 0),
                )));
            }
            let permit = match inner.acquire() {
                Ok(permit) => permit,
                Err(error) => {
                    return SyncWriteResponse::ready(Err(CompletionError::not_applied(
                        request.failure(error, 0),
                    )));
                }
            };
            let Some(connection_state) = inner.connections.get(&connection) else {
                return SyncWriteResponse::ready(Err(CompletionError::not_applied(
                    request.failure(NetworkError::ConnectionClosed, 0),
                )));
            };
            let local = endpoint(connection_state, side);
            let peer = endpoint(connection_state, side.other());
            if !local.alive || !peer.alive || !peer.read_open {
                return SyncWriteResponse::ready(Err(CompletionError::not_applied(
                    request.failure(NetworkError::ConnectionClosed, 0),
                )));
            }
            if !local.write_open {
                return SyncWriteResponse::ready(Err(CompletionError::not_applied(
                    request.failure(NetworkError::WriteClosed, 0),
                )));
            }
            if inner
                .connections
                .get_mut(&connection)
                .expect("connection was checked above")
                .pipe_mut(outgoing_direction(side))
                .pending_writes
                .try_reserve(1)
                .is_err()
            {
                return SyncWriteResponse::ready(Err(CompletionError::not_applied(
                    request.failure(
                        NetworkError::ResourceExhausted {
                            resource: "pending write allocation",
                            limit: inner.config.max_inflight_operations,
                        },
                        0,
                    ),
                )));
            }
            let (future, cell) = SyncWriteResponse::pending(permit, request.is_vectored());
            inner
                .connections
                .get_mut(&connection)
                .expect("connection was checked above")
                .pipe_mut(outgoing_direction(side))
                .pending_writes
                .push_back(PendingWrite { request, cell });
            inner.service_direction(connection, outgoing_direction(side), actions);
            future
        })
    }

    fn submit_shutdown_write(
        state: &Arc<Mutex<Self>>,
        connection: u64,
        side: Side,
    ) -> MemoryOperation<CompletionResult<(), NetworkFailure>> {
        transact(state, |inner, actions| {
            let permit = match inner.acquire() {
                Ok(permit) => permit,
                Err(error) => return MemoryOperation::ready(operation_failure(error)),
            };
            let Some(connection_state) = inner.connections.get(&connection) else {
                return MemoryOperation::ready(operation_failure(NetworkError::ConnectionClosed));
            };
            if !endpoint(connection_state, side).alive {
                return MemoryOperation::ready(operation_failure(NetworkError::ConnectionClosed));
            }
            let (future, cell) = MemoryOperation::pending(Some(permit));
            let pending = {
                let connection = inner
                    .connections
                    .get_mut(&connection)
                    .expect("connection was checked above");
                endpoint_mut(connection, side).write_open = false;
                let pipe = connection.pipe_mut(outgoing_direction(side));
                pipe.sender_closed = true;
                std::mem::take(&mut pipe.pending_writes)
            };
            complete_pending_writes(pending, NetworkError::WriteClosed, actions);
            inner.service_direction(connection, outgoing_direction(side), actions);
            actions.complete(&cell, Ok(()));
            future
        })
    }

    fn submit_close(
        state: &Arc<Mutex<Self>>,
        connection: u64,
        side: Side,
    ) -> MemoryOperation<CompletionResult<(), NetworkFailure>> {
        transact(state, |inner, actions| {
            let permit = match inner.acquire() {
                Ok(permit) => permit,
                Err(error) => return MemoryOperation::ready(operation_failure(error)),
            };
            let (future, cell) = MemoryOperation::pending(Some(permit));
            inner.close_side_immediately(connection, side, actions);
            actions.complete(&cell, Ok(()));
            future
        })
    }

    fn close_listener_immediately(&mut self, listener: u64, actions: &mut Actions) {
        let Some(listener) = self.listeners.remove(&listener) else {
            return;
        };
        self.bindings.remove(&listener.address);
        for accept in listener.pending_accepts {
            actions.complete(
                &accept.cell,
                Err(CompletionError::not_applied(
                    NetworkFailure::without_buffer(NetworkError::ListenerClosed),
                )),
            );
        }
        for connection in listener.queued_connections {
            self.close_side_immediately(connection, Side::Right, actions);
        }
    }

    fn close_side_immediately(&mut self, connection: u64, side: Side, actions: &mut Actions) {
        let Some(connection_state) = self.connections.get_mut(&connection) else {
            return;
        };
        if !endpoint(connection_state, side).alive {
            return;
        }
        let released_client_binding = if side == Side::Left {
            connection_state.client_binding.take()
        } else {
            None
        };
        let (outgoing_writes, incoming_writes, incoming_reads) = {
            let local = endpoint_mut(connection_state, side);
            local.alive = false;
            local.read_open = false;
            local.write_open = false;
            let outgoing = connection_state.pipe_mut(outgoing_direction(side));
            outgoing.sender_closed = true;
            let outgoing_writes = std::mem::take(&mut outgoing.pending_writes);
            let incoming = connection_state.pipe_mut(incoming_direction(side));
            incoming.receiver_open = false;
            incoming.bytes.clear();
            (
                outgoing_writes,
                std::mem::take(&mut incoming.pending_writes),
                std::mem::take(&mut incoming.pending_reads),
            )
        };
        if let Some(address) = released_client_binding {
            self.client_bindings.remove(&address);
        }
        complete_pending_writes(outgoing_writes, NetworkError::ConnectionClosed, actions);
        complete_pending_writes(incoming_writes, NetworkError::ConnectionClosed, actions);
        for read in incoming_reads {
            actions.complete(
                &read.cell,
                transfer_failure(NetworkError::ConnectionClosed, read.request.buffer),
            );
        }
        self.service_direction(connection, outgoing_direction(side), actions);
        let remove = self
            .connections
            .get(&connection)
            .is_some_and(|connection| !connection.left.alive && !connection.right.alive);
        if remove {
            self.connections.remove(&connection);
        }
    }

    fn service_direction(&mut self, connection: u64, direction: Direction, actions: &mut Actions) {
        loop {
            let Some(connection_state) = self.connections.get(&connection) else {
                return;
            };
            let pipe = connection_state.pipe(direction);
            let read_ready = pipe.pending_reads.front().is_some_and(|read| {
                !pipe.receiver_open
                    || !pipe.bytes.is_empty()
                    || pipe.sender_closed
                    || read.request.max_bytes == 0
            });
            let write_ready = pipe.pending_writes.front().is_some_and(|write| {
                pipe.receiver_open
                    && !pipe.sender_closed
                    && (write.request.is_empty() || pipe.bytes.len() < pipe.capacity)
            });
            let receiver_open = pipe.receiver_open;
            let sender_closed = pipe.sender_closed;

            if read_ready {
                let mut read = self
                    .connections
                    .get_mut(&connection)
                    .expect("connection still exists")
                    .pipe_mut(direction)
                    .pending_reads
                    .pop_front()
                    .expect("ready read was observed");
                if !receiver_open {
                    actions.complete(
                        &read.cell,
                        transfer_failure(NetworkError::ConnectionClosed, read.request.buffer),
                    );
                    continue;
                }
                let bytes_read = {
                    let pipe = self
                        .connections
                        .get_mut(&connection)
                        .expect("connection still exists")
                        .pipe_mut(direction);
                    let bytes_read = pipe
                        .bytes
                        .len()
                        .min(read.request.max_bytes)
                        .min(self.config.max_chunk_bytes);
                    read.request.buffer.extend(pipe.bytes.drain(..bytes_read));
                    bytes_read
                };
                let pipe = self
                    .connections
                    .get(&connection)
                    .expect("connection still exists")
                    .pipe(direction);
                let end_of_stream = read.request.max_bytes != 0
                    && bytes_read == 0
                    && pipe.sender_closed
                    && pipe.bytes.is_empty();
                actions.complete(
                    &read.cell,
                    Ok(ReadResult {
                        buffer: read.request.buffer,
                        bytes_read,
                        end_of_stream,
                    }),
                );
                continue;
            }

            if write_ready {
                let write = self
                    .connections
                    .get_mut(&connection)
                    .expect("connection still exists")
                    .pipe_mut(direction)
                    .pending_writes
                    .pop_front()
                    .expect("ready write was observed");
                let progress = {
                    let pipe = self
                        .connections
                        .get_mut(&connection)
                        .expect("connection still exists")
                        .pipe_mut(direction);
                    let available = pipe.capacity - pipe.bytes.len();
                    let bytes_written = write
                        .request
                        .len()
                        .min(available)
                        .min(self.config.max_chunk_bytes);
                    write
                        .request
                        .append_prefix(&mut pipe.bytes, bytes_written)
                        .map(|()| bytes_written)
                };
                actions.complete_write(
                    &write.cell,
                    match progress {
                        Ok(bytes_written) => Ok(write.request.success(bytes_written)),
                        Err(error) => Err(CompletionError::not_applied(
                            write.request.failure(error, 0),
                        )),
                    },
                );
                continue;
            }

            if !receiver_open || sender_closed {
                let Some(write) = self
                    .connections
                    .get_mut(&connection)
                    .expect("connection still exists")
                    .pipe_mut(direction)
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
                actions.complete_write(
                    &write.cell,
                    Err(CompletionError::not_applied(
                        write.request.failure(error, 0),
                    )),
                );
                continue;
            }
            return;
        }
    }
}

impl Pipe {
    fn new(capacity: usize) -> Self {
        Self {
            // Grow only as writes arrive; `capacity` is still an exact hard
            // bound, without eagerly reserving every configured connection.
            bytes: VecDeque::new(),
            capacity,
            sender_closed: false,
            receiver_open: true,
            pending_reads: VecDeque::new(),
            pending_writes: VecDeque::new(),
        }
    }
}

impl Connection {
    fn pipe(&self, direction: Direction) -> &Pipe {
        match direction {
            Direction::LeftToRight => &self.left_to_right,
            Direction::RightToLeft => &self.right_to_left,
        }
    }

    fn pipe_mut(&mut self, direction: Direction) -> &mut Pipe {
        match direction {
            Direction::LeftToRight => &mut self.left_to_right,
            Direction::RightToLeft => &mut self.right_to_left,
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

fn complete_pending_writes(
    pending: VecDeque<PendingWrite>,
    error: NetworkError,
    actions: &mut Actions,
) {
    for write in pending {
        actions.complete_write(
            &write.cell,
            Err(CompletionError::not_applied(
                write.request.failure(error.clone(), 0),
            )),
        );
    }
}

fn operation_failure<T>(error: NetworkError) -> CompletionResult<T, NetworkFailure> {
    Err(CompletionError::not_applied(
        NetworkFailure::without_buffer(error),
    ))
}

fn transfer_failure<T>(
    error: NetworkError,
    buffer: Vec<u8>,
) -> CompletionResult<T, NetworkFailure> {
    Err(CompletionError::not_applied(NetworkFailure::with_buffer(
        error, buffer, 0,
    )))
}

fn validate_config(config: MemoryNetworkConfig) -> Result<(), NetworkError> {
    for (value, reason) in [
        (config.max_listeners, "max_listeners must be nonzero"),
        (
            config.max_listener_backlog,
            "max_listener_backlog must be nonzero",
        ),
        (config.max_connections, "max_connections must be nonzero"),
        (
            config.max_inflight_operations,
            "max_inflight_operations must be nonzero",
        ),
        (
            config.directional_buffer_bytes,
            "directional_buffer_bytes must be nonzero",
        ),
        (
            config.max_operation_bytes,
            "max_operation_bytes must be nonzero",
        ),
        (config.max_chunk_bytes, "max_chunk_bytes must be nonzero"),
    ] {
        if value == 0 {
            return Err(NetworkError::InvalidConfig { reason });
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::future::Future;
    use std::panic::{AssertUnwindSafe, catch_unwind};
    use std::pin::Pin;
    use std::sync::Arc;
    use std::task::{Context, Poll, Wake, Waker};

    use kr_runtime::{CompletionCertainty, SimRuntime};

    use super::*;
    use crate::conformance::{check_connected_stream_pair, check_network_provider};
    use crate::network::{NodeId, SendNetworkProviderSubmit};

    fn poll_once<F: Future + Unpin>(future: &mut F) -> Poll<F::Output> {
        let waker = Waker::noop();
        let mut context = Context::from_waker(waker);
        Pin::new(future).poll(&mut context)
    }

    fn config() -> MemoryNetworkConfig {
        MemoryNetworkConfig {
            max_listeners: 4,
            max_listener_backlog: 4,
            max_connections: 8,
            max_inflight_operations: 32,
            directional_buffer_bytes: 32,
            max_operation_bytes: 64,
            max_chunk_bytes: 2,
        }
    }

    #[test]
    fn implements_send_contract_and_shared_stream_conformance() {
        fn assert_send_provider<T: SendNetworkProviderSubmit>() {}
        assert_send_provider::<MemoryNetwork>();

        let network = MemoryNetwork::new(config()).expect("memory network config is valid");
        let (left, right) = network
            .connected_pair()
            .expect("connected pair is admitted");
        let mut runtime = SimRuntime::default();
        runtime
            .block_on(async move { check_connected_stream_pair(&left, &right).await })
            .expect("runtime drives stream conformance")
            .expect("memory streams satisfy shared conformance");
    }

    #[test]
    fn passes_shared_provider_conformance() {
        let network = MemoryNetwork::new(config()).expect("memory network config is valid");
        let mut runtime = SimRuntime::default();
        runtime
            .block_on(async move {
                check_network_provider(
                    &network,
                    ListenRequest {
                        address: NetworkAddress {
                            node: NodeId(20),
                            port: 7_000,
                        },
                        backlog: 4,
                    },
                    NetworkAddress {
                        node: NodeId(10),
                        port: 4_001,
                    },
                    NetworkAddress {
                        node: NodeId(11),
                        port: 4_002,
                    },
                )
                .await
            })
            .expect("runtime drives provider conformance")
            .expect("memory network satisfies shared provider conformance");
    }

    #[test]
    fn dropped_capacity_blocked_write_still_commits_after_read() {
        let network = MemoryNetwork::new(MemoryNetworkConfig {
            directional_buffer_bytes: 1,
            max_chunk_bytes: 8,
            ..config()
        })
        .expect("memory network config is valid");
        let (left, right) = network
            .connected_pair()
            .expect("connected pair is admitted");
        let mut runtime = SimRuntime::default();

        runtime
            .block_on(left.submit_write(WriteRequest {
                buffer: b"a".to_vec(),
            }))
            .expect("runtime drives first write")
            .expect("first write succeeds");
        let empty = runtime
            .block_on(left.submit_write(WriteRequest { buffer: Vec::new() }))
            .expect("runtime drives empty write")
            .expect("empty write succeeds despite full capacity");
        assert_eq!(empty.bytes_written, 0);
        drop(left.submit_write(WriteRequest {
            buffer: b"b".to_vec(),
        }));
        assert_eq!(network.status().pending_writes, 1);

        let first = runtime
            .block_on(right.submit_read(ReadRequest {
                buffer: Vec::new(),
                max_bytes: 1,
            }))
            .expect("runtime drives first read")
            .expect("first read succeeds");
        assert_eq!(first.buffer, b"a");
        assert_eq!(network.status().pending_writes, 0);
        let second = runtime
            .block_on(right.submit_read(ReadRequest {
                buffer: Vec::new(),
                max_bytes: 1,
            }))
            .expect("runtime drives second read")
            .expect("second read succeeds");
        assert_eq!(second.buffer, b"b");
    }

    #[test]
    fn pending_operations_are_bounded_by_inflight_limit() {
        let network = MemoryNetwork::new(MemoryNetworkConfig {
            max_inflight_operations: 1,
            ..config()
        })
        .expect("memory network config is valid");
        let (left, _right) = network
            .connected_pair()
            .expect("connected pair is admitted");
        let mut first = left.submit_read(ReadRequest {
            buffer: Vec::new(),
            max_bytes: 1,
        });
        assert!(poll_once(&mut first).is_pending());

        let Poll::Ready(error) = poll_once(&mut left.submit_read(ReadRequest {
            buffer: Vec::new(),
            max_bytes: 1,
        })) else {
            panic!("capacity rejection must be ready");
        };
        let error = error.expect_err("second read is rejected");
        assert_eq!(error.certainty(), CompletionCertainty::NotApplied);
        assert_eq!(
            error.error().error(),
            &NetworkError::ResourceExhausted {
                resource: "inflight operations",
                limit: 1,
            }
        );
        assert_eq!(network.status().inflight_operations, 1);
    }

    #[test]
    fn oversized_reserved_capacity_is_rejected_before_provider_retention() {
        let network = MemoryNetwork::new(MemoryNetworkConfig {
            max_operation_bytes: 4,
            ..config()
        })
        .expect("memory network config is valid");
        let (left, _right) = network
            .connected_pair()
            .expect("connected pair is admitted");

        let mut read_buffer = Vec::with_capacity(5);
        read_buffer.push(b'r');
        let Poll::Ready(read) = poll_once(&mut left.submit_read(ReadRequest {
            buffer: read_buffer,
            max_bytes: 1,
        })) else {
            panic!("oversized read allocation must be rejected eagerly");
        };
        let returned = read
            .expect_err("oversized read allocation is rejected")
            .into_parts()
            .1
            .into_buffer()
            .expect("read buffer is returned");
        assert!(returned.capacity() > 4);

        let mut write_buffer = Vec::with_capacity(5);
        write_buffer.push(b'w');
        let Poll::Ready(write) = poll_once(&mut left.submit_write(WriteRequest {
            buffer: write_buffer,
        })) else {
            panic!("oversized write allocation must be rejected eagerly");
        };
        let returned = write
            .expect_err("oversized write allocation is rejected")
            .into_parts()
            .1
            .into_buffer()
            .expect("write buffer is returned");
        assert!(returned.capacity() > 4);
        assert_eq!(network.status().inflight_operations, 0);
    }

    struct PanicWake;

    impl Wake for PanicWake {
        fn wake(self: Arc<Self>) {
            panic!("test waker panic");
        }
    }

    #[test]
    fn completion_contains_panicking_waker() {
        let network = MemoryNetwork::new(MemoryNetworkConfig {
            directional_buffer_bytes: 1,
            max_chunk_bytes: 1,
            ..config()
        })
        .expect("memory network config is valid");
        let (left, right) = network
            .connected_pair()
            .expect("connected pair is admitted");
        let mut runtime = SimRuntime::default();
        runtime
            .block_on(left.submit_write(WriteRequest {
                buffer: b"a".to_vec(),
            }))
            .expect("runtime drives first write")
            .expect("first write succeeds");

        let mut blocked = left.submit_write(WriteRequest {
            buffer: b"b".to_vec(),
        });
        let waker = Waker::from(Arc::new(PanicWake));
        let mut context = Context::from_waker(&waker);
        assert!(Pin::new(&mut blocked).poll(&mut context).is_pending());

        let boundary = catch_unwind(AssertUnwindSafe(|| {
            drop(right.submit_read(ReadRequest {
                buffer: Vec::new(),
                max_bytes: 1,
            }));
        }));
        assert!(boundary.is_ok(), "waker panic escaped provider completion");
        assert!(poll_once(&mut blocked).is_ready());
    }
}
