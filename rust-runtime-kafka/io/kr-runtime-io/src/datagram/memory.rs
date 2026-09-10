//! Thread-safe, host-I/O-free deterministic datagram transport.
//!
//! Operations take effect eagerly under one bounded mutex. Bind, send,
//! nonblocking receive, and close therefore return terminal operations without
//! depending on an executor. Blocking receives remain in provider state until
//! a send, close, or explicit clock advance terminalizes them. Dropping their
//! response does not cancel that retained receive.
//!
//! [`MemoryDatagramNetwork::set_now`] and [`MemoryDatagramNetwork::advance`]
//! are the only sources of time progress. When packet delivery and a deadline
//! use the same [`SimInstant`], the mutation which acquires the provider
//! mutex first wins: a send made before advancing to the deadline is delivered,
//! while advancing first expires the receive.

use super::{
    DatagramBindRequest, DatagramError, DatagramFailure, DatagramProviderSubmit,
    DatagramSocketSubmit, ReceiveMode, RecvFromRequest, RecvFromResult, SendToRequest,
    SendToResult, receive_success,
};
use crate::completion::{SafeWaker, SyncCell, lock_unpoisoned};
use crate::network::NetworkAddress;
use kr_runtime::{CompletionError, CompletionResult, SimDuration, SimInstant};
use std::collections::{BTreeMap, VecDeque};
use std::fmt;
use std::num::NonZeroU64;
use std::sync::{Arc, Mutex};

/// Bounds for the deterministic in-memory datagram provider.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct MemoryDatagramConfig {
    /// Maximum simultaneously bound sockets.
    pub max_sockets: usize,
    /// Maximum payload accepted by one send.
    pub max_datagram_bytes: usize,
    /// Maximum caller-owned capacity and logical receive-operation bytes.
    pub max_operation_bytes: usize,
    /// Maximum arrived packets retained by one socket.
    pub max_queued_datagrams_per_socket: usize,
    /// Maximum arrived payload bytes retained by one socket.
    pub max_queued_bytes_per_socket: usize,
    /// Maximum blocking or deadline receives retained by one socket.
    pub max_pending_receives_per_socket: usize,
}

impl Default for MemoryDatagramConfig {
    fn default() -> Self {
        Self {
            max_sockets: 1_024,
            max_datagram_bytes: 65_507,
            max_operation_bytes: 256 * 1_024,
            max_queued_datagrams_per_socket: 1_024,
            max_queued_bytes_per_socket: 1_024 * 1_024,
            max_pending_receives_per_socket: 1_024,
        }
    }
}

/// Passive bounded state for deterministic assertions and diagnostics.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct MemoryDatagramStatus {
    /// Current explicit provider time.
    pub now: SimInstant,
    /// Live bound sockets.
    pub sockets: usize,
    /// Arrived packets retained across sockets.
    pub queued_datagrams: usize,
    /// Arrived payload bytes retained across sockets.
    pub queued_bytes: usize,
    /// Blocking or deadline receives retained across sockets.
    pub pending_receives: usize,
    /// Sends discarded because no destination was bound.
    pub dropped_unbound: u64,
    /// Sends discarded because the destination ingress bound was full.
    pub dropped_ingress_full: u64,
}

/// A cloneable, thread-safe deterministic in-memory datagram network.
///
/// Sends to an unbound address or to a destination whose bounded ingress is
/// full still complete locally and increment the corresponding drop counter in
/// [`MemoryDatagramStatus`], matching UDP's lack of a delivery guarantee.
/// Receive deadlines use the explicit provider clock: a send and an advance to
/// the same instant are ordered by whichever acquires the provider mutex first.
#[derive(Clone)]
pub struct MemoryDatagramNetwork {
    state: Arc<Mutex<State>>,
}

impl MemoryDatagramNetwork {
    /// Creates an empty network at [`SimInstant::ZERO`].
    ///
    /// # Errors
    ///
    /// Returns [`DatagramError::InvalidConfig`] for a zero required bound or
    /// when the payload limit exceeds the per-operation limit.
    pub fn new(config: MemoryDatagramConfig) -> Result<Self, DatagramError> {
        validate_config(config)?;
        Ok(Self {
            state: Arc::new(Mutex::new(State {
                config,
                now: SimInstant::ZERO,
                next_socket_key: 1,
                sockets: BTreeMap::new(),
                bindings: BTreeMap::new(),
                dropped_unbound: 0,
                dropped_ingress_full: 0,
            })),
        })
    }

    /// Returns the provider's explicit monotonic time.
    #[must_use]
    pub fn now(&self) -> SimInstant {
        lock_unpoisoned(&self.state).now
    }

    /// Advances by an exact simulation duration and expires eligible receives.
    ///
    /// The returned count is the number of deadline receives terminalized.
    /// No state changes when the target instant would overflow.
    pub fn advance(&self, duration: SimDuration) -> Result<usize, DatagramError> {
        let completions = {
            let mut state = lock_unpoisoned(&self.state);
            state.now = state
                .now
                .checked_add(duration)
                .ok_or(DatagramError::InvalidRequest {
                    reason: "memory datagram clock advance overflowed",
                })?;
            let expired = state.take_expired_receives();
            install_receive_failures(expired, DatagramError::DeadlineExceeded)
        };
        let count = completions.len();
        run_receive_completions(completions);
        Ok(count)
    }

    /// Sets a later-or-equal explicit time and expires eligible receives.
    ///
    /// Time never moves backwards. Expired operations are removed atomically
    /// under the provider mutex, then their caller wakers are notified outside
    /// that mutex.
    pub fn set_now(&self, now: SimInstant) -> Result<usize, DatagramError> {
        let completions = {
            let mut state = lock_unpoisoned(&self.state);
            if now < state.now {
                return Err(DatagramError::InvalidRequest {
                    reason: "memory datagram clock cannot move backwards",
                });
            }
            state.now = now;
            let expired = state.take_expired_receives();
            install_receive_failures(expired, DatagramError::DeadlineExceeded)
        };
        let count = completions.len();
        run_receive_completions(completions);
        Ok(count)
    }

    /// Returns a bounded passive state snapshot.
    #[must_use]
    pub fn status(&self) -> MemoryDatagramStatus {
        lock_unpoisoned(&self.state).status()
    }
}

impl DatagramProviderSubmit for MemoryDatagramNetwork {
    type Address = NetworkAddress;
    type Instant = SimInstant;
    type Socket = MemoryDatagramSocket;
    type BindResponse =
        MemoryDatagramOperation<CompletionResult<MemoryDatagramSocket, DatagramFailure>>;

    fn submit_bind(&self, request: DatagramBindRequest<NetworkAddress>) -> Self::BindResponse {
        let output = {
            let mut state = lock_unpoisoned(&self.state);
            if state.bindings.contains_key(&request.address) {
                Err(CompletionError::not_applied(
                    DatagramFailure::without_buffer(DatagramError::AddressInUse),
                ))
            } else if state.sockets.len() >= state.config.max_sockets {
                Err(CompletionError::not_applied(
                    DatagramFailure::without_buffer(DatagramError::ResourceExhausted {
                        resource: "memory datagram sockets",
                        limit: state.config.max_sockets,
                    }),
                ))
            } else {
                let socket_key = state.allocate_socket_key();
                state.bindings.insert(request.address, socket_key);
                state.sockets.insert(
                    socket_key,
                    SocketState {
                        address: request.address,
                        queued: VecDeque::new(),
                        queued_bytes: 0,
                        pending_receives: VecDeque::new(),
                    },
                );
                Ok(MemoryDatagramSocket {
                    state: Arc::clone(&self.state),
                    socket_key,
                    address: request.address,
                })
            }
        };
        MemoryDatagramOperation::ready(output)
    }
}

/// One exclusive binding in a [`MemoryDatagramNetwork`].
pub struct MemoryDatagramSocket {
    state: Arc<Mutex<State>>,
    socket_key: SocketKey,
    address: NetworkAddress,
}

impl fmt::Debug for MemoryDatagramSocket {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("MemoryDatagramSocket")
            .field("address", &self.address)
            .finish_non_exhaustive()
    }
}

type SendOutput = CompletionResult<SendToResult, DatagramFailure>;
type RecvOutput = CompletionResult<RecvFromResult<NetworkAddress>, DatagramFailure>;
type ControlOutput = CompletionResult<(), DatagramFailure>;

impl DatagramSocketSubmit for MemoryDatagramSocket {
    type Address = NetworkAddress;
    type Instant = SimInstant;
    type SendResponse = MemoryDatagramOperation<SendOutput>;
    type RecvResponse = MemoryDatagramOperation<RecvOutput>;
    type ControlResponse = MemoryDatagramOperation<ControlOutput>;

    fn local_addr(&self) -> NetworkAddress {
        self.address
    }

    fn submit_send_to(&self, request: SendToRequest<NetworkAddress>) -> Self::SendResponse {
        let mut receive_completion = None;
        let output = {
            let mut state = lock_unpoisoned(&self.state);
            if !state.sockets.contains_key(&self.socket_key) {
                rejection(DatagramError::SocketClosed, request.buffer)
            } else if request.buffer.len() > state.config.max_datagram_bytes {
                let limit = state.config.max_datagram_bytes;
                rejection(
                    DatagramError::MessageTooLarge {
                        max_payload_bytes: Some(limit),
                    },
                    request.buffer,
                )
            } else if request.buffer.capacity() > state.config.max_operation_bytes {
                let limit = state.config.max_operation_bytes;
                rejection(
                    DatagramError::ResourceExhausted {
                        resource: "memory datagram send operation bytes",
                        limit,
                    },
                    request.buffer,
                )
            } else {
                let packet = Packet {
                    source: self.address,
                    payload: request.buffer.clone(),
                };
                receive_completion = state.deliver(request.destination, packet);
                let bytes_sent = request.buffer.len();
                Ok(SendToResult {
                    buffer: request.buffer,
                    bytes_sent,
                })
            }
        };
        if let Some(completion) = receive_completion {
            completion.run();
        }
        MemoryDatagramOperation::ready(output)
    }

    fn submit_recv_from(&self, request: RecvFromRequest) -> Self::RecvResponse {
        self.submit_receive(request, ReceiveMode::Wait)
    }

    fn submit_try_recv_from(&self, request: RecvFromRequest) -> Self::RecvResponse {
        self.submit_receive(request, ReceiveMode::Try)
    }

    fn submit_recv_from_until(
        &self,
        request: RecvFromRequest,
        deadline: SimInstant,
    ) -> Self::RecvResponse {
        self.submit_receive(request, ReceiveMode::Deadline(deadline))
    }

    fn submit_close(&self) -> Self::ControlResponse {
        let (output, completions) = {
            let mut state = lock_unpoisoned(&self.state);
            let pending = state.close_socket(self.socket_key);
            let completions = install_receive_failures(pending, DatagramError::SocketClosed);
            (Ok(()), completions)
        };
        run_receive_completions(completions);
        MemoryDatagramOperation::ready(output)
    }
}

impl MemoryDatagramSocket {
    fn submit_receive(
        &self,
        request: RecvFromRequest,
        mode: ReceiveMode,
    ) -> MemoryDatagramOperation<RecvOutput> {
        let mut request = Some(request);
        let mut immediate = None;
        let mut pending_cell = None;
        {
            let mut state = lock_unpoisoned(&self.state);
            let receive = request.take().expect("receive request is present");
            let total_bytes = receive.buffer.len().checked_add(receive.max_bytes);
            if receive.buffer.capacity() > state.config.max_operation_bytes
                || total_bytes.is_none_or(|bytes| bytes > state.config.max_operation_bytes)
            {
                immediate = Some(rejection(
                    DatagramError::ResourceExhausted {
                        resource: "memory datagram receive operation bytes",
                        limit: state.config.max_operation_bytes,
                    },
                    receive.buffer,
                ));
            } else if !state.sockets.contains_key(&self.socket_key) {
                immediate = Some(rejection(DatagramError::SocketClosed, receive.buffer));
            } else if matches!(mode, ReceiveMode::Deadline(deadline) if deadline <= state.now) {
                immediate = Some(rejection(DatagramError::DeadlineExceeded, receive.buffer));
            } else {
                let max_pending = state.config.max_pending_receives_per_socket;
                let socket = state
                    .sockets
                    .get_mut(&self.socket_key)
                    .expect("socket existence checked above");
                if let Some(packet) = socket.queued.pop_front() {
                    socket.queued_bytes = socket.queued_bytes.saturating_sub(packet.payload.len());
                    immediate = Some(receive_success(receive, packet.source, &packet.payload));
                } else if matches!(mode, ReceiveMode::Try) {
                    immediate = Some(rejection(DatagramError::WouldBlock, receive.buffer));
                } else if socket.pending_receives.len() >= max_pending {
                    immediate = Some(rejection(
                        DatagramError::ResourceExhausted {
                            resource: "pending memory datagram receives per socket",
                            limit: max_pending,
                        },
                        receive.buffer,
                    ));
                } else {
                    let cell = Arc::new(SyncCell::new(None));
                    socket.pending_receives.push_back(PendingReceive {
                        request: receive,
                        deadline: match mode {
                            ReceiveMode::Deadline(deadline) => Some(deadline),
                            ReceiveMode::Wait | ReceiveMode::Try => None,
                        },
                        cell: Arc::clone(&cell),
                    });
                    pending_cell = Some(cell);
                }
            }
        }
        if let Some(output) = immediate {
            MemoryDatagramOperation::ready(output)
        } else {
            MemoryDatagramOperation::from_cell(
                pending_cell.expect("nonterminal receive retained its completion cell"),
            )
        }
    }
}

impl Drop for MemoryDatagramSocket {
    fn drop(&mut self) {
        let completions = {
            let mut state = lock_unpoisoned(&self.state);
            let pending = state.close_socket(self.socket_key);
            install_receive_failures(pending, DatagramError::SocketClosed)
        };
        run_receive_completions(completions);
    }
}

/// An owned, thread-safe memory-provider operation.
///
/// Polling after it returned `Ready` panics. Dropping it abandons only the
/// response; a provider-retained receive continues to completion.
pub type MemoryDatagramOperation<T> = crate::completion::SyncOperation<T>;

struct State {
    config: MemoryDatagramConfig,
    now: SimInstant,
    next_socket_key: u64,
    sockets: BTreeMap<SocketKey, SocketState>,
    bindings: BTreeMap<NetworkAddress, SocketKey>,
    dropped_unbound: u64,
    dropped_ingress_full: u64,
}

struct SocketState {
    address: NetworkAddress,
    queued: VecDeque<Packet>,
    queued_bytes: usize,
    pending_receives: VecDeque<PendingReceive>,
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct SocketKey(NonZeroU64);

struct Packet {
    source: NetworkAddress,
    payload: Vec<u8>,
}

struct PendingReceive {
    request: RecvFromRequest,
    deadline: Option<SimInstant>,
    cell: Arc<SyncCell<RecvOutput>>,
}

struct ReceiveCompletion {
    cell: Arc<SyncCell<RecvOutput>>,
    output: RecvOutput,
}

impl ReceiveCompletion {
    /// Installs terminal state while the caller still holds the provider state
    /// lock. The returned action defers only caller notification and final
    /// completion-cell destruction until after that lock is released.
    fn install(self) -> ReceiveCompletionAction {
        let waker = self.cell.set_output(self.output);
        ReceiveCompletionAction {
            cell: self.cell,
            waker,
        }
    }
}

struct ReceiveCompletionAction {
    cell: Arc<SyncCell<RecvOutput>>,
    waker: Option<SafeWaker>,
}

impl ReceiveCompletionAction {
    fn run(mut self) {
        if let Some(waker) = self.waker.take() {
            waker.wake();
        }
        drop(self.cell);
    }
}

impl State {
    fn allocate_socket_key(&mut self) -> SocketKey {
        let value = NonZeroU64::new(self.next_socket_key)
            .expect("memory datagram socket key is always nonzero");
        self.next_socket_key = value
            .get()
            .checked_add(1)
            .expect("memory datagram socket key space exhausted");
        SocketKey(value)
    }

    fn deliver(
        &mut self,
        destination: NetworkAddress,
        packet: Packet,
    ) -> Option<ReceiveCompletionAction> {
        let Some(&socket_key) = self.bindings.get(&destination) else {
            self.dropped_unbound = self.dropped_unbound.saturating_add(1);
            return None;
        };
        let socket = self
            .sockets
            .get_mut(&socket_key)
            .expect("binding points at one live memory datagram socket");
        if let Some(pending) = socket.pending_receives.pop_front() {
            return Some(
                ReceiveCompletion {
                    cell: pending.cell,
                    output: receive_success(pending.request, packet.source, &packet.payload),
                }
                .install(),
            );
        }
        let count_full = socket.queued.len() >= self.config.max_queued_datagrams_per_socket;
        let bytes_full = socket
            .queued_bytes
            .checked_add(packet.payload.len())
            .is_none_or(|bytes| bytes > self.config.max_queued_bytes_per_socket);
        if count_full || bytes_full {
            self.dropped_ingress_full = self.dropped_ingress_full.saturating_add(1);
        } else {
            socket.queued_bytes += packet.payload.len();
            socket.queued.push_back(packet);
        }
        None
    }

    fn take_expired_receives(&mut self) -> Vec<PendingReceive> {
        let now = self.now;
        let mut expired = Vec::new();
        for socket in self.sockets.values_mut() {
            let mut retained = VecDeque::with_capacity(socket.pending_receives.len());
            while let Some(pending) = socket.pending_receives.pop_front() {
                if pending.deadline.is_some_and(|deadline| deadline <= now) {
                    expired.push(pending);
                } else {
                    retained.push_back(pending);
                }
            }
            socket.pending_receives = retained;
        }
        expired
    }

    fn close_socket(&mut self, socket_key: SocketKey) -> Vec<PendingReceive> {
        let Some(socket) = self.sockets.remove(&socket_key) else {
            return Vec::new();
        };
        if self.bindings.get(&socket.address) == Some(&socket_key) {
            self.bindings.remove(&socket.address);
        }
        socket.pending_receives.into_iter().collect()
    }

    fn status(&self) -> MemoryDatagramStatus {
        let mut queued_datagrams = 0;
        let mut queued_bytes = 0;
        let mut pending_receives = 0;
        for socket in self.sockets.values() {
            queued_datagrams += socket.queued.len();
            queued_bytes += socket.queued_bytes;
            pending_receives += socket.pending_receives.len();
        }
        MemoryDatagramStatus {
            now: self.now,
            sockets: self.sockets.len(),
            queued_datagrams,
            queued_bytes,
            pending_receives,
            dropped_unbound: self.dropped_unbound,
            dropped_ingress_full: self.dropped_ingress_full,
        }
    }
}

fn rejection<T>(error: DatagramError, buffer: Vec<u8>) -> CompletionResult<T, DatagramFailure> {
    Err(CompletionError::not_applied(DatagramFailure::with_buffer(
        error, buffer, 0,
    )))
}

fn install_receive_failures(
    pending: Vec<PendingReceive>,
    error: DatagramError,
) -> Vec<ReceiveCompletionAction> {
    pending
        .into_iter()
        .map(|pending| {
            ReceiveCompletion {
                cell: pending.cell,
                output: rejection(error.clone(), pending.request.buffer),
            }
            .install()
        })
        .collect()
}

fn run_receive_completions(completions: Vec<ReceiveCompletionAction>) {
    for completion in completions {
        completion.run();
    }
}

fn validate_config(config: MemoryDatagramConfig) -> Result<(), DatagramError> {
    for (value, reason) in [
        (config.max_sockets, "max_sockets must be nonzero"),
        (
            config.max_operation_bytes,
            "max_operation_bytes must be nonzero",
        ),
        (
            config.max_queued_datagrams_per_socket,
            "max_queued_datagrams_per_socket must be nonzero",
        ),
        (
            config.max_queued_bytes_per_socket,
            "max_queued_bytes_per_socket must be nonzero",
        ),
        (
            config.max_pending_receives_per_socket,
            "max_pending_receives_per_socket must be nonzero",
        ),
    ] {
        if value == 0 {
            return Err(DatagramError::InvalidConfig { reason });
        }
    }
    if config.max_datagram_bytes > config.max_operation_bytes {
        return Err(DatagramError::InvalidConfig {
            reason: "max_datagram_bytes must not exceed max_operation_bytes",
        });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::conformance::check_datagram_provider;
    use crate::datagram::SendDatagramProviderSubmit;
    use crate::network::NodeId;
    use kr_runtime::{CompletionCertainty, SimRuntime};
    use std::future::Future;
    use std::pin::Pin;
    use std::sync::Arc;
    use std::task::{Context, Wake, Waker};

    fn address(node: u64, port: u16) -> NetworkAddress {
        NetworkAddress {
            node: NodeId(node),
            port,
        }
    }

    fn bind(
        runtime: &mut SimRuntime,
        network: &MemoryDatagramNetwork,
        address: NetworkAddress,
    ) -> MemoryDatagramSocket {
        runtime
            .block_on(network.submit_bind(DatagramBindRequest { address }))
            .expect("runtime drives bind")
            .expect("memory bind succeeds")
    }

    #[test]
    fn implements_send_contract_and_shared_conformance() {
        fn assert_send_provider<T: SendDatagramProviderSubmit>() {}
        assert_send_provider::<MemoryDatagramNetwork>();

        let network = MemoryDatagramNetwork::new(MemoryDatagramConfig::default())
            .expect("default memory datagram config is valid");
        let mut runtime = SimRuntime::default();
        runtime
            .block_on(check_datagram_provider(
                &network,
                address(1, 4_001),
                address(2, 4_002),
                address(3, 4_003),
                SimInstant::ZERO,
            ))
            .expect("runtime drives memory conformance")
            .expect("memory datagram provider conforms");
    }

    #[test]
    fn ingress_and_pending_receive_bounds_are_enforced() {
        let network = MemoryDatagramNetwork::new(MemoryDatagramConfig {
            max_sockets: 2,
            max_datagram_bytes: 8,
            max_operation_bytes: 8,
            max_queued_datagrams_per_socket: 1,
            max_queued_bytes_per_socket: 8,
            max_pending_receives_per_socket: 1,
        })
        .expect("bounded config is valid");
        let mut runtime = SimRuntime::default();
        let a = bind(&mut runtime, &network, address(1, 5_001));
        let b = bind(&mut runtime, &network, address(2, 5_002));

        for payload in [b"first".as_slice(), b"lost".as_slice()] {
            runtime
                .block_on(a.submit_send_to(SendToRequest {
                    buffer: payload.to_vec(),
                    destination: b.local_addr(),
                }))
                .expect("runtime drives send")
                .expect("local send succeeds even when ingress drops");
        }
        assert_eq!(network.status().queued_datagrams, 1);
        assert_eq!(network.status().dropped_ingress_full, 1);
        let first = runtime
            .block_on(b.submit_recv_from(RecvFromRequest {
                buffer: Vec::new(),
                max_bytes: 8,
            }))
            .expect("runtime drives receive")
            .expect("first packet is queued");
        assert_eq!(first.buffer, b"first");
        let empty = runtime
            .block_on(b.submit_try_recv_from(RecvFromRequest {
                buffer: Vec::new(),
                max_bytes: 8,
            }))
            .expect("runtime drives nonblocking receive")
            .expect_err("second packet was dropped at the bound");
        assert_eq!(empty.error().error(), &DatagramError::WouldBlock);

        let first_pending = b.submit_recv_from(RecvFromRequest {
            buffer: Vec::new(),
            max_bytes: 8,
        });
        let rejected = runtime
            .block_on(b.submit_recv_from(RecvFromRequest {
                buffer: b"owned".to_vec(),
                max_bytes: 3,
            }))
            .expect("runtime drives rejected receive")
            .expect_err("pending receive bound rejects a second waiter");
        assert_eq!(rejected.certainty(), CompletionCertainty::NotApplied);
        assert!(matches!(
            rejected.error().error(),
            DatagramError::ResourceExhausted { limit: 1, .. }
        ));
        assert_eq!(rejected.error().buffer(), Some(&b"owned"[..]));

        runtime
            .block_on(a.submit_send_to(SendToRequest {
                buffer: b"wake".to_vec(),
                destination: b.local_addr(),
            }))
            .expect("runtime drives wake send")
            .expect("wake send succeeds");
        let received = runtime
            .block_on(first_pending)
            .expect("runtime drives retained receive")
            .expect("retained receive completes");
        assert_eq!(received.buffer, b"wake");
        assert_eq!(network.status().pending_receives, 0);
    }

    #[test]
    fn explicit_deadlines_and_same_instant_order_are_deterministic() {
        let network =
            MemoryDatagramNetwork::new(MemoryDatagramConfig::default()).expect("default config");
        let mut runtime = SimRuntime::default();
        let a = bind(&mut runtime, &network, address(1, 6_001));
        let b = bind(&mut runtime, &network, address(2, 6_002));

        let expires = b.submit_recv_from_until(
            RecvFromRequest {
                buffer: b"prefix".to_vec(),
                max_bytes: 8,
            },
            SimInstant::from_nanos(10),
        );
        assert_eq!(network.set_now(SimInstant::from_nanos(9)), Ok(0));
        assert_eq!(network.set_now(SimInstant::from_nanos(10)), Ok(1));
        let expired = runtime
            .block_on(expires)
            .expect("runtime drives expired receive")
            .expect_err("deadline expires without consuming a packet");
        assert_eq!(expired.error().error(), &DatagramError::DeadlineExceeded);
        assert_eq!(expired.error().buffer(), Some(&b"prefix"[..]));

        let delivery_wins = b.submit_recv_from_until(
            RecvFromRequest {
                buffer: Vec::new(),
                max_bytes: 8,
            },
            SimInstant::from_nanos(20),
        );
        drop(a.submit_send_to(SendToRequest {
            buffer: b"before".to_vec(),
            destination: b.local_addr(),
        }));
        assert_eq!(network.set_now(SimInstant::from_nanos(20)), Ok(0));
        assert_eq!(
            runtime
                .block_on(delivery_wins)
                .expect("runtime drives receive")
                .expect("send before same-instant advance wins")
                .buffer,
            b"before"
        );

        let deadline_wins = b.submit_recv_from_until(
            RecvFromRequest {
                buffer: Vec::new(),
                max_bytes: 8,
            },
            SimInstant::from_nanos(30),
        );
        assert_eq!(network.set_now(SimInstant::from_nanos(30)), Ok(1));
        drop(a.submit_send_to(SendToRequest {
            buffer: b"after".to_vec(),
            destination: b.local_addr(),
        }));
        let expired = runtime
            .block_on(deadline_wins)
            .expect("runtime drives receive")
            .expect_err("advance before same-instant send wins");
        assert_eq!(expired.error().error(), &DatagramError::DeadlineExceeded);
        let after = runtime
            .block_on(b.submit_try_recv_from(RecvFromRequest {
                buffer: Vec::new(),
                max_bytes: 8,
            }))
            .expect("runtime drives queued receive")
            .expect("post-deadline packet remains available");
        assert_eq!(after.buffer, b"after");
    }

    #[test]
    fn explicit_clock_rejects_regression_and_overflow_without_changing_time() {
        let network =
            MemoryDatagramNetwork::new(MemoryDatagramConfig::default()).expect("default config");

        assert_eq!(network.set_now(SimInstant::from_nanos(10)), Ok(0));
        assert!(matches!(
            network.set_now(SimInstant::from_nanos(9)),
            Err(DatagramError::InvalidRequest { .. })
        ));
        assert_eq!(network.now(), SimInstant::from_nanos(10));

        assert_eq!(network.set_now(SimInstant::from_nanos(u64::MAX)), Ok(0));
        assert!(matches!(
            network.advance(SimDuration::from_nanos(1)),
            Err(DatagramError::InvalidRequest { .. })
        ));
        assert_eq!(network.now(), SimInstant::from_nanos(u64::MAX));
    }

    #[test]
    fn close_fences_pending_receive_and_stale_drop_preserves_rebinding() {
        let network =
            MemoryDatagramNetwork::new(MemoryDatagramConfig::default()).expect("default config");
        let mut runtime = SimRuntime::default();
        let sender = bind(&mut runtime, &network, address(1, 6_101));
        let receiver_address = address(2, 6_102);
        let receiver = bind(&mut runtime, &network, receiver_address);
        let pending = receiver.submit_recv_from(RecvFromRequest {
            buffer: b"owned".to_vec(),
            max_bytes: 8,
        });

        let close = receiver.submit_close();
        let rebound = bind(&mut runtime, &network, receiver_address);
        runtime
            .block_on(close)
            .expect("runtime drives close")
            .expect("close fence succeeds");
        let failure = runtime
            .block_on(pending)
            .expect("runtime drives fenced receive")
            .expect_err("close terminalizes the pending receive");
        assert_eq!(failure.error().error(), &DatagramError::SocketClosed);
        assert_eq!(failure.error().buffer(), Some(&b"owned"[..]));

        drop(receiver);
        drop(sender.submit_send_to(SendToRequest {
            buffer: b"rebound".to_vec(),
            destination: receiver_address,
        }));
        let received = runtime
            .block_on(rebound.submit_recv_from(RecvFromRequest {
                buffer: Vec::new(),
                max_bytes: 8,
            }))
            .expect("runtime drives rebound receive")
            .expect("stale socket drop did not close the rebound socket");
        assert_eq!(received.buffer, b"rebound");
    }

    #[test]
    fn receive_terminal_state_is_installed_at_the_provider_state_boundary() {
        let network =
            MemoryDatagramNetwork::new(MemoryDatagramConfig::default()).expect("default config");
        let mut runtime = SimRuntime::default();
        let sender = bind(&mut runtime, &network, address(1, 6_201));
        let receiver = bind(&mut runtime, &network, address(2, 6_202));
        let receive = receiver.submit_recv_from(RecvFromRequest {
            buffer: Vec::new(),
            max_bytes: 8,
        });

        let delivery = {
            let mut state = lock_unpoisoned(&network.state);
            let completion = state
                .deliver(
                    receiver.local_addr(),
                    Packet {
                        source: sender.local_addr(),
                        payload: b"atomic".to_vec(),
                    },
                )
                .expect("pending receive is terminalized");
            assert!(
                receive.output_installed(),
                "delivery must install the terminal result before releasing provider state"
            );
            completion
        };
        delivery.run();
        let result = runtime
            .block_on(receive)
            .expect("runtime drives delivered receive")
            .expect("receive succeeds");
        assert_eq!(result.buffer, b"atomic");

        let closed_receive = receiver.submit_recv_from(RecvFromRequest {
            buffer: b"owned".to_vec(),
            max_bytes: 8,
        });
        let close_completions = {
            let mut state = lock_unpoisoned(&network.state);
            let pending = state.close_socket(receiver.socket_key);
            let completions = install_receive_failures(pending, DatagramError::SocketClosed);
            assert!(
                closed_receive.output_installed(),
                "close must install pending failures before releasing provider state"
            );
            completions
        };
        run_receive_completions(close_completions);
        let failure = runtime
            .block_on(closed_receive)
            .expect("runtime drives closed receive")
            .expect_err("close rejects the pending receive");
        assert_eq!(failure.error().error(), &DatagramError::SocketClosed);
        assert_eq!(failure.error().buffer(), Some(&b"owned"[..]));
    }

    struct PanicWake;

    impl Wake for PanicWake {
        fn wake(self: Arc<Self>) {
            panic!("test waker panic");
        }
    }

    struct PanicOnDropWake;

    #[expect(
        clippy::manual_noop_waker,
        reason = "the custom destructor panic is the behavior under test"
    )]
    impl Wake for PanicOnDropWake {
        fn wake(self: Arc<Self>) {}
    }

    impl Drop for PanicOnDropWake {
        fn drop(&mut self) {
            panic!("test waker drop panic");
        }
    }

    #[test]
    fn dropped_receive_consumes_and_panicking_waker_is_contained() {
        let network =
            MemoryDatagramNetwork::new(MemoryDatagramConfig::default()).expect("default config");
        let mut runtime = SimRuntime::default();
        let a = bind(&mut runtime, &network, address(1, 7_001));
        let b = bind(&mut runtime, &network, address(2, 7_002));

        drop(b.submit_recv_from(RecvFromRequest {
            buffer: Vec::new(),
            max_bytes: 8,
        }));
        drop(a.submit_send_to(SendToRequest {
            buffer: b"consumed".to_vec(),
            destination: b.local_addr(),
        }));
        let empty = runtime
            .block_on(b.submit_try_recv_from(RecvFromRequest {
                buffer: Vec::new(),
                max_bytes: 8,
            }))
            .expect("runtime drives try receive")
            .expect_err("dropped receive consumed the datagram");
        assert_eq!(empty.error().error(), &DatagramError::WouldBlock);

        let mut deadline = b.submit_recv_from_until(
            RecvFromRequest {
                buffer: Vec::new(),
                max_bytes: 8,
            },
            SimInstant::from_nanos(5),
        );
        let waker = Waker::from(Arc::new(PanicWake));
        let mut context = Context::from_waker(&waker);
        assert!(Pin::new(&mut deadline).poll(&mut context).is_pending());
        assert_eq!(network.set_now(SimInstant::from_nanos(5)), Ok(1));
        let error = runtime
            .block_on(deadline)
            .expect("runtime drives deadline")
            .expect_err("deadline still completes after panicking wake");
        assert_eq!(error.error().error(), &DatagramError::DeadlineExceeded);
    }

    #[test]
    fn replacing_a_waker_contains_its_panicking_destructor() {
        let network =
            MemoryDatagramNetwork::new(MemoryDatagramConfig::default()).expect("default config");
        let mut runtime = SimRuntime::default();
        let socket = bind(&mut runtime, &network, address(1, 7_101));
        let mut receive = socket.submit_recv_from(RecvFromRequest {
            buffer: Vec::new(),
            max_bytes: 8,
        });

        {
            let waker = Waker::from(Arc::new(PanicOnDropWake));
            let mut context = Context::from_waker(&waker);
            assert!(Pin::new(&mut receive).poll(&mut context).is_pending());
        }
        let boundary = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let mut context = Context::from_waker(Waker::noop());
            assert!(Pin::new(&mut receive).poll(&mut context).is_pending());
        }));
        assert!(boundary.is_ok(), "stored waker destructor panic escaped");

        drop(socket.submit_close());
        let error = runtime
            .block_on(receive)
            .expect("runtime drives closed receive")
            .expect_err("close still terminalizes the receive");
        assert_eq!(error.error().error(), &DatagramError::SocketClosed);
    }
}
