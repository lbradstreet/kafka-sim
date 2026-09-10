//! Deterministic, bounded datagram transport simulation.
//!
//! Send admission captures its link and fault plan. Packet delivery and local
//! send completion then advance on independent points of one virtual timeline:
//! either may become observable first. When both are due at the same
//! [`SimInstant`], delivery is processed before local completion. Separate
//! sends due at the same instant retain admission order, and duplicate copies
//! retain copy order.
//!
//! A packet-arrival versus receive-deadline tie follows the runtime's
//! deterministic timer-registration and ready-task FIFO order: whichever
//! callback removes the pending receive first wins. A deadline already equal
//! to `Handle::now()` wins synchronously at receive admission.
//!
//! A directional partition models an unobservable remote network failure: the
//! local send still succeeds while delivery is discarded. Delayed packets
//! resolve the destination binding when their delivery event fires, so they
//! may reach a socket that bound after the send or rebound after a close.
//!
//! Socket validity, operation admission, and caller-buffer bounds are checked
//! before a directional send ordinal is consumed. Once a script is selected,
//! exclusive before-enqueue faults win; otherwise actions compose in insertion
//! order. A selected drop or partition suppresses delivery-only delay,
//! duplication, and delivery-capacity validation because no physical copy is
//! retained. The link and script plan is immutable after admission.

use super::{
    DatagramBindRequest, DatagramError, DatagramFailure, DatagramProviderSubmit,
    DatagramSocketSubmit, ReceiveMode, RecvFromRequest, RecvFromResult, SendToRequest,
    SendToResult, receive_success,
};
use crate::completion::{LocalCell, LocalPermitPool};
use crate::network::NetworkAddress;
use kr_runtime::{
    CompletionCertainty, CompletionError, CompletionResult, Handle, SimDuration, SimInstant, Sleep,
    contain_panic,
};
use std::cell::{Cell, RefCell};
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::fmt;
use std::future::Future;
use std::num::NonZeroU64;
use std::pin::Pin;
use std::rc::Rc;
use std::task::{Context, Poll, Waker};

/// One exact simulated UDP direction, including node-local ports.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct DatagramDirection {
    /// Bound source address.
    pub source: NetworkAddress,
    /// Destination address supplied to `send_to`.
    pub destination: NetworkAddress,
}

/// Deterministic behavior captured by sends in one direction.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SimDatagramLinkConfig {
    /// Delay before a packet delivery event.
    pub delivery_latency: SimDuration,
    /// Delay before the sender observes local completion.
    pub send_completion_latency: SimDuration,
    /// Maximum delayed packet copies retained for this direction.
    pub max_scheduled_datagrams: usize,
    /// Whether newly admitted sends are locally accepted but remotely dropped.
    pub partitioned: bool,
}

impl Default for SimDatagramLinkConfig {
    fn default() -> Self {
        Self {
            delivery_latency: SimDuration::ZERO,
            send_completion_latency: SimDuration::ZERO,
            max_scheduled_datagrams: 1_024,
            partitioned: false,
        }
    }
}

/// Certainty reported by an injected failure after local enqueue.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SimDatagramAfterEnqueueCertainty {
    /// The simulator reports its known local enqueue effect precisely.
    Applied,
    /// The simulator deliberately models an ambiguous backend completion.
    MayHaveApplied,
}

/// One composable deterministic fault action.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SimDatagramFault {
    /// Fail before local enqueue. This action must be the only action selected
    /// for a send.
    FailBefore,
    /// Force a local queue-capacity rejection before enqueue. This action must
    /// be the only action selected for a send.
    QueueFull,
    /// Silently discard every physical delivery copy after local acceptance.
    Drop,
    /// Silently discard this send as if its direction were partitioned.
    Partition,
    /// Add virtual delay to packet delivery, allowing deterministic reordering.
    Delay { additional: SimDuration },
    /// Add physical delivery copies. Capacity for all copies is admitted
    /// atomically.
    Duplicate { additional_copies: usize },
    /// XOR one payload byte. For a nonempty payload, the offset wraps modulo
    /// the payload length.
    Corrupt { offset: usize, xor: u8 },
    /// Shorten the packet before it reaches the receive buffer.
    Truncate { len: usize },
    /// Report an error after the whole datagram has been locally enqueued.
    ErrorAfterEnqueue {
        certainty: SimDatagramAfterEnqueueCertainty,
    },
}

/// A tagged action selected by exact direction and one-based send ordinal.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ScriptedDatagramFault {
    /// Stable script tag. `FailBefore` and `ErrorAfterEnqueue` return it in an
    /// injected error; other actions contribute to aggregate fault diagnostics.
    pub tag: u64,
    /// Exact source-to-destination direction.
    pub direction: DatagramDirection,
    /// One-based send ordinal within `direction`.
    pub send_ordinal: u64,
    /// Fault action composed in insertion order with other actions for the send.
    pub action: SimDatagramFault,
}

/// Bounded resources for [`SimDatagramNetwork`].
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SimDatagramConfig {
    /// Maximum live or closing bound sockets.
    pub max_sockets: usize,
    /// Maximum admitted operation responses retained by the provider.
    pub max_inflight_operations: usize,
    /// Maximum payload length accepted by one send.
    pub max_datagram_bytes: usize,
    /// Maximum retained caller `Vec` capacity and total logical bytes requested
    /// by one send or receive operation.
    pub max_operation_bytes: usize,
    /// Maximum delayed physical packet copies across all directions.
    pub max_scheduled_datagrams: usize,
    /// Maximum delayed packet bytes across all directions.
    pub max_scheduled_bytes: usize,
    /// Maximum arrived packets buffered by one socket.
    pub max_queued_datagrams_per_socket: usize,
    /// Maximum arrived packet bytes buffered by one socket.
    pub max_queued_bytes_per_socket: usize,
    /// Maximum simultaneously admitted receives on one socket.
    pub max_pending_receives_per_socket: usize,
    /// Maximum directions retaining ordinals, scripts, counters, or overrides.
    pub max_directions: usize,
    /// Maximum queued fault actions.
    pub max_scripted_faults: usize,
    /// Maximum composed actions selected for one send.
    pub max_faults_per_send: usize,
    /// Maximum additional physical copies selected for one send.
    pub max_duplicate_copies: usize,
    /// Default behavior for a direction not explicitly configured.
    pub default_link: SimDatagramLinkConfig,
}

impl Default for SimDatagramConfig {
    fn default() -> Self {
        Self {
            max_sockets: 1_024,
            max_inflight_operations: 4_096,
            max_datagram_bytes: 65_507,
            max_operation_bytes: 256 * 1_024,
            max_scheduled_datagrams: 16_384,
            max_scheduled_bytes: 16 * 1_024 * 1_024,
            max_queued_datagrams_per_socket: 1_024,
            max_queued_bytes_per_socket: 1_024 * 1_024,
            max_pending_receives_per_socket: 1_024,
            max_directions: 4_096,
            max_scripted_faults: 4_096,
            max_faults_per_send: 16,
            max_duplicate_copies: 16,
            default_link: SimDatagramLinkConfig::default(),
        }
    }
}

/// Monotonic simulator diagnostics.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct SimDatagramCounters {
    /// Send calls that reached the provider, including local rejections.
    pub sends_attempted: u64,
    /// Sends successfully admitted to their completion timeline.
    pub sends_admitted: u64,
    /// Physical packet copies whose delivery events fired.
    pub delivery_events: u64,
    /// Delivery events placed directly into a pending receive.
    pub delivered_to_receivers: u64,
    /// Delivery events buffered at a bound destination.
    pub queued_at_receivers: u64,
    /// Delivery events discarded because no destination was bound.
    pub dropped_unbound: u64,
    /// Delivery events discarded because a destination ingress queue was full.
    pub dropped_ingress_full: u64,
    /// Logical sends discarded by loss faults or directional partitions.
    pub dropped_by_fault_or_partition: u64,
    /// Send plans rejected atomically by a delivery count or byte bound.
    pub delivery_admission_rejections: u64,
    /// Scripted actions selected by a send ordinal.
    pub fault_hits: u64,
}

/// Passive bounded state for deterministic assertions and diagnostics.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SimDatagramStatus {
    /// Live or closing sockets.
    pub sockets: usize,
    /// Sockets waiting for earlier local operations to terminalize.
    pub closing_sockets: usize,
    /// Operation permits retained by futures or internal state.
    pub inflight_operations: usize,
    /// Delayed physical packet copies.
    pub scheduled_datagrams: usize,
    /// Payload bytes held by delayed packet copies.
    pub scheduled_bytes: usize,
    /// Arrived packets buffered across sockets.
    pub queued_datagrams: usize,
    /// Arrived payload bytes buffered across sockets.
    pub queued_bytes: usize,
    /// Blocking or deadline receives awaiting a packet.
    pub pending_receives: usize,
    /// Directions retaining simulator state.
    pub directions: usize,
    /// Scripted actions not yet selected.
    pub pending_faults: usize,
    /// Monotonic event and fault counters.
    pub counters: SimDatagramCounters,
}

/// A deterministic bound-socket datagram network driven by virtual time.
#[derive(Clone)]
pub struct SimDatagramNetwork {
    state: Rc<RefCell<State>>,
}

impl SimDatagramNetwork {
    /// Creates an empty simulated network.
    ///
    /// # Errors
    ///
    /// Returns [`DatagramError::InvalidConfig`] when a required bound is zero
    /// or one bound contradicts another.
    pub fn new(handle: Handle, config: SimDatagramConfig) -> Result<Self, DatagramError> {
        validate_config(config)?;
        Ok(Self {
            state: Rc::new(RefCell::new(State {
                handle,
                config,
                permits: Rc::new(LocalPermitPool::new(config.max_inflight_operations)),
                next_operation_key: 1,
                next_socket_key: 1,
                sockets: BTreeMap::new(),
                bindings: BTreeMap::new(),
                directions: BTreeMap::new(),
                scheduled_datagrams: 0,
                scheduled_bytes: 0,
                pending_faults: 0,
                counters: SimDatagramCounters::default(),
            })),
        })
    }

    /// Replaces the policy captured by future sends in one exact direction.
    ///
    /// # Errors
    ///
    /// Returns an invalid-config error for a zero directional queue bound or a
    /// resource error when a new direction would exceed the configured bound.
    pub fn set_link(
        &self,
        direction: DatagramDirection,
        config: SimDatagramLinkConfig,
    ) -> Result<(), DatagramError> {
        if config.max_scheduled_datagrams == 0 {
            return Err(DatagramError::InvalidConfig {
                reason: "link max_scheduled_datagrams must be nonzero",
            });
        }
        let mut state = self.state.borrow_mut();
        state.ensure_direction(direction)?.config = config;
        Ok(())
    }

    /// Changes only the persistent partition flag for future sends.
    ///
    /// # Errors
    ///
    /// Returns a resource error when a new direction would exceed its bound.
    pub fn set_partitioned(
        &self,
        direction: DatagramDirection,
        partitioned: bool,
    ) -> Result<(), DatagramError> {
        let mut state = self.state.borrow_mut();
        state.ensure_direction(direction)?.config.partitioned = partitioned;
        Ok(())
    }

    /// Adds one deterministic tagged action for a future send ordinal.
    ///
    /// Actions for the same send compose in insertion order. `FailBefore` and
    /// `QueueFull` are exclusive terminal-before-enqueue actions. At most one
    /// `ErrorAfterEnqueue` may be selected for a send.
    ///
    /// # Errors
    ///
    /// Returns a typed request, configuration, or capacity error without
    /// changing the script.
    pub fn push_fault(&self, fault: ScriptedDatagramFault) -> Result<(), DatagramError> {
        let mut state = self.state.borrow_mut();
        state.push_fault(fault)
    }

    /// Returns bounded diagnostic state without driving virtual time.
    #[must_use]
    pub fn status(&self) -> SimDatagramStatus {
        self.state.borrow().status()
    }
}

impl DatagramProviderSubmit for SimDatagramNetwork {
    type Address = NetworkAddress;
    type Instant = SimInstant;
    type Socket = SimDatagramSocket;
    type BindResponse = SimDatagramOperation<CompletionResult<SimDatagramSocket, DatagramFailure>>;

    fn submit_bind(&self, request: DatagramBindRequest<NetworkAddress>) -> Self::BindResponse {
        State::submit_bind(&self.state, request)
    }
}

/// One exclusive simulated datagram binding.
pub struct SimDatagramSocket {
    state: Rc<RefCell<State>>,
    socket_key: SocketKey,
    address: NetworkAddress,
}

impl fmt::Debug for SimDatagramSocket {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SimDatagramSocket")
            .field("address", &self.address)
            .finish_non_exhaustive()
    }
}

impl DatagramSocketSubmit for SimDatagramSocket {
    type Address = NetworkAddress;
    type Instant = SimInstant;
    type SendResponse = SimDatagramOperation<CompletionResult<SendToResult, DatagramFailure>>;
    type RecvResponse =
        SimDatagramOperation<CompletionResult<RecvFromResult<NetworkAddress>, DatagramFailure>>;
    type ControlResponse = SimDatagramOperation<CompletionResult<(), DatagramFailure>>;

    fn local_addr(&self) -> NetworkAddress {
        self.address
    }

    fn submit_send_to(&self, request: SendToRequest<NetworkAddress>) -> Self::SendResponse {
        State::submit_send(&self.state, self.socket_key, request)
    }

    fn submit_recv_from(&self, request: RecvFromRequest) -> Self::RecvResponse {
        State::submit_receive(&self.state, self.socket_key, request, ReceiveMode::Wait)
    }

    fn submit_try_recv_from(&self, request: RecvFromRequest) -> Self::RecvResponse {
        State::submit_receive(&self.state, self.socket_key, request, ReceiveMode::Try)
    }

    fn submit_recv_from_until(
        &self,
        request: RecvFromRequest,
        deadline: SimInstant,
    ) -> Self::RecvResponse {
        State::submit_receive(
            &self.state,
            self.socket_key,
            request,
            ReceiveMode::Deadline(deadline),
        )
    }

    fn submit_close(&self) -> Self::ControlResponse {
        State::submit_close(&self.state, self.socket_key)
    }
}

impl Drop for SimDatagramSocket {
    fn drop(&mut self) {
        State::drop_socket(&self.state, self.socket_key);
    }
}

/// An owned simulator operation. Polling it after completion panics.
///
/// Dropping this future abandons only its response. Internal state retains an
/// admitted send or receive until it terminalizes.
pub type SimDatagramOperation<T> = crate::completion::LocalOperation<T>;

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct OperationKey(NonZeroU64);

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct SocketKey(NonZeroU64);

struct State {
    handle: Handle,
    config: SimDatagramConfig,
    permits: Rc<LocalPermitPool>,
    next_operation_key: u64,
    next_socket_key: u64,
    sockets: BTreeMap<SocketKey, SocketState>,
    bindings: BTreeMap<NetworkAddress, SocketKey>,
    directions: BTreeMap<DatagramDirection, DirectionState>,
    scheduled_datagrams: usize,
    scheduled_bytes: usize,
    pending_faults: usize,
    counters: SimDatagramCounters,
}

struct SocketState {
    address: NetworkAddress,
    closing: bool,
    queued: VecDeque<Packet>,
    queued_bytes: usize,
    pending_receives: VecDeque<PendingReceive>,
    prior_operations: BTreeSet<OperationKey>,
    close_waiters: Vec<CloseWaiter>,
}

struct PendingReceive {
    operation_key: OperationKey,
    request: RecvFromRequest,
    driver_signal: Option<Rc<ReceiveSignal>>,
    cell: Rc<RecvCell>,
}

type RecvOutput = CompletionResult<RecvFromResult<NetworkAddress>, DatagramFailure>;
type RecvCell = LocalCell<RecvOutput>;
type CloseOutput = CompletionResult<(), DatagramFailure>;
type CloseCell = LocalCell<CloseOutput>;
type SendOutput = CompletionResult<SendToResult, DatagramFailure>;
type SendCell = LocalCell<SendOutput>;

struct CloseWaiter {
    cell: Rc<CloseCell>,
}

struct Packet {
    source: NetworkAddress,
    destination: NetworkAddress,
    payload: Vec<u8>,
}

struct DirectionState {
    config: SimDatagramLinkConfig,
    next_send_ordinal: u64,
    faults: BTreeMap<u64, Vec<ScriptedAction>>,
    scheduled_datagrams: usize,
    scheduled_bytes: usize,
}

#[derive(Clone)]
struct ScriptedAction {
    tag: u64,
    action: SimDatagramFault,
}

struct SendPlan {
    operation_key: OperationKey,
    socket_key: SocketKey,
    direction: DatagramDirection,
    completion_at: SimInstant,
    delivery_at: Option<SimInstant>,
    packets: Vec<Packet>,
    completion: PendingSendCompletion,
    dropped: bool,
}

struct PendingSendCompletion {
    buffer: Vec<u8>,
    terminal: SendTerminal,
    effect_applied: bool,
}

enum SendTerminal {
    Success,
    Failure {
        error: DatagramError,
        certainty: CompletionCertainty,
        bytes_transferred: usize,
    },
}

struct SendTaskGuard {
    state: Rc<RefCell<State>>,
    socket_key: SocketKey,
    operation_key: OperationKey,
    completion: Option<PendingSendCompletion>,
    cell: Option<Rc<SendCell>>,
    admitted: Rc<Cell<bool>>,
    rollback_delivery: Option<Rc<DeliveryControl>>,
}

struct DeliveryControl {
    state: Rc<RefCell<State>>,
    direction: DatagramDirection,
    packets: RefCell<Vec<Packet>>,
}

struct DeliveryTaskGuard {
    control: Rc<DeliveryControl>,
    delivered: bool,
}

struct ReceiveTaskGuard {
    state: Rc<RefCell<State>>,
    socket_key: SocketKey,
    operation_key: OperationKey,
    armed: bool,
}

struct ReceiveSignal {
    cancelled: Cell<bool>,
    waker: RefCell<Option<Waker>>,
}

struct DeadlineWait {
    sleep: Sleep,
    signal: Rc<ReceiveSignal>,
}

struct SignalWait {
    signal: Rc<ReceiveSignal>,
}

enum DeadlineWaitResult {
    Deadline,
    Cancelled,
    DriverUnavailable,
}

struct ReceiveCompletion {
    cell: Rc<RecvCell>,
    output: RecvOutput,
    driver_signal: Option<Rc<ReceiveSignal>>,
}

struct CloseCompletion {
    cell: Rc<CloseCell>,
    output: CloseOutput,
}

struct DeliveryEffects {
    receive: Option<ReceiveCompletion>,
    closes: Vec<CloseCompletion>,
}

impl DeliveryEffects {
    fn complete(self) {
        if let Some(receive) = self.receive {
            receive.complete();
        }
        complete_closes(self.closes);
    }
}

impl State {
    fn allocate_operation_key(&mut self) -> OperationKey {
        let value = NonZeroU64::new(self.next_operation_key)
            .expect("simulated datagram operation key is nonzero");
        self.next_operation_key = value
            .get()
            .checked_add(1)
            .expect("simulated datagram operation key space exhausted");
        OperationKey(value)
    }

    fn allocate_socket_key(&mut self) -> SocketKey {
        let value = NonZeroU64::new(self.next_socket_key)
            .expect("simulated datagram socket key is nonzero");
        self.next_socket_key = value
            .get()
            .checked_add(1)
            .expect("simulated datagram socket key space exhausted");
        SocketKey(value)
    }

    fn ensure_direction(
        &mut self,
        direction: DatagramDirection,
    ) -> Result<&mut DirectionState, DatagramError> {
        if !self.directions.contains_key(&direction) {
            if self.directions.len() >= self.config.max_directions {
                return Err(DatagramError::ResourceExhausted {
                    resource: "datagram directions",
                    limit: self.config.max_directions,
                });
            }
            self.directions.insert(
                direction,
                DirectionState {
                    config: self.config.default_link,
                    next_send_ordinal: 1,
                    faults: BTreeMap::new(),
                    scheduled_datagrams: 0,
                    scheduled_bytes: 0,
                },
            );
        }
        Ok(self
            .directions
            .get_mut(&direction)
            .expect("direction inserted or already present"))
    }

    fn push_fault(&mut self, fault: ScriptedDatagramFault) -> Result<(), DatagramError> {
        if fault.send_ordinal == 0 {
            return Err(DatagramError::InvalidRequest {
                reason: "scripted send ordinal must be one-based",
            });
        }
        if self.pending_faults >= self.config.max_scripted_faults {
            return Err(DatagramError::ResourceExhausted {
                resource: "scripted datagram faults",
                limit: self.config.max_scripted_faults,
            });
        }
        let max_faults = self.config.max_faults_per_send;
        let direction = self.ensure_direction(fault.direction)?;
        if fault.send_ordinal < direction.next_send_ordinal {
            return Err(DatagramError::InvalidRequest {
                reason: "cannot script a datagram send ordinal that already passed",
            });
        }
        let existing = direction
            .faults
            .get(&fault.send_ordinal)
            .map_or(&[][..], Vec::as_slice);
        if existing.len() >= max_faults {
            return Err(DatagramError::ResourceExhausted {
                resource: "fault actions per datagram send",
                limit: max_faults,
            });
        }
        validate_fault_composition(existing, &fault.action)?;
        direction
            .faults
            .entry(fault.send_ordinal)
            .or_default()
            .push(ScriptedAction {
                tag: fault.tag,
                action: fault.action,
            });
        self.pending_faults += 1;
        Ok(())
    }

    fn status(&self) -> SimDatagramStatus {
        let mut queued_datagrams = 0usize;
        let mut queued_bytes = 0usize;
        let mut pending_receives = 0usize;
        let mut closing_sockets = 0usize;
        for socket in self.sockets.values() {
            queued_datagrams += socket.queued.len();
            queued_bytes += socket.queued_bytes;
            pending_receives += socket.pending_receives.len();
            closing_sockets += usize::from(socket.closing);
        }
        SimDatagramStatus {
            sockets: self.sockets.len(),
            closing_sockets,
            inflight_operations: self.permits.in_use(),
            scheduled_datagrams: self.scheduled_datagrams,
            scheduled_bytes: self.scheduled_bytes,
            queued_datagrams,
            queued_bytes,
            pending_receives,
            directions: self.directions.len(),
            pending_faults: self.pending_faults,
            counters: self.counters,
        }
    }

    fn submit_bind(
        state: &Rc<RefCell<Self>>,
        request: DatagramBindRequest<NetworkAddress>,
    ) -> SimDatagramOperation<CompletionResult<SimDatagramSocket, DatagramFailure>> {
        let (permit, error, socket_key) = {
            let mut inner = state.borrow_mut();
            let permit = inner.permits.acquire();
            let error = if permit.is_none() {
                Some(DatagramError::ResourceExhausted {
                    resource: "inflight datagram operations",
                    limit: inner.config.max_inflight_operations,
                })
            } else if inner.bindings.contains_key(&request.address) {
                Some(DatagramError::AddressInUse)
            } else if inner.sockets.len() >= inner.config.max_sockets {
                Some(DatagramError::ResourceExhausted {
                    resource: "datagram sockets",
                    limit: inner.config.max_sockets,
                })
            } else {
                None
            };
            let socket_key = error.is_none().then(|| inner.allocate_socket_key());
            (permit, error, socket_key)
        };

        if let Some(error) = error {
            return SimDatagramOperation::ready_with_permit(
                permit,
                Err(CompletionError::not_applied(
                    DatagramFailure::without_buffer(error),
                )),
            );
        }
        let socket_key = socket_key.expect("successful bind allocated a socket key");

        {
            let mut inner = state.borrow_mut();
            inner.bindings.insert(request.address, socket_key);
            inner.sockets.insert(
                socket_key,
                SocketState {
                    address: request.address,
                    closing: false,
                    queued: VecDeque::new(),
                    queued_bytes: 0,
                    pending_receives: VecDeque::new(),
                    prior_operations: BTreeSet::new(),
                    close_waiters: Vec::new(),
                },
            );
        }
        SimDatagramOperation::ready_with_permit(
            permit,
            Ok(SimDatagramSocket {
                state: Rc::clone(state),
                socket_key,
                address: request.address,
            }),
        )
    }

    fn submit_send(
        state: &Rc<RefCell<Self>>,
        socket_key: SocketKey,
        request: SendToRequest<NetworkAddress>,
    ) -> SimDatagramOperation<SendOutput> {
        let (permit, closed) = {
            let mut inner = state.borrow_mut();
            inner.counters.sends_attempted = inner.counters.sends_attempted.saturating_add(1);
            let closed = inner
                .sockets
                .get(&socket_key)
                .is_none_or(|socket| socket.closing);
            let permit = (!closed).then(|| inner.permits.acquire()).flatten();
            (permit, closed)
        };
        if closed {
            return SimDatagramOperation::ready(Err(CompletionError::not_applied(
                DatagramFailure::with_buffer(DatagramError::SocketClosed, request.buffer, 0),
            )));
        }
        let Some(permit) = permit else {
            let limit = state.borrow().config.max_inflight_operations;
            return SimDatagramOperation::ready(Err(CompletionError::not_applied(
                DatagramFailure::with_buffer(
                    DatagramError::ResourceExhausted {
                        resource: "inflight datagram operations",
                        limit,
                    },
                    request.buffer,
                    0,
                ),
            )));
        };
        let operation_key = state.borrow_mut().allocate_operation_key();
        let cell = Rc::new(SendCell::new(Some(permit)));
        let operation = SimDatagramOperation::from_cell(Rc::clone(&cell));

        let plan = {
            let mut inner = state.borrow_mut();
            inner.plan_send(socket_key, operation_key, request)
        };
        let plan = match plan {
            Ok(plan) => plan,
            Err(failure) => {
                cell.complete(Err(CompletionError::not_applied(failure)));
                return operation;
            }
        };

        let admitted = Rc::new(Cell::new(false));
        let handle = state.borrow().handle.clone();
        let was_dropped = plan.dropped;
        let mut delivery_task = None;
        let rollback_delivery = if let Some(delivery_at) = plan.delivery_at {
            let control = Rc::new(DeliveryControl {
                state: Rc::clone(state),
                direction: plan.direction,
                packets: RefCell::new(plan.packets),
            });
            let task_control = Rc::clone(&control);
            let task_handle = handle.clone();
            let spawned = handle.spawn(async move {
                let mut guard = DeliveryTaskGuard {
                    control: task_control,
                    delivered: false,
                };
                if task_handle.sleep_until(delivery_at).await.is_ok() {
                    guard.deliver();
                }
            });
            match spawned {
                Ok(task) => {
                    delivery_task = Some(task);
                    Some(control)
                }
                Err(_) => {
                    control.cancel();
                    let closes = state
                        .borrow_mut()
                        .finish_socket_operation(plan.socket_key, plan.operation_key);
                    cell.complete(plan.completion.driver_unavailable(false));
                    complete_closes(closes);
                    return operation;
                }
            }
        } else {
            debug_assert!(plan.packets.is_empty());
            None
        };
        let mut guard = SendTaskGuard {
            state: Rc::clone(state),
            socket_key: plan.socket_key,
            operation_key: plan.operation_key,
            completion: Some(plan.completion),
            cell: Some(cell),
            admitted: Rc::clone(&admitted),
            rollback_delivery,
        };
        let task_handle = handle.clone();
        let spawned = handle.spawn(async move {
            if task_handle.sleep_until(plan.completion_at).await.is_err() {
                return;
            }
            if guard.admitted.get() {
                guard.complete_send();
            }
        });
        if spawned.is_ok() {
            admitted.set(true);
            let mut inner = state.borrow_mut();
            inner.counters.sends_admitted = inner.counters.sends_admitted.saturating_add(1);
            inner.counters.dropped_by_fault_or_partition = inner
                .counters
                .dropped_by_fault_or_partition
                .saturating_add(u64::from(was_dropped));
        } else if let Some(task) = delivery_task {
            task.abort();
        }
        operation
    }

    fn plan_send(
        &mut self,
        socket_key: SocketKey,
        operation_key: OperationKey,
        request: SendToRequest<NetworkAddress>,
    ) -> Result<SendPlan, DatagramFailure> {
        let Some(socket) = self.sockets.get(&socket_key) else {
            return Err(DatagramFailure::with_buffer(
                DatagramError::SocketClosed,
                request.buffer,
                0,
            ));
        };
        if socket.closing {
            return Err(DatagramFailure::with_buffer(
                DatagramError::SocketClosed,
                request.buffer,
                0,
            ));
        }
        if request.buffer.len() > self.config.max_datagram_bytes {
            return Err(DatagramFailure::with_buffer(
                DatagramError::MessageTooLarge {
                    max_payload_bytes: Some(self.config.max_datagram_bytes),
                },
                request.buffer,
                0,
            ));
        }
        if request.buffer.capacity() > self.config.max_operation_bytes {
            return Err(DatagramFailure::with_buffer(
                DatagramError::ResourceExhausted {
                    resource: "datagram send operation bytes",
                    limit: self.config.max_operation_bytes,
                },
                request.buffer,
                0,
            ));
        }
        let source = socket.address;
        let direction_key = DatagramDirection {
            source,
            destination: request.destination,
        };
        let max_duplicate_copies = self.config.max_duplicate_copies;
        let (link, actions) = {
            let direction = match self.ensure_direction(direction_key) {
                Ok(direction) => direction,
                Err(error) => {
                    return Err(DatagramFailure::with_buffer(error, request.buffer, 0));
                }
            };
            let ordinal = direction.next_send_ordinal;
            let next_ordinal = ordinal
                .checked_add(1)
                .expect("simulated datagram send ordinal space exhausted");
            direction.next_send_ordinal = next_ordinal;
            (
                direction.config,
                direction.faults.remove(&ordinal).unwrap_or_default(),
            )
        };
        self.pending_faults = self.pending_faults.saturating_sub(actions.len());
        self.counters.fault_hits = self
            .counters
            .fault_hits
            .saturating_add(u64::try_from(actions.len()).unwrap_or(u64::MAX));

        if let Some(action) = actions.first()
            && matches!(
                action.action,
                SimDatagramFault::FailBefore | SimDatagramFault::QueueFull
            )
        {
            let error = match action.action {
                SimDatagramFault::FailBefore => DatagramError::Injected { tag: action.tag },
                SimDatagramFault::QueueFull => DatagramError::ResourceExhausted {
                    resource: "simulated datagram delivery queue",
                    limit: self.config.max_scheduled_datagrams,
                },
                _ => unreachable!(),
            };
            let completion_at =
                match checked_deadline(self.handle.now(), link.send_completion_latency) {
                    Ok(deadline) => deadline,
                    Err(error) => {
                        return Err(DatagramFailure::with_buffer(error, request.buffer, 0));
                    }
                };
            self.sockets
                .get_mut(&socket_key)
                .expect("validated socket remains present during admission")
                .prior_operations
                .insert(operation_key);
            return Ok(SendPlan {
                operation_key,
                socket_key,
                direction: direction_key,
                completion_at,
                delivery_at: None,
                packets: Vec::new(),
                completion: PendingSendCompletion {
                    buffer: request.buffer,
                    terminal: SendTerminal::Failure {
                        error,
                        certainty: CompletionCertainty::NotApplied,
                        bytes_transferred: 0,
                    },
                    effect_applied: false,
                },
                dropped: false,
            });
        }

        let mut payload = request.buffer.clone();
        let mut extra_delay = SimDuration::ZERO;
        let mut additional_copies = 0usize;
        let dropped = link.partitioned
            || actions.iter().any(|action| {
                matches!(
                    action.action,
                    SimDatagramFault::Drop | SimDatagramFault::Partition
                )
            });
        let mut after_error = None;
        for action in &actions {
            match action.action {
                SimDatagramFault::Drop | SimDatagramFault::Partition => {}
                SimDatagramFault::Delay { additional } if !dropped => {
                    let Some(combined) = extra_delay.checked_add(additional) else {
                        return Err(DatagramFailure::with_buffer(
                            DatagramError::InvalidRequest {
                                reason: "simulated delivery delay overflowed",
                            },
                            request.buffer,
                            0,
                        ));
                    };
                    extra_delay = combined;
                }
                SimDatagramFault::Duplicate {
                    additional_copies: copies,
                } if !dropped => {
                    let Some(combined) = additional_copies.checked_add(copies) else {
                        return Err(DatagramFailure::with_buffer(
                            DatagramError::InvalidRequest {
                                reason: "simulated duplicate count overflowed",
                            },
                            request.buffer,
                            0,
                        ));
                    };
                    additional_copies = combined;
                    if additional_copies > max_duplicate_copies {
                        return Err(DatagramFailure::with_buffer(
                            DatagramError::ResourceExhausted {
                                resource: "duplicate datagram copies",
                                limit: max_duplicate_copies,
                            },
                            request.buffer,
                            0,
                        ));
                    }
                }
                SimDatagramFault::Delay { .. } | SimDatagramFault::Duplicate { .. } => {}
                SimDatagramFault::Corrupt { offset, xor } => {
                    if !payload.is_empty() && xor != 0 {
                        let index = offset % payload.len();
                        payload[index] ^= xor;
                    }
                }
                SimDatagramFault::Truncate { len } => payload.truncate(len),
                SimDatagramFault::ErrorAfterEnqueue { certainty } => {
                    after_error = Some((action.tag, certainty));
                }
                SimDatagramFault::FailBefore | SimDatagramFault::QueueFull => unreachable!(
                    "fault composition validation makes before-enqueue actions exclusive"
                ),
            }
        }
        let copy_count = if dropped {
            0
        } else {
            let Some(copy_count) = additional_copies.checked_add(1) else {
                return Err(DatagramFailure::with_buffer(
                    DatagramError::ResourceExhausted {
                        resource: "duplicate datagram copies",
                        limit: max_duplicate_copies,
                    },
                    request.buffer,
                    0,
                ));
            };
            copy_count
        };
        let now = self.handle.now();
        let completion_at = match checked_deadline(now, link.send_completion_latency) {
            Ok(deadline) => deadline,
            Err(error) => {
                return Err(DatagramFailure::with_buffer(error, request.buffer, 0));
            }
        };
        let delivery_at = if copy_count == 0 {
            None
        } else {
            let Some(delivery_delay) = link.delivery_latency.checked_add(extra_delay) else {
                return Err(DatagramFailure::with_buffer(
                    DatagramError::InvalidRequest {
                        reason: "simulated delivery delay overflowed",
                    },
                    request.buffer,
                    0,
                ));
            };
            match checked_deadline(now, delivery_delay) {
                Ok(deadline) => Some(deadline),
                Err(error) => {
                    return Err(DatagramFailure::with_buffer(error, request.buffer, 0));
                }
            }
        };
        let Some(retained_bytes) = payload.len().checked_mul(copy_count) else {
            return Err(DatagramFailure::with_buffer(
                DatagramError::ResourceExhausted {
                    resource: "scheduled datagram bytes",
                    limit: self.config.max_scheduled_bytes,
                },
                request.buffer,
                0,
            ));
        };
        if let Err(error) = self.reserve_deliveries(direction_key, copy_count, retained_bytes) {
            self.counters.delivery_admission_rejections = self
                .counters
                .delivery_admission_rejections
                .saturating_add(1);
            return Err(DatagramFailure::with_buffer(error, request.buffer, 0));
        }

        let packets = (0..copy_count)
            .map(|_| Packet {
                source,
                destination: request.destination,
                payload: payload.clone(),
            })
            .collect();
        self.sockets
            .get_mut(&socket_key)
            .expect("validated socket remains present during admission")
            .prior_operations
            .insert(operation_key);
        let terminal = after_error.map_or(SendTerminal::Success, |(tag, certainty)| {
            SendTerminal::Failure {
                error: DatagramError::Injected { tag },
                certainty: match certainty {
                    SimDatagramAfterEnqueueCertainty::Applied => CompletionCertainty::Applied,
                    SimDatagramAfterEnqueueCertainty::MayHaveApplied => {
                        CompletionCertainty::MayHaveApplied
                    }
                },
                bytes_transferred: request.buffer.len(),
            }
        });
        Ok(SendPlan {
            operation_key,
            socket_key,
            direction: direction_key,
            completion_at,
            delivery_at,
            packets,
            completion: PendingSendCompletion {
                buffer: request.buffer,
                terminal,
                effect_applied: true,
            },
            dropped,
        })
    }

    fn reserve_deliveries(
        &mut self,
        direction_key: DatagramDirection,
        count: usize,
        bytes: usize,
    ) -> Result<(), DatagramError> {
        if count == 0 {
            return Ok(());
        }
        let global_count = self
            .scheduled_datagrams
            .checked_add(count)
            .filter(|value| *value <= self.config.max_scheduled_datagrams)
            .ok_or(DatagramError::ResourceExhausted {
                resource: "scheduled datagrams",
                limit: self.config.max_scheduled_datagrams,
            })?;
        let global_bytes = self
            .scheduled_bytes
            .checked_add(bytes)
            .filter(|value| *value <= self.config.max_scheduled_bytes)
            .ok_or(DatagramError::ResourceExhausted {
                resource: "scheduled datagram bytes",
                limit: self.config.max_scheduled_bytes,
            })?;
        let direction = self
            .directions
            .get_mut(&direction_key)
            .expect("send planning ensured direction state");
        let direction_count = direction
            .scheduled_datagrams
            .checked_add(count)
            .filter(|value| *value <= direction.config.max_scheduled_datagrams)
            .ok_or(DatagramError::ResourceExhausted {
                resource: "directional scheduled datagrams",
                limit: direction.config.max_scheduled_datagrams,
            })?;
        let direction_bytes = direction.scheduled_bytes.checked_add(bytes).ok_or(
            DatagramError::ResourceExhausted {
                resource: "directional scheduled datagram bytes",
                limit: self.config.max_scheduled_bytes,
            },
        )?;
        self.scheduled_datagrams = global_count;
        self.scheduled_bytes = global_bytes;
        direction.scheduled_datagrams = direction_count;
        direction.scheduled_bytes = direction_bytes;
        Ok(())
    }

    fn release_reservations(
        &mut self,
        direction_key: DatagramDirection,
        count: usize,
        bytes: usize,
    ) {
        self.scheduled_datagrams = self.scheduled_datagrams.saturating_sub(count);
        self.scheduled_bytes = self.scheduled_bytes.saturating_sub(bytes);
        if let Some(direction) = self.directions.get_mut(&direction_key) {
            direction.scheduled_datagrams = direction.scheduled_datagrams.saturating_sub(count);
            direction.scheduled_bytes = direction.scheduled_bytes.saturating_sub(bytes);
        }
    }

    fn deliver_packet(&mut self, direction: DatagramDirection, packet: Packet) -> DeliveryEffects {
        self.release_reservations(direction, 1, packet.payload.len());
        self.counters.delivery_events = self.counters.delivery_events.saturating_add(1);
        let Some(&socket_key) = self.bindings.get(&packet.destination) else {
            self.counters.dropped_unbound = self.counters.dropped_unbound.saturating_add(1);
            return DeliveryEffects {
                receive: None,
                closes: Vec::new(),
            };
        };
        let Some(socket) = self.sockets.get_mut(&socket_key) else {
            self.counters.dropped_unbound = self.counters.dropped_unbound.saturating_add(1);
            return DeliveryEffects {
                receive: None,
                closes: Vec::new(),
            };
        };
        if socket.closing {
            self.counters.dropped_unbound = self.counters.dropped_unbound.saturating_add(1);
            return DeliveryEffects {
                receive: None,
                closes: Vec::new(),
            };
        }
        if let Some(pending) = socket.pending_receives.pop_front() {
            socket.prior_operations.remove(&pending.operation_key);
            let output = receive_success(pending.request, packet.source, &packet.payload);
            self.counters.delivered_to_receivers =
                self.counters.delivered_to_receivers.saturating_add(1);
            let closes = self.maybe_finish_close(socket_key);
            return DeliveryEffects {
                receive: Some(ReceiveCompletion {
                    cell: pending.cell,
                    output,
                    driver_signal: pending.driver_signal,
                }),
                closes,
            };
        }
        let count_full = socket.queued.len() >= self.config.max_queued_datagrams_per_socket;
        let bytes_full = socket
            .queued_bytes
            .checked_add(packet.payload.len())
            .is_none_or(|bytes| bytes > self.config.max_queued_bytes_per_socket);
        if count_full || bytes_full {
            self.counters.dropped_ingress_full =
                self.counters.dropped_ingress_full.saturating_add(1);
        } else {
            socket.queued_bytes += packet.payload.len();
            socket.queued.push_back(packet);
            self.counters.queued_at_receivers = self.counters.queued_at_receivers.saturating_add(1);
        }
        DeliveryEffects {
            receive: None,
            closes: Vec::new(),
        }
    }

    fn submit_receive(
        state: &Rc<RefCell<Self>>,
        socket_key: SocketKey,
        request: RecvFromRequest,
        mode: ReceiveMode,
    ) -> SimDatagramOperation<RecvOutput> {
        let (permit, closed) = {
            let inner = state.borrow();
            let closed = inner
                .sockets
                .get(&socket_key)
                .is_none_or(|socket| socket.closing);
            let permit = (!closed).then(|| inner.permits.acquire()).flatten();
            (permit, closed)
        };
        if closed {
            return SimDatagramOperation::ready(Err(CompletionError::not_applied(
                DatagramFailure::with_buffer(DatagramError::SocketClosed, request.buffer, 0),
            )));
        }
        let Some(permit) = permit else {
            let limit = state.borrow().config.max_inflight_operations;
            return SimDatagramOperation::ready(Err(CompletionError::not_applied(
                DatagramFailure::with_buffer(
                    DatagramError::ResourceExhausted {
                        resource: "inflight datagram operations",
                        limit,
                    },
                    request.buffer,
                    0,
                ),
            )));
        };
        let operation_key = state.borrow_mut().allocate_operation_key();
        let cell = Rc::new(RecvCell::new(Some(permit)));
        let operation = SimDatagramOperation::from_cell(Rc::clone(&cell));

        let deadline = match mode {
            ReceiveMode::Deadline(deadline) => Some(deadline),
            ReceiveMode::Wait | ReceiveMode::Try => None,
        };
        let driver_signal = match mode {
            ReceiveMode::Wait | ReceiveMode::Deadline(_) => Some(Rc::new(ReceiveSignal::new())),
            ReceiveMode::Try => None,
        };
        let mut immediate = None;
        let mut spawn_driver = None;
        let mut receive_completions = Vec::new();
        let mut close_completions = Vec::new();
        {
            let mut inner = state.borrow_mut();
            let total_bytes = request.buffer.len().checked_add(request.max_bytes);
            if request.buffer.capacity() > inner.config.max_operation_bytes
                || total_bytes.is_none_or(|bytes| bytes > inner.config.max_operation_bytes)
            {
                immediate = Some(Err(CompletionError::not_applied(
                    DatagramFailure::with_buffer(
                        DatagramError::ResourceExhausted {
                            resource: "datagram receive operation bytes",
                            limit: inner.config.max_operation_bytes,
                        },
                        request.buffer,
                        0,
                    ),
                )));
            } else if deadline.is_some_and(|deadline| deadline <= inner.handle.now()) {
                immediate = Some(Err(CompletionError::not_applied(
                    DatagramFailure::with_buffer(
                        DatagramError::DeadlineExceeded,
                        request.buffer,
                        0,
                    ),
                )));
            } else if !inner.sockets.contains_key(&socket_key)
                || inner
                    .sockets
                    .get(&socket_key)
                    .is_some_and(|socket| socket.closing)
            {
                immediate = Some(Err(CompletionError::not_applied(
                    DatagramFailure::with_buffer(DatagramError::SocketClosed, request.buffer, 0),
                )));
            } else {
                let max_pending = inner.config.max_pending_receives_per_socket;
                let socket = inner
                    .sockets
                    .get_mut(&socket_key)
                    .expect("socket existence checked above");
                if socket.pending_receives.len() >= max_pending {
                    immediate = Some(Err(CompletionError::not_applied(
                        DatagramFailure::with_buffer(
                            DatagramError::ResourceExhausted {
                                resource: "pending datagram receives per socket",
                                limit: max_pending,
                            },
                            request.buffer,
                            0,
                        ),
                    )));
                } else {
                    socket.pending_receives.push_back(PendingReceive {
                        operation_key,
                        request,
                        driver_signal: driver_signal.clone(),
                        cell: Rc::clone(&cell),
                    });
                    socket.prior_operations.insert(operation_key);
                    receive_completions = inner.service_available_packets(socket_key);
                    if matches!(mode, ReceiveMode::Try)
                        && inner.receive_is_pending(socket_key, operation_key)
                    {
                        let pending = inner
                            .remove_pending_receive(socket_key, operation_key)
                            .expect("new try receive remains pending");
                        close_completions =
                            inner.finish_socket_operation(socket_key, operation_key);
                        immediate = Some(Err(CompletionError::not_applied(
                            DatagramFailure::with_buffer(
                                DatagramError::WouldBlock,
                                pending.request.buffer,
                                0,
                            ),
                        )));
                    } else if inner.receive_is_pending(socket_key, operation_key) {
                        spawn_driver = Some(mode);
                    }
                }
            }
        }
        for completion in receive_completions {
            completion.complete();
        }
        complete_closes(close_completions);
        if let Some(output) = immediate {
            cell.complete(output);
            return operation;
        }
        if let Some(driver_mode) = spawn_driver {
            let handle = state.borrow().handle.clone();
            let signal = driver_signal.expect("pending receive has a driver signal");
            match driver_mode {
                ReceiveMode::Wait => {
                    let guard = ReceiveTaskGuard {
                        state: Rc::clone(state),
                        socket_key,
                        operation_key,
                        armed: true,
                    };
                    let _ = handle.spawn(async move {
                        let mut guard = guard;
                        SignalWait { signal }.await;
                        guard.disarm();
                    });
                }
                ReceiveMode::Deadline(deadline) => {
                    let guard = ReceiveTaskGuard {
                        state: Rc::clone(state),
                        socket_key,
                        operation_key,
                        armed: true,
                    };
                    let sleep = handle.sleep_until(deadline);
                    let _ = handle.spawn(async move {
                        let mut guard = guard;
                        let wait = DeadlineWait { sleep, signal };
                        match wait.await {
                            DeadlineWaitResult::Deadline => guard.expire(),
                            DeadlineWaitResult::Cancelled => guard.disarm(),
                            DeadlineWaitResult::DriverUnavailable => {}
                        }
                    });
                }
                ReceiveMode::Try => {
                    unreachable!("nonblocking receive never retains a driver task")
                }
            }
        }
        operation
    }

    fn service_available_packets(&mut self, socket_key: SocketKey) -> Vec<ReceiveCompletion> {
        let mut completions = Vec::new();
        loop {
            let pair = {
                let Some(socket) = self.sockets.get_mut(&socket_key) else {
                    break;
                };
                match (
                    socket.pending_receives.pop_front(),
                    socket.queued.pop_front(),
                ) {
                    (Some(pending), Some(packet)) => {
                        socket.queued_bytes =
                            socket.queued_bytes.saturating_sub(packet.payload.len());
                        socket.prior_operations.remove(&pending.operation_key);
                        Some((pending, packet))
                    }
                    (Some(pending), None) => {
                        socket.pending_receives.push_front(pending);
                        None
                    }
                    (None, Some(packet)) => {
                        socket.queued.push_front(packet);
                        None
                    }
                    (None, None) => None,
                }
            };
            let Some((pending, packet)) = pair else {
                break;
            };
            let output = receive_success(pending.request, packet.source, &packet.payload);
            completions.push(ReceiveCompletion {
                cell: pending.cell,
                output,
                driver_signal: pending.driver_signal,
            });
        }
        completions
    }

    fn receive_is_pending(&self, socket_key: SocketKey, operation_key: OperationKey) -> bool {
        self.sockets.get(&socket_key).is_some_and(|socket| {
            socket
                .pending_receives
                .iter()
                .any(|pending| pending.operation_key == operation_key)
        })
    }

    fn remove_pending_receive(
        &mut self,
        socket_key: SocketKey,
        operation_key: OperationKey,
    ) -> Option<PendingReceive> {
        let socket = self.sockets.get_mut(&socket_key)?;
        let index = socket
            .pending_receives
            .iter()
            .position(|pending| pending.operation_key == operation_key)?;
        socket.pending_receives.remove(index)
    }

    fn finish_socket_operation(
        &mut self,
        socket_key: SocketKey,
        operation_key: OperationKey,
    ) -> Vec<CloseCompletion> {
        if let Some(socket) = self.sockets.get_mut(&socket_key) {
            socket.prior_operations.remove(&operation_key);
        }
        self.maybe_finish_close(socket_key)
    }

    fn maybe_finish_close(&mut self, socket_key: SocketKey) -> Vec<CloseCompletion> {
        let should_finish = self
            .sockets
            .get(&socket_key)
            .is_some_and(|socket| socket.closing && socket.prior_operations.is_empty());
        if !should_finish {
            return Vec::new();
        }
        let socket = self
            .sockets
            .remove(&socket_key)
            .expect("close completion checked socket existence");
        if self.bindings.get(&socket.address) == Some(&socket_key) {
            self.bindings.remove(&socket.address);
        }
        socket
            .close_waiters
            .into_iter()
            .map(|waiter| CloseCompletion {
                output: Ok(()),
                cell: waiter.cell,
            })
            .collect()
    }

    fn submit_close(
        state: &Rc<RefCell<Self>>,
        socket_key: SocketKey,
    ) -> SimDatagramOperation<CloseOutput> {
        let (permit, already_closed) = {
            let inner = state.borrow();
            let already_closed = !inner.sockets.contains_key(&socket_key);
            let permit = (!already_closed).then(|| inner.permits.acquire()).flatten();
            (permit, already_closed)
        };
        if already_closed {
            return SimDatagramOperation::ready(Ok(()));
        }
        let Some(permit) = permit else {
            let limit = state.borrow().config.max_inflight_operations;
            return SimDatagramOperation::ready(Err(CompletionError::not_applied(
                DatagramFailure::without_buffer(DatagramError::ResourceExhausted {
                    resource: "inflight datagram operations",
                    limit,
                }),
            )));
        };
        let cell = Rc::new(CloseCell::new(Some(permit)));
        let operation = SimDatagramOperation::from_cell(Rc::clone(&cell));
        let completions = {
            let mut inner = state.borrow_mut();
            inner.begin_close(
                socket_key,
                Some(CloseWaiter {
                    cell: Rc::clone(&cell),
                }),
            )
        };
        let Some((receives, closes)) = completions else {
            cell.complete(Ok(()));
            return operation;
        };
        for receive in receives {
            receive.complete();
        }
        complete_closes(closes);
        operation
    }

    fn drop_socket(state: &Rc<RefCell<Self>>, socket_key: SocketKey) {
        let Some((receives, closes)) = state.borrow_mut().begin_close(socket_key, None) else {
            return;
        };
        for receive in receives {
            receive.complete();
        }
        complete_closes(closes);
    }

    /// Starts closing a socket: marks it closing, fails every pending receive
    /// with `SocketClosed`, and discards its queued datagrams. `close_waiter`
    /// is registered before close completion is attempted, so an explicit
    /// close observes its own operation. Returns `None` when the socket is
    /// already gone; otherwise the receive and close completions to run
    /// outside the state borrow.
    fn begin_close(
        &mut self,
        socket_key: SocketKey,
        close_waiter: Option<CloseWaiter>,
    ) -> Option<(Vec<ReceiveCompletion>, Vec<CloseCompletion>)> {
        let socket = self.sockets.get_mut(&socket_key)?;
        socket.closing = true;
        let mut receives = Vec::new();
        while let Some(pending) = socket.pending_receives.pop_front() {
            socket.prior_operations.remove(&pending.operation_key);
            receives.push(ReceiveCompletion {
                cell: pending.cell,
                output: Err(CompletionError::not_applied(DatagramFailure::with_buffer(
                    DatagramError::SocketClosed,
                    pending.request.buffer,
                    0,
                ))),
                driver_signal: pending.driver_signal,
            });
        }
        socket.queued.clear();
        socket.queued_bytes = 0;
        if let Some(waiter) = close_waiter {
            socket.close_waiters.push(waiter);
        }
        Some((receives, self.maybe_finish_close(socket_key)))
    }

    fn expire_receive(
        &mut self,
        socket_key: SocketKey,
        operation_key: OperationKey,
        error: DatagramError,
    ) -> Option<(ReceiveCompletion, Vec<CloseCompletion>)> {
        let pending = self.remove_pending_receive(socket_key, operation_key)?;
        let closes = self.finish_socket_operation(socket_key, operation_key);
        Some((
            ReceiveCompletion {
                cell: pending.cell,
                output: Err(CompletionError::not_applied(DatagramFailure::with_buffer(
                    error,
                    pending.request.buffer,
                    0,
                ))),
                driver_signal: pending.driver_signal,
            },
            closes,
        ))
    }
}

impl PendingSendCompletion {
    fn into_output(self) -> SendOutput {
        match self.terminal {
            SendTerminal::Success => {
                let bytes_sent = self.buffer.len();
                Ok(SendToResult {
                    buffer: self.buffer,
                    bytes_sent,
                })
            }
            SendTerminal::Failure {
                error,
                certainty,
                bytes_transferred,
            } => Err(CompletionError::new(
                certainty,
                DatagramFailure::with_buffer(error, self.buffer, bytes_transferred),
            )),
        }
    }

    fn driver_unavailable(self, admitted: bool) -> SendOutput {
        let certainty = if admitted && self.effect_applied {
            CompletionCertainty::Applied
        } else {
            CompletionCertainty::NotApplied
        };
        let bytes_transferred = if certainty == CompletionCertainty::Applied {
            self.buffer.len()
        } else {
            0
        };
        Err(CompletionError::new(
            certainty,
            DatagramFailure::with_buffer(
                DatagramError::CompletionDriverUnavailable,
                self.buffer,
                bytes_transferred,
            ),
        ))
    }
}

impl SendTaskGuard {
    fn complete_send(&mut self) {
        let Some(completion) = self.completion.take() else {
            return;
        };
        let closes = self
            .state
            .borrow_mut()
            .finish_socket_operation(self.socket_key, self.operation_key);
        let output = completion.into_output();
        if let Some(cell) = self.cell.take() {
            cell.complete(output);
        }
        self.rollback_delivery.take();
        complete_closes(closes);
    }
}

impl Drop for SendTaskGuard {
    fn drop(&mut self) {
        let Some(completion) = self.completion.take() else {
            return;
        };
        let admitted = self.admitted.get();
        if !admitted && let Some(delivery) = self.rollback_delivery.take() {
            delivery.cancel();
        }
        let closes = self
            .state
            .borrow_mut()
            .finish_socket_operation(self.socket_key, self.operation_key);
        let output = completion.driver_unavailable(admitted);
        if let Some(cell) = self.cell.take() {
            cell.complete(output);
        }
        complete_closes(closes);
    }
}

impl DeliveryControl {
    fn deliver(&self) {
        loop {
            let packet = {
                let mut packets = self.packets.borrow_mut();
                if packets.is_empty() {
                    break;
                }
                packets.remove(0)
            };
            let effects = self
                .state
                .borrow_mut()
                .deliver_packet(self.direction, packet);
            effects.complete();
        }
    }

    fn cancel(&self) {
        let packets = std::mem::take(&mut *self.packets.borrow_mut());
        if packets.is_empty() {
            return;
        }
        let count = packets.len();
        let bytes = packets.iter().map(|packet| packet.payload.len()).sum();
        self.state
            .borrow_mut()
            .release_reservations(self.direction, count, bytes);
    }
}

impl Drop for DeliveryControl {
    fn drop(&mut self) {
        self.cancel();
    }
}

impl DeliveryTaskGuard {
    fn deliver(&mut self) {
        self.control.deliver();
        self.delivered = true;
    }
}

impl Drop for DeliveryTaskGuard {
    fn drop(&mut self) {
        if !self.delivered {
            self.control.cancel();
        }
    }
}

impl ReceiveTaskGuard {
    fn expire(&mut self) {
        let effect = self.state.borrow_mut().expire_receive(
            self.socket_key,
            self.operation_key,
            DatagramError::DeadlineExceeded,
        );
        self.armed = false;
        if let Some((receive, closes)) = effect {
            receive.complete();
            complete_closes(closes);
        }
    }

    fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for ReceiveTaskGuard {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        let effect = self.state.borrow_mut().expire_receive(
            self.socket_key,
            self.operation_key,
            DatagramError::CompletionDriverUnavailable,
        );
        if let Some((receive, closes)) = effect {
            receive.complete();
            complete_closes(closes);
        }
    }
}

impl ReceiveSignal {
    fn new() -> Self {
        Self {
            cancelled: Cell::new(false),
            waker: RefCell::new(None),
        }
    }

    fn cancel(&self) {
        if self.cancelled.replace(true) {
            return;
        }
        let waker = self.waker.borrow_mut().take();
        if let Some(waker) = waker {
            contain_panic(|| waker.wake());
        }
    }
}

impl Future for DeadlineWait {
    type Output = DeadlineWaitResult;

    fn poll(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        if self.signal.cancelled.get() {
            return Poll::Ready(DeadlineWaitResult::Cancelled);
        }
        *self.signal.waker.borrow_mut() = Some(context.waker().clone());
        match Pin::new(&mut self.sleep).poll(context) {
            Poll::Ready(Ok(())) => {
                self.signal.cancelled.set(true);
                self.signal.waker.borrow_mut().take();
                Poll::Ready(DeadlineWaitResult::Deadline)
            }
            Poll::Ready(Err(_)) => {
                self.signal.cancelled.set(true);
                self.signal.waker.borrow_mut().take();
                Poll::Ready(DeadlineWaitResult::DriverUnavailable)
            }
            Poll::Pending if self.signal.cancelled.get() => {
                Poll::Ready(DeadlineWaitResult::Cancelled)
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

impl Future for SignalWait {
    type Output = ();

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        if self.signal.cancelled.get() {
            Poll::Ready(())
        } else {
            *self.signal.waker.borrow_mut() = Some(context.waker().clone());
            if self.signal.cancelled.get() {
                Poll::Ready(())
            } else {
                Poll::Pending
            }
        }
    }
}

impl ReceiveCompletion {
    fn complete(self) {
        if let Some(signal) = self.driver_signal {
            signal.cancel();
        }
        self.cell.complete(self.output);
    }
}

fn validate_config(config: SimDatagramConfig) -> Result<(), DatagramError> {
    for (value, reason) in [
        (config.max_sockets, "max_sockets must be nonzero"),
        (
            config.max_inflight_operations,
            "max_inflight_operations must be nonzero",
        ),
        (
            config.max_operation_bytes,
            "max_operation_bytes must be nonzero",
        ),
        (
            config.max_scheduled_datagrams,
            "max_scheduled_datagrams must be nonzero",
        ),
        (
            config.max_scheduled_bytes,
            "max_scheduled_bytes must be nonzero",
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
        (config.max_directions, "max_directions must be nonzero"),
        (
            config.max_scripted_faults,
            "max_scripted_faults must be nonzero",
        ),
        (
            config.max_faults_per_send,
            "max_faults_per_send must be nonzero",
        ),
        (
            config.default_link.max_scheduled_datagrams,
            "default link max_scheduled_datagrams must be nonzero",
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

fn validate_fault_composition(
    existing: &[ScriptedAction],
    action: &SimDatagramFault,
) -> Result<(), DatagramError> {
    let before = |action: &SimDatagramFault| {
        matches!(
            action,
            SimDatagramFault::FailBefore | SimDatagramFault::QueueFull
        )
    };
    if before(action) && !existing.is_empty() || existing.iter().any(|item| before(&item.action)) {
        return Err(DatagramError::InvalidRequest {
            reason: "FailBefore and QueueFull must be the only action for a send",
        });
    }
    if matches!(action, SimDatagramFault::ErrorAfterEnqueue { .. })
        && existing
            .iter()
            .any(|item| matches!(item.action, SimDatagramFault::ErrorAfterEnqueue { .. }))
    {
        return Err(DatagramError::InvalidRequest {
            reason: "at most one ErrorAfterEnqueue action may target a send",
        });
    }
    Ok(())
}

fn checked_deadline(now: SimInstant, delay: SimDuration) -> Result<SimInstant, DatagramError> {
    now.checked_add(delay).ok_or(DatagramError::InvalidRequest {
        reason: "simulated datagram event deadline overflowed",
    })
}

fn complete_closes(completions: Vec<CloseCompletion>) {
    for completion in completions {
        completion.cell.complete(completion.output);
    }
}
