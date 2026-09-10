//! Host/simulation connection setup boundary shared by Kafka client roles.
//! DNS, TLS and authentication stay outside passive workload state machines.
use crate::{
    config::BrokerEndpoint,
    control::{Capabilities, ControlError},
    transport::{ConnectionDriver, DriverConfig},
};
use kr_runtime::RuntimeInstant;
use kr_runtime_io::network::{ByteStreamVectoredSubmit, NetworkError};
use std::{fmt, future::Future, sync::Arc};

/// Shared control reserve left available for two concurrent data-lane handshakes.
pub const DATA_SETUP_BYTES: usize = 128 * 1024;
pub const DATA_SETUP_RESERVED_BYTES: usize = 2 * DATA_SETUP_BYTES;

#[derive(Clone)]
pub struct ConnectTarget {
    pub endpoint: BrokerEndpoint,
    pub broker_id: Option<i32>,
    pub lane: u8,
    pub deadline: RuntimeInstant,
    pub driver: DriverConfig,
    /// Carried into provider-owned buffers/terminal state before first admission.
    /// Dropping a setup observer or the actor cannot return live I/O capacity.
    pub lifetime_guard: Option<Arc<dyn Send + Sync>>,
}
impl fmt::Debug for ConnectTarget {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ConnectTarget")
            .field("endpoint", &self.endpoint)
            .field("broker_id", &self.broker_id)
            .field("lane", &self.lane)
            .field("deadline", &self.deadline)
            .field("driver", &self.driver)
            .field("guarded", &self.lifetime_guard.is_some())
            .finish()
    }
}
#[derive(Clone, Debug)]
#[non_exhaustive]
pub enum ConnectError {
    Network(NetworkError),
    Protocol(ControlError),
    Authentication,
    Timeout,
    ResourceExhausted,
    WorkerFailed,
    TransportUnavailable,
    InvalidConfiguration,
}
impl fmt::Display for ConnectError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "connection setup failed: {self:?}")
    }
}
impl std::error::Error for ConnectError {}

/// A negotiated, certificate-verified/authenticated connection when configured.
/// The original driver (including any admitted read) moves directly to the actor.
pub struct Connected<S: ByteStreamVectoredSubmit> {
    pub driver: ConnectionDriver<S>,
    pub capabilities: Capabilities,
}

/// One connector instance belongs to its client owner actor. Implementations
/// may reserve bounded DNS/auth jobs and create cold operations, but never spawn
/// hidden record tasks or perform blocking work from `connect` or a future poll.
/// Production authentication must use cryptographically secure nonce entropy;
/// deterministic test connectors inject their explicit replay source instead.
pub trait Connector: 'static {
    type Stream: ByteStreamVectoredSubmit;
    type ConnectFuture: Future<Output = Result<Connected<Self::Stream>, ConnectError>> + 'static;
    /// Optional diagnostics sink. This cold configuration hook must not start
    /// I/O, change connection policy, or consume randomness.
    fn set_telemetry(&mut self, _telemetry: Arc<crate::telemetry::TransportTelemetry>) {}
    /// Constructing a setup future admits nothing. First poll begins bounded
    /// connection setup, including capability discovery and configured authentication.
    /// After admission the actor retains the future through terminal completion,
    /// even when its request deadline has expired or close has been requested.
    fn connect(&mut self, target: ConnectTarget) -> Self::ConnectFuture;
}

/// Bounded setup memory supplied by the workload's resource authority. This
/// operation only reserves storage; it performs no I/O or application callbacks.
/// Host setup invokes it on first poll and attaches the returned guard to each
/// admitted worker/provider owner before that owner can outlive the observer.
/// A failed reservation leaves the authority unchanged. Guard destruction must
/// only return capacity and must not panic or invoke application work.
pub trait SetupBudget: Send + Sync {
    fn reserve(&self, bytes: usize) -> Result<Arc<dyn Send + Sync>, ConnectError>;
}
