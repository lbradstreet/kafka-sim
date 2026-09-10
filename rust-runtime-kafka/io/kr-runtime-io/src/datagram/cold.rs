//! Cold application-facing datagram operations over the warm submission
//! traits.
//!
//! [`ColdDatagramNetwork`] and [`ColdDatagramSocket`] are the default way for
//! application code to use the datagram transport: a never-polled operation
//! has not started. A successful `bind` returns a cold-wrapped socket, so
//! application code never falls back through the warm boundary accidentally.
//! The eager `submit_*` traits below remain available to providers and to
//! systems code that intentionally needs explicit submission. The staged
//! migration that produced this split is recorded in
//! `COLD-IO-FUTURES-PROPOSAL.md`.
//!
//! The lower socket handle is exclusive and releases its binding from `Drop`.
//! Each cold wrapper owns its warm handle through a private [`Arc`], and
//! every operation future retains one reference, so an admitted or unpolled
//! operation keeps the binding alive until the future is dropped. The
//! wrappers expose no public cloning.
//!
//! Deadlines remain absolute provider-native instants. A cold
//! `recv_from_until` registers nothing before its first poll; a future
//! constructed before its deadline and first polled after it observes the
//! expired deadline at admission and consumes no datagram.

use std::future::Future;
use std::sync::Arc;

use kr_runtime::CompletionResult;

use super::{
    DatagramBindRequest, DatagramFailure, DatagramProviderSubmit, DatagramSocketSubmit,
    RecvFromRequest, RecvFromResult, SendToRequest, SendToResult,
};

/// Cold datagram bind control plane over a warm provider.
///
/// Calling `bind` constructs an owned `'static` future and admits nothing;
/// the first poll attempts admission through the provider exactly once and
/// then follows the eager contract. Dropping a never-polled future binds no
/// address and consumes no capacity.
///
/// A cold future is [`Send`] exactly when the provider and its response
/// futures are. A [`SimDatagramNetwork`](super::SimDatagramNetwork)-backed
/// future is not:
///
/// ```compile_fail
/// fn requires_send<T: Send>(_value: T) {}
/// fn demand(
///     cold: &kr_runtime_io::datagram::ColdDatagramNetwork<kr_runtime_io::datagram::SimDatagramNetwork>,
/// ) {
///     let address = kr_runtime_io::network::NetworkAddress {
///         node: kr_runtime_io::network::NodeId(1),
///         port: 1,
///     };
///     requires_send(cold.bind(kr_runtime_io::datagram::DatagramBindRequest { address }));
/// }
/// ```
pub struct ColdDatagramNetwork<P> {
    provider: Arc<P>,
}

impl<P> ColdDatagramNetwork<P> {
    #[must_use]
    pub fn new(provider: P) -> Self {
        Self {
            provider: Arc::new(provider),
        }
    }
}

// Each operation defers to `crate::cold_submit`, which retains the provider
// or socket reference for the future's whole life and drops the warm response
// before it.
impl<P: DatagramProviderSubmit> ColdDatagramNetwork<P> {
    pub fn bind(
        &self,
        request: DatagramBindRequest<P::Address>,
    ) -> impl Future<Output = CompletionResult<ColdDatagramSocket<P::Socket>, DatagramFailure>>
    + 'static
    + use<P> {
        let submit = crate::cold_submit(Arc::clone(&self.provider), move |provider| {
            provider.submit_bind(request)
        });
        async move { submit.await.map(ColdDatagramSocket::new) }
    }
}

/// Cold bound datagram socket over a warm exclusive socket handle.
///
/// A never-polled send enqueues no datagram, a never-polled receive consumes
/// neither a datagram nor pending-receive capacity, and a never-polled
/// `close` submits no explicit close, though dropping the last retaining
/// handle or future still runs the socket's ordinary drop teardown. After an
/// admitting first poll the eager contract applies unchanged, including the
/// deadline, nonblocking, and close-fence rules.
pub struct ColdDatagramSocket<S> {
    socket: Arc<S>,
}

impl<S> ColdDatagramSocket<S> {
    /// Wraps an already-bound warm socket.
    #[must_use]
    pub fn new(socket: S) -> Self {
        Self {
            socket: Arc::new(socket),
        }
    }
}

impl<S: DatagramSocketSubmit> ColdDatagramSocket<S> {
    /// Returns the concrete bound address without driving I/O.
    #[must_use]
    pub fn local_addr(&self) -> S::Address {
        self.socket.local_addr()
    }

    pub fn send_to(
        &self,
        request: SendToRequest<S::Address>,
    ) -> impl Future<Output = CompletionResult<SendToResult, DatagramFailure>> + 'static + use<S>
    {
        crate::cold_submit(Arc::clone(&self.socket), move |socket| {
            socket.submit_send_to(request)
        })
    }

    pub fn recv_from(
        &self,
        request: RecvFromRequest,
    ) -> impl Future<Output = CompletionResult<RecvFromResult<S::Address>, DatagramFailure>>
    + 'static
    + use<S> {
        crate::cold_submit(Arc::clone(&self.socket), move |socket| {
            socket.submit_recv_from(request)
        })
    }

    pub fn try_recv_from(
        &self,
        request: RecvFromRequest,
    ) -> impl Future<Output = CompletionResult<RecvFromResult<S::Address>, DatagramFailure>>
    + 'static
    + use<S> {
        crate::cold_submit(Arc::clone(&self.socket), move |socket| {
            socket.submit_try_recv_from(request)
        })
    }

    pub fn recv_from_until(
        &self,
        request: RecvFromRequest,
        deadline: S::Instant,
    ) -> impl Future<Output = CompletionResult<RecvFromResult<S::Address>, DatagramFailure>>
    + 'static
    + use<S> {
        crate::cold_submit(Arc::clone(&self.socket), move |socket| {
            socket.submit_recv_from_until(request, deadline)
        })
    }

    pub fn close(
        &self,
    ) -> impl Future<Output = CompletionResult<(), DatagramFailure>> + 'static + use<S> {
        crate::cold_submit(Arc::clone(&self.socket), |socket| socket.submit_close())
    }
}

#[cfg(test)]
mod tests {
    use super::{ColdDatagramNetwork, ColdDatagramSocket};
    use crate::conformance::check_cold_datagram_provider;
    use crate::datagram::{
        DatagramBindRequest, MemoryDatagramConfig, MemoryDatagramNetwork, RecvFromRequest,
        SendToRequest, SimDatagramConfig, SimDatagramNetwork,
    };
    use crate::network::{NetworkAddress, NodeId};
    use kr_runtime::{RuntimeConfig, SimInstant, SimRuntime};

    fn address(node: u64, port: u16) -> NetworkAddress {
        NetworkAddress {
            node: NodeId(node),
            port,
        }
    }

    #[test]
    fn memory_datagram_network_passes_cold_conformance() {
        let network = MemoryDatagramNetwork::new(MemoryDatagramConfig::default())
            .expect("default memory datagram config is valid");
        let cold = ColdDatagramNetwork::new(network);
        let mut runtime = SimRuntime::default();
        runtime
            .block_on(async move {
                check_cold_datagram_provider(
                    &cold,
                    address(1, 4_001),
                    address(2, 4_002),
                    SimInstant::ZERO,
                )
                .await
            })
            .expect("runtime completes")
            .expect("memory datagram provider satisfies cold conformance");
    }

    #[test]
    fn simulated_datagram_network_passes_cold_conformance() {
        let mut runtime = SimRuntime::new(RuntimeConfig::default());
        let network = SimDatagramNetwork::new(runtime.handle(), SimDatagramConfig::default())
            .expect("valid datagram simulator config");
        let cold = ColdDatagramNetwork::new(network);
        runtime
            .block_on(async move {
                check_cold_datagram_provider(
                    &cold,
                    address(21, 14_001),
                    address(22, 14_002),
                    SimInstant::ZERO,
                )
                .await
            })
            .expect("runtime completes")
            .expect("simulated datagram provider satisfies cold conformance");
    }

    #[test]
    fn an_unpolled_future_retains_the_socket_capability() {
        let network = MemoryDatagramNetwork::new(MemoryDatagramConfig::default())
            .expect("default memory datagram config is valid");
        let mut runtime = SimRuntime::default();
        let (constructed, bound, peer) = runtime
            .block_on(async {
                let cold = ColdDatagramNetwork::new(network);
                let socket = cold
                    .bind(DatagramBindRequest {
                        address: address(5, 5_001),
                    })
                    .await
                    .expect("cold bind succeeds");
                let peer = cold
                    .bind(DatagramBindRequest {
                        address: address(6, 5_002),
                    })
                    .await
                    .expect("peer cold bind succeeds");
                let constructed = socket.recv_from(RecvFromRequest {
                    buffer: Vec::new(),
                    max_bytes: 8,
                });
                let bound = socket.local_addr();
                drop(socket);
                (constructed, bound, peer)
            })
            .expect("runtime completes");

        // The unpolled future holds the last socket capability; the binding
        // must stay live until that future is dropped.
        let received = runtime
            .block_on(async move {
                peer.send_to(SendToRequest {
                    buffer: b"ok".to_vec(),
                    destination: bound,
                })
                .await
                .expect("peer send succeeds");
                constructed.await
            })
            .expect("runtime completes")
            .expect("receive through the retained capability succeeds");
        assert_eq!(received.buffer, b"ok");
    }

    #[test]
    fn memory_backed_cold_futures_are_send() {
        fn assert_send<T: Send>(_future: T) {}

        let network = MemoryDatagramNetwork::new(MemoryDatagramConfig::default())
            .expect("default memory datagram config is valid");
        let cold = ColdDatagramNetwork::new(network);
        assert_send(cold.bind(DatagramBindRequest {
            address: address(1, 1),
        }));
        let mut runtime = SimRuntime::default();
        let socket = runtime
            .block_on(cold.bind(DatagramBindRequest {
                address: address(2, 2),
            }))
            .expect("runtime completes")
            .expect("cold bind succeeds");
        let _: &ColdDatagramSocket<_> = &socket;
        assert_send(socket.send_to(SendToRequest {
            buffer: Vec::new(),
            destination: address(3, 3),
        }));
        assert_send(socket.recv_from(RecvFromRequest {
            buffer: Vec::new(),
            max_bytes: 1,
        }));
        assert_send(socket.try_recv_from(RecvFromRequest {
            buffer: Vec::new(),
            max_bytes: 1,
        }));
        assert_send(socket.recv_from_until(
            RecvFromRequest {
                buffer: Vec::new(),
                max_bytes: 1,
            },
            SimInstant::ZERO,
        ));
        assert_send(socket.close());
    }
}
