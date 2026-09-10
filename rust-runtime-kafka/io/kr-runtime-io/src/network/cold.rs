//! Cold application-facing networking over the warm submission traits.
//!
//! [`ColdNetwork`], [`ColdListener`], and [`ColdStream`] are the default way
//! for application code to perform connected networking: a never-polled
//! operation has not started. Successful `listen`, `connect`, and `accept`
//! operations return cold-wrapped handles, so application code never falls
//! back through the warm boundary accidentally. The eager `submit_*` traits
//! below remain available to providers and to systems code that intentionally
//! needs explicit submission. The staged migration that produced this split
//! is recorded in `COLD-IO-FUTURES-PROPOSAL.md`.
//!
//! The lower listener and stream handles are exclusive and close from `Drop`.
//! Each cold wrapper therefore owns its warm handle through a private
//! [`Arc`], and every operation future retains one reference so an admitted
//! or unpolled operation keeps the resource alive until the future is
//! dropped. The wrappers expose no public cloning: exclusivity of the lower
//! handle remains the application-visible contract, and the shared ownership
//! exists only so futures can be owned and `'static`.

use std::future::Future;
use std::sync::Arc;

use kr_runtime::CompletionResult;

use super::{
    ByteStreamSubmit, ConnectRequest, ListenRequest, NetworkFailure, NetworkListenerSubmit,
    NetworkProviderSubmit, ReadRequest, ReadResult, WriteRequest, WriteResult,
};

/// Cold connection-oriented control plane over a warm provider.
///
/// Calling `listen` or `connect` constructs an owned `'static` future and
/// admits nothing; the first poll attempts admission through the provider
/// exactly once and then follows the eager contract. Dropping a never-polled
/// future binds no address and consumes no connection or capacity. Dropping a
/// future after an admitting first poll abandons only the response.
///
/// A cold future is [`Send`] exactly when the provider and its response
/// futures are. A [`SimNetwork`](super::SimNetwork)-backed future is not:
///
/// ```compile_fail
/// fn requires_send<T: Send>(_value: T) {}
/// fn demand(cold: &kr_runtime_io::network::ColdNetwork<kr_runtime_io::network::SimNetwork>) {
///     let address = kr_runtime_io::network::NetworkAddress {
///         node: kr_runtime_io::network::NodeId(1),
///         port: 1,
///     };
///     requires_send(cold.connect(kr_runtime_io::network::ConnectRequest {
///         local: address.clone(),
///         remote: address,
///     }));
/// }
/// ```
pub struct ColdNetwork<P> {
    provider: Arc<P>,
}

impl<P> ColdNetwork<P> {
    #[must_use]
    pub fn new(provider: P) -> Self {
        Self {
            provider: Arc::new(provider),
        }
    }
}

// Each operation defers to `crate::cold_submit`, which retains the provider
// or handle reference for the future's whole life and drops the warm response
// before it.
impl<P: NetworkProviderSubmit> ColdNetwork<P> {
    pub fn listen(
        &self,
        request: ListenRequest<P::Address>,
    ) -> impl Future<Output = CompletionResult<ColdListener<P::Listener>, NetworkFailure>>
    + 'static
    + use<P> {
        let submit = crate::cold_submit(Arc::clone(&self.provider), move |provider| {
            provider.submit_listen(request)
        });
        async move { submit.await.map(ColdListener::new) }
    }

    pub fn connect(
        &self,
        request: ConnectRequest<P::Address>,
    ) -> impl Future<Output = CompletionResult<ColdStream<P::Stream>, NetworkFailure>> + 'static + use<P>
    {
        let submit = crate::cold_submit(Arc::clone(&self.provider), move |provider| {
            provider.submit_connect(request)
        });
        async move { submit.await.map(ColdStream::new) }
    }
}

/// Cold bound listener over a warm exclusive listener handle.
///
/// A never-polled `accept` consumes no connection and no FIFO position; a
/// never-polled `close` does not submit the explicit close operation, though
/// dropping the last retaining handle or future still runs the listener's
/// ordinary drop teardown.
pub struct ColdListener<L> {
    listener: Arc<L>,
}

impl<L> ColdListener<L> {
    /// Wraps an already-open warm listener.
    #[must_use]
    pub fn new(listener: L) -> Self {
        Self {
            listener: Arc::new(listener),
        }
    }
}

impl<L: NetworkListenerSubmit> ColdListener<L> {
    /// Returns the bound address without driving I/O.
    #[must_use]
    pub fn local_address(&self) -> L::Address {
        self.listener.local_address()
    }

    pub fn accept(
        &self,
    ) -> impl Future<Output = CompletionResult<ColdStream<L::Stream>, NetworkFailure>> + 'static + use<L>
    {
        let submit = crate::cold_submit(Arc::clone(&self.listener), |listener| {
            listener.submit_accept()
        });
        async move { submit.await.map(ColdStream::new) }
    }

    pub fn close(
        &self,
    ) -> impl Future<Output = CompletionResult<(), NetworkFailure>> + 'static + use<L> {
        crate::cold_submit(Arc::clone(&self.listener), |listener| {
            listener.submit_close()
        })
    }
}

/// Cold connected byte stream over a warm exclusive stream handle.
///
/// A never-polled `read` consumes no bytes and no admission capacity, a
/// never-polled `write` sends nothing, and a never-polled `shutdown_write` or
/// `close` submits no control operation. After an admitting first poll, the
/// eager contract applies unchanged: dropping the future abandons only the
/// response, and an admitted read may still consume bytes.
pub struct ColdStream<S> {
    stream: Arc<S>,
}

impl<S> ColdStream<S> {
    /// Wraps an already-connected warm stream.
    #[must_use]
    pub fn new(stream: S) -> Self {
        Self {
            stream: Arc::new(stream),
        }
    }
}

impl<S: super::ByteStreamVectoredSubmit> ColdStream<S> {
    #[must_use]
    pub fn max_segments(&self) -> usize {
        self.stream.max_segments()
    }

    pub fn write_vectored(
        &self,
        request: super::VectoredWriteRequest,
    ) -> impl Future<
        Output = CompletionResult<super::VectoredWriteResult, super::VectoredWriteFailure>,
    >
    + 'static
    + use<S> {
        crate::cold_submit(Arc::clone(&self.stream), move |stream| {
            stream.submit_write_vectored(request)
        })
    }
}

impl<S: ByteStreamSubmit> ColdStream<S> {
    pub fn read(
        &self,
        request: ReadRequest,
    ) -> impl Future<Output = CompletionResult<ReadResult, NetworkFailure>> + 'static + use<S> {
        crate::cold_submit(Arc::clone(&self.stream), move |stream| {
            stream.submit_read(request)
        })
    }

    pub fn write(
        &self,
        request: WriteRequest,
    ) -> impl Future<Output = CompletionResult<WriteResult, NetworkFailure>> + 'static + use<S>
    {
        crate::cold_submit(Arc::clone(&self.stream), move |stream| {
            stream.submit_write(request)
        })
    }

    pub fn shutdown_write(
        &self,
    ) -> impl Future<Output = CompletionResult<(), NetworkFailure>> + 'static + use<S> {
        crate::cold_submit(Arc::clone(&self.stream), |stream| {
            stream.submit_shutdown_write()
        })
    }

    pub fn close(
        &self,
    ) -> impl Future<Output = CompletionResult<(), NetworkFailure>> + 'static + use<S> {
        crate::cold_submit(Arc::clone(&self.stream), |stream| stream.submit_close())
    }
}

#[cfg(test)]
mod tests {
    use super::{ColdNetwork, ColdStream};
    use crate::conformance::{check_cold_network_provider, check_cold_stream_pair};
    use crate::network::{
        ByteStreamSubmit, ConnectRequest, LinkConfig, ListenRequest, MemoryNetwork,
        MemoryNetworkConfig, NetworkAddress, NetworkConfig, NodeId, ReadRequest, SimNetwork,
        WriteRequest,
    };
    use kr_runtime::{RuntimeConfig, SimRuntime};

    fn memory_config() -> MemoryNetworkConfig {
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

    fn sim_config() -> NetworkConfig {
        NetworkConfig {
            directional_buffer_bytes: 32,
            default_link: LinkConfig {
                max_chunk_bytes: 2,
                ..LinkConfig::default()
            },
            ..NetworkConfig::default()
        }
    }

    fn listen_request() -> ListenRequest {
        ListenRequest {
            address: NetworkAddress {
                node: NodeId(20),
                port: 7_000,
            },
            backlog: 4,
        }
    }

    fn client(node: u64, port: u16) -> NetworkAddress {
        NetworkAddress {
            node: NodeId(node),
            port,
        }
    }

    #[test]
    fn memory_network_passes_cold_conformance() {
        let network = MemoryNetwork::new(memory_config()).expect("memory network config is valid");
        let mut runtime = SimRuntime::default();
        let cold = ColdNetwork::new(network);
        runtime
            .block_on(async move {
                check_cold_network_provider(
                    &cold,
                    listen_request(),
                    client(10, 4_001),
                    client(11, 4_002),
                )
                .await
            })
            .expect("runtime completes")
            .expect("memory network satisfies cold conformance");
    }

    #[test]
    fn simulated_network_passes_cold_conformance() {
        let mut runtime = SimRuntime::new(RuntimeConfig::default());
        let network =
            SimNetwork::new(runtime.handle(), sim_config()).expect("valid network config");
        let cold = ColdNetwork::new(network);
        runtime
            .block_on(async move {
                check_cold_network_provider(
                    &cold,
                    listen_request(),
                    client(10, 4_001),
                    client(11, 4_002),
                )
                .await
            })
            .expect("runtime completes")
            .expect("simulated network satisfies cold conformance");
    }

    #[test]
    fn memory_pair_passes_cold_stream_conformance() {
        let network = MemoryNetwork::new(memory_config()).expect("memory network config is valid");
        let (left, right) = network
            .connected_pair()
            .expect("connected pair is admitted");
        let (left, right) = (ColdStream::new(left), ColdStream::new(right));
        let mut runtime = SimRuntime::default();
        runtime
            .block_on(async move { check_cold_stream_pair(&left, &right).await })
            .expect("runtime completes")
            .expect("memory streams satisfy cold conformance");
    }

    #[test]
    fn simulated_pair_passes_cold_stream_conformance() {
        let mut runtime = SimRuntime::new(RuntimeConfig::default());
        let network =
            SimNetwork::new(runtime.handle(), sim_config()).expect("valid network config");
        let (left, right) = network
            .connected_pair(NodeId(1), NodeId(2))
            .expect("connected pair is admitted");
        let (left, right) = (ColdStream::new(left), ColdStream::new(right));
        runtime
            .block_on(async move { check_cold_stream_pair(&left, &right).await })
            .expect("runtime completes")
            .expect("simulated streams satisfy cold conformance");
    }

    #[test]
    fn an_unpolled_future_retains_the_stream_capability() {
        let network = MemoryNetwork::new(memory_config()).expect("memory network config is valid");
        let (left, right) = network
            .connected_pair()
            .expect("connected pair is admitted");
        let cold = ColdStream::new(left);

        let constructed = cold.read(ReadRequest {
            buffer: Vec::new(),
            max_bytes: 2,
        });
        drop(cold);

        // The unpolled future holds the last left-stream capability; the
        // stream must stay open until that future is dropped.
        let mut runtime = SimRuntime::default();
        let read = runtime
            .block_on(async move {
                right
                    .submit_write(WriteRequest {
                        buffer: b"ok".to_vec(),
                    })
                    .await
                    .expect("peer write succeeds");
                constructed.await
            })
            .expect("runtime completes")
            .expect("read through the retained capability succeeds");
        assert_eq!(read.buffer, b"ok");
    }

    #[test]
    fn memory_backed_cold_futures_are_send() {
        fn assert_send<T: Send>(_future: T) {}

        let network = MemoryNetwork::new(memory_config()).expect("memory network config is valid");
        let (left, _right) = network
            .connected_pair()
            .expect("connected pair is admitted");
        let cold_network = ColdNetwork::new(network);
        assert_send(cold_network.listen(listen_request()));
        assert_send(cold_network.connect(ConnectRequest {
            local: client(1, 1),
            remote: client(2, 2),
        }));
        let cold_stream = ColdStream::new(left);
        assert_send(cold_stream.read(ReadRequest {
            buffer: Vec::new(),
            max_bytes: 1,
        }));
        assert_send(cold_stream.write(WriteRequest { buffer: Vec::new() }));
        assert_send(cold_stream.shutdown_write());
        assert_send(cold_stream.close());
    }
}
