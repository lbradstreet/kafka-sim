//! Native connection setup. The returned driver is the exact driver that read
//! ApiVersions and SASL responses, including its retained receive operation.
use crate::{
    SecurityError,
    config::{Backend, HostBounds, choose_backend},
    control::ControlJobs,
    sasl::{self, SaslLimits, ScramClient},
    stream::TlsStream,
    tls::TlsClient,
};
use kr_kafka_client::{
    config::{ConnectionConfig, SaslMechanism, SecurityConfig, TransportPolicy},
    connector::{ConnectError, ConnectTarget, Connected, Connector, SetupBudget},
    control::{ControlCodec, ControlError, Negotiation, Probe},
    transport::{
        ConnectionDriver, DriverEvent, OwnedSendPlan, RetireReason, SendRequest, TransportError,
    },
};
use kr_runtime::{CompletionResult, HostConfig, HostHandle, HostRuntime};
use kr_runtime_io::network::{
    ByteStreamSubmit, ByteStreamVectoredSubmit, ConnectRequest, NetworkError, NetworkFailure,
    NetworkOperationKind, NetworkProviderSubmit, ReadRequest, ReadResult, VectoredWriteFailure,
    VectoredWriteRequest, VectoredWriteResult, WriteRequest, WriteResult,
};
use kr_runtime_io_readiness::{ReadinessConfig, ReadinessNet, ReadinessStream};
use kr_runtime_io_uring::{
    PooledUringStream, UringNetPool, UringNetPoolConfig, UringNetPoolOpenError,
};
use rustls::{
    ClientConfig, RootCertStore,
    pki_types::{CertificateDer, ServerName},
};
use std::{
    future::{Future, poll_fn},
    net::{IpAddr, SocketAddr, ToSocketAddrs},
    pin::Pin,
    sync::{Arc, Mutex},
    task::Poll,
};
use zeroize::Zeroizing;

type Response<T, E = NetworkFailure> = Pin<Box<dyn Future<Output = CompletionResult<T, E>> + Send>>;

pub enum NativeStream {
    Uring(PooledUringStream),
    Readiness(ReadinessStream),
}
#[derive(Clone)]
pub enum HostStream {
    Plain(Arc<NativeStream>),
    Tls(Arc<TlsStream<NativeStream>>),
}
macro_rules! stream_impl {
    ($ty:ident,$a:ident,$b:ident) => {
        impl ByteStreamSubmit for $ty {
            type ReadResponse = Response<ReadResult>;
            type WriteResponse = Response<WriteResult>;
            type ControlResponse = Response<()>;
            fn submit_read(&self, r: ReadRequest) -> Self::ReadResponse {
                match self {
                    Self::$a(s) => Box::pin(s.submit_read(r)),
                    Self::$b(s) => Box::pin(s.submit_read(r)),
                }
            }
            fn submit_write(&self, r: WriteRequest) -> Self::WriteResponse {
                match self {
                    Self::$a(s) => Box::pin(s.submit_write(r)),
                    Self::$b(s) => Box::pin(s.submit_write(r)),
                }
            }
            fn submit_shutdown_write(&self) -> Self::ControlResponse {
                match self {
                    Self::$a(s) => Box::pin(s.submit_shutdown_write()),
                    Self::$b(s) => Box::pin(s.submit_shutdown_write()),
                }
            }
            fn submit_close(&self) -> Self::ControlResponse {
                match self {
                    Self::$a(s) => Box::pin(s.submit_close()),
                    Self::$b(s) => Box::pin(s.submit_close()),
                }
            }
        }
        impl ByteStreamVectoredSubmit for $ty {
            type WriteVectoredResponse = Response<VectoredWriteResult, VectoredWriteFailure>;
            fn max_segments(&self) -> usize {
                match self {
                    Self::$a(s) => s.max_segments(),
                    Self::$b(s) => s.max_segments(),
                }
            }
            fn submit_write_vectored(
                &self,
                r: VectoredWriteRequest,
            ) -> Self::WriteVectoredResponse {
                match self {
                    Self::$a(s) => Box::pin(s.submit_write_vectored(r)),
                    Self::$b(s) => Box::pin(s.submit_write_vectored(r)),
                }
            }
        }
    };
}
impl ByteStreamSubmit for NativeStream {
    type ReadResponse =
        kr_runtime_io::completion::SyncOperation<CompletionResult<ReadResult, NetworkFailure>>;
    type WriteResponse =
        kr_runtime_io::completion::SyncOperation<CompletionResult<WriteResult, NetworkFailure>>;
    type ControlResponse =
        kr_runtime_io::completion::SyncOperation<CompletionResult<(), NetworkFailure>>;
    fn submit_read(&self, request: ReadRequest) -> Self::ReadResponse {
        match self {
            Self::Uring(stream) => stream.submit_read(request),
            Self::Readiness(stream) => stream.submit_read(request),
        }
    }
    fn submit_write(&self, request: WriteRequest) -> Self::WriteResponse {
        match self {
            Self::Uring(stream) => stream.submit_write(request),
            Self::Readiness(stream) => stream.submit_write(request),
        }
    }
    fn submit_shutdown_write(&self) -> Self::ControlResponse {
        match self {
            Self::Uring(stream) => stream.submit_shutdown_write(),
            Self::Readiness(stream) => stream.submit_shutdown_write(),
        }
    }
    fn submit_close(&self) -> Self::ControlResponse {
        match self {
            Self::Uring(stream) => stream.submit_close(),
            Self::Readiness(stream) => stream.submit_close(),
        }
    }
}
impl ByteStreamVectoredSubmit for NativeStream {
    type WriteVectoredResponse = Response<VectoredWriteResult, VectoredWriteFailure>;
    fn max_segments(&self) -> usize {
        match self {
            Self::Uring(stream) => stream.max_segments(),
            Self::Readiness(stream) => stream.max_segments(),
        }
    }
    fn submit_write_vectored(&self, request: VectoredWriteRequest) -> Self::WriteVectoredResponse {
        match self {
            Self::Uring(stream) => Box::pin(stream.submit_write_vectored(request)),
            Self::Readiness(stream) => Box::pin(stream.submit_write_vectored(request)),
        }
    }
}
stream_impl!(HostStream, Plain, Tls);

#[derive(Clone)]
enum NativeNetwork {
    Uring(UringNetPool),
    Readiness(ReadinessNet),
}
impl NativeNetwork {
    async fn connect(&self, remote: SocketAddr) -> Result<NativeStream, ConnectError> {
        let local = SocketAddr::new(
            if remote.is_ipv4() {
                IpAddr::from([0, 0, 0, 0])
            } else {
                IpAddr::from([0u16; 8])
            },
            0,
        );
        let request = ConnectRequest { local, remote };
        match self {
            Self::Uring(net) => net.submit_connect(request).await.map(NativeStream::Uring),
            Self::Readiness(net) => net
                .submit_connect(request)
                .await
                .map(NativeStream::Readiness),
        }
        .map_err(|failure| ConnectError::Network(failure.error().error().clone()))
    }
}

struct Settings {
    config: ConnectionConfig,
    bounds: HostBounds,
    tls: Option<Arc<ClientConfig>>,
    codec: ControlCodec,
    jobs: ControlJobs,
    setup_budget: Arc<dyn SetupBudget>,
    telemetry: Mutex<Option<Arc<kr_kafka_client::telemetry::TransportTelemetry>>>,
    completions: std::sync::OnceLock<Arc<kr_runtime_io::completion::CompletionMetrics>>,
}
/// One owner-local connector, with a dedicated bounded blocking control fleet.
/// Construction validates and provisions startup resources; `connect` itself is
/// cold and neither resolves DNS nor opens a socket until its first poll.
pub struct HostConnector {
    handle: HostHandle,
    network: NativeNetwork,
    settings: Arc<Settings>,
    backend: Backend,
}
impl HostConnector {
    pub fn new(
        handle: HostHandle,
        config: ConnectionConfig,
        codec: ControlCodec,
        setup_budget: Arc<dyn SetupBudget>,
    ) -> Result<Self, ConnectError> {
        let bounds = HostBounds::from_config(&config).map_err(security_error)?;
        // Every setup holds a fixed 128KiB work guard. The injected codec must
        // preserve the response/auth sub-bounds covered by that reservation.
        if codec.limits().owned_bytes > 64 * 1024 || codec.limits().auth_bytes > 16 * 1024 {
            return Err(ConnectError::InvalidConfiguration);
        }
        let tls = build_tls(&config)?;
        let network = if config.transport == TransportPolicy::Readiness {
            NativeNetwork::Readiness(
                ReadinessNet::new(readiness_config(bounds)).map_err(ConnectError::Network)?,
            )
        } else {
            let open = UringNetPool::new(UringNetPoolConfig {
                max_streams: bounds.streams,
                command_queue_capacity: 2,
                ring_entries: bounds.ring_entries,
                max_operation_bytes: bounds.operation_bytes,
                max_io_chunk_bytes: 64 * 1024,
                max_listeners: 1,
                max_listener_backlog: 1,
                connect_timeout: bounds.connect_timeout,
            });
            match open {
                Ok(pool) => NativeNetwork::Uring(pool),
                Err(error) => {
                    let unavailable = matches!(
                        error,
                        UringNetPoolOpenError::Io {
                            raw_os_error: Some(1 | 13 | 38 | 95),
                            ..
                        }
                    );
                    if choose_backend(config.transport, false, unavailable).ok()
                        != Some(Backend::Readiness)
                    {
                        return Err(ConnectError::TransportUnavailable);
                    }
                    NativeNetwork::Readiness(
                        ReadinessNet::new(readiness_config(bounds))
                            .map_err(ConnectError::Network)?,
                    )
                }
            }
        };
        let backend = match &network {
            NativeNetwork::Uring(_) => Backend::Uring,
            NativeNetwork::Readiness(_) => Backend::Readiness,
        };
        // This fleet has no encoding/data jobs; its queue is admitted by ControlJobs.
        let workers = HostRuntime::new(HostConfig {
            blocking_workers: 2,
            ..Default::default()
        })
        .map_err(|_| ConnectError::InvalidConfiguration)?;
        let blocking = workers.blocking().map_err(|_| ConnectError::WorkerFailed)?;
        workers.finish().map_err(|_| ConnectError::WorkerFailed)?;
        let jobs = ControlJobs::new(blocking, bounds.control_jobs, bounds.control_bytes)
            .map_err(security_error)?;
        Ok(Self {
            handle,
            network,
            settings: Arc::new(Settings {
                config,
                bounds,
                tls,
                codec,
                jobs,
                setup_budget,
                telemetry: Mutex::new(None),
                completions: std::sync::OnceLock::new(),
            }),
            backend,
        })
    }
    /// Attaches passive counters and weak runtime/provider observers before setup.
    /// The diagnostics handle cannot keep the provider or runtime alive.
    /// # Errors
    /// Rejects repeated attachment without changing either diagnostics bank.
    pub fn attach_diagnostics(
        &self,
        diagnostics: &crate::diagnostics::HostDiagnostics,
        runtime: kr_runtime::HostRuntimeObserver,
    ) -> Result<(), ConnectError> {
        self.settings
            .completions
            .set(diagnostics.completions.clone())
            .map_err(|_| ConnectError::InvalidConfiguration)?;
        diagnostics.install(runtime, self.observer());
        Ok(())
    }
    pub(crate) fn observer(&self) -> crate::diagnostics::ProviderObserver {
        match &self.network {
            NativeNetwork::Uring(n) => crate::diagnostics::ProviderObserver::Uring(n.observer()),
            NativeNetwork::Readiness(n) => {
                crate::diagnostics::ProviderObserver::Readiness(n.observer())
            }
        }
    }
    pub fn backend(&self) -> Backend {
        self.backend
    }
    pub fn bounds(&self) -> HostBounds {
        self.settings.bounds
    }
    pub fn control_usage(&self) -> (usize, usize) {
        self.settings.jobs.usage()
    }
}
impl Connector for HostConnector {
    type Stream = HostStream;
    type ConnectFuture = Pin<Box<dyn Future<Output = Result<Connected<HostStream>, ConnectError>>>>;
    fn set_telemetry(&mut self, metrics: Arc<kr_kafka_client::telemetry::TransportTelemetry>) {
        *self
            .settings
            .telemetry
            .lock()
            .unwrap_or_else(|p| p.into_inner()) = Some(metrics);
    }
    fn connect(&mut self, target: ConnectTarget) -> Self::ConnectFuture {
        let network = self.network.clone();
        let settings = self.settings.clone();
        let handle = self.handle.clone();
        Box::pin(async move { setup(handle, network, settings, target).await })
    }
}
fn readiness_config(bounds: HostBounds) -> ReadinessConfig {
    ReadinessConfig {
        connect_timeout: bounds.connect_timeout,
        max_streams: bounds.streams,
        max_listeners: 1,
        max_listener_backlog: 1,
        max_control_operations: bounds.streams * 2,
        max_read_operations: bounds.streams,
        max_write_operations: bounds.streams,
        max_operation_bytes: bounds.operation_bytes,
        max_outstanding_read_bytes: bounds.read_bytes,
        max_outstanding_write_bytes: bounds.write_bytes,
        max_segments: 64,
        max_chunk_bytes: 64 * 1024,
        socket_buffer_bytes: 256 * 1024,
    }
}
fn build_tls(config: &ConnectionConfig) -> Result<Option<Arc<ClientConfig>>, ConnectError> {
    let tls = match &config.security {
        SecurityConfig::Plaintext => return Ok(None),
        SecurityConfig::Tls { tls } | SecurityConfig::SaslTls { tls, .. } => tls,
    };
    let mut roots = RootCertStore::empty();
    let mut bytes = 0usize;
    for root in &tls.roots_der {
        bytes = bytes
            .checked_add(root.len())
            .filter(|n| *n <= config.control_bytes)
            .ok_or(ConnectError::InvalidConfiguration)?;
        roots
            .add(CertificateDer::from(root.clone()))
            .map_err(|_| ConnectError::InvalidConfiguration)?;
    }
    if tls.use_system_roots {
        let loaded = rustls_native_certs::load_native_certs();
        if !loaded.errors.is_empty() {
            return Err(ConnectError::InvalidConfiguration);
        }
        for root in loaded.certs {
            roots
                .add(root)
                .map_err(|_| ConnectError::InvalidConfiguration)?;
        }
    }
    if roots.is_empty() {
        return Err(ConnectError::InvalidConfiguration);
    }
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let client = ClientConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()
        .map_err(|_| ConnectError::InvalidConfiguration)?
        .with_root_certificates(roots)
        .with_no_client_auth();
    Ok(Some(Arc::new(client)))
}

struct SetupTarget {
    target: ConnectTarget,
    working: Arc<dyn Send + Sync>,
}
impl std::ops::Deref for SetupTarget {
    type Target = ConnectTarget;
    fn deref(&self) -> &ConnectTarget {
        &self.target
    }
}

async fn setup(
    handle: HostHandle,
    network: NativeNetwork,
    settings: Arc<Settings>,
    target: ConnectTarget,
) -> Result<Connected<HostStream>, ConnectError> {
    let working: Arc<dyn Send + Sync> =
        if target.broker_id.is_some() || target.lifetime_guard.is_none() {
            settings
                .setup_budget
                .reserve(kr_kafka_client::connector::DATA_SETUP_BYTES)?
        } else {
            target
                .lifetime_guard
                .clone()
                .expect("control connection budget")
        };
    let target = SetupTarget { target, working };
    if handle.now() >= target.deadline {
        return Err(ConnectError::Timeout);
    }
    if target.endpoint.host.is_empty()
        || target.endpoint.host.len() > 253
        || target.endpoint.host.as_bytes().contains(&0)
        || target.endpoint.port == 0
        || target.driver.rx_bytes > settings.bounds.operation_bytes
        || target.driver.max_operation_bytes > settings.bounds.operation_bytes
        || target.driver.rx_bytes < 8
        || target.driver.rx_bytes > target.driver.max_operation_bytes
        || target.driver.staging_bytes == 0
        || target.driver.staging_bytes > target.driver.max_operation_bytes
        || target.driver.max_inflight_requests == 0
    {
        return Err(ConnectError::InvalidConfiguration);
    }
    let mut addresses = Vec::new();
    addresses
        .try_reserve_exact(16)
        .map_err(|_| ConnectError::ResourceExhausted)?;
    if let Ok(ip) = target.endpoint.host.parse::<IpAddr>() {
        addresses.push(SocketAddr::new(ip, target.endpoint.port));
    } else {
        let host = target.endpoint.host.clone();
        let port = target.endpoint.port;
        // The selected result is capped at sixteen addresses. System resolver
        // internals are native runtime overhead, outside Kafka byte pools.
        addresses = settings
            .jobs
            .submit_guarded(
                host.capacity() + 4096 + 16 * std::mem::size_of::<SocketAddr>(),
                target.working.clone(),
                move || normalize_addresses(addresses, (host.as_str(), port).to_socket_addrs()),
            )
            .map_err(security_error)?
            .await
            .map_err(security_error)?;
    }
    let mut connected = None;
    let mut failure = ConnectError::TransportUnavailable;
    for address in addresses {
        if handle.now() >= target.deadline {
            return Err(ConnectError::Timeout);
        }
        match network.connect(address).await {
            Ok(stream) => {
                connected = Some(stream);
                break;
            }
            Err(error) => failure = error,
        }
    }
    let native = connected.ok_or(failure)?;
    if let Some(metrics) = settings.completions.get() {
        let result = match &native {
            NativeStream::Uring(s) => s.attach_completion_metrics(metrics.clone()),
            NativeStream::Readiness(s) => s.attach_completion_metrics(metrics.clone()),
        };
        if let Err(error) = result {
            let _ = native.submit_close().await;
            return Err(ConnectError::Network(error));
        }
    }
    if let Some(guard) = &target.lifetime_guard {
        let result = match &native {
            NativeStream::Uring(s) => s.attach_lifetime_guard(guard.clone()),
            NativeStream::Readiness(s) => s.attach_lifetime_guard(guard.clone()),
        };
        if let Err(error) = result {
            let _ = native.submit_close().await;
            return Err(ConnectError::Network(error));
        }
    }
    if handle.now() >= target.deadline {
        let _ = native.submit_close().await;
        return Err(ConnectError::Timeout);
    }
    let mut plain = None;
    let stream = if let Some(config) = &settings.tls {
        let name = match &settings.config.security {
            SecurityConfig::Tls { tls } | SecurityConfig::SaslTls { tls, .. } => {
                tls.server_name.as_deref().unwrap_or(&target.endpoint.host)
            }
            SecurityConfig::Plaintext => unreachable!(),
        };
        let name = match ServerName::try_from(name.to_owned()) {
            Ok(name) => name,
            Err(_) => {
                let _ = native.submit_close().await;
                return Err(ConnectError::InvalidConfiguration);
            }
        };
        let client = match TlsClient::new(config.clone(), name, settings.bounds.tls_client.unwrap())
        {
            Ok(client) => client,
            Err(error) => {
                let _ = native.submit_close().await;
                return Err(security_error(error));
            }
        };
        let tls = TlsStream::new(native, client, settings.bounds.tls_stream.unwrap())
            .map_err(security_error)?;
        if let Some(metrics) = settings
            .telemetry
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .clone()
        {
            tls.attach_telemetry(metrics).map_err(security_error)?;
        }
        if let Some(guard) = &target.lifetime_guard {
            tls.attach_lifetime_guard(guard.clone())
                .map_err(security_error)?;
        }
        let mut handshake = Box::pin(tls.handshake());
        let mut handshake_completed = false;
        let mut timer = Box::pin(handle.sleep_until(target.deadline));
        let result = poll_fn(|cx| {
            if handle.now() >= target.deadline || timer.as_mut().poll(cx).is_ready() {
                return Poll::Ready(Err(ConnectError::Timeout));
            }
            match handshake.as_mut().poll(cx) {
                Poll::Ready(result) => {
                    handshake_completed = true;
                    Poll::Ready(result.map_err(security_error))
                }
                Poll::Pending => Poll::Pending,
            }
        })
        .await;
        if let Err(error) = result {
            let _ = tls.retire().await;
            // Only a timer can leave handshake observation unfinished. A
            // certificate/proof error already consumed its one terminal result.
            if !handshake_completed {
                let _ = handshake.await;
            }
            return Err(error);
        }
        let make_plain = || -> Result<Option<Zeroizing<Vec<u8>>>, ConnectError> {
            if let SecurityConfig::SaslTls {
                mechanism: SaslMechanism::Plain,
                username,
                password,
                ..
            } = &settings.config.security
            {
                let peer = tls.peer_verified().ok_or(ConnectError::Authentication)?;
                Ok(Some(
                    sasl::plain_response(
                        &peer.proof(),
                        username,
                        password.expose(),
                        SaslLimits::default(),
                    )
                    .map_err(security_error)?,
                ))
            } else {
                Ok(None)
            }
        };
        plain = match make_plain() {
            Ok(plain) => plain,
            Err(error) => {
                let _ = tls.retire().await;
                return Err(error);
            }
        };
        HostStream::Tls(Arc::new(tls))
    } else {
        HostStream::Plain(Arc::new(native))
    };
    let cleanup = stream.clone();
    let mut driver = match ConnectionDriver::new(stream, target.driver) {
        Ok(driver) => driver,
        Err(error) => {
            let _ = cleanup.submit_close().await;
            return Err(transport_error(error));
        }
    };
    drop(cleanup);
    let result = negotiate(&handle, &settings, &target, &mut driver, plain).await;
    match result {
        Ok(capabilities) => Ok(Connected {
            driver,
            capabilities,
        }),
        Err(error) => {
            driver.retire(RetireReason::Requested);
            drain(&handle, &mut driver).await;
            Err(error)
        }
    }
}
// The resolver's system-owned iterator is visited at most sixteen times. The
// caller charged and reserved this bounded result vector before dispatching its
// blocking job; error text never copies host names or unbounded resolver text.
fn normalize_addresses(
    mut addresses: Vec<SocketAddr>,
    resolved: std::io::Result<impl IntoIterator<Item = SocketAddr>>,
) -> Result<Vec<SocketAddr>, SecurityError> {
    let resolved = resolved.map_err(|error| {
        SecurityError::Network(NetworkError::Backend {
            operation: NetworkOperationKind::Connect,
            raw_os_error: error.raw_os_error(),
            message: "DNS address resolution failed".into(),
        })
    })?;
    for address in resolved.into_iter().take(16) {
        if !addresses.contains(&address) {
            addresses.push(address);
        }
    }
    if addresses.is_empty() {
        Err(SecurityError::Network(NetworkError::Backend {
            operation: NetworkOperationKind::Connect,
            raw_os_error: None,
            message: "DNS returned no socket addresses".into(),
        }))
    } else {
        Ok(addresses)
    }
}

async fn negotiate(
    handle: &HostHandle,
    settings: &Settings,
    target: &SetupTarget,
    driver: &mut ConnectionDriver<HostStream>,
    plain: Option<Zeroizing<Vec<u8>>>,
) -> Result<kr_kafka_client::control::Capabilities, ConnectError> {
    let codec = &settings.codec;
    let mut probe = Probe::V3;
    let mut correlation = -1;
    let mut capabilities = loop {
        let request = codec
            .api_versions_request(correlation, probe)
            .map_err(ConnectError::Protocol)?;
        let negotiation = exchange(handle, target, driver, correlation, request, |frame| {
            codec.parse_api_versions(frame, correlation, probe)
        })
        .await?;
        match negotiation {
            Negotiation::Ready(capabilities) => break capabilities,
            Negotiation::ProbeClassic if probe == Probe::V3 => {
                probe = Probe::V0;
                correlation -= 1;
            }
            Negotiation::ProbeClassic => return Err(ConnectError::InvalidConfiguration),
        }
    };
    // Advertisements remain live through SASL and may outlive the driver. Share
    // one charged allocation across clones, and leave only the unused response
    // allowance available to each subsequent parser.
    let owned_bytes = codec
        .limits()
        .owned_bytes
        .checked_sub(capabilities.retained_capacity_bytes())
        .ok_or(ConnectError::ResourceExhausted)?;
    capabilities
        .attach_lifetime_guard(target.working.clone())
        .map_err(|_| ConnectError::InvalidConfiguration)?;
    if let SecurityConfig::SaslTls {
        mechanism,
        username,
        password,
        ..
    } = &settings.config.security
    {
        capabilities
            .require(17, 1)
            .map_err(ConnectError::Protocol)?;
        capabilities
            .require(36, 2)
            .map_err(ConnectError::Protocol)?;
        correlation -= 1;
        let request = codec
            .sasl_handshake_request(correlation, *mechanism)
            .map_err(ConnectError::Protocol)?;
        let handshake = exchange(handle, target, driver, correlation, request, |frame| {
            codec.parse_sasl_handshake_with_owned_limit(frame, correlation, *mechanism, owned_bytes)
        })
        .await?;
        if handshake.error_code != 0 {
            return Err(ConnectError::Authentication);
        }
        drop(handshake);
        if *mechanism == SaslMechanism::Plain {
            let response = authenticate(
                handle,
                settings,
                target,
                driver,
                &mut correlation,
                owned_bytes,
                &plain.ok_or(ConnectError::Authentication)?,
            )
            .await?;
            if !response.auth_bytes.is_empty() {
                return Err(ConnectError::Authentication);
            }
        } else {
            let mechanism = if *mechanism == SaslMechanism::ScramSha256 {
                sasl::ScramMechanism::Sha256
            } else {
                sasl::ScramMechanism::Sha512
            };
            let client = ScramClient::start(
                mechanism,
                username,
                password.expose(),
                SaslLimits::default(),
            )
            .map_err(security_error)?;
            let challenge = authenticate(
                handle,
                settings,
                target,
                driver,
                &mut correlation,
                owned_bytes,
                client.initial_response(),
            )
            .await?;
            let work = client
                .on_server_first(&challenge.auth_bytes)
                .map_err(security_error)?;
            drop(challenge);
            let proof = settings
                .jobs
                .submit_guarded(
                    work.retained_bytes().map_err(security_error)?,
                    target.working.clone(),
                    move || work.compute(),
                )
                .map_err(security_error)?
                .await
                .map_err(security_error)?;
            if handle.now() >= target.deadline {
                return Err(ConnectError::Timeout);
            }
            let final_message = authenticate(
                handle,
                settings,
                target,
                driver,
                &mut correlation,
                owned_bytes,
                &proof.response,
            )
            .await?;
            proof
                .verifier
                .verify(&final_message.auth_bytes)
                .map_err(security_error)?;
        }
    }
    Ok(capabilities)
}
async fn authenticate(
    handle: &HostHandle,
    settings: &Settings,
    target: &SetupTarget,
    driver: &mut ConnectionDriver<HostStream>,
    correlation: &mut i32,
    owned_bytes: usize,
    bytes: &[u8],
) -> Result<kr_kafka_client::control::AuthenticationResponse, ConnectError> {
    *correlation -= 1;
    let request = settings
        .codec
        .sasl_authenticate_request(*correlation, bytes)
        .map_err(ConnectError::Protocol)?;
    let response = exchange(handle, target, driver, *correlation, request, |frame| {
        settings
            .codec
            .parse_sasl_authenticate_with_owned_limit(frame, *correlation, owned_bytes)
    })
    .await?;
    if response.error_code != 0 {
        return Err(ConnectError::Authentication);
    }
    Ok(response)
}
async fn exchange<T>(
    handle: &HostHandle,
    target: &SetupTarget,
    driver: &mut ConnectionDriver<HostStream>,
    correlation: i32,
    frame: Vec<u8>,
    parse: impl Fn(&[u8]) -> Result<T, kr_kafka_client::control::ControlError>,
) -> Result<T, ConnectError> {
    let mut plan =
        OwnedSendPlan::from_frame(frame, target.driver.rx_bytes).map_err(transport_error)?;
    plan.retain_metadata_guard(target.working.clone());
    driver
        .enqueue(SendRequest {
            correlation,
            deadline: target.deadline,
            plan,
        })
        .map_err(|rejected| transport_error(rejected.error))?;
    let mut timer = Box::pin(handle.sleep_until(target.deadline));
    poll_fn(|cx| {
        if handle.now() >= target.deadline || timer.as_mut().poll(cx).is_ready() {
            return Poll::Ready(Err(ConnectError::Timeout));
        }
        for _ in 0..32 {
            match driver.poll_event(cx, handle.now()) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Some(DriverEvent::Frame {
                    correlation: actual,
                    bytes,
                })) if actual == correlation => {
                    return Poll::Ready(parse(bytes).map_err(ConnectError::Protocol));
                }
                Poll::Ready(Some(
                    DriverEvent::WriteAdmitted { .. } | DriverEvent::WriteProgress { .. },
                )) => {}
                Poll::Ready(Some(DriverEvent::Frame { .. })) => {
                    return Poll::Ready(Err(ConnectError::Protocol(ControlError::Invalid(
                        "unexpected setup response correlation",
                    ))));
                }
                Poll::Ready(Some(DriverEvent::Retiring { reason })) => {
                    return Poll::Ready(Err(retire_error(reason)));
                }
                Poll::Ready(
                    None | Some(DriverEvent::Released | DriverEvent::RequestRetired { .. }),
                ) => {
                    return Poll::Ready(Err(ConnectError::Network(NetworkError::ConnectionClosed)));
                }
            }
        }
        cx.waker().wake_by_ref();
        Poll::Pending
    })
    .await
}
async fn drain(handle: &HostHandle, driver: &mut ConnectionDriver<HostStream>) {
    poll_fn(|cx| {
        for _ in 0..32 {
            match driver.poll_event(cx, handle.now()) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(None | Some(DriverEvent::Released)) => return Poll::Ready(()),
                _ => {}
            }
        }
        cx.waker().wake_by_ref();
        Poll::Pending
    })
    .await;
}
fn transport_error(error: TransportError) -> ConnectError {
    match error {
        TransportError::ResourceExhausted { .. } | TransportError::AllocationFailed => {
            ConnectError::ResourceExhausted
        }
        TransportError::Closed => ConnectError::Network(NetworkError::ConnectionClosed),
        TransportError::Protocol(error) => ConnectError::Protocol(ControlError::Wire(error)),
        _ => ConnectError::InvalidConfiguration,
    }
}
fn retire_error(reason: RetireReason) -> ConnectError {
    match reason {
        RetireReason::Network(error) => ConnectError::Network(error),
        RetireReason::Requested | RetireReason::EndOfStream => {
            ConnectError::Network(NetworkError::ConnectionClosed)
        }
        RetireReason::Deadline { .. } => ConnectError::Timeout,
        RetireReason::Protocol(error) => ConnectError::Protocol(ControlError::Wire(error)),
        RetireReason::Transport(error) => transport_error(error),
    }
}
fn security_error(error: SecurityError) -> ConnectError {
    match error {
        SecurityError::ResourceExhausted { .. } => ConnectError::ResourceExhausted,
        SecurityError::WorkerPanicked => ConnectError::WorkerFailed,
        SecurityError::InvalidConfig { .. } => ConnectError::InvalidConfiguration,
        SecurityError::Network(error) => ConnectError::Network(error),
        SecurityError::TruncatedTls => ConnectError::Network(NetworkError::ConnectionClosed),
        _ => ConnectError::Authentication,
    }
}

#[cfg(test)]
mod tests;
