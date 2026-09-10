//! Dedicated owner thread and explicit close/join for a native Kafka producer.
use crate::calibration::Calibration;
#[cfg(target_os = "linux")]
use crate::calibration::calibrate;
use kr_kafka_client::transport::WriteMode;
use kr_kafka_host::{SecurityError, config::Backend, diagnostics::HostDiagnostics};
#[cfg(target_os = "linux")]
use kr_kafka_host::{config::HostBounds, native::HostConnector};
#[cfg(target_os = "linux")]
use kr_kafka_producer::{
    actor::ProducerActor, client::ClientClock, config::Compression, engine::ProducerEngine,
};
use kr_kafka_producer::{
    client::ProducerClient, config::ProducerConfig, connector::ConnectError, engine::EngineStatus,
};
use kr_runtime::RuntimeDuration;
#[cfg(target_os = "linux")]
use kr_runtime::{HostConfig, HostRuntime, RuntimeHandle};
#[cfg(any(target_os = "linux", test))]
use std::{sync::mpsc::sync_channel, thread};
#[cfg(any(target_os = "linux", test))]
type Startup = Result<(ProducerClient, Calibration, Backend), HostError>;
use std::{
    fmt,
    sync::{Arc, Mutex},
    thread::JoinHandle,
};

#[derive(Clone, Debug)]
pub enum HostError {
    Config(SecurityError),
    Connect(ConnectError),
    Engine(String),
    Actor(String),
    Runtime(String),
    Thread(String),
    Panicked,
}
impl fmt::Display for HostError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Kafka host failed: {self:?}")
    }
}
impl std::error::Error for HostError {}
#[derive(Clone, Debug)]
pub enum HostStatus {
    Running,
    Finished(EngineStatus),
    Failed(HostError),
}

/// Native producer execution options. The default preserves staging writes and
/// disables diagnostic clock/mutex work. Both write modes retain the same
/// immutable request bytes until their provider operation actually terminates.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct HostProducerOptions {
    pub write_mode: WriteMode,
    /// Enable fixed-size native operation timing and passive queue sampling.
    pub diagnostics: bool,
}
impl Default for HostProducerOptions {
    fn default() -> Self {
        Self {
            write_mode: WriteMode::Staging,
            diagnostics: false,
        }
    }
}
impl HostProducerOptions {
    #[cfg(any(target_os = "linux", test))]
    fn actor_config(self, calibration: Calibration) -> kr_kafka_producer::actor::ActorConfig {
        kr_kafka_producer::actor::ActorConfig {
            write_mode: self.write_mode,
            encode_bytes_per_poll: calibration.encode_bytes_per_poll,
            sim_encode_cost: RuntimeDuration::ZERO,
        }
    }
}

/// `client` is thread-safe; its owner actor and codec contexts stay on this
/// producer's single thread. Dropping this wrapper requests immediate close and
/// detaches observation; use `join` to wait for actual resource retirement.
pub struct HostProducer {
    client: ProducerClient,
    diagnostics: Option<HostDiagnostics>,
    calibration: Calibration,
    backend: Backend,
    status: Arc<Mutex<HostStatus>>,
    worker: Option<JoinHandle<Result<EngineStatus, HostError>>>,
}
impl HostProducer {
    /// Waits only for local validation/provisioning and actor construction. The
    /// returned client reports broker/topic readiness through normal events.
    pub fn start(config: ProducerConfig) -> Result<Self, HostError> {
        Self::start_with_options(config, HostProducerOptions::default())
    }
    /// Enables fixed-size native operation timing and passive queue sampling.
    /// Clock/mutex overhead is part of this instrumented run; use `start` for
    /// an uninstrumented baseline.
    pub fn start_with_diagnostics(config: ProducerConfig) -> Result<Self, HostError> {
        Self::start_with_options(
            config,
            HostProducerOptions {
                diagnostics: true,
                ..Default::default()
            },
        )
    }
    /// Starts the native owner with the selected staging or vectored send path.
    /// Vectored writes use the provider's existing owned segment submission;
    /// TLS may still copy plaintext into its bounded encryption records.
    pub fn start_with_options(
        config: ProducerConfig,
        options: HostProducerOptions,
    ) -> Result<Self, HostError> {
        #[cfg(target_os = "linux")]
        {
            let connection = config.connection_config().map_err(|_| {
                HostError::Config(SecurityError::InvalidConfig { field: "producer" })
            })?;
            let bounds = HostBounds::from_config(&connection).map_err(HostError::Config)?;
            let diagnostics = options.diagnostics.then(HostDiagnostics::default);
            let worker_diagnostics = diagnostics.clone();
            let mut host = Self::spawn_owner(move |send| {
                run_native(
                    config,
                    connection,
                    bounds,
                    send,
                    worker_diagnostics,
                    options,
                )
            })?;
            host.diagnostics = diagnostics;
            Ok(host)
        }
        #[cfg(not(target_os = "linux"))]
        {
            let _ = (config, options);
            Err(HostError::Connect(ConnectError::TransportUnavailable))
        }
    }
    #[cfg(any(target_os = "linux", test))]
    fn spawn_owner(
        run: impl FnOnce(std::sync::mpsc::SyncSender<Startup>) -> Result<EngineStatus, HostError>
        + Send
        + 'static,
    ) -> Result<Self, HostError> {
        let (send, receive) = sync_channel(1);
        let status = Arc::new(Mutex::new(HostStatus::Running));
        let thread_status = status.clone();
        let worker = thread::Builder::new()
            .name("kr-kafka-owner".into())
            .spawn(move || {
                let mut result = Err(HostError::Panicked);
                // A custom panic payload may itself panic during destruction.
                // Contain both levels before publishing the final owner status.
                kr_runtime::contain_panic(|| {
                    result = run(send);
                });
                *thread_status.lock().unwrap_or_else(|p| p.into_inner()) = match &result {
                    Ok(status) => HostStatus::Finished(*status),
                    Err(error) => HostStatus::Failed(error.clone()),
                };
                result
            })
            .map_err(|e| HostError::Thread(e.to_string()))?;
        let (client, calibration, backend) = match receive.recv() {
            Ok(Ok(value)) => value,
            Ok(Err(error)) => {
                let _ = join_worker(worker);
                return Err(error);
            }
            Err(_) => {
                let _ = join_worker(worker);
                return Err(HostError::Panicked);
            }
        };
        Ok(Self {
            client,
            diagnostics: None,
            calibration,
            backend,
            status,
            worker: Some(worker),
        })
    }
    pub fn diagnostics(&self) -> Option<HostDiagnostics> {
        self.diagnostics.clone()
    }
    pub fn client(&self) -> ProducerClient {
        self.client.clone()
    }
    pub fn calibration(&self) -> Calibration {
        self.calibration
    }
    pub fn backend(&self) -> Backend {
        self.backend
    }
    pub fn status(&self) -> HostStatus {
        self.status
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .clone()
    }
    pub fn close(&self, timeout: RuntimeDuration) -> Result<(), HostError> {
        self.client
            .close(timeout)
            .map_err(|e| HostError::Actor(e.to_string()))
    }
    /// Joins after the actor is terminal. Outstanding application-owned native
    /// leases must actually be released; a delivery deadline cannot free them.
    pub fn join(mut self) -> Result<EngineStatus, HostError> {
        join_worker(self.worker.take().expect("owner thread exists"))
    }
}

fn join_worker(
    worker: JoinHandle<Result<EngineStatus, HostError>>,
) -> Result<EngineStatus, HostError> {
    let mut result = Err(HostError::Panicked);
    kr_runtime::contain_panic(|| {
        if let Ok(output) = worker.join() {
            result = output;
        }
    });
    result
}
impl Drop for HostProducer {
    fn drop(&mut self) {
        if self.worker.is_some() {
            let _ = self.client.close(RuntimeDuration::ZERO);
        }
    }
}

#[cfg(target_os = "linux")]
fn run_native(
    config: ProducerConfig,
    connection: kr_kafka_client::config::ConnectionConfig,
    bounds: HostBounds,
    send: std::sync::mpsc::SyncSender<Startup>,
    diagnostics: Option<HostDiagnostics>,
    options: HostProducerOptions,
) -> Result<EngineStatus, HostError> {
    let prepared = (|| {
        let level = match config.compression {
            Compression::None => 1,
            Compression::Zstd { level } => level,
        };
        let calibration = calibrate(config.target_poll_ms, level, config.codec_window_log)
            .map_err(HostError::Config)?;
        let runtime = HostRuntime::new(HostConfig {
            max_tasks: 1,
            max_timers: bounds.streams + 8,
            max_ingress: bounds.streams * 4 + config.mailbox_capacity as usize,
            max_ingress_per_turn: config.max_completions_per_poll as usize,
            blocking_workers: 2,
            ..Default::default()
        })
        .map_err(|e| HostError::Runtime(e.to_string()))?;
        let mut limits = kr_kafka_producer::control::ControlLimits::from_config(&config);
        limits.owned_bytes = 64 * 1024;
        limits.auth_bytes = 16 * 1024;
        let codec =
            kr_kafka_client::control::ControlCodec::new(config.client_id.clone(), limits.common())
                .map_err(|error| HostError::Connect(ConnectError::Protocol(error)))?;
        let engine =
            ProducerEngine::new(config, None).map_err(|e| HostError::Engine(e.to_string()))?;
        let connector = HostConnector::new(
            runtime.handle(),
            connection,
            codec,
            Arc::new(crate::setup::ProducerSetupBudget::new(engine.credits())),
        )
        .map_err(HostError::Connect)?;
        if let Some(diagnostics) = &diagnostics {
            connector
                .attach_diagnostics(diagnostics, runtime.control().observer())
                .map_err(HostError::Connect)?;
        }
        let backend = connector.backend();
        let (client, actor) = ProducerActor::new(
            RuntimeHandle::Host(runtime.handle()),
            engine,
            connector,
            ClientClock::Host(runtime.control()),
            options.actor_config(calibration),
        )
        .map_err(|e| HostError::Actor(e.to_string()))?;
        Ok::<_, HostError>((runtime, client, actor, calibration, backend))
    })();
    let (mut runtime, client, actor, calibration, backend) = match prepared {
        Ok(value) => value,
        Err(error) => {
            let _ = send.send(Err(error.clone()));
            return Err(error);
        }
    };
    announce(send, &client, calibration, backend);
    let outcome = runtime
        .block_on(actor)
        .map_err(|e| HostError::Runtime(e.to_string()))
        .and_then(|r| r.map_err(|e| HostError::Actor(e.to_string())));
    let finished = runtime
        .finish()
        .map_err(|e| HostError::Runtime(e.to_string()));
    match outcome {
        Ok(status) => finished.map(|()| status),
        Err(error) => Err(error),
    }
}

#[cfg(test)]
mod tests;

#[cfg(any(target_os = "linux", test))]
fn announce(
    send: std::sync::mpsc::SyncSender<Startup>,
    client: &ProducerClient,
    calibration: Calibration,
    backend: Backend,
) {
    if send
        .send(Ok((client.clone(), calibration, backend)))
        .is_err()
    {
        let _ = client.close(RuntimeDuration::ZERO);
    }
}
