//! Endpoint, transport and security inputs shared by Kafka client roles.
use kr_runtime::RuntimeDuration;
use std::fmt;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BrokerEndpoint {
    pub host: String,
    pub port: u16,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TransportPolicy {
    Uring,
    Readiness,
    Auto,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SaslMechanism {
    Plain,
    ScramSha256,
    ScramSha512,
}
/// Debug output deliberately excludes the secret's contents.
#[derive(Clone)]
pub struct Secret(String);
impl Secret {
    #[must_use]
    pub fn new(value: String) -> Self {
        Self(value)
    }
    #[must_use]
    pub fn expose(&self) -> &str {
        &self.0
    }
}
impl fmt::Debug for Secret {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("Secret(<redacted>)")
    }
}
#[derive(Clone, Debug, Default)]
pub struct TlsConfig {
    pub roots_der: Vec<Vec<u8>>,
    pub use_system_roots: bool,
    pub server_name: Option<String>,
}
#[derive(Clone, Debug)]
pub enum SecurityConfig {
    Plaintext,
    Tls {
        tls: TlsConfig,
    },
    SaslTls {
        tls: TlsConfig,
        mechanism: SaslMechanism,
        username: String,
        password: Secret,
    },
}

/// Workload-independent inputs for a native connection provider. The upper
/// layer derives these bounds before construction; the host validates kernel,
/// TLS and arithmetic constraints before opening native resources.
#[derive(Clone, Debug)]
pub struct ConnectionConfig {
    pub client_id: String,
    pub max_connections: usize,
    pub max_operation_bytes: usize,
    pub rx_bytes_per_connection: usize,
    pub staging_bytes_per_connection: usize,
    pub control_jobs: usize,
    pub control_bytes: usize,
    pub connect_timeout: RuntimeDuration,
    pub tls_plaintext_bytes: usize,
    pub tls_ciphertext_bytes: usize,
    pub security: SecurityConfig,
    pub transport: TransportPolicy,
}
