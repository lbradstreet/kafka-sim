use kr_kafka_producer::{
    config::{
        BrokerEndpoint, Compression, ProducerConfig, SaslMechanism, Secret, SecurityConfig,
        TlsConfig, TransportPolicy,
    },
    routing::UnkeyedPolicy,
};
use kr_runtime::RuntimeDuration;
use serde::{Deserialize, Serialize};

/// Shared comparison profile. Unknown fields are errors, preventing typo-driven
/// comparisons with silently different settings. Secret values live in env vars.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Profile {
    pub bootstrap: String,
    pub topic: String,
    pub rate: u64,
    pub records: u64,
    pub record_bytes: usize,
    pub seed: u64,
    pub pattern: String,
    pub routing: String,
    pub partitions: u32,
    pub compression: String,
    pub linger_us: u64,
    pub batch_bytes: u32,
    pub request_bytes: u32,
    pub input_bytes: usize,
    pub delivery_timeout_ms: u64,
    pub request_timeout_ms: u64,
    pub backend: String,
    pub security: String,
    /// Opt-in diagnostic run; native completion clocks add measurement overhead.
    #[serde(default)]
    pub native_diagnostics: bool,
    #[serde(default)]
    pub ca_der: Option<String>,
    #[serde(default)]
    pub ca_pem: Option<String>,
    #[serde(default)]
    pub username_env: Option<String>,
    #[serde(default)]
    pub password_env: Option<String>,
}
impl Profile {
    pub fn validate(&self) -> Result<(), String> {
        crate::OfferedLoad::new(self.rate, self.records).map_err(str::to_owned)?;
        if self.records > 100_000_000
            || !(1..=1024 * 1024).contains(&self.record_bytes)
            || self.topic.is_empty()
            || self.topic.len() > 249
            || !(1..=65536).contains(&self.partitions)
            || !["compressible", "incompressible"].contains(&self.pattern.as_str())
            || !["hot", "many", "skewed", "unkeyed"].contains(&self.routing.as_str())
            || !["none", "zstd1", "zstd3"].contains(&self.compression.as_str())
            || !["uring", "readiness"].contains(&self.backend.as_str())
            || !["plaintext", "tls", "plain", "scram256", "scram512"]
                .contains(&self.security.as_str())
            || self.record_bytes + 128 > self.request_bytes as usize
            || !(1024..=1024 * 1024).contains(&self.batch_bytes)
            || self.input_bytes > 1024 * 1024 * 1024
            || self.linger_us > 1_000_000
            || !(1..=600_000).contains(&self.request_timeout_ms)
            || self.delivery_timeout_ms < self.request_timeout_ms
            || self.delivery_timeout_ms > 3_600_000
        {
            return Err("invalid benchmark profile bounds or enum".into());
        }
        Ok(())
    }
    pub fn producer(&self) -> Result<ProducerConfig, String> {
        self.validate()?;
        let bootstrap = self
            .bootstrap
            .split(',')
            .map(|endpoint| {
                let (host, port) = endpoint
                    .rsplit_once(':')
                    .ok_or("bootstrap must contain host:port")?;
                let host = host.trim_matches(['[', ']']);
                if host.is_empty() {
                    return Err("bootstrap host is empty".to_string());
                }
                Ok(BrokerEndpoint {
                    host: host.into(),
                    port: port.parse().map_err(|_| "invalid bootstrap port")?,
                })
            })
            .collect::<Result<Vec<_>, String>>()?;
        let tls = if self.security == "plaintext" {
            None
        } else {
            let path = self.ca_der.as_ref().ok_or("TLS profile requires ca_der")?;
            Some(TlsConfig {
                roots_der: vec![std::fs::read(path).map_err(|e| e.to_string())?],
                ..Default::default()
            })
        };
        let security = match self.security.as_str() {
            "plaintext" => SecurityConfig::Plaintext,
            "tls" => SecurityConfig::Tls { tls: tls.unwrap() },
            other => {
                let username = std::env::var(
                    self.username_env
                        .as_ref()
                        .ok_or("SASL requires username_env")?,
                )
                .map_err(|_| "username env is absent")?;
                let password = std::env::var(
                    self.password_env
                        .as_ref()
                        .ok_or("SASL requires password_env")?,
                )
                .map_err(|_| "password env is absent")?;
                let mechanism = match other {
                    "plain" => SaslMechanism::Plain,
                    "scram256" => SaslMechanism::ScramSha256,
                    _ => SaslMechanism::ScramSha512,
                };
                SecurityConfig::SaslTls {
                    tls: tls.unwrap(),
                    mechanism,
                    username,
                    password: Secret::new(password),
                }
            }
        };
        let config = ProducerConfig {
            bootstrap,
            client_id: "kr-open-loop".into(),
            security,
            transport: if self.backend == "uring" {
                TransportPolicy::Uring
            } else {
                TransportPolicy::Readiness
            },
            compression: match self.compression.as_str() {
                "none" => Compression::None,
                "zstd1" => Compression::Zstd { level: 1 },
                _ => Compression::Zstd { level: 3 },
            },
            linger_max: RuntimeDuration::from_nanos(self.linger_us * 1000),
            linger_skip_below_rate: None,
            batch_target_bytes: self.batch_bytes,
            request_target_bytes: self.request_bytes,
            request_hard_bytes: self.request_bytes,
            batch_hard_bytes: self.request_bytes,
            input_bytes: self.input_bytes,
            delivery_timeout: RuntimeDuration::from_nanos(self.delivery_timeout_ms * 1_000_000),
            request_timeout: RuntimeDuration::from_nanos(self.request_timeout_ms * 1_000_000),
            unkeyed_policy: UnkeyedPolicy::default(),
            ..Default::default()
        };
        config.validate().map_err(|e| e.to_string())?;
        Ok(config)
    }
    /// Explicit hints isolate batching tests. Skewed keys exercise each client's
    /// murmur2-compatible keyed routing (90% share key zero).
    pub fn route(&self, index: u64) -> (Option<i32>, Option<[u8; 8]>) {
        match self.routing.as_str() {
            "hot" => (Some(0), None),
            "many" => (Some((index % u64::from(self.partitions)) as i32), None),
            "skewed" => (
                None,
                Some((if index.is_multiple_of(10) { index } else { 0 }).to_be_bytes()),
            ),
            _ => (None, None),
        }
    }
}
