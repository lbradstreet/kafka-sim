//! Checked native transport mapping, before provider threads or sockets exist.
use crate::{SecurityError, stream::TlsStreamLimits, tls::TlsLimits};
use kr_kafka_client::config::{ConnectionConfig, SaslMechanism, SecurityConfig, TransportPolicy};
use std::time::Duration;

/// Auto fallback remains disabled until the documented native conformance gate
/// has executed. Explicit readiness selection is available for its test rollout.
pub const READINESS_AUTO_GATE_PASSED: bool = false;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Backend {
    Uring,
    Readiness,
}

/// Only classified kernel-unavailable errors may fall back. Resource/config
/// errors never change the requested backend.
pub fn choose_backend(
    policy: TransportPolicy,
    uring_available: bool,
    uring_unavailable: bool,
) -> Result<Backend, SecurityError> {
    match policy {
        TransportPolicy::Readiness => Ok(Backend::Readiness),
        TransportPolicy::Uring | TransportPolicy::Auto if uring_available => Ok(Backend::Uring),
        TransportPolicy::Auto if uring_unavailable && READINESS_AUTO_GATE_PASSED => {
            Ok(Backend::Readiness)
        }
        _ => Err(SecurityError::InvalidConfig {
            field: "transport unavailable",
        }),
    }
}

#[derive(Clone, Copy, Debug)]
pub struct HostBounds {
    pub streams: usize,
    pub ring_entries: u32,
    pub operation_bytes: usize,
    pub read_bytes: usize,
    pub write_bytes: usize,
    pub control_jobs: usize,
    pub control_bytes: usize,
    pub connect_timeout: Duration,
    pub tls_client: Option<TlsLimits>,
    pub tls_stream: Option<TlsStreamLimits>,
}
impl HostBounds {
    /// Validates all arithmetic before any native resource construction.
    pub fn from_config(config: &ConnectionConfig) -> Result<Self, SecurityError> {
        if config.client_id.len() > i16::MAX as usize {
            return Err(invalid("client_id"));
        }
        validate_security(config)?;
        let streams = config.max_connections;
        if streams == 0
            || config.max_operation_bytes < 8
            || config.max_operation_bytes > u32::MAX as usize
            || config.rx_bytes_per_connection < 8
            || config.rx_bytes_per_connection > config.max_operation_bytes
            || config.staging_bytes_per_connection == 0
            || config.staging_bytes_per_connection > config.max_operation_bytes
        {
            return Err(invalid("connection buffers"));
        }
        let ring_entries = u32::try_from(streams.checked_mul(2).ok_or(invalid("ring_entries"))?)
            .ok()
            .and_then(u32::checked_next_power_of_two)
            .filter(|n| *n <= 32768)
            .ok_or(invalid("ring_entries"))?;
        let operation_bytes = config.max_operation_bytes;
        let read_bytes = streams
            .checked_mul(config.rx_bytes_per_connection)
            .ok_or(invalid("read_bytes"))?;
        let write_bytes = streams
            .checked_mul(operation_bytes)
            .ok_or(invalid("write_bytes"))?;
        let connect_timeout = Duration::from_nanos(config.connect_timeout.as_nanos());
        if connect_timeout.is_zero() || connect_timeout > Duration::from_secs(86400) {
            return Err(invalid("connect_timeout"));
        }
        // Setup is serial per connection; a small dedicated fleet leaves at least
        // one control worker available while another resolver is blocked.
        let control_jobs = config.control_jobs.min(8);
        if control_jobs < 2 || config.control_bytes < 128 * 1024 {
            return Err(invalid("control reserve"));
        }
        let mut result = Self {
            streams,
            ring_entries,
            operation_bytes,
            read_bytes,
            write_bytes,
            control_jobs,
            control_bytes: config.control_bytes,
            connect_timeout,
            tls_client: None,
            tls_stream: None,
        };
        if !matches!(config.security, SecurityConfig::Plaintext) {
            let plain = config.tls_plaintext_bytes;
            let cipher = config.tls_ciphertext_bytes;
            // The adapter owns a 16KiB plaintext stage and two transport arrays;
            // the passive client owns the remaining configured capacity exactly.
            if !(32 * 1024..=1024 * 1024 + 16 * 1024).contains(&plain)
                || !(36 * 1024 + 2..=16 * 1024 * 1024 + 32 * 1024).contains(&cipher)
                || !cipher.is_multiple_of(2)
            {
                return Err(invalid("TLS buffers"));
            }
            let transport = (16 * 1024).min((cipher - 36 * 1024) / 2);
            result.tls_client = Some(TlsLimits {
                plaintext_bytes: plain - 16 * 1024,
                ciphertext_bytes: cipher - 2 * transport,
            });
            result.tls_stream = Some(TlsStreamLimits {
                read_operations: 1,
                write_operations: 1,
                control_operations: 2,
                read_bytes: operation_bytes,
                write_bytes: operation_bytes,
                operation_bytes,
                max_segments: 64,
                transport_bytes: transport,
                transitions_per_poll: 64,
            });
        }
        Ok(result)
    }
}
fn validate_security(config: &ConnectionConfig) -> Result<(), SecurityError> {
    let tls = match &config.security {
        SecurityConfig::Plaintext => return Ok(()),
        SecurityConfig::Tls { tls } => tls,
        SecurityConfig::SaslTls {
            tls,
            mechanism,
            username,
            password,
        } => {
            for credential in [username.as_str(), password.expose()] {
                if credential.is_empty()
                    || credential.len() > crate::sasl::SaslLimits::default().credential_bytes
                    || credential.contains('\0')
                    || (*mechanism != SaslMechanism::Plain
                        && (!credential.is_ascii()
                            || credential.bytes().any(|byte| byte.is_ascii_control())))
                {
                    return Err(invalid("security.credentials"));
                }
            }
            tls
        }
    };
    if !tls.use_system_roots && tls.roots_der.is_empty() {
        return Err(invalid("security.tls"));
    }
    if tls.server_name.as_ref().is_some_and(|name| {
        name.is_empty()
            || name.len() > 253
            || rustls::pki_types::ServerName::try_from(name.as_str()).is_err()
    }) {
        return Err(invalid("security.server_name"));
    }
    tls.roots_der.iter().try_fold(0usize, |bytes, root| {
        bytes
            .checked_add(root.len())
            .filter(|bytes| *bytes <= config.control_bytes)
            .ok_or(invalid("security.roots"))
    })?;
    Ok(())
}
fn invalid(field: &'static str) -> SecurityError {
    SecurityError::InvalidConfig { field }
}

#[cfg(test)]
mod tests {
    use super::*;
    use kr_kafka_client::config::TlsConfig;
    fn fixture() -> ConnectionConfig {
        ConnectionConfig {
            client_id: "host-test".into(),
            max_connections: 66,
            max_operation_bytes: 2 * 1024 * 1024,
            rx_bytes_per_connection: 1024 * 1024,
            staging_bytes_per_connection: 64 * 1024,
            control_jobs: 16,
            control_bytes: 1024 * 1024,
            connect_timeout: kr_runtime::RuntimeDuration::from_nanos(30_000_000_000),
            tls_plaintext_bytes: 32 * 1024,
            tls_ciphertext_bytes: 64 * 1024,
            security: SecurityConfig::Plaintext,
            transport: TransportPolicy::Uring,
        }
    }
    #[test]
    fn startup_maps_streams_ring_and_direction_reserves() {
        let config = fixture();
        let bounds = HostBounds::from_config(&config).unwrap();
        assert_eq!(bounds.streams, 66);
        assert_eq!(bounds.ring_entries, 256);
        assert_eq!(bounds.read_bytes, 66 * 1024 * 1024);
        assert_eq!(bounds.control_jobs, 8);
        assert!(
            HostBounds::from_config(&ConnectionConfig {
                max_connections: usize::MAX,
                ..config
            })
            .is_err()
        );
    }
    #[test]
    fn required_uring_and_ungated_auto_never_silently_fall_back() {
        assert!(choose_backend(TransportPolicy::Uring, false, true).is_err());
        assert!(choose_backend(TransportPolicy::Auto, false, true).is_err());
        assert!(choose_backend(TransportPolicy::Auto, false, false).is_err());
        assert_eq!(
            choose_backend(TransportPolicy::Readiness, false, false).unwrap(),
            Backend::Readiness
        );
        assert_eq!(
            choose_backend(TransportPolicy::Auto, true, false).unwrap(),
            Backend::Uring
        );
    }
    #[test]
    fn default_tls_mapping_counts_every_fixed_buffer_once() {
        let config = ConnectionConfig {
            security: SecurityConfig::Tls {
                tls: TlsConfig {
                    roots_der: vec![vec![1]],
                    ..Default::default()
                },
            },
            ..fixture()
        };
        let bounds = HostBounds::from_config(&config).unwrap();
        let client = bounds.tls_client.unwrap();
        let stream = bounds.tls_stream.unwrap();
        assert_eq!(
            client.plaintext_bytes + 16 * 1024,
            config.tls_plaintext_bytes
        );
        assert_eq!(
            client.ciphertext_bytes + 2 * stream.transport_bytes,
            config.tls_ciphertext_bytes
        );
        assert!(
            HostBounds::from_config(&ConnectionConfig {
                tls_plaintext_bytes: 16 * 1024,
                ..config.clone()
            })
            .is_err()
        );
        assert!(
            HostBounds::from_config(&ConnectionConfig {
                tls_ciphertext_bytes: 36 * 1024,
                ..config
            })
            .is_err()
        );
    }
    #[test]
    fn generic_config_rejects_invalid_buffers_names_and_credentials_before_provisioning() {
        for config in [
            ConnectionConfig {
                max_connections: 0,
                ..fixture()
            },
            ConnectionConfig {
                max_operation_bytes: 0,
                ..fixture()
            },
            ConnectionConfig {
                rx_bytes_per_connection: 7,
                ..fixture()
            },
            ConnectionConfig {
                staging_bytes_per_connection: 0,
                ..fixture()
            },
            ConnectionConfig {
                staging_bytes_per_connection: usize::MAX,
                ..fixture()
            },
            ConnectionConfig {
                control_jobs: 1,
                ..fixture()
            },
            ConnectionConfig {
                connect_timeout: kr_runtime::RuntimeDuration::ZERO,
                ..fixture()
            },
            ConnectionConfig {
                client_id: "x".repeat(i16::MAX as usize + 1),
                ..fixture()
            },
            ConnectionConfig {
                security: SecurityConfig::Tls {
                    tls: TlsConfig::default(),
                },
                ..fixture()
            },
            ConnectionConfig {
                security: SecurityConfig::Tls {
                    tls: TlsConfig {
                        use_system_roots: true,
                        server_name: Some("bad\0name".into()),
                        ..Default::default()
                    },
                },
                ..fixture()
            },
            ConnectionConfig {
                security: SecurityConfig::SaslTls {
                    tls: TlsConfig {
                        use_system_roots: true,
                        ..Default::default()
                    },
                    mechanism: SaslMechanism::Plain,
                    username: "user".into(),
                    password: kr_kafka_client::config::Secret::new("bad\0password".into()),
                },
                ..fixture()
            },
        ] {
            assert!(HostBounds::from_config(&config).is_err());
        }
        // There is no plaintext SASL configuration: credentials always require TLS.
        let config = ConnectionConfig {
            security: SecurityConfig::SaslTls {
                tls: TlsConfig {
                    use_system_roots: true,
                    ..Default::default()
                },
                mechanism: SaslMechanism::Plain,
                username: "user".into(),
                password: kr_kafka_client::config::Secret::new("password".into()),
            },
            ..fixture()
        };
        assert!(
            HostBounds::from_config(&config)
                .unwrap()
                .tls_client
                .is_some()
        );
    }
}
