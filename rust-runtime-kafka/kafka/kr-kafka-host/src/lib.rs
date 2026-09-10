//! Bounded Kafka host construction, native transport, TLS and SASL.
//!
//! Security transforms stay passive. The native connector owns a shared Linux
//! provider and a bounded control fleet. Producer ownership is provided separately
//! by `kr-kafka-producer-host`.
#![forbid(unsafe_code)]

pub mod config;
pub mod control;
pub mod diagnostics;
#[cfg(target_os = "linux")]
pub mod native;
pub mod sasl;
pub mod stream;
pub mod tls;

use std::fmt;

/// Failures contain no credentials, challenges, proofs, or plaintext.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum SecurityError {
    InvalidConfig {
        field: &'static str,
    },
    ResourceExhausted {
        resource: &'static str,
        limit: usize,
    },
    InvalidState,
    InvalidCredentials,
    InvalidChallenge,
    UnsupportedExtension,
    InvalidServerNonce,
    InvalidServerSignature,
    AuthenticationRejected,
    EntropyUnavailable,
    /// The injected transport failed; this is not a peer-authentication failure.
    Network(kr_runtime_io::network::NetworkError),
    TlsFailed,
    TruncatedTls,
    WorkerPanicked,
}

impl fmt::Display for SecurityError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidConfig { field } => write!(f, "invalid security configuration: {field}"),
            Self::ResourceExhausted { resource, limit } => {
                write!(f, "{resource} exhausted (limit {limit})")
            }
            Self::InvalidState => f.write_str("invalid security state"),
            Self::InvalidCredentials => f.write_str("invalid SASL credentials"),
            Self::InvalidChallenge => f.write_str("invalid SASL challenge"),
            Self::UnsupportedExtension => f.write_str("unsupported mandatory SASL extension"),
            Self::InvalidServerNonce => f.write_str("invalid SCRAM server nonce"),
            Self::InvalidServerSignature => f.write_str("SCRAM server signature mismatch"),
            Self::AuthenticationRejected => f.write_str("SASL authentication rejected"),
            Self::EntropyUnavailable => f.write_str("cryptographic entropy unavailable"),
            Self::Network(error) => write!(f, "TLS transport failed: {error}"),
            Self::TlsFailed => f.write_str("TLS authentication or record processing failed"),
            Self::TruncatedTls => f.write_str("transport ended before TLS close_notify"),
            Self::WorkerPanicked => f.write_str("security control worker panicked"),
        }
    }
}

impl std::error::Error for SecurityError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Network(error) => Some(error),
            _ => None,
        }
    }
}

fn reserve(
    bytes: &mut Vec<u8>,
    additional: usize,
    resource: &'static str,
) -> Result<(), SecurityError> {
    bytes
        .try_reserve_exact(additional)
        .map_err(|_| SecurityError::ResourceExhausted {
            resource,
            limit: additional,
        })
}
