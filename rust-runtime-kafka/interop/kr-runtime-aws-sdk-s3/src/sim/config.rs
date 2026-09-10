//! Sim-only client configuration.
//!
//! The simulated client needs exactly one thing the real SDK resolves from
//! its environment: the endpoint to connect to on the simulated network.

use std::net::SocketAddr;

/// Configuration for the simulated [`Client`](crate::Client).
#[derive(Clone, Copy, Debug)]
pub struct Config {
    pub(crate) endpoint: SocketAddr,
}

impl Config {
    /// Returns a builder with no endpoint configured.
    #[must_use]
    pub fn builder() -> Builder {
        Builder { endpoint: None }
    }

    /// Returns the configured simulated endpoint.
    #[must_use]
    pub const fn endpoint_addr(&self) -> SocketAddr {
        self.endpoint
    }
}

/// Builder for [`Config`].
#[derive(Clone, Copy, Debug, Default)]
pub struct Builder {
    endpoint: Option<SocketAddr>,
}

impl Builder {
    /// Sets the simulated endpoint the client connects to.
    #[must_use]
    pub fn endpoint_addr(mut self, endpoint: SocketAddr) -> Self {
        self.endpoint = Some(endpoint);
        self
    }

    /// Builds the configuration.
    ///
    /// # Panics
    ///
    /// Panics when no endpoint was configured: the simulated client has no
    /// region or environment resolution to fall back on.
    #[must_use]
    pub fn build(self) -> Config {
        Config {
            endpoint: self
                .endpoint
                .expect("simulated S3 Config requires endpoint_addr"),
        }
    }
}
