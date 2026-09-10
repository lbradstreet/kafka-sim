//! The in-simulation S3 server.

pub mod service;

use crate::sim::wire;
use kr_runtime_tokio::io::{AsyncReadExt, AsyncWriteExt};
use kr_runtime_tokio::net::{TcpListener, TcpStream};
use service::S3Service;
use std::io;
use std::rc::Rc;

/// A simulated S3 server serving [`S3Service`] over the simulated network.
#[derive(Clone, Debug, Default)]
pub struct SimServer {
    buckets: Vec<String>,
}

impl SimServer {
    /// Returns a server with no pre-created buckets.
    #[must_use]
    pub fn builder() -> Self {
        Self::default()
    }

    /// Pre-creates a bucket before serving.
    #[must_use]
    pub fn with_bucket(mut self, bucket: impl Into<String>) -> Self {
        self.buckets.push(bucket.into());
        self
    }

    /// Serves requests on an already-bound listener, forever.
    ///
    /// The caller binds the listener (choosing which ambient
    /// [`SimNetContext`](kr_runtime_tokio::net::SimNetContext) provides the
    /// server's node identity) and typically spawns this future; it returns
    /// only when accepting fails.
    ///
    /// # Errors
    ///
    /// Returns the first accept failure.
    pub async fn serve(self, listener: TcpListener) -> io::Result<()> {
        let service = Rc::new(S3Service::new());
        for bucket in &self.buckets {
            service.create_bucket(bucket);
        }
        loop {
            let (stream, _) = listener.accept().await?;
            let service = Rc::clone(&service);
            kr_runtime_tokio::task::spawn(async move {
                // A malformed or interrupted exchange only ends that
                // connection; the model state is untouched by decode errors.
                let _ = handle_connection(&service, stream).await;
            });
        }
    }
}

async fn handle_connection(service: &S3Service, mut stream: TcpStream) -> io::Result<()> {
    let mut request = Vec::new();
    stream.read_to_end(&mut request).await?;
    let request = wire::decode_request(&request)?;
    let response = service.handle(&request);
    stream.write_all(&wire::encode_response(&response)).await?;
    stream.shutdown().await
}
