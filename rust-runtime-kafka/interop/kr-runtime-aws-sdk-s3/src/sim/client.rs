//! The simulated S3 client transport.

use crate::sim::config::Config;
use crate::sim::wire::{self, Request, Response};
use aws_sdk_s3::error::SdkError;
use aws_smithy_runtime_api::client::result::ConnectorError;
use aws_smithy_runtime_api::http::{Response as HttpResponse, StatusCode};
use aws_smithy_types::body::SdkBody;
use kr_runtime_tokio::io::{AsyncReadExt, AsyncWriteExt};
use kr_runtime_tokio::net::TcpStream;
use std::io;
use std::rc::Rc;

/// An aws-sdk-s3-shaped client whose operations run over the simulated
/// network against an in-simulation [`S3Service`](crate::server::S3Service).
///
/// Each operation opens one connection to the configured endpoint through
/// the ambient [`SimNetContext`](kr_runtime_tokio::net::SimNetContext), sends one
/// request frame, half-closes, and reads one response frame. Operation
/// methods (`put_object`, `get_object`, …) are defined in
/// [`crate::operation`], mirroring the SDK's fluent-builder surface.
#[derive(Clone, Debug)]
pub struct Client {
    config: Rc<Config>,
}

impl Client {
    /// Creates a client from a simulated configuration.
    #[must_use]
    pub fn from_conf(config: Config) -> Self {
        Self {
            config: Rc::new(config),
        }
    }

    /// Returns the client's configuration.
    #[must_use]
    pub fn config(&self) -> &Config {
        &self.config
    }

    pub(crate) async fn exchange(&self, request: &Request) -> io::Result<Response> {
        let mut stream = TcpStream::connect(self.config.endpoint).await?;
        stream.write_all(&wire::encode_request(request)).await?;
        stream.shutdown().await?;
        let mut response = Vec::new();
        stream.read_to_end(&mut response).await?;
        wire::decode_response(&response)
    }
}

/// Wraps a transport-level failure the way the SDK reports connector errors.
pub(crate) fn dispatch_error<E>(error: io::Error) -> SdkError<E> {
    SdkError::dispatch_failure(ConnectorError::io(Box::new(error)))
}

/// Wraps a modeled service error with an SDK-shaped raw response.
pub(crate) fn service_error<E>(error: E, status: u16) -> SdkError<E> {
    let status = StatusCode::try_from(status).expect("status codes used by the shim are valid");
    SdkError::service_error(error, HttpResponse::new(status, SdkBody::empty()))
}

/// Reports a response frame that does not answer the submitted request.
pub(crate) fn protocol_error<E>(operation: &'static str) -> SdkError<E> {
    dispatch_error(io::Error::new(
        io::ErrorKind::InvalidData,
        format!("simulated S3 returned a response that does not answer {operation}"),
    ))
}
