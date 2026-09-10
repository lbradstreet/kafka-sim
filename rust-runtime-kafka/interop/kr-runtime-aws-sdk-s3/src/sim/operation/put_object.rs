//! Simulated `PutObject`.

pub use aws_sdk_s3::operation::put_object::{PutObjectError, PutObjectInput, PutObjectOutput};

pub mod builders {
    use super::{PutObjectError, PutObjectOutput};
    use crate::sim::client::{Client, dispatch_error, protocol_error, service_error};
    use crate::sim::wire::{ErrorCode, Request, Response};
    use aws_sdk_s3::error::SdkError;
    use aws_sdk_s3::primitives::ByteStream;
    use std::io;

    pub use aws_sdk_s3::operation::put_object::builders::PutObjectInputBuilder;

    impl Client {
        /// Starts a simulated `PutObject`.
        #[must_use]
        pub fn put_object(&self) -> PutObjectFluentBuilder {
            PutObjectFluentBuilder {
                client: self.clone(),
                inner: PutObjectInputBuilder::default(),
            }
        }
    }

    /// Fluent builder for the simulated `PutObject`.
    pub struct PutObjectFluentBuilder {
        client: Client,
        inner: PutObjectInputBuilder,
    }

    impl PutObjectFluentBuilder {
        #[must_use]
        pub fn bucket(mut self, input: impl Into<String>) -> Self {
            self.inner = self.inner.bucket(input.into());
            self
        }

        #[must_use]
        pub fn key(mut self, input: impl Into<String>) -> Self {
            self.inner = self.inner.key(input.into());
            self
        }

        #[must_use]
        pub fn body(mut self, input: ByteStream) -> Self {
            self.inner = self.inner.body(input);
            self
        }

        /// Sends the request over the simulated network.
        ///
        /// # Errors
        ///
        /// Construction failures, transport failures (as SDK dispatch
        /// failures), and modeled service errors, exactly as the real SDK
        /// shapes them.
        pub async fn send(self) -> Result<PutObjectOutput, SdkError<PutObjectError>> {
            let input = self.inner.build().map_err(SdkError::construction_failure)?;
            let bucket = input.bucket().unwrap_or_default().to_string();
            let key = input.key().unwrap_or_default().to_string();
            let body = input
                .body
                .collect()
                .await
                .map_err(|error| dispatch_error(io::Error::new(io::ErrorKind::InvalidData, error)))?
                .to_vec();
            let response = self
                .client
                .exchange(&Request::Put { bucket, key, body })
                .await
                .map_err(dispatch_error)?;
            match response {
                Response::PutOk { e_tag } => Ok(PutObjectOutput::builder().e_tag(e_tag).build()),
                Response::Error { code, message } => {
                    debug_assert_eq!(code, ErrorCode::NoSuchBucket);
                    Err(service_error(PutObjectError::unhandled(message), 404))
                }
                _ => Err(protocol_error("PutObject")),
            }
        }
    }
}
