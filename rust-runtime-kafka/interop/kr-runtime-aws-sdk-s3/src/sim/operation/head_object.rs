//! Simulated `HeadObject`.

pub use aws_sdk_s3::operation::head_object::{HeadObjectError, HeadObjectInput, HeadObjectOutput};

pub mod builders {
    use super::{HeadObjectError, HeadObjectOutput};
    use crate::sim::client::{Client, dispatch_error, protocol_error, service_error};
    use crate::sim::wire::{ErrorCode, Request, Response};
    use aws_sdk_s3::error::SdkError;
    use aws_sdk_s3::types::error::NotFound;

    pub use aws_sdk_s3::operation::head_object::builders::HeadObjectInputBuilder;

    impl Client {
        /// Starts a simulated `HeadObject`.
        #[must_use]
        pub fn head_object(&self) -> HeadObjectFluentBuilder {
            HeadObjectFluentBuilder {
                client: self.clone(),
                inner: HeadObjectInputBuilder::default(),
            }
        }
    }

    /// Fluent builder for the simulated `HeadObject`.
    pub struct HeadObjectFluentBuilder {
        client: Client,
        inner: HeadObjectInputBuilder,
    }

    impl HeadObjectFluentBuilder {
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

        /// Sends the request over the simulated network.
        ///
        /// # Errors
        ///
        /// A missing object or bucket is the SDK's typed `NotFound`.
        pub async fn send(self) -> Result<HeadObjectOutput, SdkError<HeadObjectError>> {
            let input = self.inner.build().map_err(SdkError::construction_failure)?;
            let bucket = input.bucket().unwrap_or_default().to_string();
            let key = input.key().unwrap_or_default().to_string();
            let response = self
                .client
                .exchange(&Request::Head { bucket, key })
                .await
                .map_err(dispatch_error)?;
            match response {
                Response::HeadOk {
                    e_tag,
                    content_length,
                } => Ok(HeadObjectOutput::builder()
                    .e_tag(e_tag)
                    .content_length(content_length as i64)
                    .build()),
                Response::Error { code, message } => Err(match code {
                    ErrorCode::NotFound | ErrorCode::NoSuchKey => service_error(
                        HeadObjectError::NotFound(NotFound::builder().message(message).build()),
                        404,
                    ),
                    ErrorCode::NoSuchBucket => {
                        service_error(HeadObjectError::unhandled(message), 404)
                    }
                }),
                _ => Err(protocol_error("HeadObject")),
            }
        }
    }
}
