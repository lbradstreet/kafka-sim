//! Simulated `GetObject`.

pub use aws_sdk_s3::operation::get_object::{GetObjectError, GetObjectInput, GetObjectOutput};

pub mod builders {
    use super::{GetObjectError, GetObjectOutput};
    use crate::sim::client::{Client, dispatch_error, protocol_error, service_error};
    use crate::sim::wire::{ErrorCode, Request, Response};
    use aws_sdk_s3::error::SdkError;
    use aws_sdk_s3::primitives::ByteStream;
    use aws_sdk_s3::types::error::NoSuchKey;

    pub use aws_sdk_s3::operation::get_object::builders::GetObjectInputBuilder;

    impl Client {
        /// Starts a simulated `GetObject`.
        #[must_use]
        pub fn get_object(&self) -> GetObjectFluentBuilder {
            GetObjectFluentBuilder {
                client: self.clone(),
                inner: GetObjectInputBuilder::default(),
            }
        }
    }

    /// Fluent builder for the simulated `GetObject`.
    pub struct GetObjectFluentBuilder {
        client: Client,
        inner: GetObjectInputBuilder,
    }

    impl GetObjectFluentBuilder {
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
        /// `NoSuchKey` is the SDK's typed error; a missing bucket surfaces
        /// as the SDK's unhandled service error, matching madsim's model.
        pub async fn send(self) -> Result<GetObjectOutput, SdkError<GetObjectError>> {
            let input = self.inner.build().map_err(SdkError::construction_failure)?;
            let bucket = input.bucket().unwrap_or_default().to_string();
            let key = input.key().unwrap_or_default().to_string();
            let response = self
                .client
                .exchange(&Request::Get { bucket, key })
                .await
                .map_err(dispatch_error)?;
            match response {
                Response::GetOk { e_tag, body } => Ok(GetObjectOutput::builder()
                    .e_tag(e_tag)
                    .content_length(body.len() as i64)
                    .body(ByteStream::from(body))
                    .build()),
                Response::Error { code, message } => Err(match code {
                    ErrorCode::NoSuchKey => service_error(
                        GetObjectError::NoSuchKey(NoSuchKey::builder().message(message).build()),
                        404,
                    ),
                    _ => service_error(GetObjectError::unhandled(message), 404),
                }),
                _ => Err(protocol_error("GetObject")),
            }
        }
    }
}
