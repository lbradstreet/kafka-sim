//! Simulated `DeleteObject`.

pub use aws_sdk_s3::operation::delete_object::{
    DeleteObjectError, DeleteObjectInput, DeleteObjectOutput,
};

pub mod builders {
    use super::{DeleteObjectError, DeleteObjectOutput};
    use crate::sim::client::{Client, dispatch_error, protocol_error, service_error};
    use crate::sim::wire::{Request, Response};
    use aws_sdk_s3::error::SdkError;

    pub use aws_sdk_s3::operation::delete_object::builders::DeleteObjectInputBuilder;

    impl Client {
        /// Starts a simulated `DeleteObject`.
        #[must_use]
        pub fn delete_object(&self) -> DeleteObjectFluentBuilder {
            DeleteObjectFluentBuilder {
                client: self.clone(),
                inner: DeleteObjectInputBuilder::default(),
            }
        }
    }

    /// Fluent builder for the simulated `DeleteObject`.
    pub struct DeleteObjectFluentBuilder {
        client: Client,
        inner: DeleteObjectInputBuilder,
    }

    impl DeleteObjectFluentBuilder {
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
        /// Deleting an absent key succeeds, matching S3's idempotent delete.
        ///
        /// # Errors
        ///
        /// Transport failures and a missing bucket, as the SDK shapes them.
        pub async fn send(self) -> Result<DeleteObjectOutput, SdkError<DeleteObjectError>> {
            let input = self.inner.build().map_err(SdkError::construction_failure)?;
            let bucket = input.bucket().unwrap_or_default().to_string();
            let key = input.key().unwrap_or_default().to_string();
            let response = self
                .client
                .exchange(&Request::Delete { bucket, key })
                .await
                .map_err(dispatch_error)?;
            match response {
                Response::DeleteOk => Ok(DeleteObjectOutput::builder().build()),
                Response::Error { message, .. } => {
                    Err(service_error(DeleteObjectError::unhandled(message), 404))
                }
                _ => Err(protocol_error("DeleteObject")),
            }
        }
    }
}
