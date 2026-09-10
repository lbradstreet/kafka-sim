//! Simulated `ListObjectsV2`.

pub use aws_sdk_s3::operation::list_objects_v2::{
    ListObjectsV2Error, ListObjectsV2Input, ListObjectsV2Output,
};

pub mod builders {
    use super::{ListObjectsV2Error, ListObjectsV2Output};
    use crate::sim::client::{Client, dispatch_error, protocol_error, service_error};
    use crate::sim::wire::{ErrorCode, Request, Response};
    use aws_sdk_s3::error::SdkError;
    use aws_sdk_s3::types::Object;
    use aws_sdk_s3::types::error::NoSuchBucket;

    pub use aws_sdk_s3::operation::list_objects_v2::builders::ListObjectsV2InputBuilder;

    impl Client {
        /// Starts a simulated `ListObjectsV2`.
        #[must_use]
        pub fn list_objects_v2(&self) -> ListObjectsV2FluentBuilder {
            ListObjectsV2FluentBuilder {
                client: self.clone(),
                inner: ListObjectsV2InputBuilder::default(),
            }
        }
    }

    /// Fluent builder for the simulated `ListObjectsV2`.
    pub struct ListObjectsV2FluentBuilder {
        client: Client,
        inner: ListObjectsV2InputBuilder,
    }

    impl ListObjectsV2FluentBuilder {
        #[must_use]
        pub fn bucket(mut self, input: impl Into<String>) -> Self {
            self.inner = self.inner.bucket(input.into());
            self
        }

        #[must_use]
        pub fn prefix(mut self, input: impl Into<String>) -> Self {
            self.inner = self.inner.prefix(input.into());
            self
        }

        /// Sends the request over the simulated network.
        ///
        /// The simulated listing is complete and key-ordered; pagination is
        /// not modeled, so `is_truncated` is always `false`.
        ///
        /// # Errors
        ///
        /// A missing bucket is the SDK's typed `NoSuchBucket`.
        pub async fn send(self) -> Result<ListObjectsV2Output, SdkError<ListObjectsV2Error>> {
            let input = self.inner.build().map_err(SdkError::construction_failure)?;
            let bucket = input.bucket().unwrap_or_default().to_string();
            let prefix = input.prefix().map(str::to_string);
            let response = self
                .client
                .exchange(&Request::List {
                    bucket: bucket.clone(),
                    prefix: prefix.clone(),
                })
                .await
                .map_err(dispatch_error)?;
            match response {
                Response::ListOk { objects } => {
                    let contents: Vec<Object> = objects
                        .into_iter()
                        .map(|object| {
                            Object::builder()
                                .key(object.key)
                                .size(object.size as i64)
                                .e_tag(object.e_tag)
                                .build()
                        })
                        .collect();
                    Ok(ListObjectsV2Output::builder()
                        .name(bucket)
                        .set_prefix(prefix)
                        .key_count(contents.len() as i32)
                        .is_truncated(false)
                        .set_contents(Some(contents))
                        .build())
                }
                Response::Error { code, message } => Err(match code {
                    ErrorCode::NoSuchBucket => service_error(
                        ListObjectsV2Error::NoSuchBucket(
                            NoSuchBucket::builder().message(message).build(),
                        ),
                        404,
                    ),
                    _ => service_error(ListObjectsV2Error::unhandled(message), 404),
                }),
                _ => Err(protocol_error("ListObjectsV2")),
            }
        }
    }
}
