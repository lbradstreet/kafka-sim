//! Fluent operation builders mirroring `aws_sdk_s3::operation`.
//!
//! Each module re-exports the real SDK's input, output, and error types and
//! defines a fluent builder whose `send` runs over the simulated transport.

pub mod delete_object;
pub mod get_object;
pub mod head_object;
pub mod list_objects_v2;
pub mod put_object;
