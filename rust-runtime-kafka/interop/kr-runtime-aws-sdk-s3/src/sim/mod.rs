//! The simulated aws-sdk-s3 surface, active under `--cfg kr_runtime_sim`.

pub mod client;
pub mod config;
mod etag;
pub mod operation;
pub mod server;
mod wire;

pub use client::Client;
pub use config::Config;

// Types, primitives, and error vocabulary are the real SDK's, so simulated
// results are indistinguishable in shape from production results.
pub use aws_sdk_s3::{error, primitives, types};
