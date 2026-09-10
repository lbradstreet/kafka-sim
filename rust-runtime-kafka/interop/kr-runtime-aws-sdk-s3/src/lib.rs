//! An aws-sdk-s3-shaped facade with a deterministic simulated S3 under kr-runtime.
//!
//! Without `--cfg kr_runtime_sim` this crate is a transparent re-export of
//! `aws_sdk_s3`. With it, the [`Client`] operation surface is reimplemented
//! over the kr-runtime-tokio simulated network: requests and object bodies cross
//! [`SimNetwork`](kr_runtime_io) byte streams as real bytes — so link latency,
//! partitions, and partial I/O apply to the actual payload path — and are
//! served by an in-simulation [`server::S3Service`] model.
//!
//! The shim reuses the real SDK's input builders, output types, and typed
//! errors (`NoSuchKey`, `NotFound`, …), so application code written against
//! `aws_sdk_s3`'s API compiles unchanged and observes SDK-shaped results.
//! This is the madsim-aws-sdk-s3 approach: the *client API* is simulated,
//! not the SDK's internal retry/orchestration code.

#[cfg(not(kr_runtime_sim))]
pub use aws_sdk_s3::*;

#[cfg(kr_runtime_sim)]
mod sim;
#[cfg(kr_runtime_sim)]
pub use sim::*;
