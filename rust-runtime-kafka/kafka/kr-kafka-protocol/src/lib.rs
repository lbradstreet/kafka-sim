//! Kafka wire codecs compiled offline from the repository's pinned JSON schemas.
#![doc = include_str!("../README.md")]
#![no_std]
#![forbid(unsafe_code)]

extern crate alloc;

pub mod errors;
pub mod frame;
pub mod generated;
pub mod plan;
pub mod registry;
pub mod wire;

pub use generated::*;
pub use registry::{Request, Response, SUPPORTED, api_version};
