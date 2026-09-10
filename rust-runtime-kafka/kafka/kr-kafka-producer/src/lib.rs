//! Bounded Kafka producer state, admission, and lifecycle contracts.
#![forbid(unsafe_code)]

pub mod accumulator;
pub mod actor;
pub mod admission;
mod batching;
pub mod client;
pub mod config;
pub mod credit;
pub mod engine;
pub mod estimation;
mod fixed;
pub mod input;
pub mod lifecycle;
pub mod mailbox;
pub mod pool;
pub mod request_policy;
pub mod routing;
pub mod telemetry;
pub mod topic;
pub mod transport;
pub mod types;

pub mod sequence;

pub mod connector;
pub mod control;
