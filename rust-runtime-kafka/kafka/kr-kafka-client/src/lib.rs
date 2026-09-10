//! Shared Kafka control/stateless Fetch codecs, passive transport and connection contracts.
//! Workload admission, delivery, idempotency and routing policies live above
//! this crate; native providers and TLS/SASL implementations live at its edge.
#![forbid(unsafe_code)]

pub mod config;
pub mod connector;
pub mod control;
pub mod fetch;
pub mod telemetry;
pub mod transport;
pub mod types;
