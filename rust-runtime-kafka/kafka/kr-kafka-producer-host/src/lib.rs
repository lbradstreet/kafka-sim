//! Dedicated native Kafka producer ownership and startup calibration.
//! Reusable transport and security live in `kr-kafka-host`.
#![forbid(unsafe_code)]

pub mod calibration;
pub mod producer;
pub mod setup;
