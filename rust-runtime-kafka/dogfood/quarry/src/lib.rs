//! A bounded leased work queue used to dogfood `kr-runtime`.
//!
//! Quarry deliberately promises at-least-once processing. Lease tokens fence
//! mutations of queue state, but they do not make arbitrary worker side effects
//! exactly once.

#![forbid(unsafe_code)]

mod broker;
mod durable;
mod engine;
mod mailbox;
mod record;
mod types;

pub use broker::{BrokerJoin, BrokerStartError, QueueClient, start_broker};
pub use durable::{
    DEFAULT_MAX_REPLAY_RECORDS, DEFAULT_RECOVERY_READ_BYTES, DurableQueue, DurableQueueError,
    RecoveryConfig,
};
pub use engine::InMemoryQueue;
pub use types::{
    AckOutcome, CompletedSnapshot, JobId, JobSnapshot, JobStatus, LeaseToken, LeasedJob,
    NackOutcome, QueueConfig, QueueError, QueueSnapshot, RenewOutcome, RequestId, SubmitOutcome,
    SubmitRequest, WorkerId,
};
