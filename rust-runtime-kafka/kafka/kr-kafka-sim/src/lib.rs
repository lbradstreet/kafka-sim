//! Reproducible, bounded Kafka producer actor scenarios over the real simulated
//! byte stream provider. Domain history remains separate from runtime EventKind.
#![forbid(unsafe_code)]

pub const MANIFEST_VERSION: u32 = 7;
pub const HISTORY_VERSION: u32 = 5;
pub const SCENARIO_VERSION: u32 = 3;
pub const MODEL_VERSION: u32 = 3;
pub const DRIVER_VERSION: u32 = 5;
mod campaign;
pub mod faults;
pub use campaign::{CampaignVariant, PINNED_CASES, campaign_cases, campaign_manifest};
mod config;
mod manifest;
pub use manifest::*;
mod experiment_link;
pub use experiment_link::{BrokerLink, LinkDirection, LinkOutage, OutageMode};
mod experiment;
pub use experiment::{
    ExperimentWorkload, LoadShape, LoadSpec, PollingPause, ScheduledAction, TimedControl,
};
mod template;
pub use template::{LanePolicy, Partitioning, RecordTemplate, ValuePattern};
mod metrics;
pub use metrics::{
    DistributionSample, MetricScope, MetricsSample, MetricsSampling, MissedMetricsRequest,
    ScopeSample,
};
mod request_observation;
pub use request_observation::DispatchBatch;
mod history;
mod setup;
mod stream;
pub use history::{Coverage, DomainEvent, DomainHistory, HistoryEntry};
pub mod external;
mod probe;
mod runner;
pub use runner::{
    RunFailure, RunReport, TerminalCheckpoint, run, run_and_retain_failure, run_replayed,
    verify_trace_transparency,
};
