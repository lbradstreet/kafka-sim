//! Complete producer configuration encoding for a self-contained replay.
use kr_kafka_producer::{
    config::{BrokerEndpoint, Compression, ProducerConfig, SecurityConfig, TransportPolicy},
    routing::{PartitionerConfig, UnkeyedPolicy},
};
use kr_runtime::RuntimeDuration;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
#[serde(remote = "ProducerConfig", deny_unknown_fields)]
pub(crate) struct ProducerDef {
    #[serde(with = "MetricsDef")]
    metrics: kr_kafka_producer::telemetry::metrics::MetricsConfig,
    #[serde(with = "duration")]
    delivery_timeout: RuntimeDuration,
    #[serde(with = "duration")]
    request_timeout: RuntimeDuration,
    max_in_flight_per_connection: u8,
    connection_wire_window_bytes: u32,
    lanes: u8,
    batch_target_bytes: u32,
    #[serde(default = "legacy_batch_target", with = "BatchTargetDef")]
    batch_target_mode: kr_kafka_producer::config::BatchTargetMode,
    batch_hard_bytes: u32,
    #[serde(with = "duration")]
    linger_max: RuntimeDuration,
    linger_skip_below_rate: Option<u32>,
    request_target_bytes: u32,
    request_hard_bytes: u32,
    request_max_partitions: u16,
    #[serde(default, with = "RequestBatchingDef")]
    request_batching_policy: kr_kafka_producer::config::RequestBatchingPolicy,
    input_bytes: usize,
    record_descriptors: u32,
    #[serde(default, with = "DescriptorAdmissionDef")]
    descriptor_admission_policy: kr_kafka_producer::config::DescriptorAdmissionPolicy,
    compressed_bytes: usize,
    codec_contexts: u8,
    staging_bytes_per_connection: u32,
    rx_bytes_per_connection: u32,
    control_reserve_bytes: usize,
    delivery_event_capacity: u32,
    release_event_capacity: u32,
    mailbox_capacity: u32,
    #[serde(with = "UnkeyedDef")]
    unkeyed_policy: UnkeyedPolicy,
    #[serde(with = "partitioner")]
    partitioner: PartitionerConfig,
    client_id: String,
    #[serde(with = "CompressionDef")]
    compression: Compression,
    #[serde(with = "brokers")]
    bootstrap: Vec<BrokerEndpoint>,
    #[serde(with = "duration")]
    metadata_max_age: RuntimeDuration,
    #[serde(with = "duration")]
    topic_resolve_timeout: RuntimeDuration,
    max_open_topics: u32,
    pending_records_per_topic: u32,
    #[serde(with = "security")]
    security: SecurityConfig,
    #[serde(with = "TransportDef")]
    transport: TransportPolicy,
    brokers_max: u16,
    max_live_leases: u32,
    max_batches: u32,
    worker_jobs: u16,
    codec_window_log: u32,
    codec_workspace_bytes: usize,
    output_chunk_bytes: u32,
    progressive_threshold: u32,
    tls_plaintext_bytes: u32,
    tls_ciphertext_bytes: u32,
    max_header_count: u32,
    max_submissions_per_poll: u32,
    max_submission_records: u32,
    max_completions_per_poll: u32,
    sim_encode_bytes_per_poll: u32,
    target_poll_ms: u32,
    #[serde(with = "duration")]
    retry_backoff_min: RuntimeDuration,
    #[serde(with = "duration")]
    retry_backoff_max: RuntimeDuration,
    max_attempts: u8,
    coalesce_below_bytes: u32,
}
#[derive(Serialize, Deserialize)]
#[serde(
    remote = "kr_kafka_producer::telemetry::metrics::MetricsConfig",
    deny_unknown_fields
)]
struct MetricsDef {
    enabled: bool,
    significant_digits: u8,
    highest_duration_nanos: u64,
    highest_bytes: u64,
    highest_count: u64,
    max_broker_scopes: u16,
    max_partition_scopes: u16,
    max_storage_bytes: usize,
}
#[derive(Serialize, Deserialize)]
#[serde(remote = "Compression")]
enum CompressionDef {
    None,
    Zstd { level: u8 },
}
fn legacy_batch_target() -> kr_kafka_producer::config::BatchTargetMode {
    kr_kafka_producer::config::BatchTargetMode::Raw
}
#[derive(Serialize, Deserialize)]
#[serde(remote = "kr_kafka_producer::config::BatchTargetMode")]
enum BatchTargetDef {
    Raw,
    EstimatedWire,
}
#[derive(Serialize, Deserialize)]
#[serde(remote = "kr_kafka_producer::config::RequestBatchingPolicy")]
enum RequestBatchingDef {
    SinglePartition,
    Sealed,
    BrokerReady,
}
#[derive(Serialize, Deserialize)]
#[serde(remote = "kr_kafka_producer::config::DescriptorAdmissionPolicy")]
enum DescriptorAdmissionDef {
    Shared,
    PartitionPressure,
}
#[derive(Serialize, Deserialize)]
#[serde(remote = "TransportPolicy")]
enum TransportDef {
    Uring,
    Readiness,
    Auto,
}
#[derive(Serialize, Deserialize)]
#[serde(remote = "UnkeyedPolicy")]
enum UnkeyedDef {
    UniformBytes { run_bytes: u32 },
    Adaptive { run_bytes: u32 },
}
mod duration {
    use super::*;
    pub(super) fn serialize<S: serde::Serializer>(
        value: &RuntimeDuration,
        s: S,
    ) -> Result<S::Ok, S::Error> {
        value.as_nanos().serialize(s)
    }
    pub(super) fn deserialize<'de, D: serde::Deserializer<'de>>(
        d: D,
    ) -> Result<RuntimeDuration, D::Error> {
        Ok(RuntimeDuration::from_nanos(u64::deserialize(d)?))
    }
}
mod brokers {
    use super::*;
    pub(super) fn serialize<S: serde::Serializer>(
        value: &[BrokerEndpoint],
        s: S,
    ) -> Result<S::Ok, S::Error> {
        value
            .iter()
            .map(|node| (&node.host, node.port))
            .collect::<Vec<_>>()
            .serialize(s)
    }
    pub(super) fn deserialize<'de, D: serde::Deserializer<'de>>(
        d: D,
    ) -> Result<Vec<BrokerEndpoint>, D::Error> {
        Ok(Vec::<(String, u16)>::deserialize(d)?
            .into_iter()
            .map(|(host, port)| BrokerEndpoint { host, port })
            .collect())
    }
}
mod security {
    use super::*;
    pub(super) fn serialize<S: serde::Serializer>(
        value: &SecurityConfig,
        s: S,
    ) -> Result<S::Ok, S::Error> {
        if !matches!(value, SecurityConfig::Plaintext) {
            return Err(serde::ser::Error::custom(
                "simulation manifests cannot contain host TLS credentials",
            ));
        }
        "Plaintext".serialize(s)
    }
    pub(super) fn deserialize<'de, D: serde::Deserializer<'de>>(
        d: D,
    ) -> Result<SecurityConfig, D::Error> {
        if String::deserialize(d)? != "Plaintext" {
            return Err(serde::de::Error::custom(
                "simulation requires plaintext modeled streams",
            ));
        }
        Ok(SecurityConfig::Plaintext)
    }
}
mod partitioner {
    use super::*;
    pub(super) fn serialize<S: serde::Serializer>(
        value: &PartitionerConfig,
        s: S,
    ) -> Result<S::Ok, S::Error> {
        match value {
            PartitionerConfig::Builtin => "Builtin".serialize(s),
            _ => Err(serde::ser::Error::custom(
                "simulation requires recorded builtin routing",
            )),
        }
    }
    pub(super) fn deserialize<'de, D: serde::Deserializer<'de>>(
        d: D,
    ) -> Result<PartitionerConfig, D::Error> {
        if String::deserialize(d)? != "Builtin" {
            return Err(serde::de::Error::custom(
                "simulation requires builtin routing",
            ));
        }
        Ok(PartitionerConfig::Builtin)
    }
}
