//! Fixed C layouts. No Rust enum, Option, Vec or String crosses this boundary.
use std::mem::size_of;
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct KrSpan {
    pub ptr: *const u8,
    pub len: u32,
}
impl Default for KrSpan {
    fn default() -> Self {
        Self {
            ptr: std::ptr::null(),
            len: 0,
        }
    }
}
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct KrBroker {
    pub struct_size: u32,
    pub host: KrSpan,
    pub port: u32,
}
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct KrHeader {
    pub struct_size: u32,
    pub key: KrSpan,
    pub value: KrSpan,
    pub value_is_null: u32,
}
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct KrRecord {
    pub struct_size: u32,
    pub topic: u32,
    pub partition_hint: i32,
    pub lane_hint: i32,
    pub key: KrSpan,
    pub key_is_null: u32,
    pub value: KrSpan,
    pub value_is_null: u32,
    pub headers: *const KrHeader,
    pub header_count: u32,
    pub timestamp_ms: i64,
    pub user_token: u64,
    pub delivery_timeout_ns: u64,
}
#[repr(C)]
#[derive(Clone, Copy, Debug, Default)]
pub struct KrEvent {
    pub struct_size: u32,
    pub kind: u32,
    pub token: u64,
    pub user_token: u64,
    pub topic: u32,
    pub topic_id: [u8; 16],
    pub partition: i32,
    pub outcome: u32,
    pub reason: u32,
    pub base_offset: i64,
    pub base_offset_present: u32,
    pub timestamp_ms: i64,
    pub timestamp_present: u32,
    pub attempts: u32,
    pub count: u32,
}
impl KrEvent {
    pub fn empty() -> Self {
        Self {
            struct_size: size_of::<Self>() as u32,
            ..Default::default()
        }
    }
}
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct KrProducerConfig {
    pub struct_size: u32,
    pub delivery_timeout_ns: u64,
    pub request_timeout_ns: u64,
    pub linger_max_ns: u64,
    pub metadata_max_age_ns: u64,
    pub topic_resolve_timeout_ns: u64,
    pub retry_backoff_min_ns: u64,
    pub retry_backoff_max_ns: u64,
    pub input_bytes: u64,
    pub compressed_bytes: u64,
    pub control_reserve_bytes: u64,
    pub codec_workspace_bytes: u64,
    pub max_in_flight_per_connection: u32,
    pub lanes: u32,
    pub codec_contexts: u32,
    pub max_attempts: u32,
    pub request_max_partitions: u32,
    pub brokers_max: u32,
    pub worker_jobs: u32,
    pub connection_wire_window_bytes: u32,
    pub batch_target_bytes: u32,
    pub batch_hard_bytes: u32,
    pub request_target_bytes: u32,
    pub request_hard_bytes: u32,
    pub record_descriptors: u32,
    pub staging_bytes_per_connection: u32,
    pub rx_bytes_per_connection: u32,
    pub delivery_event_capacity: u32,
    pub release_event_capacity: u32,
    pub mailbox_capacity: u32,
    pub max_open_topics: u32,
    pub pending_records_per_topic: u32,
    pub max_live_leases: u32,
    pub max_batches: u32,
    pub codec_window_log: u32,
    pub output_chunk_bytes: u32,
    pub progressive_threshold: u32,
    pub tls_plaintext_bytes: u32,
    pub tls_ciphertext_bytes: u32,
    pub max_header_count: u32,
    pub max_submissions_per_poll: u32,
    pub max_submission_records: u32,
    pub max_completions_per_poll: u32,
    pub sim_encode_bytes_per_poll: u32,
    pub target_poll_ms: u32,
    pub coalesce_below_bytes: u32,
    pub linger_skip_below_rate: u32,
    pub unkeyed_policy: u32,
    pub unkeyed_run_bytes: u32,
    pub partitioner: u32,
    pub compression: u32,
    pub compression_level: u32,
    pub transport: u32,
    pub security: u32,
    pub sasl_mechanism: u32,
    pub tls_system_roots: u32,
    pub client_id: KrSpan,
    pub bootstrap: *const KrBroker,
    pub bootstrap_count: u32,
    pub tls_roots: *const KrSpan,
    pub tls_root_count: u32,
    pub tls_server_name: KrSpan,
    pub username: KrSpan,
    pub password: KrSpan,
    /// 0 = estimated wire bytes (default), 1 = legacy raw bytes.
    pub batch_target_mode: u32,
    /// 0 = sealed (default), 1 = single partition, 2 = broker ready.
    pub request_batching_policy: u32,
    /// Must be zero. Keeps ABI 4 size distinct from ABI 3 on every target.
    pub reserved_request_policy: u32,
}
impl Default for KrProducerConfig {
    fn default() -> Self {
        let config = kr_kafka_producer::config::ProducerConfig::default();
        Self {
            struct_size: size_of::<Self>() as u32,
            delivery_timeout_ns: config.delivery_timeout.as_nanos(),
            request_timeout_ns: config.request_timeout.as_nanos(),
            linger_max_ns: config.linger_max.as_nanos(),
            metadata_max_age_ns: config.metadata_max_age.as_nanos(),
            topic_resolve_timeout_ns: config.topic_resolve_timeout.as_nanos(),
            retry_backoff_min_ns: config.retry_backoff_min.as_nanos(),
            retry_backoff_max_ns: config.retry_backoff_max.as_nanos(),
            input_bytes: config.input_bytes as u64,
            compressed_bytes: config.compressed_bytes as u64,
            control_reserve_bytes: config.control_reserve_bytes as u64,
            codec_workspace_bytes: config.codec_workspace_bytes as u64,
            max_in_flight_per_connection: config.max_in_flight_per_connection as u32,
            lanes: config.lanes as u32,
            codec_contexts: config.codec_contexts as u32,
            max_attempts: config.max_attempts as u32,
            request_max_partitions: config.request_max_partitions as u32,
            brokers_max: config.brokers_max as u32,
            worker_jobs: config.worker_jobs as u32,
            connection_wire_window_bytes: config.connection_wire_window_bytes,
            batch_target_bytes: config.batch_target_bytes,
            batch_target_mode: 0,
            request_batching_policy: 0,
            reserved_request_policy: 0,
            batch_hard_bytes: config.batch_hard_bytes,
            request_target_bytes: config.request_target_bytes,
            request_hard_bytes: config.request_hard_bytes,
            record_descriptors: config.record_descriptors,
            staging_bytes_per_connection: config.staging_bytes_per_connection,
            rx_bytes_per_connection: config.rx_bytes_per_connection,
            delivery_event_capacity: config.delivery_event_capacity,
            release_event_capacity: config.release_event_capacity,
            mailbox_capacity: config.mailbox_capacity,
            max_open_topics: config.max_open_topics,
            pending_records_per_topic: config.pending_records_per_topic,
            max_live_leases: config.max_live_leases,
            max_batches: config.max_batches,
            codec_window_log: config.codec_window_log,
            output_chunk_bytes: config.output_chunk_bytes,
            progressive_threshold: config.progressive_threshold,
            tls_plaintext_bytes: config.tls_plaintext_bytes,
            tls_ciphertext_bytes: config.tls_ciphertext_bytes,
            max_header_count: config.max_header_count,
            max_submissions_per_poll: config.max_submissions_per_poll,
            max_submission_records: config.max_submission_records,
            max_completions_per_poll: config.max_completions_per_poll,
            sim_encode_bytes_per_poll: config.sim_encode_bytes_per_poll,
            target_poll_ms: config.target_poll_ms,
            coalesce_below_bytes: config.coalesce_below_bytes,
            linger_skip_below_rate: config.linger_skip_below_rate.unwrap_or(0),
            unkeyed_policy: 0,
            unkeyed_run_bytes: config.unkeyed_policy.run_bytes(),
            partitioner: 0,
            compression: 1,
            compression_level: 1,
            transport: 2,
            security: 0,
            sasl_mechanism: 0,
            tls_system_roots: 0,
            client_id: KrSpan::default(),
            bootstrap: std::ptr::null(),
            bootstrap_count: 0,
            tls_roots: std::ptr::null(),
            tls_root_count: 0,
            tls_server_name: KrSpan::default(),
            username: KrSpan::default(),
            password: KrSpan::default(),
        }
    }
}

#[repr(C)]
#[derive(Clone, Copy, Debug, Default)]
pub struct KrTopicStatus {
    pub struct_size: u32,
    pub status: u32,
    pub generation: u64,
    pub topic_id: [u8; 16],
    pub partition_count: u32,
    pub reason: u32,
}
#[repr(C)]
#[derive(Clone, Copy, Debug, Default)]
pub struct KrMetadataSnapshot {
    pub struct_size: u32,
    pub status: u32,
    pub generation: u64,
    pub topic_id: [u8; 16],
    pub partition_count: u32,
    pub reason: u32,
    pub snapshot: u64,
    pub broker_count: u32,
}
#[repr(C)]
#[derive(Clone, Copy, Debug, Default)]
pub struct KrMetadataBroker {
    pub struct_size: u32,
    pub id: i32,
    pub port: u32,
    pub host_len: u32,
    pub rack_len: u32,
    pub rack_present: u32,
}
#[repr(C)]
#[derive(Clone, Copy, Debug, Default)]
pub struct KrMetadataPartition {
    pub struct_size: u32,
    pub partition: i32,
    pub leader: i32,
    pub leader_epoch: i32,
    pub error_code: i32,
    pub replica_count: u32,
    pub isr_count: u32,
    pub offline_count: u32,
}
