//! Producer configuration validated before admission or I/O can begin.
use crate::{
    credit::Resource,
    routing::{PartitionerConfig, UnkeyedPolicy},
};
use kr_runtime::RuntimeDuration;
use std::fmt;

pub use crate::request_policy::RequestBatchingPolicy;

mod memory;
pub use memory::{
    FixedMetadataBudget, IncompleteMemoryAccounting, MemoryBudgetReport, MemoryScopeExclusion,
    UnaccountedMemory,
};

pub use kr_kafka_client::config::{
    BrokerEndpoint, SaslMechanism, Secret, SecurityConfig, TlsConfig, TransportPolicy,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Compression {
    None,
    Zstd { level: u8 },
}

/// Units and readiness behavior of the soft batch target. Hard raw/output
/// envelopes and delivery deadlines remain independent of this estimate.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum BatchTargetMode {
    /// Legacy raw record bytes, excluding the batch header; seals immediately.
    Raw,
    /// Estimated finished wire bytes, including the 61-byte batch header.
    /// A reached target seals when dispatch credit is available. Until then
    /// records may accumulate within the hard limit.
    #[default]
    EstimatedWire,
}

/// Optional descriptor isolation. Other resource pools retain their own limits.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum DescriptorAdmissionPolicy {
    /// Existing global and lane credit limits.
    #[default]
    Shared,
    /// Share the first 75% freely, then favor destinations with small backlogs.
    /// For capacity C, total held H and destination held P, one record requires
    /// H < floor(3*C/4) or P + 1 <= C - H, as well as all ordinary credits.
    /// Idle destinations reserve nothing. This is not a starvation guarantee.
    PartitionPressure,
}

#[derive(Clone, Debug)]
pub struct ProducerConfig {
    pub metrics: crate::telemetry::metrics::MetricsConfig,
    pub delivery_timeout: RuntimeDuration,
    pub request_timeout: RuntimeDuration,
    pub max_in_flight_per_connection: u8,
    pub connection_wire_window_bytes: u32,
    pub lanes: u8,
    pub batch_target_bytes: u32,
    pub batch_target_mode: BatchTargetMode,
    pub batch_hard_bytes: u32,
    pub linger_max: RuntimeDuration,
    pub linger_skip_below_rate: Option<u32>,
    pub request_target_bytes: u32,
    pub request_hard_bytes: u32,
    pub request_max_partitions: u16,
    pub request_batching_policy: RequestBatchingPolicy,
    pub input_bytes: usize,
    pub record_descriptors: u32,
    pub descriptor_admission_policy: DescriptorAdmissionPolicy,
    pub compressed_bytes: usize,
    pub codec_contexts: u8,
    pub staging_bytes_per_connection: u32,
    pub rx_bytes_per_connection: u32,
    pub control_reserve_bytes: usize,
    pub delivery_event_capacity: u32,
    pub release_event_capacity: u32,
    pub mailbox_capacity: u32,
    pub unkeyed_policy: UnkeyedPolicy,
    pub partitioner: PartitionerConfig,
    pub client_id: String,
    pub compression: Compression,
    pub bootstrap: Vec<BrokerEndpoint>,
    pub metadata_max_age: RuntimeDuration,
    pub topic_resolve_timeout: RuntimeDuration,
    pub max_open_topics: u32,
    pub pending_records_per_topic: u32,
    pub security: SecurityConfig,
    pub transport: TransportPolicy,
    // Bounds for the named design quantities omitted from its abbreviated struct.
    pub brokers_max: u16,
    pub max_live_leases: u32,
    pub max_batches: u32,
    pub worker_jobs: u16,
    pub codec_window_log: u32,
    pub codec_workspace_bytes: usize,
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
    pub retry_backoff_min: RuntimeDuration,
    pub retry_backoff_max: RuntimeDuration,
    pub max_attempts: u8,
    pub coalesce_below_bytes: u32,
}
impl Default for ProducerConfig {
    fn default() -> Self {
        Self {
            metrics: crate::telemetry::metrics::MetricsConfig::default(),
            delivery_timeout: RuntimeDuration::from_nanos(120_000_000_000),
            request_timeout: RuntimeDuration::from_nanos(30_000_000_000),
            max_in_flight_per_connection: 5,
            connection_wire_window_bytes: 4 * 1024 * 1024,
            lanes: 1,
            batch_target_bytes: 128 * 1024,
            batch_target_mode: BatchTargetMode::default(),
            batch_hard_bytes: 1024 * 1024,
            linger_max: RuntimeDuration::from_nanos(500_000),
            linger_skip_below_rate: Some(200),
            request_target_bytes: 512 * 1024,
            request_hard_bytes: 1024 * 1024,
            request_max_partitions: 64,
            request_batching_policy: RequestBatchingPolicy::default(),
            input_bytes: 64 * 1024 * 1024,
            record_descriptors: 1_000_000,
            descriptor_admission_policy: DescriptorAdmissionPolicy::Shared,
            compressed_bytes: 64 * 1024 * 1024,
            codec_contexts: 4,
            staging_bytes_per_connection: 256 * 1024,
            rx_bytes_per_connection: 1024 * 1024,
            control_reserve_bytes: 2 * 1024 * 1024,
            delivery_event_capacity: 1_000_000,
            release_event_capacity: 65_536,
            mailbox_capacity: 1024,
            unkeyed_policy: UnkeyedPolicy::default(),
            partitioner: PartitionerConfig::Builtin,
            client_id: "kr-kafka".into(),
            compression: Compression::Zstd { level: 1 },
            bootstrap: vec![BrokerEndpoint {
                host: "localhost".into(),
                port: 9092,
            }],
            metadata_max_age: RuntimeDuration::from_nanos(300_000_000_000),
            topic_resolve_timeout: RuntimeDuration::from_nanos(60_000_000_000),
            max_open_topics: 1024,
            pending_records_per_topic: 65_536,
            security: SecurityConfig::Plaintext,
            transport: TransportPolicy::Auto,
            brokers_max: 64,
            max_live_leases: 65_536,
            max_batches: 65_536,
            worker_jobs: 8,
            codec_window_log: 20,
            codec_workspace_bytes: 8 * 1024 * 1024,
            output_chunk_bytes: 512 * 1024,
            progressive_threshold: 16 * 1024,
            tls_plaintext_bytes: 32 * 1024,
            tls_ciphertext_bytes: 64 * 1024,
            max_header_count: 1024,
            max_submissions_per_poll: 64,
            max_submission_records: 4096,
            max_completions_per_poll: 128,
            sim_encode_bytes_per_poll: 64 * 1024,
            target_poll_ms: 2,
            retry_backoff_min: RuntimeDuration::from_nanos(10_000_000),
            retry_backoff_max: RuntimeDuration::from_nanos(1_000_000_000),
            max_attempts: u8::MAX,
            coalesce_below_bytes: 512,
        }
    }
}
#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum ConfigError {
    Invalid {
        field: &'static str,
        reason: &'static str,
    },
    Overflow {
        field: &'static str,
    },
}
impl fmt::Display for ConfigError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Invalid { field, reason } => write!(f, "invalid {field}: {reason}"),
            Self::Overflow { field } => write!(f, "{field} cannot be represented"),
        }
    }
}
impl std::error::Error for ConfigError {}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ValidatedConfig {
    pub max_connections: usize,
    pub effective_batch_payload_bytes: u32,
    pub max_send_segments: usize,
    /// Per-request shared metadata arena plus bounded small-span coalescing.
    pub request_metadata_bytes: usize,
    pub credits: [usize; Resource::COUNT],
    pub memory: MemoryBudgetReport,
}
fn invalid(field: &'static str, reason: &'static str) -> ConfigError {
    ConfigError::Invalid { field, reason }
}
fn add(a: usize, b: usize, field: &'static str) -> Result<usize, ConfigError> {
    a.checked_add(b).ok_or(ConfigError::Overflow { field })
}
fn mul(a: usize, b: usize, field: &'static str) -> Result<usize, ConfigError> {
    a.checked_mul(b).ok_or(ConfigError::Overflow { field })
}
impl ProducerConfig {
    /// Projects validated producer limits onto the request-independent host
    /// connection boundary. Producer-only policy stays in this configuration.
    /// # Errors
    /// Preserves the original field-specific validation failure before cloning
    /// endpoint/security state or constructing any host resource.
    pub fn connection_config(
        &self,
    ) -> Result<kr_kafka_client::config::ConnectionConfig, ConfigError> {
        let validated = self.validate()?;
        Ok(kr_kafka_client::config::ConnectionConfig {
            client_id: self.client_id.clone(),
            max_connections: validated.max_connections,
            max_operation_bytes: self
                .request_hard_bytes
                .max(self.staging_bytes_per_connection)
                .max(self.rx_bytes_per_connection) as usize,
            rx_bytes_per_connection: self.rx_bytes_per_connection as usize,
            staging_bytes_per_connection: self.staging_bytes_per_connection as usize,
            control_jobs: usize::from(self.worker_jobs).min(8),
            control_bytes: self.control_reserve_bytes,
            connect_timeout: self.request_timeout,
            tls_plaintext_bytes: self.tls_plaintext_bytes as usize,
            tls_ciphertext_bytes: self.tls_ciphertext_bytes as usize,
            security: self.security.clone(),
            transport: self.transport,
        })
    }

    /// Validates numeric bounds and relationships before creating any resource.
    ///
    /// Actual cluster response lengths and measured codec workspace are checked
    /// again at startup. Data batches reserve the worst supported single-topic
    /// request envelope, so equal configured batch/request caps cannot create an
    /// undispatchable batch. The returned effective payload cap is observable.
    ///
    /// # Errors
    /// Returns a field-specific error or checked-arithmetic overflow.
    pub fn validate(&self) -> Result<ValidatedConfig, ConfigError> {
        if self.descriptor_admission_policy == DescriptorAdmissionPolicy::PartitionPressure
            && self.record_descriptors < 4
        {
            return Err(ConfigError::Invalid {
                field: "record_descriptors",
                reason: "partition pressure requires at least four descriptors",
            });
        }
        if !(1..=4).contains(&self.lanes) {
            return Err(invalid("lanes", "must be in 1..=4"));
        }
        if !(1..=5).contains(&self.max_in_flight_per_connection) {
            return Err(invalid("max_in_flight_per_connection", "must be in 1..=5"));
        }
        for (field, value) in [
            (
                "connection_wire_window_bytes",
                self.connection_wire_window_bytes as usize,
            ),
            ("batch_target_bytes", self.batch_target_bytes as usize),
            ("batch_hard_bytes", self.batch_hard_bytes as usize),
            ("request_target_bytes", self.request_target_bytes as usize),
            ("request_hard_bytes", self.request_hard_bytes as usize),
            (
                "request_max_partitions",
                self.request_max_partitions as usize,
            ),
            ("input_bytes", self.input_bytes),
            ("record_descriptors", self.record_descriptors as usize),
            ("compressed_bytes", self.compressed_bytes),
            (
                "staging_bytes_per_connection",
                self.staging_bytes_per_connection as usize,
            ),
            (
                "rx_bytes_per_connection",
                self.rx_bytes_per_connection as usize,
            ),
            ("control_reserve_bytes", self.control_reserve_bytes),
            ("mailbox_capacity", self.mailbox_capacity as usize),
            ("brokers_max", self.brokers_max as usize),
            ("max_open_topics", self.max_open_topics as usize),
            (
                "pending_records_per_topic",
                self.pending_records_per_topic as usize,
            ),
            ("max_live_leases", self.max_live_leases as usize),
            ("max_batches", self.max_batches as usize),
            ("worker_jobs", self.worker_jobs as usize),
            ("output_chunk_bytes", self.output_chunk_bytes as usize),
            (
                "max_submissions_per_poll",
                self.max_submissions_per_poll as usize,
            ),
            (
                "max_submission_records",
                self.max_submission_records as usize,
            ),
            (
                "max_completions_per_poll",
                self.max_completions_per_poll as usize,
            ),
            (
                "sim_encode_bytes_per_poll",
                self.sim_encode_bytes_per_poll as usize,
            ),
            ("target_poll_ms", self.target_poll_ms as usize),
            ("max_attempts", self.max_attempts as usize),
            (
                "unkeyed_policy.run_bytes",
                self.unkeyed_policy.run_bytes() as usize,
            ),
        ] {
            if value == 0 {
                return Err(invalid(field, "must be nonzero"));
            }
        }
        if self.batch_target_bytes > self.batch_hard_bytes
            || self.batch_hard_bytes > self.request_hard_bytes
        {
            return Err(invalid(
                "batch_hard_bytes",
                "requires batch_target <= batch_hard <= request_hard",
            ));
        }
        if self.request_target_bytes > self.request_hard_bytes {
            return Err(invalid(
                "request_target_bytes",
                "exceeds request_hard_bytes",
            ));
        }
        if self.request_hard_bytes > i32::MAX as u32
            || self.rx_bytes_per_connection > i32::MAX as u32
        {
            return Err(invalid(
                "request_hard_bytes",
                "Kafka frames use signed i32 lengths",
            ));
        }
        if self.delivery_event_capacity < self.record_descriptors {
            return Err(invalid(
                "delivery_event_capacity",
                "must cover every record descriptor",
            ));
        }
        if self.release_event_capacity < self.max_live_leases {
            return Err(invalid(
                "release_event_capacity",
                "must cover every live lease",
            ));
        }
        if self.pending_records_per_topic > self.record_descriptors {
            return Err(invalid(
                "pending_records_per_topic",
                "exceeds record_descriptors",
            ));
        }
        if self.client_id.len() > i16::MAX as usize {
            return Err(invalid(
                "client_id",
                "classic header string exceeds i16 length",
            ));
        }
        if self.bootstrap.is_empty() || self.bootstrap.len() > usize::from(self.brokers_max) {
            return Err(invalid("bootstrap", "requires 1..=brokers_max endpoints"));
        }
        for endpoint in &self.bootstrap {
            if endpoint.host.is_empty()
                || endpoint.host.len() > 253
                || endpoint.host.as_bytes().contains(&0)
                || endpoint.port == 0
            {
                return Err(invalid("bootstrap", "invalid host or port"));
            }
        }
        if let Compression::Zstd { level } = self.compression
            && (!(1..=3).contains(&level) || self.codec_contexts == 0)
        {
            return Err(invalid(
                "compression",
                "zstd requires level 1..=3 and a context",
            ));
        }
        if !(10..=23).contains(&self.codec_window_log) {
            return Err(invalid("codec_window_log", "must be in 10..=23"));
        }
        if self.codec_workspace_bytes == 0 && self.codec_contexts > 0 {
            return Err(invalid("codec_workspace_bytes", "must cover live contexts"));
        }
        if self.progressive_threshold > self.batch_hard_bytes {
            return Err(invalid("progressive_threshold", "exceeds batch_hard_bytes"));
        }
        for (field, value) in [
            ("delivery_timeout", self.delivery_timeout),
            ("request_timeout", self.request_timeout),
            ("metadata_max_age", self.metadata_max_age),
            ("topic_resolve_timeout", self.topic_resolve_timeout),
        ] {
            if value == RuntimeDuration::ZERO {
                return Err(invalid(field, "must be nonzero"));
            }
        }
        if self.request_timeout.as_nanos() / 1_000_000 > i32::MAX as u64 {
            return Err(invalid("request_timeout", "exceeds Kafka timeout field"));
        }
        if self.request_timeout > self.delivery_timeout {
            return Err(invalid(
                "request_timeout",
                "must not exceed delivery_timeout",
            ));
        }
        if self.retry_backoff_min > self.retry_backoff_max {
            return Err(invalid("retry_backoff_min", "exceeds retry_backoff_max"));
        }
        if self.linger_skip_below_rate == Some(0) {
            return Err(invalid("linger_skip_below_rate", "use None to disable"));
        }
        let tls = match &self.security {
            SecurityConfig::Plaintext => None,
            SecurityConfig::Tls { tls } => Some(tls),
            SecurityConfig::SaslTls {
                tls,
                username,
                password,
                ..
            } => {
                if username.is_empty()
                    || username.contains('\0')
                    || password.expose().contains('\0')
                {
                    return Err(invalid(
                        "security.credentials",
                        "empty username or NUL in credentials",
                    ));
                }
                Some(tls)
            }
        };
        if let Some(tls) = tls {
            if !tls.use_system_roots && tls.roots_der.is_empty() {
                return Err(invalid(
                    "security.tls",
                    "requires explicit roots or system roots",
                ));
            }
            if tls
                .server_name
                .as_ref()
                .is_some_and(|n| n.is_empty() || n.len() > 253 || n.contains('\0'))
            {
                return Err(invalid("security.server_name", "invalid TLS name"));
            }
            if self.tls_plaintext_bytes < 16 * 1024 || self.tls_ciphertext_bytes < 18 * 1024 {
                return Err(invalid(
                    "tls_ciphertext_bytes",
                    "TLS buffers must hold a maximum TLS record",
                ));
            }
        }
        let max_connections = add(
            mul(
                usize::from(self.lanes),
                usize::from(self.brokers_max),
                "max_connections",
            )?,
            2,
            "max_connections",
        )?;
        // Conservative Produce13 bound covers the client ID, UUID, partition
        // metadata and compact prefixes, leaving room for bounded tags.
        let request_envelope = add(self.client_id.len(), 320, "request_envelope")?;
        let payload = (self.request_hard_bytes as usize)
            .checked_sub(add(request_envelope, 61, "request_envelope")?)
            .ok_or_else(|| {
                invalid(
                    "request_hard_bytes",
                    "cannot hold one batch plus its request header",
                )
            })?;
        let payload = payload.min(self.batch_hard_bytes as usize) as u32;
        if payload < self.batch_target_bytes {
            return Err(invalid(
                "batch_target_bytes",
                "exceeds effective request-framed payload cap",
            ));
        }
        if self.output_chunk_bytes < 61 || self.output_chunk_bytes > payload + 61 {
            return Err(invalid(
                "output_chunk_bytes",
                "must be in 61..=batch envelope",
            ));
        }
        // The finalized 61-byte header is its own chunk and is promoted into
        // request metadata. At most two immutable payload chunks then preserve
        // the request plan's three-segments-per-partition envelope.
        if 1 + payload.div_ceil(self.output_chunk_bytes) > 3 {
            return Err(invalid(
                "output_chunk_bytes",
                "batch header plus payload may occupy at most three chunks",
            ));
        }
        if self.compressed_bytes < (payload as usize + 61) {
            return Err(invalid(
                "compressed_bytes",
                "must activate at least one legal batch",
            ));
        }
        let codec_workspace = mul(
            self.codec_contexts as usize,
            self.codec_workspace_bytes,
            "codec_workspace",
        )?;
        let per_connection = add(
            self.staging_bytes_per_connection as usize,
            self.rx_bytes_per_connection as usize,
            "transport",
        )?;
        let per_connection = if tls.is_some() {
            add(
                per_connection,
                add(
                    self.tls_plaintext_bytes as usize,
                    self.tls_ciphertext_bytes as usize,
                    "TLS buffers",
                )?,
                "transport",
            )?
        } else {
            per_connection
        };
        let transport = mul(max_connections, per_connection, "transport")?;
        let raw_segments = add(
            mul(self.request_max_partitions as usize, 4, "request metadata")?,
            4,
            "request metadata",
        )?;
        let arena = add(
            add(self.client_id.len(), 64, "request metadata")?,
            // One selected batch per partition: protocol fields plus the
            // finalized 61-byte header copied into the request metadata arena.
            mul(
                self.request_max_partitions as usize,
                32 + 61,
                "request metadata",
            )?,
            "request metadata",
        )?
        .min(self.request_hard_bytes as usize);
        let coalesced = mul(
            raw_segments,
            self.coalesce_below_bytes.saturating_sub(1) as usize,
            "request coalescing",
        )?
        .min(self.request_hard_bytes as usize);
        let request_metadata_bytes = add(arena, coalesced, "request metadata")?;
        let tls_bytes = if tls.is_some() {
            mul(
                max_connections,
                add(
                    self.tls_plaintext_bytes as usize,
                    self.tls_ciphertext_bytes as usize,
                    "TLS buffers",
                )?,
                "TLS buffers",
            )?
        } else {
            0
        };
        let request_metadata = mul(
            mul(
                max_connections,
                self.max_in_flight_per_connection as usize,
                "request metadata",
            )?,
            request_metadata_bytes,
            "request metadata",
        )?;
        let total = [
            self.input_bytes,
            self.compressed_bytes,
            codec_workspace,
            transport,
            self.control_reserve_bytes,
            request_metadata,
        ]
        .into_iter()
        .try_fold(0, |a, b| add(a, b, "configured byte pools"))?;
        let mut credits = [0; Resource::COUNT];
        for (resource, limit) in [
            (Resource::TlsBytes, tls_bytes),
            (Resource::Mailbox, self.mailbox_capacity as usize),
            (Resource::Descriptors, self.record_descriptors as usize),
            (Resource::InputBytes, self.input_bytes),
            (
                Resource::ReleaseEvents,
                self.release_event_capacity as usize,
            ),
            (
                Resource::DeliveryEvents,
                self.delivery_event_capacity as usize,
            ),
            (Resource::CodecContexts, self.codec_contexts as usize),
            (Resource::CompressedBytes, self.compressed_bytes),
            (
                Resource::StagingBytes,
                mul(
                    max_connections,
                    self.staging_bytes_per_connection as usize,
                    "staging_bytes",
                )?,
            ),
            (
                Resource::RequestSlots,
                mul(
                    max_connections,
                    self.max_in_flight_per_connection as usize,
                    "request_slots",
                )?,
            ),
            // One legal request can exceed each connection's soft byte window.
            (
                Resource::WireWindow,
                mul(
                    max_connections,
                    add(
                        self.connection_wire_window_bytes as usize,
                        self.request_hard_bytes as usize,
                        "wire_window",
                    )?,
                    "wire_window",
                )?,
            ),
            (
                Resource::RxBytes,
                mul(
                    max_connections,
                    self.rx_bytes_per_connection as usize,
                    "rx_bytes",
                )?,
            ),
            (Resource::ControlReserve, self.control_reserve_bytes),
            (Resource::WorkerJobs, self.worker_jobs as usize),
            (Resource::RequestMetadata, request_metadata),
            (
                Resource::ControlEvents,
                add(16, self.max_open_topics as usize, "control_events")?,
            ),
        ] {
            credits[resource as usize] = limit;
        }
        let fixed_metadata = memory::fixed_metadata(self, max_connections, &credits)?;
        let configured_capacity_subtotal = add(
            total,
            fixed_metadata.configured_bytes,
            "configured capacity subtotal",
        )?;
        Ok(ValidatedConfig {
            max_connections,
            effective_batch_payload_bytes: payload,
            max_send_segments: self.request_max_partitions as usize * 3 + 4,
            request_metadata_bytes,
            credits,
            memory: MemoryBudgetReport {
                input: self.input_bytes,
                compressed: self.compressed_bytes,
                codec_workspace,
                transport,
                control: self.control_reserve_bytes,
                request_metadata,
                configured_byte_pools: total,
                fixed_metadata,
                configured_capacity_subtotal,
                unaccounted: memory::gaps(),
                exclusions: memory::exclusions(),
            },
        })
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::field_reassign_with_default)]
    use super::*;
    #[test]
    fn defaults_reserve_frame_overhead_and_report_configured_byte_bounds() {
        let config = ProducerConfig::default();
        let v = config.validate().unwrap();
        assert!(v.effective_batch_payload_bytes < config.batch_hard_bytes);
        assert_eq!(v.max_connections, 66);
        assert_eq!(v.max_send_segments, 196);
        assert_eq!(
            v.memory.configured_byte_pools,
            v.memory.input
                + v.memory.compressed
                + v.memory.codec_workspace
                + v.memory.transport
                + v.memory.control
                + v.memory.request_metadata
        );
    }
    #[test]
    fn fixed_storage_is_disjoint_and_cannot_be_mistaken_for_a_complete_bound() {
        let v = ProducerConfig::default().validate().unwrap();
        let fixed = v.memory.fixed_metadata;
        assert!(fixed.engine_object_pools > 0);
        assert!(fixed.engine_queues > 0);
        assert!(fixed.client_queues > 0);
        assert!(fixed.input_registry > 0);
        assert!(fixed.routing_scratch > 0);
        assert_eq!(
            fixed.configured_bytes,
            fixed.engine_object_pools
                + fixed.engine_queues
                + fixed.client_queues
                + fixed.input_registry
                + fixed.routing_scratch
                + fixed.metrics.configured_bytes
        );
        assert_eq!(
            v.memory.configured_capacity_subtotal,
            v.memory.configured_byte_pools + fixed.configured_bytes
        );
        let incomplete = v.memory.require_complete_core_bound().unwrap_err();
        assert_eq!(incomplete.unaccounted, v.memory.unaccounted);
        assert!(
            incomplete
                .unaccounted
                .contains(&UnaccountedMemory::OrderedIndexes)
        );
        assert!(
            incomplete
                .unaccounted
                .contains(&UnaccountedMemory::CollectionReservationSlack)
        );
        assert!(
            v.memory
                .exclusions
                .contains(&MemoryScopeExclusion::InjectedRuntimeAndProviderState)
        );
    }
    #[test]
    fn metadata_overflow_is_rejected_even_when_all_byte_pools_fit() {
        let mut config = ProducerConfig::default();
        let original = config.validate().unwrap().memory;
        config.input_bytes = usize::MAX - (original.configured_byte_pools - config.input_bytes);
        assert_eq!(
            config.validate(),
            Err(ConfigError::Overflow {
                field: "configured capacity subtotal"
            })
        );
    }
    #[test]
    fn standalone_header_leaves_exactly_two_payload_chunks_at_the_boundary() {
        let mut config = ProducerConfig::default();
        let payload = config.validate().unwrap().effective_batch_payload_bytes;
        config.output_chunk_bytes = payload.div_ceil(2);
        assert!(config.validate().is_ok());
        config.output_chunk_bytes -= 1;
        assert!(matches!(
            config.validate(),
            Err(ConfigError::Invalid {
                field: "output_chunk_bytes",
                ..
            })
        ));
    }
    #[test]
    fn request_metadata_reserves_one_promoted_header_per_selected_partition() {
        let config = ProducerConfig::default();
        let validated = config.validate().unwrap();
        let partitions = config.request_max_partitions as usize;
        let arena = config.client_id.len() + 64 + (32 + 61) * partitions;
        let coalesced = (4 * partitions + 4) * (config.coalesce_below_bytes as usize - 1);
        assert_eq!(validated.request_metadata_bytes, arena + coalesced);
        assert_eq!(
            validated.memory.request_metadata,
            validated.request_metadata_bytes * validated.credits[Resource::RequestSlots as usize]
        );
    }
    #[test]
    fn invalid_relationships_and_overflow_fail_before_admission() {
        let mut c = ProducerConfig::default();
        c.lanes = 0;
        assert!(c.validate().is_err());
        c.lanes = 5;
        assert!(c.validate().is_err());
        c.lanes = 1;
        c.delivery_event_capacity = 1;
        assert!(c.validate().is_err());
        c = ProducerConfig::default();
        c.input_bytes = usize::MAX;
        assert!(matches!(c.validate(), Err(ConfigError::Overflow { .. })));
        c = ProducerConfig::default();
        c.request_hard_bytes = c.batch_target_bytes;
        c.batch_hard_bytes = c.batch_target_bytes;
        c.request_target_bytes = c.batch_target_bytes;
        assert!(c.validate().is_err());
        c = ProducerConfig::default();
        c.output_chunk_bytes = 1024;
        assert!(c.validate().is_err());
    }
    #[test]
    fn credentials_never_appear_in_configuration_debug() {
        let mut c = ProducerConfig::default();
        c.security = SecurityConfig::SaslTls {
            tls: TlsConfig {
                use_system_roots: true,
                ..Default::default()
            },
            mechanism: SaslMechanism::Plain,
            username: "alice".into(),
            password: Secret::new("never-log-this-secret".into()),
        };
        assert!(c.validate().is_ok());
        assert!(!format!("{c:?}").contains("never-log-this-secret"));
    }
}
