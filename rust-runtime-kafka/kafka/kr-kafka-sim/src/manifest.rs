use crate::{
    DRIVER_VERSION, HISTORY_VERSION, MANIFEST_VERSION, MODEL_VERSION, SCENARIO_VERSION,
    config::ProducerDef,
};
use kr_kafka_producer::config::{Compression, ProducerConfig};
use kr_runtime::{
    RuntimeConfig, RuntimeDuration,
    rng::{DETERMINISTIC_RNG_VERSION, DeterministicRng, RandomStream, derive_stream_seed},
};
use serde::{Deserialize, Serialize};

/// Complete experiment decision tapes can exceed the legacy JSON envelope.
/// Presentation artifacts have independent, much smaller loader limits.
pub const MAX_EXPERIMENT_MANIFEST_BYTES: usize = 1024 * 1024 * 1024;
const MAX_LEGACY_MANIFEST_BYTES: usize = 16 * 1024 * 1024;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct Versions {
    pub manifest: u32,
    pub history: u32,
    pub scenario: u32,
    pub model: u32,
    pub driver: u32,
    pub rng: u32,
    pub package: String,
    pub source_sha256: String,
    pub kafka_schema_sha256: String,
    pub kafka_revision: String,
}
impl Default for Versions {
    fn default() -> Self {
        Self {
            manifest: MANIFEST_VERSION,
            history: HISTORY_VERSION,
            scenario: SCENARIO_VERSION,
            model: MODEL_VERSION,
            driver: DRIVER_VERSION,
            rng: DETERMINISTIC_RNG_VERSION,
            package: env!("CARGO_PKG_VERSION").into(),
            source_sha256: env!("PRODUCER_SOURCE_SHA256").into(),
            kafka_schema_sha256: kr_kafka_protocol::SCHEMA_SHA256.into(),
            kafka_revision: kr_kafka_protocol::errors::ERROR_SOURCE_REVISION.into(),
        }
    }
}
#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct CampaignLimits {
    pub records: u32,
    pub record_bytes: u32,
    pub history_events: usize,
    pub steps: u64,
    pub elapsed_ns: u64,
    pub trace_bytes: usize,
}
impl Default for CampaignLimits {
    fn default() -> Self {
        Self {
            records: 32,
            record_bytes: 512,
            history_events: 65536,
            steps: 500_000,
            elapsed_ns: 5_000_000_000,
            trace_bytes: 4 * 1024 * 1024,
        }
    }
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct RngInput {
    pub stream: String,
    pub seed: u64,
    pub state: u64,
    pub draws: u64,
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct BrokerSpec {
    pub id: i32,
    pub host: String,
    pub port: u16,
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct TopicSpec {
    pub id: [u8; 16],
    pub name: String,
    pub leaders: Vec<i32>,
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct HeaderSpec {
    pub key: String,
    pub value: Option<Vec<u8>>,
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct RecordSpec {
    pub id: u64,
    pub topic: u32,
    pub partition: i32,
    pub key_routed: bool,
    pub lane: u8,
    pub key: Option<Vec<u8>>,
    pub value: Option<Vec<u8>>,
    pub timestamp_ms: i64,
    pub native: bool,
    pub headers: Vec<HeaderSpec>,
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub enum Workload {
    SettleAllAccepted {
        timeout_ns: u64,
        require_acked: bool,
    },
    SleepUntil {
        at_ns: u64,
    },
    BeginRound {
        round: u32,
    },
    Settle {
        count: u32,
        timeout_ns: u64,
        require_acked: bool,
    },
    AwaitFlush {
        timeout_ns: u64,
        require_acked: bool,
    },
    CreateTopic {
        topic: u32,
    },
    DeleteTopic {
        topic: u32,
    },
    RecreateTopic {
        topic: u32,
        new_id: [u8; 16],
    },
    AddPartitions {
        topic: u32,
        additional_leaders: Vec<i32>,
    },
    MoveLeader {
        topic: u32,
        partition: i32,
        broker: i32,
    },
    CloseTopic {
        topic: u32,
    },
    OpenTopic {
        topic: u32,
    },
    Submit {
        records: Vec<RecordSpec>,
    },
    WaitDeliveries {
        count: u32,
    },
    Flush,
    Sleep {
        nanos: u64,
    },
    StopPolling {
        nanos: u64,
    },
    Cancel {
        record_id: u64,
    },
    Close {
        deadline_ns: u64,
    },
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub enum Fault {
    DropBeforeCommit,
    DropAfterCommit,
    DisconnectBeforeCommit,
    DisconnectAfterCommit,
    DuplicateSequence,
    RejectSequence,
    LeaderMove {
        topic: u32,
        partition: i32,
        broker: i32,
    },
    Throttle {
        millis: i32,
    },
    Delete {
        topic: u32,
    },
    Recreate {
        topic: u32,
        new_id: [u8; 16],
    },
    AddPartitions {
        topic: u32,
        additional_leaders: Vec<i32>,
    },
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct FaultRule {
    pub produce_index: u32,
    pub fault: Fault,
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct RealizedFault {
    pub produce_index: u32,
    pub correlation: i32,
    pub rule: FaultRule,
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct DriverSettings {
    pub admission_retry_limit: u32,
    pub admission_retry_delay_ns: u64,
    pub vectored: bool,
    pub encode_bytes: u32,
    pub encode_cost_ns: u64,
    pub service_delay_ns: u64,
    pub chunk_bytes: usize,
    pub link_latency_ns: u64,
    pub jitter_ns: u64,
    pub pipe_bytes: usize,
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct RuntimeBounds {
    pub tasks: usize,
    pub timers: usize,
}
impl Default for RuntimeBounds {
    fn default() -> Self {
        Self {
            tasks: 128,
            timers: 1024,
        }
    }
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct NetworkBounds {
    pub connections: usize,
    pub operations: usize,
    pub listeners: usize,
    pub backlog: usize,
    pub operation_bytes: usize,
    pub read_bytes: usize,
    pub write_bytes: usize,
    pub scripted_faults: usize,
    pub link_overrides: usize,
    pub blocked_links: usize,
}
impl Default for NetworkBounds {
    fn default() -> Self {
        Self {
            connections: 16,
            operations: 128,
            listeners: 4,
            backlog: 8,
            operation_bytes: 2 * 1024 * 1024,
            read_bytes: 16 * 1024 * 1024,
            write_bytes: 16 * 1024 * 1024,
            scripted_faults: 64,
            link_overrides: 16,
            blocked_links: 16,
        }
    }
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ModelBounds {
    pub frame_bytes: usize,
    pub topics: usize,
    pub partitions: usize,
    pub brokers: usize,
    pub producers: usize,
    pub log_batches: usize,
    pub log_records: usize,
    pub log_bytes: usize,
    pub string_bytes: usize,
    pub topic_id_prefix: u64,
    pub batch_wire_bytes: usize,
    pub batch_raw_bytes: usize,
    pub batch_records: usize,
    pub batch_headers: usize,
    pub batch_field_bytes: usize,
    pub zstd_window_log: u32,
}
impl Default for ModelBounds {
    fn default() -> Self {
        Self {
            frame_bytes: 4 * 1024 * 1024,
            topics: 8,
            partitions: 32,
            brokers: 4,
            producers: 256,
            log_batches: 512,
            log_records: 1024,
            log_bytes: 8 * 1024 * 1024,
            string_bytes: 1024,
            topic_id_prefix: 0x4b61666b614d6f64,
            batch_wire_bytes: 1024 * 1024 + 61,
            batch_raw_bytes: 1024 * 1024,
            batch_records: 131072,
            batch_headers: 131072,
            batch_field_bytes: 1024 * 1024,
            zstd_window_log: 23,
        }
    }
}
impl ModelBounds {
    pub(crate) fn config(&self, version: i16) -> kr_kafka_broker_model::BrokerConfig {
        kr_kafka_broker_model::BrokerConfig {
            produce_max_version: version,
            max_frame_bytes: self.frame_bytes,
            max_topics: self.topics,
            max_partitions: self.partitions,
            max_brokers: self.brokers,
            max_producers: self.producers,
            max_log_batches: self.log_batches,
            max_log_records: self.log_records,
            max_log_bytes: self.log_bytes,
            max_string_bytes: self.string_bytes,
            topic_id_prefix: self.topic_id_prefix,
            batch_limits: kr_kafka_record::BatchDecodeLimits {
                max_wire_bytes: self.batch_wire_bytes,
                max_raw_bytes: self.batch_raw_bytes,
                max_records: self.batch_records,
                max_headers: self.batch_headers,
                max_field_bytes: self.batch_field_bytes,
                max_zstd_window_log: self.zstd_window_log,
            },
        }
    }
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReplayManifest {
    pub versions: Versions,
    pub seed: u64,
    pub start_ns: u64,
    pub limits: CampaignLimits,
    pub runtime: RuntimeBounds,
    pub network: NetworkBounds,
    pub model: ModelBounds,
    #[serde(with = "ProducerDef")]
    pub producer: ProducerConfig,
    pub driver: DriverSettings,
    pub brokers: Vec<BrokerSpec>,
    pub topics: Vec<TopicSpec>,
    pub produce_max_version: i16,
    pub workload: Vec<Workload>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub experiment: Option<crate::ExperimentWorkload>,
    #[serde(default, skip_serializing_if = "is_false")]
    pub observe_requests: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub metrics_sampling: Option<crate::MetricsSampling>,
    pub fault_plan: Vec<FaultRule>,
    pub rng_inputs: Vec<RngInput>,
    pub realized_faults: Option<Vec<RealizedFault>>,
    pub initially_absent_topics: Vec<u32>,
    pub require_all_acked: bool,
    pub minimum_acked: u32,
    /// Applies a single aggregate deadline to each generated recovery round.
    pub recovery_round_timeout_ns: Option<u64>,
    pub require_fault_coverage: bool,
    /// Third observation after the producer closes; no consumer policy.
    pub fetch_probe: bool,
    pub faults: crate::faults::FaultConfig,
    pub fault_decisions: Option<Vec<crate::faults::Decision>>,
}
impl ReplayManifest {
    pub fn from_seed(seed: u64, limits: CampaignLimits) -> Result<Self, String> {
        if !(2..=256).contains(&limits.records)
            || !(128..=4096).contains(&limits.record_bytes)
            || limits.history_events < 64
            || limits.steps == 0
            || limits.elapsed_ns < 1_000_000_000
            || limits.trace_bytes < 4096
        {
            return Err("invalid campaign limits".into());
        }
        let mut scenario = DeterministicRng::from_root_seed(seed, RandomStream::Scenario);
        let mut workload = DeterministicRng::from_root_seed(seed, RandomStream::Workload);
        let mut schedule = DeterministicRng::from_root_seed(seed, RandomStream::Schedule);
        let mut fault = DeterministicRng::from_root_seed(seed, RandomStream::Fault);
        let partitions = 1 + scenario.u64_below(3).map_err(|e| e.to_string())? as usize;
        let lanes = 1 + scenario.u64_below(2).map_err(|e| e.to_string())? as u8;
        let mut id = [0u8; 16];
        id[..8].copy_from_slice(&seed.to_be_bytes());
        id[8..].copy_from_slice(&scenario.next_u64().to_be_bytes());
        let descriptor_limit = 16 + scenario.u64_below(49).map_err(|e| e.to_string())? as u32;
        let inflight = 1 + scenario.u64_below(5).map_err(|e| e.to_string())? as u8;
        let window = 16384 * (1 + scenario.u64_below(4).map_err(|e| e.to_string())? as u32);
        let config = ProducerConfig {
            max_in_flight_per_connection: inflight,
            connection_wire_window_bytes: window,
            compression: if scenario.next_u64() & 1 == 0 {
                Compression::None
            } else {
                Compression::Zstd { level: 1 }
            },
            lanes,
            codec_contexts: 2,
            record_descriptors: descriptor_limit,
            delivery_event_capacity: 64,
            release_event_capacity: 16,
            max_live_leases: 16,
            max_batches: 64,
            max_open_topics: 4,
            pending_records_per_topic: descriptor_limit,
            brokers_max: 2,
            input_bytes: 1024 * 1024,
            compressed_bytes: 512 * 1024,
            batch_target_bytes: 256,
            batch_hard_bytes: 8192,
            request_target_bytes: 8192,
            request_hard_bytes: 16384,
            request_max_partitions: 4,
            output_chunk_bytes: 4096,
            progressive_threshold: 64,
            mailbox_capacity: 2,
            max_submission_records: 256,
            max_completions_per_poll: 8,
            max_submissions_per_poll: 4,
            request_timeout: RuntimeDuration::from_nanos(20_000_000),
            delivery_timeout: RuntimeDuration::from_nanos(1_000_000_000),
            retry_backoff_min: RuntimeDuration::from_nanos(100_000),
            retry_backoff_max: RuntimeDuration::from_nanos(2_000_000),
            linger_skip_below_rate: None,
            bootstrap: vec![kr_kafka_producer::config::BrokerEndpoint {
                host: "model".into(),
                port: 9092,
            }],
            ..ProducerConfig::default()
        };
        let driver = DriverSettings {
            admission_retry_limit: 4096,
            admission_retry_delay_ns: 1_000_000,
            vectored: scenario.next_u64() & 1 == 0,
            encode_bytes: 128,
            encode_cost_ns: 1000,
            service_delay_ns: schedule.u64_below(1000).map_err(|e| e.to_string())?,
            chunk_bytes: 8 + schedule.u64_below(64).map_err(|e| e.to_string())? as usize,
            link_latency_ns: schedule.u64_below(1000).map_err(|e| e.to_string())?,
            jitter_ns: schedule.u64_below(1000).map_err(|e| e.to_string())?,
            pipe_bytes: 257,
        };
        let mut records = Vec::new();
        for record in 1..=limits.records {
            let bytes = 128
                + workload
                    .u64_below(u64::from(limits.record_bytes - 127))
                    .map_err(|e| e.to_string())? as usize;
            let mut value = vec![0u8; bytes];
            value[..8].copy_from_slice(&u64::from(record).to_be_bytes());
            for byte in &mut value[8..] {
                *byte = (workload.next_u64() & 15) as u8;
            }
            records.push(RecordSpec {
                id: u64::from(record),
                topic: 0,
                partition: (record as usize % partitions) as i32,
                key_routed: false,
                lane: 0,
                key: if workload.next_u64() & 1 == 0 {
                    None
                } else {
                    Some(Vec::new())
                },
                value: Some(value),
                timestamp_ms: i64::from(record),
                native: record % 3 == 0,
                headers: identity_headers(u64::from(record)),
            });
        }
        let mut first = records.remove(0);
        first.value.as_mut().expect("generated value").truncate(128);
        let mut operations = vec![
            Workload::Submit {
                records: vec![first],
            },
            Workload::WaitDeliveries { count: 1 },
        ];
        // Separate published bulks exceed the mailbox's two regular slots before
        // its owner runs, guaranteeing real accepted-prefix backpressure.
        for bulk in records.chunks(4) {
            operations.push(Workload::Submit {
                records: bulk.to_vec(),
            });
        }
        let mut fault_plan = Vec::new();
        let selected = if seed == 0 {
            None
        } else {
            Some((seed % 12, fault.next_u64()))
        };
        if let Some((kind, draw)) = selected {
            let effect = match kind {
                0 => Fault::DropBeforeCommit,
                1 => Fault::DropAfterCommit,
                2 => Fault::DisconnectBeforeCommit,
                3 => Fault::DisconnectAfterCommit,
                4 => Fault::DuplicateSequence,
                5 => Fault::RejectSequence,
                6 => Fault::LeaderMove {
                    topic: 0,
                    partition: 0,
                    broker: 2,
                },
                7 => Fault::Throttle {
                    millis: 1 + (draw % 10) as i32,
                },
                8 => Fault::Delete { topic: 0 },
                9 => {
                    let mut new_id = id;
                    new_id[15] ^= 0xff;
                    Fault::Recreate { topic: 0, new_id }
                }
                10 => Fault::AddPartitions {
                    topic: 0,
                    additional_leaders: vec![1],
                },
                _ => Fault::DropAfterCommit,
            };
            fault_plan.push(FaultRule {
                produce_index: 1,
                fault: effect,
            });
            if seed.is_multiple_of(4) {
                operations.push(Workload::StopPolling { nanos: 5_000_000 });
            }
            if seed.is_multiple_of(7) {
                operations.push(Workload::Cancel {
                    record_id: u64::from(limits.records),
                });
            }
        }
        operations.extend([
            Workload::Flush,
            Workload::Close {
                deadline_ns: 2_000_000_000,
            },
        ]);
        let rng_inputs = [
            (RandomStream::Scenario, &scenario),
            (RandomStream::Workload, &workload),
            (RandomStream::Schedule, &schedule),
            (RandomStream::Fault, &fault),
        ]
        .into_iter()
        .map(|(stream, rng)| RngInput {
            stream: format!("{stream:?}"),
            seed: derive_stream_seed(seed, stream),
            state: rng.checkpoint().state(),
            draws: rng.checkpoint().draws(),
        })
        .chain(std::iter::once(RngInput {
            stream: "Debug".into(),
            seed: derive_stream_seed(seed, RandomStream::Debug),
            state: derive_stream_seed(seed, RandomStream::Debug),
            draws: 0,
        }))
        .collect();
        let manifest = Self {
            versions: Versions::default(),
            seed,
            start_ns: RuntimeConfig::derived_start_time(seed).as_nanos(),
            limits,
            producer: config,
            driver,
            runtime: RuntimeBounds::default(),
            network: NetworkBounds::default(),
            model: ModelBounds::default(),
            brokers: vec![
                BrokerSpec {
                    id: 1,
                    host: "model".into(),
                    port: 9092,
                },
                BrokerSpec {
                    id: 2,
                    host: "model".into(),
                    port: 9093,
                },
            ],
            topics: vec![TopicSpec {
                id,
                name: "events".into(),
                leaders: vec![1; partitions],
            }],
            produce_max_version: 13,
            workload: operations,
            experiment: None,
            observe_requests: false,
            metrics_sampling: None,
            fault_plan,
            rng_inputs,
            realized_faults: None,
            initially_absent_topics: Vec::new(),
            require_all_acked: false,
            minimum_acked: 1,
            recovery_round_timeout_ns: None,
            require_fault_coverage: false,
            fetch_probe: false,
            faults: crate::faults::FaultConfig::default(),
            fault_decisions: None,
        };
        manifest.validate()?;
        Ok(manifest)
    }
    pub fn validate(&self) -> Result<(), String> {
        if self.versions != Versions::default() {
            return Err("replay version/source mismatch".into());
        }
        self.faults.validate()?;
        if self.faults.crash_on_isolation && self.experiment.is_none() {
            return Err("crash isolation requires experiment workload".into());
        }
        let validated = self.producer.validate().map_err(|e| e.to_string())?;
        let l = self.limits;
        if self.minimum_acked > l.records {
            return Err("minimum Acked bound".into());
        }
        if !(2..=1_000_000).contains(&l.records)
            || !(128..=4096).contains(&l.record_bytes)
            || !(64..=8_000_000).contains(&l.history_events)
            || !(1..=200_000_000).contains(&l.steps)
            || !(1_000_000_000..=300_000_000_000).contains(&l.elapsed_ns)
            || !(4096..=64 * 1024 * 1024).contains(&l.trace_bytes)
            || self.start_ns.checked_add(l.elapsed_ns).is_none()
        {
            return Err("manifest campaign bounds".into());
        }
        if validated.memory.configured_byte_pools > 512 * 1024 * 1024
            || self.producer.record_descriptors > 4096
            || self.producer.max_batches > 1024
            || self.producer.mailbox_capacity > 4096
            || self.producer.max_submission_records > 4096
        {
            return Err("producer campaign allocation bounds".into());
        }
        let bounded = |n: usize, max: usize| n != 0 && n <= max;
        let n = &self.network;
        let m = &self.model;
        if !bounded(self.runtime.tasks, 512)
            || !bounded(self.runtime.timers, 8192)
            || !bounded(n.connections, 128)
            || !bounded(n.operations, 4096)
            || !bounded(n.listeners, 32)
            || !bounded(n.backlog, 1024)
            || !bounded(n.operation_bytes, 16 * 1024 * 1024)
            || !bounded(n.read_bytes, 256 * 1024 * 1024)
            || !bounded(n.write_bytes, 256 * 1024 * 1024)
            || !bounded(n.scripted_faults, 1024)
            || !bounded(n.link_overrides, 1024)
            || !bounded(n.blocked_links, 1024)
            || !bounded(m.frame_bytes, 16 * 1024 * 1024)
            || !bounded(m.topics, 128)
            || !bounded(m.partitions, 1024)
            || !bounded(m.brokers, 32)
            || !bounded(m.producers, 4096)
            || !bounded(m.log_batches, 1_000_000)
            || !bounded(m.log_records, 1_000_000)
            || !bounded(m.log_bytes, 2 * 1024 * 1024 * 1024)
            || !bounded(m.string_bytes, 4096)
            || !bounded(m.batch_wire_bytes, 16 * 1024 * 1024)
            || !bounded(m.batch_raw_bytes, 16 * 1024 * 1024)
            || !bounded(m.batch_records, 131072)
            || !bounded(m.batch_headers, 131072)
            || !bounded(m.batch_field_bytes, 16 * 1024 * 1024)
            || !(10..=23).contains(&m.zstd_window_log)
        {
            return Err("runtime/provider/model allocation bounds".into());
        }
        if self.brokers.is_empty()
            || self.brokers.len() > 5
            || self.topics.is_empty()
            || self.topics.len() > 4
            || (self.workload.is_empty() && self.experiment.is_none())
            || self.workload.len() > 65536
            || self.fault_plan.len() > 128
            || !bounded(self.driver.chunk_bytes, n.operation_bytes)
            || !bounded(self.driver.encode_bytes as usize, 16 * 1024 * 1024)
            || !bounded(self.driver.pipe_bytes, 16 * 1024 * 1024)
            || self.driver.encode_cost_ns == 0
            || !(1..=8192).contains(&self.driver.admission_retry_limit)
            || !(1..=1_000_000_000).contains(&self.driver.admission_retry_delay_ns)
            || [
                self.driver.encode_cost_ns,
                self.driver.service_delay_ns,
                self.driver.link_latency_ns,
                self.driver.jitter_ns,
            ]
            .into_iter()
            .any(|ns| ns > l.elapsed_ns)
            || !matches!(self.produce_max_version, 9 | 13)
        {
            return Err("manifest count/driver bounds".into());
        }
        let mut broker_ids = std::collections::BTreeSet::new();
        let mut broker_endpoints = std::collections::BTreeSet::new();
        for broker in &self.brokers {
            if broker.id < 0
                || broker.host.is_empty()
                || broker.host.len() > 255
                || broker.port == 0
                || !broker_ids.insert(broker.id)
                || !broker_endpoints.insert((&broker.host, broker.port))
            {
                return Err("invalid/duplicate broker".into());
            }
        }
        let mut topic_ids = std::collections::BTreeSet::new();
        let mut names = std::collections::BTreeSet::new();
        for topic in &self.topics {
            if topic.id == [0; 16]
                || !topic_ids.insert(topic.id)
                || topic.name.is_empty()
                || topic.name.len() > 249
                || !names.insert(&topic.name)
                || topic.leaders.is_empty()
                || topic.leaders.len()
                    > if self.experiment.is_some() {
                        m.partitions
                    } else {
                        16
                    }
                || topic.leaders.iter().any(|id| !broker_ids.contains(id))
            {
                return Err("invalid/duplicate topic".into());
            }
        }
        for topic in &self.initially_absent_topics {
            if *topic as usize >= self.topics.len() {
                return Err("initially absent topic index".into());
            }
        }
        if self.initially_absent_topics.len() > self.topics.len()
            || self
                .initially_absent_topics
                .iter()
                .collect::<std::collections::BTreeSet<_>>()
                .len()
                != self.initially_absent_topics.len()
        {
            return Err("initially absent topic bound".into());
        }
        for service in &self.faults.services {
            if !broker_ids.contains(&service.broker) {
                return Err("unknown service broker".into());
            }
        }
        if crate::experiment_link::enabled(&self.faults) && self.experiment.is_none() {
            return Err("experiment links require the experiment timeline".into());
        }
        for rule in &self.faults.environment {
            if rule.broker.is_some_and(|id| !broker_ids.contains(&id)) || rule.end_ns > l.elapsed_ns
            {
                return Err("environment topology/time bounds".into());
            }
        }
        for link in &self.faults.links {
            if !broker_ids.contains(&link.broker)
                || link.to_broker_latency_ns > l.elapsed_ns
                || link.from_broker_latency_ns > l.elapsed_ns
            {
                return Err("experiment link topology/time bounds".into());
            }
        }
        for window in &self.faults.link_outages {
            if !broker_ids.contains(&window.broker) || window.end_ns > l.elapsed_ns {
                return Err("link outage topology/time bounds".into());
            }
        }
        for isolation in &self.faults.isolations {
            if !broker_ids.contains(&isolation.broker) || isolation.end_ns > l.elapsed_ns {
                return Err("isolation topology/time bounds".into());
            }
        }
        if self
            .fault_decisions
            .as_ref()
            .is_some_and(|tape| tape.len() > self.faults.max_decisions)
        {
            return Err("fault replay decision capacity".into());
        }
        if self.fetch_probe && (!self.require_all_acked || self.produce_max_version != 13) {
            return Err(
                "Fetch probe requires the fully acknowledged strict-identity profile".into(),
            );
        }
        if let Some(sampling) = self.metrics_sampling {
            sampling.validate(self)?;
        }
        if self.recovery_round_timeout_ns.is_some() {
            crate::campaign::validate_finite_profile(self)?;
        }
        if let Some(experiment) = &self.experiment {
            experiment.validate(self)?;
        }
        let mut partition_counts: Vec<_> = self
            .topics
            .iter()
            .map(|topic| topic.leaders.len())
            .collect();
        let mut ids = std::collections::BTreeSet::new();
        let mut closed = false;
        let mut waits = 0u64;
        for op in &self.workload {
            if let Workload::Settle { timeout_ns, .. } = op
                && (*timeout_ns == 0 || *timeout_ns > l.elapsed_ns)
            {
                return Err("barrier timeout bound".into());
            }
            if closed {
                return Err("workload operation after close".into());
            }
            match op {
                Workload::Submit { records } => {
                    for record in records {
                        self.topics
                            .get(record.topic as usize)
                            .ok_or("record topic")?;
                        if record.id == 0
                            || !ids.insert(record.id)
                            || record.partition < 0
                            || record.partition as usize >= partition_counts[record.topic as usize]
                            || record.lane >= self.producer.lanes
                            || record.key_routed && record.key.is_none()
                            || record
                                .value
                                .as_ref()
                                .is_some_and(|value| value.len() > l.record_bytes as usize)
                            || record_id(
                                record
                                    .headers
                                    .iter()
                                    .map(|header| (header.key.as_str(), header.value.as_deref())),
                            )? != record.id
                            || record
                                .key
                                .as_ref()
                                .is_some_and(|key| key.len() > l.record_bytes as usize)
                            || record.headers.len() > 16
                            || record.headers.iter().any(|header| {
                                header.key.len() > 128
                                    || header.value.as_ref().is_some_and(|value| value.len() > 128)
                            })
                        {
                            return Err("manifest record/UID bounds".into());
                        }
                    }
                }
                Workload::SettleAllAccepted { timeout_ns, .. } => {
                    if *timeout_ns == 0 || *timeout_ns > l.elapsed_ns {
                        return Err("barrier timeout bound".into());
                    }
                }
                Workload::SleepUntil { at_ns } => {
                    if *at_ns > l.elapsed_ns {
                        return Err("sleep-until time bound".into());
                    }
                }
                Workload::WaitDeliveries { count } | Workload::Settle { count, .. } => {
                    if *count as usize > ids.len() {
                        return Err("wait exceeds submitted record count".into());
                    }
                }
                Workload::Cancel { record_id } => {
                    if !ids.contains(record_id) {
                        return Err("cancel before submission".into());
                    }
                }
                Workload::Sleep { nanos } | Workload::StopPolling { nanos } => {
                    waits = waits.checked_add(*nanos).ok_or("workload sleep overflow")?;
                }
                Workload::Close { deadline_ns } => {
                    if *deadline_ns > l.elapsed_ns {
                        return Err("close deadline bound".into());
                    }
                    closed = true;
                }
                Workload::Flush => {}
                Workload::BeginRound { round } => {
                    if *round > 1024 {
                        return Err("round count bound".into());
                    }
                }
                Workload::AwaitFlush { timeout_ns, .. } => {
                    if *timeout_ns == 0 || *timeout_ns > l.elapsed_ns {
                        return Err("barrier timeout bound".into());
                    }
                }
                Workload::CreateTopic { topic }
                | Workload::DeleteTopic { topic }
                | Workload::CloseTopic { topic }
                | Workload::OpenTopic { topic } => {
                    if *topic as usize >= self.topics.len() {
                        return Err("workload topic index".into());
                    }
                }
                Workload::RecreateTopic { topic, new_id } => {
                    if *topic as usize >= self.topics.len()
                        || *new_id == [0; 16]
                        || !topic_ids.insert(*new_id)
                    {
                        return Err("workload recreation identity".into());
                    }
                    partition_counts[*topic as usize] = self.topics[*topic as usize].leaders.len();
                }
                Workload::AddPartitions {
                    topic,
                    additional_leaders,
                } => {
                    let count = partition_counts
                        .get_mut(*topic as usize)
                        .ok_or("workload growth topic")?;
                    if additional_leaders.is_empty()
                        || *count + additional_leaders.len() > 16
                        || additional_leaders.iter().any(|id| !broker_ids.contains(id))
                    {
                        return Err("workload growth bounds".into());
                    }
                    *count += additional_leaders.len();
                }
                Workload::MoveLeader {
                    topic,
                    partition,
                    broker,
                } => {
                    if *partition < 0
                        || *partition as usize
                            >= *partition_counts
                                .get(*topic as usize)
                                .ok_or("workload leader topic")?
                        || !broker_ids.contains(broker)
                    {
                        return Err("workload leader bounds".into());
                    }
                }
            }
        }
        if (self.experiment.is_none() && !closed)
            || ids.len() > l.records as usize
            || waits > l.elapsed_ns
        {
            return Err("workload lifecycle bounds".into());
        }
        let topic = |index: u32| self.topics.get(index as usize).ok_or("fault topic index");
        for rule in &self.fault_plan {
            if rule.produce_index > 4096 {
                return Err("fault request index bound".into());
            }
            match &rule.fault {
                Fault::LeaderMove {
                    topic: index,
                    partition,
                    broker,
                } => {
                    if *partition < 0
                        || *partition as usize >= topic(*index)?.leaders.len()
                        || !broker_ids.contains(broker)
                    {
                        return Err("leader fault bounds".into());
                    }
                }
                Fault::Delete { topic: index } => {
                    topic(*index)?;
                }
                Fault::Recreate {
                    topic: index,
                    new_id,
                } => {
                    topic(*index)?;
                    if *new_id == [0; 16] || topic_ids.contains(new_id) {
                        return Err("recreate must allocate a fresh ID".into());
                    }
                }
                Fault::AddPartitions {
                    topic: index,
                    additional_leaders,
                } => {
                    if additional_leaders.is_empty()
                        || topic(*index)?.leaders.len() + additional_leaders.len() > 16
                        || additional_leaders.iter().any(|id| !broker_ids.contains(id))
                    {
                        return Err("expansion fault bounds".into());
                    }
                }
                Fault::Throttle { millis }
                    if *millis < 0 || *millis as u64 * 1_000_000 > l.elapsed_ns =>
                {
                    return Err("throttle fault bounds".into());
                }
                _ => {}
            }
        }
        if self.realized_faults.as_ref().is_some_and(|rules| {
            rules.len() > self.fault_plan.len()
                || rules.iter().any(|rule| {
                    rule.produce_index != rule.rule.produce_index
                        || !self.fault_plan.contains(&rule.rule)
                })
        }) {
            return Err("unplanned realized fault".into());
        }
        if self.rng_inputs.len() != 5 {
            return Err("missing RNG domain checkpoint".into());
        }
        for (input, stream) in self.rng_inputs.iter().zip([
            RandomStream::Scenario,
            RandomStream::Workload,
            RandomStream::Schedule,
            RandomStream::Fault,
            RandomStream::Debug,
        ]) {
            if input.stream != format!("{stream:?}")
                || input.seed != derive_stream_seed(self.seed, stream)
                || input.draws > 2_000_000
            {
                return Err("RNG domain input mismatch".into());
            }
            let mut rng = DeterministicRng::from_root_seed(self.seed, stream);
            for _ in 0..input.draws {
                rng.next_u64();
            }
            if rng.checkpoint().state() != input.state {
                return Err("RNG checkpoint state mismatch".into());
            }
        }
        Ok(())
    }
    pub fn to_json(&self) -> Result<Vec<u8>, String> {
        serde_json::to_vec_pretty(self).map_err(|e| e.to_string())
    }
    pub fn from_json(bytes: &[u8]) -> Result<Self, String> {
        if bytes.len() > MAX_EXPERIMENT_MANIFEST_BYTES {
            return Err("manifest byte limit".into());
        }
        let manifest: Self = serde_json::from_slice(bytes).map_err(|e| e.to_string())?;
        if manifest.experiment.is_none() && bytes.len() > MAX_LEGACY_MANIFEST_BYTES {
            return Err("legacy manifest byte limit".into());
        }
        manifest.validate()?;
        Ok(manifest)
    }
}

/// Harness identity is independent of nullable application key/value fields.
pub const RECORD_ID_HEADER: &str = "kr-dst-id";
pub fn identity_headers(id: u64) -> Vec<HeaderSpec> {
    vec![
        HeaderSpec {
            key: RECORD_ID_HEADER.into(),
            value: Some(id.to_be_bytes().to_vec()),
        },
        HeaderSpec {
            key: RECORD_ID_HEADER.into(),
            value: None,
        },
    ]
}
pub(crate) fn record_id<'a>(
    headers: impl Iterator<Item = (&'a str, Option<&'a [u8]>)>,
) -> Result<u64, String> {
    let mut ids = headers.filter(|(key, _)| *key == RECORD_ID_HEADER);
    let bytes = ids
        .next()
        .and_then(|(_, value)| value)
        .ok_or("missing workload ID header")?;
    let id = u64::from_be_bytes(bytes.try_into().map_err(|_| "invalid workload ID header")?);
    if id == 0 || ids.next() != Some((RECORD_ID_HEADER, None)) || ids.next().is_some() {
        return Err("invalid ordered workload ID headers".into());
    }
    Ok(id)
}

fn is_false(value: &bool) -> bool {
    !value
}
