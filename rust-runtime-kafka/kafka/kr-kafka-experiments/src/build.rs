//! Explicit shared defaults. Scenario builders own their Full and Test phases.
use crate::{Params, Size};
use kr_kafka_producer::config::{BrokerEndpoint, Compression};
use kr_kafka_sim::*;
use kr_runtime::RuntimeDuration;
pub const MS: u64 = 1_000_000;
pub const SECOND: u64 = 1_000 * MS;
pub fn ns(n: u64) -> RuntimeDuration {
    RuntimeDuration::from_nanos(n)
}
pub fn base(seed: u64, size: Size, brokers: usize) -> Result<ReplayManifest, String> {
    let mut m = ReplayManifest::from_seed(seed, CampaignLimits::default())?;
    m.workload.clear();
    m.fault_plan.clear();
    m.faults = Default::default();
    m.require_all_acked = false;
    m.minimum_acked = 0;
    m.brokers = (1..=brokers)
        .map(|id| BrokerSpec {
            id: id as i32,
            host: format!("broker-{id}"),
            port: 9092,
        })
        .collect();
    m.topics[0].name = "experiment".into();
    m.topics[0].leaders = (0..6).map(|p| m.brokers[p % brokers].id).collect();
    m.limits.records = if size == Size::Test { 512 } else { 1_000_000 };
    m.limits.record_bytes = 4096;
    m.limits.history_events = if size == Size::Test {
        262_144
    } else {
        8_000_000
    };
    m.limits.steps = 200_000_000;
    m.limits.elapsed_ns = 120 * SECOND;
    m.model.brokers = brokers;
    m.model.partitions = 64;
    m.model.log_records = m.limits.records as usize;
    m.model.log_batches = m.limits.records as usize;
    m.model.log_bytes = if size == Size::Test {
        8 * 1024 * 1024
    } else {
        2 * 1024 * 1024 * 1024
    };
    m.faults.max_decisions = if size == Size::Test {
        65_536
    } else {
        2_000_000
    };
    m.network.connections = 64;
    m.network.operations = 512;
    m.network.link_overrides = 32;
    m.runtime.tasks = 512;
    m.runtime.timers = 4096;
    let p = &mut m.producer;
    p.bootstrap = m
        .brokers
        .iter()
        .map(|b| BrokerEndpoint {
            host: b.host.clone(),
            port: b.port,
        })
        .collect();
    p.brokers_max = brokers as u16;
    p.lanes = 2;
    p.max_attempts = 100;
    p.delivery_timeout = ns(30 * SECOND);
    p.request_timeout = ns(200 * MS);
    p.topic_resolve_timeout = ns(5 * SECOND);
    p.retry_backoff_min = ns(10 * MS);
    p.retry_backoff_max = ns(100 * MS);
    p.metadata_max_age = ns(100 * MS);
    p.linger_max = ns(5 * MS);
    p.linger_skip_below_rate = None;
    p.max_in_flight_per_connection = 3;
    p.batch_target_bytes = 4096;
    p.batch_hard_bytes = 64 * 1024;
    p.output_chunk_bytes = 32 * 1024;
    p.request_target_bytes = 64 * 1024;
    p.request_hard_bytes = 128 * 1024;
    p.request_max_partitions = 16;
    p.connection_wire_window_bytes = 256 * 1024;
    p.input_bytes = 8 * 1024 * 1024;
    p.record_descriptors = 512;
    p.delivery_event_capacity = 512;
    p.pending_records_per_topic = 512;
    p.max_live_leases = 512;
    p.release_event_capacity = 512;
    p.max_batches = 256;
    p.mailbox_capacity = 512;
    p.compression = Compression::None;
    p.codec_contexts = 0;
    p.compressed_bytes = 8 * 1024 * 1024;
    p.metrics.max_broker_scopes = brokers as u16;
    p.metrics.max_partition_scopes = 32;
    p.metrics.significant_digits = 2;
    p.metrics.max_storage_bytes = 256 * 1024 * 1024;
    m.driver.vectored = true;
    m.driver.encode_bytes = 64 * 1024;
    m.driver.encode_cost_ns = 1000;
    m.driver.chunk_bytes = 64 * 1024;
    m.driver.pipe_bytes = 256 * 1024;
    m.driver.link_latency_ns = 1_000;
    m.driver.jitter_ns = 0;
    m.driver.service_delay_ns = MS;
    m.faults.links = m
        .brokers
        .iter()
        .map(|b| BrokerLink {
            broker: b.id,
            to_broker_latency_ns: 200_000,
            from_broker_latency_ns: 200_000,
            chunk_bytes: 64 * 1024,
        })
        .collect();
    m.observe_requests = true;
    m.metrics_sampling = Some(MetricsSampling {
        interval_ns: 250 * MS,
    });
    m.experiment = Some(ExperimentWorkload {
        loads: vec![],
        scheduled_actions: vec![],
        polling_pauses: vec![],
        offer_deadline_ns: 70 * SECOND,
        settle_timeout_ns: 35 * SECOND,
        close_timeout_ns: 5 * SECOND,
        require_acked: false,
    });
    Ok(m)
}
pub fn apply(m: &mut ReplayManifest, p: &Params) {
    let c = &mut m.producer;
    if let Some(enabled) = p.partition_pressure {
        c.descriptor_admission_policy = if enabled {
            kr_kafka_producer::config::DescriptorAdmissionPolicy::PartitionPressure
        } else {
            kr_kafka_producer::config::DescriptorAdmissionPolicy::Shared
        };
    }
    if let Some(v) = p.in_flight {
        c.max_in_flight_per_connection = v;
    }
    if let Some(v) = p.lanes {
        c.lanes = v;
    }
    if let Some(v) = p.linger_ns {
        c.linger_max = ns(v);
    }
    if let Some((min, max)) = p.backoff_ns {
        c.retry_backoff_min = ns(min);
        c.retry_backoff_max = ns(max);
    }
    if let Some(v) = p.request_timeout_ns {
        c.request_timeout = ns(v);
    }
    if let Some(v) = p.delivery_timeout_ns {
        c.delivery_timeout = ns(v);
    }
    if let Some(v) = p.metadata_max_age_ns {
        c.metadata_max_age = ns(v);
    }
    if let Some(v) = p.batch_target_bytes {
        c.batch_target_bytes = v;
    }
    if let Some(v) = p.wire_window_bytes {
        c.connection_wire_window_bytes = v;
    }
    if let Some(v) = p.compression {
        c.compression = if v == 0 {
            Compression::None
        } else {
            Compression::Zstd { level: v }
        };
        c.codec_contexts = if v == 0 { 0 } else { 4 };
    }
}
pub fn template(first_id: u64, p: &Params) -> RecordTemplate {
    RecordTemplate {
        first_id,
        topic: 0,
        partitioning: Partitioning::Keyed {
            keys: 64,
            skew_ppm: 0,
        },
        value_bytes: p.value_bytes.unwrap_or(512),
        key_bytes: 8,
        lane: LanePolicy::ByPartition,
        native: false,
        value_pattern: ValuePattern::Compressible,
    }
}
pub fn closed(m: &mut ReplayManifest, p: &Params, size: Size) {
    m.experiment.as_mut().unwrap().loads.push(LoadSpec {
        template: template(1, p),
        shape: LoadShape::ClosedLoop {
            start_ns: 0,
            count: if size == Size::Test { 128 } else { 4096 },
            outstanding: p.outstanding.unwrap_or(32),
        },
    });
}
