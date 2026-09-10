use kr_kafka_producer::config::Compression;
use kr_kafka_sim::{CampaignLimits, RecordSpec, ReplayManifest, RunReport, Workload};
use kr_runtime::RuntimeDuration;

pub(crate) const MS: u64 = 1_000_000;

pub(crate) fn scenario() -> ReplayManifest {
    let mut manifest = ReplayManifest::from_seed(
        0,
        CampaignLimits {
            records: 16,
            elapsed_ns: 60_000 * MS,
            steps: 2_000_000,
            ..CampaignLimits::default()
        },
    )
    .unwrap();
    manifest.workload.clear();
    manifest.fault_plan.clear();
    manifest.require_all_acked = true;
    manifest.topics[0].leaders = vec![manifest.brokers[0].id];
    manifest.producer.compression = Compression::None;
    manifest.producer.codec_contexts = 0;
    manifest.producer.lanes = 1;
    manifest.producer.max_attempts = 100;
    manifest.producer.delivery_timeout = RuntimeDuration::from_nanos(60_000 * MS);
    manifest.producer.topic_resolve_timeout = RuntimeDuration::from_nanos(60_000 * MS);
    manifest.producer.request_timeout = RuntimeDuration::from_nanos(200 * MS);
    manifest.producer.retry_backoff_min = RuntimeDuration::from_nanos(10 * MS);
    manifest.producer.retry_backoff_max = RuntimeDuration::from_nanos(100 * MS);
    manifest.driver.encode_bytes = 4096;
    manifest.driver.encode_cost_ns = 1000;
    manifest.driver.chunk_bytes = 4096;
    manifest.driver.pipe_bytes = 65536;
    manifest.driver.service_delay_ns = MS;
    manifest.driver.link_latency_ns = 50_000;
    manifest.driver.jitter_ns = 0;
    manifest
}

pub(crate) fn record(id: u64, topic: u32, partition: i32) -> RecordSpec {
    RecordSpec {
        id,
        topic,
        partition,
        key_routed: false,
        lane: 0,
        key: Some(id.to_be_bytes().to_vec()),
        value: Some(vec![id as u8; 160]),
        timestamp_ms: 1_000 + id as i64,
        native: id.is_multiple_of(2),
        headers: kr_kafka_sim::identity_headers(id),
    }
}

pub(crate) fn settle(count: u32, require_acked: bool) -> Workload {
    Workload::Settle {
        count,
        timeout_ns: 8_000 * MS,
        require_acked,
    }
}

pub(crate) fn run(manifest: &ReplayManifest) -> RunReport {
    kr_kafka_sim::run_replayed(manifest).unwrap_or_else(|failure| {
        panic!(
            "{failure}: {:?}",
            failure
                .history
                .entries
                .iter()
                .rev()
                .take(12)
                .collect::<Vec<_>>()
        )
    })
}
