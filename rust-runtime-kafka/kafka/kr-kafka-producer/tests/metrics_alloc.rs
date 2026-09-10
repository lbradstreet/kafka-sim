//! Isolated safe allocation probe. The dev-only dependency supplies the
//! allocator; the producer and this test keep workspace forbid(unsafe_code).
use kr_kafka_producer::telemetry::metrics::{Metric, MetricsConfig, MetricsRecorder};

#[test]
fn actual_record_registration_and_owner_handoff_do_not_allocate() {
    let config = MetricsConfig {
        max_broker_scopes: 1,
        max_partition_scopes: 2,
        significant_digits: 2,
        ..MetricsConfig::default()
    };
    let mut recorder = MetricsRecorder::new(config).unwrap();
    let reader = recorder.reader();
    reader.request_snapshot().unwrap();
    let allocations = allocation_counter::measure(|| {
        config.memory().unwrap();
        let broker = recorder.register_broker(1);
        let old = recorder.register_partition([1; 16], 0);
        let recreated = recorder.register_partition([2; 16], 0);
        for value in 0..10_000 {
            recorder.record(Metric::ProduceRttNanos, broker, value);
            recorder.record(Metric::BatchRawBytes, old, value);
            recorder.record(Metric::BatchRawBytes, recreated, value);
        }
        assert!(recorder.publish_if_requested());
    });
    assert_eq!(allocations.count_total, 0);
    assert_eq!(allocations.bytes_total, 0);
    let snapshot = reader.try_take_snapshot().unwrap();
    assert_eq!(snapshot.scopes().count(), 4);
    let reset = allocation_counter::measure(|| drop(snapshot));
    assert_eq!(reset.count_total, 0);
}

#[test]
fn saturated_large_scope_registration_reuses_first_admitted_tokens_without_allocation() {
    use kr_kafka_producer::telemetry::metrics::Scope;
    const SCOPES: usize = 2048;
    let config = MetricsConfig {
        max_partition_scopes: SCOPES as u16,
        significant_digits: 1,
        highest_duration_nanos: 64,
        highest_bytes: 64,
        highest_count: 64,
        max_storage_bytes: 128 * 1024 * 1024,
        ..MetricsConfig::default()
    };
    let mut recorder = MetricsRecorder::new(config).unwrap();
    let mut tokens = Vec::with_capacity(SCOPES);
    let reader = recorder.reader();
    let allocations = allocation_counter::measure(|| {
        for ordinal in 0..SCOPES {
            let key = (ordinal * 719) % SCOPES;
            tokens.push(recorder.register_partition((key as u128).to_be_bytes(), 0));
        }
        for ordinal in 0..SCOPES * 8 {
            let admitted = ordinal % SCOPES;
            let key = (admitted * 719) % SCOPES;
            let token = recorder.register_partition((key as u128).to_be_bytes(), 0);
            assert_eq!(token, tokens[admitted]);
            recorder.record(Metric::BatchRawBytes, token, 7);
            let omitted =
                recorder.register_partition(((SCOPES + ordinal) as u128).to_be_bytes(), 0);
            recorder.record(Metric::BatchRawBytes, omitted, 11);
        }
        reader.request_snapshot().unwrap();
        assert!(recorder.publish_if_requested());
    });
    assert_eq!(allocations.count_total, 0);
    assert_eq!(allocations.bytes_total, 0);
    let snapshot = reader.try_take_snapshot().unwrap();
    assert_eq!(snapshot.scopes().count(), SCOPES + 1);
    assert_eq!(snapshot.scope_capacity_rejections(), (SCOPES * 8) as u64);
    assert_eq!(snapshot.omitted_scope_samples(), (SCOPES * 8) as u64);
    assert_eq!(
        snapshot
            .distribution(Scope::Global, Metric::BatchRawBytes)
            .unwrap()
            .count(),
        (SCOPES * 16) as u64
    );
    for key in 0..SCOPES {
        let metric = snapshot
            .distribution(
                Scope::Partition {
                    topic_id: (key as u128).to_be_bytes(),
                    partition: 0,
                },
                Metric::BatchRawBytes,
            )
            .unwrap();
        assert_eq!(metric.count(), 8);
        assert_eq!(metric.exact_max(), Some(7));
    }
}

#[test]
fn first_owner_poll_and_first_reader_request_allocate_nothing_after_construction() {
    let mut recorder = MetricsRecorder::new(MetricsConfig::default()).unwrap();
    let reader = recorder.reader();
    let allocations = allocation_counter::measure(|| {
        assert!(!recorder.publish_if_requested());
        reader.request_snapshot().unwrap();
        assert!(recorder.publish_if_requested());
    });
    assert_eq!(allocations.count_total, 0);
    assert_eq!(allocations.bytes_total, 0);
}
