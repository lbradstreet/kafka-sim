use super::*;

fn small() -> MetricsConfig {
    MetricsConfig {
        significant_digits: 2,
        highest_duration_nanos: 1_000_000,
        highest_bytes: 1_000_000,
        highest_count: 1_000,
        ..MetricsConfig::default()
    }
}
fn snapshot(recorder: &mut MetricsRecorder) -> MetricsSnapshot {
    let reader = recorder.reader();
    reader.request_snapshot().unwrap();
    assert!(recorder.publish_if_requested());
    reader.try_take_snapshot().unwrap()
}

#[test]
fn public_metrics_container_backing_is_exact_for_all_three_banks() {
    let config = MetricsConfig {
        max_broker_scopes: 2,
        max_partition_scopes: 3,
        ..small()
    };
    let recorder = MetricsRecorder::new(config).unwrap();
    let slots = config.slots().unwrap();
    let exchange = lock(recorder.exchange.as_ref().unwrap());
    let mut bank_bytes = 0;
    for bank in [
        recorder.active.as_ref().unwrap(),
        exchange.spare[0].as_ref().unwrap(),
        exchange.spare[1].as_ref().unwrap(),
    ] {
        assert_eq!(bank.labels.capacity(), slots);
        assert_eq!(bank.distributions.capacity(), slots * Metric::COUNT);
        bank_bytes += bank.labels.capacity() * size_of::<Option<Scope>>()
            + bank.distributions.capacity() * size_of::<Distribution>();
    }
    assert_eq!(recorder.scopes.capacity(), slots);
    let actual_metadata = bank_bytes
        + ScopeIndex::configured_storage_bytes(recorder.scopes.capacity()).unwrap()
        + size_of::<Exchange>()
        + size_of::<Bank>();
    assert_eq!(actual_metadata, config.memory().unwrap().fixed_metadata);
    // The histogram's private counter Vec is deliberately absent: logical
    // distinct_values() cannot prove its retained allocation capacity.
}

#[test]
fn sorted_reference_samples_match_quantile_equivalent_ranges() {
    for seed in 0..64u64 {
        let mut recorder = MetricsRecorder::new(small()).unwrap();
        let mut values = vec![0, 1, 2, 999_999, 1_000_000];
        let mut random = seed;
        for _ in 0..128 {
            random = random.wrapping_mul(6364136223846793005).wrapping_add(1);
            values.push(random % 1_000_001);
        }
        for &value in &values {
            recorder.record(Metric::ProduceRttNanos, ScopeToken::GLOBAL, value);
        }
        values.sort_unstable();
        let snapshot = snapshot(&mut recorder);
        let d = snapshot
            .distribution(Scope::Global, Metric::ProduceRttNanos)
            .unwrap();
        assert_eq!(d.count(), values.len() as u64);
        assert_eq!(d.exact_max(), values.last().copied());
        for q in [0, 1, 100_000, 500_000, 990_000, 999_000, 1_000_000] {
            let index = ((values.len() as u128 * u128::from(q))
                .div_ceil(1_000_000)
                .max(1)
                - 1) as usize;
            let range = d.quantile(q).unwrap();
            assert!(
                (range.low..=range.high).contains(&values[index]),
                "seed={seed} q={q} range={range:?}"
            );
        }
        assert!(d.quantile(1_000_001).is_none());
        assert_eq!(d.buckets().map(|(_, count)| count).sum::<u64>(), d.count());
    }
}

#[test]
fn retained_banks_backpressure_only_snapshots_and_reuse_clears_intervals() {
    let mut recorder = MetricsRecorder::new(small()).unwrap();
    let reader = recorder.reader();
    recorder.record(Metric::BatchRawBytes, ScopeToken::GLOBAL, 10);
    let first = snapshot(&mut recorder);
    let second = snapshot(&mut recorder);
    assert_eq!(reader.request_snapshot(), Err(SnapshotError::NoSpareBank));
    for n in 0..1_000 {
        recorder.record(Metric::BatchRawBytes, ScopeToken::GLOBAL, n);
    }
    assert_eq!(first.epoch(), 1);
    assert_eq!(
        first
            .distribution(Scope::Global, Metric::BatchRawBytes)
            .unwrap()
            .count(),
        1
    );
    drop(second);
    let third = snapshot(&mut recorder);
    assert_eq!(third.epoch(), 3);
    assert_eq!(
        third
            .distribution(Scope::Global, Metric::BatchRawBytes)
            .unwrap()
            .count(),
        1_000
    );
    drop(first);
    let fourth = snapshot(&mut recorder);
    assert_eq!(
        fourth
            .distribution(Scope::Global, Metric::BatchRawBytes)
            .unwrap()
            .count(),
        0
    );
}

#[test]
fn closed_owner_preserves_published_and_final_intervals_without_owner_cycle() {
    let mut recorder = MetricsRecorder::new(small()).unwrap();
    let reader = recorder.reader();
    let weak = Arc::downgrade(recorder.exchange.as_ref().unwrap());
    recorder.record(Metric::InFlightRequests, ScopeToken::GLOBAL, 1);
    reader.request_snapshot().unwrap();
    assert!(recorder.publish_if_requested());
    recorder.record(Metric::InFlightRequests, ScopeToken::GLOBAL, 2);
    drop(recorder);
    assert!(reader.is_closed());
    let first = reader.try_take_snapshot().unwrap();
    let final_interval = reader.try_take_snapshot().unwrap();
    assert_eq!((first.epoch(), final_interval.epoch()), (1, 2));
    assert_eq!(reader.request_snapshot(), Err(SnapshotError::Closed));
    drop(reader);
    assert!(weak.upgrade().is_none());
    assert_eq!(
        final_interval
            .distribution(Scope::Global, Metric::InFlightRequests)
            .unwrap()
            .exact_max(),
        Some(2)
    );
}

#[test]
fn scope_caps_keep_global_samples_and_never_merge_recreated_topics() {
    let mut recorder = MetricsRecorder::new(MetricsConfig {
        max_broker_scopes: 1,
        max_partition_scopes: 1,
        ..small()
    })
    .unwrap();
    let old = recorder.register_partition([1; 16], 0);
    let repeated = recorder.register_partition([1; 16], 0);
    assert_eq!(old, repeated);
    let recreated = recorder.register_partition([2; 16], 0);
    let broker = recorder.register_broker(7);
    for (scope, n) in [(old, 1), (recreated, 2), (broker, 3)] {
        recorder.record(Metric::BatchRawBytes, scope, n);
    }
    let snapshot = snapshot(&mut recorder);
    assert_eq!(snapshot.omitted_scope_samples(), 1);
    assert_eq!(snapshot.scope_capacity_rejections(), 1);
    assert_eq!(
        snapshot
            .distribution(Scope::Global, Metric::BatchRawBytes)
            .unwrap()
            .count(),
        3
    );
    assert_eq!(
        snapshot
            .distribution(
                Scope::Partition {
                    topic_id: [1; 16],
                    partition: 0
                },
                Metric::BatchRawBytes
            )
            .unwrap()
            .exact_max(),
        Some(1)
    );
    assert!(
        snapshot
            .distribution(
                Scope::Partition {
                    topic_id: [2; 16],
                    partition: 0
                },
                Metric::BatchRawBytes
            )
            .is_none()
    );
}

#[test]
fn outliers_count_overflow_and_empty_distributions_are_explicit() {
    let mut distribution = Distribution::new(small(), Metric::RecordsPerBatch).unwrap();
    assert_eq!(distribution.quantile(500_000), None);
    distribution.record(1_001);
    assert_eq!((distribution.count(), distribution.out_of_range()), (0, 1));
    distribution.histogram.record_n(3, u64::MAX).unwrap();
    distribution.record(4);
    assert_eq!(distribution.count(), u64::MAX);
    assert_eq!(distribution.count_overflow(), 1);
    assert_eq!(distribution.histogram.count_at(4), 0);
    distribution.count_overflow = u64::MAX;
    distribution.record(4);
    assert!(distribution.diagnostic_overflow());
    assert!(!distribution.histogram.is_auto_resize());
}

#[test]
fn layout_preflight_matches_pinned_hdr_without_allocating_on_validation() {
    for digits in 0..=5 {
        for high in [2, 127, 128, 255, 256, 1_000_000, u64::MAX] {
            let h = Histogram::<u64>::new_with_bounds(1, high, digits).unwrap();
            assert_eq!(config::bins(high, digits).unwrap(), h.distinct_values());
        }
    }
    let memory = MetricsConfig::default().memory().unwrap();
    assert!(memory.configured_bytes < MetricsConfig::default().max_storage_bytes);
    assert_eq!(
        memory.configured_bytes,
        memory.histogram_counts + memory.fixed_metadata
    );
    let mut config = small();
    config.max_storage_bytes = config.memory().unwrap().configured_bytes - 1;
    assert!(matches!(
        MetricsRecorder::new(config),
        Err(MetricsError::StorageLimit { .. })
    ));
    config.enabled = false;
    let recorder = MetricsRecorder::new(config).unwrap();
    assert_eq!(config.memory().unwrap(), MetricsMemory::default());
    assert!(recorder.active.is_none() && recorder.scopes.capacity() == 0);
    assert_eq!(
        recorder.reader().request_snapshot(),
        Err(SnapshotError::Disabled)
    );
}

#[test]
fn busy_and_contended_handoff_preserve_request_without_recording_lock() {
    let mut recorder = MetricsRecorder::new(small()).unwrap();
    let reader = recorder.reader();
    reader.request_snapshot().unwrap();
    assert_eq!(reader.request_snapshot(), Err(SnapshotError::Busy));
    let exchange = reader.exchange.as_ref().unwrap().clone();
    let guard = lock(&exchange);
    for n in 0..100 {
        recorder.record(Metric::InFlightRequests, ScopeToken::GLOBAL, n);
    }
    assert!(!recorder.publish_if_requested());
    drop(guard);
    assert!(recorder.publish_if_requested());
    assert_eq!(
        reader
            .try_take_snapshot()
            .unwrap()
            .distribution(Scope::Global, Metric::InFlightRequests)
            .unwrap()
            .count(),
        100
    );
}

#[test]
fn supplied_interval_times_are_monotonic_and_never_invented() {
    let mut recorder = MetricsRecorder::new(small()).unwrap();
    let reader = recorder.reader();
    let at = RuntimeInstant::from_nanos;
    recorder.observe_time(at(100));
    recorder.record_elapsed(Metric::QueueWaitNanos, ScopeToken::GLOBAL, at(100), at(130));
    reader.request_snapshot().unwrap();
    assert!(recorder.publish_at(at(150)));
    let first = reader.try_take_snapshot().unwrap();
    assert_eq!(
        first.bounds(),
        IntervalBounds {
            start: Some(at(100)),
            end: Some(at(150))
        }
    );
    recorder.observe_time(at(140));
    recorder.observe_time(at(160));
    recorder.record_elapsed(Metric::QueueWaitNanos, ScopeToken::GLOBAL, at(180), at(160));
    let second = snapshot(&mut recorder);
    assert_eq!(second.invalid_time_samples(), 2);
    assert_eq!(
        second.bounds(),
        IntervalBounds {
            start: Some(at(150)),
            end: Some(at(160))
        }
    );
    assert_eq!(
        second
            .distribution(Scope::Global, Metric::QueueWaitNanos)
            .unwrap()
            .count(),
        0
    );
    assert_eq!(
        first
            .distribution(Scope::Global, Metric::QueueWaitNanos)
            .unwrap()
            .exact_max(),
        Some(30)
    );
}

#[test]
fn exhausted_epoch_fences_only_snapshots_and_preserves_terminal_samples() {
    let mut recorder = MetricsRecorder::new(small()).unwrap();
    recorder.active.as_mut().unwrap().epoch = u64::MAX;
    let reader = recorder.reader();
    reader.request_snapshot().unwrap();
    assert!(recorder.snapshot_requested());
    assert!(!recorder.publish_if_requested());
    assert!(!recorder.snapshot_requested());
    assert_eq!(
        reader.request_snapshot(),
        Err(SnapshotError::EpochExhausted)
    );
    recorder.record(Metric::BatchWireBytes, ScopeToken::GLOBAL, 7);
    drop(recorder);
    let final_interval = reader.try_take_snapshot().unwrap();
    assert_eq!(final_interval.epoch(), u64::MAX);
    assert_eq!(
        final_interval
            .distribution(Scope::Global, Metric::BatchWireBytes)
            .unwrap()
            .count(),
        1
    );
}

#[test]
fn reader_can_hold_old_histograms_while_owner_records_on_another_thread() {
    let mut recorder = MetricsRecorder::new(small()).unwrap();
    recorder.record(Metric::RecordsPerBatch, ScopeToken::GLOBAL, 7);
    let first = snapshot(&mut recorder);
    let reader = recorder.reader();
    let barrier = std::sync::Barrier::new(2);
    std::thread::scope(|scope| {
        scope.spawn(|| {
            barrier.wait();
            for _ in 0..100 {
                assert_eq!(
                    first
                        .distribution(Scope::Global, Metric::RecordsPerBatch)
                        .unwrap()
                        .exact_max(),
                    Some(7)
                );
            }
            barrier.wait();
        });
        barrier.wait();
        for _ in 0..100 {
            recorder.record(Metric::RecordsPerBatch, ScopeToken::GLOBAL, 9);
        }
        barrier.wait();
    });
    drop(first);
    reader.request_snapshot().unwrap();
    assert!(recorder.publish_if_requested());
    let next = reader.try_take_snapshot().unwrap();
    assert_eq!(
        next.distribution(Scope::Global, Metric::RecordsPerBatch)
            .unwrap()
            .count(),
        100
    );
}

#[test]
fn global_depth_and_broker_depth_are_not_double_counted_or_conflated() {
    let mut recorder = MetricsRecorder::new(MetricsConfig {
        max_broker_scopes: 1,
        ..small()
    })
    .unwrap();
    let broker = recorder.register_broker(7);
    recorder.record(Metric::InFlightRequests, ScopeToken::GLOBAL, 5);
    recorder.record_scoped(Metric::InFlightRequests, broker, 2);
    recorder.record_scoped(Metric::InFlightRequests, ScopeToken::GLOBAL, 99);
    recorder.missing_time();
    let snapshot = snapshot(&mut recorder);
    let global = snapshot
        .distribution(Scope::Global, Metric::InFlightRequests)
        .unwrap();
    assert_eq!((global.count(), global.exact_max()), (1, Some(5)));
    let broker = snapshot
        .distribution(Scope::Broker(7), Metric::InFlightRequests)
        .unwrap();
    assert_eq!((broker.count(), broker.exact_max()), (1, Some(2)));
    assert_eq!(snapshot.missing_time_samples(), 1);
}
