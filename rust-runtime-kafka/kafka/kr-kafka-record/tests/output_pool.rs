use kr_kafka_record::*;
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use std::task::{Wake, Waker};

fn config(payload: u32, chunk: u32) -> BatchConfig {
    BatchConfig {
        raw_limit: payload,
        output_limit: payload,
        chunk_bytes: chunk,
        progressive_threshold: 0,
    }
}
fn record(value: &[u8]) -> OwnedRecord {
    OwnedRecord::copy_from(Record {
        timestamp: 7,
        key: None,
        value: Some(value),
        headers: &[],
    })
}
fn builder(pool: &OutputPool, config: BatchConfig, value: &[u8]) -> RecordBatchBuilder {
    let mut builder = RecordBatchBuilder::new(config, Compression::None, pool.clone()).unwrap();
    builder.push(record(value)).unwrap();
    builder.request_seal().unwrap();
    builder
}
fn finish(builder: &mut RecordBatchBuilder, codecs: &mut CodecPool) -> FinalizedBatch {
    for _ in 0..1000 {
        let progress = builder
            .progress(
                codecs,
                EncodeBudget {
                    input_bytes: 13,
                    codec_calls: 3,
                },
            )
            .unwrap();
        assert!(!progress.waiting_for_output);
        if progress.sealed {
            return builder
                .take_sealed()
                .unwrap()
                .finalize(Identity {
                    producer_id: 9,
                    producer_epoch: 0,
                    base_sequence: 0,
                })
                .unwrap();
        }
    }
    panic!("bounded builder stalled");
}
fn reap(pool: &OutputPool) -> usize {
    let mut items = 0;
    while pool.has_reclaim_work() {
        let progress = pool.reclaim_step(1);
        assert_eq!(progress.work_items, 1);
        items += 1;
        assert!(items <= 10_000);
    }
    items
}
#[derive(Default)]
struct Notifications(AtomicUsize);
impl Wake for Notifications {
    fn wake(self: Arc<Self>) {
        self.0.fetch_add(1, Ordering::Relaxed);
    }
}

#[test]
fn one_held_provider_survives_reservation_churn_without_polling_scans() {
    let cfg = config(128, 64);
    let pool = OutputPool::with_limits(4 * cfg.envelope_bytes() as usize, 4, 6).unwrap();
    let metadata = pool.metadata_capacity_bytes().unwrap();
    let mut codecs = CodecPool::new(0, ZstdConfig::default()).unwrap();
    let batch = finish(&mut builder(&pool, cfg, &[1; 96]), &mut codecs);
    assert_eq!(batch.chunks()[0].len(), BATCH_HEADER_BYTES);
    assert_eq!(batch.chunks()[0].allocation_len(), BATCH_HEADER_BYTES);
    let held = batch.chunks()[1].clone();
    let original = held.as_slice().to_vec();
    drop(batch);
    assert!(
        !pool.has_reclaim_work(),
        "provider ownership is not ready maintenance"
    );
    for turn in 0..1000 {
        let next = finish(&mut builder(&pool, cfg, &[turn as u8; 96]), &mut codecs);
        drop(next);
        let before = pool.status();
        assert_eq!(before.batches, 2);
        assert_eq!(
            pool.reclaim_step(0),
            OutputReclaimProgress {
                remaining: true,
                ..Default::default()
            }
        );
        assert_eq!(pool.status(), before, "zero budget is observational");
        assert_eq!(reap(&pool), 2, "one work item per payload allocation");
        assert_eq!(pool.status().batches, 1);
        assert_eq!(held.as_slice(), original);
        assert!(pool.status().allocated_bytes <= pool.status().capacity_bytes);
    }
    drop(held);
    assert_eq!(reap(&pool), 2);
    assert_eq!(pool.status().reserved_bytes, 0);
    assert_eq!(pool.metadata_capacity_bytes().unwrap(), metadata);
    pool.request_cache_clear();
    reap(&pool);
    assert_eq!(pool.status().allocated_bytes, 0);
}

#[test]
fn concurrent_provider_release_publishes_every_fixed_slot_and_wakes_owner() {
    let cfg = config(128, 64);
    let pool = OutputPool::with_limits(37 * cfg.envelope_bytes() as usize, 37, 74).unwrap();
    let mut codecs = CodecPool::new(0, ZstdConfig::default()).unwrap();
    for round in 0..8 {
        let wake = Arc::new(Notifications::default());
        pool.register_reclaim_waker(&Waker::from(wake.clone()));
        let mut providers = [Vec::new(), Vec::new(), Vec::new(), Vec::new()];
        for index in 0..37 {
            let batch = finish(&mut builder(&pool, cfg, &[round; 96]), &mut codecs);
            providers[index % 4].push(batch.chunks()[index % 3].clone());
            drop(batch);
        }
        assert!(!pool.has_reclaim_work());
        std::thread::scope(|scope| {
            for retained in providers {
                scope.spawn(move || drop(retained));
            }
        });
        assert!(wake.0.load(Ordering::Relaxed) > 0);
        // Merely observing status must neither free slots nor scan references.
        assert_eq!(pool.status().batches, 37);
        assert_eq!(reap(&pool), 74);
        assert_eq!(pool.status().reserved_bytes, 0);
    }
    pool.request_cache_clear();
    assert_eq!(reap(&pool), 74);
    assert_eq!(pool.status().allocated_bytes, 0);
}

#[test]
fn incompatible_cache_eviction_is_budgeted_and_preserves_partial_record_cursor() {
    let cfg = config(256, 64);
    let pool = OutputPool::with_limits(cfg.envelope_bytes() as usize, 1, 4).unwrap();
    let mut codecs = CodecPool::new(0, ZstdConfig::default()).unwrap();
    let value: Vec<_> = (0..200).map(|index| index as u8).collect();
    let old = finish(&mut builder(&pool, cfg, &value), &mut codecs);
    let expected: Vec<_> = old
        .chunks()
        .iter()
        .flat_map(|chunk| chunk.as_slice().iter().copied())
        .collect();
    drop(old);
    assert_eq!(reap(&pool), 4);
    assert_eq!(pool.status().cached_bytes, 256);
    let mut next = builder(&pool, config(256, 128), &value);
    let mut waits = 0;
    let actual = loop {
        let progress = next
            .progress(
                &mut codecs,
                EncodeBudget {
                    input_bytes: 1000,
                    codec_calls: 100,
                },
            )
            .unwrap();
        assert!(pool.status().allocated_bytes <= pool.status().capacity_bytes);
        if progress.sealed {
            break next
                .take_sealed()
                .unwrap()
                .finalize(Identity {
                    producer_id: 9,
                    producer_epoch: 0,
                    base_sequence: 0,
                })
                .unwrap();
        }
        assert!(progress.waiting_for_output);
        assert!(pool.has_reclaim_work());
        let eviction = pool.reclaim_step(1);
        assert_eq!(eviction.work_items, 1);
        assert_eq!(eviction.freed_allocated_bytes, 64);
        waits += 1;
        assert!(waits <= 4);
    };
    assert_eq!(waits, 4);
    let wire: Vec<_> = actual
        .chunks()
        .iter()
        .flat_map(|chunk| chunk.as_slice().iter().copied())
        .collect();
    assert_eq!(
        wire, expected,
        "resumed encoding must not duplicate a partial span"
    );
}

#[test]
fn released_token_does_not_keep_owner_or_registered_waker_alive() {
    let cfg = config(128, 64);
    let pool = OutputPool::with_limits(cfg.envelope_bytes() as usize, 1, 2).unwrap();
    let mut codecs = CodecPool::new(0, ZstdConfig::default()).unwrap();
    let wake = Arc::new(Notifications::default());
    pool.register_reclaim_waker(&Waker::from(wake.clone()));
    let batch = finish(&mut builder(&pool, cfg, &[7; 96]), &mut codecs);
    let retained = batch.chunks()[1].clone();
    let bytes = retained.as_slice().to_vec();
    drop(batch);
    drop(pool);
    assert_eq!(
        Arc::strong_count(&wake),
        1,
        "pool destruction detaches owner waker"
    );
    assert_eq!(retained.as_slice(), bytes);
    drop(retained);
    assert_eq!(wake.0.load(Ordering::Relaxed), 0);
}

#[test]
fn header_pressure_does_not_create_empty_reservations_that_starve_eviction() {
    let cfg = config(128, 64);
    let pool = OutputPool::with_limits(3 * cfg.envelope_bytes() as usize, 3, 6).unwrap();
    let mut codecs = CodecPool::new(0, ZstdConfig::default()).unwrap();
    let prior: Vec<_> = (0..3)
        .map(|_| finish(&mut builder(&pool, cfg, &[3; 96]), &mut codecs))
        .collect();
    drop(prior);
    reap(&pool);
    assert_eq!(pool.status().cached_bytes, 384);
    // A new size class consumes the remaining physical headroom while the old
    // cache still holds payloads. Its small sealed envelope leaves slot credit.
    let retained = finish(&mut builder(&pool, config(128, 100), &[4; 40]), &mut codecs);
    assert_eq!(pool.status().allocated_bytes, 545);
    let mut blocked = builder(&pool, config(128, 100), &[5; 40]);
    let progress = blocked
        .progress(&mut codecs, EncodeBudget::default())
        .unwrap();
    assert!(progress.waiting_for_output);
    assert_eq!(
        pool.status().batches,
        1,
        "failed activation consumes no reservation slot"
    );
    let work = pool.reclaim_step(1);
    assert_eq!(work.released_batches, 0);
    assert_eq!(work.freed_allocated_bytes, 64);
    for _ in 0..8 {
        let progress = blocked
            .progress(&mut codecs, EncodeBudget::default())
            .unwrap();
        if progress.sealed {
            break;
        }
        assert!(progress.waiting_for_output);
        assert_eq!(pool.reclaim_step(1).work_items, 1);
    }
    assert_eq!(blocked.state(), BatchState::Sealed);
    drop(retained);
}
