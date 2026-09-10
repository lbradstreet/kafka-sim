use kr_kafka_record::*;

fn identity() -> Identity {
    Identity {
        producer_id: 42,
        producer_epoch: 3,
        base_sequence: 11,
    }
}
fn config() -> BatchConfig {
    BatchConfig {
        raw_limit: 4096,
        output_limit: 4096,
        chunk_bytes: 67,
        progressive_threshold: 16,
    }
}
#[cfg(feature = "zstd")]
#[test]
fn compacted_tail_preserves_wire_bytes_and_provider_ownership_without_scratch_waits() {
    for output_limit in [4096, 8192] {
        let mut encoded = Vec::new();
        for compact in [false, true] {
            let config = BatchConfig {
                raw_limit: 4096,
                output_limit,
                chunk_bytes: 4096,
                progressive_threshold: 0,
            };
            let output = OutputPool::new(config.envelope_bytes() as usize).unwrap();
            let mut codecs = CodecPool::new(1, ZstdConfig::default()).unwrap();
            let mut builder =
                RecordBatchBuilder::new(config, Compression::Zstd { level: 1 }, output.clone())
                    .unwrap();
            if compact {
                builder.enable_tail_compaction();
            }
            builder.push(input(0, None, Some(&[7; 2048]))).unwrap();
            builder.request_seal().unwrap();
            for _ in 0..4096 {
                let done = builder
                    .progress(
                        &mut codecs,
                        EncodeBudget {
                            input_bytes: 64,
                            codec_calls: 1,
                        },
                    )
                    .unwrap();
                assert!(!done.waiting_for_output);
                assert!(output.status().allocated_bytes <= config.envelope_bytes() as usize);
                if done.sealed {
                    break;
                }
            }
            let batch = builder.take_sealed().unwrap().finalize(identity()).unwrap();
            encoded.push(flatten(&batch));
            let capacity = output.status().allocated_bytes;
            if compact && output_limit == 8192 {
                assert_eq!(capacity, batch.wire_bytes());
                assert!(capacity < 256);
            } else {
                assert_eq!(capacity, 4096 + BATCH_HEADER_BYTES);
            }
            let provider = batch.chunks()[1].clone();
            drop(batch);
            output.request_cache_clear();
            output.reclaim_step(16);
            assert_eq!(output.status().allocated_bytes, capacity);
            drop(provider);
            output.reclaim_step(16);
            assert_eq!(output.status().allocated_bytes, 0);
            assert_eq!(output.status().reserved_bytes, 0);
        }
        assert_eq!(encoded[0], encoded[1]);
    }
}
fn input(timestamp: i64, key: Option<&[u8]>, value: Option<&[u8]>) -> OwnedRecord {
    OwnedRecord::copy_from(Record {
        timestamp,
        key,
        value,
        headers: &[],
    })
}
fn canonical(case: usize) -> Vec<OwnedRecord> {
    match case {
        0 => vec![
            input(1_700_000_000_000, None, Some(b"hello")),
            input(1_700_000_000_064, Some(b""), None),
            OwnedRecord::copy_from(Record {
                timestamp: 1_699_999_999_999,
                key: Some("κλειδί".as_bytes()),
                value: Some(&[0, 255, 127]),
                headers: &[
                    Header {
                        key: "trace",
                        value: Some("α".as_bytes()),
                    },
                    Header {
                        key: "empty",
                        value: Some(b""),
                    },
                    Header {
                        key: "nil",
                        value: None,
                    },
                    Header {
                        key: "trace",
                        value: Some("β".as_bytes()),
                    },
                ],
            }),
        ],
        1 => vec![input(0, None, None), input(i64::MAX, Some(b""), Some(b""))],
        2 => vec![
            input(23, Some(b"a"), Some(&[0; 64])),
            input(0, Some(b"a"), Some(&[0; 128])),
        ],
        _ => unreachable!(),
    }
}
fn fixture(case: usize) -> Vec<u8> {
    let text = match case {
        0 => include_str!("fixtures/java-none-0.hex"),
        1 => include_str!("fixtures/java-none-1.hex"),
        2 => include_str!("fixtures/java-none-2.hex"),
        _ => unreachable!(),
    };
    text.trim()
        .as_bytes()
        .chunks_exact(2)
        .map(|b| u8::from_str_radix(core::str::from_utf8(b).unwrap(), 16).unwrap())
        .collect()
}
fn flatten(batch: &FinalizedBatch) -> Vec<u8> {
    batch
        .chunks()
        .iter()
        .flat_map(|b| b.as_slice().iter().copied())
        .collect()
}
fn build(
    records: Vec<OwnedRecord>,
    compression: Compression,
    quota: usize,
    chunk_bytes: u32,
) -> FinalizedBatch {
    let c = BatchConfig {
        chunk_bytes,
        ..config()
    };
    let output = OutputPool::new(c.envelope_bytes() as usize).unwrap();
    let mut codecs = CodecPool::new(
        usize::from(compression != Compression::None),
        ZstdConfig::default(),
    )
    .unwrap();
    let mut builder = RecordBatchBuilder::new(c, compression, output.clone()).unwrap();
    for record in records {
        builder.push(record).unwrap();
    }
    builder.request_seal().unwrap();
    let mut released = 0;
    for _ in 0..100_000 {
        let result = builder
            .progress(
                &mut codecs,
                EncodeBudget {
                    input_bytes: quota,
                    codec_calls: 3,
                },
            )
            .unwrap();
        assert!(result.input_bytes <= quota);
        assert!(result.codec_calls <= 3);
        released += result.records_released;
        if result.sealed {
            break;
        }
    }
    assert_eq!(builder.state(), BatchState::Sealed);
    assert_eq!(released, builder.record_count() as u32);
    assert_eq!(codecs.status().capacity, codecs.status().available);
    let batch = builder.take_sealed().unwrap().finalize(identity()).unwrap();
    assert!(builder.take_sealed().is_none());
    assert!(output.status().allocated_bytes <= c.envelope_bytes() as usize);
    batch
}
#[test]
fn independent_java_magic2_fixtures_across_chunk_and_work_boundaries() {
    for case in 0..3 {
        for quota in [1, 3, 64, 4096] {
            for chunks in [61, 67, 128, 4096] {
                let batch = build(canonical(case), Compression::None, quota, chunks);
                assert_eq!(
                    flatten(&batch),
                    fixture(case),
                    "case={case} quota={quota} chunks={chunks}"
                );
            }
        }
    }
}
#[test]
fn crc_castagnoli_known_vector_and_mutation() {
    assert_eq!(crc32c(b"123456789"), 0xe3069283);
    assert_eq!(crc32c(b""), 0);
    for case in 0..3 {
        let mut bytes = fixture(case);
        let crc = u32::from_be_bytes(bytes[17..21].try_into().unwrap());
        assert_eq!(crc32c(&bytes[21..]), crc);
        let n = bytes.len();
        bytes[n - 1] ^= 1;
        assert_ne!(crc32c(&bytes[21..]), crc);
    }
}
#[test]
fn deferred_input_release_and_progressive_quanta() {
    let c = BatchConfig {
        progressive_threshold: 64,
        ..config()
    };
    let out = OutputPool::new(c.envelope_bytes() as usize).unwrap();
    let mut codecs = CodecPool::new(0, ZstdConfig::default()).unwrap();
    let bytes = SharedBytes::from(vec![9; 32]);
    let mut builder = RecordBatchBuilder::new(c, Compression::None, out.clone()).unwrap();
    builder
        .push(OwnedRecord {
            timestamp: 1,
            key: None,
            value: Some(bytes.clone()),
            headers: vec![],
        })
        .unwrap();
    assert_eq!(bytes.strong_count(), 2);
    assert_eq!(
        builder
            .progress(&mut codecs, EncodeBudget::default())
            .unwrap()
            .input_bytes,
        0
    );
    reclaim(&out);
    assert_eq!(out.status().reserved_bytes, 0);
    builder.push(input(2, None, Some(&[8; 64]))).unwrap();
    let progress = builder
        .progress(
            &mut codecs,
            EncodeBudget {
                input_bytes: 8,
                codec_calls: 3,
            },
        )
        .unwrap();
    assert!(progress.input_bytes <= 8);
    assert_eq!(progress.records_released, 0);
    assert_eq!(bytes.strong_count(), 2);
    let progress = builder
        .progress(&mut codecs, EncodeBudget::default())
        .unwrap();
    assert_eq!(progress.records_released, 2);
    assert_eq!(bytes.strong_count(), 1);
    assert_eq!(builder.state(), BatchState::Progressive);
    assert!(out.status().allocated_bytes > 0);
    drop(builder);
    reclaim(&out);
    assert_eq!(out.status().reserved_bytes, 0);
}
#[test]
fn output_reservation_survives_batch_drop_until_provider_release() {
    let c = config();
    let out = OutputPool::new(c.envelope_bytes() as usize).unwrap();
    let mut codecs = CodecPool::new(0, ZstdConfig::default()).unwrap();
    let mut builder = RecordBatchBuilder::new(c, Compression::None, out.clone()).unwrap();
    builder.push(input(0, None, Some(&[9; 100]))).unwrap();
    builder.request_seal().unwrap();
    while !builder
        .progress(&mut codecs, EncodeBudget::default())
        .unwrap()
        .sealed
    {}
    let batch = builder.take_sealed().unwrap().finalize(identity()).unwrap();
    let retained = batch.chunks()[1].clone();
    let original = retained.as_slice().to_vec();
    let allocated = out.status().allocated_bytes;
    drop(batch);
    assert_eq!(out.status().reserved_bytes, allocated);
    let mut next = RecordBatchBuilder::new(c, Compression::None, out.clone()).unwrap();
    next.push(input(0, None, Some(&[8; 100]))).unwrap();
    assert!(
        next.progress(&mut codecs, EncodeBudget::default())
            .unwrap()
            .waiting_for_output
    );
    assert_eq!(retained.as_slice(), original);
    drop(retained);
    reclaim(&out);
    assert_eq!(out.status().reserved_bytes, 0);
    assert!(
        !next
            .progress(&mut codecs, EncodeBudget::default())
            .unwrap()
            .waiting_for_output
    );
    drop(next);
    reclaim(&out);
    assert_eq!(out.status().reserved_bytes, 0);
}
#[test]
fn refinalization_is_atomic_requires_unshared_header_and_preserves_payload() {
    let mut batch = build(canonical(0), Compression::None, 16, 67);
    let original = flatten(&batch);
    let external = batch.chunks()[0].clone();
    let next = Identity {
        producer_epoch: 4,
        base_sequence: 0,
        ..identity()
    };
    assert_eq!(batch.refinalize(next), Err(Error::SharedOutput));
    assert_eq!(flatten(&batch), original);
    drop(external);
    batch.refinalize(next).unwrap();
    let updated = flatten(&batch);
    assert_eq!(&updated[61..], &original[61..]);
    assert_eq!(&updated[43..51], &original[43..51]);
    assert_eq!(
        u32::from_be_bytes(updated[17..21].try_into().unwrap()),
        crc32c(&updated[21..])
    );
    batch.refinalize(next).unwrap();
    assert_eq!(flatten(&batch), updated);
    batch.mark_transmitted();
    assert_eq!(batch.refinalize(identity()), Err(Error::Transmitted));
    assert_eq!(flatten(&batch), updated);
}
#[test]
fn invalid_descriptors_raw_limit_and_failed_output_release_all_owners() {
    let mut c = config();
    c.raw_limit = 32;
    c.progressive_threshold = 0;
    let out = OutputPool::new(c.envelope_bytes() as usize).unwrap();
    let mut codecs = CodecPool::new(0, ZstdConfig::default()).unwrap();
    let mut b = RecordBatchBuilder::new(c, Compression::None, out.clone()).unwrap();
    assert_eq!(b.request_seal(), Err(Error::EmptyBatch));
    assert_eq!(
        b.push(input(0, None, Some(&[0; 32]))),
        Err(Error::RawTooLarge)
    );
    assert_eq!(b.record_count(), 0);
    let bad = OwnedRecord {
        timestamp: 0,
        key: None,
        value: None,
        headers: vec![OwnedHeader {
            key: SharedBytes::from(vec![255]),
            value: None,
        }],
    };
    assert_eq!(b.push(bad), Err(Error::InvalidConfig));
    b.push(input(i64::MIN, None, None)).unwrap();
    assert_eq!(
        b.push(input(i64::MAX, None, None)),
        Err(Error::LengthOverflow)
    );
    assert_eq!(b.record_count(), 1);
    let c = BatchConfig {
        output_limit: 4,
        chunk_bytes: 61,
        ..config()
    };
    let out = OutputPool::new(c.envelope_bytes() as usize).unwrap();
    let bytes = SharedBytes::from(vec![0; 24]);
    let mut b = RecordBatchBuilder::new(c, Compression::None, out.clone()).unwrap();
    b.push(OwnedRecord {
        timestamp: 0,
        key: None,
        value: Some(bytes.clone()),
        headers: vec![],
    })
    .unwrap();
    b.request_seal().unwrap();
    assert_eq!(
        b.progress(&mut codecs, EncodeBudget::default()),
        Err(Error::CompressedTooLarge)
    );
    assert_eq!(b.state(), BatchState::Failed);
    assert_eq!(bytes.strong_count(), 1);
    reclaim(&out);
    assert_eq!(out.status().reserved_bytes, 0);
    assert_eq!(
        b.progress(&mut codecs, EncodeBudget::default()),
        Err(Error::Failed)
    );
}

#[cfg(feature = "zstd")]
#[test]
fn streaming_zstd_decompresses_to_java_records_with_unknown_content_size() {
    for case in 0..3 {
        let expected = fixture(case);
        let mut previous = None;
        for quota in [1, 7, 4096] {
            for chunk in [61, 67, 4096] {
                let batch = build(
                    canonical(case),
                    Compression::Zstd { level: 1 },
                    quota,
                    chunk,
                );
                let bytes = flatten(&batch);
                assert_eq!(bytes[22] & 7, 4);
                assert_eq!(
                    crc32c(&bytes[21..]),
                    u32::from_be_bytes(bytes[17..21].try_into().unwrap())
                );
                assert_eq!(
                    zstd_safe::get_frame_content_size(&bytes[61..]).unwrap(),
                    None
                );
                let mut plain = vec![0; 4096];
                let n = zstd_safe::DCtx::create()
                    .decompress(&mut plain[..], &bytes[61..])
                    .unwrap();
                assert_eq!(&plain[..n], &expected[61..]);
                if let Some(previous) = &previous {
                    assert_eq!(&bytes, previous, "quotas must not flush frames");
                }
                previous = Some(bytes);
            }
        }
    }
}
#[cfg(feature = "zstd")]
#[test]
fn context_pool_contention_release_and_output_overflow() {
    let c = BatchConfig {
        progressive_threshold: 0,
        ..config()
    };
    let out = OutputPool::new(3 * c.envelope_bytes() as usize).unwrap();
    let mut codecs = CodecPool::new(1, ZstdConfig::default()).unwrap();
    assert!(codecs.status().workspace_bytes > 0);
    let mut first =
        RecordBatchBuilder::new(c, Compression::Zstd { level: 1 }, out.clone()).unwrap();
    first.push(input(0, None, Some(&[0; 32]))).unwrap();
    first
        .progress(&mut codecs, EncodeBudget::default())
        .unwrap();
    assert_eq!(codecs.status().available, 0);
    let mut second =
        RecordBatchBuilder::new(c, Compression::Zstd { level: 1 }, out.clone()).unwrap();
    second.push(input(0, None, Some(&[0; 32]))).unwrap();
    assert!(
        second
            .progress(&mut codecs, EncodeBudget::default())
            .unwrap()
            .waiting_for_context
    );
    drop(first);
    assert_eq!(codecs.status().available, 1);
    second.request_seal().unwrap();
    while !second
        .progress(&mut codecs, EncodeBudget::default())
        .unwrap()
        .sealed
    {}
    assert_eq!(codecs.status().available, 1);
    drop(second);
    reclaim(&out);
    assert_eq!(out.status().reserved_bytes, 0);
    let c = BatchConfig {
        raw_limit: 128,
        output_limit: 24,
        chunk_bytes: 61,
        progressive_threshold: 0,
    };
    let mut third =
        RecordBatchBuilder::new(c, Compression::Zstd { level: 1 }, out.clone()).unwrap();
    let values: Vec<_> = (0..100).map(|n| n as u8).collect();
    third.push(input(0, None, Some(&values))).unwrap();
    third.request_seal().unwrap();
    assert_eq!(
        third.progress(&mut codecs, EncodeBudget::default()),
        Err(Error::CompressedTooLarge)
    );
    assert_eq!(codecs.status().available, 1);
    reclaim(&out);
    assert_eq!(out.status().reserved_bytes, 0);
}

// A deliberately separate record writer is the semantic model. It builds a
// plaintext image only in tests and uses a different signed-integer algorithm.
fn model_varint(value: i64, out: &mut Vec<u8>) {
    let mut n = if value >= 0 {
        (value as u64) * 2
    } else {
        value.unsigned_abs().wrapping_mul(2).wrapping_sub(1)
    };
    loop {
        let rest = n / 128;
        out.push((n % 128) as u8 | if rest != 0 { 128 } else { 0 });
        if rest == 0 {
            break;
        }
        n = rest;
    }
}
fn model_optional(value: Option<&[u8]>, out: &mut Vec<u8>) {
    match value {
        None => model_varint(-1, out),
        Some(value) => {
            model_varint(value.len() as i64, out);
            out.extend(value);
        }
    }
}
fn model_records(records: &[OwnedRecord]) -> Vec<u8> {
    let mut output = vec![];
    let base = records[0].timestamp;
    for (i, record) in records.iter().enumerate() {
        let mut body = vec![0];
        model_varint(record.timestamp - base, &mut body);
        model_varint(i as i64, &mut body);
        model_optional(record.key.as_ref().map(SharedBytes::as_slice), &mut body);
        model_optional(record.value.as_ref().map(SharedBytes::as_slice), &mut body);
        model_varint(record.headers.len() as i64, &mut body);
        for header in &record.headers {
            model_optional(Some(header.key.as_slice()), &mut body);
            model_optional(header.value.as_ref().map(SharedBytes::as_slice), &mut body);
        }
        model_varint(body.len() as i64, &mut output);
        output.extend(body);
    }
    output
}
#[test]
fn seeded_record_model_and_resource_conservation() {
    for seed in 1u64..=64 {
        let mut random = seed;
        let mut draw = || {
            random ^= random << 13;
            random ^= random >> 7;
            random ^= random << 17;
            random
        };
        let mut records = vec![];
        for _ in 0..16 {
            let key: Vec<_> = (0..draw() % 17).map(|_| draw() as u8).collect();
            let value: Vec<_> = (0..draw() % 128).map(|_| draw() as u8).collect();
            let mut record = input(
                (draw() % 1024) as i64,
                if draw() % 3 == 0 { None } else { Some(&key) },
                if draw() % 3 == 0 { None } else { Some(&value) },
            );
            if draw() % 2 == 0 {
                record.headers.push(OwnedHeader {
                    key: SharedBytes::from(b"header".to_vec()),
                    value: Some(SharedBytes::from(value)),
                });
            }
            records.push(record);
        }
        let expected = model_records(&records);
        let batch = build(
            records,
            Compression::None,
            1 + (seed % 63) as usize,
            61 + (seed % 35) as u32,
        );
        assert_eq!(&flatten(&batch)[61..], expected, "seed={seed}");
        assert_eq!(batch.raw_bytes() as usize, expected.len());
    }
}

#[test]
fn finalized_allocations_are_reused_only_after_every_provider_reference_retires() {
    let c = config();
    let out = OutputPool::new(c.envelope_bytes() as usize).unwrap();
    let mut codec = CodecPool::new(0, ZstdConfig::default()).unwrap();
    let finish = |out: OutputPool, codec: &mut CodecPool, value: u8| {
        let mut b = RecordBatchBuilder::new(c, Compression::None, out).unwrap();
        b.push(input(0, None, Some(&[value; 100]))).unwrap();
        b.request_seal().unwrap();
        while !b.progress(codec, EncodeBudget::default()).unwrap().sealed {}
        b.take_sealed().unwrap().finalize(identity()).unwrap()
    };
    let first = finish(out.clone(), &mut codec, 9);
    let retained = first.chunks()[0].clone();
    let old_pointers: Vec<_> = first.chunks().iter().map(SharedBytes::as_ptr).collect();
    drop(first);
    assert_eq!(out.status().cached_bytes, 0);
    assert_eq!(retained.as_slice()[16], 2);
    drop(retained);
    reclaim(&out);
    let status = out.status();
    assert_eq!(status.reserved_bytes, 0);
    assert!(status.cached_bytes > 0);
    assert!(status.allocated_bytes <= status.capacity_bytes);
    let next = finish(out.clone(), &mut codec, 7);
    assert!(
        next.chunks()[1..]
            .iter()
            .all(|c| old_pointers.contains(&c.as_ptr()))
    );
    drop(next);
    reclaim(&out);
    let status = out.status();
    assert_eq!(status.reserved_bytes, 0);
    assert!(status.allocated_bytes <= status.capacity_bytes);
}

#[test]
fn cooperative_abort_bounds_input_and_output_release_in_every_payload_state() {
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };
    struct Release(Arc<AtomicUsize>);
    impl Drop for Release {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }
    #[cfg(not(feature = "zstd"))]
    let compressions = [Compression::None];
    #[cfg(feature = "zstd")]
    let compressions = [Compression::None, Compression::Zstd { level: 1 }];
    for compression in compressions {
        for state in 0..4 {
            let c = BatchConfig {
                raw_limit: 4096,
                output_limit: 4096,
                chunk_bytes: 61,
                progressive_threshold: 0,
            };
            let out = OutputPool::new(c.envelope_bytes() as usize).unwrap();
            let mut codecs = CodecPool::new(
                usize::from(compression != Compression::None),
                ZstdConfig::default(),
            )
            .unwrap();
            let releases = Arc::new(AtomicUsize::new(0));
            let mut builder = RecordBatchBuilder::new(c, compression, out.clone()).unwrap();
            for index in 0..33 {
                let bytes = SharedBytes::from(vec![index; 17])
                    .attach_guard(Arc::new(Release(releases.clone())))
                    .unwrap();
                builder
                    .push(OwnedRecord {
                        timestamp: 0,
                        key: None,
                        value: Some(bytes),
                        headers: vec![],
                    })
                    .unwrap();
            }
            let mut provider = None;
            if state == 1 {
                builder
                    .progress_retained(
                        &mut codecs,
                        EncodeBudget {
                            input_bytes: 3,
                            codec_calls: 1,
                        },
                    )
                    .unwrap();
            } else if state >= 2 {
                builder.request_seal().unwrap();
                for _ in 0..4096 {
                    if builder
                        .progress_retained(
                            &mut codecs,
                            EncodeBudget {
                                input_bytes: 31,
                                codec_calls: 2,
                            },
                        )
                        .unwrap()
                        .sealed
                    {
                        break;
                    }
                }
                assert_eq!(builder.state(), BatchState::Sealed);
            }
            let mut abort = if state < 2 {
                builder.into_abort()
            } else {
                let sealed = builder.take_sealed().unwrap();
                if state == 2 {
                    sealed.into_abort()
                } else {
                    let finalized = sealed.finalize(identity()).unwrap();
                    provider = Some(finalized.chunks()[0].clone());
                    finalized.into_abort()
                }
            };
            let before = releases.load(Ordering::SeqCst);
            let workspace = codecs.status();
            let output = out.status();
            let zero = abort.abort_step(0, 0);
            assert_eq!(zero.records_released, 0);
            assert_eq!(zero.chunks_released, 0);
            assert!(!zero.done);
            assert_eq!(releases.load(Ordering::SeqCst), before);
            assert_eq!(codecs.status(), workspace);
            assert_eq!(out.status(), output);
            let mut done = false;
            for _ in 0..128 {
                let before = releases.load(Ordering::SeqCst);
                let progress = abort.abort_step(3, 2);
                assert!(progress.records_released <= 3);
                assert!(progress.chunks_released <= 2);
                assert_eq!(
                    releases.load(Ordering::SeqCst) - before,
                    progress.records_released
                );
                assert_eq!(codecs.status().available, codecs.status().capacity);
                if progress.done {
                    done = true;
                    break;
                }
                assert!(progress.records_released != 0 || progress.chunks_released != 0);
            }
            assert!(done, "state={state} compression={compression:?}");
            assert_eq!(releases.load(Ordering::SeqCst), 33);
            if provider.is_some() {
                assert!(
                    out.status().reserved_bytes > 0,
                    "provider still retains output"
                );
            }
            drop(provider);
            reclaim(&out);
            assert_eq!(out.status().reserved_bytes, 0);
            assert_eq!(
                abort.abort_step(3, 2),
                kr_kafka_record::AbortProgress {
                    done: true,
                    ..Default::default()
                }
            );
        }
    }
}

#[test]
fn retained_encoding_failure_keeps_unconsumed_inputs_for_budgeted_abort() {
    let c = BatchConfig {
        raw_limit: 4096,
        output_limit: 1,
        chunk_bytes: 61,
        progressive_threshold: 0,
    };
    let out = OutputPool::new(c.envelope_bytes() as usize).unwrap();
    let mut codecs = CodecPool::new(0, ZstdConfig::default()).unwrap();
    let bytes = SharedBytes::from(vec![7; 17]);
    let mut builder = RecordBatchBuilder::new(c, Compression::None, out.clone()).unwrap();
    for _ in 0..33 {
        builder
            .push(OwnedRecord {
                timestamp: 0,
                key: None,
                value: Some(bytes.clone()),
                headers: vec![],
            })
            .unwrap();
    }
    builder.request_seal().unwrap();
    assert_eq!(
        builder.progress_retained(&mut codecs, EncodeBudget::default()),
        Err(Error::CompressedTooLarge)
    );
    assert_eq!(builder.state(), BatchState::Failed);
    assert_eq!(builder.retained_records(), 33);
    assert_eq!(bytes.strong_count(), 34);
    assert!(out.status().reserved_bytes > 0);
    let mut abort = builder.into_abort();
    assert_eq!(abort.abort_step(1, 0).records_released, 1);
    assert_eq!(bytes.strong_count(), 33);
    assert!(out.status().reserved_bytes > 0);
    for _ in 0..32 {
        abort.abort_step(1, 1);
    }
    assert!(abort.abort_step(0, 0).done);
    assert_eq!(bytes.strong_count(), 1);
    reclaim(&out);
    assert_eq!(out.status().reserved_bytes, 0);
}

fn reclaim(pool: &OutputPool) {
    while pool.has_reclaim_work() {
        let progress = pool.reclaim_step(3);
        assert!((1..=3).contains(&progress.work_items));
    }
}

#[cfg(feature = "zstd")]
#[test]
fn seal_calls_report_actual_zero_input_compressor_work_only() {
    let c = config();
    let output = OutputPool::new(c.envelope_bytes() as usize).unwrap();
    let mut codecs = CodecPool::new(1, ZstdConfig::default()).unwrap();
    let mut builder = RecordBatchBuilder::new(c, Compression::Zstd { level: 1 }, output).unwrap();
    builder.push(input(0, None, Some(&[7; 256]))).unwrap();
    while builder.retained_records() != 0 {
        let progress = builder
            .progress(
                &mut codecs,
                EncodeBudget {
                    input_bytes: 13,
                    codec_calls: 1,
                },
            )
            .unwrap();
        assert_eq!(progress.seal_calls, 0);
        assert_eq!(progress.codec_calls, 1);
    }
    builder.request_seal().unwrap();
    let idle = builder
        .progress(
            &mut codecs,
            EncodeBudget {
                input_bytes: 0,
                codec_calls: 1,
            },
        )
        .unwrap();
    assert_eq!(idle.seal_calls, 0);
    assert_eq!(idle.codec_calls, 0);
    let mut calls = 0;
    for _ in 0..100 {
        let progress = builder
            .progress(
                &mut codecs,
                EncodeBudget {
                    input_bytes: 13,
                    codec_calls: 1,
                },
            )
            .unwrap();
        assert_eq!(progress.input_bytes, 0);
        assert_eq!(progress.seal_calls, 1);
        assert_eq!(progress.codec_calls, progress.seal_calls);
        calls += progress.seal_calls;
        if progress.sealed {
            break;
        }
    }
    assert!(calls > 0);
    assert_eq!(builder.state(), BatchState::Sealed);
    assert_eq!(
        builder
            .progress(&mut codecs, EncodeBudget::default())
            .unwrap()
            .seal_calls,
        0
    );
}
