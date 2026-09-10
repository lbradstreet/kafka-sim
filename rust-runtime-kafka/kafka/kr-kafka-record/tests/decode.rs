use kr_kafka_record::*;
fn fixture() -> Vec<u8> {
    include_str!("fixtures/java-none-0.hex")
        .trim()
        .as_bytes()
        .chunks_exact(2)
        .map(|b| u8::from_str_radix(core::str::from_utf8(b).unwrap(), 16).unwrap())
        .collect()
}
fn checksum(bytes: &mut [u8]) {
    let crc = crc32c(&bytes[21..]);
    bytes[17..21].copy_from_slice(&crc.to_be_bytes());
}
#[test]
fn java_batch_inspection_preserves_null_empty_utf8_and_timestamps() {
    let bytes = fixture();
    let batch = inspect_batch(&bytes, BatchDecodeLimits::default()).unwrap();
    assert_eq!(batch.header.record_count, 3);
    assert_eq!(
        batch.header.identity,
        Identity {
            producer_id: 42,
            producer_epoch: 3,
            base_sequence: 11
        }
    );
    let records: Vec<_> = batch
        .records()
        .collect::<core::result::Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(records[0].key, None);
    assert_eq!(records[0].value, Some(b"hello".as_slice()));
    assert_eq!(records[1].key, Some(b"".as_slice()));
    assert_eq!(records[1].value, None);
    assert_eq!(records[2].timestamp, 1_699_999_999_999);
    assert_eq!(records[2].key, Some("κλειδί".as_bytes()));
    let headers: Vec<_> = records[2]
        .headers
        .collect::<core::result::Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(headers[0].key, "trace");
    assert_eq!(headers[1].value, Some(b"".as_slice()));
    assert_eq!(headers[2].value, None);
    assert_eq!(headers[3].value, Some("β".as_bytes()));
}
#[test]
fn every_truncation_and_independent_limit_fails_before_exposing_records() {
    let bytes = fixture();
    for end in 0..bytes.len() {
        assert!(
            inspect_batch(&bytes[..end], BatchDecodeLimits::default()).is_err(),
            "end={end}"
        );
    }
    for limits in [
        BatchDecodeLimits {
            max_wire_bytes: bytes.len() - 1,
            ..Default::default()
        },
        BatchDecodeLimits {
            max_raw_bytes: bytes.len() - 62,
            ..Default::default()
        },
        BatchDecodeLimits {
            max_records: 2,
            ..Default::default()
        },
        BatchDecodeLimits {
            max_headers: 3,
            ..Default::default()
        },
        BatchDecodeLimits {
            max_field_bytes: 1,
            ..Default::default()
        },
    ] {
        assert!(matches!(
            inspect_batch(&bytes, limits),
            Err(BatchDecodeError::Limit(_))
        ));
    }
    let mut doubled = bytes.clone();
    doubled.extend_from_slice(&bytes);
    assert_eq!(
        inspect_batch(&doubled, BatchDecodeLimits::default()).unwrap_err(),
        BatchDecodeError::Length
    );
}
#[test]
fn malformed_header_count_attributes_offsets_varints_and_checksums_fail_closed() {
    let bytes = fixture();
    for (index, value, error) in [
        (16, 1, BatchDecodeError::Magic),
        (22, 1, BatchDecodeError::Attributes),
        (60, 0, BatchDecodeError::Count),
        (62, 1, BatchDecodeError::Attributes),
        (64, 2, BatchDecodeError::Offset),
        (65, 3, BatchDecodeError::Length),
    ] {
        let mut corrupt = bytes.clone();
        corrupt[index] = value;
        checksum(&mut corrupt);
        assert_eq!(
            inspect_batch(&corrupt, BatchDecodeLimits::default()).unwrap_err(),
            error,
            "index={index}"
        );
    }
    let mut corrupt = bytes.clone();
    corrupt[61] = 0x80;
    corrupt[62] = 0;
    checksum(&mut corrupt);
    assert_eq!(
        inspect_batch(&corrupt, BatchDecodeLimits::default()).unwrap_err(),
        BatchDecodeError::Varint
    );
    let mut corrupt = bytes.clone();
    corrupt[63] ^= 1;
    assert_eq!(
        inspect_batch(&corrupt, BatchDecodeLimits::default()).unwrap_err(),
        BatchDecodeError::Checksum
    );
}
#[test]
fn seeded_malformed_record_corpus_is_bounded_and_deterministic() {
    let source = fixture();
    for seed in 1u64..=2048 {
        let mut random = seed;
        let mut bytes = source.clone();
        for _ in 0..8 {
            random ^= random << 13;
            random ^= random >> 7;
            random ^= random << 17;
            let index = 21 + random as usize % (bytes.len() - 21);
            bytes[index] = (random >> 24) as u8;
        }
        checksum(&mut bytes);
        let limits = BatchDecodeLimits {
            max_raw_bytes: 512,
            max_wire_bytes: 512,
            max_records: 32,
            max_headers: 16,
            max_field_bytes: 128,
            ..Default::default()
        };
        let first = inspect_batch(&bytes, limits);
        let second = inspect_batch(&bytes, limits);
        assert_eq!(
            first.as_ref().map(|b| b.header),
            second.as_ref().map(|b| b.header),
            "seed={seed}"
        );
        if let Ok(batch) = first {
            assert!(batch.records().count() <= 32);
            for record in batch.records() {
                let record = record.unwrap();
                assert!(record.headers.count() <= 16);
            }
        }
    }
}
#[cfg(feature = "zstd")]
#[test]
fn compressed_inspection_caps_window_and_expansion_and_rejects_concatenated_frames() {
    let cfg = BatchConfig {
        raw_limit: 32768,
        output_limit: 32768,
        chunk_bytes: 4096,
        progressive_threshold: 0,
    };
    let mut codec = CodecPool::new(
        1,
        ZstdConfig {
            level: 1,
            window_log: 15,
        },
    )
    .unwrap();
    let pool = OutputPool::new(cfg.envelope_bytes() as usize).unwrap();
    let mut b = RecordBatchBuilder::new(cfg, Compression::Zstd { level: 1 }, pool).unwrap();
    let input = vec![0; 16384];
    b.push(OwnedRecord::copy_from(Record {
        timestamp: 1,
        key: None,
        value: Some(&input),
        headers: &[],
    }))
    .unwrap();
    b.request_seal().unwrap();
    while !b
        .progress(&mut codec, EncodeBudget::default())
        .unwrap()
        .sealed
    {}
    let b = b
        .take_sealed()
        .unwrap()
        .finalize(Identity {
            producer_id: 1,
            producer_epoch: 0,
            base_sequence: 0,
        })
        .unwrap();
    let bytes: Vec<_> = b
        .chunks()
        .iter()
        .flat_map(|b| b.as_slice())
        .copied()
        .collect();
    let batch = inspect_batch(&bytes, BatchDecodeLimits::default()).unwrap();
    assert_eq!(
        batch.records().next().unwrap().unwrap().value,
        Some(input.as_slice())
    );
    assert!(
        inspect_batch(
            &bytes,
            BatchDecodeLimits {
                max_raw_bytes: 32,
                ..Default::default()
            }
        )
        .is_err()
    );
    assert!(
        inspect_batch(
            &bytes,
            BatchDecodeLimits {
                max_zstd_window_log: 10,
                ..Default::default()
            }
        )
        .is_err()
    );
    let mut concatenated = bytes.clone();
    concatenated.extend_from_slice(&bytes[61..]);
    let length = (concatenated.len() - 12) as i32;
    concatenated[8..12].copy_from_slice(&length.to_be_bytes());
    checksum(&mut concatenated);
    assert_eq!(
        inspect_batch(&concatenated, BatchDecodeLimits::default()).unwrap_err(),
        BatchDecodeError::TrailingBytes
    );
}
