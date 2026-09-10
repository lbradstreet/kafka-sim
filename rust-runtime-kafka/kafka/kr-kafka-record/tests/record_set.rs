use kr_kafka_record::*;

fn java_batch(base: i64) -> Vec<u8> {
    let mut bytes: Vec<_> = include_str!("fixtures/java-none-0.hex")
        .trim()
        .as_bytes()
        .chunks_exact(2)
        .map(|hex| u8::from_str_radix(core::str::from_utf8(hex).unwrap(), 16).unwrap())
        .collect();
    bytes[..8].copy_from_slice(&base.to_be_bytes());
    bytes[12..16].copy_from_slice(&7i32.to_be_bytes());
    bytes
}
#[cfg(feature = "zstd")]
fn zstd_batch(base: i64) -> Vec<u8> {
    let mut bytes = java_batch(base);
    let mut compressed = vec![0; zstd_safe::compress_bound(bytes.len() - 61)];
    let len = zstd_safe::compress(&mut compressed[..], &bytes[61..], 1).unwrap();
    bytes.truncate(61);
    bytes.extend_from_slice(&compressed[..len]);
    let length = (bytes.len() - 12) as i32;
    bytes[8..12].copy_from_slice(&length.to_be_bytes());
    bytes[21..23].copy_from_slice(&4i16.to_be_bytes());
    let crc = crc32c(&bytes[21..]);
    bytes[17..21].copy_from_slice(&crc.to_be_bytes());
    bytes
}

#[test]
fn canonical_batches_preserve_boundaries_offsets_and_aggregate_counts() {
    let mut wire = java_batch(7);
    wire.extend_from_slice(&java_batch(10));
    #[cfg(feature = "zstd")]
    wire.extend_from_slice(&zstd_batch(20));
    #[cfg(not(feature = "zstd"))]
    wire.extend_from_slice(&java_batch(20));
    let mut iter = RecordSetIter::new(&wire, RecordSetLimits::default()).unwrap();
    let mut offsets = Vec::new();
    let mut raw = 0;
    let mut headers = 0;
    for expected in [7, 10, 20] {
        let batch = iter.next().unwrap().unwrap();
        assert_eq!(batch.header.base_offset, expected);
        assert_eq!(batch.header.leader_epoch, 7);
        raw += batch.raw_bytes().len();
        for record in batch.records() {
            let record = record.unwrap();
            offsets.push(expected + i64::from(record.offset_delta));
            headers += record.headers.count();
        }
    }
    assert!(iter.next().is_none());
    assert_eq!(offsets, [7, 8, 9, 10, 11, 12, 20, 21, 22]);
    assert_eq!(
        iter.finish().unwrap(),
        RecordSetStats {
            batches: 3,
            wire_bytes: wire.len(),
            raw_bytes: raw,
            records: 9,
            headers,
            first_offset: Some(7),
            next_offset: Some(23),
        }
    );
    assert_eq!(
        RecordSetIter::new(&[], RecordSetLimits::default())
            .unwrap()
            .finish()
            .unwrap(),
        RecordSetStats::default()
    );
}

#[test]
fn aggregate_limits_include_later_batches_and_accept_exact_boundaries() {
    let mut wire = java_batch(0);
    wire.extend_from_slice(&java_batch(3));
    let stats = RecordSetIter::new(&wire, RecordSetLimits::default())
        .unwrap()
        .finish()
        .unwrap();
    let exact = RecordSetLimits {
        max_wire_bytes: stats.wire_bytes,
        max_raw_bytes: stats.raw_bytes,
        max_batches: stats.batches,
        max_records: stats.records,
        max_headers: stats.headers,
        ..Default::default()
    };
    assert_eq!(
        RecordSetIter::new(&wire, exact).unwrap().finish().unwrap(),
        stats
    );
    for limits in [
        RecordSetLimits {
            max_wire_bytes: exact.max_wire_bytes - 1,
            ..exact
        },
        RecordSetLimits {
            max_raw_bytes: exact.max_raw_bytes - 1,
            ..exact
        },
        RecordSetLimits {
            max_batches: exact.max_batches - 1,
            ..exact
        },
        RecordSetLimits {
            max_records: exact.max_records - 1,
            ..exact
        },
        RecordSetLimits {
            max_headers: exact.max_headers - 1,
            ..exact
        },
    ] {
        assert!(matches!(
            RecordSetIter::new(&wire, limits).and_then(RecordSetIter::finish),
            Err(BatchDecodeError::Limit(_))
        ));
    }
}

#[test]
fn malformed_tail_fuses_and_cannot_be_hidden_by_finish_or_advance_offsets() {
    let first = java_batch(0);
    let second = java_batch(3);
    for end in 1..second.len() {
        let mut wire = first.clone();
        wire.extend_from_slice(&second[..end]);
        let mut iter = RecordSetIter::new(&wire, RecordSetLimits::default()).unwrap();
        assert!(iter.next().unwrap().is_ok());
        let error = iter.next().unwrap().unwrap_err();
        assert!(iter.next().is_none());
        assert_eq!(iter.stats().records, 3);
        assert_eq!(iter.finish().unwrap_err(), error);
    }
    for mut corrupt in [java_batch(2), java_batch(-1), java_batch(i64::MAX)] {
        let mut wire = first.clone();
        wire.append(&mut corrupt);
        assert_eq!(
            RecordSetIter::new(&wire, RecordSetLimits::default())
                .unwrap()
                .finish()
                .unwrap_err(),
            BatchDecodeError::Offset
        );
    }
    for bad_length in [-1i32, 0, 48, i32::MAX] {
        let mut corrupt = second.clone();
        corrupt[8..12].copy_from_slice(&bad_length.to_be_bytes());
        assert!(
            RecordSetIter::new(&corrupt, RecordSetLimits::default())
                .unwrap()
                .finish()
                .is_err()
        );
    }
    let mut corrupt = second.clone();
    *corrupt.last_mut().unwrap() ^= 1;
    let mut wire = first;
    wire.extend_from_slice(&corrupt);
    let mut offset = 0;
    let validated = RecordSetIter::new(&wire, RecordSetLimits::default())
        .unwrap()
        .finish();
    if let Ok(stats) = validated {
        offset = stats.next_offset.unwrap();
    }
    assert_eq!(
        offset, 0,
        "a good first batch cannot authorize progress past a bad response tail"
    );
    assert_eq!(validated.unwrap_err(), BatchDecodeError::Checksum);
}

#[cfg(feature = "zstd")]
#[test]
fn zstd_expansion_and_record_fields_are_bounded_across_the_record_set() {
    let mut wire = zstd_batch(0);
    wire.extend_from_slice(&zstd_batch(3));
    for limits in [
        RecordSetLimits {
            max_raw_bytes: 1,
            ..Default::default()
        },
        RecordSetLimits {
            batch: BatchDecodeLimits {
                max_field_bytes: 1,
                ..Default::default()
            },
            ..Default::default()
        },
        RecordSetLimits {
            batch: BatchDecodeLimits {
                max_zstd_window_log: 9,
                ..Default::default()
            },
            ..Default::default()
        },
    ] {
        assert!(RecordSetIter::new(&wire, limits).unwrap().finish().is_err());
    }
    let first_len = zstd_batch(0).len();
    wire[first_len + 61] ^= 0xff;
    let crc = crc32c(&wire[first_len + 21..]);
    wire[first_len + 17..first_len + 21].copy_from_slice(&crc.to_be_bytes());
    assert_eq!(
        RecordSetIter::new(&wire, RecordSetLimits::default())
            .unwrap()
            .finish()
            .unwrap_err(),
        BatchDecodeError::Compression
    );
}
