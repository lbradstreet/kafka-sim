//! Isolate final-tail retention with identical raw batch membership. Output
//! backing/reservations exclude input, codec workspace, metadata and transport.
use kr_kafka_record::*;

fn main() -> std::result::Result<(), Box<dyn std::error::Error>> {
    println!(
        "chunk_bytes,records,value_bytes,random,compact,wire_bytes,retained_output_bytes,retained_reservation_bytes"
    );
    for chunk_bytes in [32 * 1024, 512 * 1024] {
        for count in [1, 16] {
            for random in [false, true] {
                let mut state = 0x1234_5678_9abc_def0u64;
                let values: Vec<Vec<u8>> = (0..count)
                    .map(|_| {
                        (0..2048)
                            .map(|_| {
                                state = state.wrapping_mul(6364136223846793005).wrapping_add(1);
                                if random { (state >> 32) as u8 } else { 7 }
                            })
                            .collect()
                    })
                    .collect();
                let mut original = None;
                for compact in [false, true] {
                    let config = BatchConfig {
                        raw_limit: 128 * 1024,
                        output_limit: 1024 * 1024,
                        chunk_bytes,
                        progressive_threshold: 0,
                    };
                    let output = OutputPool::new(config.envelope_bytes() as usize)?;
                    let mut codecs = CodecPool::new(1, ZstdConfig::default())?;
                    let mut builder = RecordBatchBuilder::new(
                        config,
                        Compression::Zstd { level: 1 },
                        output.clone(),
                    )?;
                    if compact {
                        builder.enable_tail_compaction();
                    }
                    for (index, value) in values.iter().enumerate() {
                        builder.push(OwnedRecord::copy_from(Record {
                            timestamp: index as i64,
                            key: None,
                            value: Some(value),
                            headers: &[],
                        }))?;
                    }
                    builder.request_seal()?;
                    for _ in 0..4096 {
                        let progress = builder.progress(
                            &mut codecs,
                            EncodeBudget {
                                input_bytes: 4096,
                                codec_calls: 8,
                            },
                        )?;
                        assert!(!progress.waiting_for_output);
                        if progress.sealed {
                            break;
                        }
                    }
                    let batch = builder
                        .take_sealed()
                        .expect("bounded completion")
                        .finalize(Identity {
                            producer_id: 42,
                            producer_epoch: 3,
                            base_sequence: 0,
                        })?;
                    let bytes: Vec<_> = batch
                        .chunks()
                        .iter()
                        .flat_map(|c| c.as_slice())
                        .copied()
                        .collect();
                    let decoded = inspect_batch(&bytes, BatchDecodeLimits::default())?;
                    assert_eq!(decoded.header.record_count as usize, count);
                    for (record, expected) in decoded.records().zip(&values) {
                        assert_eq!(record?.value, Some(expected.as_slice()));
                    }
                    if let Some(original) = &original {
                        assert_eq!(original, &bytes, "compaction changed wire bytes");
                    } else {
                        original = Some(bytes.clone());
                    }
                    let status = output.status();
                    println!(
                        "{chunk_bytes},{count},2048,{random},{compact},{},{},{}",
                        bytes.len(),
                        status.allocated_bytes,
                        status.reserved_bytes
                    );
                }
            }
        }
    }
    Ok(())
}
