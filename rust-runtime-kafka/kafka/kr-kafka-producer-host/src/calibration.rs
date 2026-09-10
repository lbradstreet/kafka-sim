//! Startup-only calibration. The measured quota stays fixed for this producer.
use kr_kafka_host::SecurityError;
use kr_kafka_record::{
    BatchConfig, CodecPool, Compression, EncodeBudget, OutputPool, OwnedRecord, RecordBatchBuilder,
    SharedBytes, ZstdConfig,
};
use std::time::{Duration, Instant};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Calibration {
    pub sample_bytes: usize,
    pub elapsed: Duration,
    pub encode_bytes_per_poll: u32,
}
/// Encodes one fixed 1MiB sample using the real bounded record encoder. The
/// calibration workspace is retired before production pools are constructed.
pub fn calibrate(
    target_poll_ms: u32,
    level: u8,
    window_log: u32,
) -> Result<Calibration, SecurityError> {
    if target_poll_ms == 0 {
        return Err(SecurityError::InvalidConfig {
            field: "target_poll_ms",
        });
    }
    let sample_bytes = 1024 * 1024;
    let mut payload = Vec::new();
    payload
        .try_reserve_exact(sample_bytes)
        .map_err(|_| SecurityError::ResourceExhausted {
            resource: "calibration sample",
            limit: sample_bytes,
        })?;
    // Fixed mixed-entropy data avoids measuring only the all-zero fast path.
    let mut state = 0x8e47_99a2_491d_23b5u64;
    for index in 0..sample_bytes {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        payload.push(if index % 64 < 32 {
            (index % 23) as u8
        } else {
            state as u8
        });
    }
    let mut codecs = CodecPool::new(1, ZstdConfig { level, window_log }).map_err(codec_error)?;
    let pool = OutputPool::new(2 * sample_bytes + 4096).map_err(codec_error)?;
    let mut batch = RecordBatchBuilder::new(
        BatchConfig {
            raw_limit: (sample_bytes + 128) as u32,
            output_limit: (sample_bytes + 4096) as u32,
            chunk_bytes: 64 * 1024,
            progressive_threshold: 0,
        },
        Compression::Zstd { level },
        pool,
    )
    .map_err(codec_error)?;
    batch
        .push(OwnedRecord {
            timestamp: 0,
            key: None,
            value: Some(SharedBytes::from(payload)),
            headers: vec![],
        })
        .map_err(codec_error)?;
    batch.request_seal().map_err(codec_error)?;
    let start = Instant::now();
    loop {
        let progress = batch
            .progress(
                &mut codecs,
                EncodeBudget {
                    input_bytes: 16 * 1024,
                    codec_calls: 64,
                },
            )
            .map_err(codec_error)?;
        if progress.sealed {
            break;
        }
        if progress.waiting_for_context || progress.waiting_for_output {
            return Err(SecurityError::InvalidState);
        }
    }
    let elapsed = start.elapsed();
    Ok(Calibration {
        sample_bytes,
        elapsed,
        encode_bytes_per_poll: quota(sample_bytes, elapsed, target_poll_ms),
    })
}
fn quota(bytes: usize, elapsed: Duration, target_ms: u32) -> u32 {
    ((bytes as u128).saturating_mul(u128::from(target_ms) * 1_000_000) / elapsed.as_nanos().max(1))
        .clamp(16 * 1024, 1024 * 1024) as u32
}
fn codec_error(_: kr_kafka_record::Error) -> SecurityError {
    SecurityError::InvalidConfig {
        field: "calibration codec",
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn calibration_uses_actual_codec_and_clamps_both_extremes() {
        let measured = calibrate(2, 1, 20).unwrap();
        assert_eq!(measured.sample_bytes, 1024 * 1024);
        assert!((16 * 1024..=1024 * 1024).contains(&measured.encode_bytes_per_poll));
        assert_eq!(quota(1024 * 1024, Duration::ZERO, u32::MAX), 1024 * 1024);
        assert_eq!(quota(1024 * 1024, Duration::from_secs(100), 1), 16 * 1024);
        assert!(calibrate(0, 1, 20).is_err());
    }
}
