//! Passive compression estimates, stored with bounded partition state. They
//! influence only soft packing; neither admission nor output safety trusts them.
const SCALE: u64 = 65_536;
const MINIMUM: u64 = SCALE / 64;
const MAXIMUM: u64 = SCALE * 2;

#[derive(Clone, Copy, Debug)]
pub(crate) struct CompressionEstimate(u32);

impl Default for CompressionEstimate {
    fn default() -> Self {
        Self(SCALE as u32)
    }
}

impl CompressionEstimate {
    /// Include a 5% margin, and learn from completed payloads only. Good
    /// compression may at most double the next batch's raw target; worse
    /// compression takes effect immediately. Integer arithmetic pins replay.
    pub(crate) fn observe(&mut self, raw: u32, payload: usize) {
        if raw == 0 {
            return;
        }
        let observed = (payload as u64)
            .saturating_mul(SCALE * 105)
            .div_ceil(u64::from(raw) * 100)
            .clamp(MINIMUM, MAXIMUM);
        self.0 = observed.max(u64::from(self.0).div_ceil(2)) as u32;
    }

    pub(crate) fn wire_bytes(self, raw: u32) -> u64 {
        u64::from(raw)
            .saturating_mul(u64::from(self.0))
            .div_ceil(SCALE)
            + kr_kafka_record::BATCH_HEADER_BYTES as u64
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn entropy_changes_are_bounded_and_worse_compression_updates_immediately() {
        let mut estimate = CompressionEstimate::default();
        for _ in 0..16 {
            let previous = estimate.wire_bytes(65_536) - 61;
            estimate.observe(65_536, 64);
            let next = estimate.wire_bytes(65_536) - 61;
            assert!(next >= previous.div_ceil(2));
            assert!(next >= 1024);
        }
        estimate.observe(65_536, 65_545);
        assert!(estimate.wire_bytes(65_536) > 65_545 + 61);
    }

    #[test]
    fn generated_observations_never_make_predictions_zero_or_nonmonotonic() {
        for seed in 1..65u64 {
            let mut state = seed;
            let mut estimate = CompressionEstimate::default();
            for step in 0..128 {
                state = state.wrapping_mul(6364136223846793005).wrapping_add(1);
                let raw = (state >> 32) as u32;
                estimate.observe(raw, (state as usize) & 0xffff_ffff);
                let predicted = estimate.wire_bytes(raw);
                assert!(
                    (61..=2 * u64::from(raw) + 61).contains(&predicted),
                    "seed={seed} step={step} raw={raw} estimate={estimate:?}"
                );
                if raw < u32::MAX {
                    assert!(estimate.wire_bytes(raw + 1) >= predicted);
                }
            }
        }
    }
}
