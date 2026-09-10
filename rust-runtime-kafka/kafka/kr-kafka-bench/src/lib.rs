//! Fixed offered-load timing and bounded latency aggregation shared by the host
//! benchmark. Scheduling never observes producer capacity or delivery progress.
#![forbid(unsafe_code)]
#![recursion_limit = "256"]

pub mod config;
pub mod host;

use serde::Serialize;

/// Offer `i` is due at floor(i * 1 second / rate), including offer zero at start.
#[derive(Clone, Debug)]
pub struct OfferedLoad {
    rate: u64,
    records: u64,
    next: u64,
}
impl OfferedLoad {
    pub fn new(rate: u64, records: u64) -> Result<Self, &'static str> {
        if rate == 0 || rate > 1_000_000_000 || records == 0 {
            return Err("rate must be 1..=1e9 and records must be positive");
        }
        if u128::from(records) * 1_000_000_000 / u128::from(rate) > u128::from(u64::MAX) {
            return Err("offered duration overflows nanoseconds");
        }
        Ok(Self {
            rate,
            records,
            next: 0,
        })
    }
    pub fn due_ns(&self, index: u64) -> u64 {
        (u128::from(index) * 1_000_000_000 / u128::from(self.rate)) as u64
    }
    pub fn next_due_ns(&self) -> Option<u64> {
        (self.next < self.records).then(|| self.due_ns(self.next))
    }
    /// Consume exactly one due offer. A delayed driver catches up against the
    /// original clock and records its lateness; there is no backpressure reset.
    pub fn take_due(&mut self, now_ns: u64) -> Option<(u64, u64)> {
        let due = self.next_due_ns()?;
        if due > now_ns {
            return None;
        }
        let index = self.next;
        self.next += 1;
        Some((index, due))
    }
}

/// Fixed 1025-counter histogram. Above 16ns, buckets have at most 6.25% relative
/// width. Quantiles report bucket upper bounds; the maximum remains exact.
#[derive(Clone, Debug)]
pub struct Histogram {
    bins: [u64; 1025],
    count: u64,
    sum: u128,
    max: u64,
}
impl Default for Histogram {
    fn default() -> Self {
        Self {
            bins: [0; 1025],
            count: 0,
            sum: 0,
            max: 0,
        }
    }
}
#[derive(Debug, Serialize)]
pub struct Distribution {
    pub count: u64,
    pub mean_ns: f64,
    pub p50_ns_upper: u64,
    pub p99_ns_upper: u64,
    pub p999_ns_upper: u64,
    pub max_ns: u64,
}
impl Histogram {
    fn bucket(value: u64) -> usize {
        if value < 16 {
            return value as usize;
        }
        let exponent = 63 - value.leading_zeros();
        let step = 1u64 << (exponent - 4);
        16 + (exponent as usize - 4) * 16 + ((value - (1u64 << exponent)) / step) as usize
    }
    fn upper(index: usize) -> u64 {
        if index < 16 {
            return index as u64;
        }
        let exponent = 4 + (index - 16) / 16;
        let sub = (index - 16) % 16;
        ((1u128 << exponent) + (sub as u128 + 1) * (1u128 << (exponent - 4)) - 1)
            .min(u128::from(u64::MAX)) as u64
    }
    pub fn record(&mut self, value: u64) {
        self.bins[Self::bucket(value)] += 1;
        self.count += 1;
        self.sum += u128::from(value);
        self.max = self.max.max(value);
    }
    fn quantile(&self, thousandths: u64) -> u64 {
        let rank = (u128::from(self.count) * u128::from(thousandths)).div_ceil(1000) as u64;
        if rank == 0 {
            return 0;
        }
        let mut cumulative = 0;
        for (index, count) in self.bins.iter().enumerate() {
            cumulative += count;
            if cumulative >= rank {
                return Self::upper(index).min(self.max);
            }
        }
        self.max
    }
    pub fn distribution(&self) -> Distribution {
        Distribution {
            count: self.count,
            mean_ns: if self.count == 0 {
                0.0
            } else {
                self.sum as f64 / self.count as f64
            },
            p50_ns_upper: self.quantile(500),
            p99_ns_upper: self.quantile(990),
            p999_ns_upper: self.quantile(999),
            max_ns: self.max,
        }
    }
}

/// Identical deterministic payload corpus for Rust, Java, and Python runners.
/// A maximum 16MiB corpus is allocated before the measured interval.
pub fn corpus(record_bytes: usize, seed: u64, incompressible: bool) -> Vec<Vec<u8>> {
    let count = (16 * 1024 * 1024 / record_bytes).clamp(1, 1024);
    let mut state = seed;
    (0..count)
        .map(|_| {
            (0..record_bytes)
                .map(|_| {
                    state = state
                        .wrapping_mul(6364136223846793005)
                        .wrapping_add(1442695040888963407);
                    if incompressible {
                        (state >> 56) as u8
                    } else {
                        b'a'
                    }
                })
                .collect()
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn offered_load_never_slows_with_rejection_or_delayed_polling() {
        for rate in [1, 3, 1_000, 999_983, 1_000_000_000] {
            let mut load = OfferedLoad::new(rate, 1000).unwrap();
            let mut last = 0;
            for index in 0..1000 {
                // A simulated application stall changes lateness only.
                let delayed = load.due_ns(index) + if index % 7 == 0 { 9_000_000_000 } else { 0 };
                let now = last.max(delayed);
                assert_eq!(
                    load.take_due(now),
                    Some((index, (index as u128 * 1_000_000_000 / rate as u128) as u64))
                );
                last = now;
            }
            assert_eq!(load.take_due(u64::MAX), None);
        }
        assert!(OfferedLoad::new(0, 1).is_err());
        assert!(OfferedLoad::new(1, u64::MAX).is_err());
    }
    #[test]
    fn histogram_bounds_are_monotonic_and_cover_full_u64() {
        let mut h = Histogram::default();
        let mut last = 0;
        for (sample, value) in (0..4096)
            .chain((0..64).map(|bit| 1u64 << bit))
            .chain([u64::MAX])
            .enumerate()
        {
            let bucket = Histogram::bucket(value);
            assert!(bucket < h.bins.len());
            assert!(Histogram::upper(bucket) >= value);
            if sample < 4096 {
                assert!(bucket >= last);
                last = bucket;
            }
            h.record(value);
        }
        assert_eq!(h.distribution().max_ns, u64::MAX);
        assert!(h.distribution().p999_ns_upper >= h.distribution().p99_ns_upper);
        assert_eq!(Histogram::default().distribution().count, 0);
    }
    #[test]
    fn payload_fixture_is_language_independent() {
        assert_eq!(
            corpus(8, 1, true)[0],
            [108, 130, 165, 98, 203, 128, 141, 16]
        );
        assert_eq!(corpus(8, 1, false)[0], b"aaaaaaaa");
    }
}
