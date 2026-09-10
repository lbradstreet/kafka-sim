//! Passive deadline estimates. All elapsed time is supplied by the owner:
//! runtime elapsed time on the host, modeled cost in simulation.
//!
//! The fixed-point EWMAs use a new-sample weight of 1/8 and round upward.
//! A complete encode invocation can contain input and stream-end calls. Its
//! elapsed time is apportioned by those actual call counts; this is a cost
//! estimate, not a measurement of each stage. Idle traversal is never a sample.
use kr_runtime::RuntimeDuration;
use std::fmt;

const FRACTION_BITS: u32 = 16;
const SCALE: u128 = 1 << FRACTION_BITS;

/// Actual encoder work, excluding descriptor traversal and resource waits.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct EncodeWork {
    pub raw_bytes: u32,
    pub input_calls: u32,
    pub seal_calls: u32,
    /// Completed zstd stream ends. Uncompressed seals perform no end calls.
    pub seals_completed: u32,
}
impl EncodeWork {
    #[must_use]
    pub const fn is_empty(self) -> bool {
        self.input_calls == 0 && self.seal_calls == 0
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum EstimationError {
    InvalidWork(EncodeWork),
    InvalidHeadroom { fallback: u64, maximum: u64 },
}
impl fmt::Display for EstimationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidWork(work) => write!(f, "invalid encoder timing sample: {work:?}"),
            Self::InvalidHeadroom { fallback, maximum } => write!(
                f,
                "invalid deadline headroom: fallback {fallback}ns, maximum {maximum}ns"
            ),
        }
    }
}
impl std::error::Error for EstimationError {}

/// Shared input cost and fixed stream-end cost for one producer's codec mode.
/// Pending stream-end time is bounded by the configured maximum and is consumed
/// when a seal completes. The owner must clear pending time on an aborted seal;
/// losing a partial timing sample is preferable to charging an unrelated batch.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct EncodingCost {
    input_ns_per_byte: Option<u128>,
    finish_ns: Option<u64>,
    pending_finish_ns: u64,
    maximum_ns: u64,
}
impl EncodingCost {
    #[must_use]
    pub const fn new(maximum: RuntimeDuration) -> Self {
        Self {
            input_ns_per_byte: None,
            finish_ns: None,
            pending_finish_ns: 0,
            maximum_ns: maximum.as_nanos(),
        }
    }

    /// Applies one actual encoder-work sample. Returns whether predicted cost
    /// changed. The first input and completed-seal samples initialize their
    /// respective EWMAs directly. Samples and pending end time are capped.
    ///
    /// # Errors
    /// Rejects inconsistent work counters without changing any estimator state.
    pub fn observe(
        &mut self,
        work: EncodeWork,
        elapsed: RuntimeDuration,
    ) -> Result<bool, EstimationError> {
        if (work.raw_bytes != 0 && work.input_calls == 0) || work.seals_completed > work.seal_calls
        {
            return Err(EstimationError::InvalidWork(work));
        }
        if work.is_empty() {
            return Ok(false);
        }
        let old = (self.input_ns_per_byte, self.finish_ns);
        let elapsed = elapsed.as_nanos().min(self.maximum_ns);
        let calls = u64::from(work.input_calls) + u64::from(work.seal_calls);
        // u64 duration × u32 call count fits u128. The complementary shares
        // sum to the supplied duration, with rounding assigned to stream end.
        let finish =
            (u128::from(elapsed) * u128::from(work.seal_calls)).div_ceil(u128::from(calls)) as u64;
        let input = elapsed - finish;
        if work.raw_bytes != 0 {
            let sample = (u128::from(input) * SCALE).div_ceil(u128::from(work.raw_bytes));
            self.input_ns_per_byte = Some(ewma(self.input_ns_per_byte, sample));
        }
        self.pending_finish_ns = self
            .pending_finish_ns
            .saturating_add(finish)
            .min(self.maximum_ns);
        if work.seals_completed != 0 {
            let sample = self
                .pending_finish_ns
                .div_ceil(u64::from(work.seals_completed));
            self.finish_ns = Some(ewma(self.finish_ns.map(u128::from), u128::from(sample)) as u64);
            self.pending_finish_ns = 0;
        }
        Ok(old != (self.input_ns_per_byte, self.finish_ns))
    }

    /// Discards unfinished stream-end timing after abort. Learned completed
    /// samples remain valid. This does not change existing predicted deadlines.
    pub fn reset_pending_seal(&mut self) {
        self.pending_finish_ns = 0;
    }

    #[must_use]
    pub fn estimate(self, raw_bytes: u32) -> RuntimeDuration {
        RuntimeDuration::from_nanos(
            self.estimate_ns(raw_bytes).min(u128::from(self.maximum_ns)) as u64
        )
    }
    fn estimate_ns(self, raw_bytes: u32) -> u128 {
        // The Q16 input sample is bounded by u64::MAX ns/byte. Its product
        // with a u32 batch length, plus a u64 finish term, fits u128.
        let input = (self.input_ns_per_byte.unwrap_or(0) * u128::from(raw_bytes)).div_ceil(SCALE);
        input + u128::from(self.finish_ns.unwrap_or(0))
    }
}

/// Per-broker observed response latency, from full confirmed send to a parsed
/// matching response. No sample is inferred from a successful socket write.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct RoundTripTime(Option<u64>);
impl RoundTripTime {
    /// Returns whether the estimate changed. A zero-duration modeled response
    /// is valid; the estimator never invents a host-clock observation.
    pub fn observe(&mut self, elapsed: RuntimeDuration) -> bool {
        let next = ewma(self.0.map(u128::from), u128::from(elapsed.as_nanos())) as u64;
        let changed = self.0 != Some(next);
        self.0 = Some(next);
        changed
    }
    #[must_use]
    pub const fn estimate(self) -> Option<RuntimeDuration> {
        match self.0 {
            Some(nanos) => Some(RuntimeDuration::from_nanos(nanos)),
            None => None,
        }
    }
}

/// Immutable estimate snapshot carried by an open batch. Request timeout is
/// the downstream fallback until a response is observed; delivery timeout caps
/// the total headroom. Changes never alter the record's delivery deadline.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DeadlineHeadroom {
    encoding: EncodingCost,
    round_trip: RoundTripTime,
    fallback_ns: u64,
    maximum_ns: u64,
}
impl DeadlineHeadroom {
    /// # Errors
    /// Rejects a zero maximum or a fallback larger than the maximum.
    pub const fn new(
        encoding: EncodingCost,
        round_trip: RoundTripTime,
        fallback: RuntimeDuration,
        maximum: RuntimeDuration,
    ) -> Result<Self, EstimationError> {
        if maximum.as_nanos() == 0 || fallback.as_nanos() > maximum.as_nanos() {
            return Err(EstimationError::InvalidHeadroom {
                fallback: fallback.as_nanos(),
                maximum: maximum.as_nanos(),
            });
        }
        Ok(Self {
            encoding,
            round_trip,
            fallback_ns: fallback.as_nanos(),
            maximum_ns: maximum.as_nanos(),
        })
    }
    #[must_use]
    pub fn estimate(self, raw_bytes: u32) -> RuntimeDuration {
        let rtt = self.round_trip.0.unwrap_or(self.fallback_ns);
        let nanos = self.encoding.estimate_ns(raw_bytes) + u128::from(rtt);
        RuntimeDuration::from_nanos(nanos.min(u128::from(self.maximum_ns)) as u64)
    }
}

fn ewma(previous: Option<u128>, sample: u128) -> u128 {
    previous.map_or(sample, |previous| (previous * 7 + sample).div_ceil(8))
}

#[cfg(test)]
mod tests {
    use super::*;
    fn ns(value: u64) -> RuntimeDuration {
        RuntimeDuration::from_nanos(value)
    }
    fn input(raw_bytes: u32) -> EncodeWork {
        EncodeWork {
            raw_bytes,
            input_calls: 1,
            ..EncodeWork::default()
        }
    }
    fn finish(completed: bool) -> EncodeWork {
        EncodeWork {
            seal_calls: 1,
            seals_completed: u32::from(completed),
            ..EncodeWork::default()
        }
    }
    #[test]
    fn first_sample_initializes_then_ewma_weights_new_cost_by_one_eighth() {
        let mut cost = EncodingCost::new(ns(10_000));
        assert!(cost.observe(input(100), ns(1_000)).unwrap());
        assert_eq!(cost.estimate(20), ns(200));
        cost.observe(input(100), ns(3_000)).unwrap();
        assert_eq!(cost.estimate(20), ns(250));
        let mut rtt = RoundTripTime::default();
        rtt.observe(ns(100));
        rtt.observe(ns(300));
        assert_eq!(rtt.estimate(), Some(ns(125)));
    }
    #[test]
    fn zero_byte_end_quanta_accumulate_fixed_cost_and_abort_discards_pending_work() {
        let mut cost = EncodingCost::new(ns(10_000));
        assert!(!cost.observe(finish(false), ns(100)).unwrap());
        assert!(!cost.observe(finish(false), ns(200)).unwrap());
        assert!(cost.observe(finish(true), ns(300)).unwrap());
        assert_eq!(cost.estimate(0), ns(600));
        cost.observe(finish(false), ns(9_999)).unwrap();
        cost.reset_pending_seal();
        cost.observe(finish(true), ns(80)).unwrap();
        assert_eq!(cost.estimate(0), ns(535));
    }
    #[test]
    fn mixed_work_splits_elapsed_time_without_training_on_idle_traversal() {
        let mut cost = EncodingCost::new(ns(1_000));
        cost.observe(
            EncodeWork {
                raw_bytes: 4,
                input_calls: 3,
                seal_calls: 1,
                seals_completed: 1,
            },
            ns(400),
        )
        .unwrap();
        assert_eq!(cost.estimate(4), ns(400));
        assert_eq!(cost.estimate(8), ns(700));
        let before = cost;
        assert!(!cost.observe(EncodeWork::default(), ns(u64::MAX)).unwrap());
        assert_eq!(cost, before);
        assert!(
            cost.observe(
                EncodeWork {
                    raw_bytes: 1,
                    ..EncodeWork::default()
                },
                ns(1)
            )
            .is_err()
        );
        assert_eq!(cost, before);
    }
    #[test]
    fn fallback_and_maximum_bound_all_extreme_samples_and_batch_lengths() {
        let mut cost = EncodingCost::new(ns(100));
        let unknown =
            DeadlineHeadroom::new(cost, RoundTripTime::default(), ns(40), ns(100)).unwrap();
        assert_eq!(unknown.estimate(u32::MAX), ns(40));
        for _ in 0..16 {
            cost.observe(finish(false), ns(u64::MAX)).unwrap();
        }
        cost.observe(finish(true), ns(u64::MAX)).unwrap();
        assert_eq!(cost.estimate(0), ns(100));
        cost.observe(input(1), ns(u64::MAX)).unwrap();
        let mut rtt = RoundTripTime::default();
        rtt.observe(ns(u64::MAX));
        let bounded = DeadlineHeadroom::new(cost, rtt, ns(40), ns(100)).unwrap();
        assert_eq!(bounded.estimate(u32::MAX), ns(100));
        assert!(DeadlineHeadroom::new(cost, rtt, ns(101), ns(100)).is_err());
        assert!(DeadlineHeadroom::new(cost, rtt, ns(0), ns(0)).is_err());
    }
    #[test]
    #[allow(clippy::manual_div_ceil)] // Independent arithmetic oracle avoids the production helper.
    fn integer_reference_matches_bounded_seeded_sample_histories() {
        for seed in 0..64u64 {
            let mut state = seed + 1;
            let mut cost = EncodingCost::new(ns(1_000_000));
            let mut rate = None;
            let mut rtt = RoundTripTime::default();
            let mut latency = None;
            for step in 0..128 {
                state ^= state << 13;
                state ^= state >> 7;
                state ^= state << 17;
                let bytes = (state % 1024 + 1) as u32;
                let elapsed = (state >> 10) % 100_000;
                cost.observe(input(bytes), ns(elapsed)).unwrap();
                rtt.observe(ns(elapsed));
                let sample =
                    (u128::from(elapsed) * 65536 + u128::from(bytes) - 1) / u128::from(bytes);
                rate = Some(match rate {
                    None => sample,
                    Some(old) => (7 * old + sample + 7) / 8,
                });
                latency = Some(match latency {
                    None => u128::from(elapsed),
                    Some(old) => (7 * old + u128::from(elapsed) + 7) / 8,
                });
                let size = (state >> 32) as u32;
                let expected = ((rate.unwrap() * u128::from(size) + 65535) / 65536
                    + latency.unwrap())
                .min(1_000_000) as u64;
                let estimate = DeadlineHeadroom::new(cost, rtt, ns(1_000), ns(1_000_000)).unwrap();
                assert_eq!(
                    estimate.estimate(size),
                    ns(expected),
                    "seed={seed} step={step}"
                );
            }
        }
    }
}
