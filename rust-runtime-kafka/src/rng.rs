//! Portable deterministic randomness for simulation and replay.
//!
//! This module deliberately owns both its generator and its result mappings.
//! Using a standard-library distribution would make replay depend on details
//! that are not part of Rust's portability guarantees. The algorithm here is
//! SplitMix64, with wrapping arithmetic and constants pinned by golden tests.
//!
//! A simulation should create one [`DeterministicRng`] per [`RandomStream`].
//! Keeping higher-level schedule-affecting choices, scenario construction,
//! workloads, faults, and debug-only instrumentation on separate streams
//! prevents an incidental draw in one domain from perturbing all of the others.
//! The executor itself preserves strict FIFO ready ordering and does not
//! consume the schedule stream for ready-queue tie-breaking. The simulated I/O
//! providers do: a provider given a [`RandomStream::Schedule`] source draws its
//! per-operation completion jitter from it, which is where seed-dependent
//! completion orders come from.
//!
//! This generator is intended for simulation, not cryptography.

use std::fmt;

/// The version of the deterministic RNG algorithm and seed-derivation scheme.
///
/// Incrementing this is a replay-breaking change. A replay artifact should
/// record this value alongside its root seed.
pub const DETERMINISTIC_RNG_VERSION: u32 = 1;

/// The version of the seed-to-start-time derivation scheme.
///
/// Incrementing this is a replay-breaking change for any harness that
/// constructs runtimes with [`derive_start_time_nanos`]. A reproduction
/// manifest that records a derived start time should record this value
/// alongside it; the derived instant itself is already pinned by the
/// runtime configuration it was placed in.
pub const START_TIME_DERIVATION_VERSION: u32 = 1;

const SPLITMIX64_GAMMA: u64 = 0x9e37_79b9_7f4a_7c15;
const SPLITMIX64_MUL1: u64 = 0xbf58_476d_1ce4_e5b9;
const SPLITMIX64_MUL2: u64 = 0x94d0_49bb_1331_11eb;

/// An independent domain of deterministic random choices.
///
/// Each variant has a stable, explicit domain tag. Deriving all streams from a
/// single root seed gives a run one convenient reproduction key without making
/// unrelated subsystems share mutable RNG state.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
#[repr(u64)]
#[non_exhaustive]
pub enum RandomStream {
    /// Higher-level race/select choices and modeled I/O/network completion
    /// timing. The kernel does not use this stream to perturb ready ordering.
    Schedule = 0x5343_4845_4455_4c45, // `SCHEDULE`
    /// Topology, configuration, and other initial scenario construction.
    Scenario = 0x5343_454e_4152_494f, // `SCENARIO`
    /// Operations selected by the test workload.
    Workload = 0x574f_524b_4c4f_4144, // `WORKLOAD`
    /// Fault selection, placement, timing, and severity.
    Fault = 0x4641_554c_5400_0000, // `FAULT\0\0\0`
    /// Identifiers and diagnostics that must not affect simulated behavior.
    Debug = 0x4445_4255_4700_0000, // `DEBUG\0\0\0`
}

impl RandomStream {
    const fn domain_tag(self) -> u64 {
        self as u64
    }
}

/// Derives the raw seed for a domain-specific stream from a run's root seed.
///
/// The derivation is stable and domain separated: for a given `root_seed`, all
/// current [`RandomStream`] variants produce distinct seeds. It is not a key
/// derivation function and provides no cryptographic guarantees.
#[must_use]
pub const fn derive_stream_seed(root_seed: u64, stream: RandomStream) -> u64 {
    splitmix64_mix(root_seed ^ stream.domain_tag())
}

/// Domain tag for the start-time derivation, disjoint from every
/// [`RandomStream`] tag so a derived start time never collides with a
/// stream seed.
const START_TIME_DOMAIN_TAG: u64 = 0x5354_4152_5454_494d; // `STARTTIM`

/// Nanosecond span of derivable start times: `1..=2^62`.
///
/// The upper bound (about 146 years) leaves more than 400 years of `u64`
/// nanosecond headroom before instant arithmetic can exhaust, and the lower
/// bound guarantees a derived start is never the zero default, so a harness
/// that silently drops the derivation cannot keep passing by accident.
const START_TIME_SPAN_MASK: u64 = (1 << 62) - 1;

/// Derives a nonzero virtual start time, in nanoseconds, from a run's root
/// seed.
///
/// The mapping consumes no random-stream draws and is pinned by
/// [`START_TIME_DERIVATION_VERSION`]: for a given `root_seed` it returns the
/// same instant in every build, and golden tests fail until a deliberate
/// version bump when the mapping changes.
#[must_use]
pub const fn derive_start_time_nanos(root_seed: u64) -> u64 {
    (splitmix64_mix(root_seed ^ START_TIME_DOMAIN_TAG) & START_TIME_SPAN_MASK) + 1
}

/// An error returned by a deterministic random source.
///
/// Validation failures do not consume a primitive RNG draw.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum RandomError {
    /// The runtime that owns the source has entered its terminal state.
    RuntimeStopped,
    /// A bounded integer was requested with an exclusive upper bound of zero.
    ZeroUpperBound,
    /// A rational probability had a zero denominator or a numerator larger
    /// than its denominator.
    InvalidRatio {
        /// The requested probability numerator.
        numerator: u64,
        /// The requested probability denominator.
        denominator: u64,
    },
}

impl fmt::Display for RandomError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match *self {
            Self::RuntimeStopped => formatter.write_str("random source runtime is stopped"),
            Self::ZeroUpperBound => formatter.write_str("random upper bound must be non-zero"),
            Self::InvalidRatio {
                numerator,
                denominator,
            } => write!(
                formatter,
                "probability ratio {numerator}/{denominator} is invalid"
            ),
        }
    }
}

impl std::error::Error for RandomError {}

/// Validates a `0..upper_exclusive` selection bound.
///
/// Runtime random sources call this before their stopped-state check so an
/// invalid request reports the same error before and after shutdown, without
/// consuming a draw.
pub(crate) const fn validate_upper_bound(upper_exclusive: u64) -> Result<(), RandomError> {
    if upper_exclusive == 0 {
        return Err(RandomError::ZeroUpperBound);
    }
    Ok(())
}

/// Validates an exact rational probability.
///
/// Runtime random sources call this before their stopped-state check so an
/// invalid request reports the same error before and after shutdown, without
/// consuming a draw.
pub(crate) const fn validate_ratio(numerator: u64, denominator: u64) -> Result<(), RandomError> {
    if denominator == 0 || numerator > denominator {
        return Err(RandomError::InvalidRatio {
            numerator,
            denominator,
        });
    }
    Ok(())
}

/// A resumable position in a [`DeterministicRng`] stream.
///
/// The state is algorithm-specific. Persist checkpoints only together with
/// [`DETERMINISTIC_RNG_VERSION`], because restoring a checkpoint under a future
/// algorithm version need not reproduce the same values.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct RngCheckpoint {
    state: u64,
    draws: u64,
}

impl RngCheckpoint {
    /// Reconstructs a checkpoint from its algorithm-specific raw parts.
    ///
    /// This is useful when loading a replay artifact. `state` must have been
    /// produced by this module at the same [`DETERMINISTIC_RNG_VERSION`].
    #[must_use]
    pub const fn from_raw_parts(state: u64, draws: u64) -> Self {
        Self { state, draws }
    }

    /// Returns the raw SplitMix64 state before the next draw.
    #[must_use]
    pub const fn state(self) -> u64 {
        self.state
    }

    /// Returns the number of primitive 64-bit draws already consumed.
    #[must_use]
    pub const fn draws(self) -> u64 {
        self.draws
    }
}

/// A small, portable deterministic random number generator.
///
/// Every higher-level operation maps values itself rather than delegating to a
/// standard-library distribution. The `draws` counter counts primitive
/// [`next_u64`](Self::next_u64) calls. Rejection sampling can therefore consume
/// more than one draw for a single bounded choice. The generator,
/// bounded-integer mapping, ratio mapping, and stream derivation are all part of
/// the versioned replay contract; changing any of them requires incrementing
/// [`DETERMINISTIC_RNG_VERSION`].
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DeterministicRng {
    state: u64,
    draws: u64,
}

impl DeterministicRng {
    /// Creates a generator from a raw stream seed.
    ///
    /// Prefer [`from_root_seed`](Self::from_root_seed) when constructing the
    /// standard simulation streams.
    #[must_use]
    pub const fn new(seed: u64) -> Self {
        Self {
            state: seed,
            draws: 0,
        }
    }

    /// Creates an independent domain-specific generator from a root seed.
    #[must_use]
    pub const fn from_root_seed(root_seed: u64, stream: RandomStream) -> Self {
        Self::new(derive_stream_seed(root_seed, stream))
    }

    /// Reconstructs a generator at a previously recorded checkpoint.
    #[must_use]
    pub const fn from_checkpoint(checkpoint: RngCheckpoint) -> Self {
        Self {
            state: checkpoint.state,
            draws: checkpoint.draws,
        }
    }

    /// Returns the next 64 random bits and advances the stream once.
    pub fn next_u64(&mut self) -> u64 {
        self.state = self.state.wrapping_add(SPLITMIX64_GAMMA);
        self.draws = self
            .draws
            .checked_add(1)
            .expect("deterministic RNG draw counter overflow");
        splitmix64_mix(self.state)
    }

    /// Selects uniformly from `0..upper_exclusive` using rejection sampling.
    ///
    /// This consumes at least one primitive draw and may consume more when a
    /// candidate is rejected. The method uses no floating-point arithmetic.
    ///
    /// # Errors
    ///
    /// Returns [`RandomError::ZeroUpperBound`] without consuming a draw when
    /// `upper_exclusive` is zero.
    pub fn u64_below(&mut self, upper_exclusive: u64) -> Result<u64, RandomError> {
        validate_upper_bound(upper_exclusive)?;

        // 2^64 modulo the bound, computed without representing 2^64. Values
        // below this threshold are the incomplete tail of the input domain.
        let rejection_threshold = upper_exclusive.wrapping_neg() % upper_exclusive;
        loop {
            let candidate = self.next_u64();
            if candidate >= rejection_threshold {
                return Ok(candidate % upper_exclusive);
            }
        }
    }

    /// Returns `true` with the exact rational probability `numerator/denominator`.
    ///
    /// The decision uses integer rejection sampling, so it has neither
    /// floating-point rounding nor bias. Even probabilities zero and one
    /// consume at least one primitive draw; this keeps each requested decision
    /// visible in the draw counter.
    ///
    /// # Errors
    ///
    /// Returns [`RandomError::InvalidRatio`] without consuming a draw if
    /// `denominator` is zero or `numerator > denominator`.
    pub fn bool_ratio(&mut self, numerator: u64, denominator: u64) -> Result<bool, RandomError> {
        validate_ratio(numerator, denominator)?;
        Ok(self.u64_below(denominator)? < numerator)
    }

    /// Returns the number of primitive 64-bit draws consumed so far.
    #[must_use]
    pub const fn draws(&self) -> u64 {
        self.draws
    }

    /// Captures a resumable stream position without consuming a value.
    #[must_use]
    pub const fn checkpoint(&self) -> RngCheckpoint {
        RngCheckpoint {
            state: self.state,
            draws: self.draws,
        }
    }
}

const fn splitmix64_mix(mut value: u64) -> u64 {
    value = (value ^ (value >> 30)).wrapping_mul(SPLITMIX64_MUL1);
    value = (value ^ (value >> 27)).wrapping_mul(SPLITMIX64_MUL2);
    value ^ (value >> 31)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn splitmix64_seed_zero_matches_golden_vector() {
        let mut rng = DeterministicRng::new(0);
        let actual = [
            rng.next_u64(),
            rng.next_u64(),
            rng.next_u64(),
            rng.next_u64(),
            rng.next_u64(),
        ];

        assert_eq!(
            actual,
            [
                0xe220_a839_7b1d_cdaf,
                0x6e78_9e6a_a1b9_65f4,
                0x06c4_5d18_8009_454f,
                0xf88b_b8a8_724c_81ec,
                0x1b39_896a_51a8_749b,
            ]
        );
        assert_eq!(rng.draws(), 5);
    }

    #[test]
    fn stream_derivation_matches_golden_vector() {
        let root_seed = 0x0123_4567_89ab_cdef;
        let actual = [
            derive_stream_seed(root_seed, RandomStream::Schedule),
            derive_stream_seed(root_seed, RandomStream::Scenario),
            derive_stream_seed(root_seed, RandomStream::Workload),
            derive_stream_seed(root_seed, RandomStream::Fault),
            derive_stream_seed(root_seed, RandomStream::Debug),
        ];

        // Values are filled in from the independently checked v1 derivation.
        assert_eq!(
            actual,
            [
                0xbfe3_6947_f814_347e,
                0x9c9b_dcb2_cb86_4d53,
                0xa503_1145_3ff6_f983,
                0x32de_2008_24c1_4fea,
                0x6b36_c408_eb21_b7ce,
            ]
        );
    }

    #[test]
    fn start_time_derivation_matches_golden_vector() {
        // Values are filled in from the independently checked v1 derivation.
        assert_eq!(START_TIME_DERIVATION_VERSION, 1);
        assert_eq!(derive_start_time_nanos(0), 3_868_693_840_289_130_663);
        assert_eq!(derive_start_time_nanos(17), 1_234_623_339_628_287_327);
        assert_eq!(derive_start_time_nanos(u64::MAX), 2_324_755_258_818_371_601);
    }

    #[test]
    fn derived_start_times_are_nonzero_and_bounded() {
        for seed in [0, 1, 17, 0xfeed_face_cafe_beef, u64::MAX] {
            let start = derive_start_time_nanos(seed);
            assert!(start >= 1);
            assert!(start <= 1 << 62);
        }
    }

    #[test]
    fn start_time_domain_tag_is_disjoint_from_stream_tags() {
        let stream_tags = [
            RandomStream::Schedule as u64,
            RandomStream::Scenario as u64,
            RandomStream::Workload as u64,
            RandomStream::Fault as u64,
            RandomStream::Debug as u64,
        ];
        assert!(!stream_tags.contains(&START_TIME_DOMAIN_TAG));
    }

    #[test]
    fn stream_domains_are_independent() {
        let root_seed = 0xfeed_face_cafe_beef;
        let streams = [
            RandomStream::Schedule,
            RandomStream::Scenario,
            RandomStream::Workload,
            RandomStream::Fault,
            RandomStream::Debug,
        ];
        let seeds = streams.map(|stream| derive_stream_seed(root_seed, stream));

        for (index, seed) in seeds.iter().enumerate() {
            assert!(!seeds[..index].contains(seed));
        }

        let mut schedule = DeterministicRng::from_root_seed(root_seed, RandomStream::Schedule);
        let mut debug = DeterministicRng::from_root_seed(root_seed, RandomStream::Debug);
        let schedule_checkpoint = schedule.checkpoint();
        let _ = debug.next_u64();
        let _ = debug.next_u64();
        assert_eq!(schedule.checkpoint(), schedule_checkpoint);
        assert_ne!(schedule.next_u64(), debug.next_u64());
    }

    #[test]
    fn checkpoint_restores_exact_position() {
        let mut rng = DeterministicRng::new(42);
        let _ = rng.next_u64();
        let checkpoint = rng.checkpoint();
        let expected = [rng.next_u64(), rng.next_u64(), rng.next_u64()];

        let mut resumed = DeterministicRng::from_checkpoint(checkpoint);
        assert_eq!(
            [resumed.next_u64(), resumed.next_u64(), resumed.next_u64()],
            expected
        );
        assert_eq!(checkpoint.state(), checkpoint.state);
        assert_eq!(checkpoint.draws(), checkpoint.draws);
    }

    #[test]
    fn bounded_mapping_rejects_the_incomplete_tail() {
        let mut rng = DeterministicRng::new(3);
        let value = rng.u64_below((1_u64 << 63) + 1).unwrap();

        assert!(value < (1_u64 << 63) + 1);
        assert_eq!(rng.draws(), 2);
    }

    #[test]
    fn ratio_boundaries_are_exact_and_visible_in_the_draw_counter() {
        let mut rng = DeterministicRng::new(7);

        let before = rng.draws();
        assert!(!rng.bool_ratio(0, 1).unwrap());
        assert!(rng.bool_ratio(1, 1).unwrap());
        assert_eq!(rng.draws(), before + 2);
    }

    #[test]
    fn invalid_requests_return_errors_without_consuming_draws() {
        let mut rng = DeterministicRng::new(0);
        let checkpoint = rng.checkpoint();

        assert_eq!(rng.u64_below(0), Err(RandomError::ZeroUpperBound));
        assert_eq!(
            rng.bool_ratio(1, 0),
            Err(RandomError::InvalidRatio {
                numerator: 1,
                denominator: 0,
            })
        );
        assert_eq!(
            rng.bool_ratio(2, 1),
            Err(RandomError::InvalidRatio {
                numerator: 2,
                denominator: 1,
            })
        );
        assert_eq!(rng.checkpoint(), checkpoint);
    }
}
