//! Versioned completion-latency models for the simulated providers.
//!
//! A simulated provider decides *when* an admitted operation completes. With a
//! fixed latency, two concurrent operations admitted in the same order always
//! complete in that order, so one workload explores essentially one completion
//! interleaving no matter how many seeds run. This module supplies the missing
//! diversity: a jitter increment drawn from [`RandomStream::Schedule`], the
//! stream reserved for "higher-level race/select choices and modeled I/O and
//! network completion timing".
//!
//! The executor is deliberately not involved. Ready ordering stays strict FIFO
//! and the kernel still never consumes the schedule stream; diversity enters at
//! this replay-visible boundary instead, where every draw lands in the run's
//! determinism checkpoint like any other modeled choice.
//!
//! # Where jitter applies
//!
//! Jitter perturbs a provider's *own* completion latency: storage's default
//! per-operation latency and the network's link-derived completion delay. It
//! never perturbs a scripted fault's explicit delay. A test that scripts an
//! exact delay is asserting an exact virtual-time deadline, and this workspace
//! asserts exact values rather than tolerances.
//!
//! # Draw discipline
//!
//! A model draws at most once per operation, and only when it can actually
//! perturb that operation: [`SimLatencyModel::Fixed`] and a zero-width
//! [`SimLatencyModel::UniformJitterV1`] consume no draws at all. Bounds are
//! validated when the provider is constructed, not when a draw is made, so
//! sampling itself is infallible. After the owning runtime reaches its terminal
//! state a draw is refused by the kernel; [`SimLatency::jitter`] then yields no
//! jitter, which is honest because the operation it would have delayed can no
//! longer complete.

use kr_runtime::rng::RandomStream;
use kr_runtime::{RandomHandle, SimDuration};

/// The version of the simulated latency models and their draw mapping.
///
/// Incrementing this is a replay-breaking change: a recorded run's completion
/// order depends on both the model shape and how it consumes schedule draws.
pub const SIM_LATENCY_MODEL_VERSION: u32 = 1;

/// How a simulated provider perturbs its own completion latency.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
#[non_exhaustive]
pub enum SimLatencyModel {
    /// Every operation completes after exactly its configured latency.
    ///
    /// This is the deterministic reference behavior and consumes no draws.
    #[default]
    Fixed,
    /// Adds a uniform jitter in `0..=max_jitter` to each completion latency.
    ///
    /// The draw is one [`RandomHandle::random_below`] over
    /// `max_jitter.as_nanos() + 1`, so both endpoints are reachable. A zero
    /// `max_jitter` is equivalent to [`Self::Fixed`] and consumes no draws.
    UniformJitterV1 {
        /// The inclusive upper bound of the added jitter.
        max_jitter: SimDuration,
    },
}

impl SimLatencyModel {
    /// Returns the deterministic reference model.
    #[must_use]
    pub const fn fixed() -> Self {
        Self::Fixed
    }

    /// Returns a uniform jitter model bounded by `max_jitter`.
    #[must_use]
    pub const fn uniform_jitter_v1(max_jitter: SimDuration) -> Self {
        Self::UniformJitterV1 { max_jitter }
    }

    /// Returns the largest jitter this model can add to one operation.
    #[must_use]
    pub const fn max_jitter(self) -> SimDuration {
        match self {
            Self::Fixed => SimDuration::ZERO,
            Self::UniformJitterV1 { max_jitter } => max_jitter,
        }
    }

    /// Returns whether this model can perturb completion order at all.
    #[must_use]
    pub const fn is_deterministic(self) -> bool {
        self.max_jitter().as_nanos() == 0
    }
}

/// Why a latency model cannot be installed on a provider.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum SimLatencyError {
    /// The supplied random source is not scoped to [`RandomStream::Schedule`].
    WrongStream {
        /// The stream the source was actually bound to.
        stream: RandomStream,
    },
    /// A perturbing model was configured without a schedule random source.
    MissingScheduleRandom,
    /// The configured base latency plus the model's maximum jitter overflows.
    LatencyOverflow {
        /// The largest base latency the provider can apply.
        base: SimDuration,
        /// The model's inclusive maximum jitter.
        max_jitter: SimDuration,
    },
}

impl std::fmt::Display for SimLatencyError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::WrongStream { stream } => write!(
                formatter,
                "latency random source must use the Schedule stream, got {stream:?}"
            ),
            Self::MissingScheduleRandom => {
                formatter.write_str("a perturbing latency model requires a Schedule random source")
            }
            Self::LatencyOverflow { base, max_jitter } => write!(
                formatter,
                "base latency {} ns plus maximum jitter {} ns overflows",
                base.as_nanos(),
                max_jitter.as_nanos()
            ),
        }
    }
}

impl std::error::Error for SimLatencyError {}

/// A provider's installed latency model and its schedule random source.
#[derive(Clone)]
pub struct SimLatency {
    model: SimLatencyModel,
    random: Option<RandomHandle>,
}

impl SimLatency {
    /// Returns the deterministic model, which needs no random source.
    #[must_use]
    pub const fn fixed() -> Self {
        Self {
            model: SimLatencyModel::Fixed,
            random: None,
        }
    }

    /// Installs `model`, validating its random source and latency bound.
    ///
    /// `max_base` is the largest base latency the calling provider can apply
    /// to one operation. Validating the sum here means [`Self::jitter`] cannot
    /// overflow later, so sampling stays infallible on the hot path.
    ///
    /// # Errors
    ///
    /// Returns [`SimLatencyError::WrongStream`] when `random` is not scoped to
    /// [`RandomStream::Schedule`], [`SimLatencyError::MissingScheduleRandom`]
    /// when a perturbing model has no source, and
    /// [`SimLatencyError::LatencyOverflow`] when `max_base` plus the model's
    /// maximum jitter exceeds the duration range.
    pub fn new(
        model: SimLatencyModel,
        random: Option<RandomHandle>,
        max_base: SimDuration,
    ) -> Result<Self, SimLatencyError> {
        if let Some(random) = &random
            && random.stream() != RandomStream::Schedule
        {
            return Err(SimLatencyError::WrongStream {
                stream: random.stream(),
            });
        }
        if model.is_deterministic() {
            return Ok(Self { model, random });
        }
        let Some(random) = random else {
            return Err(SimLatencyError::MissingScheduleRandom);
        };
        let max_jitter = model.max_jitter();
        // The inclusive bound is drawn as an exclusive one, so a model whose
        // bound cannot be widened by a nanosecond is rejected regardless of
        // the base a provider applies it to.
        if max_jitter.as_nanos().checked_add(1).is_none()
            || max_base.checked_add(max_jitter).is_none()
        {
            return Err(SimLatencyError::LatencyOverflow {
                base: max_base,
                max_jitter,
            });
        }
        Ok(Self {
            model,
            random: Some(random),
        })
    }

    /// Returns the installed model.
    #[must_use]
    pub const fn model(&self) -> SimLatencyModel {
        self.model
    }

    /// Draws this operation's jitter increment.
    ///
    /// Returns [`SimDuration::ZERO`] without consuming a draw for a
    /// deterministic model, and also when the owning runtime has reached its
    /// terminal state and refuses further draws.
    #[must_use]
    pub fn jitter(&self) -> SimDuration {
        let SimLatencyModel::UniformJitterV1 { max_jitter } = self.model else {
            return SimDuration::ZERO;
        };
        let nanos = max_jitter.as_nanos();
        if nanos == 0 {
            return SimDuration::ZERO;
        }
        let Some(random) = &self.random else {
            return SimDuration::ZERO;
        };
        // `new` rejects a perturbing model whose bound cannot be widened by
        // one, so the inclusive upper endpoint is always representable.
        let upper_exclusive = nanos
            .checked_add(1)
            .expect("a perturbing model has a bound below u64::MAX");
        match random.random_below(upper_exclusive) {
            Ok(drawn) => SimDuration::from_nanos(drawn),
            Err(_terminal) => SimDuration::ZERO,
        }
    }
}

impl std::fmt::Debug for SimLatency {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("SimLatency")
            .field("model", &self.model)
            .field("has_random", &self.random.is_some())
            .finish()
    }
}

impl Default for SimLatency {
    fn default() -> Self {
        Self::fixed()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use kr_runtime::{RuntimeConfig, SimRuntime};

    fn runtime() -> SimRuntime {
        SimRuntime::new(RuntimeConfig::default())
    }

    #[test]
    fn the_model_version_is_pinned() {
        assert_eq!(SIM_LATENCY_MODEL_VERSION, 1);
    }

    #[test]
    fn a_fixed_model_needs_no_random_source_and_draws_nothing() {
        let latency = SimLatency::fixed();
        assert!(latency.model().is_deterministic());
        assert_eq!(latency.jitter(), SimDuration::ZERO);
    }

    #[test]
    fn a_zero_width_jitter_model_is_deterministic_and_draws_nothing() {
        let runtime = runtime();
        let random = runtime.random_source(RandomStream::Schedule);
        let before = random.random_position();
        let latency = SimLatency::new(
            SimLatencyModel::uniform_jitter_v1(SimDuration::ZERO),
            Some(random.clone()),
            SimDuration::from_nanos(10),
        )
        .expect("a zero-width model is valid");
        assert_eq!(latency.jitter(), SimDuration::ZERO);
        assert_eq!(random.random_position(), before, "no draw was consumed");
    }

    #[test]
    fn a_wrong_stream_source_is_rejected_before_any_draw() {
        let runtime = runtime();
        let random = runtime.random_source(RandomStream::Fault);
        let error = SimLatency::new(
            SimLatencyModel::uniform_jitter_v1(SimDuration::from_nanos(4)),
            Some(random),
            SimDuration::ZERO,
        )
        .expect_err("the Fault stream is not a latency source");
        assert_eq!(
            error,
            SimLatencyError::WrongStream {
                stream: RandomStream::Fault
            }
        );
    }

    #[test]
    fn a_perturbing_model_without_a_source_is_rejected() {
        let error = SimLatency::new(
            SimLatencyModel::uniform_jitter_v1(SimDuration::from_nanos(4)),
            None,
            SimDuration::ZERO,
        )
        .expect_err("jitter requires a source");
        assert_eq!(error, SimLatencyError::MissingScheduleRandom);
    }

    #[test]
    fn a_bound_that_cannot_be_added_to_the_base_is_rejected() {
        let runtime = runtime();
        let random = runtime.random_source(RandomStream::Schedule);
        let error = SimLatency::new(
            SimLatencyModel::uniform_jitter_v1(SimDuration::from_nanos(u64::MAX)),
            Some(random),
            SimDuration::from_nanos(1),
        )
        .expect_err("the sum overflows");
        assert_eq!(
            error,
            SimLatencyError::LatencyOverflow {
                base: SimDuration::from_nanos(1),
                max_jitter: SimDuration::from_nanos(u64::MAX),
            }
        );
    }

    #[test]
    fn jitter_stays_within_its_inclusive_bounds_and_reaches_both_ends() {
        let runtime = runtime();
        let random = runtime.random_source(RandomStream::Schedule);
        let max_jitter = SimDuration::from_nanos(3);
        let latency = SimLatency::new(
            SimLatencyModel::uniform_jitter_v1(max_jitter),
            Some(random),
            SimDuration::from_nanos(64),
        )
        .expect("a valid model");
        let mut seen = std::collections::BTreeSet::new();
        for _ in 0..256 {
            let drawn = latency.jitter();
            assert!(drawn <= max_jitter, "{drawn:?} exceeds the bound");
            seen.insert(drawn.as_nanos());
        }
        assert!(seen.contains(&0), "the lower endpoint is reachable");
        assert!(seen.contains(&3), "the upper endpoint is reachable");
    }

    #[test]
    fn one_operation_consumes_exactly_one_draw() {
        let runtime = runtime();
        let random = runtime.random_source(RandomStream::Schedule);
        let latency = SimLatency::new(
            SimLatencyModel::uniform_jitter_v1(SimDuration::from_nanos(8)),
            Some(random.clone()),
            SimDuration::from_nanos(8),
        )
        .expect("a valid model");
        let before = random.random_position();
        let _ = latency.jitter();
        let after = random.random_position();
        assert_ne!(before, after, "a perturbing draw advances the stream");
        let _ = latency.jitter();
        assert_ne!(random.random_position(), after, "each call draws once");
    }
}
