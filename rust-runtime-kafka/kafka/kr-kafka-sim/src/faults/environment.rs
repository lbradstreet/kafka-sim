use super::*;

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct Ramp {
    pub end_delay_ns: u64,
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct EnvironmentRule {
    pub broker: Option<i32>,
    pub api: Option<i16>,
    pub phase: Phase,
    pub start_ns: u64,
    pub end_ns: u64,
    pub probability_ppm: u32,
    pub effects: Effects,
    pub ramp: Option<Ramp>,
}
impl EnvironmentRule {
    pub(super) fn matches(&self, hook: &Hook) -> bool {
        self.phase == hook.phase
            && self.broker.is_none_or(|b| b == hook.broker)
            && self.api.is_none_or(|api| Some(api) == hook.api)
            && self.start_ns <= hook.now_ns
            && hook.now_ns < self.end_ns
    }
    fn delay_fraction(&self, now: u64) -> (u128, u128) {
        match self.ramp {
            None => (u128::from(self.effects.delay_ns), 1),
            Some(ramp) => {
                let duration = u128::from(self.end_ns - self.start_ns);
                let elapsed = u128::from(now.clamp(self.start_ns, self.end_ns) - self.start_ns);
                (
                    u128::from(self.effects.delay_ns) * (duration - elapsed)
                        + u128::from(ramp.end_delay_ns) * elapsed,
                    duration,
                )
            }
        }
    }
    pub(super) fn effects_at(&self, now: u64) -> Effects {
        let (numerator, denominator) = self.delay_fraction(now);
        Effects {
            delay_ns: (numerator / denominator) as u64,
            ..self.effects
        }
    }
    fn validate(&self) -> Result<(), String> {
        Match {
            phase: self.phase,
            api: self.api,
            broker: self.broker,
            ..Match::default()
        }
        .validate()?;
        self.effects.validate(self.phase)?;
        if self.start_ns >= self.end_ns
            || self.end_ns > 300_000_000_000
            || self.probability_ppm > 1_000_000
            || self.ramp.is_some_and(|r| r.end_delay_ns > MAX_DELAY_NS)
            || self.effects.is_empty() && self.ramp.is_none_or(|r| r.end_delay_ns == 0)
            || self.effects.reject_error.is_some() && self.api != Some(0)
        {
            return Err("invalid environment window/effects".into());
        }
        Ok(())
    }
}
pub(super) fn validate(rules: &[EnvironmentRule]) -> Result<(), String> {
    if rules.len() > MAX_ENVIRONMENT {
        return Err("environment rule capacity".into());
    }
    for rule in rules {
        rule.validate()?;
    }
    for (index, a) in rules.iter().enumerate() {
        for b in &rules[..index] {
            let start = a.start_ns.max(b.start_ns);
            let end = a.end_ns.min(b.end_ns);
            if start >= end
                || a.probability_ppm != 1_000_000
                || b.probability_ppm != 1_000_000
                || a.phase != b.phase
                || a.broker.zip(b.broker).is_some_and(|(a, b)| a != b)
                || a.api.zip(b.api).is_some_and(|(a, b)| a != b)
            {
                continue;
            }
            for at in [start, end - 1] {
                let mut merged = a.effects_at(at);
                merged
                    .merge(b.effects_at(at))
                    .map_err(|_| "conflicting deterministic environment rules")?;
                // Bound the continuous linear sum as well as its rounded
                // endpoints; opposing ramps can differ by a nanosecond inside.
                let (an, ad) = a.delay_fraction(at);
                let (bn, bd) = b.delay_fraction(at);
                if (an * bd + bn * ad) / (ad * bd) > u128::from(MAX_DELAY_NS) {
                    return Err("combined deterministic environment delay bound".into());
                }
            }
        }
    }
    Ok(())
}

impl FaultEngine {
    pub(super) fn base_effects(&self, hook: &Hook) -> Effects {
        let delay_ns = if hook.phase == Phase::BeforeAppend {
            self.config
                .services
                .iter()
                .find(|s| s.broker == hook.broker)
                .map_or(0, |s| s.delay_ns)
        } else {
            0
        };
        let outcome = match hook.phase {
            Phase::IsolationStart => Outcome::Isolate,
            Phase::IsolationEnd => Outcome::Restore,
            Phase::Setup
                if self.config.isolations.iter().any(|w| {
                    w.broker == hook.broker && w.start_ns <= hook.now_ns && hook.now_ns < w.end_ns
                }) || crate::experiment_link::setup_failed(
                    &self.config,
                    hook.broker,
                    hook.now_ns,
                ) =>
            {
                Outcome::SetupFailure
            }
            _ => Outcome::Continue,
        };
        Effects {
            delay_ns,
            outcome,
            ..Effects::default()
        }
    }
    pub(super) fn expected_draws(&self, hook: &Hook) -> usize {
        self.config
            .environment
            .iter()
            .filter(|r| r.matches(hook) && r.probability_ppm != 1_000_000)
            .count()
            + self
                .config
                .random
                .iter()
                .filter(|r| r.matcher.matches(hook, self.round))
                .count()
                * 2
    }
    /// Applies unbudgeted rules in manifest order. Deterministic rules draw
    /// nothing; every matching probabilistic rule consumes exactly one draw,
    /// including probability zero. A firing at the zero endpoint of a ramp is
    /// still retained as phase evidence.
    pub(super) fn apply_environment(
        &self,
        hook: &Hook,
        effects: &mut Effects,
        mut draw: impl FnMut() -> Result<u64, String>,
    ) -> Result<Vec<usize>, String> {
        let mut indices = Vec::new();
        indices
            .try_reserve_exact(
                self.config
                    .environment
                    .iter()
                    .filter(|r| r.matches(hook))
                    .count(),
            )
            .map_err(|_| "environment index allocation")?;
        for (index, rule) in self.config.environment.iter().enumerate() {
            if !rule.matches(hook) {
                continue;
            }
            let hit = rule.probability_ppm == 1_000_000
                || u128::from(draw()?) < (u128::from(rule.probability_ppm) << 64) / 1_000_000;
            if hit {
                effects.merge(rule.effects_at(hook.now_ns))?;
                indices.push(index);
            }
        }
        Ok(indices)
    }
}
