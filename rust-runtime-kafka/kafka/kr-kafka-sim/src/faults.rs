//! Bounded passive broker-hook faults. All times are relative to manifest
//! start_ns. Replay consumes the recorded effects and verifies the same Fault
//! RNG draws; it never samples a new effect from a recorded decision.
use serde::{Deserialize, Serialize};
mod environment;
pub use environment::{EnvironmentRule, Ramp};

const MAX_RULES: usize = 64;
const MAX_RANDOM_RULES: usize = 8;
const MAX_ENVIRONMENT: usize = 256;
const MAX_DECISIONS: usize = 2_000_000;
const MAX_DELAY_NS: u64 = 60_000_000_000;

#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
pub enum Phase {
    /// First poll of connection setup, before ApiVersions is sent.
    Setup,
    /// Before the model handles a complete API frame. For Produce this is
    /// before validation/append; other APIs have no record append.
    #[default]
    BeforeAppend,
    /// After model handling, including rejection and duplicate responses.
    /// BrokerCommit is the separate evidence that new records were appended.
    AfterAppend,
    /// Before writing the first byte of a model response.
    BeforeResponse,
    IsolationStart,
    IsolationEnd,
}
#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct Match {
    pub phase: Phase,
    pub api: Option<i16>,
    pub broker: Option<i32>,
    pub connection: Option<u64>,
    pub round: Option<u32>,
}
impl Match {
    fn matches(&self, hook: &Hook, round: u32) -> bool {
        self.phase == hook.phase
            && self.api.is_none_or(|api| hook.api == Some(api))
            && self.broker.is_none_or(|broker| hook.broker == broker)
            && self
                .connection
                .is_none_or(|connection| hook.connection == connection)
            && self.round.is_none_or(|expected| expected == round)
    }
    fn validate(&self) -> Result<(), String> {
        if self.api.is_some_and(|api| api < 0)
            || self.broker.is_some_and(|broker| broker < 0)
            || self.connection == Some(0)
            || matches!(self.phase, Phase::IsolationStart | Phase::IsolationEnd)
            || self.phase == Phase::Setup && self.api.is_some()
        {
            return Err("invalid fault matcher".into());
        }
        Ok(())
    }
}
#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
pub enum Outcome {
    #[default]
    Continue,
    Drop,
    Disconnect,
    SetupFailure,
    Isolate,
    Restore,
}
#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct Effects {
    pub delay_ns: u64,
    pub outcome: Outcome,
    pub reject_error: Option<i16>,
    pub throttle_ms: u32,
}
impl Effects {
    fn is_empty(&self) -> bool {
        *self == Self::default()
    }
    fn validate(&self, phase: Phase) -> Result<(), String> {
        if self.delay_ns > MAX_DELAY_NS
            || self.throttle_ms > i32::MAX as u32
            || self
                .reject_error
                .is_some_and(|error| error <= 0 || error == 46)
            || self.reject_error.is_some() && phase != Phase::BeforeAppend
            || self.throttle_ms != 0 && phase != Phase::BeforeAppend
            || self.delay_ns != 0 && matches!(phase, Phase::IsolationStart | Phase::IsolationEnd)
            || match phase {
                Phase::Setup => !matches!(self.outcome, Outcome::Continue | Outcome::SetupFailure),
                Phase::IsolationStart => self.outcome != Outcome::Isolate,
                Phase::IsolationEnd => self.outcome != Outcome::Restore,
                _ => !matches!(
                    self.outcome,
                    Outcome::Continue | Outcome::Drop | Outcome::Disconnect
                ),
            }
        {
            return Err("invalid fault effects for phase".into());
        }
        Ok(())
    }
    fn merge(&mut self, other: Self) -> Result<(), String> {
        self.delay_ns = self
            .delay_ns
            .checked_add(other.delay_ns)
            .filter(|delay| *delay <= MAX_DELAY_NS)
            .ok_or("combined fault delay bound")?;
        self.throttle_ms = self.throttle_ms.max(other.throttle_ms);
        if let Some(error) = other.reject_error {
            if self.reject_error.is_some_and(|old| old != error) {
                return Err("conflicting fault rejections".into());
            }
            self.reject_error = Some(error);
        }
        self.outcome = match (self.outcome, other.outcome) {
            (left, Outcome::Continue) => left,
            (Outcome::Continue, right) => right,
            (Outcome::Drop, Outcome::Disconnect) | (Outcome::Disconnect, Outcome::Drop) => {
                Outcome::Disconnect
            }
            (left, right) if left == right => left,
            _ => return Err("conflicting fault outcomes".into()),
        };
        Ok(())
    }
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ScriptRule {
    pub matcher: Match,
    pub skip: u32,
    pub take: u32,
    pub effects: Effects,
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct RandomRule {
    pub matcher: Match,
    /// A fixed one-draw threshold with resolution 1/2^64; the represented
    /// probability is floor(ppm * 2^64 / 1_000_000) / 2^64.
    pub probability_ppm: u32,
    pub max_delay_ns: u64,
    pub outcome: Outcome,
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct IsolationWindow {
    pub broker: i32,
    pub start_ns: u64,
    pub end_ns: u64,
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct BrokerService {
    pub broker: i32,
    pub delay_ns: u64,
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct FaultConfig {
    /// Experiment crash semantics: abandon frames whose pre-append service
    /// interval intersects an isolation. Default isolation only closes sockets.
    #[serde(default, skip_serializing_if = "zero")]
    pub crash_on_isolation: bool,
    pub scripts: Vec<ScriptRule>,
    pub random: Vec<RandomRule>,
    pub isolations: Vec<IsolationWindow>,
    pub services: Vec<BrokerService>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub links: Vec<crate::BrokerLink>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub link_outages: Vec<crate::LinkOutage>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub environment: Vec<EnvironmentRule>,
    /// Hard retained tape cap; exhaustion fails the run instead of truncating.
    pub max_decisions: usize,
    /// Applied finite-script/random effects across every API and setup in one
    /// round. Service delays and isolation windows are fixed environment and
    /// remain active independently of this budget. Zero disables injections.
    pub budget_per_round: u32,
    /// Optional random effects also obey this cap, leaving shared budget for
    /// mandatory finite scripts. Their RNG draws continue after exhaustion.
    pub max_random_effects_per_round: u32,
}
impl Default for FaultConfig {
    fn default() -> Self {
        Self {
            crash_on_isolation: false,
            scripts: Vec::new(),
            random: Vec::new(),
            isolations: Vec::new(),
            services: Vec::new(),
            links: Vec::new(),
            link_outages: Vec::new(),
            environment: Vec::new(),
            max_decisions: 65_536,
            budget_per_round: 8,
            max_random_effects_per_round: 3,
        }
    }
}
impl FaultConfig {
    pub fn validate(&self) -> Result<(), String> {
        crate::experiment_link::validate(self)?;
        environment::validate(&self.environment)?;
        if self.scripts.len() > MAX_RULES
            || self.random.len() > MAX_RANDOM_RULES
            || self.isolations.len() > MAX_ENVIRONMENT
            || self.services.len() > MAX_ENVIRONMENT
            || self.max_decisions == 0
            || self.max_decisions > MAX_DECISIONS
            || self.budget_per_round > 1024
            || self.max_random_effects_per_round > 32
        {
            return Err("fault configuration capacity".into());
        }
        for rule in &self.scripts {
            rule.matcher.validate()?;
            rule.effects.validate(rule.matcher.phase)?;
            if rule.take == 0
                || rule.skip.checked_add(rule.take).is_none()
                || rule.effects.is_empty()
                || rule.effects.reject_error.is_some() && rule.matcher.api != Some(0)
            {
                return Err("empty or overflowing finite fault script".into());
            }
        }
        for rule in &self.random {
            rule.matcher.validate()?;
            Effects {
                delay_ns: rule.max_delay_ns,
                outcome: rule.outcome,
                ..Default::default()
            }
            .validate(rule.matcher.phase)?;
            if rule.probability_ppm > 1_000_000 {
                return Err("fault probability exceeds one".into());
            }
        }
        for (index, window) in self.isolations.iter().enumerate() {
            if window.broker < 0
                || window.start_ns >= window.end_ns
                || self.isolations[..index].iter().any(|other| {
                    other.broker == window.broker
                        && other.start_ns < window.end_ns
                        && window.start_ns < other.end_ns
                })
            {
                return Err("invalid or overlapping broker isolation".into());
            }
        }
        for (index, service) in self.services.iter().enumerate() {
            if service.broker < 0
                || service.delay_ns > MAX_DELAY_NS
                || self.services[..index]
                    .iter()
                    .any(|other| other.broker == service.broker)
            {
                return Err("invalid or repeated broker service delay".into());
            }
        }
        Ok(())
    }
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct Hook {
    pub phase: Phase,
    /// Relative to ReplayManifest.start_ns; history's outer time stays absolute.
    pub now_ns: u64,
    pub broker: i32,
    pub connection: u64,
    /// Per-connection frame ordinal; zero for setup, window ordinal for timers.
    pub frame: u64,
    pub api: Option<i16>,
    pub correlation: Option<i32>,
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct Decision {
    pub hook_id: u64,
    pub round: u32,
    pub hook: Hook,
    pub effects: Effects,
    /// Matching probabilistic environment draws precede two draws per matching
    /// budgeted random rule. Deterministic environment rules draw nothing.
    pub draws: Vec<u64>,
    /// Only finite script/random effects charge the round budget.
    pub effects_applied: u32,
    #[serde(default, skip_serializing_if = "zero")]
    pub environment_effects: u32,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub environment_indices: Vec<usize>,
    pub script_effects: u32,
    /// Unique configured script indices applied by this hook.
    pub script_indices: Vec<usize>,
    pub random_effects: u32,
    pub budget_remaining: u32,
    pub random_remaining: u32,
}
#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct FaultStats {
    #[serde(default, skip_serializing_if = "zero")]
    pub environment_effects: u64,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub environment_firings: Vec<u32>,
    pub decisions: u64,
    pub script_effects: u64,
    pub script_firings: Vec<u32>,
    pub random_effects: u64,
    pub setup_failures: u64,
    pub dropped_frames: u64,
    pub disconnects: u64,
    pub delayed_hooks: u64,
    pub isolation_closed: u64,
    /// Explicit response loss after this frame actually appended new records.
    pub committed_response_losses: u64,
}
pub struct FaultEngine {
    config: FaultConfig,
    replay: Option<Vec<Decision>>,
    tape: Vec<Decision>,
    script_visits: Vec<u32>,
    round: u32,
    remaining: u32,
    random_remaining: u32,
    stats: FaultStats,
}
impl FaultEngine {
    pub fn new(config: FaultConfig, replay: Option<Vec<Decision>>) -> Result<Self, String> {
        config.validate()?;
        if replay.as_ref().is_some_and(|tape| {
            tape.len() > config.max_decisions
                || tape.iter().any(|decision| {
                    decision.draws.len() > MAX_ENVIRONMENT + MAX_RANDOM_RULES * 2
                        || decision.environment_indices.len() > MAX_ENVIRONMENT
                        || decision.script_indices.len() > MAX_RULES
                })
        }) {
            return Err("fault replay tape capacity".into());
        }
        let mut tape = Vec::new();
        tape.try_reserve_exact(config.max_decisions.min(262_144))
            .map_err(|_| "fault tape allocation")?;
        let script_visits = vec![0; config.scripts.len()];
        let stats = FaultStats {
            script_firings: vec![0; config.scripts.len()],
            environment_firings: vec![0; config.environment.len()],
            ..Default::default()
        };
        Ok(Self {
            remaining: config.budget_per_round,
            random_remaining: config.max_random_effects_per_round,
            config,
            replay,
            tape,
            script_visits,
            round: 0,
            stats,
        })
    }
    /// Repeating the current round is a no-op, never a budget refill.
    pub fn begin_round(&mut self, round: u32) -> Result<(), String> {
        if round < self.round {
            return Err("fault rounds moved backwards".into());
        }
        if round != self.round {
            self.round = round;
            self.remaining = self.config.budget_per_round;
            self.random_remaining = self.config.max_random_effects_per_round;
        }
        Ok(())
    }
    pub fn tape(&self) -> &[Decision] {
        &self.tape
    }
    pub fn stats(&self) -> &FaultStats {
        &self.stats
    }
    pub(crate) fn isolations(&self) -> &[IsolationWindow] {
        &self.config.isolations
    }
    pub(crate) fn has_service(&self, broker: i32) -> bool {
        self.config
            .services
            .iter()
            .any(|service| service.broker == broker)
    }
    pub fn finish(&self) -> Result<(), String> {
        if self
            .replay
            .as_ref()
            .is_some_and(|tape| tape.len() != self.tape.len())
        {
            return Err("unconsumed fault replay decisions".into());
        }
        Ok(())
    }
    pub(crate) fn committed_response_lost(&mut self) -> Result<(), String> {
        self.stats.committed_response_losses = self
            .stats
            .committed_response_losses
            .checked_add(1)
            .ok_or("committed response loss counter overflow")?;
        Ok(())
    }
    pub(crate) fn isolation_closed(&mut self) -> Result<(), String> {
        self.stats.isolation_closed = self
            .stats
            .isolation_closed
            .checked_add(1)
            .ok_or("isolation counter overflow")?;
        Ok(())
    }
    fn validate_replay_sources(&self, decision: &Decision) -> Result<(), String> {
        if decision.script_indices.len() != decision.script_effects as usize {
            return Err("fault replay script index count mismatch".into());
        }
        let hook = &decision.hook;
        if decision.draws.len() != self.expected_draws(hook) {
            return Err("fault replay draw count mismatch".into());
        }
        let mut base = self.base_effects(hook);
        let mut draws = decision.draws.iter();
        let indices = self.apply_environment(hook, &mut base, || {
            draws
                .next()
                .copied()
                .ok_or("missing environment draw".into())
        })?;
        if indices != decision.environment_indices
            || indices.len() != decision.environment_effects as usize
        {
            return Err("fault replay environment source mismatch".into());
        }
        for (position, index) in decision.script_indices.iter().copied().enumerate() {
            let rule = self
                .config
                .scripts
                .get(index)
                .ok_or("fault replay script index out of range")?;
            let visit = self.script_visits[index];
            if decision.script_indices[..position].contains(&index)
                || !rule.matcher.matches(hook, self.round)
                || visit < rule.skip
                || visit - rule.skip >= rule.take
            {
                return Err("fault replay script index not eligible".into());
            }
            base.merge(rule.effects)?;
        }
        if decision.random_effects == 0 {
            if decision.effects != base {
                return Err("fault replay uncharged effects".into());
            }
            return Ok(());
        }
        if decision.effects.reject_error != base.reject_error
            || decision.effects.throttle_ms != base.throttle_ms
        {
            return Err("fault replay effects lack a charged matching source".into());
        }
        let mut matching = [None; MAX_RANDOM_RULES];
        let mut count = 0;
        for rule in &self.config.random {
            if rule.matcher.matches(hook, self.round) {
                matching[count] = Some(rule);
                count += 1;
            }
        }
        // Recorded effects are authoritative: do not re-evaluate probability
        // or choose a delay from a RNG draw. Validate that exactly the charged
        // number of distinct matching sources can produce the recorded shape.
        // Eight configured random rules bound this search to 256 subsets.
        let mut supported = false;
        for mask in 0u32..(1 << count) {
            if mask.count_ones() != decision.random_effects {
                continue;
            }
            let mut candidate = base;
            let mut minimum_delay = base.delay_ns;
            let mut maximum_delay = base.delay_ns;
            let mut valid = true;
            for (index, rule) in matching[..count].iter().enumerate() {
                if mask & (1 << index) == 0 {
                    continue;
                }
                let rule = rule.expect("populated matching rule");
                if rule.outcome == Outcome::Continue {
                    if rule.max_delay_ns == 0 {
                        valid = false;
                        break;
                    }
                    minimum_delay += 1; // A charged Continue must actually delay.
                }
                maximum_delay += rule.max_delay_ns;
                candidate.merge(Effects {
                    outcome: rule.outcome,
                    ..Default::default()
                })?;
            }
            supported |= valid
                && candidate.outcome == decision.effects.outcome
                && minimum_delay <= decision.effects.delay_ns
                && decision.effects.delay_ns <= maximum_delay;
            if supported {
                break;
            }
        }
        if !supported {
            return Err("fault replay effects lack a charged matching source".into());
        }
        Ok(())
    }
    pub fn decide(
        &mut self,
        hook: Hook,
        draw: &mut impl FnMut() -> Result<u64, String>,
    ) -> Result<Decision, String> {
        let timer = matches!(hook.phase, Phase::IsolationStart | Phase::IsolationEnd);
        if hook.broker < 0
            || hook.api.is_some_and(|api| api < 0)
            || if timer {
                hook.connection != 0
                    || hook.frame == 0
                    || hook.api.is_some()
                    || hook.correlation.is_some()
            } else if hook.phase == Phase::Setup {
                hook.connection == 0
                    || hook.frame != 0
                    || hook.api.is_some()
                    || hook.correlation.is_some()
            } else {
                hook.connection == 0
                    || hook.frame == 0
                    || hook.api.is_none()
                    || hook.correlation.is_none()
            }
        {
            return Err("invalid broker fault hook".into());
        }
        if self.tape.len() == self.config.max_decisions {
            return Err("fault decision capacity exhausted".into());
        }
        // Preserve old gate reservation sizes; grow large experiment tapes only
        // as used, before consuming RNG or mutating decision accounting.
        self.tape
            .try_reserve(1)
            .map_err(|_| "fault tape allocation")?;
        let id = self.tape.len() as u64 + 1;
        let decision = if let Some(replay) = &self.replay {
            let decision = replay
                .get(self.tape.len())
                .ok_or("missing fault replay decision")?;
            if decision.hook_id != id
                || decision.round != self.round
                || decision.hook != hook
                || decision.effects_applied > self.remaining
                || decision.budget_remaining != self.remaining - decision.effects_applied
                || decision.random_effects > self.random_remaining
                || decision.random_remaining != self.random_remaining - decision.random_effects
                || decision.script_effects.checked_add(decision.random_effects)
                    != Some(decision.effects_applied)
                || decision.script_effects as usize > self.config.scripts.len()
                || decision.random_effects as usize > self.config.random.len()
            {
                return Err("fault replay hook or budget mismatch".into());
            }
            decision.effects.validate(hook.phase)?;
            self.validate_replay_sources(decision)?;
            let expected_draws = self.expected_draws(&hook);
            if decision.draws.len() != expected_draws {
                return Err("fault replay draw count mismatch".into());
            }
            for expected in &decision.draws {
                if draw()? != *expected {
                    return Err("fault replay RNG draw mismatch".into());
                }
            }
            decision.clone()
        } else {
            let mut effects = self.base_effects(&hook);
            let mut draws = Vec::new();
            draws
                .try_reserve_exact(self.expected_draws(&hook))
                .map_err(|_| "fault draw allocation")?;
            let environment_indices = self.apply_environment(&hook, &mut effects, || {
                let value = draw()?;
                draws.push(value);
                Ok(value)
            })?;
            let mut applied = 0;
            let mut script_effects = 0;
            let mut script_indices = Vec::new();
            script_indices
                .try_reserve_exact(self.config.scripts.len())
                .map_err(|_| "fault script index allocation")?;
            let mut random_effects = 0;
            for (index, rule) in self.config.scripts.iter().enumerate() {
                if !rule.matcher.matches(&hook, self.round) {
                    continue;
                }
                let visit = self.script_visits[index];
                if visit >= rule.skip && visit - rule.skip < rule.take && applied < self.remaining {
                    effects.merge(rule.effects)?;
                    applied += 1;
                    script_effects += 1;
                    script_indices.push(index);
                }
            }
            for rule in &self.config.random {
                if !rule.matcher.matches(&hook, self.round) {
                    continue;
                }
                let hit = draw()?;
                let delay = draw()?;
                draws.extend([hit, delay]);
                let threshold = (u128::from(rule.probability_ppm) << 64) / 1_000_000;
                let candidate = Effects {
                    delay_ns: ((u128::from(delay) * (u128::from(rule.max_delay_ns) + 1)) >> 64)
                        as u64,
                    outcome: rule.outcome,
                    ..Default::default()
                };
                if u128::from(hit) < threshold
                    && applied < self.remaining
                    && random_effects < self.random_remaining
                    && !candidate.is_empty()
                {
                    effects.merge(candidate)?;
                    applied += 1;
                    random_effects += 1;
                }
            }
            Decision {
                hook_id: id,
                round: self.round,
                hook,
                effects,
                draws,
                effects_applied: applied,
                environment_effects: environment_indices.len() as u32,
                environment_indices,
                script_effects,
                script_indices,
                random_effects,
                budget_remaining: self.remaining - applied,
                random_remaining: self.random_remaining - random_effects,
            }
        };
        for (index, rule) in self.config.scripts.iter().enumerate() {
            if rule.matcher.matches(&decision.hook, self.round) {
                self.script_visits[index] = self.script_visits[index]
                    .checked_add(1)
                    .ok_or("fault match counter overflow")?;
            }
        }
        for index in &decision.script_indices {
            self.stats.script_firings[*index] = self.stats.script_firings[*index]
                .checked_add(1)
                .ok_or("fault script firing overflow")?;
        }
        for index in &decision.environment_indices {
            self.stats.environment_firings[*index] = self.stats.environment_firings[*index]
                .checked_add(1)
                .ok_or("environment firing overflow")?;
        }
        self.stats.environment_effects += u64::from(decision.environment_effects);
        self.remaining = decision.budget_remaining;
        self.random_remaining = decision.random_remaining;
        self.stats.decisions += 1;
        self.stats.script_effects += u64::from(decision.script_effects);
        self.stats.random_effects += u64::from(decision.random_effects);
        self.stats.setup_failures += u64::from(decision.effects.outcome == Outcome::SetupFailure);
        self.stats.dropped_frames += u64::from(decision.effects.outcome == Outcome::Drop);
        self.stats.disconnects += u64::from(decision.effects.outcome == Outcome::Disconnect);
        self.stats.delayed_hooks += u64::from(decision.effects.delay_ns != 0);
        self.tape.push(decision.clone());
        Ok(decision)
    }
}

fn zero<T: Default + PartialEq>(value: &T) -> bool {
    *value == T::default()
}

#[cfg(test)]
mod tests;
