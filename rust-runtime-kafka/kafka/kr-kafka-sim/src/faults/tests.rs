use super::*;
fn hook(phase: Phase, frame: u64) -> Hook {
    Hook {
        phase,
        now_ns: frame * 10,
        broker: 2,
        connection: 7,
        frame,
        api: Some(0),
        correlation: Some(frame as i32),
    }
}
fn no_draw() -> Result<u64, String> {
    panic!("unexpected Fault RNG draw")
}

fn environment_rule() -> EnvironmentRule {
    EnvironmentRule {
        broker: Some(2),
        api: Some(0),
        phase: Phase::BeforeAppend,
        start_ns: 10,
        end_ns: 110,
        probability_ppm: 1_000_000,
        effects: Effects {
            delay_ns: 0,
            ..Effects::default()
        },
        ramp: Some(Ramp { end_delay_ns: 1000 }),
    }
}

#[test]
fn deterministic_environment_outlives_empty_budgets_and_uses_half_open_ramps() {
    let mut engine = FaultEngine::new(
        FaultConfig {
            environment: vec![environment_rule()],
            budget_per_round: 0,
            max_random_effects_per_round: 0,
            ..FaultConfig::default()
        },
        None,
    )
    .unwrap();
    for (frame, delay, fire) in [
        (1, 0, 1),
        (2, 100, 1),
        (6, 500, 1),
        (10, 900, 1),
        (11, 0, 0),
    ] {
        let decision = engine
            .decide(hook(Phase::BeforeAppend, frame), &mut no_draw)
            .unwrap();
        assert_eq!(decision.effects.delay_ns, delay);
        assert_eq!(decision.environment_effects, fire);
        assert_eq!(
            (
                decision.effects_applied,
                decision.budget_remaining,
                decision.random_remaining
            ),
            (0, 0, 0)
        );
        assert!(decision.draws.is_empty());
    }
    assert_eq!(engine.stats().environment_effects, 4);
    assert_eq!(engine.stats().environment_firings, [4]);
    let mut falling = environment_rule();
    falling.effects.delay_ns = 1000;
    falling.ramp = Some(Ramp { end_delay_ns: 0 });
    let mut engine = FaultEngine::new(
        FaultConfig {
            environment: vec![falling],
            ..FaultConfig::default()
        },
        None,
    )
    .unwrap();
    assert_eq!(
        engine
            .decide(hook(Phase::BeforeAppend, 6), &mut no_draw)
            .unwrap()
            .effects
            .delay_ns,
        500
    );
}

#[test]
fn environment_draws_precede_random_draws_and_replay_checks_exact_environment_sources() {
    let mut probability = environment_rule();
    probability.probability_ppm = 500_000;
    probability.effects = Effects {
        outcome: Outcome::Drop,
        ..Effects::default()
    };
    probability.ramp = None;
    let config = FaultConfig {
        environment: vec![environment_rule(), probability],
        scripts: vec![ScriptRule {
            matcher: Match {
                api: Some(0),
                ..Match::default()
            },
            skip: 0,
            take: 1,
            effects: Effects {
                delay_ns: 7,
                ..Effects::default()
            },
        }],
        random: vec![RandomRule {
            matcher: Match {
                api: Some(0),
                ..Match::default()
            },
            probability_ppm: 1_000_000,
            max_delay_ns: 5,
            outcome: Outcome::Continue,
        }],
        budget_per_round: 1,
        max_random_effects_per_round: 1,
        ..FaultConfig::default()
    };
    let mut engine = FaultEngine::new(config.clone(), None).unwrap();
    for (frame, env_draw, hit) in [(1, 0, true), (2, u64::MAX, false), (3, 0, true)] {
        let mut draws = [env_draw, 17, 29].into_iter();
        let decision = engine
            .decide(hook(Phase::BeforeAppend, frame), &mut || {
                Ok(draws.next().unwrap())
            })
            .unwrap();
        assert!(draws.next().is_none());
        assert_eq!(decision.draws, [env_draw, 17, 29]);
        assert_eq!(
            decision.environment_indices,
            if hit { vec![0, 1] } else { vec![0] }
        );
        assert_eq!(decision.random_effects, 0);
        assert_eq!(decision.budget_remaining, 0);
    }
    let tape = engine.tape().to_vec();
    let mut replay = FaultEngine::new(config.clone(), Some(tape.clone())).unwrap();
    for decision in &tape {
        let mut draws = decision.draws.iter().copied();
        assert_eq!(
            replay
                .decide(decision.hook.clone(), &mut || Ok(draws.next().unwrap()))
                .unwrap(),
            *decision
        );
    }
    replay.finish().unwrap();
    assert_eq!(replay.stats(), engine.stats());
    for mutation in 0..4 {
        let mut bad = tape.clone();
        match mutation {
            0 => bad[0].environment_indices.reverse(),
            1 => bad[0].environment_effects -= 1,
            2 => bad[0].draws[0] = u64::MAX,
            _ => {
                bad[0].draws.pop();
            }
        }
        let mut replay = FaultEngine::new(config.clone(), Some(bad)).unwrap();
        assert!(replay.decide(tape[0].hook.clone(), &mut || Ok(0)).is_err());
    }
    let mut changed = config;
    changed.environment[0].effects.delay_ns = 1;
    let mut replay = FaultEngine::new(changed, Some(tape.clone())).unwrap();
    assert!(replay.decide(tape[0].hook.clone(), &mut || Ok(0)).is_err());
}

#[test]
fn environment_zero_probability_still_draws_and_conflicting_deterministic_windows_are_rejected() {
    let mut rule = environment_rule();
    rule.probability_ppm = 0;
    let mut engine = FaultEngine::new(
        FaultConfig {
            environment: vec![rule.clone()],
            ..FaultConfig::default()
        },
        None,
    )
    .unwrap();
    let decision = engine
        .decide(hook(Phase::BeforeAppend, 2), &mut || Ok(0))
        .unwrap();
    assert_eq!(decision.draws, [0]);
    assert_eq!(decision.environment_effects, 0);
    let mut a = environment_rule();
    a.effects.reject_error = Some(19);
    let mut b = a.clone();
    b.effects.reject_error = Some(7);
    let mut config = FaultConfig {
        environment: vec![a, b],
        ..FaultConfig::default()
    };
    assert!(config.validate().unwrap_err().contains("conflicting"));
    config.environment[1].broker = Some(3);
    config.validate().unwrap();
    config.environment[1].broker = Some(2);
    config.environment[1].start_ns = 110;
    config.environment[1].end_ns = 210;
    config.validate().unwrap();
}
fn script() -> ScriptRule {
    ScriptRule {
        matcher: Match {
            api: Some(0),
            broker: Some(2),
            connection: Some(7),
            round: Some(0),
            ..Default::default()
        },
        skip: 1,
        take: 2,
        effects: Effects {
            outcome: Outcome::Disconnect,
            ..Default::default()
        },
    }
}
#[test]
fn finite_scripts_match_all_dimensions_and_never_restart_on_new_round() {
    let mut engine = FaultEngine::new(
        FaultConfig {
            scripts: vec![script()],
            ..Default::default()
        },
        None,
    )
    .unwrap();
    for field in 0..4 {
        let mut other = hook(Phase::BeforeAppend, 1);
        match field {
            0 => other.phase = Phase::AfterAppend,
            1 => other.api = Some(3),
            2 => other.broker = 3,
            _ => other.connection = 8,
        }
        assert_eq!(
            engine.decide(other, &mut no_draw).unwrap().effects,
            Effects::default()
        );
    }
    for (frame, expected) in [
        (1, Outcome::Continue),
        (2, Outcome::Disconnect),
        (3, Outcome::Disconnect),
        (4, Outcome::Continue),
    ] {
        assert_eq!(
            engine
                .decide(hook(Phase::BeforeAppend, frame), &mut no_draw)
                .unwrap()
                .effects
                .outcome,
            expected
        );
    }
    engine.begin_round(1).unwrap();
    assert_eq!(
        engine
            .decide(hook(Phase::BeforeAppend, 5), &mut no_draw)
            .unwrap()
            .effects
            .outcome,
        Outcome::Continue
    );
    assert_eq!(engine.stats().script_effects, 2);
}
#[test]
fn round_budget_counts_effects_across_script_and_random_and_same_round_cannot_refill() {
    let mut first = script();
    first.skip = 0;
    first.take = 10;
    first.matcher.round = None;
    let config = FaultConfig {
        scripts: vec![first],
        random: vec![RandomRule {
            matcher: Match::default(),
            probability_ppm: 1_000_000,
            max_delay_ns: 1,
            outcome: Outcome::Drop,
        }],
        budget_per_round: 2,
        ..Default::default()
    };
    let mut engine = FaultEngine::new(config, None).unwrap();
    let a = engine
        .decide(hook(Phase::BeforeAppend, 1), &mut || Ok(0))
        .unwrap();
    assert_eq!(
        (a.script_effects, a.random_effects, a.budget_remaining),
        (1, 1, 0)
    );
    assert_eq!(a.effects.outcome, Outcome::Disconnect);
    engine.begin_round(0).unwrap();
    let b = engine
        .decide(hook(Phase::BeforeAppend, 2), &mut || Ok(0))
        .unwrap();
    assert_eq!(b.draws, vec![0, 0]);
    assert_eq!(b.effects, Effects::default());
    engine.begin_round(1).unwrap();
    assert_eq!(
        engine
            .decide(hook(Phase::BeforeAppend, 3), &mut || Ok(0))
            .unwrap()
            .effects_applied,
        2
    );
    assert!(engine.begin_round(0).is_err());
}
#[test]
fn random_probability_endpoints_delay_bounds_and_no_effect_draws_are_recorded() {
    for probability in [0, 1_000_000] {
        let config = FaultConfig {
            random: vec![RandomRule {
                matcher: Match::default(),
                probability_ppm: probability,
                max_delay_ns: 99,
                outcome: Outcome::Disconnect,
            }],
            ..Default::default()
        };
        let mut engine = FaultEngine::new(config, None).unwrap();
        let a = engine
            .decide(hook(Phase::BeforeAppend, 1), &mut || Ok(u64::MAX))
            .unwrap();
        assert_eq!(a.draws, [u64::MAX, u64::MAX]);
        assert_eq!(a.effects.delay_ns, if probability == 0 { 0 } else { 99 });
        assert_eq!(a.random_effects, u32::from(probability != 0));
        let b = engine
            .decide(hook(Phase::AfterAppend, 1), &mut no_draw)
            .unwrap();
        assert!(b.draws.is_empty());
        assert_eq!(engine.tape().len(), 2, "no-effect hooks are part of replay");
    }
}
#[test]
fn replay_consumes_draws_and_authoritative_effects_without_resampling() {
    let config = FaultConfig {
        random: vec![RandomRule {
            matcher: Match::default(),
            probability_ppm: 0,
            max_delay_ns: 100,
            outcome: Outcome::Disconnect,
        }],
        ..Default::default()
    };
    let mut first = FaultEngine::new(config.clone(), None).unwrap();
    let mut decision = first
        .decide(hook(Phase::BeforeAppend, 1), &mut || Ok(17))
        .unwrap();
    // A recorded effect is authoritative even if current probability would
    // choose none. Context, source draws, bounds and accounting still verify.
    decision.effects = Effects {
        delay_ns: 7,
        outcome: Outcome::Disconnect,
        ..Default::default()
    };
    decision.effects_applied = 1;
    decision.random_effects = 1;
    decision.budget_remaining -= 1;
    decision.random_remaining -= 1;
    let mut replay = FaultEngine::new(config, Some(vec![decision.clone()])).unwrap();
    assert_eq!(
        replay
            .decide(hook(Phase::BeforeAppend, 1), &mut || Ok(17))
            .unwrap(),
        decision
    );
    replay.finish().unwrap();
    assert_eq!(replay.stats().disconnects, 1);
    assert_eq!(replay.stats().random_effects, 1);
    assert!(
        replay
            .decide(hook(Phase::AfterAppend, 1), &mut no_draw)
            .is_err()
    );
}
#[test]
fn replay_rejects_wrong_hook_draw_budget_and_unconsumed_rows() {
    let config = FaultConfig {
        random: vec![RandomRule {
            matcher: Match::default(),
            probability_ppm: 0,
            max_delay_ns: 0,
            outcome: Outcome::Continue,
        }],
        ..Default::default()
    };
    let mut first = FaultEngine::new(config.clone(), None).unwrap();
    let decision = first
        .decide(hook(Phase::BeforeAppend, 1), &mut || Ok(42))
        .unwrap();
    for mutation in 0..5 {
        let mut altered = decision.clone();
        match mutation {
            0 => altered.hook.now_ns += 1,
            1 => altered.draws[0] ^= 1,
            2 => altered.budget_remaining -= 1,
            3 => altered.hook_id += 1,
            _ => altered.round += 1,
        }
        let mut replay = FaultEngine::new(config.clone(), Some(vec![altered])).unwrap();
        assert!(
            replay
                .decide(hook(Phase::BeforeAppend, 1), &mut || Ok(42))
                .is_err()
        );
        assert!(replay.tape().is_empty());
    }
    let replay = FaultEngine::new(config, Some(vec![decision])).unwrap();
    assert!(replay.finish().is_err());
}
#[test]
fn environment_delay_and_isolation_are_independent_of_exhausted_script_budget() {
    let mut engine = FaultEngine::new(
        FaultConfig {
            budget_per_round: 0,
            services: vec![BrokerService {
                broker: 2,
                delay_ns: 19,
            }],
            isolations: vec![IsolationWindow {
                broker: 2,
                start_ns: 20,
                end_ns: 30,
            }],
            ..Default::default()
        },
        None,
    )
    .unwrap();
    assert_eq!(
        engine
            .decide(hook(Phase::BeforeAppend, 1), &mut no_draw)
            .unwrap()
            .effects
            .delay_ns,
        19
    );
    for (now, outcome) in [
        (19, Outcome::Continue),
        (20, Outcome::SetupFailure),
        (29, Outcome::SetupFailure),
        (30, Outcome::Continue),
    ] {
        let setup = Hook {
            phase: Phase::Setup,
            now_ns: now,
            broker: 2,
            connection: 7,
            frame: 0,
            api: None,
            correlation: None,
        };
        assert_eq!(
            engine.decide(setup, &mut no_draw).unwrap().effects.outcome,
            outcome
        );
    }
    assert_eq!(engine.stats().setup_failures, 2);
}
#[test]
fn validation_rejects_unbounded_shapes_and_hook_errors_before_retention() {
    let mut config = FaultConfig {
        max_decisions: 0,
        ..Default::default()
    };
    assert!(config.validate().is_err());
    config.max_decisions = 1;
    let mut engine = FaultEngine::new(config, None).unwrap();
    let mut invalid = hook(Phase::Setup, 0);
    invalid.api = None;
    assert!(engine.decide(invalid, &mut no_draw).is_err());
    assert!(engine.tape().is_empty());
    engine
        .decide(hook(Phase::BeforeAppend, 1), &mut no_draw)
        .unwrap();
    assert!(
        engine
            .decide(hook(Phase::BeforeAppend, 2), &mut no_draw)
            .is_err()
    );
    let mut config = FaultConfig {
        isolations: vec![
            IsolationWindow {
                broker: 2,
                start_ns: 10,
                end_ns: 20,
            },
            IsolationWindow {
                broker: 2,
                start_ns: 19,
                end_ns: 30,
            },
        ],
        ..Default::default()
    };
    assert!(config.validate().is_err());
    config.isolations[1].start_ns = 20;
    assert!(config.validate().is_ok());
    config.scripts = vec![ScriptRule {
        effects: Effects {
            reject_error: Some(46),
            ..Default::default()
        },
        ..script()
    }];
    assert!(
        config.validate().is_err(),
        "a synthetic duplicate must not invent commit evidence"
    );
}

#[test]
fn replay_cannot_inject_uncharged_effects_or_counterfeit_script_realization() {
    let config = FaultConfig {
        budget_per_round: 0,
        ..Default::default()
    };
    let mut live = FaultEngine::new(config.clone(), None).unwrap();
    let mut row = live
        .decide(hook(Phase::BeforeAppend, 1), &mut no_draw)
        .unwrap();
    row.effects.outcome = Outcome::Drop;
    let mut replay = FaultEngine::new(config, Some(vec![row])).unwrap();
    assert!(
        replay
            .decide(hook(Phase::BeforeAppend, 1), &mut no_draw)
            .unwrap_err()
            .contains("uncharged")
    );

    let config = FaultConfig {
        scripts: vec![ScriptRule {
            skip: 0,
            ..script()
        }],
        ..Default::default()
    };
    let mut live = FaultEngine::new(config.clone(), None).unwrap();
    let row = live
        .decide(hook(Phase::BeforeAppend, 1), &mut no_draw)
        .unwrap();
    assert_eq!(row.script_indices, [0]);
    assert_eq!(live.stats().script_firings, [1]);
    for indices in [vec![], vec![1], vec![0, 0]] {
        let mut invalid = row.clone();
        invalid.script_indices = indices;
        let mut replay = FaultEngine::new(config.clone(), Some(vec![invalid])).unwrap();
        assert!(
            replay
                .decide(hook(Phase::BeforeAppend, 1), &mut no_draw)
                .is_err()
        );
        assert_eq!(replay.stats().script_firings, [0]);
    }
    let mut replay = FaultEngine::new(config, Some(vec![row.clone()])).unwrap();
    replay
        .decide(hook(Phase::BeforeAppend, 1), &mut no_draw)
        .unwrap();
    assert_eq!(replay.stats(), live.stats());
}

#[test]
fn replay_cannot_charge_one_random_source_for_multiple_sources_combined_delay() {
    let config = FaultConfig {
        random: vec![
            RandomRule {
                matcher: Match::default(),
                probability_ppm: 1_000_000,
                max_delay_ns: 5,
                outcome: Outcome::Drop,
            },
            RandomRule {
                matcher: Match::default(),
                probability_ppm: 1_000_000,
                max_delay_ns: 5,
                outcome: Outcome::Continue,
            },
        ],
        budget_per_round: 1,
        ..Default::default()
    };
    let mut live = FaultEngine::new(config.clone(), None).unwrap();
    let mut row = live
        .decide(hook(Phase::BeforeAppend, 1), &mut || Ok(u64::MAX))
        .unwrap();
    assert_eq!(row.random_effects, 1);
    assert_eq!(row.effects.delay_ns, 5);
    row.effects.delay_ns = 10;
    let mut replay = FaultEngine::new(config, Some(vec![row])).unwrap();
    assert!(
        replay
            .decide(hook(Phase::BeforeAppend, 1), &mut || Ok(u64::MAX))
            .is_err()
    );
}

#[test]
fn optional_random_cap_preserves_shared_budget_and_keeps_all_draws_in_tape() {
    let mut mandatory = script();
    mandatory.matcher.round = None;
    mandatory.skip = 3;
    mandatory.take = 1;
    let config = FaultConfig {
        scripts: vec![mandatory],
        random: vec![RandomRule {
            matcher: Match::default(),
            probability_ppm: 1_000_000,
            max_delay_ns: 0,
            outcome: Outcome::Drop,
        }],
        budget_per_round: 8,
        max_random_effects_per_round: 3,
        ..Default::default()
    };
    let mut live = FaultEngine::new(config.clone(), None).unwrap();
    for frame in 1..=6 {
        let row = live
            .decide(hook(Phase::BeforeAppend, frame), &mut || Ok(0))
            .unwrap();
        assert_eq!(row.draws.len(), 2);
        assert_eq!(row.random_effects, u32::from(frame <= 3));
        if frame == 4 {
            assert_eq!(row.script_indices, [0]);
        }
    }
    assert_eq!(live.stats().random_effects, 3);
    assert_eq!(live.stats().script_firings, [1]);
    assert_eq!(live.tape().last().unwrap().budget_remaining, 4);
    live.begin_round(0).unwrap();
    assert_eq!(
        live.decide(hook(Phase::BeforeAppend, 7), &mut || Ok(0))
            .unwrap()
            .random_effects,
        0
    );
    live.begin_round(1).unwrap();
    assert_eq!(
        live.decide(hook(Phase::BeforeAppend, 8), &mut || Ok(0))
            .unwrap()
            .random_effects,
        1
    );
    let mut replay = FaultEngine::new(config, Some(live.tape().to_vec())).unwrap();
    for frame in 1..=8 {
        if frame == 8 {
            replay.begin_round(1).unwrap();
        }
        assert_eq!(
            replay
                .decide(hook(Phase::BeforeAppend, frame), &mut || Ok(0))
                .unwrap(),
            live.tape()[(frame - 1) as usize]
        );
    }
    assert_eq!(replay.stats(), live.stats());
}
