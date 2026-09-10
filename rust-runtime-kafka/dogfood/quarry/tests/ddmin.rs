#[allow(dead_code)]
mod support;

use support::bounded_ddmin;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum SyntheticOp {
    Setup,
    Noise(u8),
    Trigger,
}

#[test]
fn ddmin_preserves_valid_setup_and_minimizes_a_synthetic_failure() {
    let input = [
        SyntheticOp::Setup,
        SyntheticOp::Noise(1),
        SyntheticOp::Noise(2),
        SyntheticOp::Trigger,
        SyntheticOp::Noise(3),
    ];
    let result = bounded_ddmin(
        &input,
        32,
        |candidate| candidate.first() == Some(&SyntheticOp::Setup),
        |candidate| candidate.contains(&SyntheticOp::Trigger),
    );

    assert_eq!(
        result.minimized,
        vec![SyntheticOp::Setup, SyntheticOp::Trigger]
    );
    assert!(result.attempts <= 32);
    assert!(!result.attempt_limit_reached);
}

#[test]
fn ddmin_never_exceeds_its_execution_attempt_bound() {
    let input = [0, 1, 2, 3, 4, 5];
    let result = bounded_ddmin(&input, 1, |_| true, |candidate| candidate.contains(&5));

    assert_eq!(result.attempts, 1);
    assert!(result.attempt_limit_reached);
}
