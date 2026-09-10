use super::*;

#[test]
fn repeated_broker_size_rejection_keeps_the_engine_live() {
    for error in [code::MESSAGE_TOO_LARGE, code::RECORD_LIST_TOO_LARGE] {
        let mut f = Fixture::new(config(), 4);
        for partition in 0..4 {
            f.submit(12, partition, 0);
        }
        let mut terminal = Vec::new();
        for step in 0..48 {
            let request = f.dispatch(step * 100);
            terminal.extend(deliveries(&f.answer(
                request,
                step * 100 + 1,
                FaultPlan {
                    reject_before_commit: Some(error),
                    ..Default::default()
                },
            )));
            assert!(!f.engine.status().failed, "step {step}");
            if terminal.len() == 48 {
                break;
            }
        }
        assert_eq!(terminal.len(), 48);
        assert!(terminal.iter().all(|event| event.outcome
            == DeliveryOutcome::not_written(FailureReason::CompressedTooLarge)
            && event.attempts == 1));
        assert!(f.broker.log().is_empty());
        f.submit(1, 0, 5_000);
        let request = f.dispatch(5_001);
        let delivered = deliveries(&f.answer(request, 5_002, FaultPlan::default()));
        assert_eq!(delivered.len(), 1);
        assert_eq!(delivered[0].token, RecordToken(49));
        assert_eq!(delivered[0].outcome.kind, DeliveryKind::Acked);
        assert!(!f.engine.status().failed);
    }
}
