use super::*;

fn submit_with_timeout(
    f: &mut Fixture,
    partition: Option<i32>,
    choice: PartitionChoice,
    now: u64,
    timeout: u64,
) {
    let records = [RecordDescriptor {
        topic: f.topic,
        partition_hint: partition,
        lane_hint: None,
        key: None,
        value: Some(&[42; 80]),
        headers: &[],
        timestamp_ms: 0,
        user_token: 0,
        delivery_timeout: Some(RuntimeDuration::from_nanos(timeout)),
    }];
    let (result, batch) = f.admission.prepare_copy(at(now), &records, &[Ok(0)]);
    assert_eq!(result.accepted, 1, "{:?}", result.error);
    f.engine.admit(at(now), batch.unwrap(), &[choice]).unwrap();
}

#[test]
fn younger_expiry_waits_for_inflight_success_and_flush_follows_both_deliveries() {
    for compression in [Compression::None, Compression::Zstd { level: 1 }] {
        let mut config = config();
        config.compression = compression;
        config.codec_contexts = u8::from(compression != Compression::None);
        let mut f = Fixture::new(config, 2);
        f.submit(1, 0, 0);
        let head = f.dispatch(1);
        submit_with_timeout(&mut f, Some(0), PartitionChoice::Partition(0), 2, 10);
        f.submit(1, 1, 2);
        let flush = f.engine.flush(at(3), RecordToken(2)).unwrap();
        f.engine.on_deadline(at(12), budget());
        assert!(deliveries(&f.events()).is_empty());
        // An unrelated partition can complete while partition zero is blocked.
        f.engine.cancel(at(12), RecordToken(3)).unwrap();
        let other = deliveries(&f.events());
        assert_eq!(
            other.iter().map(|event| event.token.0).collect::<Vec<_>>(),
            [3]
        );
        let events = f.answer(head, 13, FaultPlan::default());
        let delivered = deliveries(&events);
        assert_eq!(
            delivered
                .iter()
                .map(|event| event.token.0)
                .collect::<Vec<_>>(),
            [1, 2]
        );
        assert_eq!(delivered[0].outcome, DeliveryOutcome::ACKED);
        assert_eq!(
            delivered[1].outcome,
            DeliveryOutcome::not_written(FailureReason::Deadline)
        );
        let fence = events
            .iter()
            .position(|event| *event == Event::FlushDone { token: flush })
            .unwrap();
        assert_eq!(deliveries(&events[..fence]).len(), 2);
    }
}

#[test]
fn an_unhinted_predecessor_blocks_only_until_its_partition_is_known() {
    let mut f = Fixture::new(config(), 2);
    submit_with_timeout(&mut f, None, PartitionChoice::Pending, 0, 1000);
    submit_with_timeout(&mut f, None, PartitionChoice::Partition(0), 0, 10);
    f.engine.on_deadline(at(10), budget());
    assert!(deliveries(&f.events()).is_empty());
    f.engine
        .route_pending(at(11), RecordToken(1), PartitionChoice::Partition(1))
        .unwrap();
    let delivered = deliveries(&f.events());
    assert_eq!(
        delivered
            .iter()
            .map(|event| event.token.0)
            .collect::<Vec<_>>(),
        [2]
    );
    f.engine.cancel(at(11), RecordToken(1)).unwrap();
    assert_eq!(deliveries(&f.events())[0].token, RecordToken(1));
}

#[test]
fn pending_unhinted_predecessor_routed_to_same_partition_keeps_native_admission_order() {
    let mut f = Fixture::new(config(), 1);
    submit_with_timeout(&mut f, None, PartitionChoice::Pending, 0, 1000);
    submit_with_timeout(&mut f, None, PartitionChoice::Partition(0), 0, 10);
    f.engine.on_deadline(at(10), budget());
    assert!(deliveries(&f.events()).is_empty());
    f.engine
        .route_pending(at(11), RecordToken(1), PartitionChoice::Partition(0))
        .unwrap();
    assert!(deliveries(&f.events()).is_empty());
    f.engine.cancel(at(11), RecordToken(1)).unwrap();
    assert_eq!(
        deliveries(&f.events())
            .iter()
            .map(|event| event.token.0)
            .collect::<Vec<_>>(),
        [1, 2]
    );
}

#[test]
fn retry_exhaustion_and_fatal_shutdown_cannot_overtake_a_held_younger_failure() {
    for fatal in [false, true] {
        let mut config = config();
        config.max_attempts = 2;
        let mut f = Fixture::new(config, 1);
        f.submit(1, 0, 0);
        let head = f.dispatch(1);
        submit_with_timeout(&mut f, Some(0), PartitionChoice::Partition(0), 2, 10);
        f.engine.on_deadline(at(12), budget());
        assert!(deliveries(&f.events()).is_empty());
        let events = if fatal {
            f.engine.on_write_admitted(head.request).unwrap();
            f.engine
                .on_write(
                    head.connection,
                    head.correlation,
                    head.plan.len(),
                    Certainty::Applied,
                )
                .unwrap();
            f.engine.fail_producer(FailureReason::SequenceUnresolved);
            drop(head);
            f.events()
        } else {
            let fault = FaultPlan {
                reject_before_commit: Some(code::NOT_ENOUGH_REPLICAS),
                ..Default::default()
            };
            assert!(deliveries(&f.answer(head, 13, fault)).is_empty());
            let retry = f.dispatch(1000);
            let mut events = f.answer(retry, 1001, fault);
            for step in 0..64 {
                f.engine.on_deadline(at(2000 + step), budget());
                f.engine.encode(at(2000 + step), budget());
                f.engine.schedule(at(2000 + step), budget());
                while let Some(order) = f.engine.pop_order() {
                    match order {
                        EngineOrder::Connect { key, .. } => f
                            .engine
                            .on_connection(key, at(2000 + step), ConnectionEvent::Active)
                            .unwrap(),
                        EngineOrder::Retire { connection, .. } => f
                            .engine
                            .on_connection(connection, at(2000 + step), ConnectionEvent::Released)
                            .unwrap(),
                        EngineOrder::Dispatch { .. } => {
                            panic!("attempt limit admitted a third request")
                        }
                        _ => {}
                    }
                }
                events.extend(f.events());
                if !deliveries(&events).is_empty() {
                    break;
                }
            }
            events
        };
        let delivered = deliveries(&events);
        assert_eq!(
            delivered
                .iter()
                .map(|event| event.token.0)
                .collect::<Vec<_>>(),
            [1, 2],
            "fatal={fatal} events={events:?} status={:?}",
            f.engine.status()
        );
        assert_ne!(delivered[0].outcome.kind, DeliveryKind::Acked);
        assert_eq!(
            delivered[1].outcome,
            DeliveryOutcome::not_written(FailureReason::Deadline)
        );
        if fatal {
            assert_eq!(delivered[0].outcome.kind, DeliveryKind::Unknown);
        } else {
            assert_eq!(delivered[0].attempts, 2);
        }
    }
}
