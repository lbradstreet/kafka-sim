use super::*;

fn submit(f: &mut Fixture, partition: i32, now: u64, timeout: u64) -> RecordToken {
    let record = RecordDescriptor {
        topic: f.topic,
        partition_hint: Some(partition),
        lane_hint: None,
        key: None,
        value: Some(&[42; 80]),
        headers: &[],
        timestamp_ms: 0,
        user_token: now,
        delivery_timeout: Some(RuntimeDuration::from_nanos(timeout)),
    };
    let (result, batch) = f.admission.prepare_copy(at(now), &[record], &[Ok(0)]);
    assert_eq!(result.accepted, 1, "{:?}", result.error);
    f.engine
        .admit(
            at(now),
            batch.unwrap(),
            &[PartitionChoice::Partition(partition)],
        )
        .unwrap();
    RecordToken(f.engine.status().accepted)
}

fn transmit(f: &mut Fixture, request: &Dispatch) {
    f.engine.on_write_admitted(request.request).unwrap();
    f.engine
        .on_write(
            request.connection,
            request.correlation,
            request.plan.len(),
            Certainty::Applied,
        )
        .unwrap();
}

// Explicit driver completion, without scheduling new Produce requests. Retiring
// a connection here means its provider writes have actually completed/released.
fn settle(f: &mut Fixture, now: u64) -> (Vec<DeliveryEvent>, usize) {
    let mut delivered = Vec::new();
    let mut identity_requests = 0;
    for _ in 0..1024 {
        assert!(
            f.engine
                .on_deadline(
                    at(now),
                    WorkBudget {
                        items: 1,
                        bytes: 65536
                    }
                )
                .items
                <= 1
        );
        while let Some(order) = f.engine.pop_order() {
            match order {
                EngineOrder::Retire { connection, .. } => f
                    .engine
                    .on_connection(connection, at(now), ConnectionEvent::Released)
                    .unwrap(),
                EngineOrder::InitProducerId { previous } => {
                    assert_eq!(previous, None, "rollover requests a fresh ID like Java");
                    identity_requests += 1;
                    let codec = ControlCodec::from_config(f.engine.config()).unwrap();
                    let request = codec.init_producer_id_request(99, previous).unwrap();
                    let BrokerAction::Reply(response) = f
                        .broker
                        .handle_frame(0, &request, FaultPlan::default())
                        .unwrap()
                    else {
                        panic!("identity response")
                    };
                    f.engine
                        .install_identity(
                            codec
                                .parse_identity(&response, 99)
                                .unwrap()
                                .identity
                                .unwrap(),
                        )
                        .unwrap();
                }
                EngineOrder::Metadata { .. } => {}
                other => panic!("unexpected recovery order {other:?}"),
            }
        }
        while let Some(event) = f.engine.pop_event() {
            match event.event {
                Event::Delivery(delivery) => delivered.push(delivery),
                Event::Fatal { code } => panic!("unexpected fatal {code}"),
                _ => {}
            }
        }
        if !f.engine.has_maintenance_work() && !f.engine.has_terminal_work() {
            break;
        }
    }
    (delivered, identity_requests)
}

#[test]
fn ambiguous_expiry_recovers_and_epoch_exhaustion_rolls_over_without_replaying_terminals() {
    for initial_epoch in [0, i16::MAX - 1] {
        for appended in [false, true] {
            for compression in [Compression::None, Compression::Zstd { level: 1 }] {
                let mut config = config();
                config.compression = compression;
                config.codec_contexts = u8::from(compression != Compression::None);
                config.request_max_partitions = 1;
                let mut f = Fixture::with_epoch(config, 2, initial_epoch);
                // Establish sequence history so the delayed old record has an
                // observable position, rather than relying on absent PID state.
                f.submit(1, 0, 0);
                let warm = f.dispatch(1);
                assert_eq!(
                    deliveries(&f.answer(warm, 2, FaultPlan::default())).len(),
                    1
                );
                for round in 0..2 {
                    let now = 10_000 * round + 10;
                    let old_identity = f.engine.status().identity.unwrap();
                    let token = submit(&mut f, 0, now, 1000);
                    let old = f.dispatch(now + 1);
                    let old_bytes = old.bytes();
                    let old_connection = old.connection;
                    let old_correlation = old.correlation;
                    transmit(&mut f, &old);
                    let lost_response = if appended {
                        let BrokerAction::Reply(response) = f
                            .broker
                            .handle_frame(0, &old_bytes, FaultPlan::default())
                            .unwrap()
                        else {
                            panic!("append")
                        };
                        Some(response)
                    } else {
                        None
                    };
                    drop(old);
                    let (expired, requests) = settle(&mut f, now + 1000);
                    assert_eq!(expired.len(), 1);
                    assert_eq!(expired[0].token, token);
                    assert_eq!(
                        expired[0].outcome,
                        DeliveryOutcome::unknown(FailureReason::Deadline)
                    );
                    assert_eq!(expired[0].attempts, 1);
                    let identity = f.engine.status().identity.unwrap();
                    if let Some(next) = old_identity.next_epoch() {
                        assert_eq!(identity, next);
                        assert_eq!(requests, 0);
                    } else {
                        assert_ne!(identity.producer_id, old_identity.producer_id);
                        assert_eq!(identity.epoch, 0);
                        assert_eq!(requests, 1);
                    }
                    // Both unaffected and recovered destinations remain usable.
                    for partition in [1, 0] {
                        let probe = submit(&mut f, partition, now + 1100, 1000);
                        let request = f.dispatch(now + 1101);
                        let delivered =
                            deliveries(&f.answer(request, now + 1102, FaultPlan::default()));
                        assert_eq!(delivered.len(), 1);
                        assert_eq!(delivered[0].token, probe);
                        assert_eq!(delivered[0].outcome, DeliveryOutcome::ACKED);
                        let batch = f.broker.log().last().unwrap();
                        assert_eq!(batch.identity.producer_id, identity.producer_id);
                        assert_eq!(batch.identity.producer_epoch, identity.epoch);
                        assert_eq!(batch.identity.base_sequence, 0);
                    }
                    // A broker meta-oracle: higher epochs fence old requests.
                    // Fresh PIDs deliberately have Java's weaker rollover boundary.
                    let BrokerAction::Reply(response) = f
                        .broker
                        .handle_frame(0, &old_bytes, FaultPlan::default())
                        .unwrap()
                    else {
                        panic!("straggler response")
                    };
                    let parsed = ControlCodec::from_config(f.engine.config())
                        .unwrap()
                        .parse_produce13(
                            &response,
                            old_correlation,
                            &[TopicPartition {
                                topic: f.id,
                                partition: 0,
                            }],
                        )
                        .unwrap();
                    assert_eq!(
                        parsed.partitions[0].error_code,
                        if identity.producer_id == old_identity.producer_id {
                            code::INVALID_PRODUCER_EPOCH
                        } else {
                            code::NONE
                        }
                    );
                    if let Some(response) = lost_response {
                        assert!(
                            f.engine
                                .on_frame(old_connection, at(now + 1200), &response)
                                .is_err()
                        );
                    }
                    assert!(
                        settle(&mut f, now + 1200).0.is_empty(),
                        "no second terminal result"
                    );
                    assert!(!f.engine.is_failed());
                }
                // Old records may be stored, but none was reissued by the engine
                // under its new generation. Every append above has one record.
                let expected = 5 + if appended {
                    2
                } else if initial_epoch == i16::MAX - 1 {
                    1
                } else {
                    0
                };
                assert_eq!(f.broker.log().len(), expected);
            }
        }
    }
}

#[test]
fn global_recovery_waits_for_staggered_old_deadlines_and_preserves_queued_deadlines() {
    let mut config = config();
    config.request_max_partitions = 1;
    let mut f = Fixture::new(config, 3);
    submit(&mut f, 0, 0, 1000);
    let first = f.dispatch(1);
    transmit(&mut f, &first);
    submit(&mut f, 1, 500, 1000);
    let second = f.dispatch(501);
    transmit(&mut f, &second);
    drop(first);
    drop(second);
    assert_eq!(settle(&mut f, 1000).0.len(), 1);
    assert_eq!(f.engine.status().identity.unwrap().epoch, 0);
    // Independent healthy demand continues while the global barrier is held.
    let mut queued = Vec::new();
    for now in [1010, 1110, 1210, 1310, 1410] {
        queued.push(submit(&mut f, 2, now, 1000));
        f.engine.encode(at(now), budget());
        f.engine.schedule(at(now), budget());
        while let Some(order) = f.engine.pop_order() {
            match order {
                EngineOrder::Connect { key, .. } => f
                    .engine
                    .on_connection(key, at(now), ConnectionEvent::Active)
                    .unwrap(),
                EngineOrder::Metadata { .. } => {}
                EngineOrder::Dispatch {
                    connection,
                    request,
                    correlation,
                    plan,
                    deadline,
                } => {
                    assert_eq!(
                        deadline,
                        at(1500),
                        "only the old transmitted record may retry"
                    );
                    let retry = Dispatch {
                        connection,
                        request,
                        correlation,
                        plan,
                    };
                    transmit(&mut f, &retry);
                }
                other => panic!("unexpected order while old transmitted record remains: {other:?}"),
            }
        }
    }
    let short = submit(&mut f, 2, 1420, 10);
    settle(&mut f, 1430); // Its terminal result waits behind older partition records.
    let (expired, requests) = settle(&mut f, 1500);
    assert_eq!(requests, 0);
    assert_eq!(expired.len(), 1);
    assert_eq!(
        expired[0].outcome,
        DeliveryOutcome::unknown(FailureReason::Deadline)
    );
    assert_eq!(f.engine.status().identity.unwrap().epoch, 1);
    let mut delivered = Vec::new();
    while delivered.len() < queued.len() + 1 {
        let request = f.dispatch(1501);
        delivered.extend(deliveries(&f.answer(request, 1502, FaultPlan::default())));
    }
    assert_eq!(
        delivered
            .iter()
            .filter(|d| d.outcome == DeliveryOutcome::ACKED)
            .map(|d| d.token)
            .collect::<Vec<_>>(),
        queued
    );
    assert_eq!(delivered.last().unwrap().token, short);
    assert_eq!(
        delivered.last().unwrap().outcome,
        DeliveryOutcome::not_written(FailureReason::Deadline)
    );
}
