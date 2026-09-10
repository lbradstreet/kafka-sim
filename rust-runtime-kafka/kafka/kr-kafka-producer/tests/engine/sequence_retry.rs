use super::*;

fn transmit(fixture: &mut Fixture, dispatch: &Dispatch) {
    fixture.engine.on_write_admitted(dispatch.request).unwrap();
    fixture
        .engine
        .on_write(
            dispatch.connection,
            dispatch.correlation,
            dispatch.plan.len(),
            Certainty::Applied,
        )
        .unwrap();
}

#[test]
fn non_head_sequence_rejection_retries_after_the_missing_head_without_rotating_identity() {
    for compression in [Compression::None, Compression::Zstd { level: 1 }] {
        let mut config = config();
        config.compression = compression;
        config.codec_contexts = u8::from(compression != Compression::None);
        config.max_in_flight_per_connection = 2;
        let mut fixture = Fixture::new(config, 1);
        let original_identity = fixture.engine.status().identity;
        // Kafka may accept a nonzero initial sequence after producer-state
        // loss. Establish real broker history before dropping a later batch.
        fixture.submit(1, 0, 0);
        let warm = fixture.dispatch(0);
        assert_eq!(
            deliveries(&fixture.answer(warm, 0, FaultPlan::default())).len(),
            1
        );
        fixture.submit(2, 0, 0);
        let first = fixture.dispatch(0);
        transmit(&mut fixture, &first);
        let dropped = fixture
            .broker
            .handle_frame(
                0,
                &first.bytes(),
                FaultPlan {
                    drop_after_parse: true,
                    ..Default::default()
                },
            )
            .unwrap();
        assert!(matches!(dropped, BrokerAction::DropRequest));
        assert_eq!(fixture.broker.log().len(), 1);
        let second = fixture.dispatch(1);
        assert_eq!(second.connection, first.connection);
        transmit(&mut fixture, &second);
        let BrokerAction::Reply(rejected) = fixture
            .broker
            .handle_frame(0, &second.bytes(), FaultPlan::default())
            .unwrap()
        else {
            panic!("expected actual out-of-order response")
        };
        let parsed = ControlCodec::from_config(fixture.engine.config())
            .unwrap()
            .parse_produce13(
                &rejected,
                second.correlation,
                &[TopicPartition {
                    topic: fixture.id,
                    partition: 0,
                }],
            )
            .unwrap();
        assert_eq!(
            parsed.partitions[0].error_code,
            code::OUT_OF_ORDER_SEQUENCE_NUMBER
        );
        // The transport retires the missing response before delivering the
        // later correlated response, while both partition assignments remain.
        fixture
            .engine
            .on_request_retired(
                first.connection,
                first.correlation,
                at(2),
                first.plan.len(),
                Certainty::Applied,
            )
            .unwrap();
        fixture
            .engine
            .on_frame(second.connection, at(2), &rejected)
            .unwrap();
        assert!(deliveries(&fixture.events()).is_empty());
        drop(first);
        drop(second);

        let head_retry = fixture.dispatch(100);
        transmit(&mut fixture, &head_retry);
        fixture.engine.schedule(at(100), budget());
        while let Some(order) = fixture.engine.pop_order() {
            assert!(
                !matches!(order, EngineOrder::Dispatch { .. }),
                "the held younger sequence must not retry before the head reconciles"
            );
        }
        let BrokerAction::Reply(accepted) = fixture
            .broker
            .handle_frame(0, &head_retry.bytes(), FaultPlan::default())
            .unwrap()
        else {
            panic!("expected head success")
        };
        fixture
            .engine
            .on_frame(head_retry.connection, at(101), &accepted)
            .unwrap();
        drop(head_retry);
        let first_delivery = deliveries(&fixture.events());
        assert_eq!(first_delivery.len(), 1);
        assert_eq!(first_delivery[0].token, RecordToken(2));
        assert_eq!(first_delivery[0].outcome.kind, DeliveryKind::Acked);
        assert_eq!(fixture.engine.status().identity, original_identity);

        let child_retry = fixture.dispatch(200);
        let events = fixture.answer(child_retry, 201, FaultPlan::default());
        let second_delivery = deliveries(&events);
        assert_eq!(second_delivery.len(), 1);
        assert_eq!(second_delivery[0].token, RecordToken(3));
        assert_eq!(second_delivery[0].outcome.kind, DeliveryKind::Acked);
        assert_eq!(second_delivery[0].attempts, 2);
        assert_eq!(fixture.engine.status().identity, original_identity);
        assert_eq!(fixture.broker.log().len(), 3);
        for (index, batch) in fixture.broker.log().iter().enumerate() {
            assert_eq!(batch.base_offset, index as i64);
            assert_eq!(batch.identity.base_sequence, index as i32);
            assert_eq!(
                batch.identity.producer_id,
                original_identity.unwrap().producer_id
            );
            assert_eq!(
                batch.identity.producer_epoch,
                original_identity.unwrap().epoch
            );
        }
    }
}
