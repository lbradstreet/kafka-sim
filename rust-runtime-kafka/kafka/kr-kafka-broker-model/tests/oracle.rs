use kr_kafka_broker_model::*;
use kr_kafka_record::{Identity, SharedBytes};

#[test]
fn sparse_full_width_tokens_preserve_obligations_after_rejected_admission() {
    let mut oracle = DeliveryOracle::new(OracleLimits::default());
    let ids = [u64::MAX, 0, 1 << 63];
    for (index, token) in ids.into_iter().enumerate() {
        let record = AcceptedRecord {
            token,
            topic: [1; 16],
            partition: index as i32,
            lease: None,
            returned_at: index as u64 + 1,
        };
        oracle.accept(record).unwrap();
        assert_eq!(
            oracle.accept(record).unwrap_err().invariant,
            "C1 duplicate accepted token"
        );
    }
    assert!(oracle.input_consumed(123, 5).is_err());
    // Token order and delivery order need not be the acceptance order across
    // independent partitions. Cloning must preserve all lookup obligations.
    let mut oracle = oracle.clone();
    for (index, token) in ids.into_iter().enumerate().rev() {
        oracle.input_consumed(token, 5).unwrap();
        oracle
            .delivery(ObservedDelivery {
                token,
                topic: [1; 16],
                partition: index as i32,
                outcome: ObservedOutcome::NotWritten,
                offset: None,
                timestamp: None,
                attempts: 0,
                parsed_attempts: 0,
                at: 6,
                transmitted: false,
                definitive_broker_rejection: false,
                prior_ambiguous_attempt: false,
                response: None,
            })
            .unwrap();
    }
    oracle.closed().unwrap();
    assert_eq!(oracle.finish(&[], token).unwrap().accepted, 3);
}

fn token(record: &CommittedRecord) -> Option<u64> {
    Some(u64::from_be_bytes(
        record.value.as_ref()?.as_slice().try_into().ok()?,
    ))
}
fn accepted(token: u64) -> AcceptedRecord {
    AcceptedRecord {
        token,
        topic: [1; 16],
        partition: 0,
        lease: Some(7),
        returned_at: token,
    }
}
fn event(token: u64, outcome: ObservedOutcome) -> ObservedDelivery {
    ObservedDelivery {
        token,
        topic: [1; 16],
        partition: 0,
        outcome,
        offset: (outcome == ObservedOutcome::Acked).then_some(token as i64 - 1),
        timestamp: None,
        attempts: 1,
        parsed_attempts: 1,
        at: 10 + token,
        transmitted: true,
        definitive_broker_rejection: outcome == ObservedOutcome::NotWritten,
        prior_ambiguous_attempt: false,
        response: (outcome == ObservedOutcome::Acked).then_some(ObservedResponse::Success {
            at: 9 + token,
            offset: token as i64 - 1,
            timestamp: None,
        }),
    }
}
fn log() -> Vec<CommittedBatch> {
    vec![CommittedBatch {
        topic: [1; 16],
        partition: 0,
        identity: Identity {
            producer_id: 1,
            producer_epoch: 0,
            base_sequence: 0,
        },
        base_offset: 0,
        records: (1u64..=2)
            .map(|token| CommittedRecord {
                offset: token as i64 - 1,
                sequence: token as i32 - 1,
                timestamp: 0,
                key: None,
                value: Some(token.to_be_bytes().to_vec()),
                headers: vec![],
            })
            .collect(),
    }]
}
fn finished() -> DeliveryOracle {
    finished_with(|id| event(id, ObservedOutcome::Acked))
}
fn finished_with(mut delivery: impl FnMut(u64) -> ObservedDelivery) -> DeliveryOracle {
    let mut oracle = DeliveryOracle::new(OracleLimits::default());
    for id in 1..=2 {
        oracle.accept(accepted(id)).unwrap();
        oracle.input_consumed(id, 3 + id).unwrap();
    }
    oracle.flush(9, 6).unwrap();
    oracle.input_released(7, 7).unwrap();
    for id in 1..=2 {
        oracle.delivery(delivery(id)).unwrap();
    }
    oracle.flush_done(9, 13).unwrap();
    oracle
        .credits(CreditObservation {
            pool: 1,
            capacity: 2,
            reserved: 2,
            released: 2,
            held: 0,
        })
        .unwrap();
    oracle.closed().unwrap();
    oracle
}
#[test]
fn complete_history_obeys_delivery_lease_flush_and_credit_invariants() {
    assert_eq!(
        finished().finish(&log(), token).unwrap(),
        OracleReport {
            accepted: 2,
            acked: 2,
            not_written: 0,
            unknown: 0
        }
    );
}
#[test]
fn meta_tests_reject_dropped_duplicated_reordered_or_rebound_log_records() {
    let oracle = finished();
    let baseline = log();
    let mut dropped = baseline.clone();
    dropped[0].records.pop();
    assert!(oracle.finish(&dropped, token).is_err());
    let mut duplicate = baseline.clone();
    let extra = duplicate[0].records[0].clone();
    duplicate[0].records.push(extra);
    assert!(oracle.finish(&duplicate, token).is_err());
    let mut reorder = baseline.clone();
    reorder[0].records.swap(0, 1);
    assert!(oracle.finish(&reorder, token).is_err());
    let mut successor = baseline.clone();
    successor[0].topic = [2; 16];
    assert!(oracle.finish(&successor, token).is_err());
}
#[test]
fn meta_tests_reject_dropped_delivery_and_flipped_outcome() {
    let mut dropped = DeliveryOracle::new(OracleLimits::default());
    dropped.accept(accepted(1)).unwrap();
    dropped.input_consumed(1, 2).unwrap();
    dropped.input_released(7, 3).unwrap();
    assert!(dropped.closed().is_err());
    let mut flipped = DeliveryOracle::new(OracleLimits::default());
    for id in 1..=2 {
        flipped.accept(accepted(id)).unwrap();
        flipped
            .delivery(event(
                id,
                if id == 1 {
                    ObservedOutcome::NotWritten
                } else {
                    ObservedOutcome::Acked
                },
            ))
            .unwrap();
    }
    flipped.input_released(7, 14).unwrap();
    flipped.closed().unwrap();
    assert!(flipped.finish(&log(), token).is_err());
}
#[test]
fn premature_release_ack_without_response_and_ambiguous_notwritten_are_rejected() {
    let mut o = DeliveryOracle::new(OracleLimits::default());
    o.accept(accepted(1)).unwrap();
    assert!(o.input_released(7, 2).is_err());
    let mut e = event(1, ObservedOutcome::Acked);
    e.response = None;
    assert!(o.delivery(e).is_err());
    let mut e = event(1, ObservedOutcome::NotWritten);
    e.definitive_broker_rejection = false;
    assert!(o.delivery(e).is_err());
    let mut e = event(1, ObservedOutcome::Unknown);
    e.at = 1;
    assert!(o.delivery(e).is_err());
    let mut rejected_retry = event(1, ObservedOutcome::NotWritten);
    rejected_retry.prior_ambiguous_attempt = true;
    assert!(o.delivery(rejected_retry).is_err());
    o.delivery(event(1, ObservedOutcome::Unknown)).unwrap();
    o.input_released(7, 12).unwrap();
    assert!(o.input_released(7, 13).is_err());
    o.closed().unwrap();
    assert_eq!(o.finish(&[], token).unwrap().unknown, 1);
}
#[test]
fn flush_watermark_excludes_later_admissions_and_cannot_skip_earlier_deliveries() {
    let mut o = DeliveryOracle::new(OracleLimits::default());
    o.accept(accepted(1)).unwrap();
    o.flush(7, 2).unwrap();
    let mut later = accepted(2);
    later.returned_at = 3;
    o.accept(later).unwrap();
    assert!(o.flush_done(7, 4).is_err());
    o.delivery(event(1, ObservedOutcome::Acked)).unwrap();
    o.flush_done(7, 12).unwrap();
    assert!(o.closed().is_err());
}
#[test]
fn resource_observations_require_conservation_and_terminal_provider_release() {
    let mut o = finished();
    assert!(
        o.credits(CreditObservation {
            pool: 0,
            capacity: 1,
            reserved: 2,
            released: 0,
            held: 2
        })
        .is_err()
    );
    let segment = SharedBytes::from(vec![1, 2, 3]);
    o.retain_operation(5, core::slice::from_ref(&segment))
        .unwrap();
    assert!(o.finish(&log(), token).is_err());
    o.check_retained_operations().unwrap();
    o.release_operation(5).unwrap();
    assert!(o.finish(&log(), token).is_ok());
    assert!(o.release_operation(5).is_err());
}

#[test]
fn acknowledged_offsets_must_match_both_response_and_committed_record() {
    let mut oracle = DeliveryOracle::new(OracleLimits::default());
    oracle.accept(accepted(1)).unwrap();
    for offset in [None, Some(-1), Some(100)] {
        let mut delivery = event(1, ObservedOutcome::Acked);
        delivery.offset = offset;
        assert!(oracle.delivery(delivery).is_err());
    }
    let wrong_response = finished_with(|id| {
        let mut delivery = event(id, ObservedOutcome::Acked);
        let offset = 100 + id as i64;
        delivery.offset = Some(offset);
        delivery.response = Some(ObservedResponse::Success {
            at: 9 + id,
            offset,
            timestamp: None,
        });
        delivery
    });
    assert!(wrong_response.finish(&log(), token).is_err());

    let mut wrong_log = log();
    wrong_log[0].records[0].offset = 100;
    assert!(finished().finish(&wrong_log, token).is_err());
}

#[test]
fn duplicate_acknowledgement_alone_permits_an_absent_offset() {
    let duplicate = finished_with(|id| {
        let mut delivery = event(id, ObservedOutcome::Acked);
        delivery.offset = None;
        delivery.attempts = 2;
        delivery.parsed_attempts = 2;
        delivery.prior_ambiguous_attempt = true;
        delivery.response = Some(ObservedResponse::Duplicate { at: 9 + id });
        delivery
    });
    assert_eq!(duplicate.finish(&log(), token).unwrap().acked, 2);
    assert!(duplicate.finish(&[], token).is_err());

    let mut oracle = DeliveryOracle::new(OracleLimits::default());
    oracle.accept(accepted(1)).unwrap();
    let mut delivery = event(1, ObservedOutcome::Acked);
    delivery.response = Some(ObservedResponse::Duplicate { at: 10 });
    assert!(oracle.delivery(delivery).is_err());
    delivery.offset = None;
    delivery.timestamp = Some(42);
    assert!(oracle.delivery(delivery).is_err());
}

#[test]
fn returned_timestamp_matches_response_metadata_not_record_create_time() {
    let oracle = finished_with(|id| {
        let mut delivery = event(id, ObservedOutcome::Acked);
        delivery.timestamp = Some(777);
        delivery.response = Some(ObservedResponse::Success {
            at: 9 + id,
            offset: id as i64 - 1,
            timestamp: Some(777),
        });
        delivery
    });
    // The committed records retain CreateTime 0; response metadata is 777.
    assert_eq!(oracle.finish(&log(), token).unwrap().acked, 2);

    let mut oracle = DeliveryOracle::new(OracleLimits::default());
    oracle.accept(accepted(1)).unwrap();
    for timestamp in [Some(0), Some(-1), Some(777)] {
        let mut delivery = event(1, ObservedOutcome::Acked);
        delivery.timestamp = timestamp;
        assert!(oracle.delivery(delivery).is_err());
    }
    let mut delivery = event(1, ObservedOutcome::Acked);
    delivery.timestamp = Some(-1);
    delivery.response = Some(ObservedResponse::Success {
        at: 10,
        offset: 0,
        timestamp: Some(-1),
    });
    assert!(oracle.delivery(delivery).is_err());
}

#[test]
fn attempts_and_response_order_require_independent_request_evidence() {
    let mut oracle = DeliveryOracle::new(OracleLimits::default());
    oracle.accept(accepted(1)).unwrap();
    let mut delivery = event(1, ObservedOutcome::Acked);
    delivery.attempts = 0;
    assert!(oracle.delivery(delivery).is_err());
    delivery.attempts = 1;
    delivery.parsed_attempts = 2;
    assert!(oracle.delivery(delivery).is_err());
    delivery.parsed_attempts = 0;
    assert!(oracle.delivery(delivery).is_err());
    delivery.parsed_attempts = 1;
    delivery.transmitted = false;
    assert!(oracle.delivery(delivery).is_err());
    for at in [1, delivery.at, delivery.at + 1] {
        delivery.transmitted = true;
        delivery.response = Some(ObservedResponse::Success {
            at,
            offset: 0,
            timestamp: None,
        });
        assert!(oracle.delivery(delivery).is_err());
    }
    // Two additional admitted attempts may have failed before a complete frame
    // reached the broker; complete-request evidence is a lower bound.
    let valid = finished_with(|id| {
        let mut delivery = event(id, ObservedOutcome::Acked);
        delivery.attempts = 3;
        delivery
    });
    assert_eq!(valid.finish(&log(), token).unwrap().acked, 2);
}

#[test]
fn negative_deliveries_cannot_expose_success_metadata() {
    for outcome in [ObservedOutcome::NotWritten, ObservedOutcome::Unknown] {
        let mut oracle = DeliveryOracle::new(OracleLimits::default());
        oracle.accept(accepted(1)).unwrap();
        let mut delivery = event(1, outcome);
        delivery.offset = Some(0);
        assert!(oracle.delivery(delivery).is_err());
        delivery.offset = None;
        delivery.timestamp = Some(0);
        assert!(oracle.delivery(delivery).is_err());
    }
}

#[test]
fn unresolved_topic_binds_once_before_delivery_and_cannot_change_generation() {
    let mut oracle = DeliveryOracle::new(OracleLimits::default());
    let mut pending = accepted(1);
    pending.topic = [0; 16];
    oracle.accept(pending).unwrap();
    assert!(oracle.bind_topic(1, [1; 16], 1).is_err());
    assert!(oracle.bind_topic(1, [0; 16], 2).is_err());
    assert!(oracle.bind_topic(2, [1; 16], 2).is_err());
    oracle.bind_topic(1, [1; 16], 2).unwrap();
    assert!(oracle.bind_topic(1, [1; 16], 3).is_err());
    assert!(oracle.bind_topic(1, [2; 16], 3).is_err());
    let mut delivery = event(1, ObservedOutcome::Acked);
    delivery.topic = [2; 16];
    assert!(oracle.delivery(delivery).is_err());
    oracle.delivery(event(1, ObservedOutcome::Acked)).unwrap();
    assert!(oracle.bind_topic(1, [1; 16], 12).is_err());
    oracle.input_released(7, 13).unwrap();
    oracle.closed().unwrap();
    let mut committed = log();
    committed[0].records.truncate(1);
    assert_eq!(oracle.finish(&committed, token).unwrap().acked, 1);
}

#[test]
fn never_resolved_local_failure_retains_unknown_uuid_and_cannot_bind_later() {
    let mut oracle = DeliveryOracle::new(OracleLimits::default());
    let mut pending = accepted(1);
    pending.topic = [0; 16];
    oracle.accept(pending).unwrap();
    let mut delivery = event(1, ObservedOutcome::NotWritten);
    delivery.topic = [0; 16];
    delivery.transmitted = false;
    delivery.parsed_attempts = 0;
    delivery.attempts = 0;
    oracle.delivery(delivery).unwrap();
    assert!(oracle.bind_topic(1, [1; 16], 12).is_err());
    oracle.input_released(7, 13).unwrap();
    oracle.closed().unwrap();
    assert_eq!(oracle.finish(&[], token).unwrap().not_written, 1);
}

#[test]
fn committed_rejected_suffix_is_not_unrelated_producer_traffic() {
    let mut oracle = DeliveryOracle::new(OracleLimits::default());
    // A two-record application bulk returned only token1 as its accepted prefix.
    oracle.accept(accepted(1)).unwrap();
    oracle.delivery(event(1, ObservedOutcome::Acked)).unwrap();
    oracle.input_released(7, 12).unwrap();
    oracle.closed().unwrap();
    let committed_suffix = log();
    // The extractor identifies record2 even though it was never accepted. It
    // must not filter IDs through the accepted set and return None for this UID.
    let violation = oracle.finish(&committed_suffix, token).unwrap_err();
    assert_eq!(violation.invariant, "C1 committed unaccepted token");
    assert_eq!(violation.token, Some(2));
    let mut accepted_prefix = committed_suffix;
    accepted_prefix[0].records.truncate(1);
    assert_eq!(oracle.finish(&accepted_prefix, token).unwrap().acked, 1);
}

#[test]
fn an_unassigned_partition_sentinel_requires_unwritten_untransmitted_admission() {
    let mut record = accepted(1);
    record.lease = None;
    record.partition = 7;
    let mut observed = event(1, ObservedOutcome::NotWritten);
    observed.partition = -1;
    observed.attempts = 0;
    observed.parsed_attempts = 0;
    observed.transmitted = false;
    observed.definitive_broker_rejection = false;
    let make = |unassigned: bool| {
        let mut oracle = DeliveryOracle::new(OracleLimits::default());
        if unassigned {
            oracle.accept_unassigned_partition(record).unwrap();
        } else {
            oracle.accept(record).unwrap();
        }
        oracle.input_consumed(1, 2).unwrap();
        oracle
    };
    let mut oracle = make(true);
    oracle.delivery(observed).unwrap();
    oracle.closed().unwrap();
    oracle.finish(&[], token).unwrap();
    assert!(make(false).delivery(observed).is_err());
    for mutation in 0..8 {
        let mut e = observed;
        match mutation {
            0 => e.partition = -2,
            1 => e.partition = 6,
            2 => e.topic = [2; 16],
            3 => e.outcome = ObservedOutcome::Unknown,
            4 => e.attempts = 1,
            5 => e.transmitted = true,
            6 => e.prior_ambiguous_attempt = true,
            _ => {
                e.response = Some(ObservedResponse::Success {
                    at: 3,
                    offset: 0,
                    timestamp: None,
                })
            }
        };
        assert!(make(true).delivery(e).is_err(), "mutation {mutation}");
    }
    let mut oracle = make(true);
    let mut routed = observed;
    routed.partition = 7;
    oracle.delivery(routed).unwrap();
    oracle.closed().unwrap();
    let mut committed = log();
    committed[0].partition = 7;
    committed[0].records.truncate(1);
    assert!(oracle.finish(&committed, token).is_err());
}
