use super::*;

fn setup() -> (Admission, SharedCredits) {
    let config = ProducerConfig {
        record_descriptors: 8,
        delivery_event_capacity: 8,
        pending_records_per_topic: 8,
        descriptor_admission_policy: DescriptorAdmissionPolicy::PartitionPressure,
        ..Default::default()
    };
    let v = config.validate().unwrap();
    let credits =
        SharedCredits::with_descriptor_policy(v.credits, 1, config.descriptor_admission_policy)
            .unwrap();
    (
        Admission::new(&config, credits.clone(), v.effective_batch_payload_bytes),
        credits,
    )
}
fn route(count: u32) -> Result<AdmissionRouting, AdmissionError> {
    Ok(AdmissionRouting {
        lane: 0,
        ready: Some((TopicId([1; 16]), count)),
        builtin: true,
    })
}
fn record(partition: Option<i32>) -> RecordDescriptor<'static> {
    RecordDescriptor {
        topic: TopicHandle(1),
        partition_hint: partition,
        lane_hint: None,
        user_token: 7,
        timestamp_ms: 0,
        key: Some(b"key"),
        value: Some(b"value"),
        headers: &[],
        delivery_timeout: None,
    }
}
#[test]
fn prefix_pressure_does_not_copy_or_assign_rejected_suffix_and_terminal_returns_charge() {
    let (mut admission, credits) = setup();
    let descriptors = vec![record(Some(0)); 8];
    let (submitted, batch) =
        admission.prepare_copy_routed(RuntimeInstant::ZERO, &descriptors, &[route(2); 8]);
    assert_eq!(submitted.accepted, 6);
    assert_eq!(admission.last_token(), RecordToken(6));
    assert_eq!(admission.copied_bytes(), 6 * 8);
    assert!(matches!(
        submitted.error,
        Some(AdmissionError::Credit(CreditError::PartitionPressure(_)))
    ));
    let mut records = batch.unwrap().drain();
    let (_, mut obligation) = records.pop().unwrap().into_parts();
    obligation.input_consumed();
    assert_eq!(credits.snapshot()[Resource::Descriptors as usize].held, 6);
    let event = obligation.terminal();
    assert_eq!(credits.snapshot()[Resource::Descriptors as usize].held, 5);
    assert_eq!(
        credits.snapshot()[Resource::DeliveryEvents as usize].held,
        6
    );
    let (next, accepted) =
        admission.prepare_copy_routed(RuntimeInstant::ZERO, &[record(Some(0))], &[route(2)]);
    assert_eq!(next.first_token, Some(RecordToken(7)));
    drop((records, event, accepted));
    assert!(credits.is_empty());
}
#[test]
fn ready_keys_pin_admission_partition_and_unclassified_records_keep_normal_routing() {
    let (mut admission, credits) = setup();
    let expected = (crate::routing::murmur2(b"key") & 0x7fff_ffff) % 2;
    let (_, batch) =
        admission.prepare_copy_routed(RuntimeInstant::ZERO, &[record(None)], &[route(2)]);
    let batch = batch.unwrap();
    assert_eq!(batch.records[0].partition_hint, Some(expected as i32));
    // Later snapshots do not mutate the retained accepted choice.
    let (_, expanded) =
        admission.prepare_copy_routed(RuntimeInstant::ZERO, &[record(None)], &[route(17)]);
    assert_eq!(
        expanded.as_ref().unwrap().records[0].partition_hint,
        Some(((crate::routing::murmur2(b"key") & 0x7fff_ffff) % 17) as i32)
    );
    for routing in [
        AdmissionRouting::unclassified(0),
        AdmissionRouting {
            builtin: false,
            ..route(2).unwrap()
        },
    ] {
        let (_, pending) =
            admission.prepare_copy_routed(RuntimeInstant::ZERO, &[record(None)], &[Ok(routing)]);
        assert_eq!(pending.as_ref().unwrap().records[0].partition_hint, None);
        drop(pending);
    }
    drop((batch, expanded));
    assert!(credits.is_empty());
}
#[test]
fn leased_pressure_rejection_preserves_tokens_and_does_not_double_charge_payload() {
    let (mut admission, credits) = setup();
    let config = ProducerConfig::default();
    let leases = InputLeases::new(&config, credits.clone()).unwrap();
    let mut buffer = leases.acquire(32, 0).unwrap();
    buffer.as_mut_slice()[..8].copy_from_slice(b"keyvalue");
    let lease = buffer.commit(8).unwrap();
    let descriptor = LeasedRecordDescriptor {
        topic: TopicHandle(1),
        partition_hint: Some(0),
        lane_hint: None,
        user_token: 0,
        timestamp_ms: 0,
        key: Some(0..3),
        value: Some(3..8),
        headers: &[],
        delivery_timeout: None,
    };
    let (submitted, batch) = admission.prepare_leased_routed(
        RuntimeInstant::ZERO,
        &leases,
        lease,
        &vec![descriptor; 8],
        &[route(2); 8],
    );
    assert_eq!(submitted.accepted, 6);
    assert_eq!(admission.copied_bytes(), 0);
    assert_eq!(credits.snapshot()[Resource::InputBytes as usize].held, 32);
    leases.release(lease).unwrap();
    drop(batch);
    drop(leases.pop_released().unwrap());
    assert!(credits.is_empty());
}
