use super::*;
use crate::admission::{Admission, AdmissionError};
use crate::input::{InputLeases, LeasedRecordDescriptor};

fn engine() -> ProducerEngine {
    engine_with_policy(crate::config::DescriptorAdmissionPolicy::Shared)
}
fn engine_with_policy(
    descriptor_admission_policy: crate::config::DescriptorAdmissionPolicy,
) -> ProducerEngine {
    let config = ProducerConfig {
        descriptor_admission_policy,
        compression: Compression::None,
        codec_contexts: 0,
        record_descriptors: 8,
        delivery_event_capacity: 8,
        pending_records_per_topic: 8,
        max_batches: 8,
        max_open_topics: 2,
        max_live_leases: 2,
        release_event_capacity: 2,
        input_bytes: 65536,
        compressed_bytes: 65536,
        batch_target_bytes: 4096,
        batch_hard_bytes: 4096,
        progressive_threshold: 32,
        output_chunk_bytes: 4096,
        request_target_bytes: 8192,
        request_hard_bytes: 8192,
        metrics: crate::telemetry::metrics::MetricsConfig {
            enabled: false,
            ..Default::default()
        },
        ..ProducerConfig::default()
    };
    ProducerEngine::new(config, None).unwrap()
}

#[test]
fn pressure_pending_owners_release_on_cancel_deadline_and_topic_close() {
    for mode in 0..3 {
        let mut engine =
            engine_with_policy(crate::config::DescriptorAdmissionPolicy::PartitionPressure);
        let credits = engine.credits();
        let topic = engine.open_topic("pressure", RuntimeInstant::ZERO).unwrap();
        let mut admission = admission(&engine);
        let mut records = copied(&mut admission, topic, 6);
        for record in &mut records {
            record.deadline = RuntimeInstant::from_nanos(10);
        }
        engine
            .admit_records(
                RuntimeInstant::ZERO,
                records,
                &[PartitionChoice::Pending; 6],
            )
            .unwrap();
        assert_eq!(credits.snapshot()[Resource::Descriptors as usize].held, 6);
        match mode {
            0 => {
                for token in 1..=6 {
                    engine
                        .cancel(RuntimeInstant::from_nanos(1), RecordToken(token))
                        .unwrap();
                }
            }
            1 => {
                for _ in 0..32 {
                    engine.on_deadline(
                        RuntimeInstant::from_nanos(11),
                        WorkBudget {
                            items: 8,
                            bytes: 4096,
                        },
                    );
                }
            }
            _ => {
                engine
                    .close_topic(topic, RuntimeInstant::from_nanos(1))
                    .unwrap();
            }
        }
        for _ in 0..32 {
            engine.on_deadline(
                RuntimeInstant::from_nanos(11),
                WorkBudget {
                    items: 8,
                    bytes: 4096,
                },
            );
        }
        assert_eq!(
            credits.snapshot()[Resource::Descriptors as usize].held,
            0,
            "mode={mode}"
        );
        drop(engine);
        assert!(credits.is_empty(), "mode={mode}");
    }
}
fn admission(engine: &ProducerEngine) -> Admission {
    Admission::new(
        engine.config(),
        engine.credits(),
        engine.validated.effective_batch_payload_bytes,
    )
}
fn record(topic: TopicHandle, value: Option<&[u8]>) -> RecordDescriptor<'_> {
    RecordDescriptor {
        topic,
        partition_hint: None,
        lane_hint: None,
        key: None,
        value,
        headers: &[],
        timestamp_ms: 0,
        user_token: 0,
        delivery_timeout: None,
    }
}
fn copied(admission: &mut Admission, topic: TopicHandle, count: usize) -> Vec<AdmittedRecord> {
    let (submitted, batch) = admission.prepare_copy(
        RuntimeInstant::ZERO,
        &vec![record(topic, Some(b"payload")); count],
        &vec![Ok(0); count],
    );
    assert_eq!(submitted.accepted as usize, count);
    batch.unwrap().drain()
}
fn control(authority: &SharedCredits) -> HeldCredits {
    authority
        .reserve(&[Claim {
            resource: Resource::ControlEvents,
            amount: 1,
            lane: 0,
        }])
        .unwrap()
}
fn foreign<T>(result: Result<T>) {
    assert!(matches!(
        result,
        Err(EngineError::Credit(CreditError::ForeignCredit))
    ));
}

#[test]
fn foreign_reserved_control_cannot_mutate_target_or_grow_its_queue() {
    let mut target = engine();
    let source = engine();
    let source_credits = source.credits();
    let target_credits = target.credits();
    let now = RuntimeInstant::ZERO;
    let topic = TopicHandle(1);
    let before = target.status();
    let target_before = target_credits.snapshot();
    let source_before = source_credits.snapshot();
    foreign(target.open_topic_reserved_credit(topic, "topic", now, control(&source_credits)));
    assert_eq!(target.status(), before);
    assert_eq!(target_credits.snapshot(), target_before);
    assert!(target.topics.get(topic).is_err());
    assert_eq!(
        source_credits.snapshot().map(|p| p.held),
        source_before.map(|p| p.held)
    );
    assert_eq!(
        target
            .open_topic_reserved_credit(topic, "topic", now, control(&target_credits))
            .unwrap(),
        topic
    );

    // A foreign rejection must not consume the caller's flush token. A local
    // clone can immediately use that same token with its own reservation.
    let token = FlushToken(target.next_flush);
    let before = target.status();
    foreign(target.flush_reserved(token, RecordToken(1), now, control(&source_credits)));
    assert_eq!(target.next_flush, token.0);
    assert_eq!(target.status(), before);
    target
        .flush_reserved(token, RecordToken(1), now, control(&target_credits.clone()))
        .unwrap();

    // Retain every available local control credit in pending flushes. Repeated
    // foreign input cannot add an obligation or grow the fixed local backing.
    while let Ok(credit) = target.control_credit() {
        target
            .flush_reserved(FlushToken(target.next_flush), RecordToken(1), now, credit)
            .unwrap();
    }
    let before = target.status();
    let target_before = target_credits.snapshot();
    let flush_len = target.flushes.len();
    let flush_capacity = target.flushes.capacity();
    let event_capacity = target.events.storage_capacity_bytes();
    let token = FlushToken(target.next_flush);
    for _ in 0..2 * flush_capacity {
        foreign(target.flush_reserved(token, RecordToken(1), now, control(&source_credits)));
        foreign(target.open_topic_reserved_credit(
            TopicHandle(2),
            "other",
            now,
            control(&source_credits),
        ));
        assert_eq!(target.next_flush, token.0);
        assert_eq!(target.status(), before);
        assert_eq!(target.flushes.len(), flush_len);
        assert_eq!(target.flushes.capacity(), flush_capacity);
        assert_eq!(target.events.storage_capacity_bytes(), event_capacity);
        assert_eq!(target_credits.snapshot(), target_before);
        assert_eq!(
            source_credits.snapshot().map(|p| p.held),
            source_before.map(|p| p.held)
        );
    }
    assert!(target.pop_event().is_none());
    drop(target);
    drop(source);
    assert!(target_credits.is_empty() && source_credits.is_empty());
}

#[test]
fn foreign_and_mixed_record_buffers_reject_atomically_after_an_accepted_prefix() {
    let mut target = engine();
    let source = engine();
    let now = RuntimeInstant::ZERO;
    let topic = target.open_topic("topic", now).unwrap();
    let target_credits = target.credits();
    let source_credits = source.credits();
    let mut local = admission(&target);
    let mut other = admission(&source);
    let mut records = copied(&mut other, topic, 3);
    let pointer = records.as_ptr();
    let capacity = records.capacity();
    let source_before = source_credits.snapshot();
    let target_before = target_credits.snapshot();
    foreign(target.admit_records_buffered(now, &mut records, &[PartitionChoice::Pending; 3]));
    assert_eq!(target.status().accepted, 0);
    assert_eq!(records.as_ptr(), pointer);
    assert_eq!(records.capacity(), capacity);
    assert_eq!(
        records.iter().map(|r| r.token.0).collect::<Vec<_>>(),
        [1, 2, 3]
    );
    assert_eq!(source_credits.snapshot(), source_before);
    assert_eq!(target_credits.snapshot(), target_before);

    let mut prefix = copied(&mut local, topic, 1);
    target
        .admit_records_buffered(now, &mut prefix, &[PartitionChoice::Pending])
        .unwrap();
    let accepted_pointer = target
        .pending_record(RecordToken(1))
        .unwrap()
        .record
        .value
        .as_ref()
        .unwrap()
        .as_ptr();
    let mut mixed = copied(&mut local, topic, 1);
    // Tokens are naturally dense across the two admissions: local token2,
    // foreign token3. No public record field is edited to construct this case.
    mixed.push(records.pop().unwrap());
    drop(records);
    let before = target.status();
    let target_before = target_credits.snapshot();
    let source_before = source_credits.snapshot();
    let pointer = mixed.as_ptr();
    let payloads: Vec<_> = mixed
        .iter()
        .map(|r| r.record.value.as_ref().unwrap().as_ptr())
        .collect();
    foreign(target.admit_records_buffered(now, &mut mixed, &[PartitionChoice::Pending; 2]));
    assert_eq!(target.status(), before);
    assert_eq!(mixed.as_ptr(), pointer);
    assert_eq!(
        mixed
            .iter()
            .map(|r| r.record.value.as_ref().unwrap().as_ptr())
            .collect::<Vec<_>>(),
        payloads
    );
    assert_eq!(target_credits.snapshot(), target_before);
    assert_eq!(source_credits.snapshot(), source_before);
    assert_eq!(
        target
            .pending_record(RecordToken(1))
            .unwrap()
            .record
            .value
            .as_ref()
            .unwrap()
            .as_ptr(),
        accepted_pointer
    );
    drop(mixed.pop());
    target
        .admit_records_buffered(now, &mut mixed, &[PartitionChoice::Pending])
        .unwrap();
    assert_eq!(target.status().accepted, 2);
    assert!(mixed.is_empty());

    // A zero-input local record has an empty input guard from the same
    // authority. Empty guards must retain that identity and remain admissible.
    let (submitted, batch) = local.prepare_copy(now, &[record(topic, None)], &[Ok(0)]);
    assert_eq!(submitted.first_token, Some(RecordToken(3)));
    target
        .admit(now, batch.unwrap(), &[PartitionChoice::Pending])
        .unwrap();
    assert_eq!(target.status().accepted, 3);
    assert_eq!(
        source_credits.snapshot()[Resource::Descriptors as usize].held,
        0
    );
    drop(target);
    drop(source);
    assert!(target_credits.is_empty() && source_credits.is_empty());
}

#[test]
fn foreign_lease_registry_is_rejected_before_snapshot_or_admission_reservation() {
    let target = engine();
    let source = engine();
    let target_credits = target.credits();
    let source_credits = source.credits();
    let ours = InputLeases::new(target.config(), target_credits.clone()).unwrap();
    let theirs = InputLeases::new(source.config(), source_credits.clone()).unwrap();
    let our_lease = ours.acquire(8, 0).unwrap().commit(4).unwrap();
    let their_lease = theirs.acquire(8, 0).unwrap().commit(4).unwrap();
    assert_eq!(
        our_lease, their_lease,
        "lease IDs alone are not an authority"
    );
    let descriptor = LeasedRecordDescriptor {
        topic: TopicHandle(1),
        partition_hint: None,
        lane_hint: None,
        key: None,
        value: Some(0..4),
        headers: &[],
        timestamp_ms: 0,
        user_token: 0,
        delivery_timeout: None,
    };
    let mut admission = admission(&target);
    let target_before = target_credits.snapshot();
    let source_before = source_credits.snapshot();
    let source_status = theirs.status();
    let (submitted, batch) = admission.prepare_leased(
        RuntimeInstant::ZERO,
        &theirs,
        their_lease,
        std::slice::from_ref(&descriptor),
        &[Ok(0)],
    );
    assert_eq!(submitted.accepted, 0);
    assert_eq!(
        submitted.error,
        Some(AdmissionError::Credit(CreditError::ForeignCredit))
    );
    assert!(batch.is_none());
    assert_eq!(admission.last_token(), RecordToken(0));
    assert_eq!(target_credits.snapshot(), target_before);
    assert_eq!(source_credits.snapshot(), source_before);
    assert_eq!(theirs.status(), source_status);
    theirs.release(their_lease).unwrap();
    assert_eq!(
        theirs.pop_released().unwrap().event,
        Event::InputReleased { lease: their_lease }
    );
    assert!(theirs.pop_released().is_none());
    assert_eq!(
        source_credits.snapshot()[Resource::InputBytes as usize].held,
        0
    );

    let (submitted, batch) = admission.prepare_leased(
        RuntimeInstant::ZERO,
        &ours.clone(),
        our_lease,
        &[descriptor],
        &[Ok(0)],
    );
    assert_eq!(submitted.accepted, 1);
    assert_eq!(submitted.first_token, Some(RecordToken(1)));
    drop(batch);
    ours.release(our_lease).unwrap();
    assert_eq!(
        ours.pop_released().unwrap().event,
        Event::InputReleased { lease: our_lease }
    );
    assert!(ours.pop_released().is_none());
    drop(target);
    drop(source);
    assert!(target_credits.is_empty() && source_credits.is_empty());
}

#[test]
fn foreign_event_returns_source_guard_while_local_saturation_can_still_complete() {
    let mut target = engine();
    let mut source = engine();
    let target_credits = target.credits();
    let source_credits = source.credits();
    let now = RuntimeInstant::ZERO;

    // Genuine terminal records fill the delivery reserve, and actual native
    // lease releases fill the release reserve. Their local envelopes may cross
    // the public publication boundary without losing their reservations.
    let mut local = admission(&target);
    let mut records = copied(&mut local, TopicHandle(999), 8);
    let admitted = target
        .admit_records_buffered(now, &mut records, &[PartitionChoice::Pending; 8])
        .unwrap();
    assert_eq!(admitted.failed, 8);
    let input = InputLeases::new(target.config(), target_credits.clone()).unwrap();
    for _ in 0..2 {
        let lease = input.acquire(8, 0).unwrap().commit(4).unwrap();
        input.release(lease).unwrap();
        target.publish_event(input.pop_released().unwrap()).unwrap();
    }

    // Preserve one local obligation that will publish after the foreign
    // attempts. Fill every other available control slot with real flushes.
    let final_credit = control(&target_credits);
    while let Ok(credit) = target.control_credit() {
        target
            .flush_reserved(FlushToken(target.next_flush), RecordToken(8), now, credit)
            .unwrap();
        assert!(target.fence_step());
    }
    for resource in [
        Resource::ControlEvents,
        Resource::DeliveryEvents,
        Resource::ReleaseEvents,
    ] {
        let status = target_credits.snapshot()[resource as usize];
        assert_eq!(status.held, status.limit);
    }
    let target_before = target.status();
    let credits_before = target_credits.snapshot();
    let storage = target.events.storage_capacity_bytes();
    let next_flush = target.next_flush;
    let source_token = source.flush(now, RecordToken(0)).unwrap();
    assert!(source.fence_step());
    let mut returned = source.pop_event().unwrap();
    let expected = Event::FlushDone {
        token: source_token,
    };
    let source_before = source_credits.snapshot();

    // The same envelope can be retried without cloning its private guard. It
    // must remain caller-owned and source-charged through every rejection.
    for _ in 0..2 * target.events.len() {
        returned = target.publish_event(returned).unwrap_err();
        assert_eq!(returned.event, expected);
        assert_eq!(returned.reserved(), 1);
        assert_eq!(target.status(), target_before);
        assert_eq!(target.next_flush, next_flush);
        assert_eq!(target.events.storage_capacity_bytes(), storage);
        assert_eq!(target_credits.snapshot(), credits_before);
        assert_eq!(source_credits.snapshot(), source_before);
    }

    // Returning the rejected envelope to its actual engine succeeds. Keeping
    // the popped response still retains exactly that source reservation.
    source.publish_event(returned).unwrap();
    let returned = source.pop_event().unwrap();
    assert_eq!(returned.event, expected);
    assert_eq!(source_credits.snapshot(), source_before);
    let final_token = FlushToken(target.next_flush);
    target
        .flush_reserved(final_token, RecordToken(8), now, final_credit)
        .unwrap();
    assert!(
        target.fence_step(),
        "a later locally charged event can publish"
    );
    assert_eq!(target.events.storage_capacity_bytes(), storage);
    let mut drained = Vec::new();
    while let Some(event) = target.pop_event() {
        drained.push(event.event);
    }
    assert_eq!(drained.len(), target_before.queued_events + 1);
    assert_eq!(
        drained.last(),
        Some(&Event::FlushDone { token: final_token })
    );
    assert_eq!(source_credits.snapshot(), source_before);
    drop(returned);
    assert_eq!(
        source_credits.snapshot()[Resource::ControlEvents as usize].held + 1,
        source_before[Resource::ControlEvents as usize].held
    );
    drop(target);
    drop(source);
    assert!(target_credits.is_empty() && source_credits.is_empty());
}
