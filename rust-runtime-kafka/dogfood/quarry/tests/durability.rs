#[allow(dead_code)]
mod support;

use kr_runtime::{CompletionCertainty, SimDuration, SimInstant};
use kr_runtime_ring::{
    MemoryRing, RingCursor, RingError, RingLimits, RingOperation, RingReader, RingWriter,
};
use quarry::{
    AckOutcome, DEFAULT_MAX_REPLAY_RECORDS, DurableQueue, DurableQueueError, JobStatus,
    NackOutcome, QueueConfig, QueueError, RecoveryConfig, RequestId, SubmitOutcome, SubmitRequest,
    WorkerId,
};

use support::fault_ring::{FaultRing, InjectedFault};
use support::{assert_every_operation_is_fenced, complete};

const RECOVERY_READ_BYTES: usize = 512;

fn queue_config() -> QueueConfig {
    QueueConfig {
        active_capacity: 16,
        max_payload_bytes: 128,
        max_claim_batch: 8,
        completed_history_capacity: 16,
    }
}

fn ring_limits() -> RingLimits {
    RingLimits {
        max_record_bytes: 512,
        max_live_records: 64,
        max_live_payload_bytes: 64 * 512,
        max_read_records: 8,
        max_read_bytes: RECOVERY_READ_BYTES,
        max_batch_records: 1,
        max_batch_bytes: 512,
    }
}

fn new_ring(limits: RingLimits) -> FaultRing {
    FaultRing::new(MemoryRing::new(limits).expect("valid bounded memory ring limits"))
}

fn request(id: u64) -> SubmitRequest {
    SubmitRequest {
        request_id: RequestId::new(id),
        payload: format!("job-{id}").into_bytes(),
        not_before: SimInstant::ZERO,
    }
}

fn open_queue(
    config: QueueConfig,
    ring: FaultRing,
    read_batch_records: usize,
) -> DurableQueue<FaultRing> {
    complete(DurableQueue::recover(
        config,
        ring,
        RecoveryConfig::new(
            read_batch_records,
            RECOVERY_READ_BYTES,
            DEFAULT_MAX_REPLAY_RECORDS,
        ),
    ))
    .expect("bounded memory ring recovery succeeds")
}

fn new_queue() -> (DurableQueue<FaultRing>, FaultRing) {
    let ring = new_ring(ring_limits());
    let control = ring.clone();
    (open_queue(queue_config(), ring, 2), control)
}

#[test]
fn empty_recovery_persists_configuration_and_starts_incarnation() {
    let ring = new_ring(ring_limits());
    let control = ring.clone();
    let mut queue = open_queue(queue_config(), ring, 1);

    assert_eq!(queue.incarnation(), 1);
    let snapshot = queue.snapshot(SimInstant::ZERO).unwrap();
    assert!(snapshot.jobs.is_empty());
    assert!(snapshot.completed.is_empty());

    let status = control.status_now();
    assert_eq!(status.accepted_head, RingCursor::START);
    assert_eq!(status.durable_head, RingCursor::START);
    assert_eq!(status.accepted_tail, RingCursor::new(2));
    assert_eq!(status.durable_tail, status.accepted_tail);
    assert_eq!(status.accepted_live_records, 2);
}

#[test]
fn zero_read_limit_is_rejected_before_the_recovery_fence() {
    let ring = new_ring(ring_limits());
    ring.inject_sync_fault(InjectedFault::Before);

    let error = match complete(DurableQueue::recover(
        queue_config(),
        ring.clone(),
        RecoveryConfig::new(0, RECOVERY_READ_BYTES, DEFAULT_MAX_REPLAY_RECORDS),
    )) {
        Ok(_) => panic!("zero read limit unexpectedly recovered a queue"),
        Err(error) => error,
    };
    assert_eq!(error.certainty(), CompletionCertainty::NotApplied);
    assert_eq!(
        error.error(),
        &DurableQueueError::ZeroRecoveryLimit {
            field: "read_batch_records"
        }
    );
    let status = ring.status_now();
    assert_eq!(status.accepted_tail, RingCursor::START);
    assert_eq!(status.durable_tail, RingCursor::START);
    assert_eq!(ring.pending_fault_count_for(RingOperation::Sync), 1);
}

#[test]
fn zero_read_byte_budget_is_rejected_before_the_recovery_fence() {
    let ring = new_ring(ring_limits());
    ring.inject_sync_fault(InjectedFault::Before);

    let error = match complete(DurableQueue::recover(
        queue_config(),
        ring.clone(),
        RecoveryConfig::new(1, 0, DEFAULT_MAX_REPLAY_RECORDS),
    )) {
        Ok(_) => panic!("zero read byte budget unexpectedly recovered a queue"),
        Err(error) => error,
    };
    assert_eq!(error.certainty(), CompletionCertainty::NotApplied);
    assert_eq!(
        error.error(),
        &DurableQueueError::ZeroRecoveryLimit {
            field: "read_batch_bytes"
        }
    );
    assert_eq!(ring.status_now().accepted_tail, RingCursor::START);
    assert_eq!(ring.pending_fault_count_for(RingOperation::Sync), 1);
}

#[test]
fn recovery_rejects_a_trimmed_ring_before_issuing_its_fence() {
    let (queue, _) = new_queue();
    let ring = queue.into_ring();
    complete(ring.trim(RingCursor::new(1))).expect("trim durable configuration record");
    complete(ring.sync()).expect("make test trim durable");
    ring.inject_sync_fault(InjectedFault::Before);

    let error = match complete(DurableQueue::recover(
        queue_config(),
        ring.clone(),
        RecoveryConfig::new(1, RECOVERY_READ_BYTES, DEFAULT_MAX_REPLAY_RECORDS),
    )) {
        Ok(_) => panic!("trimmed ring unexpectedly recovered a queue"),
        Err(error) => error,
    };
    assert_eq!(error.certainty(), CompletionCertainty::NotApplied);
    assert!(matches!(
        error.error(),
        DurableQueueError::InvalidHistory { position: None, message }
            if message.contains("untrimmed ring")
    ));
    let status = ring.status_now();
    assert_eq!(status.accepted_head, RingCursor::new(1));
    assert_eq!(status.durable_head, RingCursor::new(1));
    assert_eq!(ring.pending_fault_count_for(RingOperation::Sync), 1);
}

#[test]
fn read_failure_after_recovery_fence_is_may_have_applied() {
    let (queue, ring) = new_queue();
    drop(queue);
    ring.inject_read_fault(InjectedFault::After);

    let error = match complete(DurableQueue::recover(
        queue_config(),
        ring.clone(),
        RecoveryConfig::new(1, RECOVERY_READ_BYTES, DEFAULT_MAX_REPLAY_RECORDS),
    )) {
        Ok(_) => panic!("injected read failure unexpectedly recovered a queue"),
        Err(error) => error,
    };
    assert_eq!(error.certainty(), CompletionCertainty::MayHaveApplied);
    assert!(matches!(
        error.error(),
        DurableQueueError::Ring(RingError::BackendFailure {
            operation: RingOperation::Read,
            ..
        })
    ));
    let status = ring.status_now();
    assert_eq!(status.accepted_tail, RingCursor::new(2));
    assert_eq!(status.durable_tail, RingCursor::new(2));
}

#[test]
fn final_applied_sync_error_returns_no_unverified_recovery_handle() {
    let ring = new_ring(ring_limits());
    ring.inject_sync_fault(InjectedFault::After);
    ring.inject_sync_fault(InjectedFault::After);

    let error = match complete(DurableQueue::recover(
        queue_config(),
        ring.clone(),
        RecoveryConfig::new(1, RECOVERY_READ_BYTES, DEFAULT_MAX_REPLAY_RECORDS),
    )) {
        Ok(_) => panic!("recovery swallowed its final sync error"),
        Err(error) => error,
    };
    assert_eq!(error.certainty(), CompletionCertainty::Applied);
    let status = ring.status_now();
    assert_eq!(status.accepted_tail, RingCursor::new(2));
    assert_eq!(status.durable_tail, status.accepted_tail);

    let recovered = open_queue(queue_config(), ring, 1);
    assert_eq!(
        recovered.incarnation(),
        2,
        "retry must advance past the durably ambiguous incarnation"
    );
}

#[test]
fn durable_submit_ack_and_deduplication_survive_crash() {
    let (mut queue, _) = new_queue();
    let original_request = request(7);

    let submitted = complete(queue.submit(original_request.clone(), SimInstant::ZERO)).unwrap();
    let job_id = submitted.job_id();
    assert_eq!(submitted, SubmitOutcome::Submitted { job_id });
    assert_eq!(
        complete(queue.submit(original_request.clone(), SimInstant::ZERO)).unwrap(),
        SubmitOutcome::DuplicateActive { job_id }
    );

    let leased = queue
        .claim(
            WorkerId::new(1),
            1,
            SimDuration::from_nanos(10),
            SimInstant::ZERO,
        )
        .unwrap()
        .pop()
        .unwrap();
    assert_eq!(
        complete(queue.ack(job_id, leased.lease_token, SimInstant::ZERO)).unwrap(),
        AckOutcome::Completed
    );
    assert_eq!(
        complete(queue.ack(job_id, leased.lease_token, SimInstant::ZERO)).unwrap(),
        AckOutcome::AlreadyCompleted
    );

    let ring = queue.into_ring();
    assert_eq!(ring.crash().discarded_records, 0);
    let mut recovered = open_queue(queue_config(), ring, 1);

    assert_eq!(recovered.incarnation(), 2);
    assert_eq!(
        complete(recovered.submit(original_request, SimInstant::ZERO)).unwrap(),
        SubmitOutcome::DuplicateCompleted { job_id }
    );
    let snapshot = recovered.snapshot(SimInstant::ZERO).unwrap();
    assert!(snapshot.jobs.is_empty());
    assert_eq!(snapshot.completed.len(), 1);
    assert_eq!(snapshot.completed[0].job_id, job_id);
    assert_eq!(snapshot.completed[0].ack_token, leased.lease_token);
}

#[test]
fn fixed_ring_capacity_exhaustion_is_explicit_and_not_applied() {
    let limits = RingLimits {
        max_live_records: 3,
        ..ring_limits()
    };
    let ring = new_ring(limits);
    let mut queue = open_queue(queue_config(), ring, 1);

    let first = complete(queue.submit(request(8), SimInstant::ZERO)).unwrap();
    let error = complete(queue.submit(request(9), SimInstant::ZERO)).unwrap_err();

    assert_eq!(error.certainty(), CompletionCertainty::NotApplied);
    assert_eq!(
        error.error(),
        &DurableQueueError::Ring(RingError::RecordCapacityReached {
            retained: 3,
            requested: 1,
            limit: 3,
        })
    );
    assert!(!queue.recovery_required());
    let snapshot = queue.snapshot(SimInstant::ZERO).unwrap();
    assert_eq!(snapshot.jobs.len(), 1);
    assert_eq!(snapshot.jobs[0].job_id, first.job_id());
}

#[test]
fn restart_discards_leases_and_nack_delays_and_fences_old_tokens() {
    let (mut queue, _) = new_queue();
    let first_id = complete(queue.submit(request(1), SimInstant::ZERO))
        .unwrap()
        .job_id();
    let second_id = complete(queue.submit(request(2), SimInstant::ZERO))
        .unwrap()
        .job_id();
    let leased = queue
        .claim(
            WorkerId::new(1),
            2,
            SimDuration::from_nanos(50),
            SimInstant::ZERO,
        )
        .unwrap();
    assert_eq!(leased[0].job_id, first_id);
    assert_eq!(leased[1].job_id, second_id);
    assert_eq!(leased[0].lease_token.incarnation(), 1);
    assert_eq!(
        queue
            .nack(
                second_id,
                leased[1].lease_token,
                SimDuration::from_nanos(1_000),
                SimInstant::ZERO,
            )
            .unwrap(),
        NackOutcome::Requeued {
            available_at: SimInstant::from_nanos(1_000)
        }
    );

    let ring = queue.into_ring();
    assert_eq!(ring.crash().discarded_records, 0);
    let mut recovered = open_queue(queue_config(), ring, 1);
    let snapshot = recovered.snapshot(SimInstant::ZERO).unwrap();
    assert_eq!(snapshot.jobs.len(), 2);
    assert!(
        snapshot
            .jobs
            .iter()
            .all(|job| job.status == JobStatus::Ready),
        "leased state and the later nack delay are both ephemeral"
    );

    let redelivered = recovered
        .claim(
            WorkerId::new(2),
            2,
            SimDuration::from_nanos(10),
            SimInstant::ZERO,
        )
        .unwrap();
    assert!(
        redelivered
            .iter()
            .all(|job| job.lease_token.incarnation() == 2)
    );
    assert_ne!(redelivered[0].lease_token, leased[0].lease_token);

    let stale = complete(recovered.ack(first_id, leased[0].lease_token, SimInstant::ZERO))
        .expect_err("pre-restart token must be fenced");
    assert_eq!(stale.certainty(), CompletionCertainty::NotApplied);
    assert_eq!(
        stale.error(),
        &DurableQueueError::Queue(QueueError::StaleLeaseToken {
            job_id: first_id,
            provided: leased[0].lease_token,
        })
    );
    assert_eq!(
        complete(recovered.ack(first_id, redelivered[0].lease_token, SimInstant::ZERO,)).unwrap(),
        AckOutcome::Completed
    );
}

#[test]
fn delayed_submit_keeps_its_original_virtual_deadline_after_recovery() {
    let (mut queue, _) = new_queue();
    let delayed = SubmitRequest {
        request_id: RequestId::new(55),
        payload: b"later".to_vec(),
        not_before: SimInstant::from_nanos(50),
    };
    complete(queue.submit(delayed, SimInstant::ZERO)).unwrap();

    let ring = queue.into_ring();
    ring.crash();
    let mut recovered = open_queue(queue_config(), ring, 1);
    assert!(
        recovered
            .claim(
                WorkerId::new(1),
                1,
                SimDuration::from_nanos(1),
                SimInstant::from_nanos(49),
            )
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        recovered
            .claim(
                WorkerId::new(1),
                1,
                SimDuration::from_nanos(1),
                SimInstant::from_nanos(50),
            )
            .unwrap()
            .len(),
        1
    );
}

#[test]
fn recovery_rejects_a_different_queue_configuration() {
    let (queue, _) = new_queue();
    let ring = queue.into_ring();
    ring.crash();
    let original = queue_config();
    let changed = QueueConfig {
        active_capacity: original.active_capacity + 1,
        ..original
    };

    let error = match complete(DurableQueue::recover(
        changed,
        ring,
        RecoveryConfig::new(1, RECOVERY_READ_BYTES, DEFAULT_MAX_REPLAY_RECORDS),
    )) {
        Ok(_) => panic!("recovery accepted a different capacity contract"),
        Err(error) => error,
    };
    assert_eq!(error.certainty(), CompletionCertainty::MayHaveApplied);
    assert_eq!(
        error.error(),
        &DurableQueueError::ConfigurationMismatch {
            expected: original,
            found: changed,
        }
    );
}

#[test]
fn append_completion_matrix_controls_poisoning_and_acceptance() {
    let cases = [
        (
            InjectedFault::Before,
            CompletionCertainty::NotApplied,
            false,
            false,
        ),
        (
            InjectedFault::After,
            CompletionCertainty::MayHaveApplied,
            true,
            true,
        ),
        (
            InjectedFault::MayHaveAppliedBefore,
            CompletionCertainty::MayHaveApplied,
            true,
            false,
        ),
        (
            InjectedFault::MayHaveAppliedAfter,
            CompletionCertainty::MayHaveApplied,
            true,
            true,
        ),
    ];

    for (fault, expected_certainty, poisoned, accepted) in cases {
        let (mut queue, control) = new_queue();
        let baseline = control.status_now().accepted_tail;
        control.inject_append_fault(fault);

        let error = complete(queue.submit(request(10), SimInstant::ZERO))
            .expect_err("injected append must fail");
        assert_eq!(error.certainty(), expected_certainty, "fault={fault:?}");
        assert_eq!(queue.recovery_required(), poisoned, "fault={fault:?}");
        let status = control.status_now();
        assert_eq!(
            status.accepted_tail,
            RingCursor::new(baseline.get() + u64::from(accepted)),
            "fault={fault:?}"
        );
        assert_eq!(status.durable_tail, baseline, "fault={fault:?}");

        if poisoned {
            assert_eq!(
                queue.snapshot(SimInstant::ZERO),
                Err(DurableQueueError::RecoveryRequired),
                "fault={fault:?}"
            );
        } else {
            assert!(queue.snapshot(SimInstant::ZERO).unwrap().jobs.is_empty());
            assert!(matches!(
                complete(queue.submit(request(10), SimInstant::ZERO)).unwrap(),
                SubmitOutcome::Submitted { .. }
            ));
        }
    }
}

#[test]
fn sync_completion_matrix_controls_visibility_and_poisoning() {
    let cases = [
        (
            InjectedFault::Before,
            CompletionCertainty::MayHaveApplied,
            true,
            false,
        ),
        (
            InjectedFault::After,
            CompletionCertainty::Applied,
            false,
            true,
        ),
        (
            InjectedFault::MayHaveAppliedBefore,
            CompletionCertainty::MayHaveApplied,
            true,
            false,
        ),
        (
            InjectedFault::MayHaveAppliedAfter,
            CompletionCertainty::MayHaveApplied,
            true,
            true,
        ),
    ];

    for (fault, expected_certainty, poisoned, durable) in cases {
        let (mut queue, control) = new_queue();
        let baseline = control.status_now().durable_tail;
        control.inject_sync_fault(fault);

        let error = complete(queue.submit(request(20), SimInstant::ZERO))
            .expect_err("injected sync must fail");
        assert_eq!(error.certainty(), expected_certainty, "fault={fault:?}");
        assert_eq!(queue.recovery_required(), poisoned, "fault={fault:?}");
        let status = control.status_now();
        let appended_tail = RingCursor::new(baseline.get() + 1);
        assert_eq!(status.accepted_tail, appended_tail, "fault={fault:?}");
        assert_eq!(
            status.durable_tail,
            if durable { appended_tail } else { baseline },
            "fault={fault:?}"
        );

        if poisoned {
            assert_eq!(
                queue.snapshot(SimInstant::ZERO),
                Err(DurableQueueError::RecoveryRequired),
                "fault={fault:?}"
            );
        } else {
            let queue_snapshot = queue.snapshot(SimInstant::ZERO).unwrap();
            assert_eq!(queue_snapshot.jobs.len(), 1);
            let job_id = queue_snapshot.jobs[0].job_id;
            assert_eq!(
                complete(queue.submit(request(20), SimInstant::ZERO)).unwrap(),
                SubmitOutcome::DuplicateActive { job_id }
            );
        }
    }
}

#[test]
fn ambiguous_sync_poison_requires_an_explicit_ring_reopen() {
    let (mut queue, ring) = new_queue();
    ring.inject_sync_fault(InjectedFault::MayHaveAppliedAfter);

    let error = complete(queue.submit(request(21), SimInstant::ZERO)).unwrap_err();
    assert_eq!(error.certainty(), CompletionCertainty::MayHaveApplied);

    let ring = queue.into_ring();
    let status_error = complete(ring.status()).unwrap_err();
    assert_eq!(status_error.certainty(), CompletionCertainty::NotApplied);
    assert_eq!(status_error.error(), &RingError::RecoveryRequired);

    ring.reopen();
    complete(ring.status()).expect("reopening clears the terminal ring session state");
}

#[test]
fn poisoned_queue_blocks_every_state_operation_until_recovery() {
    let (mut queue, control) = new_queue();
    control.inject_append_fault(InjectedFault::MayHaveAppliedBefore);
    let error = complete(queue.submit(request(30), SimInstant::ZERO)).unwrap_err();
    assert_eq!(error.certainty(), CompletionCertainty::MayHaveApplied);
    assert!(queue.recovery_required());

    assert_every_operation_is_fenced(&mut queue);
}

#[test]
fn recovery_reconciles_an_accepted_ambiguous_write_without_a_crash() {
    let (mut queue, control) = new_queue();
    control.inject_append_fault(InjectedFault::MayHaveAppliedAfter);
    let submitted_request = request(40);
    let error = complete(queue.submit(submitted_request.clone(), SimInstant::ZERO)).unwrap_err();
    assert_eq!(error.certainty(), CompletionCertainty::MayHaveApplied);
    assert!(queue.recovery_required());
    let accepted = control.status_now();
    assert_eq!(accepted.accepted_tail, RingCursor::new(3));
    assert_eq!(accepted.durable_tail, RingCursor::new(2));

    let ring = queue.into_ring();
    let mut recovered = open_queue(queue_config(), ring, 1);
    assert_eq!(recovered.incarnation(), 2);
    let outcome = complete(recovered.submit(submitted_request, SimInstant::ZERO)).unwrap();
    assert!(matches!(outcome, SubmitOutcome::DuplicateActive { .. }));
    assert_eq!(recovered.snapshot(SimInstant::ZERO).unwrap().jobs.len(), 1);
    let recovered_status = control.status_now();
    assert_eq!(recovered_status.accepted_tail, RingCursor::new(4));
    assert_eq!(
        recovered_status.durable_tail,
        recovered_status.accepted_tail
    );
}

#[test]
fn recovery_reads_a_longer_history_in_bounded_single_record_pages() {
    let limits = RingLimits {
        max_read_records: 1,
        ..ring_limits()
    };
    let ring = new_ring(limits);
    let control = ring.clone();
    let mut queue = open_queue(queue_config(), ring, 1);

    let expected_ids: Vec<_> = (0..6)
        .map(|index| {
            complete(queue.submit(request(100 + index), SimInstant::ZERO))
                .unwrap()
                .job_id()
        })
        .collect();
    let ring = queue.into_ring();
    assert_eq!(ring.crash().discarded_records, 0);

    let mut recovered = open_queue(queue_config(), ring, 1);
    let snapshot = recovered.snapshot(SimInstant::ZERO).unwrap();
    assert_eq!(
        snapshot
            .jobs
            .iter()
            .map(|job| job.job_id)
            .collect::<Vec<_>>(),
        expected_ids
    );
    let status = control.status_now();
    assert_eq!(status.accepted_tail, RingCursor::new(9));
    assert_eq!(status.durable_tail, status.accepted_tail);
    assert_eq!(status.accepted_live_records, 9);
}

#[test]
fn recovery_enforces_a_total_record_budget_in_addition_to_page_size() {
    let (mut queue, control) = new_queue();
    complete(queue.submit(request(150), SimInstant::ZERO)).unwrap();
    let ring = queue.into_ring();
    let before = control.status_now();

    let error = match complete(DurableQueue::recover(
        queue_config(),
        ring,
        RecoveryConfig::new(1, RECOVERY_READ_BYTES, 2),
    )) {
        Ok(_) => panic!("recovery exceeded its total record budget"),
        Err(error) => error,
    };
    assert_eq!(error.certainty(), CompletionCertainty::MayHaveApplied);
    assert_eq!(
        error.error(),
        &DurableQueueError::ReplayRecordLimitExceeded { limit: 2 }
    );
    let after = control.status_now();
    assert_eq!(after.accepted_tail, before.accepted_tail);
    assert_eq!(after.durable_tail, before.durable_tail);
}

#[test]
fn a_stale_live_queue_detects_that_recovery_advanced_the_tail() {
    let ring = new_ring(ring_limits());
    let mut stale = open_queue(queue_config(), ring.clone(), 1);
    let _new_owner = open_queue(queue_config(), ring, 1);

    let error = complete(stale.submit(request(200), SimInstant::ZERO)).unwrap_err();
    assert_eq!(error.certainty(), CompletionCertainty::NotApplied);
    assert_eq!(
        error.error(),
        &DurableQueueError::Ring(RingError::PositionConflict {
            expected: RingCursor::new(2),
            actual: RingCursor::new(3),
        })
    );
    assert!(stale.recovery_required());
    assert_eq!(
        stale.snapshot(SimInstant::ZERO),
        Err(DurableQueueError::RecoveryRequired)
    );
}
