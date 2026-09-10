use super::*;
use crate::types::TopicId;

fn identity(epoch: i16) -> ProducerIdentity {
    ProducerIdentity {
        producer_id: 42,
        epoch,
    }
}
fn partition(index: i32) -> TopicPartition {
    TopicPartition {
        topic: TopicId([1; 16]),
        partition: index,
    }
}
fn refreshing(partitions: i32) -> ProducerLedger {
    let mut ledger = ProducerLedger::new(identity(0), partitions as usize + 1, 5).unwrap();
    for index in 0..=partitions {
        ledger.register(partition(index)).unwrap();
    }
    ledger.assign(partition(partitions), u64::MAX, 7).unwrap();
    ledger.cancel(partition(partitions), u64::MAX).unwrap();
    ledger.begin_identity_refresh().unwrap();
    ledger
}
fn drain(ledger: &mut ProducerLedger) -> (usize, Vec<IdentityChange>) {
    let mut changes = Vec::new();
    for visits in 1..1000 {
        let progress = ledger.install_identity_step().unwrap();
        assert_eq!(progress.work_items, 1);
        if let Some(change) = progress.change {
            changes.push(change);
        }
        if progress.complete {
            assert!(!ledger.has_identity_install_work());
            return (visits, changes);
        }
        assert_eq!(ledger.recovery_state(), RecoveryState::RefreshingIdentity);
    }
    panic!("finite recovery work did not finish");
}

#[test]
fn empty_topology_does_not_add_visits_and_sequences_reset_lazily() {
    let mut ledger = refreshing(100_000);
    ledger.begin_identity_install(identity(1)).unwrap();
    assert_eq!(ledger.identity(), identity(0));
    assert_eq!(
        ledger.assign(partition(0), 0, 1),
        Err(LedgerError::RecoveryPending)
    );
    assert_eq!(drain(&mut ledger), (1, Vec::new()));
    assert_eq!(
        ledger
            .assign(partition(100_000), 0, 1)
            .unwrap()
            .base_sequence,
        Sequence::ZERO
    );
    assert_eq!(
        ledger
            .assign(partition(100_000), 1, 2)
            .unwrap()
            .base_sequence
            .get(),
        1
    );
}

#[test]
fn late_nonhead_cancellation_is_settled_without_a_sequence_hole() {
    let mut ledger = ProducerLedger::new(identity(0), 2, 5).unwrap();
    for p in 0..2 {
        ledger.register(partition(p)).unwrap();
    }
    for batch in 0..3 {
        ledger.assign(partition(0), batch, 3).unwrap();
    }
    ledger.assign(partition(1), 3, 1).unwrap();
    ledger.cancel(partition(1), 3).unwrap();
    ledger.begin_identity_refresh().unwrap();
    assert!(ledger.cancel(partition(0), 1).unwrap().terminal.is_empty());
    ledger.begin_identity_install(identity(1)).unwrap();
    assert_eq!(
        ledger.start_attempt(partition(0), 0, 1),
        Err(LedgerError::RecoveryPending)
    );
    let (visits, changes) = drain(&mut ledger);
    assert_eq!(visits, 5);
    assert_eq!(changes.len(), 3);
    let IdentityChange::Terminal(terminal) = changes[1] else {
        panic!("cancelled entry survived")
    };
    assert_eq!(terminal.assignment.batch, 1);
    assert_eq!(terminal.outcome.kind, DeliveryKind::NotWritten);
    assert_eq!(terminal.outcome.reason, FailureReason::Cancelled);
    assert!(!terminal.transmitted);
    assert_eq!(
        ledger
            .assignment(partition(0), 2)
            .unwrap()
            .base_sequence
            .get(),
        3
    );
    assert_eq!(ledger.stats().unresolved, 2);
    assert_eq!(
        ledger
            .assign(partition(0), 4, 1)
            .unwrap()
            .base_sequence
            .get(),
        6
    );
}

#[test]
fn removing_an_already_refinalized_head_restarts_the_survivor_layout() {
    let mut ledger = ProducerLedger::new(identity(0), 2, 5).unwrap();
    for p in 0..2 {
        ledger.register(partition(p)).unwrap();
    }
    for batch in 0..3 {
        ledger.assign(partition(0), batch, 2).unwrap();
    }
    ledger.assign(partition(1), 3, 1).unwrap();
    ledger.cancel(partition(1), 3).unwrap();
    ledger.begin_identity_refresh().unwrap();
    ledger.begin_identity_install(identity(1)).unwrap();
    for _ in 0..2 {
        assert!(ledger.install_identity_step().unwrap().change.is_some());
    }
    assert_eq!(
        ledger
            .assignment(partition(0), 1)
            .unwrap()
            .base_sequence
            .get(),
        2
    );
    assert_eq!(ledger.cancel(partition(0), 0).unwrap().terminal.len(), 1);
    let (visits, changes) = drain(&mut ledger);
    assert_eq!(visits, 5); // restart, two entries, partition end, completion
    assert_eq!(changes.len(), 2);
    assert_eq!(
        ledger.assignment(partition(0), 1).unwrap().base_sequence,
        Sequence::ZERO
    );
    assert_eq!(
        ledger
            .assignment(partition(0), 2)
            .unwrap()
            .base_sequence
            .get(),
        2
    );
}

#[test]
fn failed_close_cannot_be_resurrected_by_an_installation() {
    let mut ledger = refreshing(1);
    ledger.begin_identity_install(identity(1)).unwrap();
    ledger.begin_failure(FailureReason::Closed);
    assert!(!ledger.has_identity_install_work());
    assert_eq!(
        ledger.install_identity_step(),
        Err(LedgerError::RecoveryPending)
    );
    assert_eq!(ledger.recovery_state(), RecoveryState::FailedClosed);
    assert_eq!(ledger.identity(), identity(0));
}

#[test]
fn repeated_cancellation_of_a_pending_entry_cannot_starve_installation() {
    let mut ledger = ProducerLedger::new(identity(0), 2, 5).unwrap();
    for p in 0..2 {
        ledger.register(partition(p)).unwrap();
    }
    for batch in 0..2 {
        ledger.assign(partition(0), batch, 1).unwrap();
    }
    ledger.assign(partition(1), 2, 1).unwrap();
    ledger.cancel(partition(1), 2).unwrap();
    ledger.begin_identity_refresh().unwrap();
    ledger.begin_identity_install(identity(1)).unwrap();
    assert!(ledger.cancel(partition(0), 1).unwrap().terminal.is_empty());
    let mut terminals = 0;
    let mut complete = false;
    for _ in 0..6 {
        if terminals == 0 {
            assert!(ledger.cancel(partition(0), 1).unwrap().terminal.is_empty());
        }
        let progress = ledger.install_identity_step().unwrap();
        if let Some(IdentityChange::Terminal(terminal)) = progress.change {
            assert_eq!(terminal.assignment.batch, 1);
            terminals += 1;
        }
        if progress.complete {
            complete = true;
            break;
        }
    }
    assert!(complete);
    assert_eq!(terminals, 1);
    assert_eq!(
        ledger.assignment(partition(0), 0).unwrap().base_sequence,
        Sequence::ZERO
    );
}

#[test]
fn cancellation_after_partition_completion_invalidates_its_empty_sequence_marker() {
    let mut ledger = ProducerLedger::new(identity(0), 2, 5).unwrap();
    for p in 0..2 {
        ledger.register(partition(p)).unwrap();
    }
    ledger.assign(partition(0), 0, 12).unwrap();
    ledger.assign(partition(1), 1, 1).unwrap();
    ledger.cancel(partition(1), 1).unwrap();
    ledger.begin_identity_refresh().unwrap();
    ledger.begin_identity_install(identity(1)).unwrap();
    ledger.install_identity_step().unwrap(); // refinalize the only entry
    ledger.install_identity_step().unwrap(); // complete this partition
    assert_eq!(ledger.cancel(partition(0), 0).unwrap().terminal.len(), 1);
    assert_eq!(drain(&mut ledger), (2, Vec::new()));
    assert_eq!(
        ledger.assign(partition(0), 2, 1).unwrap().base_sequence,
        Sequence::ZERO
    );
}

#[test]
fn bounded_later_attempt_query_preserves_old_identity_fences() {
    let mut ledger = ProducerLedger::new(identity(0), 1, 5).unwrap();
    ledger.register(partition(0)).unwrap();
    for batch in 0..3 {
        ledger.assign(partition(0), batch, 1).unwrap();
    }
    assert!(!ledger.later_requires_old_identity(partition(0), 0).unwrap());
    ledger.start_attempt(partition(0), 0, 1).unwrap();
    ledger.start_attempt(partition(0), 1, 2).unwrap();
    assert!(ledger.later_requires_old_identity(partition(0), 0).unwrap());
    assert!(!ledger.later_requires_old_identity(partition(0), 1).unwrap());
    ledger.retire_attempt(partition(0), 1, 2, false).unwrap();
    assert!(!ledger.later_requires_old_identity(partition(0), 0).unwrap());
    ledger.start_attempt(partition(0), 1, 3).unwrap();
    ledger.retire_attempt(partition(0), 1, 3, true).unwrap();
    assert!(ledger.later_requires_old_identity(partition(0), 0).unwrap());
    assert_eq!(
        ledger.later_requires_old_identity(partition(0), 99),
        Err(LedgerError::UnknownBatch)
    );
}

#[test]
fn cancellation_interleavings_match_a_fifo_survivor_oracle() {
    // The oracle keeps only admission order, record counts, and cancelled IDs.
    // It does not reproduce the recovery cursor or partition-entry state.
    for seed in 1..=128u64 {
        let mut ledger = ProducerLedger::new(identity(0), 9, 5).unwrap();
        let mut model = BTreeMap::<TopicPartition, Vec<(u64, u32)>>::new();
        for p in 0..9 {
            ledger.register(partition(p)).unwrap();
        }
        for p in 0..8 {
            for slot in 0..5 {
                let batch = p as u64 * 5 + slot;
                let count = (seed.wrapping_mul(batch + 1) % 17 + 1) as u32;
                ledger.assign(partition(p), batch, count).unwrap();
                model.entry(partition(p)).or_default().push((batch, count));
            }
        }
        ledger.assign(partition(8), 100, 1).unwrap();
        ledger.cancel(partition(8), 100).unwrap();
        ledger.begin_identity_refresh().unwrap();
        ledger.begin_identity_install(identity(1)).unwrap();
        let mut cancelled = BTreeSet::new();
        let mut terminal = BTreeSet::new();
        let mut materialized = BTreeMap::<u64, Assignment>::new();
        let mut random = seed;
        let mut complete = false;
        for step in 0..500 {
            random ^= random << 13;
            random ^= random >> 7;
            random ^= random << 17;
            if step < 60 && random % 3 == 0 {
                let batch = (random >> 8) % 40;
                if cancelled.insert(batch) {
                    let change = ledger.cancel(partition((batch / 5) as i32), batch).unwrap();
                    for entry in change.terminal {
                        assert!(terminal.insert(entry.assignment.batch), "seed={seed}");
                        assert_eq!(entry.outcome.kind, DeliveryKind::NotWritten);
                        materialized.remove(&entry.assignment.batch);
                    }
                }
            }
            let progress = ledger.install_identity_step().unwrap();
            assert_eq!(progress.work_items, 1);
            match progress.change {
                Some(IdentityChange::Refinalized(change)) => {
                    materialized.insert(change.after.batch, change.after);
                }
                Some(IdentityChange::Terminal(entry)) => {
                    assert!(terminal.insert(entry.assignment.batch), "seed={seed}");
                    assert_eq!(entry.outcome.kind, DeliveryKind::NotWritten);
                    materialized.remove(&entry.assignment.batch);
                }
                None => {}
            }
            if progress.complete {
                complete = true;
                break;
            }
            assert_eq!(ledger.identity(), identity(0));
            assert_eq!(
                ledger.start_attempt(partition(0), 0, 1),
                Err(LedgerError::RecoveryPending)
            );
        }
        assert!(complete, "seed={seed}");
        assert_eq!(terminal, cancelled, "seed={seed}");
        assert_eq!(ledger.stats().unresolved, 40 - cancelled.len());
        for (partition, batches) in model {
            let mut expected_sequence = 0;
            for (batch, count) in batches {
                if cancelled.contains(&batch) {
                    continue;
                }
                let actual = ledger.assignment(partition, batch).unwrap();
                assert_eq!(
                    actual.base_sequence.get(),
                    expected_sequence,
                    "seed={seed} batch={batch}"
                );
                assert_eq!(actual.identity, identity(1));
                assert_eq!(materialized.get(&batch), Some(&actual));
                expected_sequence += count as i32;
            }
            if ledger.unresolved(partition).unwrap() == 5 {
                let head = (partition.partition as u64) * 5;
                ledger.start_attempt(partition, head, 1).unwrap();
                ledger
                    .response(
                        partition,
                        head,
                        1,
                        BrokerOutcome::Success {
                            base_offset: Some(0),
                            timestamp: None,
                        },
                    )
                    .unwrap();
            }
            assert_eq!(
                ledger
                    .assign(partition, 1000 + partition.partition as u64, 1)
                    .unwrap()
                    .base_sequence
                    .get(),
                expected_sequence
            );
        }
    }
}

#[test]
fn forgotten_history_never_resets_an_acknowledged_partition_under_the_same_identity() {
    let mut ledger = ProducerLedger::new(identity(0), 2, 5).unwrap();
    ledger.register(partition(0)).unwrap();
    assert!(ledger.can_forget_partition(partition(0)).unwrap());
    ledger.assign(partition(0), 0, 12).unwrap();
    ledger.start_attempt(partition(0), 0, 1).unwrap();
    ledger
        .response(
            partition(0),
            0,
            1,
            BrokerOutcome::Success {
                base_offset: Some(0),
                timestamp: None,
            },
        )
        .unwrap();
    assert!(!ledger.can_forget_partition(partition(0)).unwrap());
    assert_eq!(ledger.remove(partition(0)), Err(LedgerError::PartitionBusy));
    ledger.register(partition(0)).unwrap(); // same UUID reopened
    assert_eq!(
        ledger
            .assign(partition(0), 1, 1)
            .unwrap()
            .base_sequence
            .get(),
        12
    );
    ledger.cancel(partition(0), 1).unwrap();
    ledger.request_identity_refresh().unwrap();
    ledger.request_identity_refresh().unwrap();
    ledger.begin_identity_refresh().unwrap();
    ledger.request_identity_refresh().unwrap();
    ledger.begin_identity_install(identity(1)).unwrap();
    drain(&mut ledger);
    assert!(ledger.can_forget_partition(partition(0)).unwrap());
    ledger.remove(partition(0)).unwrap();
    ledger.register(partition(0)).unwrap();
    assert_eq!(
        ledger.assign(partition(0), 2, 1).unwrap().base_sequence,
        Sequence::ZERO
    );
    ledger.begin_failure(FailureReason::Closed);
    assert_eq!(
        ledger.request_identity_refresh(),
        Err(LedgerError::FailedClosed)
    );
}

#[test]
fn exact_sequence_wrap_can_be_forgotten_without_changing_the_next_assignment() {
    let mut ledger = ProducerLedger::new(identity(0), 1, 5).unwrap();
    ledger.register(partition(0)).unwrap();
    for (batch, count) in [(0, i32::MAX as u32), (1, 1)] {
        ledger.assign(partition(0), batch, count).unwrap();
        ledger
            .start_attempt(partition(0), batch, batch + 1)
            .unwrap();
        ledger
            .response(
                partition(0),
                batch,
                batch + 1,
                BrokerOutcome::Success {
                    base_offset: None,
                    timestamp: None,
                },
            )
            .unwrap();
    }
    assert!(ledger.can_forget_partition(partition(0)).unwrap());
    ledger.remove(partition(0)).unwrap();
    ledger.register(partition(0)).unwrap();
    assert_eq!(
        ledger.assign(partition(0), 2, 1).unwrap().base_sequence,
        Sequence::ZERO
    );
}

#[test]
fn overlapping_reclaim_and_unknown_triggers_share_one_bounded_epoch_installation() {
    for reclaim_first in [false, true] {
        let mut ledger = ProducerLedger::new(identity(0), 257, 5).unwrap();
        for p in 0..257 {
            ledger.register(partition(p)).unwrap();
            ledger.assign(partition(p), p as u64, 1).unwrap();
            ledger.start_attempt(partition(p), p as u64, 1).unwrap();
        }
        if reclaim_first {
            ledger.request_identity_refresh().unwrap();
        }
        for p in 0..257 {
            let change = ledger
                .expire(partition(p), p as u64, FailureReason::Deadline)
                .unwrap();
            assert_eq!(change.terminal.len(), 1);
            assert_eq!(
                change.terminal[0].outcome,
                DeliveryOutcome::unknown(FailureReason::Deadline)
            );
        }
        ledger.request_identity_refresh().unwrap();
        assert_eq!(ledger.stats().unresolved_partitions, 257);
        ledger.begin_identity_refresh().unwrap();
        ledger.request_identity_refresh().unwrap();
        ledger.begin_identity_install(identity(1)).unwrap();
        for remaining in (0..257).rev() {
            let progress = ledger.install_identity_step().unwrap();
            assert_eq!(progress.work_items, 1);
            assert!(progress.change.is_none() && !progress.complete);
            assert_eq!(ledger.stats().unresolved_partitions, remaining);
        }
        assert!(ledger.install_identity_step().unwrap().complete);
        assert_eq!(ledger.identity(), identity(1));
        for p in 0..257 {
            assert!(ledger.can_forget_partition(partition(p)).unwrap());
            ledger.remove(partition(p)).unwrap();
            ledger.register(partition(p)).unwrap();
            let fresh = ledger.assign(partition(p), 1000 + p as u64, 1).unwrap();
            assert_eq!(fresh.identity, identity(1));
            assert_eq!(fresh.base_sequence, Sequence::ZERO);
        }
    }
}
