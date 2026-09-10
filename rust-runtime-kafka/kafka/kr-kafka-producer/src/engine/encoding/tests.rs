use super::*;
use crate::admission::Admission;
use crate::control::{MetadataPartition, MetadataTopic};

fn setup(compression: Compression, lanes: u8) -> (ProducerEngine, Admission, TopicHandle) {
    let config = ProducerConfig {
        compression,
        codec_contexts: u8::from(matches!(compression, Compression::Zstd { .. })),
        lanes,
        record_descriptors: 128,
        delivery_event_capacity: 128,
        pending_records_per_topic: 128,
        max_batches: 32,
        input_bytes: 1024 * 1024,
        compressed_bytes: 1024 * 1024,
        batch_target_bytes: 4096,
        batch_hard_bytes: 4096,
        output_chunk_bytes: 4096,
        progressive_threshold: 32,
        request_target_bytes: 8192,
        request_hard_bytes: 8192,
        ..ProducerConfig::default()
    };
    let mut engine = ProducerEngine::new(
        config.clone(),
        Some(ProducerIdentity {
            producer_id: 1,
            epoch: 0,
        }),
    )
    .unwrap();
    let admission = Admission::new(
        &config,
        engine.credits(),
        engine.validated.effective_batch_payload_bytes,
    );
    let topic = engine.open_topic("encoder", RuntimeInstant::ZERO).unwrap();
    engine
        .apply_metadata(
            RuntimeInstant::ZERO,
            &[topic],
            MetadataUpdate {
                throttle_ms: 0,
                cluster_id: None,
                controller_id: 0,
                brokers: vec![BrokerNode {
                    id: 0,
                    host: "broker".into(),
                    port: 9092,
                    rack: None,
                }],
                topics: vec![MetadataTopic {
                    requested_index: 0,
                    id: TopicId([1; 16]),
                    name: Some("encoder".into()),
                    error_code: 0,
                    partitions: (0..3)
                        .map(|index| MetadataPartition {
                            replicas: Vec::new(),
                            isr: Vec::new(),
                            offline: Vec::new(),
                            index,
                            error_code: 0,
                            metadata: PartitionMetadata {
                                leader: 0,
                                leader_epoch: 0,
                            },
                        })
                        .collect(),
                }],
            },
        )
        .unwrap();
    assert!(matches!(
        engine.pop_event().unwrap().event,
        Event::TopicReady { .. }
    ));
    (engine, admission, topic)
}
fn submit(
    engine: &mut ProducerEngine,
    admission: &mut Admission,
    topic: TopicHandle,
    partition: i32,
    count: usize,
    value: &[u8],
    pending: bool,
) {
    let records: Vec<_> = (0..count)
        .map(|_| RecordDescriptor {
            topic,
            partition_hint: Some(partition),
            lane_hint: None,
            key: None,
            value: Some(value),
            headers: &[],
            timestamp_ms: 0,
            user_token: 0,
            delivery_timeout: None,
        })
        .collect();
    let lane = partition as u8 % engine.config.lanes;
    let (result, batch) =
        admission.prepare_copy(RuntimeInstant::ZERO, &records, &vec![Ok(lane); count]);
    assert_eq!(result.accepted as usize, count, "{:?}", result.error);
    let choice = if pending {
        PartitionChoice::Pending
    } else {
        PartitionChoice::Partition(partition)
    };
    engine
        .admit(RuntimeInstant::ZERO, batch.unwrap(), &vec![choice; count])
        .unwrap();
}
fn key(partition: i32) -> TopicPartition {
    TopicPartition {
        topic: TopicId([1; 16]),
        partition,
    }
}
fn one() -> WorkBudget {
    WorkBudget {
        bytes: 64,
        items: 1,
    }
}

#[test]
fn one_item_turns_alternate_append_and_actual_raw_work_under_hot_ingress() {
    let (mut engine, mut admission, topic) = setup(Compression::None, 2);
    submit(&mut engine, &mut admission, topic, 0, 32, &[7; 128], false);
    submit(&mut engine, &mut admission, topic, 1, 1, &[8; 128], false);
    let mut served = Vec::new();
    for _ in 0..8 {
        let progress = engine.encode(RuntimeInstant::ZERO, one());
        assert!(progress.items <= 1 && progress.bytes <= 64);
        if let Some(service) = engine.encoder.last_service {
            served.push(service);
        }
    }
    assert!(
        served
            .iter()
            .any(|&(partition, codec, raw)| partition == key(0) && codec && raw > 0)
    );
    assert!(
        served
            .iter()
            .any(|&(partition, codec, raw)| partition == key(1) && codec && raw > 0)
    );
    assert!(
        !engine.partitions[&key(0)].records.is_empty(),
        "hot backlog remains while cold codec progresses"
    );
    assert_eq!(served[0].0, key(0));
    assert_eq!(served[1].0, key(1));
}

#[test]
fn unrelated_credit_changes_and_unchanged_pending_choices_cannot_self_wake_encoder() {
    let (mut engine, mut admission, topic) = setup(Compression::None, 1);
    let held = engine
        .credits
        .reserve(&[Claim {
            resource: Resource::CompressedBytes,
            amount: engine.config.compressed_bytes,
            lane: 0,
        }])
        .unwrap();
    submit(&mut engine, &mut admission, topic, 0, 2, &[7; 128], false);
    for _ in 0..64 {
        if !engine
            .encode(RuntimeInstant::ZERO, one())
            .remaining_immediate
        {
            break;
        }
    }
    assert!(engine.encoder.ready.is_empty());
    assert!(!engine.encoder.has_recheck());
    for _ in 0..32 {
        drop(
            engine
                .credits
                .reserve(&[Claim {
                    resource: Resource::InputBytes,
                    amount: 1,
                    lane: 0,
                }])
                .unwrap(),
        );
        let progress = engine.encode(RuntimeInstant::ZERO, one());
        assert_eq!(progress.items, 0);
        assert!(!progress.remaining_immediate);
    }
    drop(held);
    let mut resumed = false;
    for _ in 0..16 {
        let progress = engine.encode(RuntimeInstant::ZERO, one());
        assert!(progress.items <= 1);
        resumed |= progress.bytes > 0;
    }
    assert!(
        resumed,
        "actual compressed credit release resumes its waiter"
    );

    let (mut engine, mut admission, topic) = setup(Compression::None, 1);
    submit(&mut engine, &mut admission, topic, 0, 1, &[7; 128], true);
    let token = engine.pending_records().next().unwrap().token;
    submit(&mut engine, &mut admission, topic, 0, 1, &[8; 128], false);
    for _ in 0..32 {
        engine
            .route_pending(RuntimeInstant::ZERO, token, PartitionChoice::Pending)
            .unwrap();
        let progress = engine.encode(RuntimeInstant::ZERO, one());
        assert_eq!(progress.items, 0);
        assert!(!progress.remaining_immediate);
    }
    engine
        .route_pending(RuntimeInstant::ZERO, token, PartitionChoice::Partition(0))
        .unwrap();
    assert_eq!(engine.encode(RuntimeInstant::ZERO, one()).items, 1);
}

#[test]
fn cleanup_and_codec_each_progress_with_a_one_item_phase_budget() {
    let (mut engine, mut admission, topic) = setup(Compression::None, 1);
    submit(&mut engine, &mut admission, topic, 0, 12, &[7; 128], false);
    for _ in 0..1000 {
        engine.encode(RuntimeInstant::ZERO, one());
        if engine.partitions[&key(0)].records.is_empty() {
            break;
        }
    }
    let batch = *engine.partitions[&key(0)].batches.back().unwrap();
    engine.finish_unassigned(batch, FailureReason::Cancelled);
    submit(&mut engine, &mut admission, topic, 1, 1, &[8; 128], false);
    let input_before = engine.credits.snapshot()[Resource::InputBytes as usize].held;
    let mut codec = false;
    for _ in 0..12 {
        let progress = engine.encode(RuntimeInstant::ZERO, one());
        assert!(progress.items <= 1);
        codec |= engine
            .encoder
            .last_service
            .is_some_and(|(partition, encoded, raw)| partition == key(1) && encoded && raw > 0);
    }
    assert!(codec);
    assert!(
        engine.credits.snapshot()[Resource::InputBytes as usize].held < input_before,
        "cleanup returns retained input while codec service progresses"
    );
    assert!(
        engine.status().terminal < 12,
        "cleanup did not monopolize all service turns"
    );
}

#[test]
fn context_reclamation_targets_actual_oldest_holder_and_seal_calls_consume_items() {
    let (mut engine, mut admission, topic) = setup(Compression::Zstd { level: 1 }, 1);
    engine.config.batch_target_bytes = 1024;
    // Exercise reclamation plus immediate legacy target sealing without a connection.
    engine.config.batch_target_mode = crate::config::BatchTargetMode::Raw;
    submit(&mut engine, &mut admission, topic, 2, 1, &[1], false); // older, deferred, no context
    submit(&mut engine, &mut admission, topic, 0, 1, &[7; 128], false);
    for _ in 0..128 {
        engine.encode(RuntimeInstant::ZERO, one());
        if !engine.encoder.context_owners.is_empty() && engine.encoder.ready.is_empty() {
            break;
        }
    }
    let holder = *engine.partitions[&key(0)].batches.back().unwrap();
    let deferred = *engine.partitions[&key(2)].batches.back().unwrap();
    assert!(engine.batches.get(holder).unwrap().holds_codec_context());
    assert!(!engine.batches.get(deferred).unwrap().holds_codec_context());
    submit(&mut engine, &mut admission, topic, 1, 1, &[8; 2048], false);
    let mut seals = 0;
    for _ in 0..1024 {
        let progress = engine.encode(RuntimeInstant::ZERO, one());
        assert!(progress.items <= 1);
        if engine.last_encode_work.seal_calls != 0 {
            assert_eq!(progress.items, 1);
            seals += engine.last_encode_work.seal_calls;
        }
        if engine.batches.get(holder).unwrap().state() == BatchState::Sealed
            && engine.partitions[&key(1)]
                .batches
                .back()
                .and_then(|key| engine.batches.get(*key))
                .is_some_and(|batch| batch.state() == BatchState::Sealed)
        {
            break;
        }
    }
    assert_eq!(
        engine.batches.get(holder).unwrap().seal_reason(),
        Some(SealReason::ContextReclaimed)
    );
    assert_eq!(engine.batches.get(deferred).unwrap().seal_reason(), None);
    assert!(seals >= 2);
    assert_eq!(engine.codecs.status().available, 1);
}

#[test]
fn waiter_cursor_charges_holes_and_coalesces_changes_behind_its_frontier() {
    let mut wait = Waiters::default();
    let first = Target::Append(key(0));
    let second = Target::Append(key(1));
    let third = Target::Append(key(2));
    wait.entries.extend([first, second, third]);
    wait.changed();
    assert_eq!(wait.next(), Some(Some(first)));
    wait.entries.remove(&second);
    // Every invalidation coalesces into the same cursor and at most one restart.
    for _ in 0..100 {
        wait.changed();
    }
    assert_eq!(wait.next(), Some(Some(third)));
    assert_eq!(wait.next(), Some(None));
    assert_eq!(wait.next(), Some(Some(first)));
    assert_eq!(wait.next(), Some(Some(third)));
    wait.entries.clear();
    assert_eq!(wait.next(), Some(None));
    assert_eq!(wait.next(), None);
}

#[test]
fn batch_slot_waiters_resume_only_after_terminal_payload_owners_release_the_slot() {
    let (mut engine, mut admission, topic) = setup(Compression::None, 1);
    submit(&mut engine, &mut admission, topic, 0, 1, &[7; 128], false);
    for _ in 0..32 {
        engine.encode(RuntimeInstant::ZERO, one());
    }
    submit(&mut engine, &mut admission, topic, 1, 1, &[8; 128], false);
    engine.config.max_batches = 1;
    engine.encoder_refresh_append(key(1));
    assert!(
        engine.encoder.waiters[3]
            .entries
            .contains(&Target::Append(key(1)))
    );
    for _ in 0..8 {
        assert_eq!(engine.encode(RuntimeInstant::ZERO, one()).items, 0);
    }
    let old = *engine.partitions[&key(0)].batches.back().unwrap();
    engine.finish_unassigned(old, FailureReason::Cancelled);
    assert_eq!(engine.batches.len(), 0);
    assert!(!engine.terminal_records.is_empty());
    assert!(!engine.encoder.partitions.contains_key(&key(0)));
    for _ in 0..64 {
        let progress = engine.encode(RuntimeInstant::ZERO, one());
        assert!(progress.items <= 1);
        if engine.partitions[&key(1)].records.is_empty() {
            break;
        }
    }
    assert!(engine.partitions[&key(1)].records.is_empty());
    assert!(engine.encoder.waiters[3].entries.is_empty());
    assert_eq!(engine.batches.len(), 1);
}

#[test]
fn producer_failure_removes_all_live_encoder_memberships_under_one_item_cleanup() {
    let (mut engine, mut admission, topic) = setup(Compression::Zstd { level: 1 }, 1);
    for partition in 0..3 {
        submit(
            &mut engine,
            &mut admission,
            topic,
            partition,
            2,
            &[7; 128],
            false,
        );
    }
    for _ in 0..64 {
        engine.encode(RuntimeInstant::ZERO, one());
    }
    engine.fail_producer(FailureReason::RuntimeFailed);
    for _ in 0..512 {
        assert!(engine.on_deadline(RuntimeInstant::ZERO, one()).items <= 1);
        assert!(engine.encode(RuntimeInstant::ZERO, one()).items <= 1);
        while engine.pop_event().is_some() {}
        if engine.batches.is_empty() && engine.terminal_records.is_empty() {
            break;
        }
    }
    assert_eq!(engine.status().terminal, 6);
    assert!(engine.encoder.partitions.is_empty());
    assert!(engine.encoder.ready.is_empty());
    assert!(engine.encoder.batches.is_empty());
    assert!(engine.encoder.context_owners.is_empty());
    assert!(engine.encoder.context_ages.is_empty());
    assert!(engine.encoder.reclaiming.is_none());
    assert!(
        engine
            .encoder
            .waiters
            .iter()
            .all(|wait| wait.entries.is_empty())
    );
    assert!(engine.encoder.pending.is_empty());
    assert!(engine.encoder.pending_members.is_empty());
    assert!(engine.encoder.pending_active.is_empty());
    assert!(engine.encoder.seal_waiters.is_empty());
    assert!(engine.encoder.seal_routes.is_empty());
    assert!(engine.encoder.seal_active.is_empty());
    assert!(engine.encoder.topic_waiters.is_empty());
    assert!(engine.encoder.topic_keys.is_empty());
    assert!(engine.encoder.topic_active.is_empty());
}

#[test]
fn cancelling_or_expiring_the_last_queued_record_removes_its_ready_membership() {
    for cancel in [true, false] {
        let (mut engine, mut admission, topic) = setup(Compression::None, 1);
        submit(&mut engine, &mut admission, topic, 0, 1, &[7; 128], false);
        let record = engine.partitions[&key(0)].records.front().unwrap();
        let token = record.token;
        let deadline = record.deadline;
        assert!(!engine.encoder.ready.is_empty());
        if cancel {
            engine.cancel(RuntimeInstant::ZERO, token).unwrap();
        } else {
            for _ in 0..16 {
                assert!(engine.on_deadline(deadline, one()).items <= 1);
                if engine.status().terminal != 0 {
                    break;
                }
            }
        }
        assert_eq!(engine.status().terminal, 1);
        assert!(engine.encoder.ready.is_empty());
        assert!(engine.encoder.partitions.is_empty());
        assert_eq!(
            engine.encode(RuntimeInstant::ZERO, one()),
            Progress::default()
        );
    }
}

#[test]
fn reused_slot_cannot_let_later_fifo_batch_consume_the_only_output_credit() {
    let (mut engine, mut admission, topic) = setup(Compression::None, 1);
    let now = RuntimeInstant::ZERO;
    // An unrelated completed owner leaves a lower arena index available after
    // the older partition-1 batch was created. Slot order is not FIFO order.
    submit(&mut engine, &mut admission, topic, 0, 1, &[1; 128], false);
    engine.encoder_append(key(0), now);
    let temporary = *engine.partitions[&key(0)].batches.back().unwrap();
    submit(&mut engine, &mut admission, topic, 1, 1, &[2; 128], false);
    engine.encoder_append(key(1), now);
    let older = *engine.partitions[&key(1)].batches.back().unwrap();
    engine
        .batches
        .get_mut(older)
        .unwrap()
        .seal(SealReason::Flush);
    engine.refresh_batch_deadline(older, now);
    engine.finish_unassigned(temporary, FailureReason::Cancelled);
    submit(&mut engine, &mut admission, topic, 1, 1, &[3; 128], false);
    engine.encoder_append(key(1), now);
    let later = *engine.partitions[&key(1)].batches.back().unwrap();
    assert!(
        later < older,
        "reused slot sorts before the older FIFO owner"
    );
    engine
        .batches
        .get_mut(later)
        .unwrap()
        .seal(SealReason::Flush);
    engine.refresh_batch_deadline(later, now);
    let mut pressure = engine
        .credits
        .reserve(&[Claim {
            resource: Resource::CompressedBytes,
            amount: engine.config.compressed_bytes,
            lane: 0,
        }])
        .unwrap();
    for _ in 0..128 {
        if !engine.encode(now, one()).remaining_immediate {
            break;
        }
    }
    assert_eq!(
        engine.batches.get(older).unwrap().state(),
        BatchState::Sealing
    );
    let envelope = engine.validated.effective_batch_payload_bytes as usize
        + kr_kafka_record::BATCH_HEADER_BYTES;
    pressure
        .shrink(
            Resource::CompressedBytes,
            engine.config.compressed_bytes - envelope,
        )
        .unwrap();
    assert_eq!(
        engine.encode(
            now,
            WorkBudget {
                bytes: 64,
                items: 0
            }
        ),
        Progress::default()
    );
    assert_eq!(engine.output.status().batches, 0);
    for _ in 0..256 {
        let progress = engine.encode(now, one());
        assert!(progress.items <= 1);
        if !progress.remaining_immediate {
            break;
        }
    }
    assert_eq!(
        engine.batches.get(older).unwrap().state(),
        BatchState::Sealed,
        "available output must go to the oldest undispatchable FIFO batch; later={:?}, pool={:?}",
        engine.batches.get(later).unwrap().state(),
        engine.output.status()
    );
    assert_ne!(
        engine.batches.get(later).unwrap().state(),
        BatchState::Sealed,
        "one envelope cannot be occupied by a later batch behind a blocked head"
    );
    assert_eq!(engine.output.status().batches, 1);
    assert_eq!(
        engine.credits.snapshot()[Resource::CompressedBytes as usize].held,
        engine.config.compressed_bytes
    );
    // Actual terminal ownership release, under the same item-one maintenance
    // quota, then makes the successor eligible without advancing any clock.
    engine.finish_unassigned(older, FailureReason::Cancelled);
    for _ in 0..256 {
        assert!(engine.on_deadline(now, one()).items <= 1);
        assert!(engine.encode(now, one()).items <= 1);
        if engine.batches.get(later).unwrap().state() == BatchState::Sealed {
            break;
        }
    }
    assert_eq!(
        engine.batches.get(later).unwrap().state(),
        BatchState::Sealed
    );
}
