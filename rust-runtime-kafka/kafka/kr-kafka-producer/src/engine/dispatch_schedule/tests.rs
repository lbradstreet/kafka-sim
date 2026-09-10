use super::*;
use crate::{
    admission::Admission,
    control::{MetadataPartition, MetadataTopic},
};

fn at() -> RuntimeInstant {
    RuntimeInstant::from_nanos(123_000)
}
fn budget(items: u32) -> WorkBudget {
    WorkBudget {
        bytes: 65536,
        items,
    }
}

fn sealed(partitions: usize) -> ProducerEngine {
    sealed_layout(&(0..partitions).map(|p| p as i32).collect::<Vec<_>>(), 1)
}
fn sealed_layout(partitions: &[i32], lanes: u8) -> ProducerEngine {
    layout(partitions, lanes, false)
}
fn layout(partitions: &[i32], lanes: u8, open_neighbors: bool) -> ProducerEngine {
    let config = ProducerConfig {
        compression: Compression::None,
        codec_contexts: 0,
        lanes,
        record_descriptors: 64,
        delivery_event_capacity: 64,
        pending_records_per_topic: 64,
        max_batches: 64,
        request_max_partitions: 32,
        input_bytes: 65536,
        compressed_bytes: 1024 * 1024,
        // These scheduler fixtures require sealed batches before connecting.
        batch_target_mode: crate::config::BatchTargetMode::Raw,
        batch_target_bytes: if open_neighbors { 256 } else { 64 },
        batch_hard_bytes: 4096,
        progressive_threshold: 32,
        output_chunk_bytes: 4096,
        request_target_bytes: 8192,
        request_hard_bytes: 8192,
        linger_skip_below_rate: None,
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
    let topic = engine.open_topic("wire", at()).unwrap();
    engine
        .apply_metadata(
            at(),
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
                    name: Some("wire".into()),
                    error_code: 0,
                    partitions: (0..=partitions.iter().copied().max().unwrap())
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
    let records: Vec<_> = partitions
        .iter()
        .copied()
        .map(|partition| RecordDescriptor {
            topic,
            partition_hint: Some(partition),
            lane_hint: None,
            key: None,
            value: Some(if open_neighbors && partition == 0 {
                &[8; 1024]
            } else if lanes == 2 && partition == 1 {
                &[9; 2048]
            } else {
                &[7; 128]
            }),
            headers: &[],
            timestamp_ms: 0,
            user_token: partition as u64,
            delivery_timeout: None,
        })
        .collect();
    let mut admission = Admission::new(
        &config,
        engine.credits(),
        engine.validated.effective_batch_payload_bytes,
    );
    let choices: Vec<_> = partitions
        .iter()
        .map(|p| Ok((*p as u32 % u32::from(lanes)) as u8))
        .collect();
    let (accepted, records) = admission.prepare_copy(at(), &records, &choices);
    assert_eq!(accepted.accepted as usize, partitions.len());
    engine
        .admit(
            at(),
            records.unwrap(),
            &partitions
                .iter()
                .copied()
                .map(PartitionChoice::Partition)
                .collect::<Vec<_>>(),
        )
        .unwrap();
    for _ in 0..1024 {
        if !engine.encode(at(), budget(64)).remaining_immediate {
            break;
        }
    }
    assert_eq!(engine.batches.len(), partitions.len());
    assert!(engine.batches.iter().all(|(_, batch)| batch.state()
        == if open_neighbors && batch.partition().partition != 0 {
            BatchState::Open
        } else {
            BatchState::Sealed
        }));
    while engine.pop_order().is_some() {}
    while engine.pop_event().is_some() {}
    engine
}

fn active(engine: &mut ProducerEngine) -> ConnectionKey {
    engine.connect(0, 0).unwrap();
    let EngineOrder::Connect { key, .. } = engine.pop_order().unwrap() else {
        panic!("connect")
    };
    engine
        .on_connection(key, at(), ConnectionEvent::Active)
        .unwrap();
    key
}

#[test]
fn a_one_item_one_byte_quota_eventually_emits_one_legal_large_head() {
    let mut engine = sealed(1);
    active(&mut engine);
    // Require multiple deficit turns as well as a frame larger than the poll's
    // byte quantum. The exact full wire size must be charged once on admission.
    engine.config.request_target_bytes = 32;
    let partition = TopicPartition {
        topic: TopicId([1; 16]),
        partition: 0,
    };
    let mut emitted = None;
    for _ in 0..64 {
        let before = engine.partitions[&partition].deficit;
        let progress = engine.schedule(at(), WorkBudget { bytes: 1, items: 1 });
        assert!(progress.items <= 1);
        if let Some(EngineOrder::Dispatch { request, plan, .. }) = engine.pop_order() {
            let state = engine.requests.get(Slot::from_packed(request.0)).unwrap();
            assert_eq!(state.batches.len(), 1);
            assert_eq!(progress.bytes as usize, plan.len());
            assert!(plan.len() > 1);
            assert_eq!(
                engine.partitions[&partition].deficit,
                before + 32 - plan.len()
            );
            emitted = Some(request);
            break;
        }
    }
    assert!(
        emitted.is_some(),
        "a legal head cannot be stranded by the poll byte quota"
    );
}

#[test]
fn gathering_charges_every_candidate_against_the_outer_item_budget() {
    for quota in [1, 2, 4, 8] {
        let mut engine = sealed(32);
        active(&mut engine);
        let mut emitted = false;
        for _ in 0..64 {
            let progress = engine.schedule(at(), budget(quota));
            assert!(progress.items <= quota);
            while let Some(order) = engine.pop_order() {
                if let EngineOrder::Dispatch { request, .. } = order {
                    let count = engine
                        .requests
                        .get(Slot::from_packed(request.0))
                        .unwrap()
                        .batches
                        .len();
                    assert!(count <= progress.items as usize);
                    assert!(count <= quota as usize);
                    emitted = true;
                }
            }
            if emitted {
                break;
            }
        }
        assert!(emitted);
    }
    let mut engine = sealed(8);
    active(&mut engine);
    engine.schedule(at(), budget(64));
    let EngineOrder::Dispatch { request, plan, .. } = engine.pop_order().unwrap() else {
        panic!("dispatch")
    };
    assert_eq!(
        engine
            .requests
            .get(Slot::from_packed(request.0))
            .unwrap()
            .batches
            .len(),
        8,
        "dirty same-route owners must remain available for bounded gathering"
    );
    assert!(plan.segments().len() <= 8 * 3 + 4);
}

#[test]
fn connection_and_metadata_credit_waits_park_until_their_actual_causal_change() {
    let mut engine = sealed(2);
    let mut connecting = None;
    for _ in 0..64 {
        let progress = engine.schedule(at(), budget(8));
        while let Some(order) = engine.pop_order() {
            if let EngineOrder::Connect { key, .. } = order {
                connecting = Some(key);
            } else {
                panic!("unexpected setup order")
            }
        }
        if !progress.remaining_immediate {
            break;
        }
    }
    let connecting = connecting.expect("one connection");
    let held = engine
        .credits
        .reserve(&[Claim {
            resource: Resource::RequestMetadata,
            amount: engine.validated.credits[Resource::RequestMetadata as usize],
            lane: 0,
        }])
        .unwrap();
    for _ in 0..16 {
        assert_eq!(engine.schedule(at(), budget(8)), Progress::default());
    }
    engine
        .on_connection(connecting, at(), ConnectionEvent::Active)
        .unwrap();
    for _ in 0..64 {
        if !engine.schedule(at(), budget(8)).remaining_immediate {
            break;
        }
    }
    assert_eq!(engine.requests.len(), 0);
    for _ in 0..16 {
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
        assert_eq!(engine.schedule(at(), budget(8)), Progress::default());
    }
    drop(held);
    for _ in 0..64 {
        engine.schedule(at(), budget(8));
        if !engine.requests.is_empty() {
            break;
        }
    }
    assert_eq!(engine.requests.len(), 1);
}

#[test]
fn a_busy_writer_stays_parked_until_confirmed_full_write_releases_its_slot() {
    let mut engine = sealed(2);
    let connection = active(&mut engine);
    let mut first = None;
    for _ in 0..64 {
        engine.schedule(at(), WorkBudget { bytes: 1, items: 1 });
        if let Some(EngineOrder::Dispatch {
            request,
            correlation,
            plan,
            ..
        }) = engine.pop_order()
        {
            first = Some((request, correlation, plan.len()));
            break;
        }
    }
    let (request, correlation, bytes) = first.unwrap();
    for _ in 0..64 {
        if !engine.schedule(at(), budget(8)).remaining_immediate {
            break;
        }
    }
    for _ in 0..16 {
        assert_eq!(engine.schedule(at(), budget(8)), Progress::default());
    }
    assert_eq!(engine.requests.len(), 1);
    engine.on_write_admitted(request).unwrap();
    engine
        .on_write_at(
            at(),
            connection,
            correlation,
            bytes,
            CompletionCertainty::Applied,
        )
        .unwrap();
    for _ in 0..64 {
        engine.schedule(at(), budget(8));
        if engine.requests.len() == 2 {
            break;
        }
    }
    assert_eq!(engine.requests.len(), 2);
}

#[test]
fn an_expired_ready_head_cannot_escape_while_its_deadline_cleanup_is_still_budgeted() {
    let mut engine = sealed(1);
    active(&mut engine);
    let deadline = engine
        .batches
        .iter()
        .next()
        .unwrap()
        .1
        .oldest_deadline()
        .unwrap();
    for _ in 0..8 {
        let progress = engine.schedule(deadline, budget(1));
        assert!(progress.items <= 1);
        assert!(engine.pop_order().is_none());
    }
    assert!(engine.requests.is_empty());
    for _ in 0..32 {
        engine.on_deadline(deadline, budget(1));
        engine.encode(deadline, budget(1));
        if engine.status().terminal != 0 {
            break;
        }
    }
    assert_eq!(engine.status().terminal, 1);
    let delivery = std::iter::from_fn(|| engine.pop_event())
        .find_map(|event| {
            if let Event::Delivery(delivery) = event.event {
                Some(delivery)
            } else {
                None
            }
        })
        .unwrap();
    assert_eq!(
        delivery.outcome,
        DeliveryOutcome::not_written(FailureReason::Deadline)
    );
}

#[test]
fn terminal_owner_cleanup_removes_ready_candidate_and_causal_scheduler_memberships() {
    let mut engine = sealed(8);
    engine.scheduler_identity_changed();
    engine.schedule(at(), budget(8));
    engine.fail_producer(FailureReason::RuntimeFailed);
    for _ in 0..1024 {
        engine.on_deadline(at(), budget(1));
        engine.encode(at(), budget(1));
        while engine.pop_event().is_some() {}
        if engine.status().terminal == 8 {
            break;
        }
    }
    assert_eq!(engine.status().terminal, 8);
    assert!(engine.scheduler.dirty.is_empty());
    assert!(engine.scheduler.ready.is_empty());
    assert_eq!(engine.scheduler.ready.route_len((0, 0)), 0);
    assert!(!engine.scheduler.has_reconsideration());
    assert_eq!(engine.schedule(at(), budget(8)), Progress::default());
}

#[test]
fn gathering_cannot_bypass_a_candidates_attempt_limit() {
    let mut engine = sealed(2);
    active(&mut engine);
    let (key, batch) = engine.batches.iter().nth(1).unwrap();
    let partition = batch.partition();
    let count = batch.record_count() as u32;
    let ledger = engine.ledger.as_mut().unwrap();
    let assignment = ledger.assign(partition, key.packed(), count).unwrap();
    for attempt in 1..=u64::from(engine.config.max_attempts) {
        ledger
            .start_attempt(partition, key.packed(), attempt)
            .unwrap();
        ledger
            .retire_attempt(partition, key.packed(), attempt, false)
            .unwrap();
    }
    engine
        .batches
        .get_mut(key)
        .unwrap()
        .finalize(kr_kafka_record::Identity {
            producer_id: assignment.identity.producer_id,
            producer_epoch: assignment.identity.epoch,
            base_sequence: assignment.base_sequence.get(),
        })
        .unwrap();
    engine
        .attempt_counts
        .insert(key.packed(), u32::from(engine.config.max_attempts));
    engine.scheduler_mark(partition);
    engine.schedule(at(), budget(64));
    assert!(
        engine
            .requests
            .iter()
            .all(|(_, request)| !request.batches.contains(&key))
    );
    assert!(engine.batches.get(key).is_none());
    assert_eq!(
        engine.batches.len(),
        1,
        "healthy head remains owned through identity recovery"
    );
    assert_eq!(
        engine.ledger.as_ref().unwrap().recovery_state(),
        RecoveryState::RefreshingIdentity
    );
    assert!(!engine.is_failed());
}

#[test]
fn many_small_partitions_cannot_multiply_a_lanes_wire_credit_during_gathering() {
    let mut partitions: Vec<_> = (0..31).map(|p| p * 2).collect();
    partitions.push(1); // One large cold head competes with 31 small hot heads.
    let mut engine = sealed_layout(&partitions, 2);
    let first = active(&mut engine);
    engine.connect(0, 1).unwrap();
    let EngineOrder::Connect { key: second, .. } = engine.pop_order().unwrap() else {
        panic!("second lane")
    };
    engine
        .on_connection(second, at(), ConnectionEvent::Active)
        .unwrap();
    let keys: Vec<_> = engine.partitions.keys().copied().collect();
    for partition in keys {
        engine.scheduler_classify(partition, at());
    }
    let quantum = engine.config.request_target_bytes as usize / 2;
    let progress = engine.schedule(at(), budget(128));
    assert!(progress.items <= 128);
    let mut wire = [0usize; 2];
    let mut counts = [0usize; 2];
    while let Some(order) = engine.pop_order() {
        if let EngineOrder::Dispatch {
            connection,
            request,
            plan,
            ..
        } = order
        {
            let lane = if connection == first {
                0
            } else {
                assert_eq!(connection, second);
                1
            };
            wire[lane] += plan.len();
            counts[lane] += engine
                .requests
                .get(Slot::from_packed(request.0))
                .unwrap()
                .batches
                .len();
        }
    }
    assert!(
        wire[0] > 0 && wire[0] <= quantum,
        "hot lane sent {} bytes from one {}-byte grant",
        wire[0],
        quantum
    );
    assert!(wire[1] > 0 && wire[1] <= quantum);
    assert!(counts[0] > 1 && counts[0] < 31);
    assert_eq!(counts[1], 1);
}

#[test]
fn request_policy_controls_early_neighbor_sealing_and_exact_membership() {
    use crate::config::RequestBatchingPolicy::*;
    for policy in [SinglePartition, Sealed, BrokerReady] {
        let mut engine = layout(&[0, 1], 1, true);
        engine.config.request_batching_policy = policy;
        active(&mut engine);
        let progress = engine.schedule(at(), budget(64));
        if policy == BrokerReady {
            assert!(
                progress.remaining_immediate,
                "the actor encoded before scheduling; new finish work must wake it"
            );
            assert!(
                engine.pop_order().is_none(),
                "preparation precedes dispatch"
            );
            assert_eq!(engine.seal_counter.snapshot().by_reason[7], 1);
            for _ in 0..8 {
                assert_eq!(
                    engine.schedule(at(), budget(64)),
                    Progress::default(),
                    "preparation waits passively for encoder service"
                );
            }
            engine.encode(at(), budget(64));
            engine.schedule(at(), budget(64));
        }
        let EngineOrder::Dispatch { request, .. } = engine.pop_order().expect("dispatch") else {
            panic!("dispatch order");
        };
        let request = engine.requests.get(Slot::from_packed(request.0)).unwrap();
        assert_eq!(
            request.batches.len(),
            if policy == BrokerReady { 2 } else { 1 }
        );
        let partitions: BTreeSet<_> = request
            .batches
            .iter()
            .map(|key| engine.batches.get(*key).unwrap().partition().partition)
            .collect();
        assert!(partitions.contains(&0));
        if policy != BrokerReady {
            assert_eq!(
                engine
                    .batches
                    .iter()
                    .find(|(_, b)| b.partition().partition == 1)
                    .unwrap()
                    .1
                    .state(),
                BatchState::Open
            );
        }
    }
}

#[test]
fn single_partition_policy_overrides_multi_partition_request_capacity() {
    let mut engine = sealed(8);
    engine.config.request_batching_policy = crate::config::RequestBatchingPolicy::SinglePartition;
    active(&mut engine);
    engine.schedule(at(), budget(64));
    let EngineOrder::Dispatch { request, .. } = engine.pop_order().unwrap() else {
        panic!("dispatch")
    };
    assert_eq!(
        engine
            .requests
            .get(Slot::from_packed(request.0))
            .unwrap()
            .batches
            .len(),
        1
    );
}

#[test]
fn gathering_skips_routes_with_no_possible_neighbor_without_spending_work() {
    let run = |policy| {
        let mut engine = sealed(1);
        engine.config.request_batching_policy = policy;
        active(&mut engine);
        let progress = engine.schedule(at(), budget(64));
        let EngineOrder::Dispatch { plan, .. } = engine.pop_order().expect("dispatch") else {
            panic!("dispatch order");
        };
        (progress, plan.len(), engine.seal_counter.snapshot())
    };
    assert_eq!(
        run(crate::config::RequestBatchingPolicy::Sealed),
        run(crate::config::RequestBatchingPolicy::BrokerReady),
    );
}

#[test]
fn gathering_does_not_wait_for_an_unfinished_neighbor_after_one_encoder_pass() {
    let mut engine = layout(&[0, 1, 2, 3], 1, true);
    engine.config.request_batching_policy = crate::config::RequestBatchingPolicy::BrokerReady;
    active(&mut engine);
    engine.schedule(at(), budget(64));
    assert!(engine.pop_order().is_none());
    // One small service pass need not finish every neighbor. Dispatch must
    // resume without waiting for another pass or for new network/input credit.
    engine.encode(at(), budget(1));
    for _ in 0..32 {
        engine.schedule(at(), budget(1));
        if !engine.requests.is_empty() {
            break;
        }
    }
    assert!(!engine.requests.is_empty());
}

#[test]
fn gathered_neighbors_share_their_actual_encoder_completion_boundary() {
    let mut engine = layout(&[0, 1], 1, true);
    engine.config.request_batching_policy = crate::config::RequestBatchingPolicy::BrokerReady;
    active(&mut engine);
    engine.schedule(at(), budget(64));
    let delay = RuntimeDuration::from_nanos(1);
    engine.encode_with_completion_delay(at(), budget(64), delay);
    engine.schedule(at(), budget(64));
    assert!(
        engine.pop_order().is_none(),
        "do not dispatch before neighbor completion"
    );
    let done = ProducerEngine::deadline_after(at(), delay);
    engine.on_deadline(done, budget(64));
    engine.schedule(done, budget(64));
    let EngineOrder::Dispatch { request, .. } = engine.pop_order().expect("completed gather")
    else {
        panic!("dispatch")
    };
    assert_eq!(
        engine
            .requests
            .get(Slot::from_packed(request.0))
            .unwrap()
            .batches
            .len(),
        2
    );
}

#[test]
fn open_neighbor_preparation_respects_small_work_and_partition_limits() {
    for quota in [1, 2, 4, 8] {
        for limit in [1, 2, 4] {
            let mut engine = layout(&[0, 1, 2, 3, 4, 5, 6, 7], 1, true);
            engine.config.request_batching_policy =
                crate::config::RequestBatchingPolicy::BrokerReady;
            engine.config.request_max_partitions = limit;
            active(&mut engine);
            for _ in 0..32 {
                let progress = engine.schedule(at(), budget(quota));
                assert!(progress.items <= quota);
                if !engine.requests.is_empty() {
                    break;
                }
                engine.encode(at(), budget(quota));
            }
            assert!(!engine.requests.is_empty(), "quota={quota}, limit={limit}");
            for (_, request) in engine.requests.iter() {
                assert!(request.batches.len() <= usize::from(limit));
            }
            assert!(engine.seal_counter.snapshot().by_reason[7] < u64::from(limit));
        }
    }
}
