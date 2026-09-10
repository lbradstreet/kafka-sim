use super::*;
use crate::{
    admission::{Admission, AdmissionError},
    types::{Header, RecordDescriptor, RecordToken},
};
use kr_runtime::RuntimeInstant;

fn fixture(bytes: usize, leases: u32) -> (InputLeases, SharedCredits, Admission) {
    let config = ProducerConfig {
        max_live_leases: leases,
        release_event_capacity: leases,
        ..ProducerConfig::default()
    };
    let mut limits = config.validate().unwrap().credits;
    limits[Resource::InputBytes as usize] = bytes;
    let credits = SharedCredits::new(limits, config.lanes).unwrap();
    let input = InputLeases::new(&config, credits.clone()).unwrap();
    (
        input,
        credits.clone(),
        Admission::new(&config, credits, 1024),
    )
}

fn leased(value: Option<Range<u32>>) -> LeasedRecordDescriptor<'static> {
    LeasedRecordDescriptor {
        topic: TopicHandle(1),
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

#[test]
fn shared_registration_retains_full_capacity_until_last_view_and_rejects_prior_owners() {
    let (input, credits, _) = fixture(128, 2);
    let shared = SharedBytes::from(vec![1, 2, 3]);
    assert_eq!(
        input.register_shared(shared.clone(), 0),
        Err(InputError::InvalidOwner)
    );
    assert!(credits.is_empty());
    let mut storage = Vec::with_capacity(128);
    storage.extend_from_slice(b"abcd");
    let pointer = storage.as_ptr();
    let lease = input
        .register_shared(SharedBytes::from(storage), 0)
        .unwrap();
    let view = input.view(lease, 1..3).unwrap();
    assert_eq!(view.as_ptr(), pointer.wrapping_add(1));
    assert_eq!(view.as_slice(), b"bc");
    assert_eq!(credits.snapshot()[Resource::InputBytes as usize].held, 128);
    input.release(lease).unwrap();
    assert!(input.pop_released().is_none());
    assert!(input.view(lease, 0..1).is_err());
    drop(view);
    let event = input.pop_released().unwrap();
    assert_eq!(event.event, Event::InputReleased { lease });
    assert_eq!(credits.snapshot()[Resource::InputBytes as usize].held, 0);
    assert_eq!(credits.snapshot()[Resource::ReleaseEvents as usize].held, 1);
    drop(event);
    assert!(credits.is_empty());
}

#[test]
fn native_commit_preserves_pointer_and_charges_the_entire_allocation_once() {
    let (input, credits, _) = fixture(64, 2);
    let mut buffer = input.acquire(64, 0).unwrap();
    let original = buffer.as_mut_slice().as_ptr();
    buffer.as_mut_slice()[..4].copy_from_slice(b"data");
    let lease = buffer.commit(4).unwrap();
    let view = input.view(lease, 0..4).unwrap();
    assert_eq!(view.as_ptr(), original);
    assert_eq!(view.as_slice(), b"data");
    assert_eq!(view.allocation_len(), 64);
    assert_eq!(credits.snapshot()[Resource::InputBytes as usize].held, 64);
    assert_eq!(input.status().allocation_bytes_by_lane, [64, 0, 0, 0]);
    assert!(input.view(lease, 4..5).is_err());
    input.release(lease).unwrap();
    assert!(input.pop_released().is_none());
    assert!(input.view(lease, 0..1).is_err());
    drop(view);
    assert_eq!(credits.snapshot()[Resource::InputBytes as usize].held, 0);
    assert_eq!(input.status().pending_release_events, 1);
    let event = input.pop_released().unwrap();
    assert_eq!(event.event, Event::InputReleased { lease });
    assert_eq!(input.status().live, 0);
    assert_eq!(credits.snapshot()[Resource::ReleaseEvents as usize].held, 1);
    assert!(input.pop_released().is_none());
    drop(event);
    assert!(credits.is_empty());
}

#[test]
fn bad_commit_returns_the_same_mutable_buffer_and_stale_ids_never_alias_reuse() {
    let (input, credits, _) = fixture(64, 1);
    let mut buffer = input.acquire(8, 0).unwrap();
    let first = buffer.lease_id();
    let pointer = buffer.as_mut_slice().as_ptr();
    let failure = buffer.commit(9).unwrap_err();
    assert_eq!(
        failure.error,
        InputError::InvalidUsed {
            used: 9,
            capacity: 8
        }
    );
    let mut buffer = failure.buffer;
    assert_eq!(buffer.as_mut_slice().as_ptr(), pointer);
    input.release(first).unwrap();
    let failure = buffer.commit(0).unwrap_err();
    assert_eq!(failure.error, InputError::StaleLease { lease: first });
    assert!(
        input.acquire(1, 0).is_err(),
        "live allocation still occupies its slot"
    );
    drop(failure);
    assert!(
        input.acquire(1, 0).is_err(),
        "pending release notification retains its generation"
    );
    drop(input.pop_released().unwrap());
    let second = input.acquire(8, 0).unwrap();
    assert_ne!(first, second.lease_id());
    assert_eq!(first.0 as u32, second.lease_id().0 as u32);
    let before = credits.snapshot();
    assert_eq!(
        input.release(first),
        Err(InputError::StaleLease { lease: first })
    );
    assert_eq!(credits.snapshot(), before);
    drop(second);
    drop(input.pop_released().unwrap());
    assert!(credits.is_empty());
}

#[test]
fn prefix_admission_retains_input_through_last_actual_encoder_owner() {
    let (input, credits, mut admission) = fixture(64, 1);
    let mut buffer = input.acquire(64, 0).unwrap();
    buffer.as_mut_slice()[..4].copy_from_slice(b"data");
    let lease = buffer.commit(4).unwrap();
    let (submitted, batch) = admission.prepare_leased(
        RuntimeInstant::ZERO,
        &input,
        lease,
        &[leased(Some(0..4)), leased(Some(4..5))],
        &[Ok(0), Ok(0)],
    );
    assert_eq!(submitted.accepted, 1);
    assert_eq!(submitted.error, Some(AdmissionError::InvalidLease));
    assert_eq!(admission.last_token(), RecordToken(1));
    assert_eq!(admission.copied_bytes(), 0);
    assert_eq!(credits.snapshot()[Resource::InputBytes as usize].held, 64);
    input.release(lease).unwrap();
    let record = batch.unwrap().drain().pop().unwrap();
    let (payload, mut obligation) = record.into_parts();
    obligation.input_consumed();
    assert!(
        input.pop_released().is_none(),
        "record bytes still retained by an encoder owner"
    );
    let provider = payload.value.as_ref().unwrap().clone();
    drop(payload);
    assert!(
        input.pop_released().is_none(),
        "provider clone still retains physical input"
    );
    drop(provider);
    let release = input.pop_released().unwrap();
    assert_eq!(release.event, Event::InputReleased { lease });
    assert_eq!(credits.snapshot()[Resource::InputBytes as usize].held, 0);
    assert_eq!(
        credits.snapshot()[Resource::DeliveryEvents as usize].held,
        1
    );
    let delivery = obligation.terminal();
    drop(release);
    drop(delivery);
    assert!(credits.is_empty());
}

#[test]
fn all_null_record_and_future_submissions_both_delay_release() {
    let (input, credits, mut admission) = fixture(8, 1);
    let lease = input.acquire(8, 0).unwrap().commit(0).unwrap();
    let (submitted, batch) = admission.prepare_leased(
        RuntimeInstant::ZERO,
        &input,
        lease,
        &[leased(None), leased(Some(0..0))],
        &[Ok(0), Ok(0)],
    );
    assert_eq!(submitted.accepted, 2);
    let mut records = batch.unwrap().drain();
    let (empty, mut empty_obligation) = records.pop().unwrap().into_parts();
    let (null, mut null_obligation) = records.pop().unwrap().into_parts();
    assert!(null.value.is_none());
    assert!(empty.value.as_ref().unwrap().is_empty());
    drop(null);
    drop(empty);
    empty_obligation.input_consumed();
    null_obligation.input_consumed();
    assert!(
        input.pop_released().is_none(),
        "committed lease permits further submissions"
    );
    let (submitted, final_batch) = admission.prepare_leased(
        RuntimeInstant::ZERO,
        &input,
        lease,
        &[leased(None)],
        &[Ok(0)],
    );
    assert_eq!(submitted.accepted, 1);
    input.release(lease).unwrap();
    assert!(
        input.pop_released().is_none(),
        "all-null descriptor is not yet consumed"
    );
    let (_, mut final_obligation) = final_batch.unwrap().drain().pop().unwrap().into_parts();
    final_obligation.input_consumed();
    drop(input.pop_released().unwrap());
    assert!(input.pop_released().is_none());
    drop(final_obligation.terminal());
    drop(null_obligation.terminal());
    drop(empty_obligation.terminal());
    assert!(credits.is_empty());
}

#[test]
fn close_waits_for_uncommitted_caller_buffers_and_releases_each_lease_once() {
    let (input, credits, _) = fixture(64, 2);
    let buffer = input.acquire(8, 0).unwrap();
    let first = buffer.lease_id();
    let second = input.acquire(16, 0).unwrap().commit(5).unwrap();
    input.close();
    input.close();
    assert_eq!(input.close_step(2).visited_slots, 2);
    assert!(!input.has_close_work());
    assert!(matches!(input.acquire(1, 0), Err(InputError::Closed)));
    assert_eq!(input.status().acquired, 1);
    assert_eq!(
        input.pop_released().unwrap().event,
        Event::InputReleased { lease: second }
    );
    assert_eq!(input.status().live, 1);
    let failure = buffer.commit(0).unwrap_err();
    assert_eq!(failure.error, InputError::Closed);
    drop(failure);
    assert_eq!(
        input.pop_released().unwrap().event,
        Event::InputReleased { lease: first }
    );
    assert_eq!(input.status().live, 0);
    assert!(input.pop_released().is_none());
    assert!(credits.is_empty());
}

#[test]
fn empty_header_metadata_is_charged_on_copy_and_native_paths() {
    let metadata = std::mem::size_of::<kr_kafka_record::OwnedHeader>();
    let scratch = std::mem::size_of::<kr_kafka_record::Header<'_>>();
    let (input, credits, mut admission) = fixture(metadata + scratch, 1);
    let header = [Header {
        key: "",
        value: Some(&[]),
    }];
    let record = RecordDescriptor {
        topic: TopicHandle(1),
        partition_hint: None,
        lane_hint: None,
        key: None,
        value: None,
        headers: &header,
        timestamp_ms: 0,
        user_token: 0,
        delivery_timeout: None,
    };
    let (submitted, batch) = admission.prepare_copy(RuntimeInstant::ZERO, &[record], &[Ok(0)]);
    assert_eq!(submitted.accepted, 1);
    assert_eq!(
        credits.snapshot()[Resource::InputBytes as usize].held,
        metadata
    );
    let (submitted, rejected) = admission.prepare_copy(RuntimeInstant::ZERO, &[record], &[Ok(0)]);
    assert_eq!(submitted.accepted, 0);
    assert!(rejected.is_none());
    assert_eq!(admission.copied_bytes(), 0);
    drop(batch);
    let lease = input.acquire(1, 0).unwrap().commit(0).unwrap();
    let header = [LeasedHeader {
        key: 0..0,
        value: Some(0..0),
    }];
    let record = LeasedRecordDescriptor {
        headers: &header,
        ..leased(None)
    };
    let (submitted, batch) =
        admission.prepare_leased(RuntimeInstant::ZERO, &input, lease, &[record], &[Ok(0)]);
    assert_eq!(submitted.accepted, 1);
    assert_eq!(
        credits.snapshot()[Resource::InputBytes as usize].held,
        metadata + 1
    );
    input.release(lease).unwrap();
    drop(batch);
    drop(input.pop_released().unwrap());
    assert!(credits.is_empty());
}

#[test]
fn native_allocation_fairness_retains_the_acquiring_lane() {
    let config = ProducerConfig {
        lanes: 2,
        max_live_leases: 1,
        release_event_capacity: 1,
        ..ProducerConfig::default()
    };
    let credits = SharedCredits::new(config.validate().unwrap().credits, 2).unwrap();
    let input = InputLeases::new(&config, credits.clone()).unwrap();
    let mut admission = Admission::new(&config, credits.clone(), 1024);
    let lease = input.acquire(16, 0).unwrap().commit(4).unwrap();
    let (_, batch) = admission.prepare_leased(
        RuntimeInstant::ZERO,
        &input,
        lease,
        &[leased(Some(0..4))],
        &[Ok(0)],
    );
    let mut record = batch.unwrap().drain().pop().unwrap();
    record.set_lane(1).unwrap();
    assert_eq!(record.lane, 1);
    assert_eq!(
        credits.snapshot()[Resource::Descriptors as usize].lane_held,
        [0, 1, 0, 0]
    );
    assert_eq!(
        credits.snapshot()[Resource::InputBytes as usize].lane_held,
        [16, 0, 0, 0]
    );
    assert_eq!(input.status().allocation_bytes_by_lane, [16, 0, 0, 0]);
    input.release(lease).unwrap();
    drop(record);
    drop(input.pop_released().unwrap());
    assert!(credits.is_empty());
}

#[test]
fn release_event_capacity_stays_reserved_until_application_drain() {
    let (input, credits, _) = fixture(16, 1);
    let buffer = input.acquire(8, 0).unwrap();
    drop(buffer);
    let event = input.pop_released().unwrap();
    assert_eq!(input.status().live, 0);
    let before = credits.snapshot();
    assert!(matches!(
        input.acquire(1, 0),
        Err(InputError::Credit(CreditError::ResourceExhausted {
            resource: "release_events",
            limit: 1
        }))
    ));
    assert_eq!(credits.snapshot(), before);
    drop(event);
    let buffer = input.acquire(8, 0).unwrap();
    drop(buffer);
    drop(input.pop_released().unwrap());
    assert!(credits.is_empty());
}

#[test]
fn actual_encoder_progress_releases_native_input_before_delivery_for_each_codec() {
    use crate::{
        accumulator::{Batch, BatchState},
        config::Compression,
        types::{SealReason, TopicId, TopicPartition, WorkBudget},
    };
    use kr_kafka_record::{CodecPool, OutputPool, ZstdConfig};
    for compression in [Compression::None, Compression::Zstd { level: 3 }] {
        for quantum in [1, 7, 127] {
            let config = ProducerConfig {
                compression,
                max_live_leases: 1,
                release_event_capacity: 1,
                ..ProducerConfig::default()
            };
            let validated = config.validate().unwrap();
            let credits = SharedCredits::new(validated.credits, config.lanes).unwrap();
            let input = InputLeases::new(&config, credits.clone()).unwrap();
            let mut admission = Admission::new(
                &config,
                credits.clone(),
                validated.effective_batch_payload_bytes,
            );
            let mut buffer = input.acquire(512, 0).unwrap();
            for (index, byte) in buffer.as_mut_slice().iter_mut().enumerate() {
                *byte = (index % 251) as u8;
            }
            let lease = buffer.commit(512).unwrap();
            let (_, submitted) = admission.prepare_leased(
                RuntimeInstant::ZERO,
                &input,
                lease,
                &[leased(Some(0..257)), leased(Some(257..512))],
                &[Ok(0), Ok(0)],
            );
            let output = OutputPool::new(config.compressed_bytes).unwrap();
            let level = match compression {
                Compression::None => 1,
                Compression::Zstd { level } => level,
            };
            let mut codecs = CodecPool::new(
                1,
                ZstdConfig {
                    level,
                    ..ZstdConfig::default()
                },
            )
            .unwrap();
            let mut batch = Batch::new(
                &config,
                validated.effective_batch_payload_bytes,
                TopicPartition {
                    topic: TopicId([1; 16]),
                    partition: 0,
                },
                0,
                output,
                credits.clone(),
            )
            .unwrap();
            for record in submitted.unwrap().drain() {
                batch.try_append(record, config.linger_max).unwrap();
            }
            input.release(lease).unwrap();
            batch.seal(SealReason::Flush);
            assert!(input.pop_released().is_none());
            for _ in 0..10_000 {
                let progress = batch.encode(
                    &mut codecs,
                    WorkBudget {
                        bytes: quantum,
                        items: 1,
                    },
                );
                assert!(progress.bytes <= quantum && progress.items <= 1);
                if batch.state() == BatchState::Sealed {
                    break;
                }
            }
            assert_eq!(
                batch.state(),
                BatchState::Sealed,
                "compression={compression:?} quantum={quantum} failure={:?}",
                batch.failure()
            );
            let released = input
                .pop_released()
                .expect("encoder consumed both source records");
            assert_eq!(released.event, Event::InputReleased { lease });
            assert_eq!(credits.snapshot()[Resource::InputBytes as usize].held, 0);
            assert_eq!(
                credits.snapshot()[Resource::DeliveryEvents as usize].held,
                2
            );
            let (records, payload) = batch.into_terminal();
            for record in records {
                drop(record.terminal());
            }
            drop(payload);
            drop(released);
            assert!(credits.is_empty());
        }
    }
}

#[test]
fn provider_thread_final_drop_only_enqueues_passive_release() {
    let (input, credits, _) = fixture(16, 1);
    let lease = input.acquire(16, 0).unwrap().commit(1).unwrap();
    let provider = input.view(lease, 0..1).unwrap();
    input.release(lease).unwrap();
    assert!(input.pop_released().is_none());
    std::thread::spawn(move || drop(provider)).join().unwrap();
    assert_eq!(input.status().pending_release_events, 1);
    drop(input.pop_released().unwrap());
    assert!(credits.is_empty());
}

#[test]
fn close_sweeps_sparse_slots_under_budget_then_parks_with_external_owners() {
    for quantum in [1, 2, 7, 64] {
        let (input, credits, _) = fixture(4096, 257);
        let mut callers = Vec::new();
        let mut providers = Vec::new();
        let mut holes = Vec::new();
        for index in 0..257 {
            let buffer = input.acquire(8, 0).unwrap();
            match index % 3 {
                0 => callers.push(buffer),
                1 => {
                    let lease = buffer.commit(8).unwrap();
                    providers.push(input.view(lease, 0..8).unwrap());
                }
                _ => holes.push(buffer.commit(8).unwrap()),
            }
        }
        for lease in holes {
            input.release(lease).unwrap();
            drop(input.pop_released().unwrap());
        }
        let before = input.status();
        input.close();
        assert_eq!(
            input.status(),
            InputStatus {
                closed: true,
                ..before
            }
        );
        assert_eq!(
            input.close_step(0),
            InputCloseProgress {
                remaining: true,
                ..Default::default()
            }
        );
        assert_eq!(
            input.status(),
            InputStatus {
                closed: true,
                ..before
            }
        );
        assert!(matches!(input.acquire(1, 0), Err(InputError::Closed)));
        let mut visited = 0;
        while input.has_close_work() {
            let progress = input.close_step(quantum);
            assert!((1..=quantum).contains(&progress.visited_slots));
            assert!(progress.released_leases <= progress.visited_slots);
            visited += progress.visited_slots;
            assert!(
                input.pop_released().is_none(),
                "external owners have not retired"
            );
        }
        assert_eq!(
            visited, 257,
            "sparse holes consume visits rather than hiding a scan"
        );
        assert_eq!(input.status().live, callers.len() + providers.len());
        assert_eq!(input.status().committed, 0);
        assert_eq!(input.close_step(quantum), InputCloseProgress::default());
        let expected = callers.len() + providers.len();
        drop(providers);
        drop(callers);
        let mut events = std::collections::BTreeSet::new();
        while let Some(event) = input.pop_released() {
            let Event::InputReleased { lease } = event.event else {
                unreachable!()
            };
            assert!(events.insert(lease));
        }
        assert_eq!(events.len(), expected);
        assert_eq!(input.status().live, 0);
        assert!(credits.is_empty());
    }
}

#[test]
fn provider_release_notification_sees_published_event_after_all_locks_release() {
    struct Inspect {
        state: std::sync::Weak<Mutex<State>>,
        released: std::sync::Weak<ReleaseQueue>,
        count: AtomicUsize,
    }
    impl std::task::Wake for Inspect {
        fn wake(self: Arc<Self>) {
            self.wake_by_ref();
        }
        fn wake_by_ref(self: &Arc<Self>) {
            let state = self.state.upgrade().unwrap();
            assert!(
                state.try_lock().is_ok(),
                "input state locked during scheduler notification"
            );
            let released = self.released.upgrade().unwrap();
            assert_eq!(released.queue.try_lock().unwrap().len(), 1);
            assert!(released.wake.try_lock().is_ok());
            assert_eq!(
                released.allocation_bytes_by_lane[0].load(Ordering::Acquire),
                0
            );
            self.count.fetch_add(1, Ordering::Relaxed);
        }
    }
    let (input, credits, _) = fixture(16, 1);
    let lease = input.acquire(16, 0).unwrap().commit(4).unwrap();
    let provider = input.view(lease, 0..4).unwrap();
    input.close();
    input.close_step(1);
    assert!(!input.has_close_work());
    let notify = Arc::new(Inspect {
        state: Arc::downgrade(&input.state),
        released: Arc::downgrade(&input.released),
        count: AtomicUsize::new(0),
    });
    let waker = Waker::from(notify.clone());
    input.register_release_waker(&waker);
    input.register_release_waker(&waker);
    assert_eq!(notify.count.load(Ordering::Relaxed), 0);
    drop(provider);
    assert_eq!(notify.count.load(Ordering::Relaxed), 1);
    assert!(!input.has_close_work());
    drop(input.pop_released().unwrap());
    assert!(credits.is_empty());
    input.register_release_waker(&waker);
    input.clear_release_waker();
    input.clear_release_waker();
    assert_eq!(
        Arc::strong_count(&notify),
        2,
        "registry no longer retains scheduler"
    );
}

#[test]
fn seeded_lease_status_matches_independent_owner_and_release_accounting() {
    struct Lease {
        id: LeaseId,
        buffer: Option<InputBuffer>,
        provider: Option<SharedBytes>,
        registry: bool,
        released: bool,
        capacity: usize,
    }
    fn verify(
        input: &InputLeases,
        credits: &SharedCredits,
        leases: &[Option<Lease>],
        seed: u64,
        step: usize,
    ) {
        let live: Vec<_> = leases.iter().flatten().collect();
        let allocation: usize = live
            .iter()
            .filter(|l| l.buffer.is_some() || l.registry || l.provider.is_some())
            .map(|l| l.capacity)
            .sum();
        let status = input.status();
        assert_eq!(status.live, live.len(), "seed={seed} step={step}");
        assert_eq!(
            status.acquired,
            live.iter().filter(|l| l.buffer.is_some()).count(),
            "seed={seed} step={step}"
        );
        assert_eq!(
            status.committed,
            live.iter().filter(|l| l.registry).count(),
            "seed={seed} step={step}"
        );
        assert_eq!(
            status.released_waiting,
            live.iter().filter(|l| l.released).count(),
            "seed={seed} step={step}"
        );
        assert_eq!(
            status.pending_release_events,
            live.iter()
                .filter(|l| l.buffer.is_none() && !l.registry && l.provider.is_none())
                .count(),
            "seed={seed} step={step}"
        );
        assert_eq!(
            status.allocation_bytes_by_lane,
            [allocation, 0, 0, 0],
            "seed={seed} step={step}"
        );
        assert_eq!(
            credits.snapshot()[Resource::InputBytes as usize].held,
            allocation,
            "seed={seed} step={step}"
        );
    }
    for seed in 1..65u64 {
        let (input, credits, _) = fixture(1024, 17);
        let mut random = seed;
        let mut leases: Vec<Option<Lease>> = (0..17).map(|_| None).collect();
        for step in 0..512 {
            random ^= random << 13;
            random ^= random >> 7;
            random ^= random << 17;
            let index = (random as usize / 7) % leases.len();
            match random % 6 {
                0 if leases[index].is_none() => {
                    let capacity = 1 + (random >> 32) as usize % 32;
                    let (id, buffer, registry) = if random & 64 == 0 {
                        let buffer = input.acquire(capacity as u32, 0).unwrap();
                        (buffer.lease_id(), Some(buffer), false)
                    } else {
                        (
                            input
                                .register_shared(SharedBytes::from(vec![0; capacity]), 0)
                                .unwrap(),
                            None,
                            true,
                        )
                    };
                    leases[index] = Some(Lease {
                        id,
                        buffer,
                        provider: None,
                        registry,
                        released: false,
                        capacity,
                    });
                }
                1 => {
                    if let Some(lease) = &mut leases[index]
                        && !lease.released
                        && let Some(buffer) = lease.buffer.take()
                    {
                        assert_eq!(buffer.commit(lease.capacity as u32).unwrap(), lease.id);
                        lease.registry = true;
                    }
                }
                2 => {
                    if let Some(lease) = &mut leases[index]
                        && !lease.released
                    {
                        input.release(lease.id).unwrap();
                        lease.registry = false;
                        lease.released = true;
                    }
                }
                3 => {
                    if let Some(lease) = &mut leases[index] {
                        if lease.buffer.take().is_some() {
                            lease.released = true;
                        } else {
                            lease.provider = None;
                        }
                    }
                }
                4 => {
                    if let Some(event) = input.pop_released() {
                        let Event::InputReleased { lease: id } = event.event else {
                            unreachable!()
                        };
                        let index = leases
                            .iter()
                            .position(|lease| lease.as_ref().is_some_and(|l| l.id == id))
                            .unwrap();
                        let lease = leases[index].take().unwrap();
                        assert!(
                            lease.released
                                && lease.buffer.is_none()
                                && !lease.registry
                                && lease.provider.is_none()
                        );
                    }
                }
                5 => {
                    if let Some(lease) = &mut leases[index]
                        && lease.registry
                    {
                        lease.provider =
                            Some(input.view(lease.id, 0..lease.capacity as u32).unwrap());
                    }
                }
                _ => {}
            }
            verify(&input, &credits, &leases, seed, step);
        }
        input.close();
        while input.has_close_work() {
            assert!(input.close_step(3).visited_slots <= 3);
        }
        for lease in leases.iter_mut().flatten() {
            lease.registry = false;
            lease.released = true;
        }
        verify(&input, &credits, &leases, seed, 512);
        drop(leases);
        let mut released = 0;
        while input.pop_released().is_some() {
            released += 1;
            assert!(released <= 17);
        }
        assert_eq!(input.status().live, 0);
        assert!(credits.is_empty(), "seed={seed}");
    }
}

#[test]
fn release_wake_deferrals_compose_and_re_registration_cannot_lose_publication() {
    struct Counter(AtomicUsize);
    impl std::task::Wake for Counter {
        fn wake(self: Arc<Self>) {
            self.0.fetch_add(1, Ordering::Relaxed);
        }
        fn wake_by_ref(self: &Arc<Self>) {
            self.0.fetch_add(1, Ordering::Relaxed);
        }
    }
    let (input, credits, _) = fixture(16, 1);
    let lease = input.acquire(16, 0).unwrap().commit(4).unwrap();
    let provider = input.view(lease, 0..4).unwrap();
    input.release(lease).unwrap();
    let first = Arc::new(Counter(AtomicUsize::new(0)));
    let second = Arc::new(Counter(AtomicUsize::new(0)));
    input.register_release_waker(&Waker::from(first.clone()));
    let outer = input.defer_release_wakes();
    let inner = input.defer_release_wakes();
    drop(provider);
    assert_eq!(input.status().pending_release_events, 1);
    drop(outer);
    assert_eq!(first.0.load(Ordering::Relaxed), 0);
    input.register_release_waker(&Waker::from(second.clone()));
    drop(inner);
    assert_eq!(first.0.load(Ordering::Relaxed), 0);
    assert_eq!(second.0.load(Ordering::Relaxed), 1);
    drop(input.pop_released().unwrap());
    assert!(credits.is_empty());
}

#[test]
fn hostile_release_waker_and_panic_payload_cannot_escape_final_reference_cleanup() {
    struct Payload;
    impl Drop for Payload {
        fn drop(&mut self) {
            panic!("panic payload destructor");
        }
    }
    struct Hostile;
    impl std::task::Wake for Hostile {
        fn wake(self: Arc<Self>) {
            std::panic::panic_any(Payload);
        }
        fn wake_by_ref(self: &Arc<Self>) {
            std::panic::panic_any(Payload);
        }
    }
    for deferred in [false, true] {
        let (input, credits, _) = fixture(16, 1);
        let lease = input.acquire(16, 0).unwrap().commit(4).unwrap();
        let provider = input.view(lease, 0..4).unwrap();
        input.release(lease).unwrap();
        input.register_release_waker(&Waker::from(Arc::new(Hostile)));
        let guard = deferred.then(|| input.defer_release_wakes());
        assert!(
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                drop(provider);
                drop(guard);
            }))
            .is_ok()
        );
        let event = input.pop_released().unwrap();
        assert_eq!(event.event, Event::InputReleased { lease });
        assert!(input.pop_released().is_none());
        drop(event);
        assert!(credits.is_empty());
    }
}
