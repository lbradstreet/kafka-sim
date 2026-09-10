//! Exercises retained routing work against actual admission, topic generation,
//! deadline settlement and actor destruction. The unused connector stays cold.
use super::*;
use crate::{
    config::ProducerConfig,
    connector::Connected,
    control::{BrokerNode, MetadataPartition, MetadataTopic, MetadataUpdate},
    routing::{
        NativePartitioner, PartitionChoice, PartitionerConfig, RecordMetadata, RoutingError,
        RoutingLease, RoutingSnapshot, SnapshotCollection, TopicSnapshot,
    },
};
use kr_runtime::SimRuntime;
use kr_runtime_io::network::MemoryStream;
use std::sync::atomic::{AtomicUsize, Ordering};

struct ColdConnector;
impl Connector for ColdConnector {
    type Stream = MemoryStream;
    type ConnectFuture = std::future::Pending<Result<Connected<MemoryStream>, ConnectError>>;
    fn connect(&mut self, _: ConnectTarget) -> Self::ConnectFuture {
        std::future::pending()
    }
}
#[derive(Default)]
struct Policy {
    runs: AtomicUsize,
    callbacks: Mutex<Vec<(SnapshotCollection, u32, usize)>>,
    run_quota: Option<u32>,
}
impl NativePartitioner for Policy {
    fn choose_partitions(
        &self,
        _: &[RecordMetadata],
        _: RoutingSnapshot<'_>,
        _: &mut [PartitionChoice],
    ) -> Result<(), RoutingError> {
        panic!("timing-aware override was bypassed")
    }
    fn choose_partitions_collected(
        &self,
        records: &[RecordMetadata],
        snapshot: RoutingSnapshot<'_>,
        interval: SnapshotCollection,
        choices: &mut [PartitionChoice],
    ) -> Result<(), RoutingError> {
        assert_eq!(snapshot.now, interval.completed_at);
        assert!(interval.started_at <= interval.completed_at);
        self.callbacks.lock().unwrap().push((
            interval,
            snapshot.topics[0].generation,
            records.len(),
        ));
        choices.fill(PartitionChoice::Partition(0));
        Ok(())
    }
    fn choose_run_collected(
        &self,
        topic: TopicSnapshot<'_>,
        _: RoutingSnapshot<'_>,
        _: SnapshotCollection,
    ) -> Result<Option<RoutingLease>, RoutingError> {
        self.runs.fetch_add(1, Ordering::Relaxed);
        Ok(self.run_quota.map(|byte_quota| RoutingLease {
            topic: topic.id,
            partition: 0,
            byte_quota,
        }))
    }
}
struct Harness {
    actor: ProducerActor<ColdConnector>,
    client: ProducerClient,
    _runtime: SimRuntime,
}
impl Harness {
    fn new(budget: u32, policy: Arc<Policy>) -> Self {
        Self::with_partitioner(
            budget,
            PartitionerConfig::Native(policy),
            crate::routing::UnkeyedPolicy::default(),
        )
    }
    fn with_partitioner(
        budget: u32,
        partitioner: PartitionerConfig,
        unkeyed_policy: crate::routing::UnkeyedPolicy,
    ) -> Self {
        Self::with_admission(
            budget,
            partitioner,
            unkeyed_policy,
            crate::config::DescriptorAdmissionPolicy::Shared,
        )
    }
    fn with_admission(
        budget: u32,
        partitioner: PartitionerConfig,
        unkeyed_policy: crate::routing::UnkeyedPolicy,
        descriptor_admission_policy: crate::config::DescriptorAdmissionPolicy,
    ) -> Self {
        let runtime = SimRuntime::new(Default::default());
        let config = ProducerConfig {
            max_completions_per_poll: budget,
            max_submissions_per_poll: 1,
            max_batches: 512,
            max_open_topics: 4,
            brokers_max: 1,
            record_descriptors: 64,
            delivery_event_capacity: 64,
            pending_records_per_topic: 1,
            max_live_leases: 4,
            release_event_capacity: 4,
            codec_contexts: 1,
            partitioner,
            unkeyed_policy,
            descriptor_admission_policy,
            ..Default::default()
        };
        let engine = ProducerEngine::new(config, None).unwrap();
        let (client, actor) = ProducerActor::new(
            RuntimeHandle::Sim(runtime.handle()),
            engine,
            ColdConnector,
            ClientClock::Simulation,
            ActorConfig::default(),
        )
        .unwrap();
        let actual_scratch = actor.routing.metadata_capacity_bytes()
            + actor.routing_ingress.capacity() * size_of::<crate::admission::AdmittedRecord>()
            + actor.routing_pending.capacity() * size_of::<RecordToken>();
        let reported = actor.engine.config().validate().unwrap().memory;
        assert_eq!(actual_scratch, reported.fixed_metadata.routing_scratch);
        Self {
            actor,
            client,
            _runtime: runtime,
        }
    }
    fn ingress(&mut self, nanos: u64) -> bool {
        self.actor
            .ingress(
                &mut Context::from_waker(Waker::noop()),
                RuntimeInstant::from_nanos(nanos),
            )
            .unwrap()
    }
    fn open(&mut self) -> TopicHandle {
        let topic = self
            .client
            .open_topic_at("topic", RuntimeInstant::ZERO)
            .unwrap();
        self.ingress(0);
        topic
    }
    fn begin_resolve(&mut self, topic: TopicHandle, epoch: i32) {
        self.actor
            .apply_control(
                ControlEvent::Metadata {
                    handles: vec![topic],
                    update: MetadataUpdate {
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
                            id: TopicId([9; 16]),
                            name: Some("topic".into()),
                            error_code: 0,
                            partitions: (0..257)
                                .map(|index| MetadataPartition {
                                    replicas: Vec::new(),
                                    isr: Vec::new(),
                                    offline: Vec::new(),
                                    index,
                                    error_code: 0,
                                    metadata: crate::topic::PartitionMetadata {
                                        leader: 0,
                                        leader_epoch: epoch,
                                    },
                                })
                                .collect(),
                        }],
                    },
                },
                RuntimeInstant::ZERO,
            )
            .unwrap();
    }
    fn resolve(&mut self, topic: TopicHandle, epoch: i32) {
        self.begin_resolve(topic, epoch);
        // This fixture needs a published topology before it isolates routing.
        while self.actor.engine.has_metadata_work() {
            self.actor.engine.on_deadline(
                RuntimeInstant::ZERO,
                WorkBudget {
                    bytes: 4096,
                    items: 1,
                },
            );
            self.actor.metadata_notices(1);
        }
        assert!(self.actor.engine.take_metadata_error().is_none());
    }
    fn submit(&self, topic: TopicHandle, count: usize, deadline: Option<u64>) {
        let records: Vec<_> = (0..count)
            .map(|index| RecordDescriptor {
                topic,
                partition_hint: None,
                lane_hint: None,
                key: None,
                value: Some(b"payload"),
                headers: &[],
                timestamp_ms: 0,
                user_token: index as u64,
                delivery_timeout: deadline.map(RuntimeDuration::from_nanos),
            })
            .collect();
        assert_eq!(
            self.client
                .submit_copy_at(RuntimeInstant::ZERO, &records)
                .accepted as usize,
            count
        );
    }
}

#[test]
fn pressure_routing_keeps_admission_identity_through_refresh_and_actor_abort() {
    let mut h = Harness::with_admission(
        1,
        PartitionerConfig::Builtin,
        crate::routing::UnkeyedPolicy::default(),
        crate::config::DescriptorAdmissionPolicy::PartitionPressure,
    );
    let topic = h.open();
    h.resolve(topic, 0);
    let descriptor = RecordDescriptor {
        topic,
        partition_hint: None,
        lane_hint: None,
        key: Some(b"key"),
        value: Some(b"payload"),
        headers: &[],
        timestamp_ms: 0,
        user_token: 0,
        delivery_timeout: None,
    };
    let submitted = h
        .client
        .submit_copy_at(RuntimeInstant::ZERO, &[descriptor; 7]);
    assert_eq!(submitted.accepted, 7);
    h.ingress(1);
    let partition = ((crate::routing::murmur2(b"key") & 0x7fff_ffff) % 257) as i32;
    assert_eq!(
        h.actor.pending_submission.as_ref().unwrap().as_slice()[0].partition_hint,
        Some(partition)
    );
    h.resolve(topic, 1);
    for now in 2..1024 {
        h.ingress(now);
        if h.actor.engine.status().accepted == 7 {
            break;
        }
    }
    assert_eq!(h.actor.engine.status().accepted, 7);
    let client = h.client.clone();
    drop(h);
    let mut events = [Event::Closed { unresolved: 0 }; 64];
    let count = client.poll_events(&mut events);
    let deliveries: Vec<_> = events[..count]
        .iter()
        .filter_map(|event| match event {
            Event::Delivery(d) => Some(d),
            _ => None,
        })
        .collect();
    assert_eq!(deliveries.len(), 7);
    assert!(
        deliveries
            .iter()
            .all(|d| d.partition.partition == partition)
    );
    assert!(client.credits().is_empty());
}

#[test]
fn ready_bulk_retains_preallocated_admission_storage_through_tiny_budget_invalidation() {
    for budget in [1, 7, 128] {
        let policy = Arc::new(Policy::default());
        let mut h = Harness::new(budget, policy.clone());
        let topic = h.open();
        h.resolve(topic, 0);
        let count = (budget as usize).min(7);
        let storage = h.actor.routing_storage().fixed_capacity_bytes;
        let pointer = h.actor.routing_ingress.as_ptr();
        h.submit(topic, count, None);
        h.ingress(1);
        assert_eq!(h.actor.engine.status().accepted, 0);
        assert_eq!(h.actor.routing_ingress.len(), count);
        assert_eq!(h.actor.engine.pending_records().count(), 0);
        assert!(policy.callbacks.lock().unwrap().is_empty());
        h.resolve(topic, 1); // Invalidates the unpublished UUID/generation tuple.
        let mut now = 2;
        while h.actor.engine.status().accepted == 0 {
            h.ingress(now);
            now += 1;
            assert!(now < 600, "finite collection must finish");
            assert_eq!(h.actor.routing_storage().fixed_capacity_bytes, storage);
            assert_eq!(h.actor.routing_ingress.as_ptr(), pointer);
        }
        assert_eq!(h.actor.engine.status().accepted, count as u64);
        assert_eq!(
            h.actor.engine.pending_records().count(),
            0,
            "Ready inputs cannot spend the per-topic Pending quota of one"
        );
        let calls = policy.callbacks.lock().unwrap();
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].1, 2);
        assert_eq!(calls[0].2, count);
        assert!(calls[0].0.started_at >= RuntimeInstant::from_nanos(2));
        assert_eq!(policy.runs.load(Ordering::Relaxed), 1);
        assert!(h.actor.routing_storage().callback_view_peak_bytes > 0);
        assert!(!h.ingress(now));
    }
}

#[test]
fn expired_pending_token_is_removed_during_collection_without_native_choice_consumption() {
    let policy = Arc::new(Policy::default());
    let mut h = Harness::new(1, policy.clone());
    let topic = h.open();
    h.submit(topic, 1, Some(20));
    h.ingress(0);
    assert_eq!(h.actor.engine.pending_records().count(), 1);
    h.resolve(topic, 0);
    assert!(
        h.actor
            .route_pending(RuntimeInstant::from_nanos(1))
            .unwrap()
    );
    assert_eq!(h.actor.routing_pending, [RecordToken(1)]);
    for _ in 0..4 {
        h.actor.engine.on_deadline(
            RuntimeInstant::from_nanos(20),
            WorkBudget {
                bytes: 4096,
                items: 1,
            },
        );
    }
    assert!(h.actor.engine.pending_record(RecordToken(1)).is_none());
    h.actor
        .route_pending(RuntimeInstant::from_nanos(20))
        .unwrap();
    assert!(h.actor.routing_pending.is_empty());
    assert_eq!(policy.runs.load(Ordering::Relaxed), 0);
    assert!(policy.callbacks.lock().unwrap().is_empty());
}

#[test]
fn cached_native_run_skips_collection_and_consumes_each_record_only_once() {
    let policy = Arc::new(Policy {
        run_quota: Some(1000),
        ..Default::default()
    });
    let mut h = Harness::new(7, policy.clone());
    let topic = h.open();
    h.resolve(topic, 0);
    h.submit(topic, 7, None);
    for now in 0..100 {
        h.ingress(now);
        if h.actor.engine.status().accepted == 7 {
            break;
        }
    }
    assert_eq!(h.actor.engine.status().accepted, 7);
    assert_eq!(policy.runs.load(Ordering::Relaxed), 1);
    h.submit(topic, 7, None);
    h.ingress(101);
    assert_eq!(
        h.actor.engine.status().accepted,
        14,
        "entire cached run bulk needs no snapshot poll"
    );
    assert_eq!(policy.runs.load(Ordering::Relaxed), 1);
    assert!(policy.callbacks.lock().unwrap().is_empty());
}

#[test]
fn actor_drop_settles_owned_routing_slice_before_submission_tail() {
    let policy = Arc::new(Policy::default());
    let mut h = Harness::new(1, policy.clone());
    let topic = h.open();
    h.resolve(topic, 0);
    h.submit(topic, 7, None);
    h.ingress(0);
    assert_eq!(h.actor.routing_ingress.len(), 1);
    assert_eq!(h.actor.pending_submission.as_ref().unwrap().len(), 6);
    assert_eq!(h.actor.engine.status().accepted, 0);
    let client = h.client.clone();
    drop(h);
    let mut events = [Event::Closed { unresolved: 0 }; 64];
    let count = client.poll_events(&mut events);
    let delivered: Vec<_> = events[..count]
        .iter()
        .filter_map(|event| match event {
            Event::Delivery(event) => Some((event.token, event.outcome)),
            _ => None,
        })
        .collect();
    assert_eq!(delivered.len(), 7);
    for (index, (token, outcome)) in delivered.into_iter().enumerate() {
        assert_eq!(token, RecordToken(index as u64 + 1));
        assert!(matches!(
            outcome,
            DeliveryOutcome {
                reason: FailureReason::RuntimeFailed,
                ..
            }
        ));
    }
    assert_eq!(policy.runs.load(Ordering::Relaxed), 0);
}

#[test]
fn adaptive_routing_replays_real_runtime_draws_and_never_draws_during_collection() {
    fn run() -> (Vec<u64>, kr_runtime::DeterminismCheckpoint) {
        let mut h = Harness::with_partitioner(
            1,
            PartitionerConfig::Builtin,
            crate::routing::UnkeyedPolicy::Adaptive { run_bytes: 1 },
        );
        let topic = h.open();
        h.resolve(topic, 0);
        h.submit(topic, 7, None);
        for now in 0..2000 {
            h.ingress(now);
            let accepted = h.actor.engine.status().accepted;
            let snapshot = h._runtime.snapshot();
            let rng = snapshot
                .random
                .iter()
                .find(|entry| entry.stream == kr_runtime::rng::RandomStream::Workload)
                .unwrap();
            assert_eq!(
                rng.checkpoint.draws(),
                accepted,
                "no draw before a completed decision"
            );
            if accepted == 7 {
                break;
            }
        }
        assert_eq!(h.actor.engine.status().accepted, 7);
        assert_eq!(h.actor.engine.pending_records().count(), 0);
        let distribution: Vec<_> = (0..257)
            .map(|partition| {
                h.actor
                    .engine
                    .partition_snapshot(
                        RuntimeInstant::from_nanos(2000),
                        TopicPartition {
                            topic: TopicId([9; 16]),
                            partition,
                        },
                    )
                    .queued_bytes
            })
            .collect();
        assert!(distribution.iter().filter(|&&bytes| bytes > 0).count() > 1);
        (distribution, h._runtime.snapshot().determinism_checkpoint())
    }
    assert_eq!(run(), run());
}

#[test]
fn stopped_control_and_actor_abort_release_retained_metadata_arena_after_discard() {
    use crate::credit::Resource;
    let mut h = Harness::new(1, Arc::new(Policy::default()));
    let topic = h.open();
    h.begin_resolve(topic, 0);
    let credits = h.actor.engine.credits();
    let retained = credits.snapshot()[Resource::ControlReserve as usize].held;
    assert!(retained > 0);
    h.actor.control.stop();
    let mut cx = Context::from_waker(Waker::noop());
    assert!(matches!(
        h.actor.control.poll_event(
            &mut cx,
            RuntimeInstant::ZERO,
            &mut h.actor.connector,
            h.actor.engine.topics()
        ),
        Poll::Ready(ControlEvent::Released)
    ));
    assert!(h.actor.control.working_guard().is_none());
    assert_eq!(
        credits.snapshot()[Resource::ControlReserve as usize].held,
        retained
    );
    assert_eq!(
        h.actor
            .engine
            .on_deadline(RuntimeInstant::ZERO, WorkBudget { bytes: 1, items: 1 })
            .items,
        1
    );
    assert!(h.actor.engine.has_metadata_work());
    drop(h.actor);
    assert_eq!(
        credits.snapshot()[Resource::ControlReserve as usize].held,
        0
    );
}
