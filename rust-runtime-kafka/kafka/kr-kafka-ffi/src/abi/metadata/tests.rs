use super::*;
use crate::abi::binding_test_hooks::{
    kr_test_abort, kr_test_metadata, kr_test_producer_create, kr_test_resume,
};
use std::time::{Duration, Instant};

struct Fixture(*mut KrProducer);
impl Fixture {
    fn new() -> Self {
        let mut out = std::ptr::null_mut();
        // SAFETY: independent live output for the constructor.
        assert_eq!(unsafe { kr_test_producer_create(&mut out) }, KR_OK);
        Self(out)
    }
    fn topic(&self) -> u32 {
        let mut topic = 0;
        assert_eq!(
            // SAFETY: fixture owns the live producer and independent output/name.
            unsafe { kr_topic_open(self.0, b"metadata".as_ptr(), 8, &mut topic) },
            KR_OK
        );
        topic
    }
    fn status(&self, topic: u32) -> KrTopicStatus {
        let mut status = KrTopicStatus {
            struct_size: size_of::<KrTopicStatus>() as u32,
            ..Default::default()
        };
        assert_eq!(
            // SAFETY: initialized versioned output and live producer.
            unsafe { kr_topic_get_status(self.0, topic, &mut status) },
            KR_OK
        );
        status
    }
    fn update(&self, topic: u32, count: u32, id: u32, generation: u64) {
        // SAFETY: test fixture owns this test-artifact producer.
        assert_eq!(unsafe { kr_test_metadata(self.0, topic, count, id) }, KR_OK);
        // SAFETY: first call resumes the paused owner; subsequent calls are no-ops.
        assert_eq!(unsafe { kr_test_resume(self.0) }, KR_OK);
        wait(|| {
            let status = self.status(topic);
            // SAFETY: fixture retains producer; status is a read-only client query.
            let owner = unsafe { (&*self.0).client.owner_status() };
            assert_eq!(
                owner, 0,
                "owner stopped waiting metadata topic={topic} generation={generation} status={status:?}"
            );
            status.status == 1 && status.generation == generation
        });
    }
    fn acquire(&self, topic: u32) -> KrMetadataSnapshot {
        let mut out = KrMetadataSnapshot {
            struct_size: size_of::<KrMetadataSnapshot>() as u32,
            ..Default::default()
        };
        assert_eq!(
            // SAFETY: independent initialized output and live producer.
            unsafe { kr_metadata_acquire(self.0, topic, &mut out) },
            KR_OK
        );
        out
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        // SAFETY: fixture exclusively owns the producer; no calls/spans survive.
        unsafe { kr_destroy(self.0) };
    }
}
fn wait(mut condition: impl FnMut() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(10);
    while !condition() {
        assert!(
            Instant::now() < deadline,
            "owner did not reach observable condition"
        );
        std::thread::yield_now();
    }
}
#[test]
fn snapshot_pages_are_immutable_across_refresh_retirement_and_recreated_uuid() {
    let fixture = Fixture::new();
    let topic = fixture.topic();
    assert_eq!(fixture.status(topic).status, 0);
    fixture.update(topic, 2, 7, 1);
    let old = fixture.acquire(topic);
    assert_eq!(
        (old.generation, old.partition_count, old.broker_count),
        (1, 2, 3)
    );
    assert_eq!(old.topic_id, [7; 16]);
    // SAFETY: all ABI outputs below are initialized, writable and independent.
    unsafe {
        let mut brokers = [KrMetadataBroker {
            struct_size: size_of::<KrMetadataBroker>() as u32,
            ..Default::default()
        }; 2];
        let mut written = 99;
        assert_eq!(
            kr_metadata_brokers(
                fixture.0,
                old.snapshot,
                1,
                brokers.as_mut_ptr(),
                2,
                &mut written
            ),
            KR_OK
        );
        assert_eq!(written, 2);
        assert_eq!((brokers[0].id, brokers[1].id), (1, 2));
        assert_eq!(
            (
                brokers[0].port,
                brokers[0].host_len,
                brokers[0].rack_len,
                brokers[0].rack_present
            ),
            (9092, 8, 6, 1)
        );
        let mut bytes = [0; 4];
        assert_eq!(
            kr_metadata_string(
                fixture.0,
                old.snapshot,
                1,
                0,
                4,
                bytes.as_mut_ptr(),
                4,
                &mut written
            ),
            KR_OK
        );
        assert_eq!(&bytes, b"er-1");
        assert_eq!(
            kr_metadata_string(
                fixture.0,
                old.snapshot,
                2,
                1,
                2,
                bytes.as_mut_ptr(),
                4,
                &mut written
            ),
            KR_OK
        );
        assert_eq!(&bytes, b"ck-2");
        let mut row = KrMetadataPartition {
            struct_size: size_of::<KrMetadataPartition>() as u32,
            ..Default::default()
        };
        assert_eq!(
            kr_metadata_partitions(fixture.0, old.snapshot, 1, &mut row, 1, &mut written),
            KR_OK
        );
        assert_eq!(
            (
                row.partition,
                row.leader,
                row.replica_count,
                row.isr_count,
                row.offline_count
            ),
            (1, 1, 3, 2, 1)
        );
        let mut nodes = [-1; 2];
        assert_eq!(
            kr_metadata_nodes(
                fixture.0,
                old.snapshot,
                1,
                0,
                1,
                nodes.as_mut_ptr(),
                2,
                &mut written
            ),
            KR_OK
        );
        assert_eq!(nodes, [1, 2]);
        assert_eq!(
            kr_metadata_nodes(
                fixture.0,
                old.snapshot,
                1,
                1,
                0,
                nodes.as_mut_ptr(),
                2,
                &mut written
            ),
            KR_OK
        );
        assert_eq!(nodes, [0, 1]);
        assert_eq!(
            kr_topic_refresh(fixture.0, topic),
            KR_OK,
            "topic={:?} owner={:?}",
            fixture.status(topic),
            (&*fixture.0).client.status()
        );
    }
    assert_eq!(fixture.status(topic).status, 6);
    fixture.update(topic, 4, 7, 2);
    let expanded = fixture.acquire(topic);
    assert_eq!((expanded.partition_count, expanded.generation), (4, 2));
    // SAFETY: versioned output storage and live producer; old handles stay pinned.
    unsafe {
        let mut row = KrMetadataPartition {
            struct_size: size_of::<KrMetadataPartition>() as u32,
            ..Default::default()
        };
        let mut written = 99;
        assert_eq!(
            kr_metadata_partitions(fixture.0, old.snapshot, 2, &mut row, 1, &mut written),
            KR_OK
        );
        assert_eq!(written, 0);
        assert_eq!(kr_topic_close(fixture.0, topic), KR_OK);
    }
    wait(|| fixture.status(topic).status == 5);
    let replacement = fixture.topic();
    assert_ne!(replacement, topic);
    fixture.update(replacement, 1, 9, 1);
    let replaced = fixture.acquire(replacement);
    assert_eq!(replaced.topic_id, [9; 16]);
    assert_eq!(old.topic_id, [7; 16]);
    // SAFETY: old handles remain valid despite retirement, and release once.
    unsafe {
        for snapshot in [old.snapshot, expanded.snapshot, replaced.snapshot] {
            assert_eq!(kr_metadata_release(fixture.0, snapshot), KR_OK);
            assert_eq!(kr_metadata_release(fixture.0, snapshot), KR_ERR_INVALID);
        }
    }
}
#[test]
fn snapshot_capacity_stale_handles_pages_and_versions_fail_without_partial_writes() {
    let fixture = Fixture::new();
    let topic = fixture.topic();
    fixture.update(topic, 2, 7, 1);
    let snapshots: Vec<_> = (0..4).map(|_| fixture.acquire(topic).snapshot).collect();
    // SAFETY: every output is independently owned and initialized as documented.
    unsafe {
        let mut output = KrMetadataSnapshot {
            struct_size: size_of::<KrMetadataSnapshot>() as u32,
            snapshot: 99,
            ..Default::default()
        };
        assert_eq!(
            kr_metadata_acquire(fixture.0, topic, &mut output),
            KR_ERR_EXHAUSTED
        );
        assert_eq!(output.snapshot, 0);
        let mut written = 99;
        let mut rows = [KrMetadataBroker {
            struct_size: size_of::<KrMetadataBroker>() as u32,
            id: 99,
            ..Default::default()
        }; 2];
        rows[1].struct_size -= 1;
        assert_eq!(
            kr_metadata_brokers(
                fixture.0,
                snapshots[0],
                0,
                rows.as_mut_ptr(),
                2,
                &mut written
            ),
            KR_ERR_VERSION
        );
        assert_eq!(written, 0);
        assert_eq!(rows[0].id, 99);
        rows[1].struct_size += 1;
        for (start, capacity) in [(4, 1), (0, 1025)] {
            assert_eq!(
                kr_metadata_brokers(
                    fixture.0,
                    snapshots[0],
                    start,
                    rows.as_mut_ptr(),
                    capacity,
                    &mut written
                ),
                KR_ERR_INVALID
            );
            assert_eq!(written, 0);
        }
        assert_eq!(
            kr_metadata_brokers(
                fixture.0,
                snapshots[0],
                3,
                std::ptr::null_mut(),
                0,
                &mut written
            ),
            KR_OK
        );
        assert_eq!(
            kr_metadata_brokers(
                fixture.0,
                snapshots[0],
                0,
                std::ptr::null_mut(),
                1,
                &mut written
            ),
            KR_ERR_INVALID
        );
        let mut node = 99;
        assert_eq!(
            kr_metadata_nodes(fixture.0, snapshots[0], 2, 0, 0, &mut node, 1, &mut written),
            KR_ERR_INVALID
        );
        assert_eq!(
            kr_metadata_nodes(fixture.0, snapshots[0], 0, 3, 0, &mut node, 1, &mut written),
            KR_ERR_INVALID
        );
        assert_eq!(node, 99);
        assert_eq!(kr_metadata_release(fixture.0, snapshots[0]), KR_OK);
        let next = fixture.acquire(topic).snapshot;
        assert!(next > *snapshots.last().unwrap());
        assert_eq!(
            kr_metadata_brokers(
                fixture.0,
                snapshots[0],
                0,
                rows.as_mut_ptr(),
                1,
                &mut written
            ),
            KR_ERR_INVALID
        );
        let producer = &*fixture.0;
        producer.snapshots.lock().unwrap().next = u64::MAX;
        assert_eq!(kr_metadata_release(fixture.0, next), KR_OK);
        assert_eq!(
            kr_metadata_acquire(fixture.0, topic, &mut output),
            KR_ERR_EXHAUSTED
        );
    }
}
#[test]
fn aborted_status_means_terminal_publication_completed_without_closed_event() {
    let fixture = Fixture::new();
    let topic = fixture.topic();
    let record = KrRecord {
        struct_size: size_of::<KrRecord>() as u32,
        topic,
        partition_hint: -1,
        lane_hint: -1,
        key: KrSpan::default(),
        key_is_null: 1,
        value: KrSpan::default(),
        value_is_null: 0,
        headers: std::ptr::null(),
        header_count: 0,
        timestamp_ms: 0,
        user_token: 91,
        delivery_timeout_ns: 0,
    };
    // SAFETY: fixture/descriptor/output storage live throughout each call.
    unsafe {
        let mut status = 99;
        assert_eq!(kr_owner_status(fixture.0, &mut status), KR_OK);
        assert_eq!(status, 0);
        assert_eq!(kr_submitv_copy(fixture.0, &record, 1), 1);
        assert_eq!(kr_test_abort(fixture.0), KR_OK);
        wait(|| {
            assert_eq!(kr_owner_status(fixture.0, &mut status), KR_OK);
            status == 2
        });
        let mut events = [KrEvent::empty(); 16];
        let count = kr_poll_events(fixture.0, events.as_mut_ptr(), 16) as usize;
        let delivered: Vec<_> = events[..count]
            .iter()
            .filter(|event| event.kind == 1)
            .collect();
        assert_eq!(delivered.len(), 1);
        assert_eq!(delivered[0].user_token, 91);
        assert!(!events[..count].iter().any(|event| event.kind == 6));
        assert_eq!(kr_poll_events(fixture.0, events.as_mut_ptr(), 16), 0);
        assert_eq!(kr_owner_status(fixture.0, &mut status), KR_OK);
        assert_eq!(status, 2);
    }
}

#[test]
fn normally_closed_status_follows_closed_publication_and_preserves_pinned_snapshots() {
    let fixture = Fixture::new();
    let topic = fixture.topic();
    fixture.update(topic, 2, 7, 1);
    let snapshot = fixture.acquire(topic).snapshot;
    // SAFETY: fixture exclusively retains the producer and all initialized outputs.
    unsafe {
        assert_eq!(kr_close(fixture.0, 0), KR_OK);
        let mut state = 0;
        wait(|| {
            assert_eq!(kr_owner_status(fixture.0, &mut state), KR_OK);
            state == 1
        });
        let mut rows = [KrEvent::empty(); 16];
        let count = kr_poll_events(fixture.0, rows.as_mut_ptr(), 16) as usize;
        assert!(rows[..count].iter().any(|event| event.kind == 6));
        let mut broker = KrMetadataBroker {
            struct_size: size_of::<KrMetadataBroker>() as u32,
            ..Default::default()
        };
        let mut written = 99;
        assert_eq!(
            kr_metadata_brokers(fixture.0, snapshot, 0, &mut broker, 1, &mut written),
            KR_OK
        );
        assert_eq!((written, broker.id), (1, 0));
        assert_eq!(kr_metadata_release(fixture.0, snapshot), KR_OK);
    }
}
