use super::*;
use kr_kafka_producer::{
    actor::{ActorConfig, ProducerActor},
    client::ClientClock,
    config::Compression,
    connector::{ConnectError, ConnectTarget, Connected, Connector},
    engine::ProducerEngine,
};
use kr_runtime::{HostRuntime, RuntimeHandle};
use kr_runtime_io::network::MemoryStream;
use std::{
    future::{Ready, ready},
    sync::mpsc::{SyncSender, sync_channel},
    time::{Duration, Instant},
};
struct NoConnect;
impl Connector for NoConnect {
    type Stream = MemoryStream;
    type ConnectFuture = Ready<Result<Connected<MemoryStream>, ConnectError>>;
    fn connect(&mut self, _: ConnectTarget) -> Self::ConnectFuture {
        ready(Err(ConnectError::TransportUnavailable))
    }
}
struct Fixture {
    pointer: *mut KrProducer,
    start: Option<SyncSender<()>>,
}
impl Fixture {
    fn new() -> Self {
        Self::with_owner_abort(false)
    }
    fn with_owner_abort(abort: bool) -> Self {
        let config = ProducerConfig {
            compression: Compression::None,
            codec_contexts: 1,
            record_descriptors: 8,
            delivery_event_capacity: 8,
            release_event_capacity: 4,
            max_live_leases: 4,
            max_batches: 8,
            max_open_topics: 4,
            pending_records_per_topic: 8,
            mailbox_capacity: 1,
            max_submission_records: 8,
            input_bytes: 64 * 1024,
            ..Default::default()
        };
        let thread_config = config.clone();
        let (send, receive) = sync_channel(1);
        let (start, wait) = sync_channel(1);
        let owner = std::thread::spawn(move || {
            let mut runtime = HostRuntime::default();
            let engine = ProducerEngine::new(thread_config, None).unwrap();
            let (client, actor) = ProducerActor::new(
                RuntimeHandle::Host(runtime.handle()),
                engine,
                NoConnect,
                ClientClock::Host(runtime.control()),
                ActorConfig {
                    sim_encode_cost: RuntimeDuration::ZERO,
                    ..Default::default()
                },
            )
            .unwrap();
            send.send(client).unwrap();
            wait.recv().unwrap();
            if abort {
                drop(actor); // actual owner abort, with passive input guards alive
                drop(runtime);
                return;
            }
            let outcome = runtime.block_on(actor).unwrap();
            assert!(outcome.is_ok(), "owner terminated with {outcome:?}");
            runtime.finish().unwrap();
        });
        let client = receive.recv().unwrap();
        let mut producer = KrProducer::new(client, None, config).unwrap();
        producer.test_owner = Some(owner);
        Self {
            pointer: Box::into_raw(Box::new(producer)),
            start: Some(start),
        }
    }
    fn topic(&self) -> u32 {
        let mut topic = 0;
        assert_eq!(
            // SAFETY: fixture owns a live handle and the name/output spans are valid.
            unsafe { kr_topic_open(self.pointer, b"events".as_ptr(), 6, &mut topic) },
            KR_OK
        );
        topic
    }
    fn start(&mut self) {
        if let Some(start) = self.start.take() {
            start.send(()).unwrap();
        }
    }
    fn drain(mut self, close: bool) -> Vec<KrEvent> {
        if close {
            // SAFETY: fixture retains exclusive destruction ownership.
            assert_eq!(unsafe { kr_close(self.pointer, 0) }, KR_OK);
        }
        self.start();
        let deadline = Instant::now() + Duration::from_secs(3);
        let mut events = Vec::new();
        loop {
            let mut out = [KrEvent::empty(); 16];
            // SAFETY: every event output is writable with initialized version word.
            let count = unsafe { kr_poll_events(self.pointer, out.as_mut_ptr(), out.len() as u32) };
            events.extend_from_slice(&out[..count as usize]);
            if events.iter().any(|e| e.kind == 6) {
                break;
            }
            assert!(
                Instant::now() < deadline,
                "actor did not retire: {events:?}"
            );
            std::thread::sleep(Duration::from_millis(1));
        }
        // SAFETY: all calls and foreign buffer accesses finished; destroy exactly once.
        unsafe { kr_destroy(self.pointer) };
        self.pointer = std::ptr::null_mut();
        events
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        if self.pointer.is_null() {
            return;
        }
        if let Some(start) = self.start.take() {
            let _ = start.send(());
        }
        // SAFETY: fixture owns this handle exclusively and no caller retains native spans.
        unsafe { kr_destroy(self.pointer) };
    }
}
fn record(topic: u32, user_token: u64, value: KrSpan) -> KrRecord {
    KrRecord {
        struct_size: size_of::<KrRecord>() as u32,
        topic,
        partition_hint: -1,
        lane_hint: -1,
        key: KrSpan::default(),
        key_is_null: 1,
        value,
        value_is_null: 0,
        headers: std::ptr::null(),
        header_count: 0,
        timestamp_ms: 0,
        user_token,
        delivery_timeout_ns: 0,
    }
}
#[test]
fn copied_prefix_rejects_bad_version_without_losing_the_accepted_obligation() {
    let fixture = Fixture::new();
    let topic = fixture.topic();
    let mut bytes = b"payload".to_vec();
    let first = record(
        topic,
        91,
        KrSpan {
            ptr: bytes.as_ptr(),
            len: bytes.len() as u32,
        },
    );
    let mut second = first;
    second.struct_size -= 1;
    assert_eq!(
        // SAFETY: live handle and immutable initialized descriptor/payload array for the call.
        unsafe { kr_submitv_copy(fixture.pointer, [first, second].as_ptr(), 2) },
        1
    );
    // SAFETY: live opaque handle; querying last error has no effects.
    assert_eq!(unsafe { kr_last_error(fixture.pointer) }, KR_ERR_VERSION);
    bytes.fill(0);
    drop(bytes); // boundary copy already owns its immutable payload.
    assert_eq!(
        // SAFETY: present empty value has no readable payload requirement.
        unsafe { kr_submitv_copy(fixture.pointer, &record(topic, 92, KrSpan::default()), 1) },
        0
    );
    // SAFETY: live handle diagnostic access.
    assert_eq!(unsafe { kr_last_error(fixture.pointer) }, KR_ERR_EXHAUSTED);
    let events = fixture.drain(true);
    let deliveries: Vec<_> = events.iter().filter(|e| e.kind == 1).collect();
    assert_eq!(deliveries.len(), 1);
    assert_eq!(deliveries[0].user_token, 91);
    assert_eq!(deliveries[0].outcome, 1);
}
#[test]
fn native_commit_preserves_pointer_and_lease_input_releases_exactly_once() {
    let fixture = Fixture::new();
    let topic = fixture.topic();
    let mut pointer = std::ptr::null_mut();
    let mut lease = 0;
    assert_eq!(
        // SAFETY: fixture supplies live handle and writable independent outputs.
        unsafe { kr_buffer_acquire(fixture.pointer, 32, &mut pointer, &mut lease) },
        KR_OK
    );
    // SAFETY: acquired native memory is exclusively writable until commit.
    unsafe { std::ptr::copy_nonoverlapping(b"abcdefgh".as_ptr(), pointer, 8) };
    assert_eq!(
        // SAFETY: foreign writes have stopped; invalid commit preserves the acquisition.
        unsafe { kr_buffer_commit(fixture.pointer, lease, 33) },
        KR_ERR_INVALID
    );
    assert_eq!(
        // SAFETY: same still-live acquisition; no concurrent access.
        unsafe { kr_buffer_commit(fixture.pointer, lease, 8) },
        KR_OK
    );
    // SAFETY: handle is live; snapshot only inspects safe producer accounting.
    let before = unsafe { &*fixture.pointer }.client.credits().snapshot()
        [Resource::InputBytes as usize]
        .held;
    let descriptor = record(
        topic,
        101,
        KrSpan {
            ptr: pointer,
            len: 8,
        },
    );
    assert_eq!(
        // SAFETY: payload points into the committed lease, retained by the producer.
        unsafe { kr_submitv_leased(fixture.pointer, lease, &descriptor, 1) },
        1
    );
    assert_eq!(
        // SAFETY: shared immutable access to fixture accounting while handle remains live.
        unsafe { &*fixture.pointer }.client.credits().snapshot()[Resource::InputBytes as usize]
            .held,
        before
    );
    // SAFETY: no caller uses native bytes after release; accepted record owns a view.
    assert_eq!(unsafe { kr_buffer_release(fixture.pointer, lease) }, KR_OK);
    assert_eq!(
        // SAFETY: no payload dereference occurs: the released lease is rejected first.
        unsafe { kr_submitv_leased(fixture.pointer, lease, &descriptor, 1) },
        0
    );
    let events = fixture.drain(true);
    assert_eq!(
        events
            .iter()
            .filter(|e| e.kind == 2 && e.token == lease)
            .count(),
        1
    );
    assert_eq!(
        events
            .iter()
            .filter(|e| e.kind == 1 && e.user_token == 101)
            .count(),
        1
    );
}
#[test]
fn full_mailbox_panic_latches_failure_and_preserves_terminal_delivery() {
    let fixture = Fixture::new();
    let topic = fixture.topic();
    let descriptor = record(topic, 201, KrSpan::default());
    assert_eq!(
        // SAFETY: one present-empty record and a live initialized handle.
        unsafe { kr_submitv_copy(fixture.pointer, &descriptor, 1) },
        1
    );
    // SAFETY: private test injection uses the exact production unwind boundary.
    let failed: Result<(), i32> =
        unsafe { guarded(fixture.pointer, false, |_| panic!("FFI full-mailbox panic")) };
    assert_eq!(failed, Err(KR_ERR_FAILED));
    assert_eq!(
        // SAFETY: the same live handle now rejects admission through its latched state.
        unsafe { kr_submitv_copy(fixture.pointer, &descriptor, 1) },
        0
    );
    // SAFETY: diagnostic access to live opaque handle.
    assert_eq!(unsafe { kr_last_error(fixture.pointer) }, KR_ERR_FAILED);
    let events = fixture.drain(false);
    assert!(events.iter().any(|e| e.kind == 7));
    let delivery = events
        .iter()
        .find(|e| e.kind == 1 && e.user_token == 201)
        .unwrap();
    assert_eq!(delivery.outcome, 1);
    assert_eq!(
        delivery.reason,
        kr_kafka_producer::types::FailureReason::RuntimeFailed as u32
    );
}
#[test]
fn null_empty_invalid_foreign_lengths_and_output_version_are_explicit() {
    let fixture = Fixture::new();
    let topic = fixture.topic();
    let first = record(topic, 301, KrSpan::default());
    let mut second = first;
    second.user_token = 302;
    second.value_is_null = 1;
    assert_eq!(
        // SAFETY: both descriptors have valid present-empty/null representations.
        unsafe { kr_submitv_copy(fixture.pointer, [first, second].as_ptr(), 2) },
        2
    );
    let mut lease = 99;
    assert_eq!(
        // SAFETY: impossible length is rejected before the foreign address is read.
        unsafe { kr_lease_register(fixture.pointer, std::ptr::dangling(), u64::MAX, &mut lease) },
        KR_ERR_INVALID
    );
    assert_eq!(lease, 0);
    let mut event = KrEvent::default();
    // SAFETY: output is writable; intentionally wrong version must be rejected before drain.
    assert_eq!(unsafe { kr_poll_events(fixture.pointer, &mut event, 1) }, 0);
    // SAFETY: live-handle diagnostic query.
    assert_eq!(unsafe { kr_last_error(fixture.pointer) }, KR_ERR_VERSION);
    let events = fixture.drain(true);
    assert_eq!(events.iter().filter(|e| e.kind == 1).count(), 2);
}

#[test]
fn foreign_registration_checks_ranges_and_releases_once_after_accepted_use() {
    let fixture = Fixture::new();
    let topic = fixture.topic();
    let bytes = Box::new(*b"abcd");
    let mut lease = 0;
    assert_eq!(
        // SAFETY: this box stays immutable and live through the terminal release below.
        unsafe { kr_lease_register(fixture.pointer, bytes.as_ptr(), 4, &mut lease) },
        KR_OK
    );
    let mut descriptor = record(
        topic,
        401,
        KrSpan {
            ptr: bytes.as_ptr().wrapping_add(3),
            len: 2,
        },
    );
    assert_eq!(
        // SAFETY: descriptors are live; bad leased ranges are validated without dereference.
        unsafe { kr_submitv_leased(fixture.pointer, lease, &descriptor, 1) },
        0
    );
    // SAFETY: a live handle diagnostic query.
    assert_eq!(unsafe { kr_last_error(fixture.pointer) }, KR_ERR_INVALID);
    descriptor.value = KrSpan {
        ptr: bytes.as_ptr().wrapping_add(1),
        len: 2,
    };
    assert_eq!(
        // SAFETY: valid immutable range in the still-pinned foreign box.
        unsafe { kr_submitv_leased(fixture.pointer, lease, &descriptor, 1) },
        1
    );
    // SAFETY: release forbids future submissions; box stays pinned until the event.
    assert_eq!(unsafe { kr_buffer_release(fixture.pointer, lease) }, KR_OK);
    let mut out = KrEvent::empty();
    // SAFETY: owner is paused and still holds the accepted range; output is valid.
    assert_eq!(unsafe { kr_poll_events(fixture.pointer, &mut out, 1) }, 0);
    let events = fixture.drain(true);
    assert_eq!(
        events
            .iter()
            .filter(|event| event.kind == 2 && event.token == lease)
            .count(),
        1
    );
    assert_eq!(
        events
            .iter()
            .filter(|event| event.kind == 1 && event.user_token == 401)
            .count(),
        1
    );
    drop(bytes);
}

#[test]
fn destroy_waits_for_actual_input_retirement_after_owner_abort() {
    let mut fixture = Fixture::with_owner_abort(true);
    // SAFETY: the fixture is live; this extra safe client capability deliberately
    // retains an input owner beyond the actor lifetime, as a provider can do.
    let held = unsafe { &*fixture.pointer }.client.acquire(8, 0).unwrap();
    fixture.start();
    // SAFETY: actor never accesses the FFI wrapper; exclusive test ownership lets
    // us join and clear its handle before transferring destruction to a thread.
    unsafe { &mut *fixture.pointer }
        .test_owner
        .take()
        .unwrap()
        .join()
        .unwrap();
    let address = fixture.pointer as usize;
    fixture.pointer = std::ptr::null_mut();
    let (send, receive) = sync_channel(1);
    let destroyer = std::thread::spawn(move || {
        // SAFETY: this thread receives sole destruction ownership, with no ABI
        // calls racing it. The separate safe input owner is intentionally live.
        unsafe { kr_destroy(address as *mut KrProducer) };
        send.send(()).unwrap();
    });
    assert_eq!(
        receive.recv_timeout(Duration::from_millis(20)),
        Err(std::sync::mpsc::RecvTimeoutError::Timeout)
    );
    drop(held); // last real allocation guard, independent of the aborted actor
    receive.recv_timeout(Duration::from_secs(3)).unwrap();
    destroyer.join().unwrap();
}
#[test]
fn abi_versions_and_invalid_config_fail_before_host_resources() {
    fn thread_safe<T: Send + Sync>() {}
    thread_safe::<KrProducer>();
    let mut config = KrProducerConfig::default();
    let mut pointer = std::ptr::dangling_mut::<KrProducer>();
    assert_eq!(
        // SAFETY: initialized config/output; invalid size is checked before decode.
        unsafe { kr_producer_config_init(&mut config, 1) },
        KR_ERR_VERSION
    );
    config.struct_size = 1;
    assert_eq!(
        // SAFETY: readable version word and writable output, no nested pointer is read.
        unsafe { kr_producer_create(&config, &mut pointer) },
        KR_ERR_VERSION
    );
    assert!(pointer.is_null());
    // SAFETY: NULL is an explicitly accepted no-op destruction.
    unsafe { kr_destroy(std::ptr::null_mut()) };
}

#[test]
fn per_call_bulk_limit_reports_the_rejected_suffix() {
    let fixture = Fixture::new();
    let topic = fixture.topic();
    let inputs: [KrRecord; 9] =
        std::array::from_fn(|i| record(topic, 500 + i as u64, KrSpan::default()));
    assert_eq!(
        // SAFETY: the complete input array is immutable and each present-empty span is valid.
        unsafe { kr_submitv_copy(fixture.pointer, inputs.as_ptr(), 9) },
        8
    );
    // SAFETY: diagnostic access to the live handle after a partial bulk admission.
    assert_eq!(unsafe { kr_last_error(fixture.pointer) }, KR_ERR_EXHAUSTED);
    let events = fixture.drain(true);
    let delivered: Vec<_> = events
        .iter()
        .filter(|event| event.kind == 1)
        .map(|event| event.user_token)
        .collect();
    assert_eq!(delivered, (500..508).collect::<Vec<_>>());
}

#[test]
fn header_heavy_rejected_suffix_cannot_consume_a_fitting_prefix_budget() {
    for foreign in [false, true] {
        let fixture = Fixture::new();
        let topic = fixture.topic();
        let source = Box::new(*b"v");
        let header = KrHeader {
            struct_size: size_of::<KrHeader>() as u32,
            key: KrSpan::default(),
            value: KrSpan::default(),
            value_is_null: 1,
        };
        let headers = vec![header; 1024];
        let first = record(
            topic,
            601,
            KrSpan {
                ptr: source.as_ptr(),
                len: 1,
            },
        );
        let mut suffix = record(topic, 602, KrSpan::default());
        suffix.headers = headers.as_ptr();
        suffix.header_count = headers.len() as u32;
        let records = [first, suffix, suffix, suffix, suffix];
        let mut lease = 0;
        let accepted = if foreign {
            assert_eq!(
                // SAFETY: the immutable source remains pinned until release and destroy below.
                unsafe { kr_lease_register(fixture.pointer, source.as_ptr(), 1, &mut lease) },
                KR_OK
            );
            // SAFETY: all descriptors are valid and their spans address the committed lease.
            unsafe {
                kr_submitv_leased(
                    fixture.pointer,
                    lease,
                    records.as_ptr(),
                    records.len() as u32,
                )
            }
        } else {
            // SAFETY: all source spans and descriptor/header arrays stay live through the call.
            unsafe { kr_submitv_copy(fixture.pointer, records.as_ptr(), records.len() as u32) }
        };
        assert_eq!(accepted, 1, "foreign={foreign}");
        // SAFETY: the live handle exposes the reason for the rejected suffix.
        assert_eq!(unsafe { kr_last_error(fixture.pointer) }, KR_ERR_EXHAUSTED);
        if foreign {
            // SAFETY: source remains immutable and pinned through the subsequent event drain.
            assert_eq!(unsafe { kr_buffer_release(fixture.pointer, lease) }, KR_OK);
        }
        let events = fixture.drain(true);
        let deliveries: Vec<_> = events.iter().filter(|event| event.kind == 1).collect();
        assert_eq!(deliveries.len(), 1);
        assert_eq!(deliveries[0].user_token, 601);
        drop(source);
    }
}
