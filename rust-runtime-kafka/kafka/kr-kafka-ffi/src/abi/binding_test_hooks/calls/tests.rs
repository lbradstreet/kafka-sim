use super::*;

#[test]
fn paused_owner_control_pressure_is_retryable_and_creates_no_flush_obligation() {
    let fixture = Fixture::new();
    let mut accepted = Vec::new();
    loop {
        let mut token = 0;
        // SAFETY: paused fixture retains handle and independent token output.
        let code = unsafe { kr_flush(fixture.0, &mut token) };
        if code == KR_OK {
            accepted.push(token);
            assert!(
                accepted.len() <= 64,
                "fixture control ingress must be bounded"
            );
        } else {
            assert_eq!(code, KR_ERR_EXHAUSTED);
            assert_eq!(token, 0, "rejection must not invent a flush token");
            // SAFETY: no intervening diagnostic-mutating downcall.
            assert_eq!(unsafe { kr_last_error(fixture.0) }, KR_ERR_EXHAUSTED);
            break;
        }
    }
    fixture.resume();
    assert!(
        // SAFETY: live fixture waits on actual publication after the owner resumes.
        unsafe { &*fixture.0 }
            .client
            .test_wait_publication(3, accepted.len() as u64, WAIT)
    );
    let mut actual = Vec::new();
    while actual.len() < accepted.len() {
        let events = fixture.events();
        assert!(
            !events.is_empty(),
            "all accepted flushes were already published"
        );
        actual.extend(
            events
                .iter()
                .filter(|event| event.kind == 3)
                .map(|event| event.token),
        );
    }
    assert_eq!(actual, accepted);
}

#[test]
fn actual_control_mailbox_exhaustion_is_not_invalid_input() {
    use kr_kafka_producer::mailbox::MailboxError;
    let fixture = Fixture::new();
    // Cancellation uses control ingress without consuming a ControlEvents
    // credit. Fill that distinct bound while the actual owner is paused.
    for count in 0..=64 {
        // SAFETY: fixture owns its live client and the owner remains paused.
        match unsafe { &*fixture.0 }.client.cancel(RecordToken(1)) {
            Ok(()) => assert!(count < 64),
            Err(ClientError::Mailbox(MailboxError::Full { .. })) => break,
            other => panic!("unexpected control admission: {other:?}"),
        }
    }
    let mut token = 0;
    // SAFETY: independent initialized output and retained producer.
    assert_eq!(unsafe { kr_flush(fixture.0, &mut token) }, KR_ERR_EXHAUSTED);
    assert_eq!(token, 0);
    // SAFETY: no intervening producer downcall may overwrite this error.
    assert_eq!(unsafe { kr_last_error(fixture.0) }, KR_ERR_EXHAUSTED);
}

#[test]
fn cancellation_cursor_rejects_invalid_input_and_cancels_a_real_owner_record() {
    let fixture = Fixture::new();
    let topic = fixture.topic();
    // Owner remains paused while the record and cancellation enter the mailbox.
    let call = fixture.call(1, topic, true);
    assert_eq!(call.finish(), 1);
    let mut cursor = 2;
    assert_eq!(
        // SAFETY: fixture retains handle and owns initialized independent cursor.
        unsafe { kr_test_cancel_since(fixture.0, &mut cursor) },
        KR_ERR_INVALID
    );
    assert_eq!(cursor, 2);
    assert_eq!(
        // SAFETY: deliberately null cursor is rejected before reading it.
        unsafe { kr_test_cancel_since(fixture.0, std::ptr::null_mut()) },
        KR_ERR_INVALID
    );
    cursor = 0;
    assert_eq!(
        // SAFETY: initialized cursor and live fixture; cancellation is a real command.
        unsafe { kr_test_cancel_since(fixture.0, &mut cursor) },
        KR_OK
    );
    assert_eq!(cursor, 1);
    assert_eq!(
        // SAFETY: repeating a current watermark requests no duplicate cancellation.
        unsafe { kr_test_cancel_since(fixture.0, &mut cursor) },
        KR_OK
    );
    fixture.resume();
    assert!(
        // SAFETY: fixture remains live through this observable owner-publication wait.
        unsafe { &*fixture.0 }
            .client
            .test_wait_publication(1, 1, WAIT)
    );
    let events = fixture.events();
    let deliveries: Vec<_> = events.iter().filter(|event| event.kind == 1).collect();
    assert_eq!(deliveries.len(), 1);
    assert_eq!(deliveries[0].token, 1);
    assert_eq!(
        deliveries[0].reason,
        kr_kafka_producer::types::FailureReason::Cancelled as u32
    );
}

struct Fixture(*mut KrProducer);
impl Fixture {
    fn new() -> Self {
        let mut pointer = std::ptr::null_mut();
        // SAFETY: independent writable output; fixture owns the returned handle.
        assert_eq!(unsafe { kr_test_producer_create(&mut pointer) }, KR_OK);
        Self(pointer)
    }
    fn topic(&self) -> u32 {
        let mut topic = 0;
        assert_eq!(
            // SAFETY: fixture retains the handle; name and output are independent.
            unsafe { kr_topic_open(self.0, b"events".as_ptr(), 6, &mut topic) },
            KR_OK
        );
        topic
    }
    fn resume(&self) {
        // SAFETY: fixture owns the live test producer.
        assert_eq!(unsafe { kr_test_resume(self.0) }, KR_OK);
    }
    fn arm(&self, operation: u32, flags: u32) {
        // SAFETY: test serializes arms and retains the producer.
        assert_eq!(unsafe { kr_test_arm_call(self.0, operation, flags) }, KR_OK);
    }
    fn wait(&self, phase: u32) {
        // SAFETY: fixture outlives both the waiter and armed call.
        assert_eq!(unsafe { kr_test_wait_call(self.0, phase, 5000) }, KR_OK);
    }
    fn events(&self) -> Vec<KrEvent> {
        let mut events = [KrEvent::empty(); 16];
        // SAFETY: live handle and independent initialized output slots.
        let count = unsafe { kr_poll_events(self.0, events.as_mut_ptr(), 16) };
        events[..count as usize].to_vec()
    }
    fn call(&self, operation: u32, topic: u32, valid: bool) -> Running {
        let raw = self.0 as usize;
        let worker = std::thread::spawn(move || {
            let raw = raw as *mut KrProducer;
            if operation == 1 {
                let mut record = KrRecord {
                    struct_size: size_of::<KrRecord>() as u32,
                    topic,
                    partition_hint: -1,
                    lane_hint: -1,
                    key: KrSpan::default(),
                    key_is_null: 1,
                    value: KrSpan {
                        ptr: b"payload".as_ptr(),
                        len: 7,
                    },
                    value_is_null: 0,
                    headers: std::ptr::null(),
                    header_count: 0,
                    timestamp_ms: 0,
                    user_token: 91,
                    delivery_timeout_ns: 0,
                };
                if !valid {
                    record.struct_size -= 1;
                }
                // SAFETY: fixture outlives joined thread; descriptors/static payload remain valid.
                unsafe { kr_submitv_copy(raw, &record, 1) }
            } else {
                let mut token = 0;
                // SAFETY: fixture outlives joined thread; independent writable token.
                let result = unsafe { kr_flush(raw, &mut token) };
                assert_eq!(result, KR_OK);
                token as u32
            }
        });
        Running {
            raw: self.0,
            worker: Some(worker),
        }
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        // SAFETY: all Running guards drop before their parent fixture.
        unsafe { kr_destroy(self.0) };
    }
}
struct Running {
    raw: *mut KrProducer,
    worker: Option<std::thread::JoinHandle<u32>>,
}
impl Running {
    fn finish(mut self) -> u32 {
        // SAFETY: fixture still retains the live test handle.
        assert_eq!(unsafe { kr_test_release_call(self.raw) }, KR_OK);
        self.worker.take().unwrap().join().unwrap()
    }
}
impl Drop for Running {
    fn drop(&mut self) {
        // SAFETY: release/join runs before fixture destruction, also on assertion panic.
        let _ = unsafe { kr_test_release_call(self.raw) };
        if let Some(worker) = self.worker.take() {
            let _ = worker.join();
        }
    }
}

#[test]
fn actual_owner_delivery_precedes_submit_return_and_bounded_replay_uses_that_event() {
    let fixture = Fixture::new();
    let topic = fixture.topic();
    fixture.resume();
    fixture.arm(1, CANCEL | WAIT_PUBLICATION | PAUSE);
    let call = fixture.call(1, topic, true);
    fixture.wait(PUBLISHED);
    // SAFETY: fixture retains the live test producer across the observation.
    assert_eq!(unsafe { kr_test_call_state(fixture.0) } & FINISHED, 0);
    let events = fixture.events();
    let delivery = events
        .iter()
        .find(|event| event.kind == 1)
        .expect("real delivery published before submit returns");
    assert_eq!(delivery.user_token, 91);
    assert_eq!(
        delivery.reason,
        kr_kafka_producer::types::FailureReason::Cancelled as u32
    );
    assert_eq!(call.finish(), 1);
    // SAFETY: bounded corruption probe on a live, explicit test artifact.
    assert_eq!(unsafe { kr_test_replay_delivery(fixture.0, 0) }, KR_OK);
    assert_eq!(
        // SAFETY: a second pending replay must reject without replacing the first.
        unsafe { kr_test_replay_delivery(fixture.0, 999) },
        KR_ERR_EXHAUSTED
    );
    let replay: Vec<_> = fixture
        .events()
        .into_iter()
        .filter(|event| event.kind == 1)
        .collect();
    assert_eq!(replay.len(), 1);
    assert_eq!(replay[0].token, delivery.token);
    assert_eq!(replay[0].user_token, delivery.user_token);
    // SAFETY: explicit stale/unknown token corruption after the first replay drained.
    assert_eq!(unsafe { kr_test_replay_delivery(fixture.0, 999) }, KR_OK);
    assert_eq!(
        fixture
            .events()
            .iter()
            .find(|event| event.kind == 1)
            .unwrap()
            .user_token,
        999
    );
}

#[test]
fn actual_owner_flush_publication_precedes_its_returned_token() {
    let fixture = Fixture::new();
    fixture.resume();
    fixture.arm(2, WAIT_PUBLICATION | PAUSE);
    let call = fixture.call(2, 0, true);
    fixture.wait(PUBLISHED);
    // SAFETY: live retained fixture handle.
    assert_eq!(unsafe { kr_test_call_state(fixture.0) } & FINISHED, 0);
    let events = fixture.events();
    let flushes: Vec<_> = events.iter().filter(|event| event.kind == 3).collect();
    assert_eq!(flushes.len(), 1);
    assert_eq!(u64::from(call.finish()), flushes[0].token);
}

#[test]
fn barrier_observation_preserves_rejection_diagnostics_and_real_poll_can_overwrite_them() {
    let fixture = Fixture::new();
    let topic = fixture.topic();
    fixture.arm(1, PAUSE);
    let call = fixture.call(1, topic, false);
    fixture.wait(ENTERED);
    // SAFETY: fixture retains live producer and observation calls never write diagnostics.
    unsafe {
        assert_eq!(kr_last_error(fixture.0), KR_ERR_VERSION);
        assert_eq!(kr_test_call_state(fixture.0) & FINISHED, 0);
        assert_eq!(kr_test_wait_call(fixture.0, ENTERED, 0), KR_OK);
        assert_eq!(kr_last_error(fixture.0), KR_ERR_VERSION);
    }
    assert!(fixture.events().is_empty());
    // SAFETY: ordinary event polling intentionally exhibits the legacy shared diagnostic race.
    assert_eq!(unsafe { kr_last_error(fixture.0) }, KR_OK);
    assert_eq!(call.finish(), 0);
}

#[test]
fn release_before_wait_registration_is_not_lost() {
    let fixture = Fixture::new();
    let topic = fixture.topic();
    fixture.arm(1, PAUSE);
    // SAFETY: released before the worker enters; predicate keeps this notification.
    assert_eq!(unsafe { kr_test_release_call(fixture.0) }, KR_OK);
    let call = fixture.call(1, topic, false);
    fixture.wait(FINISHED);
    assert_eq!(call.finish(), 0);
}
