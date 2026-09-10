use std::panic::panic_any;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll, Wake, Waker};

use kr_runtime::{CompletionCertainty, JoinError, SimDuration, SimRuntime};
use kr_runtime_io::{SimDisk, SimStorageConfig};

use super::*;

struct PanickingWake;

struct PanickingPayload;

impl Drop for PanickingPayload {
    fn drop(&mut self) {
        panic!("injected actor wake payload destructor panic");
    }
}

impl Wake for PanickingWake {
    fn wake(self: Arc<Self>) {
        panic_any(PanickingPayload);
    }
}

fn actor_config() -> FileRingConfig {
    FileRingConfig {
        limits: RingLimits {
            max_record_bytes: 64,
            max_live_records: 8,
            max_live_payload_bytes: 256,
            max_read_records: 4,
            max_read_bytes: 128,
            max_batch_records: 4,
            max_batch_bytes: 128,
        },
        data_capacity_bytes: 512,
        max_io_request_bytes: 4_096,
        command_queue_capacity: 4,
    }
}

fn storage_config() -> SimStorageConfig {
    test_support::sim_storage_config(actor_config(), SimDuration::from_nanos(100), 8)
}

#[test]
fn cancelling_actor_completes_active_and_queued_commands() {
    let mut runtime = SimRuntime::default();
    let disk = SimDisk::default();
    let storage = disk
        .open(runtime.handle(), storage_config())
        .expect("open simulated file");
    let driver = runtime
        .block_on(FileRingDriver::create(storage.clone(), actor_config()))
        .expect("runtime completes create")
        .expect("driver creates");
    let (ring, actor) = driver.start();
    let actor_task = runtime
        .handle()
        .spawn(actor)
        .expect("spawn manually controlled actor");

    let buffers = vec![b"owned-after-cancel".to_vec()];
    let append = ring.append(AppendRequest::new(buffers.clone()));
    let queued_buffers = vec![b"queued-owned-after-cancel".to_vec()];
    let queued_append = ring.append(AppendRequest::new(queued_buffers.clone()));
    let read = ring.read(ReadRequest::new(RingCursor::START, 1, 64));
    let sync = ring.sync();

    // Stop immediately after the actor has admitted its lower-level append
    // write. The write remains pending, so dropping the actor exercises the
    // active-command guard rather than merely draining an untouched queue.
    for _ in 0..16 {
        if storage.status().in_flight != 0 {
            break;
        }
        runtime.step().expect("runtime step succeeds");
    }
    assert_eq!(storage.status().in_flight, 1, "append I/O is pending");

    actor_task.abort();
    assert_eq!(
        runtime
            .block_on(actor_task)
            .expect("runtime joins cancelled actor"),
        Err(JoinError::Cancelled)
    );

    let append_error = runtime
        .block_on(append)
        .expect("runtime completes append response")
        .expect_err("cancelled append fails");
    assert_eq!(append_error.certainty(), CompletionCertainty::NotApplied);
    assert_eq!(append_error.error().error, RingError::RecoveryRequired);
    assert_eq!(append_error.error().records, buffers);
    assert_eq!(append_error.error().accepted_range, None);

    let queued_append_error = runtime
        .block_on(queued_append)
        .expect("runtime completes queued append response")
        .expect_err("queued append fails");
    assert_eq!(
        queued_append_error.certainty(),
        CompletionCertainty::NotApplied
    );
    assert_eq!(
        queued_append_error.error().error,
        RingError::RecoveryRequired
    );
    assert_eq!(queued_append_error.error().records, queued_buffers);
    assert_eq!(queued_append_error.error().accepted_range, None);

    let read_error = runtime
        .block_on(read)
        .expect("runtime completes queued read response")
        .expect_err("queued read fails");
    assert_eq!(read_error.certainty(), CompletionCertainty::NotApplied);
    assert_eq!(read_error.error(), &RingError::RecoveryRequired);

    let sync_error = runtime
        .block_on(sync)
        .expect("runtime completes queued sync response")
        .expect_err("queued sync fails");
    assert_eq!(sync_error.certainty(), CompletionCertainty::NotApplied);
    assert_eq!(sync_error.error().error, RingError::RecoveryRequired);
    assert_eq!(sync_error.error().checkpoint, None);

    let later_error = runtime
        .block_on(ring.status())
        .expect("runtime completes rejected later status")
        .expect_err("later admission is closed");
    assert_eq!(later_error.certainty(), CompletionCertainty::NotApplied);
    assert_eq!(later_error.error(), &RingError::RecoveryRequired);

    let actor = lock_unpoisoned(&ring.actor);
    assert!(!actor.accepting);
    assert_eq!(actor.in_flight, 0, "every command released one permit");
    assert!(actor.queue.is_empty());
}

#[test]
fn dropping_last_handle_contains_actor_waker_and_payload_drop_panics() {
    let mut runtime = SimRuntime::default();
    let storage = SimDisk::default()
        .open(runtime.handle(), storage_config())
        .expect("open simulated file");
    let driver = runtime
        .block_on(FileRingDriver::create(storage, actor_config()))
        .expect("runtime completes create")
        .expect("driver creates");
    let (ring, actor) = driver.start();
    let mut actor = Box::pin(actor);
    let waker = Waker::from(Arc::new(PanickingWake));
    let mut context = Context::from_waker(&waker);
    assert!(matches!(
        Pin::new(&mut actor).poll(&mut context),
        Poll::Pending
    ));

    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| drop(ring)));
    assert!(
        result.is_ok(),
        "last-handle drop must contain waker and payload destructor panics"
    );
}
