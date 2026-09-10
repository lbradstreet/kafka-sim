#[cfg(feature = "test-support")]
use std::future::Future;
use std::future::poll_fn;
use std::panic::{AssertUnwindSafe, catch_unwind};
#[cfg(feature = "test-support")]
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll, Wake, Waker};

#[cfg(feature = "test-support")]
use kr_runtime::CompletionResult;
use kr_runtime::{CompletionCertainty, SimDuration, SimRuntime};
#[cfg(feature = "test-support")]
use kr_runtime_io::conformance::check_empty_file;
use kr_runtime_io::storage::{
    FileIoSubmit, ReadAtRequest, SIM_FSYNC_GATE_VERSION, SIM_FSYNC_PAGE_BYTES,
    SIM_PIPELINE_MODEL_VERSION, SimDisk, SimFault, SimFaultError, SimFsyncFailure, SimOpenError,
    SimOutcome, SimPipelineModel, SimRandomSources, SimStorage, SimStorageConfig, StorageError,
    StorageOperation, WriteAtRequest,
};
#[cfg(feature = "test-support")]
use kr_runtime_io::storage::{
    FileLength, ReadAtFailure, ReadAtSuccess, SetLenSuccess, SyncSuccess, WriteAtFailure,
    WriteAtSuccess,
};

fn open(runtime: &SimRuntime, disk: &SimDisk, config: SimStorageConfig) -> SimStorage {
    disk.open(runtime.handle(), config)
        .expect("open simulated disk")
}

#[cfg(feature = "test-support")]
#[test]
fn simulated_storage_passes_shared_conformance() {
    let mut runtime = SimRuntime::default();
    let storage = open(&runtime, &SimDisk::default(), SimStorageConfig::default());

    runtime
        .block_on(check_empty_file(storage))
        .expect("runtime completes")
        .expect("storage conforms");
}

#[cfg(feature = "test-support")]
#[derive(Clone)]
struct WrongLengthStorage(SimStorage);

#[cfg(feature = "test-support")]
impl FileIoSubmit for WrongLengthStorage {
    type ReadAtResponse =
        Pin<Box<dyn Future<Output = CompletionResult<ReadAtSuccess, ReadAtFailure>>>>;
    type WriteAtResponse =
        Pin<Box<dyn Future<Output = CompletionResult<WriteAtSuccess, WriteAtFailure>>>>;
    type SetLenResponse =
        Pin<Box<dyn Future<Output = CompletionResult<SetLenSuccess, StorageError>>>>;
    type LenResponse = Pin<Box<dyn Future<Output = CompletionResult<FileLength, StorageError>>>>;
    type SyncResponse = Pin<Box<dyn Future<Output = CompletionResult<SyncSuccess, StorageError>>>>;

    fn submit_read_at(&self, request: ReadAtRequest) -> Self::ReadAtResponse {
        Box::pin(self.0.submit_read_at(request))
    }

    fn submit_write_at(&self, request: WriteAtRequest) -> Self::WriteAtResponse {
        Box::pin(self.0.submit_write_at(request))
    }

    fn submit_set_len(&self, len: u64) -> Self::SetLenResponse {
        Box::pin(self.0.submit_set_len(len))
    }

    fn submit_len(&self) -> Self::LenResponse {
        let future = self.0.submit_len();
        Box::pin(async move {
            let mut length = future.await?;
            length.len = length.len.saturating_add(1);
            Ok(length)
        })
    }

    fn submit_sync(&self) -> Self::SyncResponse {
        Box::pin(self.0.submit_sync())
    }
}

#[cfg(feature = "test-support")]
#[test]
fn shared_file_contract_rejects_a_wrong_length_mutant() {
    let mut runtime = SimRuntime::default();
    let storage = open(&runtime, &SimDisk::default(), SimStorageConfig::default());

    let error = runtime
        .block_on(check_empty_file(WrongLengthStorage(storage)))
        .expect("runtime completes mutant check")
        .expect_err("contract checker accepted a deliberately wrong file length");

    assert!(
        error.contains("new file length was 1"),
        "mutant tripped an unrelated contract assertion: {error}"
    );
}

#[test]
fn completed_simulated_operation_panics_when_polled_again() {
    let mut runtime = SimRuntime::default();
    let storage = open(&runtime, &SimDisk::default(), SimStorageConfig::default());
    let mut operation = Box::pin(storage.submit_len());
    let mut context = Context::from_waker(Waker::noop());
    loop {
        match operation.as_mut().poll(&mut context) {
            Poll::Ready(result) => {
                result.expect("length succeeds");
                break;
            }
            Poll::Pending => {
                runtime.step().expect("runtime advances storage operation");
            }
        }
    }
    let panic = catch_unwind(AssertUnwindSafe(|| operation.as_mut().poll(&mut context)));
    assert!(panic.is_err(), "completed operation was silently repolled");
}

#[test]
fn accepted_bytes_are_lost_on_crash_but_synced_bytes_survive() {
    let mut runtime = SimRuntime::default();
    let disk = SimDisk::default();
    let config = SimStorageConfig::default();
    let storage = open(&runtime, &disk, config);

    let write = storage.submit_write_at(WriteAtRequest::new(0, b"lost".to_vec()));
    runtime
        .block_on(write)
        .expect("runtime completes")
        .expect("write accepted");
    assert_eq!(storage.status().accepted_len, 4);
    assert_eq!(storage.status().durable_len, 0);

    storage.crash();
    let reopened = open(&runtime, &disk, config);
    let len = runtime
        .block_on(reopened.submit_len())
        .expect("runtime completes")
        .expect("read length");
    assert_eq!(len.len, 0);

    runtime
        .block_on(reopened.submit_write_at(WriteAtRequest::new(0, b"kept".to_vec())))
        .expect("runtime completes")
        .expect("write accepted");
    runtime
        .block_on(reopened.submit_sync())
        .expect("runtime completes")
        .expect("sync succeeds");
    reopened.crash();

    let recovered = open(&runtime, &disk, config);
    let read = runtime
        .block_on(recovered.submit_read_at(ReadAtRequest::new(0, vec![0; 8])))
        .expect("runtime completes")
        .expect("read succeeds");
    assert_eq!(read.buffer, b"kept");
    assert_eq!(disk.durable_bytes(), b"kept");
}

#[test]
fn queue_backpressure_is_decided_at_invocation_and_preserves_buffers() {
    let mut runtime = SimRuntime::default();
    let config = SimStorageConfig {
        max_in_flight: 1,
        default_latency: SimDuration::from_nanos(5),
        ..SimStorageConfig::default()
    };
    let storage = open(&runtime, &SimDisk::default(), config);

    let admitted = storage.submit_write_at(WriteAtRequest::new(0, b"first".to_vec()));
    let rejected = storage.submit_write_at(WriteAtRequest::new(5, b"second".to_vec()));
    let error = runtime
        .block_on(rejected)
        .expect("runtime completes")
        .expect_err("second request is rejected immediately");
    assert_eq!(error.certainty(), CompletionCertainty::NotApplied);
    assert_eq!(error.error().buffer, b"second");
    assert_eq!(error.error().bytes_transferred, 0);
    assert_eq!(
        error.error().error,
        StorageError::ResourceExhausted {
            resource: "in-flight storage operations",
            limit: 1,
        }
    );

    runtime
        .block_on(admitted)
        .expect("runtime completes")
        .expect("first write completes");
}

#[test]
fn byte_backpressure_is_decided_at_invocation_and_preserves_buffers() {
    let mut runtime = SimRuntime::default();
    let config = SimStorageConfig {
        max_read_bytes: 8,
        max_write_bytes: 8,
        max_outstanding_bytes: 8,
        default_latency: SimDuration::from_nanos(5),
        ..SimStorageConfig::default()
    };
    let storage = open(&runtime, &SimDisk::default(), config);

    let admitted = storage.submit_write_at(WriteAtRequest::new(0, vec![b'a'; 6]));
    assert_eq!(storage.status().outstanding_bytes, 6);

    // Three more bytes do not fit under the eight-byte budget even though the
    // in-flight count is far from its own limit.
    let rejected = storage.submit_write_at(WriteAtRequest::new(6, vec![b'b'; 3]));
    let error = runtime
        .block_on(rejected)
        .expect("runtime completes")
        .expect_err("second request exceeds the byte budget");
    assert_eq!(error.certainty(), CompletionCertainty::NotApplied);
    assert_eq!(error.error().buffer, vec![b'b'; 3]);
    assert_eq!(error.error().bytes_transferred, 0);
    assert_eq!(
        error.error().error,
        StorageError::ResourceExhausted {
            resource: "outstanding storage bytes",
            limit: 8,
        }
    );
    assert_eq!(
        storage.status().outstanding_bytes,
        6,
        "a refused admission leaves the budget untouched"
    );

    runtime
        .block_on(admitted)
        .expect("runtime completes")
        .expect("first write completes");
    assert_eq!(
        storage.status().outstanding_bytes,
        0,
        "a terminalized operation releases its whole charge"
    );
}

#[test]
fn operations_charge_their_buffer_allocation_not_their_length() {
    // A pooled buffer pins its full allocation while in flight, so the byte
    // budget charges capacity. Charging the request length instead would let
    // a fixed-size pool hold far more memory than the configured bound.
    let mut runtime = SimRuntime::default();
    let storage = open(&runtime, &SimDisk::default(), SimStorageConfig::default());

    let mut buffer = Vec::with_capacity(512);
    buffer.resize(8, 0u8);
    let capacity = buffer.capacity();
    assert!(capacity >= 512, "the buffer must be over-provisioned");

    let write = storage.submit_write_at(WriteAtRequest::new(0, buffer));
    assert_eq!(
        storage.status().outstanding_bytes,
        capacity,
        "an admitted write charges its allocation, not its payload"
    );
    runtime
        .block_on(write)
        .expect("runtime completes")
        .expect("write succeeds");
    assert_eq!(storage.status().outstanding_bytes, 0);

    let mut buffer = Vec::with_capacity(512);
    buffer.resize(8, 0u8);
    let capacity = buffer.capacity();
    let read = storage.submit_read_at(ReadAtRequest::new(0, buffer));
    assert_eq!(
        storage.status().outstanding_bytes,
        capacity,
        "an admitted read charges its allocation, not its fill target"
    );
    runtime
        .block_on(read)
        .expect("runtime completes")
        .expect("read succeeds");
    assert_eq!(storage.status().outstanding_bytes, 0);

    // An allocation the whole budget cannot hold could never be admitted no
    // matter how often it were retried, so it is rejected as over-large up
    // front — not reported as resumable exhaustion that can never resolve.
    let config = SimStorageConfig {
        max_read_bytes: 8,
        max_write_bytes: 8,
        max_outstanding_bytes: 8,
        ..SimStorageConfig::default()
    };
    let bounded = open(&runtime, &SimDisk::default(), config);
    let mut buffer = Vec::with_capacity(64);
    buffer.resize(4, 0u8);
    let capacity = buffer.capacity();
    let error = runtime
        .block_on(bounded.submit_read_at(ReadAtRequest::new(0, buffer)))
        .expect("runtime completes")
        .expect_err("an allocation larger than the whole budget is refused");
    assert_eq!(error.certainty(), CompletionCertainty::NotApplied);
    assert_eq!(
        error.error().error,
        StorageError::RequestTooLarge {
            operation: StorageOperation::ReadAt,
            requested: capacity,
            limit: 8,
        }
    );
    assert_eq!(error.error().buffer.capacity(), capacity);
}

#[test]
fn metadata_operations_hold_no_bytes_against_the_budget() {
    let mut runtime = SimRuntime::default();
    let config = SimStorageConfig {
        max_read_bytes: 8,
        max_write_bytes: 8,
        max_outstanding_bytes: 8,
        default_latency: SimDuration::from_nanos(5),
        ..SimStorageConfig::default()
    };
    let storage = open(&runtime, &SimDisk::default(), config);

    let filled = storage.submit_write_at(WriteAtRequest::new(0, vec![b'a'; 8]));
    assert_eq!(storage.status().outstanding_bytes, 8);

    // The byte budget is fully committed, so a charging operation would be
    // refused here; a metadata operation carries no caller allocation.
    let sync = storage.submit_sync();
    assert_eq!(storage.status().outstanding_bytes, 8);

    runtime
        .block_on(filled)
        .expect("runtime completes")
        .expect("write completes");
    runtime
        .block_on(sync)
        .expect("runtime completes")
        .expect("sync completes");
    assert_eq!(storage.status().outstanding_bytes, 0);
}

#[test]
fn an_abandoned_response_still_releases_its_byte_charge() {
    let mut runtime = SimRuntime::default();
    let config = SimStorageConfig {
        max_read_bytes: 8,
        max_write_bytes: 8,
        max_outstanding_bytes: 8,
        default_latency: SimDuration::from_nanos(5),
        ..SimStorageConfig::default()
    };
    let storage = open(&runtime, &SimDisk::default(), config);

    let abandoned = storage.submit_write_at(WriteAtRequest::new(0, vec![b'a'; 8]));
    assert_eq!(storage.status().outstanding_bytes, 8);
    drop(abandoned);
    assert_eq!(
        storage.status().outstanding_bytes,
        8,
        "dropping the response abandons delivery, not the admitted operation"
    );

    // A metadata operation charges nothing, so it is admissible against the
    // committed budget and drains the FIFO behind the abandoned write.
    runtime
        .block_on(storage.submit_sync())
        .expect("runtime completes")
        .expect("sync completes");
    assert_eq!(
        storage.status().outstanding_bytes,
        0,
        "the abandoned operation released its charge when it terminalized"
    );

    let later = storage.submit_write_at(WriteAtRequest::new(8, vec![b'b'; 8]));
    runtime
        .block_on(later)
        .expect("runtime completes")
        .expect("the whole budget is available again");
    assert_eq!(storage.status().outstanding_bytes, 0);
}

#[test]
fn a_byte_budget_below_a_transfer_maximum_is_rejected_at_open() {
    let runtime = SimRuntime::default();
    let disk = SimDisk::default();

    let below_read = SimStorageConfig {
        max_read_bytes: 64,
        max_outstanding_bytes: 32,
        ..SimStorageConfig::default()
    };
    assert!(matches!(
        disk.open(runtime.handle(), below_read),
        Err(SimOpenError::InvalidConfig(
            "max_outstanding_bytes is below max_read_bytes"
        ))
    ));

    let below_write = SimStorageConfig {
        max_read_bytes: 16,
        max_write_bytes: 64,
        max_outstanding_bytes: 32,
        ..SimStorageConfig::default()
    };
    assert!(matches!(
        disk.open(runtime.handle(), below_write),
        Err(SimOpenError::InvalidConfig(
            "max_outstanding_bytes is below max_write_bytes"
        ))
    ));
}

#[test]
fn the_default_byte_budget_admits_the_default_in_flight_maximum() {
    let runtime = SimRuntime::default();
    let config = SimStorageConfig::default();
    let storage = open(&runtime, &SimDisk::default(), config);

    // The default budget is derived from the other defaults' worst case, so it
    // must not bind before max_in_flight does.
    let per_operation = config.max_read_bytes.max(config.max_write_bytes);
    assert_eq!(
        config.max_outstanding_bytes,
        config.max_in_flight * per_operation
    );
    assert_eq!(storage.status().outstanding_bytes_limit, 8 * 1_024 * 1_024);
}

#[test]
fn scripted_latency_advances_only_virtual_time() {
    let mut runtime = SimRuntime::default();
    let storage = open(&runtime, &SimDisk::default(), SimStorageConfig::default());
    storage
        .inject(SimFault::new(
            StorageOperation::WriteAt,
            SimDuration::from_nanos(17),
            SimOutcome::Success,
        ))
        .expect("inject latency");

    runtime
        .block_on(storage.submit_write_at(WriteAtRequest::new(0, vec![1])))
        .expect("runtime completes")
        .expect("write succeeds");

    assert_eq!(runtime.snapshot().now.as_nanos(), 17);
}

#[test]
fn runtime_shutdown_terminalizes_storage_operations_and_releases_the_disk() {
    for worker_started in [false, true] {
        let mut runtime = SimRuntime::default();
        let disk = SimDisk::default();
        let config = SimStorageConfig {
            default_latency: SimDuration::from_nanos(100),
            ..SimStorageConfig::default()
        };
        let storage = open(&runtime, &disk, config);
        let mut write =
            Box::pin(storage.submit_write_at(WriteAtRequest::new(0, b"runtime-stopped".to_vec())));
        let mut sync = Box::pin(storage.submit_sync());

        if worker_started {
            runtime
                .step()
                .expect("storage worker registers its completion timer");
        }
        assert_eq!(storage.status().in_flight, 2);

        runtime.shutdown().expect("runtime shuts down cleanly");

        let status = storage.status();
        assert!(status.closed);
        // Shutdown terminalizes both operations, but their admissions are
        // released by consuming the responses, not by termination: until the
        // caller takes each rejection back — buffer included — the
        // reservation it holds is still real.
        assert_eq!(status.in_flight, 2);
        let mut context = Context::from_waker(Waker::noop());
        let Poll::Ready(Err(write_error)) = write.as_mut().poll(&mut context) else {
            panic!("shutdown must terminalize an admitted write");
        };
        assert_eq!(write_error.certainty(), CompletionCertainty::NotApplied);
        assert_eq!(write_error.error().error, StorageError::RuntimeStopped);
        assert_eq!(write_error.error().buffer, b"runtime-stopped");
        let Poll::Ready(Err(sync_error)) = sync.as_mut().poll(&mut context) else {
            panic!("shutdown must terminalize a queued sync");
        };
        assert_eq!(sync_error.certainty(), CompletionCertainty::NotApplied);
        assert_eq!(sync_error.error(), &StorageError::RuntimeStopped);

        // Consuming both rejections returned the buffers and with them the
        // admissions they carried.
        let drained = storage.status();
        assert_eq!(drained.in_flight, 0);
        assert_eq!(drained.outstanding_bytes, 0);

        let mut replacement_runtime = SimRuntime::default();
        let replacement = open(&replacement_runtime, &disk, config);
        assert!(!replacement.status().closed);
        replacement.crash();
        replacement_runtime
            .shutdown()
            .expect("replacement runtime shuts down");
    }
}

struct PanicWake;

struct PanicPayload;

impl Drop for PanicPayload {
    fn drop(&mut self) {
        panic!("intentional storage response wake payload destructor panic");
    }
}

impl Wake for PanicWake {
    fn wake(self: Arc<Self>) {
        std::panic::panic_any(PanicPayload);
    }
}

#[test]
fn panicking_response_waker_and_payload_drop_do_not_kill_the_storage_worker() {
    let mut runtime = SimRuntime::default();
    let storage = open(&runtime, &SimDisk::default(), SimStorageConfig::default());
    let mut first = Box::pin(storage.submit_len());
    let panic_waker = Waker::from(Arc::new(PanicWake));
    let mut panic_context = Context::from_waker(&panic_waker);
    assert!(first.as_mut().poll(&mut panic_context).is_pending());

    runtime
        .step()
        .expect("response waker panic is contained by the storage worker");

    let mut context = Context::from_waker(Waker::noop());
    let Poll::Ready(Ok(length)) = first.as_mut().poll(&mut context) else {
        panic!("first operation must retain its terminal response");
    };
    assert_eq!(length.len, 0);
    runtime
        .block_on(storage.submit_len())
        .expect("runtime drives a later storage operation")
        .expect("storage worker remains live after the wake panic");
}

#[test]
fn fault_diagnostics_count_plans_when_operations_are_admitted() {
    let mut runtime = SimRuntime::default();
    let storage = open(&runtime, &SimDisk::default(), SimStorageConfig::default());

    storage
        .inject(SimFault::new(
            StorageOperation::WriteAt,
            SimDuration::ZERO,
            SimOutcome::FailBefore,
        ))
        .expect("inject write fault");
    storage
        .inject(SimFault::new(
            StorageOperation::ReadAt,
            SimDuration::ZERO,
            SimOutcome::MayHaveAppliedBefore,
        ))
        .expect("inject read fault");
    assert_eq!(storage.status().pending_faults, 2);
    assert_eq!(storage.status().fault_hits, 0);

    let write = storage.submit_write_at(WriteAtRequest::new(0, b"write".to_vec()));
    assert_eq!(storage.status().pending_faults, 1);
    assert_eq!(storage.status().fault_hits, 1);
    let read = storage.submit_read_at(ReadAtRequest::new(0, vec![0; 5]));
    assert_eq!(storage.status().pending_faults, 0);
    assert_eq!(storage.status().fault_hits, 2);

    runtime
        .block_on(write)
        .expect("runtime completes")
        .expect_err("write fault fires");
    runtime
        .block_on(read)
        .expect("runtime completes")
        .expect_err("read fault fires");
    assert_eq!(storage.status().fault_hits, 2);
}

#[test]
fn short_transfers_are_deterministic_and_reported() {
    let mut runtime = SimRuntime::default();
    let config = SimStorageConfig {
        max_read_chunk: 1,
        max_write_chunk: 2,
        ..SimStorageConfig::default()
    };
    let storage = open(&runtime, &SimDisk::default(), config);

    let write = runtime
        .block_on(storage.submit_write_at(WriteAtRequest::new(0, b"abcd".to_vec())))
        .expect("runtime completes")
        .expect("write succeeds");
    assert_eq!(write.bytes_written, 2);
    assert_eq!(write.buffer, b"abcd");

    let read = runtime
        .block_on(storage.submit_read_at(ReadAtRequest::new(0, vec![0; 4])))
        .expect("runtime completes")
        .expect("read succeeds");
    assert_eq!(read.bytes_read, 1);
    assert_eq!(read.buffer, b"a");

    storage
        .inject(
            SimFault::new(
                StorageOperation::WriteAt,
                SimDuration::ZERO,
                SimOutcome::Success,
            )
            .with_max_bytes(0),
        )
        .expect("inject zero transfer");
    let zero = runtime
        .block_on(storage.submit_write_at(WriteAtRequest::new(8, b"z".to_vec())))
        .expect("runtime completes")
        .expect("zero-byte write is explicitly reported");
    assert_eq!(zero.bytes_written, 0);
    assert_eq!(
        storage.status().accepted_len,
        2,
        "a zero-byte transfer beyond EOF must not extend the file"
    );
}

#[test]
fn scripted_certainty_matches_whether_the_effect_ran() {
    let mut runtime = SimRuntime::default();
    let disk = SimDisk::default();
    let storage = open(&runtime, &disk, SimStorageConfig::default());

    storage
        .inject(SimFault::new(
            StorageOperation::WriteAt,
            SimDuration::ZERO,
            SimOutcome::FailBefore,
        ))
        .expect("inject before fault");
    let before = runtime
        .block_on(storage.submit_write_at(WriteAtRequest::new(0, b"a".to_vec())))
        .expect("runtime completes")
        .expect_err("fault fires");
    assert_eq!(before.certainty(), CompletionCertainty::NotApplied);
    assert_eq!(before.error().bytes_transferred, 0);
    assert_eq!(before.error().buffer, b"a");
    assert_eq!(storage.status().accepted_len, 0);

    storage
        .inject(
            SimFault::new(
                StorageOperation::WriteAt,
                SimDuration::ZERO,
                SimOutcome::MayHaveAppliedBefore,
            )
            .with_max_bytes(1),
        )
        .expect("inject ambiguous before fault");
    let ambiguous_before = runtime
        .block_on(storage.submit_write_at(WriteAtRequest::new(0, b"z".to_vec())))
        .expect("runtime completes")
        .expect_err("fault fires");
    assert_eq!(
        ambiguous_before.certainty(),
        CompletionCertainty::MayHaveApplied
    );
    assert_eq!(ambiguous_before.error().bytes_transferred, 0);
    assert_eq!(storage.status().accepted_len, 0);

    storage
        .inject(
            SimFault::new(
                StorageOperation::WriteAt,
                SimDuration::ZERO,
                SimOutcome::FailAfter,
            )
            .with_max_bytes(2),
        )
        .expect("inject applied partial-write fault");
    let applied = runtime
        .block_on(storage.submit_write_at(WriteAtRequest::new(0, b"abcd".to_vec())))
        .expect("runtime completes")
        .expect_err("fault fires after partial write");
    assert_eq!(applied.certainty(), CompletionCertainty::Applied);
    assert_eq!(applied.error().bytes_transferred, 2);
    assert_eq!(applied.error().buffer, b"abcd");
    assert_eq!(storage.status().accepted_len, 2);

    storage
        .inject(
            SimFault::new(
                StorageOperation::WriteAt,
                SimDuration::ZERO,
                SimOutcome::MayHaveAppliedAfter,
            )
            .with_max_bytes(2),
        )
        .expect("inject ambiguous partial-write fault");
    let ambiguous_after = runtime
        .block_on(storage.submit_write_at(WriteAtRequest::new(2, b"efgh".to_vec())))
        .expect("runtime completes")
        .expect_err("fault fires after partial write");
    assert_eq!(
        ambiguous_after.certainty(),
        CompletionCertainty::MayHaveApplied
    );
    assert_eq!(ambiguous_after.error().bytes_transferred, 2);
    assert_eq!(storage.status().accepted_len, 4);

    storage
        .inject(SimFault::new(
            StorageOperation::ReadAt,
            SimDuration::ZERO,
            SimOutcome::MayHaveAppliedBefore,
        ))
        .expect("inject ambiguous before-read fault");
    let read_before = runtime
        .block_on(storage.submit_read_at(ReadAtRequest::new(0, vec![0; 4])))
        .expect("runtime completes")
        .expect_err("read fault fires before transfer");
    assert_eq!(read_before.certainty(), CompletionCertainty::MayHaveApplied);
    assert_eq!(read_before.error().bytes_transferred, 0);
    assert_eq!(read_before.error().buffer, vec![0; 4]);

    storage
        .inject(
            SimFault::new(
                StorageOperation::ReadAt,
                SimDuration::ZERO,
                SimOutcome::FailAfter,
            )
            .with_max_bytes(3),
        )
        .expect("inject applied after-read fault");
    let read_after = runtime
        .block_on(storage.submit_read_at(ReadAtRequest::new(0, vec![0; 4])))
        .expect("runtime completes")
        .expect_err("read fault fires after transfer");
    assert_eq!(read_after.certainty(), CompletionCertainty::Applied);
    assert_eq!(read_after.error().bytes_transferred, 3);
    assert_eq!(read_after.error().buffer, b"abe");

    storage
        .inject(
            SimFault::new(
                StorageOperation::ReadAt,
                SimDuration::ZERO,
                SimOutcome::MayHaveAppliedAfter,
            )
            .with_max_bytes(2),
        )
        .expect("inject ambiguous after-read fault");
    let read_ambiguous_after = runtime
        .block_on(storage.submit_read_at(ReadAtRequest::new(0, vec![0; 4])))
        .expect("runtime completes")
        .expect_err("read fault fires after transfer");
    assert_eq!(
        read_ambiguous_after.certainty(),
        CompletionCertainty::MayHaveApplied
    );
    assert_eq!(read_ambiguous_after.error().bytes_transferred, 2);
    assert_eq!(read_ambiguous_after.error().buffer, b"ab");

    storage
        .inject(SimFault::new(
            StorageOperation::Sync,
            SimDuration::ZERO,
            SimOutcome::FailAfter,
        ))
        .expect("inject after-sync fault");
    let sync = runtime
        .block_on(storage.submit_sync())
        .expect("runtime completes")
        .expect_err("sync response fails after effect");
    assert_eq!(sync.certainty(), CompletionCertainty::Applied);
    assert_eq!(disk.durable_bytes(), b"abef");
    assert_eq!(storage.status().pending_faults, 0);
    assert_eq!(storage.status().fault_hits, 8);
}

#[test]
fn ambiguous_failed_fsync_can_exclude_old_dirty_pages_from_a_later_success() {
    let mut runtime = SimRuntime::default();
    let original = vec![b'a'; 2 * SIM_FSYNC_PAGE_BYTES];
    let disk = SimDisk::from_durable_bytes(original.clone());
    let storage = open(&runtime, &disk, SimStorageConfig::default());

    runtime
        .block_on(storage.submit_write_at(WriteAtRequest::new(0, vec![b'b'])))
        .expect("runtime completes")
        .expect("dirty first page");
    runtime
        .block_on(
            storage.submit_write_at(WriteAtRequest::new(SIM_FSYNC_PAGE_BYTES as u64, vec![b'c'])),
        )
        .expect("runtime completes")
        .expect("dirty second page");
    storage
        .inject(
            SimFault::new(
                StorageOperation::Sync,
                SimDuration::ZERO,
                SimOutcome::MayHaveAppliedBefore,
            )
            .with_fsync_failure(SimFsyncFailure::ExcludeDirtyPagesV1),
        )
        .expect("script explicit failed-fsync page result");

    let failure = runtime
        .block_on(storage.submit_sync())
        .expect("runtime completes")
        .expect_err("scripted fsync fails");
    assert_eq!(failure.certainty(), CompletionCertainty::MayHaveApplied);
    assert_eq!(disk.durable_bytes(), original);
    assert_eq!(storage.status().fsync_gate_version, SIM_FSYNC_GATE_VERSION);
    assert!(storage.status().has_fsync_gated_data);

    runtime
        .block_on(storage.submit_write_at(WriteAtRequest::new(1, vec![b'x'])))
        .expect("runtime completes")
        .expect("redirty first page");
    let sync = runtime
        .block_on(storage.submit_sync())
        .expect("runtime completes")
        .expect("later fsync succeeds");

    let mut expected_durable = original;
    expected_durable[0] = b'b';
    expected_durable[1] = b'x';
    assert_eq!(sync.durable_len, expected_durable.len() as u64);
    assert_eq!(disk.durable_bytes(), expected_durable);
    assert_eq!(disk.durable_bytes()[SIM_FSYNC_PAGE_BYTES], b'a');
    assert!(
        storage.status().has_fsync_gated_data,
        "the untouched second page remains accepted but excluded"
    );
}

#[test]
fn redirtying_an_old_page_does_not_ungate_failed_length_metadata() {
    let mut runtime = SimRuntime::default();
    let original = vec![b'a'; SIM_FSYNC_PAGE_BYTES];
    let disk = SimDisk::from_durable_bytes(original.clone());
    let storage = open(&runtime, &disk, SimStorageConfig::default());

    runtime
        .block_on(
            storage.submit_write_at(WriteAtRequest::new(SIM_FSYNC_PAGE_BYTES as u64, vec![b'e'])),
        )
        .expect("runtime completes")
        .expect("extend accepted file");
    storage
        .inject(
            SimFault::new(
                StorageOperation::Sync,
                SimDuration::ZERO,
                SimOutcome::MayHaveAppliedBefore,
            )
            .with_fsync_failure(SimFsyncFailure::ExcludeDirtyPagesV1),
        )
        .expect("script explicit failed-fsync page result");
    runtime
        .block_on(storage.submit_sync())
        .expect("runtime completes")
        .expect_err("scripted fsync fails");

    runtime
        .block_on(storage.submit_write_at(WriteAtRequest::new(0, vec![b'x'])))
        .expect("runtime completes")
        .expect("redirty pre-existing page");
    let sync = runtime
        .block_on(storage.submit_sync())
        .expect("runtime completes")
        .expect("later fsync succeeds");

    let mut expected = original;
    expected[0] = b'x';
    assert_eq!(sync.durable_len, SIM_FSYNC_PAGE_BYTES as u64);
    assert_eq!(disk.durable_bytes(), expected);
    assert_eq!(
        storage.status().accepted_len,
        (SIM_FSYNC_PAGE_BYTES + 1) as u64
    );
    assert!(storage.status().has_fsync_gated_data);
}

#[test]
fn failed_fsync_page_result_is_explicit_and_can_retain_dirty_pages() {
    let mut runtime = SimRuntime::default();
    let disk = SimDisk::default();
    let storage = open(&runtime, &disk, SimStorageConfig::default());

    assert_eq!(
        storage.inject(SimFault::new(
            StorageOperation::Sync,
            SimDuration::ZERO,
            SimOutcome::MayHaveAppliedBefore,
        )),
        Err(SimFaultError::MissingFsyncFailure)
    );
    assert_eq!(storage.status().pending_faults, 0);
    assert_eq!(
        storage.inject(
            SimFault::new(
                StorageOperation::WriteAt,
                SimDuration::ZERO,
                SimOutcome::MayHaveAppliedBefore,
            )
            .with_fsync_failure(SimFsyncFailure::RetainDirtyPagesV1),
        ),
        Err(SimFaultError::UnexpectedFsyncFailure)
    );
    assert_eq!(storage.status().pending_faults, 0);

    runtime
        .block_on(storage.submit_write_at(WriteAtRequest::new(0, b"dirty".to_vec())))
        .expect("runtime completes")
        .expect("write succeeds");
    storage
        .inject(
            SimFault::new(
                StorageOperation::Sync,
                SimDuration::ZERO,
                SimOutcome::MayHaveAppliedBefore,
            )
            .with_fsync_failure(SimFsyncFailure::RetainDirtyPagesV1),
        )
        .expect("script retain result");
    let failure = runtime
        .block_on(storage.submit_sync())
        .expect("runtime completes")
        .expect_err("scripted fsync fails");
    assert_eq!(failure.certainty(), CompletionCertainty::MayHaveApplied);
    assert!(!storage.status().has_fsync_gated_data);

    runtime
        .block_on(storage.submit_sync())
        .expect("runtime completes")
        .expect("retry succeeds");
    assert_eq!(disk.durable_bytes(), b"dirty");
}

#[test]
fn polling_then_abandoning_a_response_does_not_cancel_or_stale_wake() {
    let mut runtime = SimRuntime::default();
    let disk = SimDisk::default();
    let config = SimStorageConfig {
        default_latency: SimDuration::from_nanos(3),
        ..SimStorageConfig::default()
    };
    let storage = open(&runtime, &disk, config);
    let task_storage = storage.clone();

    runtime
        .block_on(async move {
            let mut abandoned =
                Box::pin(task_storage.submit_write_at(WriteAtRequest::new(0, b"written".to_vec())));
            poll_fn(|context| {
                assert!(abandoned.as_mut().poll(context).is_pending());
                Poll::Ready(())
            })
            .await;
            drop(abandoned);
            task_storage.submit_sync().await.expect("sync completes")
        })
        .expect("runtime completes");

    assert_eq!(disk.durable_bytes(), b"written");
}

#[test]
fn crash_fails_queued_operations_and_allows_immediate_reopen() {
    let mut runtime = SimRuntime::default();
    let disk = SimDisk::default();
    let config = SimStorageConfig {
        default_latency: SimDuration::from_nanos(100),
        ..SimStorageConfig::default()
    };
    let storage = open(&runtime, &disk, config);
    let write = storage.submit_write_at(WriteAtRequest::new(0, b"queued".to_vec()));
    let sync = storage.submit_sync();

    storage.crash();
    let write_error = runtime
        .block_on(write)
        .expect("runtime completes")
        .expect_err("queued write fails");
    assert_eq!(write_error.certainty(), CompletionCertainty::NotApplied);
    assert_eq!(write_error.error().buffer, b"queued");
    assert_eq!(write_error.error().bytes_transferred, 0);
    let sync_error = runtime
        .block_on(sync)
        .expect("runtime completes")
        .expect_err("queued sync fails");
    assert_eq!(sync_error.certainty(), CompletionCertainty::NotApplied);

    let reopened = open(&runtime, &disk, config);
    assert!(!reopened.status().closed);
}

#[test]
fn only_one_session_can_open_a_disk() {
    let runtime = SimRuntime::default();
    let disk = SimDisk::default();
    let storage = open(&runtime, &disk, SimStorageConfig::default());

    assert!(matches!(
        disk.open(runtime.handle(), SimStorageConfig::default()),
        Err(SimOpenError::AlreadyOpen)
    ));
    storage.crash();
    assert!(
        disk.open(runtime.handle(), SimStorageConfig::default())
            .is_ok()
    );
}

#[derive(Clone, Debug)]
enum ModelOp {
    Write {
        offset: usize,
        bytes: Vec<u8>,
        outcome: SimOutcome,
        max_bytes: usize,
    },
    SetLen(usize),
    Sync {
        outcome: SimOutcome,
        fsync_failure: Option<SimFsyncFailure>,
    },
    Crash,
    Read,
}

fn generated_ops(mut seed: u64, count: usize) -> Vec<ModelOp> {
    let mut operations = Vec::with_capacity(count);
    for _ in 0..count {
        seed = seed
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        operations.push(match seed % 8 {
            0..=3 => {
                let length = ((seed >> 8) % 4 + 1) as usize;
                let first = (seed >> 24) as u8;
                let bytes = (0..length)
                    .map(|index| first.wrapping_add(index as u8))
                    .collect();
                let outcome = match (seed >> 48) % 5 {
                    0 => SimOutcome::Success,
                    1 => SimOutcome::FailBefore,
                    2 => SimOutcome::FailAfter,
                    3 => SimOutcome::MayHaveAppliedBefore,
                    _ => SimOutcome::MayHaveAppliedAfter,
                };
                ModelOp::Write {
                    offset: ((seed >> 16) % 16) as usize,
                    bytes,
                    outcome,
                    max_bytes: ((seed >> 40) % (length as u64 + 1)) as usize,
                }
            }
            4 => ModelOp::SetLen(((seed >> 8) % 17) as usize),
            5 => match (seed >> 48) % 6 {
                0 => ModelOp::Sync {
                    outcome: SimOutcome::Success,
                    fsync_failure: None,
                },
                1 => ModelOp::Sync {
                    outcome: SimOutcome::FailBefore,
                    fsync_failure: None,
                },
                2 => ModelOp::Sync {
                    outcome: SimOutcome::FailAfter,
                    fsync_failure: None,
                },
                3 => ModelOp::Sync {
                    outcome: SimOutcome::MayHaveAppliedBefore,
                    fsync_failure: Some(SimFsyncFailure::RetainDirtyPagesV1),
                },
                4 => ModelOp::Sync {
                    outcome: SimOutcome::MayHaveAppliedBefore,
                    fsync_failure: Some(SimFsyncFailure::ExcludeDirtyPagesV1),
                },
                _ => ModelOp::Sync {
                    outcome: SimOutcome::MayHaveAppliedAfter,
                    fsync_failure: None,
                },
            },
            6 => ModelOp::Crash,
            _ => ModelOp::Read,
        });
    }
    operations
}

fn outcome_index(outcome: SimOutcome) -> usize {
    match outcome {
        SimOutcome::Success => 0,
        SimOutcome::FailBefore => 1,
        SimOutcome::FailAfter => 2,
        SimOutcome::MayHaveAppliedBefore => 3,
        SimOutcome::MayHaveAppliedAfter => 4,
    }
}

fn outcome_applies(outcome: SimOutcome) -> bool {
    matches!(
        outcome,
        SimOutcome::Success | SimOutcome::FailAfter | SimOutcome::MayHaveAppliedAfter
    )
}

fn outcome_certainty(outcome: SimOutcome) -> Option<CompletionCertainty> {
    match outcome {
        SimOutcome::Success => None,
        SimOutcome::FailBefore => Some(CompletionCertainty::NotApplied),
        SimOutcome::FailAfter => Some(CompletionCertainty::Applied),
        SimOutcome::MayHaveAppliedBefore | SimOutcome::MayHaveAppliedAfter => {
            Some(CompletionCertainty::MayHaveApplied)
        }
    }
}

fn model_write(
    accepted: &mut Vec<u8>,
    sync_candidate: &mut Vec<u8>,
    sync_candidate_len: &mut usize,
    offset: usize,
    bytes: &[u8],
) {
    if bytes.is_empty() {
        return;
    }
    let end = offset + bytes.len();
    let previous_len = accepted.len();
    accepted.resize(accepted.len().max(end), 0);
    accepted[offset..end].copy_from_slice(bytes);
    if end > previous_len {
        *sync_candidate_len = accepted.len();
    }
    sync_candidate.resize(accepted.len().max(*sync_candidate_len), 0);
    let first_page = offset / SIM_FSYNC_PAGE_BYTES;
    let last_page = (end - 1) / SIM_FSYNC_PAGE_BYTES;
    for page in first_page..=last_page {
        let start = page * SIM_FSYNC_PAGE_BYTES;
        let end = (start + SIM_FSYNC_PAGE_BYTES).min(accepted.len());
        sync_candidate[start..end].copy_from_slice(&accepted[start..end]);
    }
}

fn model_materialize_sync_candidate(candidate: &[u8], candidate_len: usize) -> Vec<u8> {
    let mut image = candidate.to_vec();
    image.resize(candidate_len, 0);
    image.truncate(candidate_len);
    image
}

fn model_has_fsync_gated_data(accepted: &[u8], candidate: &[u8], candidate_len: usize) -> bool {
    accepted.len() != candidate_len
        || candidate.len() < accepted.len()
        || accepted != &candidate[..accepted.len()]
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ByteVectorImage {
    Accepted,
    Durable,
    Read,
}

#[derive(Debug, Eq, PartialEq)]
struct ByteVectorMismatch {
    image: ByteVectorImage,
    expected: Vec<u8>,
    observed: Vec<u8>,
}

fn check_byte_vector_observation(
    expected_accepted: &[u8],
    expected_durable: &[u8],
    observed_accepted: &[u8],
    observed_durable: &[u8],
    observed_read: Option<&[u8]>,
) -> Result<(), ByteVectorMismatch> {
    for (image, expected, observed) in [
        (
            ByteVectorImage::Accepted,
            expected_accepted,
            observed_accepted,
        ),
        (ByteVectorImage::Durable, expected_durable, observed_durable),
    ] {
        if expected != observed {
            return Err(ByteVectorMismatch {
                image,
                expected: expected.to_vec(),
                observed: observed.to_vec(),
            });
        }
    }

    if let Some(observed_read) = observed_read
        && expected_accepted != observed_read
    {
        return Err(ByteVectorMismatch {
            image: ByteVectorImage::Read,
            expected: expected_accepted.to_vec(),
            observed: observed_read.to_vec(),
        });
    }
    Ok(())
}

#[test]
fn byte_vector_oracle_rejects_mutated_observations() {
    let accepted = b"accepted";
    let durable = b"durable";

    let stale_accepted =
        check_byte_vector_observation(accepted, durable, b"accepte", durable, Some(accepted))
            .expect_err("stale accepted image must be rejected");
    assert_eq!(stale_accepted.image, ByteVectorImage::Accepted);

    let corrupt_durable =
        check_byte_vector_observation(accepted, durable, accepted, b"durXble", Some(accepted))
            .expect_err("corrupt durable image must be rejected");
    assert_eq!(corrupt_durable.image, ByteVectorImage::Durable);

    let stale_read =
        check_byte_vector_observation(accepted, durable, accepted, durable, Some(b"accepte"))
            .expect_err("stale read image must be rejected");
    assert_eq!(stale_read.image, ByteVectorImage::Read);
}

#[test]
fn generated_crash_traces_match_a_boring_byte_vector_model() {
    let mut outcome_coverage = [0_usize; 5];
    let mut fsync_coverage = [0_usize; 6];
    let mut multi_byte_writes = 0;
    let mut partial_writes = 0;
    let mut observed_fault_hits = 0;

    for seed in 0..16 {
        let operations = generated_ops(seed, 64);
        let expected_fault_hits = operations
            .iter()
            .filter(|operation| matches!(operation, ModelOp::Write { .. } | ModelOp::Sync { .. }))
            .count();
        for operation in &operations {
            if let ModelOp::Write {
                bytes,
                outcome,
                max_bytes,
                ..
            } = operation
            {
                outcome_coverage[outcome_index(*outcome)] += 1;
                multi_byte_writes += usize::from(bytes.len() > 1);
                partial_writes += usize::from(*max_bytes < bytes.len());
            }
            if let ModelOp::Sync {
                outcome,
                fsync_failure,
            } = operation
            {
                let case = match (*outcome, *fsync_failure) {
                    (SimOutcome::Success, None) => 0,
                    (SimOutcome::FailBefore, None) => 1,
                    (SimOutcome::FailAfter, None) => 2,
                    (
                        SimOutcome::MayHaveAppliedBefore,
                        Some(SimFsyncFailure::RetainDirtyPagesV1),
                    ) => 3,
                    (
                        SimOutcome::MayHaveAppliedBefore,
                        Some(SimFsyncFailure::ExcludeDirtyPagesV1),
                    ) => 4,
                    (SimOutcome::MayHaveAppliedAfter, None) => 5,
                    combination => panic!("invalid generated fsync case: {combination:?}"),
                };
                fsync_coverage[case] += 1;
            }
        }
        let mut runtime = SimRuntime::default();
        let disk = SimDisk::default();
        let config = SimStorageConfig::default();
        let handle = runtime.handle();
        let initial = open(&runtime, &disk, config);

        let seed_fault_hits = runtime
            .block_on(async move {
                let mut storage = initial;
                let mut accepted = Vec::<u8>::new();
                let mut sync_candidate = Vec::<u8>::new();
                let mut sync_candidate_len = 0_usize;
                let mut durable = Vec::<u8>::new();
                let mut assigned_faults = 0;
                for (step, operation) in operations.iter().enumerate() {
                    match operation {
                        ModelOp::Write {
                            offset,
                            bytes,
                            outcome,
                            max_bytes,
                        } => {
                            storage
                                .inject(
                                    SimFault::new(
                                        StorageOperation::WriteAt,
                                        SimDuration::ZERO,
                                        *outcome,
                                    )
                                    .with_max_bytes(*max_bytes),
                                )
                                .unwrap_or_else(|error| {
                                    panic!(
                                        "seed={seed} step={step} inject for {operation:?}: {error}"
                                    )
                                });
                            assert_eq!(
                                storage.status().pending_faults,
                                1,
                                "seed={seed} step={step} before {operation:?}"
                            );
                            let previous_hits = storage.status().fault_hits;
                            let write = storage.submit_write_at(WriteAtRequest::new(
                                *offset as u64,
                                bytes.clone(),
                            ));
                            assert_eq!(
                                storage.status().pending_faults,
                                0,
                                "seed={seed} step={step} after admission of {operation:?}"
                            );
                            assert_eq!(
                                storage.status().fault_hits,
                                previous_hits + 1,
                                "seed={seed} step={step} after admission of {operation:?}"
                            );
                            assigned_faults += 1;

                            let transferred = (*max_bytes).min(bytes.len());
                            let completion = write.await;
                            match outcome_certainty(*outcome) {
                                None => {
                                    let success = completion.unwrap_or_else(|error| {
                                        panic!("seed={seed} step={step} op={operation:?}: {error}")
                                    });
                                    assert_eq!(success.bytes_written, transferred);
                                    assert_eq!(success.buffer, *bytes);
                                }
                                Some(certainty) => {
                                    let failure = completion.expect_err(&format!(
                                        "seed={seed} step={step} op={operation:?} should fail"
                                    ));
                                    assert_eq!(failure.certainty(), certainty);
                                    assert_eq!(failure.error().buffer, *bytes);
                                    assert_eq!(
                                        failure.error().bytes_transferred,
                                        if outcome_applies(*outcome) {
                                            transferred
                                        } else {
                                            0
                                        }
                                    );
                                }
                            }

                            if outcome_applies(*outcome) && transferred != 0 {
                                model_write(
                                    &mut accepted,
                                    &mut sync_candidate,
                                    &mut sync_candidate_len,
                                    *offset,
                                    &bytes[..transferred],
                                );
                            }
                        }
                        ModelOp::SetLen(len) => {
                            storage
                                .submit_set_len(*len as u64)
                                .await
                                .unwrap_or_else(|error| {
                                    panic!("seed={seed} step={step} op={operation:?}: {error}")
                                });
                            accepted.resize(*len, 0);
                            sync_candidate_len = *len;
                            sync_candidate.resize(*len, 0);
                        }
                        ModelOp::Sync {
                            outcome,
                            fsync_failure,
                        } => {
                            let mut fault =
                                SimFault::new(StorageOperation::Sync, SimDuration::ZERO, *outcome);
                            if let Some(fsync_failure) = fsync_failure {
                                fault = fault.with_fsync_failure(*fsync_failure);
                            }
                            storage.inject(fault).unwrap_or_else(|error| {
                                panic!("seed={seed} step={step} inject for {operation:?}: {error}")
                            });
                            assigned_faults += 1;
                            let completion = storage.submit_sync().await;
                            match outcome_certainty(*outcome) {
                                None => {
                                    completion.unwrap_or_else(|error| {
                                        panic!("seed={seed} step={step} op={operation:?}: {error}")
                                    });
                                }
                                Some(certainty) => {
                                    let failure = completion.expect_err(&format!(
                                        "seed={seed} step={step} op={operation:?} should fail"
                                    ));
                                    assert_eq!(failure.certainty(), certainty);
                                }
                            }
                            if outcome_applies(*outcome) {
                                durable = model_materialize_sync_candidate(
                                    &sync_candidate,
                                    sync_candidate_len,
                                );
                            } else if *fsync_failure == Some(SimFsyncFailure::ExcludeDirtyPagesV1) {
                                sync_candidate.clone_from(&durable);
                                sync_candidate_len = durable.len();
                            }
                        }
                        ModelOp::Crash => {
                            storage.crash();
                            accepted.clone_from(&durable);
                            sync_candidate.clone_from(&durable);
                            sync_candidate_len = durable.len();
                            storage = disk.open(handle.clone(), config).unwrap_or_else(|error| {
                                panic!("seed={seed} step={step} reopen: {error}")
                            });
                        }
                        ModelOp::Read => {}
                    }
                    let len = storage.submit_len().await.unwrap_or_else(|error| {
                        panic!("seed={seed} step={step} len after {operation:?}: {error}")
                    });
                    assert_eq!(
                        len.len as usize,
                        accepted.len(),
                        "seed={seed} step={step} op={operation:?}"
                    );
                    assert_eq!(
                        storage.status().has_fsync_gated_data,
                        model_has_fsync_gated_data(&accepted, &sync_candidate, sync_candidate_len),
                        "seed={seed} step={step} gate state after {operation:?}"
                    );
                    let observed_accepted = storage
                        .submit_read_at(ReadAtRequest::new(0, vec![0; 32]))
                        .await
                        .unwrap_or_else(|error| {
                            panic!("seed={seed} step={step} observe after {operation:?}: {error}")
                        });
                    let observed_durable = disk.durable_bytes();
                    check_byte_vector_observation(
                        &accepted,
                        &durable,
                        &observed_accepted.buffer,
                        &observed_durable,
                        matches!(operation, ModelOp::Read)
                            .then_some(observed_accepted.buffer.as_slice()),
                    )
                    .unwrap_or_else(|mismatch| {
                        panic!(
                            "seed={seed} step={step} op={operation:?} oracle mismatch: {mismatch:?}"
                        )
                    });
                }
                assigned_faults
            })
            .unwrap_or_else(|error| panic!("seed={seed} runtime failure: {error}"));
        assert_eq!(seed_fault_hits, expected_fault_hits, "seed={seed}");
        observed_fault_hits += seed_fault_hits;
    }

    assert!(
        outcome_coverage.iter().all(|hits| *hits > 0),
        "generated write outcomes were not all exercised: {outcome_coverage:?}"
    );
    assert!(
        fsync_coverage.iter().all(|hits| *hits > 0),
        "generated fsync outcomes were not all exercised: {fsync_coverage:?}"
    );
    assert!(multi_byte_writes > 0, "no multi-byte write was exercised");
    assert!(partial_writes > 0, "no partial write was exercised");
    assert!(observed_fault_hits > 0, "no scripted fault was consumed");
}

// --- Commuting-overlap pipeline model -------------------------------------
//
// `SimPipelineModel::CommutingOverlapV1` mirrors the Linux file pipelines:
// commuting operations admitted together start together and complete after
// their own latencies, so completion order can differ from admission order
// while fences and effect equivalence are unchanged.

fn overlap_config() -> SimStorageConfig {
    SimStorageConfig {
        pipeline_model: SimPipelineModel::CommutingOverlapV1,
        ..SimStorageConfig::default()
    }
}

/// Spawns a recorder task that awaits `operation` and records `(label, now)`.
fn record_completion<F, T, E>(
    runtime: &SimRuntime,
    order: &std::rc::Rc<std::cell::RefCell<Vec<(u8, u64)>>>,
    label: u8,
    operation: F,
) where
    F: std::future::Future<Output = Result<T, E>> + 'static,
    E: std::fmt::Debug,
{
    let order = std::rc::Rc::clone(order);
    let handle = runtime.handle();
    let clock = handle.clone();
    handle
        .spawn(async move {
            operation.await.expect("recorded operation succeeds");
            order.borrow_mut().push((label, clock.now().as_nanos()));
        })
        .expect("spawn completion recorder");
}

#[test]
fn the_pipeline_model_version_is_pinned() {
    assert_eq!(SIM_PIPELINE_MODEL_VERSION, 1);
}

#[test]
fn overlapped_reads_complete_in_latency_order_at_exact_instants() {
    let mut runtime = SimRuntime::default();
    let disk = SimDisk::from_durable_bytes(b"abcdef".to_vec());
    let storage = open(&runtime, &disk, overlap_config());
    for delay in [10, 5] {
        storage
            .inject(SimFault::new(
                StorageOperation::ReadAt,
                SimDuration::from_nanos(delay),
                SimOutcome::Success,
            ))
            .expect("script an exact read delay");
    }

    let order = std::rc::Rc::new(std::cell::RefCell::new(Vec::new()));
    let first = storage.submit_read_at(ReadAtRequest::new(0, vec![0; 3]));
    let second = storage.submit_read_at(ReadAtRequest::new(3, vec![0; 3]));
    record_completion(&runtime, &order, 0, first);
    record_completion(&runtime, &order, 1, second);

    runtime.block_on(async {}).expect("root completes");
    runtime.run_until_stalled().expect("both reads complete");
    assert_eq!(
        *order.borrow(),
        vec![(1, 5), (0, 10)],
        "the shorter-latency read must overtake the earlier-admitted one"
    );
    assert_eq!(storage.status().reordered_completions, 1);
}

#[test]
fn overlapped_nonoverlapping_writes_reorder_and_a_sync_fences_them() {
    let mut runtime = SimRuntime::default();
    let disk = SimDisk::default();
    let storage = open(&runtime, &disk, overlap_config());
    for delay in [10, 5] {
        storage
            .inject(SimFault::new(
                StorageOperation::WriteAt,
                SimDuration::from_nanos(delay),
                SimOutcome::Success,
            ))
            .expect("script an exact write delay");
    }

    // Both responses are dropped: the effects and the fence must not depend
    // on anyone polling the write completions.
    drop(storage.submit_write_at(WriteAtRequest::new(0, b"abc".to_vec())));
    drop(storage.submit_write_at(WriteAtRequest::new(4, b"xy".to_vec())));
    let synced = runtime
        .block_on(storage.submit_sync())
        .expect("runtime completes")
        .expect("sync succeeds");

    assert_eq!(synced.durable_len, 6);
    assert_eq!(disk.durable_bytes(), b"abc\0xy");
    assert_eq!(
        runtime.snapshot().now.as_nanos(),
        10,
        "the sync fence waits for the slowest overlapped write, not their sum"
    );
    assert_eq!(storage.status().reordered_completions, 1);
}

#[test]
fn overlapping_writes_never_overlap_and_apply_in_admission_order() {
    let mut runtime = SimRuntime::default();
    let disk = SimDisk::default();
    let storage = open(&runtime, &disk, overlap_config());
    for delay in [10, 3] {
        storage
            .inject(SimFault::new(
                StorageOperation::WriteAt,
                SimDuration::from_nanos(delay),
                SimOutcome::Success,
            ))
            .expect("script an exact write delay");
    }

    let order = std::rc::Rc::new(std::cell::RefCell::new(Vec::new()));
    let first = storage.submit_write_at(WriteAtRequest::new(0, b"aaaa".to_vec()));
    let second = storage.submit_write_at(WriteAtRequest::new(2, b"bbbb".to_vec()));
    record_completion(&runtime, &order, 0, first);
    record_completion(&runtime, &order, 1, second);

    runtime.block_on(async {}).expect("root completes");
    runtime.run_until_stalled().expect("both writes complete");
    assert_eq!(
        *order.borrow(),
        vec![(0, 10), (1, 13)],
        "an overlapping write is a fence: its latency starts after the prefix drains"
    );
    assert_eq!(storage.status().reordered_completions, 0);

    let read = runtime
        .block_on(storage.submit_read_at(ReadAtRequest::new(0, vec![0; 6])))
        .expect("runtime completes")
        .expect("read succeeds");
    assert_eq!(read.buffer, b"aabbbb");
}

#[test]
fn metadata_operations_fence_the_commuting_prefix() {
    let mut runtime = SimRuntime::default();
    let storage = open(&runtime, &SimDisk::default(), overlap_config());
    storage
        .inject(SimFault::new(
            StorageOperation::WriteAt,
            SimDuration::from_nanos(10),
            SimOutcome::Success,
        ))
        .expect("script an exact write delay");

    drop(storage.submit_write_at(WriteAtRequest::new(0, b"abcd".to_vec())));
    let length = runtime
        .block_on(storage.submit_len())
        .expect("runtime completes")
        .expect("len succeeds");

    assert_eq!(length.len, 4, "len observes the fenced write's effect");
    assert_eq!(
        runtime.snapshot().now.as_nanos(),
        10,
        "len waits for the admitted write instead of joining its batch"
    );
}

#[test]
fn a_crash_mid_batch_terminalizes_the_unexecuted_suffix_as_closed() {
    let mut runtime = SimRuntime::default();
    let disk = SimDisk::default();
    let storage = open(&runtime, &disk, overlap_config());
    for delay in [5, 10] {
        storage
            .inject(SimFault::new(
                StorageOperation::WriteAt,
                SimDuration::from_nanos(delay),
                SimOutcome::Success,
            ))
            .expect("script an exact write delay");
    }

    let first = storage.submit_write_at(WriteAtRequest::new(0, b"abc".to_vec()));
    let second = storage.submit_write_at(WriteAtRequest::new(4, b"xy".to_vec()));
    let crasher = storage.clone();
    let handle = runtime.handle();
    let sleeper = handle.clone();
    handle
        .spawn(async move {
            sleeper
                .sleep(SimDuration::from_nanos(7))
                .await
                .expect("crash task sleep completes");
            crasher.crash();
        })
        .expect("spawn crash task");

    runtime
        .block_on(first)
        .expect("runtime completes")
        .expect("the already-completed write survives the later crash");
    let failure = runtime
        .block_on(second)
        .expect("runtime completes")
        .expect_err("the unexecuted batch suffix is terminalized");
    assert_eq!(failure.certainty(), CompletionCertainty::NotApplied);
    assert_eq!(failure.error().error, StorageError::Closed);
    assert_eq!(failure.error().buffer, b"xy");
    assert_eq!(failure.error().bytes_transferred, 0);
    assert_eq!(disk.durable_bytes(), b"");
}

#[test]
fn the_pipeline_model_consumes_no_additional_schedule_draws() {
    use kr_runtime::rng::RandomStream;
    use kr_runtime_io::latency::SimLatencyModel;

    fn schedule_position_after_two_writes(
        model: SimPipelineModel,
    ) -> kr_runtime::rng::RngCheckpoint {
        let mut runtime = SimRuntime::default();
        let config = SimStorageConfig {
            default_latency: SimDuration::from_nanos(100),
            latency_model: SimLatencyModel::uniform_jitter_v1(SimDuration::from_nanos(50)),
            pipeline_model: model,
            ..SimStorageConfig::default()
        };
        let schedule = runtime.random_source(RandomStream::Schedule);
        let storage = SimStorage::open_with_random_sources(
            runtime.handle(),
            SimDisk::default(),
            config,
            SimRandomSources::default().with_schedule(schedule.clone()),
        )
        .expect("open a simulated disk");
        let first = storage.submit_write_at(WriteAtRequest::new(0, b"abc".to_vec()));
        let second = storage.submit_write_at(WriteAtRequest::new(8, b"xyz".to_vec()));
        runtime
            .block_on(first)
            .expect("runtime completes")
            .expect("first write succeeds");
        runtime
            .block_on(second)
            .expect("runtime completes")
            .expect("second write succeeds");
        schedule.random_position()
    }

    assert_eq!(
        schedule_position_after_two_writes(SimPipelineModel::Serial),
        schedule_position_after_two_writes(SimPipelineModel::CommutingOverlapV1),
        "latency draws happen at admission, so the pipeline model must not shift them"
    );
}

#[cfg(feature = "test-support")]
#[test]
fn simulated_storage_passes_shared_conformance_while_overlapping_and_jittered() {
    use kr_runtime::rng::RandomStream;
    use kr_runtime_io::latency::SimLatencyModel;

    let mut runtime = SimRuntime::default();
    let config = SimStorageConfig {
        default_latency: SimDuration::from_nanos(3),
        latency_model: SimLatencyModel::uniform_jitter_v1(SimDuration::from_nanos(7)),
        pipeline_model: SimPipelineModel::CommutingOverlapV1,
        ..SimStorageConfig::default()
    };
    let storage = SimStorage::open_with_random_sources(
        runtime.handle(),
        SimDisk::default(),
        config,
        SimRandomSources::default().with_schedule(runtime.random_source(RandomStream::Schedule)),
    )
    .expect("open a simulated disk");

    runtime
        .block_on(check_empty_file(storage))
        .expect("runtime completes")
        .expect("storage conforms while overlapping commuting operations");
}

/// A mutant that admits single-byte writes but never applies their effect,
/// violating the fence guarantee for abandoned commuting writes.
#[cfg(feature = "test-support")]
#[derive(Clone)]
struct EffectLosingStorage(SimStorage);

#[cfg(feature = "test-support")]
impl FileIoSubmit for EffectLosingStorage {
    type ReadAtResponse =
        Pin<Box<dyn Future<Output = CompletionResult<ReadAtSuccess, ReadAtFailure>>>>;
    type WriteAtResponse =
        Pin<Box<dyn Future<Output = CompletionResult<WriteAtSuccess, WriteAtFailure>>>>;
    type SetLenResponse =
        Pin<Box<dyn Future<Output = CompletionResult<SetLenSuccess, StorageError>>>>;
    type LenResponse = Pin<Box<dyn Future<Output = CompletionResult<FileLength, StorageError>>>>;
    type SyncResponse = Pin<Box<dyn Future<Output = CompletionResult<SyncSuccess, StorageError>>>>;

    fn submit_read_at(&self, request: ReadAtRequest) -> Self::ReadAtResponse {
        Box::pin(self.0.submit_read_at(request))
    }

    fn submit_write_at(&self, request: WriteAtRequest) -> Self::WriteAtResponse {
        if request.buffer.len() == 1 {
            // Reports success without ever submitting the effect.
            return Box::pin(std::future::ready(Ok(WriteAtSuccess {
                bytes_written: 1,
                buffer: request.buffer,
            })));
        }
        Box::pin(self.0.submit_write_at(request))
    }

    fn submit_set_len(&self, len: u64) -> Self::SetLenResponse {
        Box::pin(self.0.submit_set_len(len))
    }

    fn submit_len(&self) -> Self::LenResponse {
        Box::pin(self.0.submit_len())
    }

    fn submit_sync(&self) -> Self::SyncResponse {
        Box::pin(self.0.submit_sync())
    }
}

#[cfg(feature = "test-support")]
#[test]
fn shared_file_contract_rejects_a_provider_that_loses_abandoned_writes() {
    let mut runtime = SimRuntime::default();
    let storage = open(&runtime, &SimDisk::default(), SimStorageConfig::default());

    let error = runtime
        .block_on(check_empty_file(EffectLosingStorage(storage)))
        .expect("runtime completes mutant check")
        .expect_err("contract checker accepted a provider that loses abandoned writes");

    assert!(
        error.contains("expected abcde"),
        "mutant tripped an unrelated contract assertion: {error}"
    );
}
