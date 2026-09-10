use std::cell::Cell;
use std::future::{Future, poll_fn};
use std::pin::Pin;
use std::rc::Rc;
use std::task::Poll;

use kr_runtime::{CompletionCertainty, CompletionError, CompletionResult, SimDuration, SimRuntime};
use kr_runtime_io::{
    FileIoSubmit, FileLength, ReadAtFailure, ReadAtRequest, ReadAtSuccess, SetLenSuccess, SimDisk,
    SimFault, SimFsyncFailure, SimOutcome, SimPipelineModel, SimStorage, SimStorageConfig,
    StorageError, StorageOperation, SyncSuccess as FileSyncSuccess, WriteAtFailure, WriteAtRequest,
    WriteAtSuccess,
};

use super::*;
use crate::conformance::check_ring_contract;
use test_support::{create_sim_ring as create_ring, open_sim_ring as open_ring};

fn limits() -> RingLimits {
    RingLimits {
        max_record_bytes: 32,
        max_live_records: 8,
        max_live_payload_bytes: 128,
        max_read_records: 4,
        max_read_bytes: 64,
        max_batch_records: 4,
        max_batch_bytes: 64,
    }
}

fn file_config() -> FileRingConfig {
    FileRingConfig {
        limits: limits(),
        data_capacity_bytes: 256,
        max_io_request_bytes: 4_096,
        command_queue_capacity: 8,
    }
}

fn storage_config(config: FileRingConfig) -> SimStorageConfig {
    test_support::sim_storage_config(config, SimDuration::ZERO, 64)
}

fn append_one(runtime: &mut SimRuntime, ring: &FileRing<SimStorage>, payload: Vec<u8>) {
    runtime
        .block_on(ring.append(AppendRequest::new(vec![payload])))
        .expect("runtime completes append")
        .expect("append succeeds");
}

fn inject_sync_outcomes(storage: &SimStorage, outcomes: &[SimOutcome]) {
    for (index, outcome) in outcomes.iter().copied().enumerate() {
        let mut fault = SimFault::new(StorageOperation::Sync, SimDuration::ZERO, outcome);
        if outcome == SimOutcome::MayHaveAppliedBefore {
            fault = fault.with_fsync_failure(SimFsyncFailure::RetainDirtyPagesV1);
        }
        storage.inject(fault).unwrap_or_else(|error| {
            panic!("inject sync fault {index} with outcome {outcome:?}: {error}")
        });
    }
}

fn status(runtime: &mut SimRuntime, ring: &FileRing<SimStorage>) -> RingStatus {
    runtime
        .block_on(ring.status())
        .expect("runtime completes status")
        .expect("status succeeds")
}

fn payloads(
    runtime: &mut SimRuntime,
    ring: &FileRing<SimStorage>,
    expected_records: usize,
) -> Vec<Vec<u8>> {
    runtime
        .block_on(ring.read(ReadRequest::new(
            RingCursor::START,
            expected_records.max(1),
            64,
        )))
        .expect("runtime completes read")
        .expect("read succeeds")
        .records
        .into_iter()
        .map(|record| record.buffer)
        .collect()
}

fn assert_recovery_required(runtime: &mut SimRuntime, ring: &FileRing<SimStorage>) {
    let error = runtime
        .block_on(ring.sync())
        .expect("runtime completes poisoned sync")
        .expect_err("poisoned ring rejects later sync");
    assert_eq!(error.certainty(), CompletionCertainty::NotApplied);
    assert_eq!(error.error().checkpoint, None);
    assert_eq!(error.error().error, RingError::RecoveryRequired);
}

#[derive(Clone)]
struct BackendReadOnce<F> {
    inner: F,
    fail_next_read: Rc<Cell<Option<CompletionCertainty>>>,
}

impl<F> BackendReadOnce<F> {
    fn new(inner: F) -> Self {
        Self {
            inner,
            fail_next_read: Rc::new(Cell::new(None)),
        }
    }

    fn fail_next_read(&self) {
        self.fail_next_read
            .set(Some(CompletionCertainty::NotApplied));
    }

    fn fail_next_read_with(&self, certainty: CompletionCertainty) {
        self.fail_next_read.set(Some(certainty));
    }
}

impl<F> FileIoSubmit for BackendReadOnce<F>
where
    F: FileIoSubmit,
{
    type ReadAtResponse =
        Pin<Box<dyn Future<Output = CompletionResult<ReadAtSuccess, ReadAtFailure>>>>;
    type WriteAtResponse =
        Pin<Box<dyn Future<Output = CompletionResult<WriteAtSuccess, WriteAtFailure>>>>;
    type SetLenResponse =
        Pin<Box<dyn Future<Output = CompletionResult<SetLenSuccess, StorageError>>>>;
    type LenResponse = Pin<Box<dyn Future<Output = CompletionResult<FileLength, StorageError>>>>;
    type SyncResponse =
        Pin<Box<dyn Future<Output = CompletionResult<FileSyncSuccess, StorageError>>>>;

    fn submit_read_at(&self, request: ReadAtRequest) -> Self::ReadAtResponse {
        if let Some(certainty) = self.fail_next_read.take() {
            return Box::pin(async move {
                Err(CompletionError::new(
                    certainty,
                    ReadAtFailure {
                        error: StorageError::Backend {
                            operation: StorageOperation::ReadAt,
                            raw_os_error: None,
                            message: "transient test failure".to_owned(),
                        },
                        buffer: request.buffer,
                        bytes_transferred: 0,
                    },
                ))
            });
        }
        Box::pin(self.inner.submit_read_at(request))
    }

    fn submit_write_at(&self, request: WriteAtRequest) -> Self::WriteAtResponse {
        Box::pin(self.inner.submit_write_at(request))
    }

    fn submit_set_len(&self, len: u64) -> Self::SetLenResponse {
        Box::pin(self.inner.submit_set_len(len))
    }

    fn submit_len(&self) -> Self::LenResponse {
        Box::pin(self.inner.submit_len())
    }

    fn submit_sync(&self) -> Self::SyncResponse {
        Box::pin(self.inner.submit_sync())
    }
}

#[derive(Clone)]
struct ReadCountingFile<F> {
    inner: F,
    reads: Rc<Cell<usize>>,
}

impl<F> ReadCountingFile<F> {
    fn new(inner: F) -> Self {
        Self {
            inner,
            reads: Rc::new(Cell::new(0)),
        }
    }

    fn reads(&self) -> usize {
        self.reads.get()
    }
}

impl<F> FileIoSubmit for ReadCountingFile<F>
where
    F: FileIoSubmit,
{
    type ReadAtResponse = F::ReadAtResponse;
    type WriteAtResponse = F::WriteAtResponse;
    type SetLenResponse = F::SetLenResponse;
    type LenResponse = F::LenResponse;
    type SyncResponse = F::SyncResponse;

    fn submit_read_at(&self, request: ReadAtRequest) -> Self::ReadAtResponse {
        self.reads.set(
            self.reads
                .get()
                .checked_add(1)
                .expect("test read counter does not overflow"),
        );
        self.inner.submit_read_at(request)
    }

    fn submit_write_at(&self, request: WriteAtRequest) -> Self::WriteAtResponse {
        self.inner.submit_write_at(request)
    }

    fn submit_set_len(&self, len: u64) -> Self::SetLenResponse {
        self.inner.submit_set_len(len)
    }

    fn submit_len(&self) -> Self::LenResponse {
        self.inner.submit_len()
    }

    fn submit_sync(&self) -> Self::SyncResponse {
        self.inner.submit_sync()
    }
}

#[test]
fn public_config_validation_derives_the_exact_provider_file_bound() {
    let config = file_config();
    assert_eq!(
        config.physical_file_bytes(),
        Ok(DATA_OFFSET + config.data_capacity_bytes)
    );
    assert_eq!(config.validate(), Ok(()));

    let overflowing = FileRingConfig {
        data_capacity_bytes: u64::MAX,
        ..config
    };
    assert!(matches!(
        overflowing.physical_file_bytes(),
        Err(FileRingOpenError::InvalidConfig {
            field: "data_capacity_bytes",
            ..
        })
    ));
    assert!(matches!(
        overflowing.validate(),
        Err(FileRingOpenError::InvalidConfig {
            field: "data_capacity_bytes",
            ..
        })
    ));
}

#[test]
fn simulated_file_ring_passes_shared_contract() {
    let mut runtime = SimRuntime::default();
    let config = file_config();
    let (ring, _storage) = create_ring(
        &mut runtime,
        &SimDisk::default(),
        config,
        storage_config(config),
    );
    let check = ring.clone();
    runtime
        .block_on(async move { check_ring_contract(&check).await })
        .expect("runtime completes conformance")
        .expect("file ring conforms");
}

#[test]
fn transient_not_applied_backend_read_does_not_poison_the_ring() {
    let mut runtime = SimRuntime::default();
    let config = file_config();
    let storage = SimDisk::default()
        .open(runtime.handle(), storage_config(config))
        .expect("open simulated file");
    let file = BackendReadOnce::new(storage);
    let ring = runtime
        .block_on(FileRing::create(runtime.handle(), file.clone(), config))
        .expect("runtime completes create")
        .expect("file ring creates");
    runtime
        .block_on(ring.append(AppendRequest::new(vec![b"first".to_vec()])))
        .expect("runtime completes append")
        .expect("append succeeds");
    runtime
        .block_on(ring.sync())
        .expect("runtime completes sync")
        .expect("sync succeeds");

    file.fail_next_read();
    let error = runtime
        .block_on(ring.read(ReadRequest::new(RingCursor::START, 1, 64)))
        .expect("runtime completes failed read")
        .expect_err("one backend read fails");
    assert_eq!(error.certainty(), CompletionCertainty::NotApplied);
    assert!(matches!(
        error.error(),
        RingError::BackendFailure {
            operation: RingOperation::Read,
            ..
        }
    ));

    let retried = runtime
        .block_on(ring.read(ReadRequest::new(RingCursor::START, 1, 64)))
        .expect("runtime completes retry")
        .expect("transient read failure remains retryable");
    assert_eq!(retried.records[0].buffer, b"first");
    runtime
        .block_on(ring.append(AppendRequest::new(vec![b"second".to_vec()])))
        .expect("runtime completes later append")
        .expect("ring remains writable");
    runtime
        .block_on(ring.sync())
        .expect("runtime completes later sync")
        .expect("ring remains syncable");
}

#[test]
fn ambiguous_backend_read_does_not_poison_the_ring() {
    let mut runtime = SimRuntime::default();
    let config = file_config();
    let storage = SimDisk::default()
        .open(runtime.handle(), storage_config(config))
        .expect("open simulated file");
    let file = BackendReadOnce::new(storage);
    let ring = runtime
        .block_on(FileRing::create(runtime.handle(), file.clone(), config))
        .expect("runtime completes create")
        .expect("file ring creates");
    runtime
        .block_on(ring.append(AppendRequest::new(vec![b"first".to_vec()])))
        .expect("runtime completes append")
        .expect("append succeeds");
    runtime
        .block_on(ring.sync())
        .expect("runtime completes sync")
        .expect("sync succeeds");

    file.fail_next_read_with(CompletionCertainty::MayHaveApplied);
    let error = runtime
        .block_on(ring.read(ReadRequest::new(RingCursor::START, 1, 64)))
        .expect("runtime completes failed read")
        .expect_err("one backend read fails ambiguously");
    assert_eq!(error.certainty(), CompletionCertainty::NotApplied);
    assert!(matches!(
        error.error(),
        RingError::BackendFailure {
            operation: RingOperation::Read,
            ..
        }
    ));

    let retried = runtime
        .block_on(ring.read(ReadRequest::new(RingCursor::START, 1, 64)))
        .expect("runtime completes retry")
        .expect("read ambiguity does not poison persistent ring state");
    assert_eq!(retried.records[0].buffer, b"first");
    runtime
        .block_on(ring.append(AppendRequest::new(vec![b"second".to_vec()])))
        .expect("runtime completes later append")
        .expect("ring remains writable");
    runtime
        .block_on(ring.sync())
        .expect("runtime completes later sync")
        .expect("ring remains syncable");
}

#[test]
fn reopen_discards_unsynced_suffix_and_pending_trim() {
    let mut runtime = SimRuntime::default();
    let disk = SimDisk::default();
    let config = file_config();
    let storage_config = storage_config(config);
    let (ring, storage) = create_ring(&mut runtime, &disk, config, storage_config);

    runtime
        .block_on(ring.append(AppendRequest::new(vec![b"a".to_vec(), b"bb".to_vec()])))
        .expect("runtime completes append")
        .expect("append succeeds");
    runtime
        .block_on(ring.sync())
        .expect("runtime completes sync")
        .expect("sync succeeds");
    runtime
        .block_on(ring.trim(RingCursor::new(1)))
        .expect("runtime completes trim")
        .expect("trim succeeds");
    append_one(&mut runtime, &ring, b"unsynced".to_vec());

    storage.crash();
    drop(ring);
    let (reopened, _storage) = open_ring(&mut runtime, &disk, config, storage_config);
    let status = runtime
        .block_on(reopened.status())
        .expect("runtime completes status")
        .expect("status succeeds");
    assert_eq!(status.accepted_head, RingCursor::START);
    assert_eq!(status.accepted_tail, RingCursor::new(2));
    assert_eq!(status.durable_head, RingCursor::START);
    assert_eq!(status.durable_tail, RingCursor::new(2));
    let page = runtime
        .block_on(reopened.read(ReadRequest::new(RingCursor::START, 4, 64)))
        .expect("runtime completes read")
        .expect("read succeeds");
    assert_eq!(
        page.records
            .into_iter()
            .map(|record| record.buffer)
            .collect::<Vec<_>>(),
        vec![b"a".to_vec(), b"bb".to_vec()]
    );
}

#[derive(Clone, Copy, Debug)]
enum InFlightSyncCut {
    DataFence,
    MetadataWrite,
    MetadataFence,
}

#[test]
fn every_in_flight_sync_stage_recovers_a_complete_checkpoint() {
    for cut_stage in [
        InFlightSyncCut::DataFence,
        InFlightSyncCut::MetadataWrite,
        InFlightSyncCut::MetadataFence,
    ] {
        let mut runtime = SimRuntime::default();
        let disk = SimDisk::default();
        let config = file_config();
        let storage_config = storage_config(config);
        let (ring, storage) = create_ring(&mut runtime, &disk, config, storage_config);

        append_one(&mut runtime, &ring, b"durable".to_vec());
        runtime
            .block_on(ring.sync())
            .expect("runtime completes initial sync")
            .expect("initial checkpoint succeeds");
        append_one(&mut runtime, &ring, b"pending".to_vec());

        let delayed = SimDuration::from_nanos(100);
        let expected_fault_hits = match cut_stage {
            InFlightSyncCut::DataFence => {
                storage
                    .inject(SimFault::new(
                        StorageOperation::Sync,
                        delayed,
                        SimOutcome::Success,
                    ))
                    .expect("inject delayed data fence");
                1
            }
            InFlightSyncCut::MetadataWrite => {
                storage
                    .inject(SimFault::new(
                        StorageOperation::Sync,
                        SimDuration::ZERO,
                        SimOutcome::Success,
                    ))
                    .expect("inject successful data fence");
                storage
                    .inject(SimFault::new(
                        StorageOperation::WriteAt,
                        delayed,
                        SimOutcome::Success,
                    ))
                    .expect("inject delayed metadata write");
                2
            }
            InFlightSyncCut::MetadataFence => {
                storage
                    .inject(SimFault::new(
                        StorageOperation::Sync,
                        SimDuration::ZERO,
                        SimOutcome::Success,
                    ))
                    .expect("inject successful data fence");
                storage
                    .inject(SimFault::new(
                        StorageOperation::Sync,
                        delayed,
                        SimOutcome::Success,
                    ))
                    .expect("inject delayed metadata fence");
                2
            }
        };

        let sync = ring.sync();
        for _ in 0..64 {
            let status = storage.status();
            if status.fault_hits == expected_fault_hits && status.in_flight == 1 {
                break;
            }
            runtime
                .step()
                .unwrap_or_else(|error| panic!("{cut_stage:?}: advance to crash cut: {error}"));
        }
        let cut = storage.status();
        assert_eq!(
            cut.fault_hits, expected_fault_hits,
            "{cut_stage:?}: target operation was not admitted"
        );
        assert_eq!(
            cut.in_flight, 1,
            "{cut_stage:?}: target operation was not pending"
        );
        assert_eq!(
            runtime.snapshot().now,
            kr_runtime::SimInstant::ZERO,
            "{cut_stage:?}: crash cut must precede the delayed effect"
        );

        storage.crash();
        let error = runtime
            .block_on(sync)
            .unwrap_or_else(|error| {
                panic!("{cut_stage:?}: runtime failed interrupted sync: {error}")
            })
            .unwrap_err();
        match cut_stage {
            InFlightSyncCut::DataFence | InFlightSyncCut::MetadataWrite => {
                assert_eq!(error.certainty(), CompletionCertainty::NotApplied);
                assert_eq!(error.error().checkpoint, None);
                assert_eq!(error.error().error, RingError::RecoveryRequired);
            }
            InFlightSyncCut::MetadataFence => {
                assert_eq!(error.certainty(), CompletionCertainty::MayHaveApplied);
                assert!(matches!(
                    error.error().error,
                    RingError::BackendFailure {
                        operation: RingOperation::Sync,
                        ..
                    }
                ));
                assert_eq!(
                    error.error().checkpoint,
                    Some(SyncSuccess {
                        durable_head: RingCursor::START,
                        durable_tail: RingCursor::new(2),
                        reclaimed_records: 0,
                        reclaimed_payload_bytes: 0,
                    })
                );
            }
        }

        drop(ring);
        drop(storage);
        let (reopened, reopened_storage) = open_ring(&mut runtime, &disk, config, storage_config);
        let recovered = status(&mut runtime, &reopened);
        assert_eq!(recovered.accepted_tail, RingCursor::new(1), "{cut_stage:?}");
        assert_eq!(recovered.durable_tail, RingCursor::new(1), "{cut_stage:?}");
        assert_eq!(
            payloads(&mut runtime, &reopened, 1),
            vec![b"durable".to_vec()],
            "{cut_stage:?}"
        );

        reopened_storage.crash();
        drop(reopened);
        drop(reopened_storage);
        runtime
            .shutdown()
            .unwrap_or_else(|error| panic!("{cut_stage:?}: runtime shutdown: {error}"));
    }
}

#[test]
fn trim_checkpoint_releases_space_and_recovery_crosses_implicit_wrap() {
    let trace = test_support::run_wrap_recovery_trace(SimDuration::ZERO);
    assert_eq!(trace.steps.len(), 14);
}

#[test]
fn recovery_reads_many_records_in_bounded_spans() {
    const RECORDS: usize = 128;
    const PREVIOUS_PER_FRAME_READS: usize = 2 + 2 * RECORDS;

    let mut runtime = SimRuntime::default();
    let disk = SimDisk::default();
    let config = FileRingConfig {
        limits: RingLimits {
            max_record_bytes: 32,
            max_live_records: RECORDS,
            max_live_payload_bytes: 2_048,
            max_read_records: RECORDS,
            max_read_bytes: 2_048,
            max_batch_records: RECORDS,
            max_batch_bytes: 2_048,
        },
        data_capacity_bytes: 16 * 1_024,
        max_io_request_bytes: 4_096,
        command_queue_capacity: 8,
    };
    let storage_config = storage_config(config);
    let (ring, storage) = create_ring(&mut runtime, &disk, config, storage_config);
    let payloads = (0..RECORDS).map(|index| vec![index as u8; 8]).collect();
    runtime
        .block_on(ring.append(AppendRequest::new(payloads)))
        .expect("runtime completes many-record append")
        .expect("many-record append succeeds");
    runtime
        .block_on(ring.sync())
        .expect("runtime completes many-record sync")
        .expect("many-record sync succeeds");
    storage.crash();
    drop(ring);
    drop(storage);

    let reopened_storage = disk
        .open(runtime.handle(), storage_config)
        .expect("reopen many-record simulated file");
    let counted = ReadCountingFile::new(reopened_storage);
    let reopened = runtime
        .block_on(FileRing::open(runtime.handle(), counted.clone(), config))
        .expect("runtime completes many-record recovery")
        .expect("many-record recovery succeeds");
    let recovery_reads = counted.reads();

    assert!(
        recovery_reads <= 5,
        "128 records should recover with two superblock reads and a few bounded data spans, got {recovery_reads} read_at calls"
    );
    assert!(
        recovery_reads * 32 < PREVIOUS_PER_FRAME_READS,
        "bounded spans must use at least 32x fewer reads than the former header-plus-frame strategy ({recovery_reads} vs {PREVIOUS_PER_FRAME_READS})"
    );
    let recovered = runtime
        .block_on(reopened.status())
        .expect("runtime completes recovered status")
        .expect("recovered status succeeds");
    assert_eq!(recovered.durable_tail, RingCursor::new(RECORDS as u64));
}

#[test]
fn short_lower_level_transfers_are_retried_exactly() {
    let mut runtime = SimRuntime::default();
    let disk = SimDisk::default();
    let mut config = file_config();
    config.max_io_request_bytes = 37;
    let mut storage_config = storage_config(config);
    storage_config.max_read_bytes = 37;
    storage_config.max_write_bytes = 37;
    storage_config.max_read_chunk = 3;
    storage_config.max_write_chunk = 2;
    let (ring, storage) = create_ring(&mut runtime, &disk, config, storage_config);
    let payload = b"partial-transfer".to_vec();
    append_one(&mut runtime, &ring, payload.clone());
    runtime
        .block_on(ring.sync())
        .expect("runtime completes sync")
        .expect("sync succeeds");
    let record = runtime
        .block_on(ring.read(ReadRequest::new(RingCursor::START, 1, 64)))
        .expect("runtime completes read")
        .expect("read succeeds")
        .records
        .into_iter()
        .next()
        .expect("one record");
    assert_eq!(record.buffer, payload);

    storage.crash();
    drop(ring);
    drop(storage);
    let (reopened, _storage) = open_ring(&mut runtime, &disk, config, storage_config);
    assert_eq!(payloads(&mut runtime, &reopened, 1), vec![payload]);
}

#[test]
fn request_validation_precedes_admission_backpressure() {
    let mut runtime = SimRuntime::default();
    let mut config = file_config();
    config.command_queue_capacity = 1;
    let mut storage_config = storage_config(config);
    storage_config.default_latency = SimDuration::from_nanos(10);
    let (ring, _storage) = create_ring(&mut runtime, &SimDisk::default(), config, storage_config);

    let append = ring.append(AppendRequest::new(vec![b"one".to_vec()]));

    let empty = runtime
        .block_on(ring.append(AppendRequest::new(Vec::new())))
        .expect("runtime completes empty-batch rejection")
        .expect_err("empty append is rejected before admission");
    assert_eq!(empty.certainty(), CompletionCertainty::NotApplied);
    assert_eq!(empty.error().error, RingError::EmptyBatch);

    let zero_read = runtime
        .block_on(ring.read(ReadRequest::new(RingCursor::START, 0, 0)))
        .expect("runtime completes invalid-read rejection")
        .expect_err("invalid read is rejected before admission");
    assert_eq!(zero_read.certainty(), CompletionCertainty::NotApplied);
    assert_eq!(zero_read.error(), &RingError::ZeroReadRecordLimit);

    let rejected = ring.sync();
    let error = runtime
        .block_on(rejected)
        .expect("runtime completes rejection")
        .expect_err("second operation is rejected");
    assert_eq!(error.certainty(), CompletionCertainty::NotApplied);
    assert_eq!(error.error().error, RingError::Backpressure { limit: 1 });
    runtime
        .block_on(append)
        .expect("runtime completes admitted append")
        .expect("admitted append succeeds");
}

#[test]
fn polled_then_dropped_append_is_fenced_by_later_sync() {
    let mut runtime = SimRuntime::default();
    let config = file_config();
    let mut storage_config = storage_config(config);
    storage_config.default_latency = SimDuration::from_nanos(3);
    let (ring, _storage) = create_ring(&mut runtime, &SimDisk::default(), config, storage_config);

    let result = runtime
        .block_on(async move {
            let mut abandoned = Box::pin(ring.append(AppendRequest::new(vec![b"kept".to_vec()])));
            poll_fn(|context| {
                assert!(abandoned.as_mut().poll(context).is_pending());
                Poll::Ready(())
            })
            .await;
            drop(abandoned);
            ring.sync().await.expect("later sync succeeds");
            ring.read(ReadRequest::new(RingCursor::START, 1, 64))
                .await
                .expect("read succeeds")
        })
        .expect("runtime completes");
    assert_eq!(result.records[0].buffer, b"kept");
}

#[test]
fn data_sync_fail_before_is_retryable_and_preserves_prior_checkpoint() {
    let mut runtime = SimRuntime::default();
    let disk = SimDisk::default();
    let config = file_config();
    let storage_config = storage_config(config);
    let (ring, storage) = create_ring(&mut runtime, &disk, config, storage_config);

    append_one(&mut runtime, &ring, b"old".to_vec());
    runtime
        .block_on(ring.sync())
        .expect("runtime completes initial sync")
        .expect("initial checkpoint succeeds");
    let old_image = disk.durable_bytes();

    append_one(&mut runtime, &ring, b"pending".to_vec());
    inject_sync_outcomes(&storage, &[SimOutcome::FailBefore]);
    let error = runtime
        .block_on(ring.sync())
        .expect("runtime completes failed data fence")
        .expect_err("injected data fence fails before applying");
    assert_eq!(error.certainty(), CompletionCertainty::NotApplied);
    assert_eq!(error.error().checkpoint, None);
    assert!(matches!(
        error.error().error,
        RingError::BackendFailure {
            operation: RingOperation::Sync,
            ..
        }
    ));
    let after_failure = status(&mut runtime, &ring);
    assert_eq!(after_failure.accepted_tail, RingCursor::new(2));
    assert_eq!(after_failure.durable_tail, RingCursor::new(1));
    assert_eq!(
        disk.durable_bytes(),
        old_image,
        "FailBefore must leave the prior checkpoint and its data image unchanged"
    );

    let retried = runtime
        .block_on(ring.sync())
        .expect("runtime completes retry")
        .expect("NotApplied data-fence failure remains retryable");
    assert_eq!(retried.durable_tail, RingCursor::new(2));
    assert_eq!(status(&mut runtime, &ring).durable_tail, RingCursor::new(2));
}

#[test]
fn ambiguous_data_sync_poisons_and_reopens_prior_checkpoint() {
    let mut runtime = SimRuntime::default();
    let disk = SimDisk::default();
    let config = file_config();
    let storage_config = storage_config(config);
    let (ring, storage) = create_ring(&mut runtime, &disk, config, storage_config);

    append_one(&mut runtime, &ring, b"old".to_vec());
    runtime
        .block_on(ring.sync())
        .expect("runtime completes initial sync")
        .expect("initial checkpoint succeeds");
    append_one(&mut runtime, &ring, b"uncheckpointed".to_vec());

    inject_sync_outcomes(&storage, &[SimOutcome::MayHaveAppliedAfter]);
    let error = runtime
        .block_on(ring.sync())
        .expect("runtime completes ambiguous data fence")
        .expect_err("ambiguous data fence is surfaced");
    assert_eq!(
        error.certainty(),
        CompletionCertainty::NotApplied,
        "data durability can be ambiguous while metadata checkpoint installation is definitely absent"
    );
    assert_eq!(error.error().checkpoint, None);
    assert_eq!(status(&mut runtime, &ring).durable_tail, RingCursor::new(1));
    assert_recovery_required(&mut runtime, &ring);

    storage.crash();
    drop(ring);
    let (reopened, _storage) = open_ring(&mut runtime, &disk, config, storage_config);
    let recovered = status(&mut runtime, &reopened);
    assert_eq!(recovered.accepted_tail, RingCursor::new(1));
    assert_eq!(recovered.durable_tail, RingCursor::new(1));
    assert_eq!(payloads(&mut runtime, &reopened, 1), vec![b"old".to_vec()]);
}

#[test]
fn applied_final_metadata_sync_returns_and_publishes_checkpoint() {
    let mut runtime = SimRuntime::default();
    let disk = SimDisk::default();
    let config = file_config();
    let storage_config = storage_config(config);
    let (ring, storage) = create_ring(&mut runtime, &disk, config, storage_config);

    append_one(&mut runtime, &ring, b"old".to_vec());
    runtime
        .block_on(ring.sync())
        .expect("runtime completes initial sync")
        .expect("initial checkpoint succeeds");
    append_one(&mut runtime, &ring, b"applied".to_vec());

    inject_sync_outcomes(&storage, &[SimOutcome::Success, SimOutcome::FailAfter]);
    let error = runtime
        .block_on(ring.sync())
        .expect("runtime completes applied metadata fence")
        .expect_err("FailAfter reports the lower-level response failure");
    let expected = SyncSuccess {
        durable_head: RingCursor::START,
        durable_tail: RingCursor::new(2),
        reclaimed_records: 0,
        reclaimed_payload_bytes: 0,
    };
    assert_eq!(error.certainty(), CompletionCertainty::Applied);
    assert_eq!(error.error().checkpoint, Some(expected));
    let published = status(&mut runtime, &ring);
    assert_eq!(published.accepted_tail, RingCursor::new(2));
    assert_eq!(published.durable_tail, RingCursor::new(2));
    assert_eq!(
        runtime
            .block_on(ring.sync())
            .expect("runtime completes post-Applied sync")
            .expect("published checkpoint makes a later sync a no-op"),
        expected
    );
}

#[test]
fn ambiguous_final_sync_with_confirmed_invalidation_is_retryable() {
    let mut runtime = SimRuntime::default();
    let disk = SimDisk::default();
    let config = file_config();
    let storage_config = storage_config(config);
    let (ring, storage) = create_ring(&mut runtime, &disk, config, storage_config);

    append_one(&mut runtime, &ring, b"old".to_vec());
    runtime
        .block_on(ring.sync())
        .expect("runtime completes initial sync")
        .expect("initial checkpoint succeeds");
    append_one(&mut runtime, &ring, b"pending".to_vec());

    inject_sync_outcomes(
        &storage,
        &[SimOutcome::Success, SimOutcome::MayHaveAppliedAfter],
    );
    let error = runtime
        .block_on(ring.sync())
        .expect("runtime completes ambiguous metadata fence and rollback")
        .expect_err("ambiguous metadata fence is invalidated");
    assert_eq!(error.certainty(), CompletionCertainty::NotApplied);
    assert_eq!(error.error().checkpoint, None);
    let rolled_back = status(&mut runtime, &ring);
    assert_eq!(rolled_back.accepted_tail, RingCursor::new(2));
    assert_eq!(rolled_back.durable_tail, RingCursor::new(1));

    let retried = runtime
        .block_on(ring.sync())
        .expect("runtime completes retry after confirmed invalidation")
        .expect("confirmed inactive-slot invalidation leaves sync retryable");
    assert_eq!(retried.durable_tail, RingCursor::new(2));
}

fn check_failed_rollback_recovery(final_outcome: SimOutcome, expected_payloads: Vec<Vec<u8>>) {
    let case = format!("final_outcome={final_outcome:?}");
    let mut runtime = SimRuntime::default();
    let disk = SimDisk::default();
    let config = file_config();
    let storage_config = storage_config(config);
    let (ring, storage) = create_ring(&mut runtime, &disk, config, storage_config);

    append_one(&mut runtime, &ring, b"old".to_vec());
    runtime
        .block_on(ring.sync())
        .unwrap_or_else(|error| panic!("{case}: runtime failed initial sync: {error}"))
        .unwrap_or_else(|error| panic!("{case}: initial checkpoint failed: {error}"));
    append_one(&mut runtime, &ring, b"candidate".to_vec());
    inject_sync_outcomes(
        &storage,
        &[SimOutcome::Success, final_outcome, SimOutcome::FailBefore],
    );

    let checkpoint_result = runtime
        .block_on(ring.sync())
        .unwrap_or_else(|runtime_error| {
            panic!("{case}: runtime failed ambiguous checkpoint: {runtime_error}")
        });
    let error = match checkpoint_result {
        Err(error) => error,
        Ok(success) => panic!("{case}: checkpoint unexpectedly succeeded: {success:?}"),
    };
    assert_eq!(
        error.certainty(),
        CompletionCertainty::MayHaveApplied,
        "{case}"
    );
    assert_eq!(
        error.error().checkpoint,
        Some(SyncSuccess {
            durable_head: RingCursor::START,
            durable_tail: RingCursor::new(2),
            reclaimed_records: 0,
            reclaimed_payload_bytes: 0,
        }),
        "{case}"
    );
    assert_recovery_required(&mut runtime, &ring);

    storage.crash();
    drop(ring);
    let (reopened, _storage) = open_ring(&mut runtime, &disk, config, storage_config);
    let expected_tail = RingCursor::new(expected_payloads.len() as u64);
    let recovered = status(&mut runtime, &reopened);
    assert_eq!(recovered.accepted_tail, expected_tail, "{case}");
    assert_eq!(recovered.durable_tail, expected_tail, "{case}");
    assert_eq!(
        payloads(&mut runtime, &reopened, expected_payloads.len()),
        expected_payloads,
        "{case}"
    );
}

#[test]
fn ambiguous_final_sync_before_failed_rollback_recovers_old_checkpoint() {
    check_failed_rollback_recovery(SimOutcome::MayHaveAppliedBefore, vec![b"old".to_vec()]);
}

#[test]
fn ambiguous_final_sync_after_failed_rollback_recovers_new_checkpoint() {
    check_failed_rollback_recovery(
        SimOutcome::MayHaveAppliedAfter,
        vec![b"old".to_vec(), b"candidate".to_vec()],
    );
}

#[test]
fn every_inactive_superblock_prefix_recovers_only_complete_checkpoint() {
    let mut runtime = SimRuntime::default();
    let disk = SimDisk::default();
    let config = file_config();
    let storage_config = storage_config(config);
    let (ring, storage) = create_ring(&mut runtime, &disk, config, storage_config);

    append_one(&mut runtime, &ring, b"old".to_vec());
    runtime
        .block_on(ring.sync())
        .expect("runtime completes old checkpoint")
        .expect("old checkpoint succeeds");
    let old_image = disk.durable_bytes();
    append_one(&mut runtime, &ring, b"candidate".to_vec());
    runtime
        .block_on(ring.sync())
        .expect("runtime completes new checkpoint")
        .expect("new checkpoint succeeds");
    let new_image = disk.durable_bytes();

    assert_eq!(old_image.len(), new_image.len());
    let old_active = (0..SUPERBLOCK_COUNT)
        .filter_map(|slot| {
            let start = slot * SUPERBLOCK_LEN;
            decode_superblock(&old_image[start..start + SUPERBLOCK_LEN], slot as u8).ok()
        })
        .max_by_key(|superblock| superblock.generation)
        .expect("old image has a valid active superblock");
    assert_eq!(old_active.checkpoint.tail_seq, 1);
    let inactive_slot = 1 - usize::from(old_active.physical_slot);
    let inactive_start = inactive_slot * SUPERBLOCK_LEN;
    let new_inactive = decode_superblock(
        &new_image[inactive_start..inactive_start + SUPERBLOCK_LEN],
        inactive_slot as u8,
    )
    .expect("new image has a valid checkpoint in the formerly inactive slot");
    assert_eq!(new_inactive.checkpoint.tail_seq, 2);

    storage.crash();
    drop(ring);
    drop(storage);
    runtime
        .shutdown()
        .expect("tear down image-producing runtime");

    let data_start = usize::try_from(DATA_OFFSET).expect("data offset fits usize");
    for cut in 0..=SUPERBLOCK_LEN {
        let mut image = old_image.clone();
        image[data_start..].copy_from_slice(&new_image[data_start..]);
        image[inactive_start..inactive_start + cut]
            .copy_from_slice(&new_image[inactive_start..inactive_start + cut]);

        let mut cut_runtime = SimRuntime::default();
        let cut_disk = SimDisk::from_durable_bytes(image);
        let cut_storage = cut_disk
            .open(cut_runtime.handle(), storage_config)
            .unwrap_or_else(|error| panic!("cut={cut}: could not open simulated disk: {error}"));
        let reopened = cut_runtime
            .block_on(FileRing::open(
                cut_runtime.handle(),
                cut_storage.clone(),
                config,
            ))
            .unwrap_or_else(|error| panic!("cut={cut}: runtime failed recovery: {error}"))
            .unwrap_or_else(|error| panic!("cut={cut}: ring recovery failed: {error}"));
        let recovered = cut_runtime
            .block_on(reopened.status())
            .unwrap_or_else(|error| panic!("cut={cut}: runtime failed status: {error}"))
            .unwrap_or_else(|error| panic!("cut={cut}: recovered status failed: {error}"));
        let expected_tail = if cut == SUPERBLOCK_LEN { 2 } else { 1 };
        assert_eq!(
            recovered.accepted_tail,
            RingCursor::new(expected_tail),
            "cut={cut}: accepted tail changed before the entire {SUPERBLOCK_LEN}-byte inactive slot was installed"
        );
        assert_eq!(
            recovered.durable_tail,
            RingCursor::new(expected_tail),
            "cut={cut}: durable tail changed before the entire {SUPERBLOCK_LEN}-byte inactive slot was installed"
        );

        cut_storage.crash();
        drop(reopened);
        drop(cut_storage);
        cut_runtime
            .shutdown()
            .unwrap_or_else(|error| panic!("cut={cut}: runtime teardown failed: {error}"));
    }
}

#[test]
fn committed_frame_corruption_fails_open_without_repair() {
    let mut runtime = SimRuntime::default();
    let disk = SimDisk::default();
    let config = file_config();
    let storage_config = storage_config(config);
    let (ring, storage) = create_ring(&mut runtime, &disk, config, storage_config);
    append_one(&mut runtime, &ring, b"checksum".to_vec());
    runtime
        .block_on(ring.sync())
        .expect("runtime completes sync")
        .expect("sync succeeds");
    storage.crash();
    drop(ring);

    let mut bytes = disk.durable_bytes();
    bytes[usize::try_from(DATA_OFFSET).expect("offset fits") + FRAME_HEADER_LEN] ^= 1;
    let corrupt = SimDisk::from_durable_bytes(bytes.clone());
    let storage = corrupt
        .open(runtime.handle(), storage_config)
        .expect("open corrupt image");
    let result = runtime
        .block_on(FileRing::open(runtime.handle(), storage, config))
        .expect("runtime completes corrupt open");
    let error = match result {
        Err(error) => error,
        Ok(_) => panic!("committed corruption unexpectedly opened"),
    };
    assert_eq!(error.certainty(), CompletionCertainty::NotApplied);
    assert!(matches!(error.error(), FileRingOpenError::Corrupt { .. }));
    assert_eq!(corrupt.durable_bytes(), bytes);
}

// --- Pipelined append plans ------------------------------------------------
//
// An append's frame writes are pairwise non-overlapping, so the driver
// admits them eagerly and the provider may overlap them. These tests run the
// simulated provider in its commuting-overlap mode with scripted exact
// delays, so completion reordering is deterministic and asserted exactly.

fn overlap_storage_config(config: FileRingConfig) -> SimStorageConfig {
    SimStorageConfig {
        pipeline_model: SimPipelineModel::CommutingOverlapV1,
        ..storage_config(config)
    }
}

fn inject_write_outcomes(storage: &SimStorage, outcomes: &[(u64, SimOutcome)]) {
    for (index, (delay, outcome)) in outcomes.iter().copied().enumerate() {
        storage
            .inject(SimFault::new(
                StorageOperation::WriteAt,
                SimDuration::from_nanos(delay),
                outcome,
            ))
            .unwrap_or_else(|error| {
                panic!("inject write fault {index} with outcome {outcome:?}: {error}")
            });
    }
}

#[test]
fn a_multi_frame_append_completes_after_the_slowest_frame_not_their_sum() {
    let mut runtime = SimRuntime::default();
    let disk = SimDisk::default();
    let config = file_config();
    let (ring, storage) = create_ring(&mut runtime, &disk, config, overlap_storage_config(config));
    inject_write_outcomes(
        &storage,
        &[(10, SimOutcome::Success), (5, SimOutcome::Success)],
    );

    let before = runtime.snapshot().now;
    runtime
        .block_on(ring.append(AppendRequest::new(vec![vec![1; 4], vec![2; 4]])))
        .expect("runtime completes append")
        .expect("append succeeds");
    let elapsed = runtime
        .snapshot()
        .now
        .checked_duration_since(before)
        .expect("virtual time advances monotonically");
    assert_eq!(
        elapsed,
        SimDuration::from_nanos(10),
        "both frames run concurrently, so the append costs the slowest frame"
    );
    assert_eq!(
        storage.status().reordered_completions,
        1,
        "the shorter-latency second frame completed before the first"
    );

    runtime
        .block_on(ring.sync())
        .expect("runtime completes sync")
        .expect("sync succeeds");
    assert_eq!(
        payloads(&mut runtime, &ring, 2),
        vec![vec![1; 4], vec![2; 4]]
    );
}

#[test]
fn a_failed_frame_mid_plan_leaves_cursors_unchanged_and_the_next_append_replans_the_region() {
    let mut runtime = SimRuntime::default();
    let disk = SimDisk::default();
    let config = file_config();
    let (ring, storage) = create_ring(&mut runtime, &disk, config, overlap_storage_config(config));
    // Frame one fails before any effect, after frame two already landed as
    // inert bytes beyond the accepted tail.
    inject_write_outcomes(
        &storage,
        &[(10, SimOutcome::FailBefore), (0, SimOutcome::Success)],
    );

    let records = vec![vec![7; 4], vec![8; 4]];
    let before = status(&mut runtime, &ring);
    let failure = runtime
        .block_on(ring.append(AppendRequest::new(records.clone())))
        .expect("runtime completes append")
        .expect_err("the scripted frame failure fails the whole plan");
    assert_eq!(failure.certainty(), CompletionCertainty::NotApplied);
    assert_eq!(failure.error().records, records);
    assert!(matches!(
        failure.error().error,
        RingError::BackendFailure {
            operation: RingOperation::Append,
            ..
        }
    ));
    assert_eq!(
        status(&mut runtime, &ring),
        before,
        "a failed plan advances no cursor"
    );

    // Not poisoned: the same records append cleanly over the partially
    // written region, and reading back sees exactly them.
    runtime
        .block_on(ring.append(AppendRequest::new(records.clone())))
        .expect("runtime completes retried append")
        .expect("the replanned append succeeds");
    runtime
        .block_on(ring.sync())
        .expect("runtime completes sync")
        .expect("sync succeeds");
    assert_eq!(payloads(&mut runtime, &ring, 2), records);
}

#[test]
fn an_ambiguous_frame_anywhere_in_the_plan_poisons_even_when_the_first_failure_is_benign() {
    let mut runtime = SimRuntime::default();
    let disk = SimDisk::default();
    let config = file_config();
    let (ring, storage) = create_ring(&mut runtime, &disk, config, overlap_storage_config(config));
    // Frame one fails NotApplied and is the primary reported failure, but
    // frame two — settled first — is ambiguous. The serial path could never
    // observe this pair; the pipelined plan must still fail closed on it.
    inject_write_outcomes(
        &storage,
        &[
            (10, SimOutcome::FailBefore),
            (0, SimOutcome::MayHaveAppliedAfter),
        ],
    );

    let failure = runtime
        .block_on(ring.append(AppendRequest::new(vec![vec![7; 4], vec![8; 4]])))
        .expect("runtime completes append")
        .expect_err("the scripted frame failures fail the plan");
    assert_eq!(failure.certainty(), CompletionCertainty::NotApplied);
    assert_recovery_required(&mut runtime, &ring);
}

#[test]
fn a_plan_wider_than_the_admission_bound_defers_and_still_succeeds() {
    let mut runtime = SimRuntime::default();
    let disk = SimDisk::default();
    let config = file_config();
    let storage_config = SimStorageConfig {
        max_in_flight: 1,
        ..overlap_storage_config(config)
    };
    let (ring, _storage) = create_ring(&mut runtime, &disk, config, storage_config);

    let records = vec![vec![1; 4], vec![2; 4], vec![3; 4]];
    runtime
        .block_on(ring.append(AppendRequest::new(records.clone())))
        .expect("runtime completes append")
        .expect("a plan wider than max_in_flight degrades to deferred writes");
    runtime
        .block_on(ring.sync())
        .expect("runtime completes sync")
        .expect("sync succeeds");
    assert_eq!(payloads(&mut runtime, &ring, 3), records);
}
