//! Cold application-facing file operations over the warm [`FileIoSubmit`]
//! submission boundary.
//!
//! [`ColdFile`] is the default way for application code to perform file I/O:
//! a never-polled operation has not started. The eager `submit_*` contract
//! below it is unchanged and remains available to providers and to systems
//! code that intentionally needs explicit submission. The staged migration
//! that produced this split is recorded in `COLD-IO-FUTURES-PROPOSAL.md`.

use std::future::Future;

use kr_runtime::CompletionResult;

use super::{
    FileIoSubmit, FileLength, ReadAtFailure, ReadAtRequest, ReadAtSuccess, SetLenSuccess,
    StorageError, SyncSuccess, WriteAtFailure, WriteAtRequest, WriteAtSuccess,
};

/// Cold application-facing file handle over an eager [`FileIoSubmit`] backend.
///
/// Calling an operation method clones the backend capability and moves the
/// request into the returned future; it does not attempt admission. The
/// future's first poll attempts admission through the backend exactly once and
/// from then on behaves as the eager contract: an admission rejection is an
/// immediately ready `NotApplied` failure, and dropping the future after an
/// admitting first poll abandons only the response, never the admitted
/// operation.
///
/// A never-polled future has admitted nothing: dropping it destroys the
/// request buffer and releases the retained backend clone without consuming
/// provider capacity, queue position, or scripted faults. Releasing the final
/// backend clone still performs that handle's ordinary teardown, so an
/// unpolled future keeps the underlying session alive until it is dropped.
///
/// Ordering follows successful admission at first poll, not construction
/// order. `sync` therefore fences only operations whose futures reached their
/// first poll; a constructed-but-unpolled write is never fenced. Each future
/// submits through its own clone of the backend, so this façade provides one
/// admission-order domain only where the backend's clones already share one —
/// true for the in-tree providers, but not promised by [`FileIoSubmit`] itself.
///
/// A cold future is [`Send`] exactly when the backend and its response futures
/// are. A [`SimStorage`](super::SimStorage)-backed future is not:
///
/// ```compile_fail
/// fn requires_send<T: Send>(_value: T) {}
/// fn demand(cold: &kr_runtime_io::storage::ColdFile<kr_runtime_io::SimStorage>) {
///     requires_send(cold.len());
/// }
/// ```
#[derive(Clone)]
pub struct ColdFile<F> {
    backend: F,
}

impl<F> ColdFile<F> {
    #[must_use]
    pub const fn new(backend: F) -> Self {
        Self { backend }
    }
}

// Each operation defers to `crate::cold_submit`, which retains the backend
// clone for the future's whole life and drops the warm response before it.
#[allow(clippy::len_without_is_empty)]
impl<F: FileIoSubmit> ColdFile<F> {
    pub fn read_at(
        &self,
        request: ReadAtRequest,
    ) -> impl Future<Output = CompletionResult<ReadAtSuccess, ReadAtFailure>> + 'static + use<F>
    {
        crate::cold_submit(self.backend.clone(), move |backend| {
            backend.submit_read_at(request)
        })
    }

    pub fn write_at(
        &self,
        request: WriteAtRequest,
    ) -> impl Future<Output = CompletionResult<WriteAtSuccess, WriteAtFailure>> + 'static + use<F>
    {
        crate::cold_submit(self.backend.clone(), move |backend| {
            backend.submit_write_at(request)
        })
    }

    pub fn set_len(
        &self,
        len: u64,
    ) -> impl Future<Output = CompletionResult<SetLenSuccess, StorageError>> + 'static + use<F>
    {
        crate::cold_submit(self.backend.clone(), move |backend| {
            backend.submit_set_len(len)
        })
    }

    pub fn len(
        &self,
    ) -> impl Future<Output = CompletionResult<FileLength, StorageError>> + 'static + use<F> {
        crate::cold_submit(self.backend.clone(), FileIoSubmit::submit_len)
    }

    pub fn sync(
        &self,
    ) -> impl Future<Output = CompletionResult<SyncSuccess, StorageError>> + 'static + use<F> {
        crate::cold_submit(self.backend.clone(), FileIoSubmit::submit_sync)
    }
}

#[cfg(test)]
mod tests {
    use super::ColdFile;
    use crate::storage::{
        FileIoSubmit, MemoryFile, MemoryFileConfig, ReadAtRequest, SimDisk, SimFault, SimOpenError,
        SimOutcome, SimStorage, SimStorageConfig, StorageError, StorageOperation, WriteAtRequest,
    };
    use kr_runtime::{CompletionCertainty, SimDuration, SimRuntime};
    use std::future::poll_fn;
    use std::pin::pin;
    use std::task::Poll;

    fn open(runtime: &SimRuntime, disk: &SimDisk, config: SimStorageConfig) -> SimStorage {
        disk.open(runtime.handle(), config)
            .expect("open simulated disk")
    }

    fn delayed_config() -> SimStorageConfig {
        SimStorageConfig {
            default_latency: SimDuration::from_millis(5).expect("duration fits"),
            ..SimStorageConfig::default()
        }
    }

    #[test]
    fn unpolled_futures_admit_nothing_and_consume_no_faults() {
        let mut runtime = SimRuntime::default();
        let storage = open(&runtime, &SimDisk::default(), SimStorageConfig::default());
        let cold = ColdFile::new(storage.clone());
        storage
            .inject(SimFault::new(
                StorageOperation::WriteAt,
                SimDuration::ZERO,
                SimOutcome::FailBefore,
            ))
            .expect("inject scripted fault");

        let constructed = cold.write_at(WriteAtRequest::new(0, b"never".to_vec()));
        let status = storage.status();
        assert_eq!(status.in_flight, 0);
        assert_eq!(status.pending_faults, 1);
        assert_eq!(status.fault_hits, 0);

        drop(constructed);
        let status = storage.status();
        assert_eq!(status.in_flight, 0);
        assert_eq!(status.pending_faults, 1);
        assert_eq!(status.fault_hits, 0);

        // The first admitted write consumes the fault the unpolled future left
        // untouched.
        let error = runtime
            .block_on(async move { cold.write_at(WriteAtRequest::new(0, b"w".to_vec())).await })
            .expect("runtime completes")
            .expect_err("scripted fault fails the first admitted write");
        assert_eq!(error.certainty(), CompletionCertainty::NotApplied);
        assert_eq!(
            error.error().error,
            StorageError::Injected {
                operation: StorageOperation::WriteAt
            }
        );
        assert_eq!(storage.status().fault_hits, 1);
    }

    #[test]
    fn unpolled_memory_operations_apply_no_effect() {
        let file = MemoryFile::new(MemoryFileConfig::default()).expect("default config is valid");
        let cold = ColdFile::new(file.clone());

        drop(cold.write_at(WriteAtRequest::new(0, b"never".to_vec())));
        drop(cold.set_len(9));
        drop(cold.sync());

        let status = file.status();
        assert_eq!(status.accepted_len, 0);
        assert_eq!(status.durable_len, 0);
        assert!(!status.dirty);
    }

    #[test]
    fn repolling_a_pending_future_does_not_resubmit() {
        let mut runtime = SimRuntime::default();
        let storage = open(&runtime, &SimDisk::default(), delayed_config());
        let cold = ColdFile::new(storage.clone());

        let written = runtime
            .block_on(async move {
                let mut future = pin!(cold.write_at(WriteAtRequest::new(0, b"w".to_vec())));
                let first = poll_fn(|context| Poll::Ready(future.as_mut().poll(context))).await;
                assert!(first.is_pending(), "first poll admits a delayed write");
                assert_eq!(storage.status().in_flight, 1);

                let second = poll_fn(|context| Poll::Ready(future.as_mut().poll(context))).await;
                assert!(
                    second.is_pending(),
                    "second poll only observes the response"
                );
                assert_eq!(storage.status().in_flight, 1);

                future.await
            })
            .expect("runtime completes")
            .expect("delayed write succeeds");
        assert_eq!(written.bytes_written, 1);
    }

    #[test]
    fn dropping_a_polled_pending_write_leaves_it_admitted_and_fenced_by_sync() {
        let mut runtime = SimRuntime::default();
        let disk = SimDisk::default();
        let storage = open(&runtime, &disk, delayed_config());
        let cold = ColdFile::new(storage);

        let synced = runtime
            .block_on(async move {
                // Box so the later drop destroys the future itself, not a
                // pinned reference to a still-live local.
                let mut future = Box::pin(cold.write_at(WriteAtRequest::new(0, b"w".to_vec())));
                let first = poll_fn(|context| Poll::Ready(future.as_mut().poll(context))).await;
                assert!(first.is_pending(), "first poll admits a delayed write");
                drop(future);
                cold.sync().await
            })
            .expect("runtime completes")
            .expect("sync succeeds");

        assert_eq!(synced.durable_len, 1);
        assert_eq!(disk.durable_bytes(), b"w");
    }

    #[test]
    fn provider_state_is_observed_at_first_poll_not_construction() {
        let mut runtime = SimRuntime::default();
        let storage = open(&runtime, &SimDisk::default(), SimStorageConfig::default());
        let cold = ColdFile::new(storage.clone());

        // Injected after construction, before first poll: a cold operation
        // must observe it; an eager one would already have been admitted.
        let constructed = cold.write_at(WriteAtRequest::new(0, b"x".to_vec()));
        storage
            .inject(SimFault::new(
                StorageOperation::WriteAt,
                SimDuration::ZERO,
                SimOutcome::FailBefore,
            ))
            .expect("inject scripted fault");

        let error = runtime
            .block_on(constructed)
            .expect("runtime completes")
            .expect_err("fault installed before first poll applies to the operation");
        assert_eq!(error.certainty(), CompletionCertainty::NotApplied);
        assert_eq!(storage.status().fault_hits, 1);
    }

    #[test]
    fn admission_capacity_is_claimed_at_first_poll_not_construction() {
        let mut runtime = SimRuntime::default();
        let storage = open(
            &runtime,
            &SimDisk::default(),
            SimStorageConfig {
                max_in_flight: 1,
                ..delayed_config()
            },
        );
        let cold = ColdFile::new(storage.clone());

        let error = runtime
            .block_on(async move {
                let constructed = cold.write_at(WriteAtRequest::new(0, b"cold".to_vec()));
                // The eager backend write takes the only slot after the cold
                // future was constructed.
                let warm_response =
                    storage.submit_write_at(WriteAtRequest::new(0, b"warm".to_vec()));
                let rejected = constructed
                    .await
                    .expect_err("first poll finds capacity exhausted");
                warm_response.await.expect("warm write succeeds");
                rejected
            })
            .expect("runtime completes");
        assert_eq!(error.certainty(), CompletionCertainty::NotApplied);
        assert_eq!(
            error.error().error,
            StorageError::ResourceExhausted {
                resource: "in-flight storage operations",
                limit: 1,
            }
        );
    }

    #[test]
    fn an_unpolled_future_retains_the_session_capability() {
        let runtime = SimRuntime::default();
        let disk = SimDisk::default();
        let storage = open(&runtime, &disk, SimStorageConfig::default());
        let cold = ColdFile::new(storage);

        let constructed = cold.len();
        drop(cold);
        assert!(matches!(
            disk.open(runtime.handle(), SimStorageConfig::default()),
            Err(SimOpenError::AlreadyOpen)
        ));

        drop(constructed);
        disk.open(runtime.handle(), SimStorageConfig::default())
            .expect("dropping the last unpolled future released the session");
    }

    #[test]
    fn memory_file_passes_cold_conformance() {
        let file = MemoryFile::new(MemoryFileConfig {
            max_file_bytes: 64,
            max_read_bytes: 16,
            max_write_bytes: 16,
            max_read_chunk: 2,
            max_write_chunk: 2,
        })
        .expect("memory file config is valid");
        let mut runtime = SimRuntime::default();
        runtime
            .block_on(crate::conformance::check_cold_empty_file(ColdFile::new(
                file,
            )))
            .expect("runtime completes")
            .expect("memory file satisfies cold conformance");
    }

    #[test]
    fn simulated_storage_passes_cold_conformance() {
        let mut runtime = SimRuntime::default();
        let storage = open(&runtime, &SimDisk::default(), SimStorageConfig::default());
        runtime
            .block_on(crate::conformance::check_cold_empty_file(ColdFile::new(
                storage,
            )))
            .expect("runtime completes")
            .expect("simulated storage satisfies cold conformance");
    }

    #[test]
    fn simulated_storage_with_latency_passes_cold_conformance() {
        let mut runtime = SimRuntime::default();
        let storage = open(&runtime, &SimDisk::default(), delayed_config());
        runtime
            .block_on(crate::conformance::check_cold_empty_file(ColdFile::new(
                storage,
            )))
            .expect("runtime completes")
            .expect("delayed simulated storage satisfies cold conformance");
    }

    #[test]
    fn memory_backed_cold_futures_are_send() {
        fn assert_send<T: Send>(_future: T) {}

        let file = MemoryFile::new(MemoryFileConfig::default()).expect("default config is valid");
        let cold = ColdFile::new(file);
        assert_send(cold.read_at(ReadAtRequest::new(0, Vec::new())));
        assert_send(cold.write_at(WriteAtRequest::new(0, Vec::new())));
        assert_send(cold.set_len(0));
        assert_send(cold.len());
        assert_send(cold.sync());
    }
}
