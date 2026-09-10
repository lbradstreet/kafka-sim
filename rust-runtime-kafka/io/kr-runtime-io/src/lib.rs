//! Owned asynchronous I/O contracts and deterministic reference providers.
//!
//! Every I/O domain is split into two layers. Application code uses the cold
//! handles — [`ColdFile`], [`network::ColdNetwork`] with its
//! [`network::ColdListener`] and [`network::ColdStream`], and
//! [`datagram::ColdDatagramNetwork`] with [`datagram::ColdDatagramSocket`]:
//! calling an operation method constructs an owned future and admits nothing;
//! the first poll attempts admission exactly once, and a never-polled future
//! has not started. Providers and systems code use the warm `*Submit` traits,
//! where a `submit_*` call attempts admission during the method invocation
//! and the returned future is a completion ticket. In both layers, dropping a
//! future after admission abandons only the response: an admitted side effect
//! still runs. The staged migration that produced this split is recorded in
//! `COLD-IO-FUTURES-PROPOSAL.md`.

#![forbid(unsafe_code)]

use std::future::Future;

/// Shared body of every cold-facade operation method.
///
/// The warm response is submitted from the retained handle at first poll and
/// kept in an inner scope so it is dropped before that handle on completion
/// and cancellation alike: a final handle must not tear down while the
/// response still owns a registered waker. The handle is captured when the
/// future is constructed, so an unpolled operation keeps the underlying
/// resource alive until the future is dropped without admitting anything.
pub(crate) async fn cold_submit<H, R: Future>(
    handle: H,
    submit: impl FnOnce(&H) -> R,
) -> R::Output {
    let output = {
        let response = submit(&handle);
        response.await
    };
    drop(handle);
    output
}

pub mod completion;
pub mod datagram;
pub mod latency;
pub mod network;
pub mod storage;

pub use kr_shared_bytes::SharedBytes;

pub use latency::{SIM_LATENCY_MODEL_VERSION, SimLatency, SimLatencyError, SimLatencyModel};
pub use storage::{
    ColdFile, FileIoStatus, FileIoSubmit, FileLength, MemoryFile, MemoryFileConfig,
    MemoryFileOpenError, MemoryFileStatus, ReadAtFailure, ReadAtRequest, ReadAtSuccess,
    SIM_FSYNC_GATE_VERSION, SIM_FSYNC_PAGE_BYTES, SIM_PIPELINE_MODEL_VERSION, SendFileIoSubmit,
    SetLenSuccess, SimCrashError, SimCrashModel, SimDisk, SimFault, SimFaultError, SimFsyncFailure,
    SimOpenError, SimOutcome, SimPipelineModel, SimRandomSources, SimStorage, SimStorageConfig,
    StorageError, StorageOperation, SyncSuccess, WriteAtFailure, WriteAtRequest, WriteAtSuccess,
};

#[cfg(any(test, feature = "test-support"))]
pub mod conformance;
