//! Reusable deterministic scenarios for provider tests and diagnostic tools.

use kr_runtime::{
    CompletionCertainty, RuntimeConfig, RuntimeSnapshot, SimDuration, SimInstant, SimRuntime,
};
use kr_runtime_io::{SimDisk, SimLatencyModel, SimPipelineModel, SimStorage, SimStorageConfig};

use super::{FileRing, FileRingConfig};
use crate::{
    AppendRequest, ReadRequest, RingCursor, RingError, RingPosition, RingReader, RingStatus,
    RingWriter, SyncSuccess,
};

/// Name of the file-ring wrap and recovery scenario shared with the test suite.
pub const WRAP_RECOVERY_SCENARIO: &str =
    "trim_checkpoint_releases_space_and_recovery_crosses_implicit_wrap";

/// Simulated-storage bounds matched to one file-ring configuration.
///
/// The file bound is the ring's exact physical length; transfer bounds are
/// wide enough not to split requests. Scenarios that need short transfers or
/// other provider pressure override the returned fields directly.
#[must_use]
pub fn sim_storage_config(
    config: FileRingConfig,
    default_latency: SimDuration,
    max_scripted_faults: usize,
) -> SimStorageConfig {
    SimStorageConfig {
        max_file_bytes: usize::try_from(
            config
                .physical_file_bytes()
                .expect("scenario physical file length is valid"),
        )
        .expect("scenario file length fits usize"),
        max_read_bytes: 4_096,
        max_write_bytes: 4_096,
        max_read_chunk: 4_096,
        max_write_chunk: 4_096,
        max_in_flight: 32,
        // 32 operations at the 4 KiB transfer maximum, so the byte budget is
        // non-binding here for the same reason the transfer bounds are.
        max_outstanding_bytes: 32 * 4_096,
        max_scripted_faults,
        default_latency,
        latency_model: SimLatencyModel::Fixed,
        pipeline_model: SimPipelineModel::Serial,
    }
}

/// Creates an empty simulated file ring and returns its storage session.
///
/// # Panics
///
/// Panics when the simulated file cannot be opened or the ring cannot be
/// created; scenario setup failures are test bugs, not outcomes under test.
#[must_use]
pub fn create_sim_ring(
    runtime: &mut SimRuntime,
    disk: &SimDisk,
    config: FileRingConfig,
    storage_config: SimStorageConfig,
) -> (FileRing<SimStorage>, SimStorage) {
    let storage = disk
        .open(runtime.handle(), storage_config)
        .expect("open simulated file");
    let ring = runtime
        .block_on(FileRing::create(runtime.handle(), storage.clone(), config))
        .expect("runtime completes create")
        .expect("file ring creates");
    (ring, storage)
}

/// Recovers a simulated file ring and returns its storage session.
///
/// # Panics
///
/// Panics when the simulated file cannot be reopened or recovery fails;
/// scenario setup failures are test bugs, not outcomes under test.
#[must_use]
pub fn open_sim_ring(
    runtime: &mut SimRuntime,
    disk: &SimDisk,
    config: FileRingConfig,
    storage_config: SimStorageConfig,
) -> (FileRing<SimStorage>, SimStorage) {
    let storage = disk
        .open(runtime.handle(), storage_config)
        .expect("reopen simulated file");
    let ring = runtime
        .block_on(FileRing::open(runtime.handle(), storage.clone(), config))
        .expect("runtime completes open")
        .expect("file ring opens");
    (ring, storage)
}

/// One bounded, assertion-bearing execution of the wrap and recovery scenario.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WrapRecoveryTrace {
    /// Ring configuration used by the scenario.
    pub config: FileRingConfig,
    /// Simulated latency charged to every lower-level storage operation.
    pub storage_latency: SimDuration,
    /// Ordered operation and lifecycle observations.
    pub steps: Vec<WrapRecoveryStep>,
    /// Terminal deterministic runtime state after actor shutdown.
    pub runtime: RuntimeSnapshot,
}

/// One operation boundary and the exact ring status observed immediately after it.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WrapRecoveryStep {
    /// Diagnostic order within this scenario; it is not a runtime trace sequence.
    pub sequence: u32,
    /// Stable visual grouping such as `pressure`, `wrap`, or `recovery`.
    pub phase: &'static str,
    /// Public operation or lifecycle action.
    pub operation: &'static str,
    /// Short explanation of why this boundary matters.
    pub description: &'static str,
    /// Virtual time before invoking the operation.
    pub started_at: SimInstant,
    /// Virtual time when its observed response completed.
    pub completed_at: SimInstant,
    /// Structured result returned by the operation.
    pub detail: WrapRecoveryDetail,
    /// Exact read-only status probe after the operation, when a live handle exists.
    pub status: Option<RingStatus>,
}

/// Structured result for a [`WrapRecoveryStep`].
#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum WrapRecoveryDetail {
    /// A new empty file ring was initialized.
    Created,
    /// An append batch was atomically accepted.
    Appended {
        record_count: usize,
        payload_bytes: usize,
        first_position: RingPosition,
        next_cursor: RingCursor,
        crossed_physical_wrap: bool,
    },
    /// An append was rejected without changing logical state.
    AppendRejected {
        certainty: CompletionCertainty,
        error: RingError,
    },
    /// A consumer trim was accepted but not yet necessarily durable.
    Trimmed {
        requested: RingCursor,
        accepted_head: RingCursor,
    },
    /// A durability checkpoint was installed.
    Synced(SyncSuccess),
    /// The simulated storage session crashed.
    Crashed,
    /// The last complete checkpoint was recovered from disk.
    Reopened,
    /// Durable records were read after recovery.
    Read {
        requested: RingCursor,
        positions: Vec<RingPosition>,
        payload_markers: Vec<u8>,
    },
}

/// Runs the same physical-wrap scenario used by the file-ring unit test.
///
/// The returned status observations are explicit ordered `status()` calls in
/// this diagnostic workload and are not part of an uninstrumented workload.
/// Merely serializing or presenting the returned trace performs no ring
/// operations.
#[must_use]
pub fn run_wrap_recovery_trace(storage_latency: SimDuration) -> WrapRecoveryTrace {
    let mut runtime = SimRuntime::new(RuntimeConfig {
        seed: 0x7269_6e67_7472_6163,
        ..RuntimeConfig::default()
    });
    let disk = SimDisk::default();
    let config = wrapping_config();
    let storage_config = sim_storage_config(config, storage_latency, 64);
    let mut steps = Vec::with_capacity(14);

    let storage = disk
        .open(runtime.handle(), storage_config)
        .expect("open simulated file for wrap scenario");
    let started_at = runtime.snapshot().now;
    let ring = runtime
        .block_on(FileRing::create(runtime.handle(), storage.clone(), config))
        .expect("runtime completes file-ring creation")
        .expect("file ring creates");
    let completed_at = runtime.snapshot().now;
    let initial = observe_status(&mut runtime, &ring);
    assert_empty_status(initial, 1);
    push_step(
        &mut steps,
        "setup",
        "create",
        "Initialize two superblocks and an empty 128-byte data ring.",
        started_at,
        completed_at,
        WrapRecoveryDetail::Created,
        Some(initial),
    );

    let first = vec![1; 20];
    let second = vec![2; 20];
    let third = vec![3; 20];

    let started_at = runtime.snapshot().now;
    let appended = runtime
        .block_on(ring.append(AppendRequest::new(vec![first, second.clone()])))
        .expect("runtime completes initial append")
        .expect("two records fit");
    let completed_at = runtime.snapshot().now;
    let after_append = observe_status(&mut runtime, &ring);
    assert_eq!(after_append.accepted_tail, RingCursor::new(2));
    assert_eq!(after_append.durable_tail, RingCursor::START);
    assert_physical(after_append, 112, 16, 0, 0, 112, 1);
    push_step(
        &mut steps,
        "fill",
        "append",
        "Accept two 20-byte records; they occupy 112 physical bytes but remain invisible.",
        started_at,
        completed_at,
        WrapRecoveryDetail::Appended {
            record_count: appended.records.len(),
            payload_bytes: appended.records.iter().map(Vec::len).sum(),
            first_position: appended.first_position,
            next_cursor: appended.next_cursor,
            crossed_physical_wrap: false,
        },
        Some(after_append),
    );

    let started_at = runtime.snapshot().now;
    let first_sync = runtime
        .block_on(ring.sync())
        .expect("runtime completes initial sync")
        .expect("initial sync succeeds");
    let completed_at = runtime.snapshot().now;
    let after_first_sync = observe_status(&mut runtime, &ring);
    assert_eq!(first_sync.durable_tail, RingCursor::new(2));
    assert_physical(after_first_sync, 112, 16, 0, 112, 112, 2);
    push_step(
        &mut steps,
        "fill",
        "sync",
        "Fence both accepted records and publish metadata generation 2.",
        started_at,
        completed_at,
        WrapRecoveryDetail::Synced(first_sync),
        Some(after_first_sync),
    );

    let started_at = runtime.snapshot().now;
    let full = runtime
        .block_on(ring.append(AppendRequest::new(vec![third.clone()])))
        .expect("runtime completes capacity rejection")
        .expect_err("third record needs wrap space");
    let completed_at = runtime.snapshot().now;
    assert_eq!(full.certainty(), CompletionCertainty::NotApplied);
    assert!(matches!(
        full.error().error,
        RingError::PhysicalCapacityReached {
            protected: 112,
            requested: 72,
            limit: 128
        }
    ));
    let after_full = observe_status(&mut runtime, &ring);
    push_step(
        &mut steps,
        "pressure",
        "append",
        "Reject a wrap that needs a 16-byte end gap plus a 56-byte frame.",
        started_at,
        completed_at,
        WrapRecoveryDetail::AppendRejected {
            certainty: full.certainty(),
            error: full.error().error.clone(),
        },
        Some(after_full),
    );

    let started_at = runtime.snapshot().now;
    let trimmed = runtime
        .block_on(ring.trim(RingCursor::new(1)))
        .expect("runtime completes trim")
        .expect("trim accepted");
    let completed_at = runtime.snapshot().now;
    let after_trim = observe_status(&mut runtime, &ring);
    assert_eq!(after_trim.accepted_head, RingCursor::new(1));
    assert_eq!(after_trim.durable_head, RingCursor::START);
    assert_eq!(after_trim.pending_reclaim_records, 1);
    assert_physical(after_trim, 112, 16, 0, 112, 112, 2);
    push_step(
        &mut steps,
        "pressure",
        "trim",
        "Advance the accepted head; space stays protected until the trim is durable.",
        started_at,
        completed_at,
        WrapRecoveryDetail::Trimmed {
            requested: RingCursor::new(1),
            accepted_head: trimmed.accepted_head,
        },
        Some(after_trim),
    );

    let started_at = runtime.snapshot().now;
    let still_full = runtime
        .block_on(ring.append(AppendRequest::new(vec![third.clone()])))
        .expect("runtime completes pending-trim rejection")
        .expect_err("pending trim does not free space");
    let completed_at = runtime.snapshot().now;
    assert_eq!(still_full.certainty(), CompletionCertainty::NotApplied);
    assert!(matches!(
        still_full.error().error,
        RingError::PhysicalCapacityReached { .. }
    ));
    let after_still_full = observe_status(&mut runtime, &ring);
    push_step(
        &mut steps,
        "pressure",
        "append",
        "Show that an accepted-but-unsynced trim cannot release protected bytes.",
        started_at,
        completed_at,
        WrapRecoveryDetail::AppendRejected {
            certainty: still_full.certainty(),
            error: still_full.error().error.clone(),
        },
        Some(after_still_full),
    );

    let started_at = runtime.snapshot().now;
    let trim_sync = runtime
        .block_on(ring.sync())
        .expect("runtime completes trim sync")
        .expect("trim sync succeeds");
    let completed_at = runtime.snapshot().now;
    let after_trim_sync = observe_status(&mut runtime, &ring);
    assert_eq!(trim_sync.reclaimed_records, 1);
    assert_eq!(trim_sync.reclaimed_payload_bytes, 20);
    assert_physical(after_trim_sync, 56, 72, 56, 112, 112, 3);
    push_step(
        &mut steps,
        "reclaim",
        "sync",
        "Checkpoint the trim and reclaim record 0, opening 72 bytes of physical space.",
        started_at,
        completed_at,
        WrapRecoveryDetail::Synced(trim_sync),
        Some(after_trim_sync),
    );

    let started_at = runtime.snapshot().now;
    let wrapped = runtime
        .block_on(ring.append(AppendRequest::new(vec![third.clone()])))
        .expect("runtime completes wrapped append")
        .expect("append succeeds after reclaim");
    let completed_at = runtime.snapshot().now;
    let after_wrap = observe_status(&mut runtime, &ring);
    assert_eq!(wrapped.first_position, RingPosition::new(2));
    assert_physical(after_wrap, 128, 0, 56, 112, 56, 3);
    push_step(
        &mut steps,
        "wrap",
        "append",
        "Consume the end gap, wrap to offset 0, and accept logical position 2.",
        started_at,
        completed_at,
        WrapRecoveryDetail::Appended {
            record_count: wrapped.records.len(),
            payload_bytes: wrapped.records.iter().map(Vec::len).sum(),
            first_position: wrapped.first_position,
            next_cursor: wrapped.next_cursor,
            crossed_physical_wrap: true,
        },
        Some(after_wrap),
    );

    let started_at = runtime.snapshot().now;
    let wrap_sync = runtime
        .block_on(ring.sync())
        .expect("runtime completes wrap sync")
        .expect("wrap sync succeeds");
    let completed_at = runtime.snapshot().now;
    let after_wrap_sync = observe_status(&mut runtime, &ring);
    assert_eq!(wrap_sync.durable_head, RingCursor::new(1));
    assert_eq!(wrap_sync.durable_tail, RingCursor::new(3));
    assert_physical(after_wrap_sync, 128, 0, 56, 56, 56, 4);
    push_step(
        &mut steps,
        "wrap",
        "sync",
        "Fence the full-circle checkpoint where equal offsets mean full, not empty.",
        started_at,
        completed_at,
        WrapRecoveryDetail::Synced(wrap_sync),
        Some(after_wrap_sync),
    );

    let started_at = runtime.snapshot().now;
    storage.crash();
    drop(ring);
    let completed_at = runtime.snapshot().now;
    push_step(
        &mut steps,
        "recovery",
        "crash",
        "Lose the live storage session after the wrapped checkpoint is durable.",
        started_at,
        completed_at,
        WrapRecoveryDetail::Crashed,
        None,
    );

    let reopened_storage = disk
        .open(runtime.handle(), storage_config)
        .expect("reopen simulated file after crash");
    let started_at = runtime.snapshot().now;
    let reopened = runtime
        .block_on(FileRing::open(
            runtime.handle(),
            reopened_storage.clone(),
            config,
        ))
        .expect("runtime completes file-ring recovery")
        .expect("file ring reopens");
    let completed_at = runtime.snapshot().now;
    let recovered = observe_status(&mut runtime, &reopened);
    assert_eq!(recovered.accepted_head, RingCursor::new(1));
    assert_eq!(recovered.accepted_tail, RingCursor::new(3));
    assert_eq!(recovered.durable_head, RingCursor::new(1));
    assert_eq!(recovered.durable_tail, RingCursor::new(3));
    assert_physical(recovered, 128, 0, 56, 56, 56, 4);
    push_step(
        &mut steps,
        "recovery",
        "open",
        "Select metadata generation 4 and rebuild records 1 and 2 across wrap.",
        started_at,
        completed_at,
        WrapRecoveryDetail::Reopened,
        Some(recovered),
    );

    let started_at = runtime.snapshot().now;
    let page = runtime
        .block_on(reopened.read(ReadRequest::new(RingCursor::new(1), 3, 60)))
        .expect("runtime completes wrapped read")
        .expect("wrapped read succeeds");
    let completed_at = runtime.snapshot().now;
    let positions = page.records.iter().map(|record| record.position).collect();
    let payload_markers = page
        .records
        .iter()
        .map(|record| record.buffer[0])
        .collect::<Vec<_>>();
    assert_eq!(payload_markers, vec![2, 3]);
    let after_read = observe_status(&mut runtime, &reopened);
    push_step(
        &mut steps,
        "recovery",
        "read",
        "Read durable positions 1 and 2 in logical order despite physical wrap.",
        started_at,
        completed_at,
        WrapRecoveryDetail::Read {
            requested: RingCursor::new(1),
            positions,
            payload_markers,
        },
        Some(after_read),
    );

    let started_at = runtime.snapshot().now;
    let trim_all = runtime
        .block_on(reopened.trim(RingCursor::new(3)))
        .expect("runtime completes trim-all")
        .expect("trim-all succeeds");
    let completed_at = runtime.snapshot().now;
    let after_trim_all = observe_status(&mut runtime, &reopened);
    assert_eq!(after_trim_all.pending_reclaim_records, 2);
    push_step(
        &mut steps,
        "cleanup",
        "trim",
        "Accept consumer progress through the recovered durable tail.",
        started_at,
        completed_at,
        WrapRecoveryDetail::Trimmed {
            requested: RingCursor::new(3),
            accepted_head: trim_all.accepted_head,
        },
        Some(after_trim_all),
    );

    let started_at = runtime.snapshot().now;
    let empty_sync = runtime
        .block_on(reopened.sync())
        .expect("runtime completes empty sync")
        .expect("empty sync succeeds");
    let completed_at = runtime.snapshot().now;
    let empty = observe_status(&mut runtime, &reopened);
    assert_empty_status(empty, 5);
    push_step(
        &mut steps,
        "cleanup",
        "sync",
        "Checkpoint the empty interval and canonicalize every physical offset to 0.",
        started_at,
        completed_at,
        WrapRecoveryDetail::Synced(empty_sync),
        Some(empty),
    );

    drop(reopened);
    drop(reopened_storage);
    runtime.shutdown().expect("scenario runtime shuts down");

    WrapRecoveryTrace {
        config,
        storage_latency,
        steps,
        runtime: runtime.snapshot(),
    }
}

fn wrapping_config() -> FileRingConfig {
    FileRingConfig {
        limits: crate::RingLimits {
            max_record_bytes: 20,
            max_live_records: 3,
            max_live_payload_bytes: 60,
            max_read_records: 3,
            max_read_bytes: 60,
            max_batch_records: 2,
            max_batch_bytes: 40,
        },
        data_capacity_bytes: 128,
        max_io_request_bytes: 4_096,
        command_queue_capacity: 8,
    }
}

fn observe_status(runtime: &mut SimRuntime, ring: &FileRing<SimStorage>) -> RingStatus {
    runtime
        .block_on(ring.status())
        .expect("runtime completes diagnostic status")
        .expect("diagnostic status succeeds")
}

#[allow(clippy::too_many_arguments)]
fn push_step(
    steps: &mut Vec<WrapRecoveryStep>,
    phase: &'static str,
    operation: &'static str,
    description: &'static str,
    started_at: SimInstant,
    completed_at: SimInstant,
    detail: WrapRecoveryDetail,
    status: Option<RingStatus>,
) {
    let sequence = u32::try_from(steps.len()).expect("scenario step count fits u32");
    steps.push(WrapRecoveryStep {
        sequence,
        phase,
        operation,
        description,
        started_at,
        completed_at,
        detail,
        status,
    });
}

fn assert_empty_status(status: RingStatus, generation: u64) {
    assert_eq!(status.accepted_head, status.accepted_tail);
    assert_eq!(status.durable_head, status.durable_tail);
    assert_eq!(status.accepted_head, status.durable_head);
    assert_eq!(status.retained_records, 0);
    assert_eq!(status.retained_payload_bytes, 0);
    assert_physical(status, 0, 128, 0, 0, 0, generation);
}

#[allow(clippy::too_many_arguments)]
fn assert_physical(
    status: RingStatus,
    protected_bytes: u64,
    free_bytes: u64,
    durable_head_offset: u64,
    durable_tail_offset: u64,
    accepted_tail_offset: u64,
    generation: u64,
) {
    let physical = status.physical.expect("file ring has physical status");
    assert_eq!(physical.data_capacity_bytes, 128);
    assert_eq!(physical.protected_bytes, protected_bytes);
    assert_eq!(physical.free_bytes, free_bytes);
    assert_eq!(physical.durable_head_offset, durable_head_offset);
    assert_eq!(physical.durable_tail_offset, durable_tail_offset);
    assert_eq!(physical.accepted_tail_offset, accepted_tail_offset);
    assert_eq!(physical.metadata_generation, generation);
    assert!(!physical.recovery_required);
}
