//! Reusable deterministic storage scenarios for tests and diagnostic tools.

use kr_runtime::{
    CompletionCertainty, RuntimeConfig, RuntimeSnapshot, SimDuration, SimInstant, SimRuntime,
};

use super::{
    FileIoStatus, FileIoSubmit, ReadAtRequest, ReadAtSuccess, SIM_FSYNC_GATE_VERSION,
    SimCrashModel, SimDisk, SimFault, SimFsyncFailure, SimOutcome, SimPipelineModel, SimStorage,
    SimStorageConfig, StorageError, StorageOperation, SyncSuccess, WriteAtRequest, WriteAtSuccess,
};
use crate::latency::SimLatencyModel;

/// Stable name of the focused durability scenario.
pub const STORAGE_DURABILITY_TRACE_SCENARIO: &str = "ambiguous_sync_clean_crash_durable_recovery";

/// Seed pinned by the focused durability scenario.
pub const STORAGE_DURABILITY_TRACE_SEED: u64 = 0x7374_6f72_6167_6501;

/// Default operation latency used by the focused durability scenario.
pub const STORAGE_DURABILITY_TRACE_LATENCY: SimDuration = SimDuration::from_nanos(5);

/// Latency of the injected ambiguous sync failure.
pub const STORAGE_DURABILITY_TRACE_FAULT_LATENCY: SimDuration = SimDuration::from_nanos(7);

const INITIAL_BYTES: &[u8] = b"BASE";
const DIRTY_SUFFIX: &[u8] = b"-dirty";
const KEPT_SUFFIX: &[u8] = b"-kept";
const TEMPORARY_PREFIX: &[u8] = b"TEMP";
const FINAL_BYTES: &[u8] = b"BASE-kept";

/// Exact accepted and durable byte images at one scenario boundary.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StorageImageSnapshot {
    pub accepted: Vec<u8>,
    pub durable: Vec<u8>,
}

/// Passive runtime, provider-status, and byte-image state at one boundary.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StorageTraceSnapshot {
    pub runtime: RuntimeSnapshot,
    pub status: FileIoStatus,
    pub image: StorageImageSnapshot,
}

/// Exact request and result of one successful positional write.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StorageWriteObservation {
    pub request: WriteAtRequest,
    pub result: WriteAtSuccess,
}

/// Exact request and result of one successful positional read.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StorageReadObservation {
    pub request: ReadAtRequest,
    pub result: ReadAtSuccess,
}

/// Structured result recorded at one storage scenario boundary.
#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum StorageTraceDetail {
    WriteCompleted(StorageWriteObservation),
    FaultInjected {
        fault: SimFault,
    },
    SyncFailed {
        certainty: CompletionCertainty,
        error: StorageError,
    },
    Crashed {
        model: SimCrashModel,
    },
    Reopened,
    SyncCompleted {
        result: SyncSuccess,
    },
    ReadCompleted(StorageReadObservation),
}

/// One assertion-bearing operation boundary in the focused storage scenario.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StorageTraceStep {
    pub sequence: u32,
    pub phase: &'static str,
    pub operation: &'static str,
    pub description: &'static str,
    pub started_at: SimInstant,
    pub completed_at: SimInstant,
    pub before: StorageTraceSnapshot,
    pub after: StorageTraceSnapshot,
    pub detail: StorageTraceDetail,
}

/// Bounded deterministic execution of the focused storage durability scenario.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StorageDurabilityTrace {
    pub scenario: &'static str,
    pub seed: u64,
    pub config: SimStorageConfig,
    pub started_at: SimInstant,
    pub completed_at: SimInstant,
    pub initial: StorageTraceSnapshot,
    pub terminal: StorageTraceSnapshot,
    pub steps: Vec<StorageTraceStep>,
    pub runtime: RuntimeSnapshot,
}

#[derive(Debug)]
struct StorageModel {
    accepted: Vec<u8>,
    durable: Vec<u8>,
    sync_candidate: Vec<u8>,
    pending_faults: usize,
    fault_hits: u64,
    closed: bool,
}

impl StorageModel {
    fn new() -> Self {
        Self {
            accepted: INITIAL_BYTES.to_vec(),
            durable: INITIAL_BYTES.to_vec(),
            sync_candidate: INITIAL_BYTES.to_vec(),
            pending_faults: 0,
            fault_hits: 0,
            closed: false,
        }
    }

    fn write(&mut self, request: &WriteAtRequest, result: &WriteAtSuccess) {
        assert!(!self.closed, "model write requires an open session");
        assert_eq!(result.buffer, request.buffer, "write returns its buffer");
        assert_eq!(result.bytes_written, request.buffer.len());
        let start = usize::try_from(request.offset).expect("focused write offset fits usize");
        let end = start
            .checked_add(result.bytes_written)
            .expect("focused write range does not overflow");
        assert!(
            end <= super::SIM_FSYNC_PAGE_BYTES,
            "focused writes stay within one fsync page"
        );
        if self.accepted.len() < end {
            self.accepted.resize(end, 0);
        }
        self.accepted[start..end].copy_from_slice(&request.buffer[..result.bytes_written]);

        // Every focused write dirties the only page in this tiny file. The
        // independent model therefore makes the complete accepted page
        // eligible for the next sync.
        self.sync_candidate.clone_from(&self.accepted);
    }

    fn inject_sync_fault(&mut self) {
        assert!(
            !self.closed,
            "model fault injection requires an open session"
        );
        self.pending_faults += 1;
    }

    fn fail_sync_before_effect(&mut self) {
        assert!(!self.closed, "model sync requires an open session");
        self.pending_faults = self
            .pending_faults
            .checked_sub(1)
            .expect("focused sync consumes its injected fault");
        self.fault_hits += 1;
        // RetainDirtyPagesV1 leaves the independently tracked candidate
        // eligible, but the failed operation does not change durable bytes.
    }

    fn submit_sync(&mut self) {
        assert!(!self.closed, "model sync requires an open session");
        self.durable.clone_from(&self.sync_candidate);
    }

    fn crash_clean(&mut self) {
        assert!(!self.closed, "model crash requires an open session");
        self.accepted.clone_from(&self.durable);
        self.sync_candidate.clone_from(&self.durable);
        self.closed = true;
    }

    fn reopen(&mut self) {
        assert!(self.closed, "model reopen requires a closed session");
        self.accepted.clone_from(&self.durable);
        self.sync_candidate.clone_from(&self.durable);
        self.pending_faults = 0;
        self.fault_hits = 0;
        self.closed = false;
    }

    fn check_read(&self, request: &ReadAtRequest, result: &ReadAtSuccess) {
        assert!(!self.closed, "model read requires an open session");
        let start = usize::try_from(request.offset).expect("focused read offset fits usize");
        let expected_len = request
            .buffer
            .len()
            .min(self.accepted.len().saturating_sub(start));
        assert_eq!(result.bytes_read, expected_len);
        assert_eq!(result.buffer, self.accepted[start..start + expected_len]);
    }

    fn has_fsync_gated_data(&self) -> bool {
        self.accepted != self.sync_candidate
    }
}

struct StepStart {
    phase: &'static str,
    operation: &'static str,
    description: &'static str,
    before: StorageTraceSnapshot,
}

#[derive(Clone, Copy)]
struct ProviderContext<'a> {
    storage: &'a SimStorage,
    disk: &'a SimDisk,
    config: SimStorageConfig,
}

impl<'a> ProviderContext<'a> {
    const fn new(storage: &'a SimStorage, disk: &'a SimDisk, config: SimStorageConfig) -> Self {
        Self {
            storage,
            disk,
            config,
        }
    }
}

fn config() -> SimStorageConfig {
    SimStorageConfig {
        max_file_bytes: 32,
        max_read_bytes: 16,
        max_write_bytes: 16,
        max_read_chunk: 16,
        max_write_chunk: 16,
        max_in_flight: 2,
        // Two operations at the 16-byte maximum: non-binding, so the trace
        // snapshots keep exercising the other limits.
        max_outstanding_bytes: 32,
        max_scripted_faults: 2,
        default_latency: STORAGE_DURABILITY_TRACE_LATENCY,
        latency_model: SimLatencyModel::Fixed,
        pipeline_model: SimPipelineModel::Serial,
    }
}

fn capture(runtime: &SimRuntime, provider: ProviderContext<'_>) -> StorageTraceSnapshot {
    let status = provider.storage.status();
    let accepted = provider.storage.session.borrow().accepted.clone();
    StorageTraceSnapshot {
        runtime: runtime.snapshot(),
        status,
        image: StorageImageSnapshot {
            accepted,
            durable: provider.disk.durable_bytes(),
        },
    }
}

fn assert_provider_matches(
    snapshot: &StorageTraceSnapshot,
    model: &StorageModel,
    config: SimStorageConfig,
) {
    assert_eq!(snapshot.image.accepted, model.accepted);
    assert_eq!(snapshot.image.durable, model.durable);
    assert_eq!(snapshot.status.accepted_len, model.accepted.len() as u64);
    assert_eq!(snapshot.status.durable_len, model.durable.len() as u64);
    assert_eq!(
        snapshot.status.has_fsync_gated_data,
        model.has_fsync_gated_data()
    );
    assert_eq!(snapshot.status.fsync_gate_version, SIM_FSYNC_GATE_VERSION);
    assert_eq!(snapshot.status.in_flight, 0);
    assert_eq!(snapshot.status.in_flight_limit, config.max_in_flight);
    assert_eq!(snapshot.status.pending_faults, model.pending_faults);
    assert_eq!(snapshot.status.fault_hits, model.fault_hits);
    assert_eq!(snapshot.status.closed, model.closed);
    assert_eq!(
        snapshot.runtime.reproduction.config.seed,
        STORAGE_DURABILITY_TRACE_SEED
    );
    assert!(!snapshot.runtime.stopped);
}

fn start_step(
    runtime: &SimRuntime,
    provider: ProviderContext<'_>,
    model: &StorageModel,
    phase: &'static str,
    operation: &'static str,
    description: &'static str,
) -> StepStart {
    let before = capture(runtime, provider);
    assert_provider_matches(&before, model, provider.config);
    StepStart {
        phase,
        operation,
        description,
        before,
    }
}

fn finish_step(
    steps: &mut Vec<StorageTraceStep>,
    runtime: &SimRuntime,
    provider: ProviderContext<'_>,
    model: &StorageModel,
    start: StepStart,
    detail: StorageTraceDetail,
) {
    let after = capture(runtime, provider);
    assert_provider_matches(&after, model, provider.config);
    assert!(after.runtime.now >= start.before.runtime.now);
    let sequence = u32::try_from(steps.len()).expect("focused trace step count fits u32");
    steps.push(StorageTraceStep {
        sequence,
        phase: start.phase,
        operation: start.operation,
        description: start.description,
        started_at: start.before.runtime.now,
        completed_at: after.runtime.now,
        before: start.before,
        after,
        detail,
    });
}

fn write_step(
    steps: &mut Vec<StorageTraceStep>,
    runtime: &mut SimRuntime,
    provider: ProviderContext<'_>,
    model: &mut StorageModel,
    phase: &'static str,
    description: &'static str,
    request: WriteAtRequest,
) {
    let start = start_step(runtime, provider, model, phase, "write-at", description);
    let result = runtime
        .block_on(provider.storage.submit_write_at(request.clone()))
        .expect("runtime drives focused write")
        .expect("focused write succeeds");
    model.write(&request, &result);
    finish_step(
        steps,
        runtime,
        provider,
        model,
        start,
        StorageTraceDetail::WriteCompleted(StorageWriteObservation { request, result }),
    );
}

/// Runs the focused durability scenario used by diagnostics and its unit test.
///
/// Every boundary compares the provider's exact accepted and durable bytes
/// against a separate vector model before retaining the step. Passive
/// snapshots do not drive the runtime or alter the simulated storage state.
#[must_use]
pub fn run_storage_durability_trace() -> StorageDurabilityTrace {
    let config = config();
    let mut runtime = SimRuntime::new(RuntimeConfig {
        seed: STORAGE_DURABILITY_TRACE_SEED,
        max_tasks: 8,
        max_timers: 8,
        max_steps_per_run: 128,
        max_time: Some(SimInstant::from_nanos(1_000)),
        start_time: SimInstant::ZERO,
    });
    let disk = SimDisk::from_durable_bytes(INITIAL_BYTES.to_vec());
    let mut storage = disk
        .open(runtime.handle(), config)
        .expect("focused storage config is valid");
    let mut model = StorageModel::new();
    let initial = capture(&runtime, ProviderContext::new(&storage, &disk, config));
    assert_provider_matches(&initial, &model, config);
    let started_at = initial.runtime.now;
    let mut steps = Vec::with_capacity(11);

    write_step(
        &mut steps,
        &mut runtime,
        ProviderContext::new(&storage, &disk, config),
        &mut model,
        "ambiguous-sync",
        "Append dirty bytes that have not crossed a durability fence.",
        WriteAtRequest::new(INITIAL_BYTES.len() as u64, DIRTY_SUFFIX.to_vec()),
    );

    let fault = SimFault::new(
        StorageOperation::Sync,
        STORAGE_DURABILITY_TRACE_FAULT_LATENCY,
        SimOutcome::MayHaveAppliedBefore,
    )
    .with_fsync_failure(SimFsyncFailure::RetainDirtyPagesV1);
    let start = start_step(
        &runtime,
        ProviderContext::new(&storage, &disk, config),
        &model,
        "ambiguous-sync",
        "inject-fault",
        "Script an ambiguous pre-effect sync failure while retaining dirty pages.",
    );
    storage
        .inject(fault)
        .expect("focused sync fault is accepted");
    model.inject_sync_fault();
    finish_step(
        &mut steps,
        &runtime,
        ProviderContext::new(&storage, &disk, config),
        &model,
        start,
        StorageTraceDetail::FaultInjected { fault },
    );

    let start = start_step(
        &runtime,
        ProviderContext::new(&storage, &disk, config),
        &model,
        "ambiguous-sync",
        "sync",
        "Return an ambiguous sync error without changing the durable image.",
    );
    let failure = runtime
        .block_on(storage.submit_sync())
        .expect("runtime drives focused failing sync")
        .expect_err("focused sync fault fires");
    let (certainty, error) = failure.into_parts();
    assert_eq!(certainty, CompletionCertainty::MayHaveApplied);
    assert_eq!(
        error,
        StorageError::Injected {
            operation: StorageOperation::Sync
        }
    );
    model.fail_sync_before_effect();
    finish_step(
        &mut steps,
        &runtime,
        ProviderContext::new(&storage, &disk, config),
        &model,
        start,
        StorageTraceDetail::SyncFailed { certainty, error },
    );

    let start = start_step(
        &runtime,
        ProviderContext::new(&storage, &disk, config),
        &model,
        "rollback",
        "crash",
        "Crash with clean rollback so only the original durable bytes survive.",
    );
    storage.crash();
    model.crash_clean();
    finish_step(
        &mut steps,
        &runtime,
        ProviderContext::new(&storage, &disk, config),
        &model,
        start,
        StorageTraceDetail::Crashed {
            model: SimCrashModel::CleanRollback,
        },
    );

    let start = start_step(
        &runtime,
        ProviderContext::new(&storage, &disk, config),
        &model,
        "rollback",
        "reopen",
        "Reopen from the durable image after discarding the dirty suffix.",
    );
    let reopened = disk
        .open(runtime.handle(), config)
        .expect("focused storage reopens after crash");
    model.reopen();
    finish_step(
        &mut steps,
        &runtime,
        ProviderContext::new(&reopened, &disk, config),
        &model,
        start,
        StorageTraceDetail::Reopened,
    );
    storage = reopened;

    write_step(
        &mut steps,
        &mut runtime,
        ProviderContext::new(&storage, &disk, config),
        &mut model,
        "durable-write",
        "Append replacement bytes after recovery.",
        WriteAtRequest::new(INITIAL_BYTES.len() as u64, KEPT_SUFFIX.to_vec()),
    );

    let start = start_step(
        &runtime,
        ProviderContext::new(&storage, &disk, config),
        &model,
        "durable-write",
        "sync",
        "Fence the recovered replacement bytes durably.",
    );
    let result = runtime
        .block_on(storage.submit_sync())
        .expect("runtime drives focused successful sync")
        .expect("focused replacement sync succeeds");
    model.submit_sync();
    assert_eq!(result.durable_len, model.durable.len() as u64);
    finish_step(
        &mut steps,
        &runtime,
        ProviderContext::new(&storage, &disk, config),
        &model,
        start,
        StorageTraceDetail::SyncCompleted { result },
    );

    write_step(
        &mut steps,
        &mut runtime,
        ProviderContext::new(&storage, &disk, config),
        &mut model,
        "overwrite-rollback",
        "Overwrite the durable prefix without syncing it.",
        WriteAtRequest::new(0, TEMPORARY_PREFIX.to_vec()),
    );

    let start = start_step(
        &runtime,
        ProviderContext::new(&storage, &disk, config),
        &model,
        "overwrite-rollback",
        "crash",
        "Crash again so the temporary prefix is rolled back.",
    );
    storage.crash();
    model.crash_clean();
    finish_step(
        &mut steps,
        &runtime,
        ProviderContext::new(&storage, &disk, config),
        &model,
        start,
        StorageTraceDetail::Crashed {
            model: SimCrashModel::CleanRollback,
        },
    );

    let start = start_step(
        &runtime,
        ProviderContext::new(&storage, &disk, config),
        &model,
        "overwrite-rollback",
        "reopen",
        "Reopen from the durable replacement image.",
    );
    let reopened = disk
        .open(runtime.handle(), config)
        .expect("focused storage reopens after second crash");
    model.reopen();
    finish_step(
        &mut steps,
        &runtime,
        ProviderContext::new(&reopened, &disk, config),
        &model,
        start,
        StorageTraceDetail::Reopened,
    );
    storage = reopened;

    let request = ReadAtRequest::new(0, vec![0; FINAL_BYTES.len()]);
    let start = start_step(
        &runtime,
        ProviderContext::new(&storage, &disk, config),
        &model,
        "verify",
        "read-at",
        "Read the final recovered bytes and verify both rollback boundaries.",
    );
    let result = runtime
        .block_on(storage.submit_read_at(request.clone()))
        .expect("runtime drives focused final read")
        .expect("focused final read succeeds");
    model.check_read(&request, &result);
    assert_eq!(result.buffer, FINAL_BYTES);
    finish_step(
        &mut steps,
        &runtime,
        ProviderContext::new(&storage, &disk, config),
        &model,
        start,
        StorageTraceDetail::ReadCompleted(StorageReadObservation { request, result }),
    );

    let terminal = capture(&runtime, ProviderContext::new(&storage, &disk, config));
    assert_provider_matches(&terminal, &model, config);
    assert_eq!(terminal.image.accepted, FINAL_BYTES);
    assert_eq!(terminal.image.durable, FINAL_BYTES);
    let completed_at = terminal.runtime.now;
    assert_eq!(steps.len(), 11);
    assert_eq!(initial, steps[0].before);
    assert_eq!(steps.last().map(|step| &step.after), Some(&terminal));
    assert!(
        steps.windows(2).all(|pair| pair[0].after == pair[1].before),
        "focused storage steps must form one continuous state history"
    );

    drop(storage);
    runtime
        .shutdown()
        .expect("focused storage runtime shuts down");

    StorageDurabilityTrace {
        scenario: STORAGE_DURABILITY_TRACE_SCENARIO,
        seed: STORAGE_DURABILITY_TRACE_SEED,
        config,
        started_at,
        completed_at,
        initial,
        terminal,
        steps,
        runtime: runtime.snapshot(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn focused_storage_durability_trace_replays_and_covers_contract_edges() {
        let first = run_storage_durability_trace();
        let repeated = run_storage_durability_trace();
        assert_eq!(first, repeated, "focused storage trace did not replay");

        assert_eq!(first.scenario, STORAGE_DURABILITY_TRACE_SCENARIO);
        assert_eq!(first.seed, STORAGE_DURABILITY_TRACE_SEED);
        assert_eq!(first.started_at, SimInstant::ZERO);
        assert_eq!(first.completed_at, SimInstant::from_nanos(32));
        assert_eq!(first.runtime.now, first.completed_at);
        assert!(first.runtime.stopped);
        assert_ne!(first.config.default_latency, SimDuration::ZERO);
        assert_eq!(first.initial.image.accepted, INITIAL_BYTES);
        assert_eq!(first.initial.image.durable, INITIAL_BYTES);
        assert_eq!(first.steps.len(), 11);
        assert!(
            first
                .steps
                .iter()
                .enumerate()
                .all(|(index, step)| step.sequence == index as u32
                    && step.started_at == step.before.runtime.now
                    && step.completed_at == step.after.runtime.now
                    && step.completed_at >= step.started_at)
        );

        let mut writes = 0;
        let mut injected_fault = false;
        let mut ambiguous_sync = false;
        let mut crashes = 0;
        let mut reopens = 0;
        let mut successful_sync = false;
        let mut final_read = false;
        for step in &first.steps {
            match &step.detail {
                StorageTraceDetail::WriteCompleted(observation) => {
                    writes += 1;
                    assert_eq!(observation.result.buffer, observation.request.buffer);
                    assert_eq!(
                        observation.result.bytes_written,
                        observation.request.buffer.len()
                    );
                }
                StorageTraceDetail::FaultInjected { fault } => {
                    injected_fault = fault.operation == StorageOperation::Sync
                        && fault.outcome == SimOutcome::MayHaveAppliedBefore
                        && fault.fsync_failure == Some(SimFsyncFailure::RetainDirtyPagesV1)
                        && step.after.status.pending_faults == 1;
                }
                StorageTraceDetail::SyncFailed { certainty, error } => {
                    ambiguous_sync = *certainty == CompletionCertainty::MayHaveApplied
                        && *error
                            == StorageError::Injected {
                                operation: StorageOperation::Sync,
                            }
                        && step.after.image.accepted == b"BASE-dirty"
                        && step.after.image.durable == INITIAL_BYTES
                        && step.after.status.pending_faults == 0
                        && step.after.status.fault_hits == 1;
                }
                StorageTraceDetail::Crashed { model } => {
                    crashes += 1;
                    assert_eq!(*model, SimCrashModel::CleanRollback);
                    assert!(step.after.status.closed);
                    assert_eq!(step.after.image.accepted, step.after.image.durable);
                }
                StorageTraceDetail::Reopened => {
                    reopens += 1;
                    assert!(step.before.status.closed);
                    assert!(!step.after.status.closed);
                    assert_eq!(step.after.image.accepted, step.after.image.durable);
                    assert_eq!(step.after.status.fault_hits, 0);
                }
                StorageTraceDetail::SyncCompleted { result } => {
                    successful_sync = result.durable_len == FINAL_BYTES.len() as u64
                        && step.after.image.accepted == FINAL_BYTES
                        && step.after.image.durable == FINAL_BYTES;
                }
                StorageTraceDetail::ReadCompleted(observation) => {
                    final_read = observation.request.offset == 0
                        && observation.result.bytes_read == FINAL_BYTES.len()
                        && observation.result.buffer == FINAL_BYTES;
                }
            }
        }

        assert_eq!(writes, 3);
        assert!(
            injected_fault,
            "scenario missed explicit sync fault injection"
        );
        assert!(ambiguous_sync, "scenario missed ambiguous sync failure");
        assert_eq!(crashes, 2);
        assert_eq!(reopens, 2);
        assert!(
            successful_sync,
            "scenario missed successful durability fence"
        );
        assert!(final_read, "scenario missed final recovered read");
        assert_eq!(first.terminal.image.accepted, FINAL_BYTES);
        assert_eq!(first.terminal.image.durable, FINAL_BYTES);
        assert!(!first.terminal.status.closed);
    }
}
