use std::fmt::Debug;
use std::future::Future;

use kr_runtime::rng::RandomStream;
use kr_runtime::test_support::campaign_seed_range;
use kr_runtime::{
    CompletionCertainty, CompletionResult, DeterminismCheckpoint, RandomHandle, SimDuration,
    SimRuntime,
};
use kr_runtime_io::{
    SimCrashModel, SimDisk, SimFault, SimLatencyModel, SimOutcome, SimPipelineModel,
    SimRandomSources, SimStorage, SimStorageConfig, StorageOperation,
};

use super::{FileRing, FileRingConfig};
use crate::{
    AppendRequest, MemoryRing, ReadRequest, RingCursor, RingError, RingLimits, RingOperation,
    RingPhysicalStatus, RingReader, RingStatus, RingWriter,
};

const SEED_COUNT: u64 = 24;
const ACTIONS_PER_SEED: usize = 48;
const FORCED_FAULT_PREFIX_ACTIONS: usize = 14;

fn limits() -> RingLimits {
    RingLimits {
        max_record_bytes: 8,
        max_live_records: 8,
        max_live_payload_bytes: 40,
        max_read_records: 4,
        max_read_bytes: 16,
        max_batch_records: 3,
        max_batch_bytes: 24,
    }
}

fn file_config() -> FileRingConfig {
    FileRingConfig {
        limits: limits(),
        // Small enough for repeated trim/sync cycles to cross the circular
        // boundary, while still holding the configured maximum live set.
        data_capacity_bytes: 512,
        max_io_request_bytes: 4_096,
        command_queue_capacity: 16,
    }
}

/// Bound on the per-operation completion jitter of overlap-enabled seeds.
///
/// Small enough that scripted zero-delay faults stay well-ordered against
/// jittered frames, wide enough that a multi-frame append plan explores both
/// completion orders across seeds.
const OVERLAP_MAX_JITTER: SimDuration = SimDuration::from_nanos(3);

fn storage_config(config: FileRingConfig, pipeline: SimPipelineModel) -> SimStorageConfig {
    let mut storage = super::test_support::sim_storage_config(config, SimDuration::ZERO, 1);
    storage.pipeline_model = pipeline;
    if pipeline == SimPipelineModel::CommutingOverlapV1 {
        storage.latency_model = SimLatencyModel::uniform_jitter_v1(OVERLAP_MAX_JITTER);
    }
    storage
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum Action {
    Append {
        records: Vec<Vec<u8>>,
        expected_tail: Option<RingCursor>,
        script: Option<IoScript>,
    },
    Sync {
        script: Option<IoScript>,
    },
    TrimValid(RingCursor),
    TrimStale(RingCursor),
    ReadHead {
        request: ReadRequest,
        script: Option<IoScript>,
    },
    ReadMiddle {
        request: ReadRequest,
        script: Option<IoScript>,
    },
    ReadPastTail {
        request: ReadRequest,
        script: Option<IoScript>,
    },
    CrashReopen,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum IoScript {
    Partial,
    FailBefore,
    FailAfter,
    MayHaveAppliedAfter,
}

impl IoScript {
    const COUNT: usize = 4;

    const fn index(self) -> usize {
        match self {
            Self::Partial => 0,
            Self::FailBefore => 1,
            Self::FailAfter => 2,
            Self::MayHaveAppliedAfter => 3,
        }
    }

    const fn outcome(self) -> SimOutcome {
        match self {
            Self::Partial => SimOutcome::Success,
            Self::FailBefore => SimOutcome::FailBefore,
            Self::FailAfter => SimOutcome::FailAfter,
            Self::MayHaveAppliedAfter => SimOutcome::MayHaveAppliedAfter,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum SwarmProfile {
    Balanced,
    AppendHeavy,
    ReadHeavy,
    RecoveryHeavy,
}

impl SwarmProfile {
    fn choose(random: &RandomHandle) -> Self {
        match below(random, 4) {
            0 => Self::Balanced,
            1 => Self::AppendHeavy,
            2 => Self::ReadHeavy,
            _ => Self::RecoveryHeavy,
        }
    }

    fn action_class(self, random: &RandomHandle) -> usize {
        match self {
            Self::Balanced => below(random, 10),
            Self::AppendHeavy => [0, 0, 1, 1, 2, 3, 5][below(random, 7)],
            Self::ReadHeavy => [2, 5, 6, 6, 7, 7, 8, 8][below(random, 8)],
            Self::RecoveryHeavy => [0, 1, 2, 5, 9, 9, 9][below(random, 7)],
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct LogicalStatus {
    accepted_head: RingCursor,
    accepted_tail: RingCursor,
    durable_head: RingCursor,
    durable_tail: RingCursor,
    accepted_live_records: usize,
    accepted_live_payload_bytes: usize,
    retained_records: usize,
    retained_payload_bytes: usize,
    pending_reclaim_records: usize,
    pending_reclaim_payload_bytes: usize,
    max_live_records: usize,
    max_live_payload_bytes: usize,
}

impl From<RingStatus> for LogicalStatus {
    fn from(status: RingStatus) -> Self {
        Self {
            accepted_head: status.accepted_head,
            accepted_tail: status.accepted_tail,
            durable_head: status.durable_head,
            durable_tail: status.durable_tail,
            accepted_live_records: status.accepted_live_records,
            accepted_live_payload_bytes: status.accepted_live_payload_bytes,
            retained_records: status.retained_records,
            retained_payload_bytes: status.retained_payload_bytes,
            pending_reclaim_records: status.pending_reclaim_records,
            pending_reclaim_payload_bytes: status.pending_reclaim_payload_bytes,
            max_live_records: status.max_live_records,
            max_live_payload_bytes: status.max_live_payload_bytes,
        }
    }
}

fn below(random: &RandomHandle, upper_exclusive: usize) -> usize {
    usize::try_from(
        random
            .random_below(upper_exclusive as u64)
            .expect("model campaign random bounds are nonzero"),
    )
    .expect("bounded campaign choice fits usize")
}

fn inclusive(random: &RandomHandle, lower: usize, upper: usize) -> usize {
    lower + below(random, upper - lower + 1)
}

fn panic_with_trace(
    seed: u64,
    step: usize,
    trace: &[Action],
    message: impl std::fmt::Display,
) -> ! {
    panic!("ring model mismatch: seed={seed} step={step}\n{message}\ntrace={trace:#?}")
}

fn drive<F>(
    runtime: &mut SimRuntime,
    future: F,
    seed: u64,
    step: usize,
    trace: &[Action],
    operation: &str,
) -> F::Output
where
    F: Future + 'static,
    F::Output: 'static,
{
    runtime.block_on(future).unwrap_or_else(|error| {
        panic_with_trace(
            seed,
            step,
            trace,
            format_args!("runtime failed while driving {operation}: {error}"),
        )
    })
}

fn assert_same<T>(seed: u64, step: usize, trace: &[Action], operation: &str, model: &T, file: &T)
where
    T: Debug + PartialEq,
{
    if model != file {
        panic_with_trace(
            seed,
            step,
            trace,
            format_args!("{operation}\nMemoryRing: {model:#?}\nFileRing: {file:#?}"),
        );
    }
}

fn open_storage(
    runtime: &SimRuntime,
    disk: &SimDisk,
    config: SimStorageConfig,
    seed: u64,
    step: usize,
    trace: &[Action],
) -> SimStorage {
    SimStorage::open_with_random_sources(
        runtime.handle(),
        disk.clone(),
        config,
        SimRandomSources::default()
            .with_fault(runtime.random_source(RandomStream::Fault))
            .with_schedule(runtime.random_source(RandomStream::Schedule)),
    )
    .unwrap_or_else(|error| {
        panic_with_trace(
            seed,
            step,
            trace,
            format_args!("could not open simulated storage: {error}"),
        )
    })
}

fn create_file_ring(
    runtime: &mut SimRuntime,
    disk: &SimDisk,
    config: FileRingConfig,
    storage_config: SimStorageConfig,
    seed: u64,
    trace: &[Action],
) -> (FileRing<SimStorage>, SimStorage) {
    let storage = open_storage(runtime, disk, storage_config, seed, 0, trace);
    let handle = runtime.handle();
    let result = drive(
        runtime,
        FileRing::create(handle, storage.clone(), config),
        seed,
        0,
        trace,
        "FileRing::create",
    );
    let ring = result.unwrap_or_else(|error| {
        panic_with_trace(
            seed,
            0,
            trace,
            format_args!("FileRing::create failed: {error}"),
        )
    });
    (ring, storage)
}

fn reopen_file_ring(
    runtime: &mut SimRuntime,
    disk: &SimDisk,
    config: FileRingConfig,
    storage_config: SimStorageConfig,
    seed: u64,
    step: usize,
    trace: &[Action],
) -> (FileRing<SimStorage>, SimStorage) {
    let storage = open_storage(runtime, disk, storage_config, seed, step, trace);
    let handle = runtime.handle();
    let result = drive(
        runtime,
        FileRing::open(handle, storage.clone(), config),
        seed,
        step,
        trace,
        "FileRing::open",
    );
    let ring = result.unwrap_or_else(|error| {
        panic_with_trace(
            seed,
            step,
            trace,
            format_args!("FileRing::open failed: {error}"),
        )
    });
    (ring, storage)
}

fn compare_status(
    runtime: &mut SimRuntime,
    memory: &MemoryRing,
    file: &FileRing<SimStorage>,
    seed: u64,
    step: usize,
    trace: &[Action],
) -> (LogicalStatus, RingPhysicalStatus) {
    let memory_result: CompletionResult<LogicalStatus, _> = drive(
        runtime,
        memory.status(),
        seed,
        step,
        trace,
        "MemoryRing::status",
    )
    .map(LogicalStatus::from);
    let file_status = drive(
        runtime,
        file.status(),
        seed,
        step,
        trace,
        "FileRing::status",
    );
    let physical = file_status
        .as_ref()
        .ok()
        .and_then(|status| status.physical)
        .expect("file-ring status includes physical diagnostics");
    let file_result: CompletionResult<LogicalStatus, _> = file_status.map(LogicalStatus::from);
    assert_same(
        seed,
        step,
        trace,
        "normalized status result",
        &memory_result,
        &file_result,
    );
    let logical = memory_result.unwrap_or_else(|error| {
        panic_with_trace(
            seed,
            step,
            trace,
            format_args!("matched status calls unexpectedly failed: {error}"),
        )
    });
    (logical, physical)
}

fn payload(random: &RandomHandle, seed: u64, step: usize, record: usize) -> Vec<u8> {
    let length = inclusive(random, 0, limits().max_record_bytes);
    let mut bytes = Vec::with_capacity(length);
    for byte in 0..length {
        let random_byte = random.random_u64().expect("model runtime is active") as u8;
        bytes.push(random_byte ^ seed as u8 ^ step as u8 ^ record as u8 ^ byte as u8);
    }
    bytes
}

fn read_request(random: &RandomHandle, cursor: RingCursor, limits: RingLimits) -> ReadRequest {
    ReadRequest::new(
        cursor,
        inclusive(random, 1, limits.max_read_records),
        inclusive(random, 1, limits.max_read_bytes),
    )
}

fn forced_fault_action(
    random: &RandomHandle,
    seed: u64,
    step: usize,
    status: LogicalStatus,
) -> Option<Action> {
    let read_head = |script| Action::ReadHead {
        request: ReadRequest::new(status.durable_head, 1, limits().max_record_bytes),
        script: Some(script),
    };
    let append = |script| Action::Append {
        records: vec![payload(random, seed, step, 0)],
        expected_tail: None,
        script,
    };
    match step {
        0 => Some(append(None)),
        1 => Some(append(Some(IoScript::Partial))),
        2 => Some(Action::Sync {
            script: Some(IoScript::FailAfter),
        }),
        3 => Some(read_head(IoScript::Partial)),
        4 => Some(read_head(IoScript::FailBefore)),
        5 => Some(read_head(IoScript::FailAfter)),
        6 => Some(append(Some(IoScript::FailBefore))),
        7 => Some(append(Some(IoScript::FailAfter))),
        8 => Some(append(None)),
        9 => Some(Action::Sync {
            script: Some(IoScript::FailBefore),
        }),
        10 => Some(append(Some(IoScript::MayHaveAppliedAfter))),
        11 => Some(read_head(IoScript::MayHaveAppliedAfter)),
        12 => Some(append(None)),
        13 => Some(Action::Sync {
            script: Some(IoScript::MayHaveAppliedAfter),
        }),
        _ => None,
    }
}

fn random_fault_action(
    workload: &RandomHandle,
    faults: &RandomHandle,
    seed: u64,
    step: usize,
    status: LogicalStatus,
    limits: RingLimits,
) -> Option<Action> {
    if below(faults, 8) != 0 {
        return None;
    }
    let script = match below(faults, IoScript::COUNT) {
        0 => IoScript::Partial,
        1 => IoScript::FailBefore,
        2 => IoScript::FailAfter,
        _ => IoScript::MayHaveAppliedAfter,
    };
    let operation = below(faults, 3);
    match operation {
        0 => {
            let record = payload(workload, seed, step, 0);
            let has_record_capacity = status.retained_records < limits.max_live_records
                && status.accepted_live_records < limits.max_live_records;
            let has_payload_capacity = status
                .retained_payload_bytes
                .checked_add(record.len())
                .is_some_and(|bytes| bytes <= limits.max_live_payload_bytes)
                && status
                    .accepted_live_payload_bytes
                    .checked_add(record.len())
                    .is_some_and(|bytes| bytes <= limits.max_live_payload_bytes);
            (has_record_capacity && has_payload_capacity).then_some(Action::Append {
                records: vec![record],
                expected_tail: None,
                script: Some(script),
            })
        }
        1 if status.durable_head < status.durable_tail => Some(Action::ReadHead {
            request: ReadRequest::new(status.durable_head, 1, limits.max_record_bytes),
            script: Some(script),
        }),
        2 if status.accepted_head != status.durable_head
            || status.accepted_tail != status.durable_tail =>
        {
            let sync_script = match script {
                IoScript::Partial => IoScript::FailBefore,
                other => other,
            };
            Some(Action::Sync {
                script: Some(sync_script),
            })
        }
        _ => None,
    }
}

fn generate_action(
    random: &RandomHandle,
    faults: &RandomHandle,
    profile: SwarmProfile,
    seed: u64,
    step: usize,
    status: LogicalStatus,
    limits: RingLimits,
) -> Action {
    if let Some(action) = forced_fault_action(random, seed, step, status) {
        return action;
    }
    if let Some(action) = random_fault_action(random, faults, seed, step, status, limits) {
        return action;
    }
    let action_class = profile.action_class(random);
    match action_class {
        0 | 1 => {
            let count = inclusive(random, 1, limits.max_batch_records);
            let records = (0..count)
                .map(|record| payload(random, seed, step, record))
                .collect();
            let expected_tail = if step.is_multiple_of(10) {
                None
            } else if below(random, 2) == 0 {
                Some(status.accepted_tail)
            } else {
                let stale_or_future = if status.accepted_tail.get() == 0 {
                    1
                } else {
                    status.accepted_tail.get() - 1
                };
                Some(RingCursor::new(stale_or_future))
            };
            Action::Append {
                records,
                expected_tail,
                script: None,
            }
        }
        2 | 5 => Action::Sync { script: None },
        3 => {
            let distance = usize::try_from(status.durable_tail.get() - status.accepted_head.get())
                .expect("bounded live interval fits usize");
            let advance = if distance == 0 {
                0
            } else {
                inclusive(random, 1, distance) as u64
            };
            Action::TrimValid(RingCursor::new(status.accepted_head.get() + advance))
        }
        4 => {
            let retreat = inclusive(random, 1, 3) as u64;
            Action::TrimStale(RingCursor::new(
                status.accepted_head.get().saturating_sub(retreat),
            ))
        }
        6 => Action::ReadHead {
            request: read_request(random, status.durable_head, limits),
            script: None,
        },
        7 => {
            let live = status.durable_tail.get() - status.durable_head.get();
            let cursor = if live > 1 {
                RingCursor::new(
                    status.durable_head.get() + 1 + below(random, live as usize - 1) as u64,
                )
            } else {
                status.durable_head
            };
            Action::ReadMiddle {
                request: read_request(random, cursor, limits),
                script: None,
            }
        }
        8 => {
            let distance = inclusive(random, 1, 3) as u64;
            Action::ReadPastTail {
                request: read_request(
                    random,
                    RingCursor::new(status.durable_tail.get() + distance),
                    limits,
                ),
                script: None,
            }
        }
        9 => Action::CrashReopen,
        _ => unreachable!("modulo ten is always in range"),
    }
}

#[derive(Debug, Eq, PartialEq)]
struct CampaignRun {
    trace: Vec<Action>,
    checkpoint: DeterminismCheckpoint,
    coverage: CampaignCoverage,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
struct CampaignCoverage {
    zero_length_records: u64,
    middle_reads: u64,
    physical_wraps: u64,
    scripted_reads: [u64; IoScript::COUNT],
    scripted_writes: [u64; IoScript::COUNT],
    scripted_syncs: [u64; IoScript::COUNT],
    randomized_scripted_faults: u64,
    /// Storage completions delivered before an earlier-admitted operation,
    /// summed across every storage session a seed opens. Nonzero only when
    /// overlap-enabled seeds actually reorder a pipelined append plan.
    reordered_completions: u64,
}

impl CampaignCoverage {
    fn record_script(&mut self, operation: StorageOperation, script: IoScript) {
        let counts = match operation {
            StorageOperation::ReadAt => &mut self.scripted_reads,
            StorageOperation::WriteAt => &mut self.scripted_writes,
            StorageOperation::Sync => &mut self.scripted_syncs,
            other => panic!("model campaign cannot script {other:?}"),
        };
        counts[script.index()] += 1;
    }
}

fn inject_script(
    storage: &SimStorage,
    operation: StorageOperation,
    script: IoScript,
    seed: u64,
    step: usize,
    trace: &[Action],
) -> u64 {
    let before = storage.status();
    if before.pending_faults != 0 {
        panic_with_trace(
            seed,
            step,
            trace,
            format_args!(
                "cannot inject {operation:?}/{script:?}: {} earlier scripts remain",
                before.pending_faults
            ),
        );
    }
    let mut fault = SimFault::new(operation, SimDuration::ZERO, script.outcome());
    if script == IoScript::Partial {
        fault = fault.with_max_bytes(1);
    }
    storage.inject(fault).unwrap_or_else(|error| {
        panic_with_trace(
            seed,
            step,
            trace,
            format_args!("could not inject {operation:?}/{script:?}: {error}"),
        )
    });
    let armed = storage.status();
    if armed.pending_faults != 1 || armed.fault_hits != before.fault_hits {
        panic_with_trace(
            seed,
            step,
            trace,
            format_args!(
                "arming {operation:?}/{script:?} produced invalid status: before={before:?}, armed={armed:?}"
            ),
        );
    }
    before.fault_hits
}

fn confirm_script_consumed(
    storage: &SimStorage,
    operation: StorageOperation,
    script: IoScript,
    previous_hits: u64,
    seed: u64,
    step: usize,
    trace: &[Action],
) {
    let status = storage.status();
    if status.pending_faults != 0 || status.fault_hits != previous_hits + 1 {
        panic_with_trace(
            seed,
            step,
            trace,
            format_args!(
                "{operation:?}/{script:?} was not consumed exactly once: previous_hits={previous_hits}, status={status:?}"
            ),
        );
    }
}

/// Drives one campaign operation, optionally under a scripted backend fault,
/// and checks it against the reference model.
///
/// Any script is injected before the file-ring drive and must be consumed
/// exactly once. An unscripted or benign-scripted operation must match the
/// MemoryRing result exactly; every other script must produce the failure
/// shape accepted by `validate_failure`, which returns the panic message for
/// an invalid outcome.
#[allow(clippy::too_many_arguments)]
fn run_scripted_action<T, FileFuture, MemoryFuture>(
    runtime: &mut SimRuntime,
    storage: &SimStorage,
    coverage: &mut CampaignCoverage,
    operation: StorageOperation,
    script: Option<IoScript>,
    benign_script: IoScript,
    seed: u64,
    step: usize,
    trace: &[Action],
    file_operation: &str,
    file_future: impl FnOnce() -> FileFuture,
    memory_operation: &str,
    memory_future: impl FnOnce() -> MemoryFuture,
    comparison: &str,
    validate_failure: impl FnOnce(&T) -> Result<(), String>,
) where
    T: Debug + PartialEq + 'static,
    FileFuture: Future<Output = T> + 'static,
    MemoryFuture: Future<Output = T> + 'static,
{
    let previous_hits =
        script.map(|script| inject_script(storage, operation, script, seed, step, trace));
    let file_result = drive(runtime, file_future(), seed, step, trace, file_operation);
    if let (Some(script), Some(previous_hits)) = (script, previous_hits) {
        confirm_script_consumed(storage, operation, script, previous_hits, seed, step, trace);
        coverage.record_script(operation, script);
        if step >= FORCED_FAULT_PREFIX_ACTIONS {
            coverage.randomized_scripted_faults += 1;
        }
    }
    if script.is_none() || script == Some(benign_script) {
        let memory_result = drive(
            runtime,
            memory_future(),
            seed,
            step,
            trace,
            memory_operation,
        );
        assert_same(seed, step, trace, comparison, &memory_result, &file_result);
    } else if let Err(message) = validate_failure(&file_result) {
        panic_with_trace(seed, step, trace, message);
    }
}

fn run_seed(seed: u64) -> CampaignRun {
    let mut runtime = SimRuntime::new(kr_runtime::RuntimeConfig {
        seed,
        ..kr_runtime::RuntimeConfig::default()
    });
    let scenario = runtime.random_source(RandomStream::Scenario);
    let profile = SwarmProfile::choose(&scenario);
    // Half the seeds run the storage worker in its commuting-overlap mode
    // with jittered completion latency, so multi-frame append plans complete
    // out of admission order while the differential oracle stays unchanged.
    let pipeline = if below(&scenario, 2) == 0 {
        SimPipelineModel::Serial
    } else {
        SimPipelineModel::CommutingOverlapV1
    };
    drop(scenario);
    let random = runtime.random_source(RandomStream::Workload);
    let fault_random = runtime.random_source(RandomStream::Fault);
    let disk = SimDisk::default();
    let config = file_config();
    let storage_config = storage_config(config, pipeline);
    let memory = MemoryRing::new(config.limits).expect("model limits are valid");
    let mut trace = Vec::with_capacity(ACTIONS_PER_SEED);
    let (mut file, mut storage) =
        create_file_ring(&mut runtime, &disk, config, storage_config, seed, &trace);
    let (mut status, mut physical) = compare_status(&mut runtime, &memory, &file, seed, 0, &trace);
    let mut coverage = CampaignCoverage::default();

    for step in 0..ACTIONS_PER_SEED {
        let action = generate_action(
            &random,
            &fault_random,
            profile,
            seed,
            step,
            status,
            config.limits,
        );
        let append_action = matches!(
            &action,
            Action::Append {
                script: None | Some(IoScript::Partial),
                ..
            }
        );
        if let Action::Append { records, .. } = &action {
            coverage.zero_length_records +=
                records.iter().filter(|record| record.is_empty()).count() as u64;
        }
        if let Action::ReadMiddle { request, .. } = &action
            && request.cursor > status.durable_head
            && request.cursor < status.durable_tail
        {
            coverage.middle_reads += 1;
        }
        trace.push(action.clone());

        let reconcile_after_fault = match action {
            Action::Append {
                records,
                expected_tail,
                script,
            } => {
                let request = AppendRequest {
                    records,
                    expected_accepted_tail: expected_tail,
                };
                let file_request = request.clone();
                let expected_records = request.records.clone();
                run_scripted_action(
                    &mut runtime,
                    &storage,
                    &mut coverage,
                    StorageOperation::WriteAt,
                    script,
                    IoScript::Partial,
                    seed,
                    step,
                    &trace,
                    "FileRing::append",
                    || file.append(file_request),
                    "MemoryRing::append",
                    || memory.append(request),
                    "append result, certainty, error, and returned buffers",
                    |result| match result {
                        Ok(_) => Err(format!(
                            "scripted append unexpectedly succeeded: {script:?}"
                        )),
                        Err(error)
                            if error.certainty() != CompletionCertainty::NotApplied
                                || !matches!(
                                    &error.error().error,
                                    RingError::BackendFailure {
                                        operation: RingOperation::Append,
                                        ..
                                    }
                                )
                                || error.error().records != expected_records =>
                        {
                            Err(format!(
                                "scripted append returned an invalid failure: {error:?}"
                            ))
                        }
                        Err(_) => Ok(()),
                    },
                );
                matches!(
                    script,
                    Some(IoScript::FailAfter | IoScript::MayHaveAppliedAfter)
                )
            }
            Action::Sync { script } => {
                run_scripted_action(
                    &mut runtime,
                    &storage,
                    &mut coverage,
                    StorageOperation::Sync,
                    script,
                    IoScript::FailAfter,
                    seed,
                    step,
                    &trace,
                    "FileRing::sync",
                    || file.sync(),
                    "MemoryRing::sync",
                    || memory.sync(),
                    "sync result, certainty, error, and checkpoint",
                    |result| match result {
                        Ok(_) => Err(format!("scripted sync unexpectedly succeeded: {script:?}")),
                        Err(error)
                            if error.certainty() != CompletionCertainty::NotApplied
                                || !matches!(
                                    &error.error().error,
                                    RingError::BackendFailure {
                                        operation: RingOperation::Sync,
                                        ..
                                    }
                                ) =>
                        {
                            Err(format!(
                                "scripted sync returned an invalid failure: {error:?}"
                            ))
                        }
                        Err(_) => Ok(()),
                    },
                );
                script == Some(IoScript::MayHaveAppliedAfter)
            }
            Action::TrimValid(cursor) | Action::TrimStale(cursor) => {
                let memory_result = drive(
                    &mut runtime,
                    memory.trim(cursor),
                    seed,
                    step,
                    &trace,
                    "MemoryRing::trim",
                );
                let file_result = drive(
                    &mut runtime,
                    file.trim(cursor),
                    seed,
                    step,
                    &trace,
                    "FileRing::trim",
                );
                assert_same(
                    seed,
                    step,
                    &trace,
                    "trim result, certainty, and error",
                    &memory_result,
                    &file_result,
                );
                false
            }
            Action::ReadHead { request, script }
            | Action::ReadMiddle { request, script }
            | Action::ReadPastTail { request, script } => {
                run_scripted_action(
                    &mut runtime,
                    &storage,
                    &mut coverage,
                    StorageOperation::ReadAt,
                    script,
                    IoScript::Partial,
                    seed,
                    step,
                    &trace,
                    "FileRing::read",
                    || file.read(request),
                    "MemoryRing::read",
                    || memory.read(request),
                    "read result, certainty, error, and buffers",
                    |result| match result {
                        Ok(_) => Err(format!("scripted read unexpectedly succeeded: {script:?}")),
                        Err(error)
                            if error.certainty() != CompletionCertainty::NotApplied
                                || !matches!(
                                    error.error(),
                                    RingError::BackendFailure {
                                        operation: RingOperation::Read,
                                        ..
                                    }
                                ) =>
                        {
                            Err(format!(
                                "scripted read returned an invalid failure: {error:?}"
                            ))
                        }
                        Err(_) => Ok(()),
                    },
                );
                script == Some(IoScript::MayHaveAppliedAfter)
            }
            Action::CrashReopen => {
                let _memory_crash = memory.crash();
                storage
                    .crash_with_model(SimCrashModel::FoundationDbLikeV1)
                    .expect("ring campaign configures fault randomness");
                coverage.reordered_completions += storage.status().reordered_completions;
                drop(file);
                drop(storage);
                (file, storage) = reopen_file_ring(
                    &mut runtime,
                    &disk,
                    config,
                    storage_config,
                    seed,
                    step,
                    &trace,
                );
                false
            }
        };

        if reconcile_after_fault {
            let _memory_crash = memory.crash();
            // Resolve the deliberately ambiguous high-level result to the
            // clean-rollback legal outcome. FoundationDB-like page tearing is
            // covered by ordinary CrashReopen actions and has no single
            // MemoryRing state suitable for this differential oracle.
            storage.crash();
            coverage.reordered_completions += storage.status().reordered_completions;
            drop(file);
            drop(storage);
            (file, storage) = reopen_file_ring(
                &mut runtime,
                &disk,
                config,
                storage_config,
                seed,
                step,
                &trace,
            );
        }

        let (next_status, next_physical) =
            compare_status(&mut runtime, &memory, &file, seed, step, &trace);
        if append_action && next_physical.accepted_tail_offset < physical.accepted_tail_offset {
            coverage.physical_wraps += 1;
        }
        status = next_status;
        physical = next_physical;
    }

    storage
        .crash_with_model(SimCrashModel::FoundationDbLikeV1)
        .expect("ring campaign configures fault randomness");
    coverage.reordered_completions += storage.status().reordered_completions;
    drop(file);
    drop(storage);
    drop(random);
    drop(fault_random);
    runtime.shutdown().unwrap_or_else(|error| {
        panic_with_trace(
            seed,
            ACTIONS_PER_SEED,
            &trace,
            format_args!("runtime shutdown failed: {error}"),
        )
    });
    CampaignRun {
        trace,
        checkpoint: runtime.snapshot().determinism_checkpoint(),
        coverage,
    }
}

#[test]
fn physical_capacity_rejection_leaves_the_logical_model_reconcilable() {
    let mut runtime = SimRuntime::default();
    let disk = SimDisk::default();
    let config = FileRingConfig {
        limits: RingLimits {
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
    };
    let storage_config = storage_config(config, SimPipelineModel::Serial);
    let memory = MemoryRing::new(config.limits).expect("logical model config is valid");
    let (file, storage) = create_file_ring(&mut runtime, &disk, config, storage_config, 0, &[]);

    let initial = AppendRequest::new(vec![vec![1; 20], vec![2; 20]]);
    let memory_append = runtime
        .block_on(memory.append(initial.clone()))
        .expect("runtime completes logical append");
    let file_append = runtime
        .block_on(file.append(initial))
        .expect("runtime completes file append");
    assert_eq!(memory_append, file_append);
    let memory_sync = runtime
        .block_on(memory.sync())
        .expect("runtime completes logical sync");
    let file_sync = runtime
        .block_on(file.sync())
        .expect("runtime completes file sync");
    assert_eq!(memory_sync, file_sync);

    let rejected_records = vec![vec![3; 20]];
    let rejection = runtime
        .block_on(file.append(AppendRequest::new(rejected_records.clone())))
        .expect("runtime completes capacity rejection")
        .expect_err("logical space remains, but the wrapped frame does not physically fit");
    assert_eq!(rejection.certainty(), CompletionCertainty::NotApplied);
    assert_eq!(rejection.error().records, rejected_records);
    assert_eq!(rejection.error().accepted_range, None);
    assert!(matches!(
        rejection.error().error,
        RingError::PhysicalCapacityReached {
            protected: 112,
            requested: 72,
            limit: 128,
        }
    ));

    // The reference model has no physical layout and therefore accepts the
    // same logically valid append. Rolling that unsynced probe back selects
    // the FileRing's explicit NotApplied outcome and restores one oracle.
    runtime
        .block_on(memory.append(AppendRequest::new(rejected_records)))
        .expect("runtime completes logical capacity probe")
        .expect("logical limits still admit the third record");
    let rollback = memory.crash();
    assert_eq!(rollback.discarded_records, 1);
    let (logical, physical) = compare_status(&mut runtime, &memory, &file, 0, 0, &[]);
    assert_eq!(logical.durable_tail, RingCursor::new(2));
    assert_eq!(physical.protected_bytes, 112);

    storage.crash();
    drop(file);
    drop(storage);
    runtime.shutdown().expect("runtime shuts down cleanly");
}

#[test]
fn seeded_memory_and_file_ring_model_campaign() {
    let mut coverage = CampaignCoverage::default();
    for seed in campaign_seed_range("RING_MODEL", SEED_COUNT) {
        let first = run_seed(seed);
        let repeated = run_seed(seed);
        assert_eq!(first, repeated, "seed {seed} did not replay identically");
        coverage.zero_length_records += first.coverage.zero_length_records;
        coverage.middle_reads += first.coverage.middle_reads;
        coverage.physical_wraps += first.coverage.physical_wraps;
        coverage.randomized_scripted_faults += first.coverage.randomized_scripted_faults;
        coverage.reordered_completions += first.coverage.reordered_completions;
        for index in 0..IoScript::COUNT {
            coverage.scripted_reads[index] += first.coverage.scripted_reads[index];
            coverage.scripted_writes[index] += first.coverage.scripted_writes[index];
            coverage.scripted_syncs[index] += first.coverage.scripted_syncs[index];
        }
    }
    assert!(
        coverage.zero_length_records > 0,
        "campaign missed zero-length records: {coverage:#?}"
    );
    assert!(
        coverage.middle_reads > 0,
        "campaign missed mid-interval reads: {coverage:#?}"
    );
    assert!(
        coverage.physical_wraps > 0,
        "campaign missed physical circular wrap: {coverage:#?}"
    );
    assert!(
        coverage.randomized_scripted_faults > 0,
        "campaign consumed only its forced fault prefix: {coverage:#?}"
    );
    assert!(
        coverage.reordered_completions > 0,
        "campaign never reordered a pipelined append plan: {coverage:#?}"
    );
    for script in [
        IoScript::Partial,
        IoScript::FailBefore,
        IoScript::FailAfter,
        IoScript::MayHaveAppliedAfter,
    ] {
        assert!(
            coverage.scripted_reads[script.index()] > 0,
            "campaign missed scripted read outcome {script:?}: {coverage:#?}"
        );
        assert!(
            coverage.scripted_writes[script.index()] > 0,
            "campaign missed scripted write outcome {script:?}: {coverage:#?}"
        );
        if script != IoScript::Partial {
            assert!(
                coverage.scripted_syncs[script.index()] > 0,
                "campaign missed scripted sync outcome {script:?}: {coverage:#?}"
            );
        }
    }
}
