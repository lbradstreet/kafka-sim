use kr_runtime::rng::RandomStream;
use kr_runtime::trace::{
    EventKind, RandomChoiceKind, RecordingTrace, SamplingTrace, TRACE_SCHEMA_VERSION, TraceEvent,
    TraceReplayChecker, TraceReplayCheckerError, TraceReplayDivergence, TraceSink,
};
use kr_runtime::{DeterminismCheckpoint, RuntimeConfig, RuntimeSnapshot, SimDuration, SimRuntime};
use std::num::NonZeroU64;
use std::process::Command;
use std::rc::Rc;

const REPRODUCTION_HELPER_ENV: &str = "KR_RUNTIME_REPRODUCTION_HELPER";
const REPLAY_ARTIFACT_MARKER: &str = "KR_RUNTIME_REPLAY_ARTIFACT=";
const CANONICAL_SEED: u64 = 0x5eed;

struct Execution {
    trace: Vec<TraceEvent>,
    trace_fingerprint: u64,
    snapshot: RuntimeSnapshot,
    outcome: u64,
}

fn execute(seed: u64, debug_draws: usize) -> Execution {
    let trace = Rc::new(RecordingTrace::new(256));
    let mut runtime = SimRuntime::with_trace(
        RuntimeConfig {
            seed,
            ..RuntimeConfig::default()
        },
        trace.clone(),
    );
    let handle = runtime.handle();
    let task_handle = handle.clone();
    let debug = runtime.random_source(RandomStream::Debug);

    let outcome = runtime
        .block_on(async move {
            for _ in 0..debug_draws {
                let _ = debug.random_u64().expect("runtime is active");
            }
            let delay = task_handle.random_below(10).unwrap();
            task_handle
                .sleep(SimDuration::from_nanos(delay + 1))
                .await
                .unwrap();
            delay
        })
        .unwrap();

    assert_eq!(trace.dropped(), 0, "the replay trace must not be truncated");
    Execution {
        trace: trace.events(),
        trace_fingerprint: trace.fingerprint(),
        snapshot: runtime.snapshot(),
        outcome,
    }
}

#[derive(Debug, Eq, PartialEq)]
struct RecordedExecution {
    trace: Vec<TraceEvent>,
    outcome: u64,
    trace_fingerprint: u64,
    checkpoint: DeterminismCheckpoint,
}

impl RecordedExecution {
    fn from_execution(execution: &Execution) -> Self {
        Self {
            trace: execution.trace.clone(),
            outcome: execution.outcome,
            trace_fingerprint: execution.trace_fingerprint,
            checkpoint: execution.snapshot.determinism_checkpoint(),
        }
    }
}

fn debug_draws(snapshot: &RuntimeSnapshot) -> u64 {
    snapshot
        .random
        .iter()
        .find(|state| state.stream == RandomStream::Debug)
        .expect("the Debug stream must be present in a runtime snapshot")
        .checkpoint
        .draws()
}

fn checkpoint(snapshot: &RuntimeSnapshot, stream: RandomStream) -> (u64, u64) {
    let checkpoint = snapshot
        .random
        .iter()
        .find(|state| state.stream == stream)
        .expect("every canonical random stream must be present")
        .checkpoint;
    (checkpoint.state(), checkpoint.draws())
}

/// Encodes only explicitly ordered scalar fields. This is deliberately not a
/// `Debug` representation or a generic map serialization: it is the stable
/// replay-artifact format protected by the golden test below.
fn canonical_replay_artifact() -> String {
    let execution = execute(CANONICAL_SEED, 0);
    let snapshot = &execution.snapshot;
    let determinism = snapshot.determinism_checkpoint();
    let reproduction = &snapshot.reproduction;
    let config = &reproduction.config;
    assert_eq!(snapshot.random.len(), 5, "update replay-artifact-v5");

    let schedule = checkpoint(snapshot, RandomStream::Schedule);
    let scenario = checkpoint(snapshot, RandomStream::Scenario);
    let workload = checkpoint(snapshot, RandomStream::Workload);
    let fault = checkpoint(snapshot, RandomStream::Fault);
    let debug = checkpoint(snapshot, RandomStream::Debug);
    let last_sequence = execution.trace.last().map_or(0, |event| event.sequence);

    format!(
        concat!(
            "replay-artifact-v5;driver=block-on-root-v1;",
            "runtime-reproduction-schema={};checkpoint-schema={};",
            "seed={:016x};rng-version={};max-tasks={};max-timers={};",
            "max-steps-per-run={};",
            "max-time-ns={};start-time-ns={};outcome=ok:{};now-ns={};",
            "steps={};enqueue-sequence={};timer-sequence={};timer-id={};",
            "ready={};timers={};tasks={};stopped={};",
            "trace-schema={};events={};last-sequence={};trace-fingerprint={:016x};",
            "schedule={:016x}/{};scenario={:016x}/{};",
            "workload={:016x}/{};fault={:016x}/{};debug={:016x}/{}"
        ),
        reproduction.schema_version,
        determinism.schema_version,
        config.seed,
        reproduction.rng_version,
        config.max_tasks,
        config.max_timers,
        config.max_steps_per_run,
        config
            .max_time
            .map_or_else(|| "none".to_owned(), |time| time.as_nanos().to_string()),
        config.start_time.as_nanos(),
        execution.outcome,
        determinism.now.as_nanos(),
        determinism.total_steps,
        determinism.next_enqueue_sequence,
        determinism.next_timer_sequence,
        determinism.next_timer_id,
        determinism.ready_tasks,
        determinism.live_timers,
        determinism.live_tasks,
        determinism.stopped,
        TRACE_SCHEMA_VERSION,
        execution.trace.len(),
        last_sequence,
        execution.trace_fingerprint,
        schedule.0,
        schedule.1,
        scenario.0,
        scenario.1,
        workload.0,
        workload.1,
        fault.0,
        fault.1,
        debug.0,
        debug.1,
    )
}

fn run_reproduction_helper_process() -> String {
    let output = Command::new(std::env::current_exe().expect("locate replay integration test"))
        .args([
            "--ignored",
            "--exact",
            "cross_process_reproduction_helper",
            "--nocapture",
            "--test-threads=1",
        ])
        .env(REPRODUCTION_HELPER_ENV, "1")
        .output()
        .expect("run reproduction helper process");

    assert!(
        output.status.success(),
        "reproduction helper failed with {}\nstdout:\n{}\nstderr:\n{}",
        output.status,
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr),
    );
    let stdout = String::from_utf8(output.stdout).expect("helper stdout must be UTF-8");
    let marker = stdout
        .find(REPLAY_ARTIFACT_MARKER)
        .expect("helper stdout must contain a replay artifact");
    stdout[marker + REPLAY_ARTIFACT_MARKER.len()..]
        .lines()
        .next()
        .expect("the replay artifact must occupy one line")
        .to_owned()
}

#[test]
fn same_seed_and_inputs_produce_identical_trace_and_fingerprint() {
    let first = execute(CANONICAL_SEED, 0);
    let second = execute(CANONICAL_SEED, 0);

    assert_eq!(
        RecordedExecution::from_execution(&first),
        RecordedExecution::from_execution(&second)
    );
    assert!(
        first
            .trace
            .windows(2)
            .all(|events| events[1].sequence == events[0].sequence + 1)
    );
    assert!(first.trace.iter().any(|event| matches!(
        event.kind,
        EventKind::RandomChoice {
            stream: RandomStream::Workload,
            choice: RandomChoiceKind::Below {
                upper_exclusive: 10
            },
            ..
        }
    )));
}

#[test]
fn replay_checker_accepts_an_identical_recorded_execution() {
    let recorded = execute(CANONICAL_SEED, 0);
    let checker = Rc::new(
        TraceReplayChecker::new(TRACE_SCHEMA_VERSION, recorded.trace.clone(), 256)
            .expect("recorded trace fits the checker"),
    );

    let rerun = execute(CANONICAL_SEED, 0);
    for event in rerun.trace {
        checker.record(event);
    }

    assert_eq!(rerun.outcome, recorded.outcome);
    assert_eq!(rerun.snapshot, recorded.snapshot);
    assert_eq!(checker.schema_version(), TRACE_SCHEMA_VERSION);
    assert_eq!(checker.max_expected_events(), 256);
    assert_eq!(checker.matched_events(), recorded.trace.len());
    assert_eq!(checker.divergence(), None);
    assert_eq!(checker.finish(), Ok(()));
    checker.record(recorded.trace[0].clone());
    assert_eq!(
        checker.finish(),
        Ok(()),
        "a finalized comparison ignores later sink calls"
    );
}

#[test]
fn sampled_replay_uses_the_identical_admission_policy() {
    let recorded = execute(CANONICAL_SEED, 0);
    let expected: Vec<_> = recorded
        .trace
        .iter()
        .filter(|event| event.sequence % 2 == 0)
        .cloned()
        .collect();
    let checker = Rc::new(
        TraceReplayChecker::new(TRACE_SCHEMA_VERSION, expected.clone(), 256)
            .expect("sampled trace fits the checker"),
    );
    let sampling = SamplingTrace::new(
        checker.clone(),
        NonZeroU64::new(2).expect("period is nonzero"),
    );

    let rerun = execute(CANONICAL_SEED, 0);
    for event in rerun.trace {
        if sampling.should_record(event.sequence, event.kind.tag()) {
            sampling.record(event);
        }
    }

    assert_eq!(checker.matched_events(), expected.len());
    assert_eq!(checker.finish(), Ok(()));
}

#[test]
fn replay_checker_stops_at_the_first_changed_seed_event() {
    let recorded = execute(CANONICAL_SEED, 0);
    let checker = Rc::new(
        TraceReplayChecker::new(TRACE_SCHEMA_VERSION, recorded.trace.clone(), 256)
            .expect("recorded trace fits the checker"),
    );

    let rerun = execute(CANONICAL_SEED + 1, 0);
    for event in rerun.trace {
        checker.record(event);
    }

    let divergence = checker
        .divergence()
        .expect("the changed runtime-start event must diverge");
    let TraceReplayDivergence::EventMismatch {
        index,
        expected,
        actual,
    } = &divergence
    else {
        panic!("changed seed produced the wrong divergence: {divergence:?}");
    };
    assert_eq!(*index, 0);
    assert!(matches!(
        &expected.kind,
        kr_runtime::trace::EventKind::RuntimeStarted { seed }
            if *seed == CANONICAL_SEED
    ));
    assert!(matches!(
        &actual.kind,
        kr_runtime::trace::EventKind::RuntimeStarted { seed }
            if *seed == CANONICAL_SEED + 1
    ));
    assert_eq!(checker.matched_events(), 0);

    checker.record(recorded.trace[1].clone());
    assert_eq!(
        checker.divergence(),
        Some(divergence.clone()),
        "events after the first mismatch must be ignored"
    );
    assert_eq!(checker.finish(), Err(divergence));
}

#[test]
fn replay_checker_reports_truncated_and_extra_actual_streams() {
    let recorded = execute(CANONICAL_SEED, 0);
    let truncated = TraceReplayChecker::new(TRACE_SCHEMA_VERSION, recorded.trace.clone(), 256)
        .expect("recorded trace fits the checker");
    for event in &recorded.trace[..recorded.trace.len() - 1] {
        truncated.record(event.clone());
    }

    let early = truncated
        .finish()
        .expect_err("a truncated rerun must fail comparison");
    assert_eq!(
        early,
        TraceReplayDivergence::ActualEndedEarly {
            index: recorded.trace.len() - 1,
            expected_events: recorded.trace.len(),
            next_expected: Box::new(recorded.trace.last().expect("trace is nonempty").clone()),
        }
    );
    assert_eq!(truncated.finish(), Err(early));

    let expected_prefix = recorded.trace[..recorded.trace.len() - 1].to_vec();
    let extra = Rc::new(
        TraceReplayChecker::new(TRACE_SCHEMA_VERSION, expected_prefix.clone(), 256)
            .expect("trace prefix fits the checker"),
    );
    let rerun = execute(CANONICAL_SEED, 0);
    for event in rerun.trace {
        extra.record(event);
    }
    assert_eq!(
        extra.finish(),
        Err(TraceReplayDivergence::UnexpectedEvent {
            index: expected_prefix.len(),
            expected_events: expected_prefix.len(),
            actual: Box::new(recorded.trace[expected_prefix.len()].clone()),
        })
    );
}

#[test]
fn replay_checker_rejects_incompatible_schema_and_unbounded_expectations() {
    assert!(matches!(
        TraceReplayChecker::new(TRACE_SCHEMA_VERSION + 1, Vec::new(), 0),
        Err(TraceReplayCheckerError::UnsupportedSchema {
            expected,
            supported: TRACE_SCHEMA_VERSION,
        }) if expected == TRACE_SCHEMA_VERSION + 1
    ));

    let recorded = execute(CANONICAL_SEED, 0);
    assert!(matches!(
        TraceReplayChecker::new(TRACE_SCHEMA_VERSION, recorded.trace.clone(), 1),
        Err(TraceReplayCheckerError::ExpectedTraceTooLong { events, limit: 1 })
            if events == recorded.trace.len()
    ));
}

#[test]
fn diagnostic_randomness_does_not_perturb_behavior() {
    const DEBUG_PRECONSUMPTION_OFFSETS: [usize; 7] = [0, 1, 2, 7, 31, 100, 257];

    let baseline = execute(42, DEBUG_PRECONSUMPTION_OFFSETS[0]);
    let expected = RecordedExecution::from_execution(&baseline);
    assert_eq!(debug_draws(&baseline.snapshot), 0);

    for offset in DEBUG_PRECONSUMPTION_OFFSETS.into_iter().skip(1) {
        let instrumented = execute(42, offset);
        assert_eq!(
            RecordedExecution::from_execution(&instrumented),
            expected,
            "Debug pre-consumption offset {offset} perturbed behavior"
        );
        assert_eq!(
            debug_draws(&instrumented.snapshot),
            offset as u64,
            "the differential run must actually consume its Debug draws"
        );
    }
}

#[test]
fn different_seeds_change_the_behavioral_artifact() {
    let first = execute(1, 0);
    let second = execute(2, 0);

    assert_ne!(
        RecordedExecution::from_execution(&first),
        RecordedExecution::from_execution(&second)
    );
    assert_ne!(first.trace_fingerprint, second.trace_fingerprint);
}

#[test]
fn cross_process_reproduction_matches_inline_golden() {
    let first = run_reproduction_helper_process();
    let second = run_reproduction_helper_process();

    assert_eq!(first, second, "separate reproduction processes diverged");
    assert_eq!(
        first,
        concat!(
            "replay-artifact-v5;driver=block-on-root-v1;runtime-reproduction-schema=3;",
            "checkpoint-schema=3;seed=0000000000005eed;rng-version=1;",
            "max-tasks=100000;max-timers=100000;max-steps-per-run=1000000;",
            "max-time-ns=none;start-time-ns=0;",
            "outcome=ok:7;now-ns=8;steps=3;enqueue-sequence=2;timer-sequence=1;",
            "timer-id=1;ready=0;timers=0;tasks=0;stopped=false;",
            "trace-schema=5;events=12;last-sequence=11;",
            "trace-fingerprint=9f8dba31ade10a3f;schedule=cbf01d8e038cdbac/0;",
            "scenario=92c825ae58d4ab69/0;workload=118c41cc1469a551/1;",
            "fault=3dab2990287c5617/0;debug=ebcf22fd690a8cfa/0"
        )
    );
}

#[test]
#[ignore = "spawned by cross_process_reproduction_matches_inline_golden"]
fn cross_process_reproduction_helper() {
    if std::env::var_os(REPRODUCTION_HELPER_ENV).is_none() {
        return;
    }
    println!("{REPLAY_ARTIFACT_MARKER}{}", canonical_replay_artifact());
}
