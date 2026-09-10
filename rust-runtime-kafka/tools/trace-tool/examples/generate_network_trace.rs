use std::collections::BTreeMap;
use std::error::Error;
use std::fs::File;
use std::io::BufWriter;
use std::path::PathBuf;

use kr_runtime::rng::RandomStream;
use kr_runtime_io::network::test_support::{
    DIRECTIONAL_FLOW_TRACE_LEFT_NODE, DIRECTIONAL_FLOW_TRACE_RIGHT_NODE, DirectionalFlowSnapshot,
    DirectionalFlowTrace, DuplexFlowSnapshot, FlowDirection, NetworkTraceDetail, ReadObservation,
    WriteObservation, run_directional_flow_trace,
};
use kr_runtime_io::network::{LinkConfig, LinkState, NetworkConfig, NetworkError, NetworkStatus};
use serde::Serialize;
use serde_json::Value;

#[path = "support/json_artifact.rs"]
mod json_artifact;

use json_artifact::{DecimalU64, certainty_name, write_javascript};

const ARTIFACT_SCHEMA: u32 = 2;
const SOURCE_TEST: &str = "io/kr-runtime-io/src/network/test_support.rs::tests::focused_directional_flow_trace_replays_and_covers_contract_edges";
const MAX_SAFE_JSON_INTEGER: u64 = 9_007_199_254_740_991;

fn main() -> Result<(), Box<dyn Error>> {
    let destination = std::env::args_os().nth(1).map_or_else(
        || PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("network-trace-data.js"),
        PathBuf::from,
    );
    let trace = run_directional_flow_trace();
    let mut output = BufWriter::new(File::create(destination)?);
    write_javascript(
        &mut output,
        "generate_network_trace",
        "NETWORK_TRACE_DATA",
        &Artifact::from_trace(&trace),
    )?;
    Ok(())
}

#[derive(Serialize)]
struct Artifact<'a> {
    schema: u32,
    scenario: &'static str,
    source_test: &'static str,
    provider: &'static str,
    started_at_ns: DecimalU64,
    completed_at_ns: DecimalU64,
    endpoints: EndpointsRecord,
    runtime: RuntimeRecord,
    config: ConfigRecord,
    provider_started: ProviderStatusRecord,
    provider_completed: ProviderStatusRecord,
    initial_flow: DuplexFlowRecord<'a>,
    steps: Vec<StepRecord<'a>>,
}

impl<'a> Artifact<'a> {
    fn from_trace(trace: &'a DirectionalFlowTrace) -> Self {
        let checkpoint = trace.runtime.determinism_checkpoint();
        assert_eq!(
            trace.seed, trace.runtime.reproduction.config.seed,
            "scenario and runtime reproduction seeds must agree"
        );
        Self {
            schema: ARTIFACT_SCHEMA,
            scenario: trace.scenario,
            source_test: SOURCE_TEST,
            provider: "SimNetwork",
            started_at_ns: DecimalU64(trace.started_at.as_nanos()),
            completed_at_ns: DecimalU64(trace.completed_at.as_nanos()),
            endpoints: EndpointsRecord {
                left_node: DecimalU64(DIRECTIONAL_FLOW_TRACE_LEFT_NODE.0),
                right_node: DecimalU64(DIRECTIONAL_FLOW_TRACE_RIGHT_NODE.0),
            },
            runtime: RuntimeRecord {
                reproduction_schema: trace.runtime.reproduction.schema_version,
                checkpoint_schema: checkpoint.schema_version,
                rng_version: trace.runtime.reproduction.rng_version,
                seed: DecimalU64(trace.seed),
                max_tasks: safe_count(trace.runtime.reproduction.config.max_tasks),
                max_timers: safe_count(trace.runtime.reproduction.config.max_timers),
                max_steps_per_run: DecimalU64(trace.runtime.reproduction.config.max_steps_per_run),
                max_time_ns: trace
                    .runtime
                    .reproduction
                    .config
                    .max_time
                    .map(|time| DecimalU64(time.as_nanos())),
                now_ns: DecimalU64(checkpoint.now.as_nanos()),
                total_steps: DecimalU64(checkpoint.total_steps),
                next_enqueue_sequence: DecimalU64(checkpoint.next_enqueue_sequence),
                next_timer_sequence: DecimalU64(checkpoint.next_timer_sequence),
                next_timer_id: DecimalU64(checkpoint.next_timer_id),
                ready_tasks: safe_count(checkpoint.ready_tasks),
                live_timers: safe_count(checkpoint.live_timers),
                live_tasks: safe_count(checkpoint.live_tasks),
                stopped: checkpoint.stopped,
                random: checkpoint
                    .random
                    .iter()
                    .map(|snapshot| RandomStreamRecord {
                        stream: random_stream_name(snapshot.stream),
                        state: DecimalU64(snapshot.checkpoint.state()),
                        draws: DecimalU64(snapshot.checkpoint.draws()),
                    })
                    .collect(),
            },
            config: ConfigRecord::from_config(&trace.config),
            provider_started: ProviderStatusRecord::from_status(trace.provider_started),
            provider_completed: ProviderStatusRecord::from_status(trace.provider_completed),
            initial_flow: DuplexFlowRecord::from_snapshot(&trace.initial_flow),
            steps: trace.steps.iter().map(StepRecord::from_step).collect(),
        }
    }
}

#[derive(Serialize)]
struct EndpointsRecord {
    left_node: DecimalU64,
    right_node: DecimalU64,
}

#[derive(Serialize)]
struct RuntimeRecord {
    reproduction_schema: u32,
    checkpoint_schema: u32,
    rng_version: u32,
    seed: DecimalU64,
    max_tasks: usize,
    max_timers: usize,
    max_steps_per_run: DecimalU64,
    max_time_ns: Option<DecimalU64>,
    now_ns: DecimalU64,
    total_steps: DecimalU64,
    next_enqueue_sequence: DecimalU64,
    next_timer_sequence: DecimalU64,
    next_timer_id: DecimalU64,
    ready_tasks: usize,
    live_timers: usize,
    live_tasks: usize,
    stopped: bool,
    random: Vec<RandomStreamRecord>,
}

#[derive(Serialize)]
struct RandomStreamRecord {
    stream: &'static str,
    state: DecimalU64,
    draws: DecimalU64,
}

#[derive(Serialize)]
struct ConfigRecord {
    max_listeners: usize,
    max_listener_backlog: usize,
    max_connections: usize,
    max_inflight_operations: usize,
    directional_buffer_bytes: usize,
    max_operation_bytes: usize,
    max_scripted_faults: usize,
    default_link: LinkConfigRecord,
}

impl ConfigRecord {
    fn from_config(config: &NetworkConfig) -> Self {
        Self {
            max_listeners: safe_count(config.max_listeners),
            max_listener_backlog: safe_count(config.max_listener_backlog),
            max_connections: safe_count(config.max_connections),
            max_inflight_operations: safe_count(config.max_inflight_operations),
            directional_buffer_bytes: safe_count(config.directional_buffer_bytes),
            max_operation_bytes: safe_count(config.max_operation_bytes),
            max_scripted_faults: safe_count(config.max_scripted_faults),
            default_link: LinkConfigRecord::from_config(config.default_link),
        }
    }
}

#[derive(Serialize)]
struct LinkConfigRecord {
    latency_ns: DecimalU64,
    max_chunk_bytes: usize,
    state: &'static str,
}

impl LinkConfigRecord {
    fn from_config(config: LinkConfig) -> Self {
        Self {
            latency_ns: DecimalU64(config.latency.as_nanos()),
            max_chunk_bytes: safe_count(config.max_chunk_bytes),
            state: link_state_name(config.state),
        }
    }
}

#[derive(Serialize)]
struct StepRecord<'a> {
    sequence: u32,
    phase: &'a str,
    operation: &'a str,
    description: &'a str,
    started_at_ns: DecimalU64,
    completed_at_ns: DecimalU64,
    duration_ns: DecimalU64,
    outcome: &'static str,
    certainty: Option<&'static str>,
    summary: String,
    fields: BTreeMap<&'static str, Value>,
    provider_before: ProviderStatusRecord,
    provider_after: ProviderStatusRecord,
    flow_before: DuplexFlowRecord<'a>,
    flow_after: DuplexFlowRecord<'a>,
}

impl<'a> StepRecord<'a> {
    fn from_step(step: &'a kr_runtime_io::network::test_support::NetworkTraceStep) -> Self {
        let detail = DetailRecord::from_detail(&step.detail);
        Self {
            sequence: step.sequence,
            phase: step.phase,
            operation: step.operation,
            description: step.description,
            started_at_ns: DecimalU64(step.started_at.as_nanos()),
            completed_at_ns: DecimalU64(step.completed_at.as_nanos()),
            duration_ns: DecimalU64(
                step.completed_at
                    .checked_duration_since(step.started_at)
                    .expect("scenario time is monotonic")
                    .as_nanos(),
            ),
            outcome: detail.outcome,
            certainty: detail.certainty,
            summary: detail.summary,
            fields: detail.fields,
            provider_before: ProviderStatusRecord::from_status(step.provider_before),
            provider_after: ProviderStatusRecord::from_status(step.provider_after),
            flow_before: DuplexFlowRecord::from_snapshot(&step.flow_before),
            flow_after: DuplexFlowRecord::from_snapshot(&step.flow_after),
        }
    }
}

struct DetailRecord {
    outcome: &'static str,
    certainty: Option<&'static str>,
    summary: String,
    fields: BTreeMap<&'static str, Value>,
}

impl DetailRecord {
    fn from_detail(detail: &NetworkTraceDetail) -> Self {
        let mut fields = BTreeMap::new();
        match detail {
            NetworkTraceDetail::LinkStateChanged {
                direction,
                from,
                to,
            } => {
                fields.insert(
                    "direction",
                    Value::String(direction_name(*direction).into()),
                );
                fields.insert("from", Value::String(link_state_name(*from).into()));
                fields.insert("to", Value::String(link_state_name(*to).into()));
                Self {
                    outcome: "success",
                    certainty: None,
                    summary: format!(
                        "{} link changed from {} to {}",
                        direction_name(*direction),
                        link_state_name(*from),
                        link_state_name(*to)
                    ),
                    fields,
                }
            }
            NetworkTraceDetail::WriteCompleted(observation) => {
                fields.insert(
                    "write",
                    value_from(WriteObservationRecord::from_observation(observation)),
                );
                Self {
                    outcome: "success",
                    certainty: None,
                    summary: format!(
                        "wrote {} of {} requested byte(s) {}",
                        observation.result.bytes_written,
                        observation.request.buffer.len(),
                        direction_name(observation.direction)
                    ),
                    fields,
                }
            }
            NetworkTraceDetail::ReadCompleted(observation) => {
                fields.insert(
                    "read",
                    value_from(ReadObservationRecord::from_observation(observation)),
                );
                let eof = observation.result.end_of_stream;
                Self {
                    outcome: if eof { "eof" } else { "success" },
                    certainty: None,
                    summary: if eof {
                        format!(
                            "observed EOF {} after all buffered bytes drained",
                            direction_name(observation.direction)
                        )
                    } else {
                        format!(
                            "read {} byte(s) {}",
                            observation.result.bytes_read,
                            direction_name(observation.direction)
                        )
                    },
                    fields,
                }
            }
            NetworkTraceDetail::WriteRejected(observation) => {
                fields.insert(
                    "direction",
                    Value::String(direction_name(observation.direction).into()),
                );
                fields.insert(
                    "request_buffer",
                    Value::Array(
                        observation
                            .request
                            .buffer
                            .iter()
                            .copied()
                            .map(Value::from)
                            .collect(),
                    ),
                );
                fields.insert(
                    "returned_buffer",
                    Value::Array(
                        observation
                            .returned_buffer
                            .iter()
                            .copied()
                            .map(Value::from)
                            .collect(),
                    ),
                );
                fields.insert(
                    "bytes_transferred",
                    Value::from(safe_count(observation.bytes_transferred)),
                );
                fields.insert(
                    "error_type",
                    Value::String(network_error_name(&observation.error).into()),
                );
                if let NetworkError::Partitioned { link } = &observation.error {
                    fields.insert("link_from", Value::String(link.from.0.to_string()));
                    fields.insert("link_to", Value::String(link.to.0.to_string()));
                }
                Self {
                    outcome: "rejected",
                    certainty: Some(certainty_name(observation.certainty)),
                    summary: observation.error.to_string(),
                    fields,
                }
            }
            NetworkTraceDetail::CapacityPendingWrite {
                status_while_pending,
                unblocking_read,
                completed_write,
            } => {
                fields.insert(
                    "status_while_pending",
                    value_from(ProviderStatusRecord::from_status(*status_while_pending)),
                );
                fields.insert(
                    "unblocking_read",
                    value_from(ReadObservationRecord::from_observation(unblocking_read)),
                );
                fields.insert(
                    "completed_write",
                    value_from(WriteObservationRecord::from_observation(completed_write)),
                );
                Self {
                    outcome: "pending_then_completed",
                    certainty: None,
                    summary:
                        "write waited for capacity, a read freed one byte, and the write completed"
                            .into(),
                    fields,
                }
            }
            NetworkTraceDetail::WriteHalfClosed { direction } => {
                fields.insert(
                    "direction",
                    Value::String(direction_name(*direction).into()),
                );
                Self {
                    outcome: "half_closed",
                    certainty: None,
                    summary: format!(
                        "{} sender closed while preserving buffered bytes",
                        direction_name(*direction)
                    ),
                    fields,
                }
            }
            _ => panic!(
                "network trace artifact schema {ARTIFACT_SCHEMA} does not support detail: {detail:?}"
            ),
        }
    }
}

#[derive(Serialize)]
struct ReadObservationRecord<'a> {
    direction: &'static str,
    request_buffer: &'a [u8],
    max_bytes: usize,
    result_buffer: &'a [u8],
    bytes_read: usize,
    end_of_stream: bool,
}

impl<'a> ReadObservationRecord<'a> {
    fn from_observation(observation: &'a ReadObservation) -> Self {
        Self {
            direction: direction_name(observation.direction),
            request_buffer: &observation.request.buffer,
            max_bytes: safe_count(observation.request.max_bytes),
            result_buffer: &observation.result.buffer,
            bytes_read: safe_count(observation.result.bytes_read),
            end_of_stream: observation.result.end_of_stream,
        }
    }
}

#[derive(Serialize)]
struct WriteObservationRecord<'a> {
    direction: &'static str,
    request_buffer: &'a [u8],
    result_buffer: &'a [u8],
    bytes_written: usize,
}

impl<'a> WriteObservationRecord<'a> {
    fn from_observation(observation: &'a WriteObservation) -> Self {
        Self {
            direction: direction_name(observation.direction),
            request_buffer: &observation.request.buffer,
            result_buffer: &observation.result.buffer,
            bytes_written: safe_count(observation.result.bytes_written),
        }
    }
}

#[derive(Serialize)]
struct ProviderStatusRecord {
    listeners: usize,
    connections: usize,
    inflight_operations: usize,
    pending_faults: usize,
    fault_hits: DecimalU64,
}

impl ProviderStatusRecord {
    fn from_status(status: NetworkStatus) -> Self {
        Self {
            listeners: safe_count(status.listeners),
            connections: safe_count(status.connections),
            inflight_operations: safe_count(status.inflight_operations),
            pending_faults: safe_count(status.pending_faults),
            fault_hits: DecimalU64(status.fault_hits),
        }
    }
}

#[derive(Serialize)]
struct DuplexFlowRecord<'a> {
    left_to_right: DirectionalFlowRecord<'a>,
    right_to_left: DirectionalFlowRecord<'a>,
}

impl<'a> DuplexFlowRecord<'a> {
    fn from_snapshot(snapshot: &'a DuplexFlowSnapshot) -> Self {
        Self {
            left_to_right: DirectionalFlowRecord::from_snapshot(&snapshot.left_to_right),
            right_to_left: DirectionalFlowRecord::from_snapshot(&snapshot.right_to_left),
        }
    }
}

#[derive(Serialize)]
struct DirectionalFlowRecord<'a> {
    bytes: &'a [u8],
    sender_open: bool,
    link_state: &'static str,
}

impl<'a> DirectionalFlowRecord<'a> {
    fn from_snapshot(snapshot: &'a DirectionalFlowSnapshot) -> Self {
        Self {
            bytes: &snapshot.bytes,
            sender_open: snapshot.sender_open,
            link_state: link_state_name(snapshot.link_state),
        }
    }
}

fn value_from(value: impl Serialize) -> Value {
    serde_json::to_value(value).expect("artifact records serialize to JSON values")
}

fn safe_count(value: usize) -> usize {
    let exact = u64::try_from(value).expect("usize count fits u64");
    assert!(
        exact <= MAX_SAFE_JSON_INTEGER,
        "count {value} exceeds JavaScript's exact integer range"
    );
    value
}

fn direction_name(direction: FlowDirection) -> &'static str {
    match direction {
        FlowDirection::LeftToRight => "left_to_right",
        FlowDirection::RightToLeft => "right_to_left",
    }
}

fn link_state_name(state: LinkState) -> &'static str {
    match state {
        LinkState::Open => "open",
        LinkState::Clogged => "clogged",
        LinkState::Partitioned => "partitioned",
    }
}

fn network_error_name(error: &NetworkError) -> &'static str {
    match error {
        NetworkError::Partitioned { .. } => "partitioned",
        _ => "network_error",
    }
}

fn random_stream_name(stream: RandomStream) -> &'static str {
    match stream {
        RandomStream::Schedule => "schedule",
        RandomStream::Scenario => "scenario",
        RandomStream::Workload => "workload",
        RandomStream::Fault => "fault",
        RandomStream::Debug => "debug",
        _ => panic!(
            "network trace artifact schema {ARTIFACT_SCHEMA} does not support this random stream"
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn field<'a>(step: &'a StepRecord<'_>, name: &str) -> &'a Value {
        step.fields.get(name).expect("expected structured field")
    }

    #[test]
    fn generated_network_trace_is_deterministic_and_covers_contract_edges() {
        let render = || {
            let trace = run_directional_flow_trace();
            let artifact = Artifact::from_trace(&trace);
            let mut bytes = Vec::new();
            write_javascript(
                &mut bytes,
                "generate_network_trace",
                "NETWORK_TRACE_DATA",
                &artifact,
            )
            .unwrap();
            bytes
        };

        let first = render();
        assert_eq!(first, render());

        let trace = run_directional_flow_trace();
        let artifact = Artifact::from_trace(&trace);
        assert_eq!(artifact.schema, ARTIFACT_SCHEMA);
        assert_eq!(artifact.steps.len(), 12);

        let partition = artifact
            .steps
            .iter()
            .find(|step| step.phase == "partition" && step.outcome == "rejected")
            .expect("partition rejection step");
        assert_eq!(partition.certainty, Some("not_applied"));
        assert_eq!(field(partition, "error_type"), "partitioned");
        assert_eq!(field(partition, "bytes_transferred"), 0);
        assert_eq!(
            field(partition, "request_buffer"),
            field(partition, "returned_buffer")
        );

        let pending = artifact
            .steps
            .iter()
            .find(|step| step.outcome == "pending_then_completed")
            .expect("capacity-pending step");
        assert_eq!(
            field(pending, "status_while_pending")["inflight_operations"],
            1
        );
        assert_eq!(field(pending, "unblocking_read")["bytes_read"], 1);
        assert_eq!(field(pending, "completed_write")["bytes_written"], 1);

        let half_close = artifact
            .steps
            .iter()
            .find(|step| step.outcome == "half_closed")
            .expect("half-close step");
        assert!(half_close.flow_before.left_to_right.sender_open);
        assert!(!half_close.flow_after.left_to_right.sender_open);
        assert_eq!(
            half_close.flow_before.left_to_right.bytes,
            half_close.flow_after.left_to_right.bytes
        );

        let eof = artifact
            .steps
            .iter()
            .find(|step| step.outcome == "eof")
            .expect("EOF step");
        assert_eq!(field(eof, "read")["end_of_stream"], true);
        assert_eq!(field(eof, "read")["bytes_read"], 0);
        assert!(eof.flow_after.left_to_right.bytes.is_empty());
        assert!(!eof.flow_after.left_to_right.sender_open);
    }
}
