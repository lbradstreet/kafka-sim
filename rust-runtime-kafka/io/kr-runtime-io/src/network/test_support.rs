//! Reusable deterministic network scenarios for tests and diagnostic tools.

use std::collections::VecDeque;
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll, Waker};

use kr_runtime::{
    CompletionCertainty, RuntimeConfig, RuntimeSnapshot, SimDuration, SimInstant, SimRuntime,
};

use super::{
    ByteStreamSubmit, LinkConfig, LinkKey, LinkState, NetworkConfig, NetworkError, NetworkStatus,
    NodeId, ReadRequest, ReadResult, SimNetwork, WriteRequest, WriteResult,
};

/// Stable name of the focused directional-flow scenario.
pub const DIRECTIONAL_FLOW_TRACE_SCENARIO: &str = "partial_io_partition_backpressure_half_close";

/// Seed pinned by the focused directional-flow scenario.
pub const DIRECTIONAL_FLOW_TRACE_SEED: u64 = 0x6e65_7477_6f72_6b01;

/// Byte capacity of each directional flow in the focused scenario.
pub const DIRECTIONAL_FLOW_TRACE_CAPACITY: usize = 4;

/// Maximum bytes transferred by one completion in the focused scenario.
pub const DIRECTIONAL_FLOW_TRACE_MAX_CHUNK: usize = 3;

/// Left endpoint identity in the focused scenario.
pub const DIRECTIONAL_FLOW_TRACE_LEFT_NODE: NodeId = NodeId(1);

/// Right endpoint identity in the focused scenario.
pub const DIRECTIONAL_FLOW_TRACE_RIGHT_NODE: NodeId = NodeId(2);

const LINK_LATENCY: SimDuration = SimDuration::from_nanos(5);

/// One direction of the focused duplex connection.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FlowDirection {
    LeftToRight,
    RightToLeft,
}

impl FlowDirection {
    /// Returns the exact directional link represented by this value.
    #[must_use]
    pub const fn link(self) -> LinkKey {
        match self {
            Self::LeftToRight => LinkKey {
                from: DIRECTIONAL_FLOW_TRACE_LEFT_NODE,
                to: DIRECTIONAL_FLOW_TRACE_RIGHT_NODE,
            },
            Self::RightToLeft => LinkKey {
                from: DIRECTIONAL_FLOW_TRACE_RIGHT_NODE,
                to: DIRECTIONAL_FLOW_TRACE_LEFT_NODE,
            },
        }
    }
}

/// Independent model state for one directional byte flow.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DirectionalFlowSnapshot {
    pub bytes: Vec<u8>,
    pub sender_open: bool,
    pub link_state: LinkState,
}

/// Independent model state for both directions of the connection.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DuplexFlowSnapshot {
    pub left_to_right: DirectionalFlowSnapshot,
    pub right_to_left: DirectionalFlowSnapshot,
}

/// Exact request and result of one successful partial write.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WriteObservation {
    pub direction: FlowDirection,
    pub request: WriteRequest,
    pub result: WriteResult,
}

/// Exact request and result of one successful partial read.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReadObservation {
    pub direction: FlowDirection,
    pub request: ReadRequest,
    pub result: ReadResult,
}

/// Exact terminal information for a rejected write.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RejectedWriteObservation {
    pub direction: FlowDirection,
    pub request: WriteRequest,
    pub certainty: CompletionCertainty,
    pub error: NetworkError,
    pub returned_buffer: Vec<u8>,
    pub bytes_transferred: usize,
}

/// Structured result recorded at one scenario boundary.
#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum NetworkTraceDetail {
    LinkStateChanged {
        direction: FlowDirection,
        from: LinkState,
        to: LinkState,
    },
    WriteCompleted(WriteObservation),
    ReadCompleted(ReadObservation),
    WriteRejected(RejectedWriteObservation),
    CapacityPendingWrite {
        status_while_pending: NetworkStatus,
        unblocking_read: ReadObservation,
        completed_write: WriteObservation,
    },
    WriteHalfClosed {
        direction: FlowDirection,
    },
}

/// One assertion-bearing operation boundary in the focused network scenario.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NetworkTraceStep {
    pub sequence: u32,
    pub phase: &'static str,
    pub operation: &'static str,
    pub description: &'static str,
    pub started_at: SimInstant,
    pub completed_at: SimInstant,
    pub provider_before: NetworkStatus,
    pub provider_after: NetworkStatus,
    pub flow_before: DuplexFlowSnapshot,
    pub flow_after: DuplexFlowSnapshot,
    pub detail: NetworkTraceDetail,
}

/// Bounded deterministic execution of the focused directional-flow scenario.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DirectionalFlowTrace {
    pub scenario: &'static str,
    pub seed: u64,
    pub config: NetworkConfig,
    pub started_at: SimInstant,
    pub completed_at: SimInstant,
    pub provider_started: NetworkStatus,
    pub provider_completed: NetworkStatus,
    pub initial_flow: DuplexFlowSnapshot,
    pub steps: Vec<NetworkTraceStep>,
    pub runtime: RuntimeSnapshot,
}

#[derive(Debug)]
struct FlowModel {
    bytes: VecDeque<u8>,
    sender_open: bool,
    link_state: LinkState,
}

impl FlowModel {
    fn new() -> Self {
        Self {
            bytes: VecDeque::with_capacity(DIRECTIONAL_FLOW_TRACE_CAPACITY),
            sender_open: true,
            link_state: LinkState::Open,
        }
    }

    fn snapshot(&self) -> DirectionalFlowSnapshot {
        DirectionalFlowSnapshot {
            bytes: self.bytes.iter().copied().collect(),
            sender_open: self.sender_open,
            link_state: self.link_state,
        }
    }
}

#[derive(Debug)]
struct DuplexFlowModel {
    left_to_right: FlowModel,
    right_to_left: FlowModel,
}

impl DuplexFlowModel {
    fn new() -> Self {
        Self {
            left_to_right: FlowModel::new(),
            right_to_left: FlowModel::new(),
        }
    }

    fn flow(&self, direction: FlowDirection) -> &FlowModel {
        match direction {
            FlowDirection::LeftToRight => &self.left_to_right,
            FlowDirection::RightToLeft => &self.right_to_left,
        }
    }

    fn flow_mut(&mut self, direction: FlowDirection) -> &mut FlowModel {
        match direction {
            FlowDirection::LeftToRight => &mut self.left_to_right,
            FlowDirection::RightToLeft => &mut self.right_to_left,
        }
    }

    fn snapshot(&self) -> DuplexFlowSnapshot {
        DuplexFlowSnapshot {
            left_to_right: self.left_to_right.snapshot(),
            right_to_left: self.right_to_left.snapshot(),
        }
    }
}

struct StepStart {
    phase: &'static str,
    operation: &'static str,
    description: &'static str,
    started_at: SimInstant,
    provider_before: NetworkStatus,
    flow_before: DuplexFlowSnapshot,
}

fn start_step(
    runtime: &SimRuntime,
    network: &SimNetwork,
    model: &DuplexFlowModel,
    phase: &'static str,
    operation: &'static str,
    description: &'static str,
) -> StepStart {
    StepStart {
        phase,
        operation,
        description,
        started_at: runtime.snapshot().now,
        provider_before: network.status(),
        flow_before: model.snapshot(),
    }
}

fn finish_step(
    steps: &mut Vec<NetworkTraceStep>,
    runtime: &SimRuntime,
    network: &SimNetwork,
    model: &DuplexFlowModel,
    start: StepStart,
    detail: NetworkTraceDetail,
) {
    let sequence = u32::try_from(steps.len()).expect("focused trace step count fits u32");
    steps.push(NetworkTraceStep {
        sequence,
        phase: start.phase,
        operation: start.operation,
        description: start.description,
        started_at: start.started_at,
        completed_at: runtime.snapshot().now,
        provider_before: start.provider_before,
        provider_after: network.status(),
        flow_before: start.flow_before,
        flow_after: model.snapshot(),
        detail,
    });
}

fn poll_once<F: Future + Unpin>(future: &mut F) -> Poll<F::Output> {
    let waker = Waker::noop();
    let mut context = Context::from_waker(waker);
    Pin::new(future).poll(&mut context)
}

fn link_config(state: LinkState) -> LinkConfig {
    LinkConfig {
        latency: LINK_LATENCY,
        max_chunk_bytes: DIRECTIONAL_FLOW_TRACE_MAX_CHUNK,
        state,
    }
}

fn assert_quiescent_status(status: NetworkStatus) {
    assert_eq!(status.listeners, 0);
    assert_eq!(status.connections, 1);
    assert_eq!(status.inflight_operations, 0);
    assert_eq!(status.pending_faults, 0);
    assert_eq!(status.fault_hits, 0);
}

fn check_write(
    model: &mut FlowModel,
    request: &WriteRequest,
    result: &WriteResult,
    expected_written: usize,
) {
    assert!(model.sender_open, "model write direction must be open");
    assert_eq!(model.link_state, LinkState::Open);
    assert_eq!(
        result.buffer, request.buffer,
        "write must return its buffer"
    );
    assert_eq!(result.bytes_written, expected_written);
    assert!(expected_written <= request.buffer.len());
    assert!(model.bytes.len() + expected_written <= DIRECTIONAL_FLOW_TRACE_CAPACITY);
    model
        .bytes
        .extend(request.buffer[..expected_written].iter().copied());
}

fn check_read(
    model: &mut FlowModel,
    request: &ReadRequest,
    result: &ReadResult,
    expected_read: usize,
    expected_eof: bool,
) {
    assert!(expected_read <= request.max_bytes);
    assert_eq!(model.link_state, LinkState::Open);
    let payload = model
        .bytes
        .iter()
        .take(expected_read)
        .copied()
        .collect::<Vec<_>>();
    assert_eq!(
        payload.len(),
        expected_read,
        "model has expected read bytes"
    );
    let mut expected_buffer = request.buffer.clone();
    expected_buffer.extend_from_slice(&payload);
    assert_eq!(result.buffer, expected_buffer);
    assert_eq!(result.bytes_read, expected_read);
    assert_eq!(result.end_of_stream, expected_eof);
    for expected in payload {
        assert_eq!(model.bytes.pop_front(), Some(expected));
    }
    assert_eq!(expected_eof, expected_read == 0 && !model.sender_open);
}

/// Runs the focused duplex-flow scenario used by diagnostics and its unit test.
///
/// Every operation result is checked against a separate `VecDeque` byte-flow
/// model before the corresponding step is retained. Status calls are passive
/// observations; they do not drive the provider or the runtime.
#[must_use]
pub fn run_directional_flow_trace() -> DirectionalFlowTrace {
    let config = NetworkConfig {
        directional_buffer_bytes: DIRECTIONAL_FLOW_TRACE_CAPACITY,
        default_link: link_config(LinkState::Open),
        ..NetworkConfig::default()
    };
    let mut runtime = SimRuntime::new(RuntimeConfig {
        seed: DIRECTIONAL_FLOW_TRACE_SEED,
        ..RuntimeConfig::default()
    });
    let started_at = runtime.snapshot().now;
    let network =
        SimNetwork::new(runtime.handle(), config).expect("focused network config is valid");
    let (left, right) = network
        .connected_pair(
            DIRECTIONAL_FLOW_TRACE_LEFT_NODE,
            DIRECTIONAL_FLOW_TRACE_RIGHT_NODE,
        )
        .expect("focused network pair is admitted");
    let provider_started = network.status();
    assert_quiescent_status(provider_started);
    let mut model = DuplexFlowModel::new();
    let initial_flow = model.snapshot();
    let mut steps = Vec::with_capacity(12);
    let direction = FlowDirection::LeftToRight;

    let request = WriteRequest {
        buffer: b"ABCDE".to_vec(),
    };
    let start = start_step(
        &runtime,
        &network,
        &model,
        "partial-io",
        "write",
        "Write only the first three bytes because the link chunk is bounded.",
    );
    let result = runtime
        .block_on(left.submit_write(request.clone()))
        .expect("runtime drives partial write")
        .expect("partial write succeeds");
    check_write(model.flow_mut(direction), &request, &result, 3);
    finish_step(
        &mut steps,
        &runtime,
        &network,
        &model,
        start,
        NetworkTraceDetail::WriteCompleted(WriteObservation {
            direction,
            request,
            result,
        }),
    );

    let request = ReadRequest {
        buffer: vec![0xa5],
        max_bytes: 8,
    };
    let start = start_step(
        &runtime,
        &network,
        &model,
        "partial-io",
        "read",
        "Read the three queued bytes into a caller-owned prefix buffer.",
    );
    let result = runtime
        .block_on(right.submit_read(request.clone()))
        .expect("runtime drives partial read")
        .expect("partial read succeeds");
    check_read(model.flow_mut(direction), &request, &result, 3, false);
    finish_step(
        &mut steps,
        &runtime,
        &network,
        &model,
        start,
        NetworkTraceDetail::ReadCompleted(ReadObservation {
            direction,
            request,
            result,
        }),
    );

    for (payload, description) in [
        (
            vec![0x10, 0x11, 0x12],
            "Fill three of the four directional buffer slots.",
        ),
        (vec![0x13], "Fill the final directional buffer slot."),
    ] {
        let request = WriteRequest { buffer: payload };
        let start = start_step(&runtime, &network, &model, "fill", "write", description);
        let result = runtime
            .block_on(left.submit_write(request.clone()))
            .expect("runtime drives filling write")
            .expect("filling write succeeds");
        let expected_written = request.buffer.len();
        check_write(
            model.flow_mut(direction),
            &request,
            &result,
            expected_written,
        );
        finish_step(
            &mut steps,
            &runtime,
            &network,
            &model,
            start,
            NetworkTraceDetail::WriteCompleted(WriteObservation {
                direction,
                request,
                result,
            }),
        );
    }
    assert_eq!(
        model.flow(direction).bytes.len(),
        DIRECTIONAL_FLOW_TRACE_CAPACITY
    );

    let start = start_step(
        &runtime,
        &network,
        &model,
        "partition",
        "partition",
        "Partition the left-to-right link without changing buffered bytes.",
    );
    network
        .set_link(direction.link(), link_config(LinkState::Partitioned))
        .expect("focused partition config is valid");
    model.flow_mut(direction).link_state = LinkState::Partitioned;
    finish_step(
        &mut steps,
        &runtime,
        &network,
        &model,
        start,
        NetworkTraceDetail::LinkStateChanged {
            direction,
            from: LinkState::Open,
            to: LinkState::Partitioned,
        },
    );

    let request = WriteRequest {
        buffer: vec![0x70, 0x71],
    };
    let start = start_step(
        &runtime,
        &network,
        &model,
        "partition",
        "write",
        "Reject a write on the partitioned link without applying any bytes.",
    );
    let error = runtime
        .block_on(left.submit_write(request.clone()))
        .expect("runtime drives partitioned write")
        .expect_err("partitioned write is rejected");
    let (certainty, failure) = error.into_parts();
    let network_error = failure.error().clone();
    let bytes_transferred = failure.bytes_transferred();
    let returned_buffer = failure
        .into_buffer()
        .expect("rejected write returns its buffer");
    assert_eq!(certainty, CompletionCertainty::NotApplied);
    assert_eq!(
        network_error,
        NetworkError::Partitioned {
            link: direction.link()
        }
    );
    assert_eq!(returned_buffer, request.buffer);
    assert_eq!(bytes_transferred, 0);
    finish_step(
        &mut steps,
        &runtime,
        &network,
        &model,
        start,
        NetworkTraceDetail::WriteRejected(RejectedWriteObservation {
            direction,
            request,
            certainty,
            error: network_error,
            returned_buffer,
            bytes_transferred,
        }),
    );

    let start = start_step(
        &runtime,
        &network,
        &model,
        "partition",
        "heal",
        "Heal the left-to-right link while retaining the full buffer.",
    );
    network
        .set_link(direction.link(), link_config(LinkState::Open))
        .expect("focused healed config is valid");
    model.flow_mut(direction).link_state = LinkState::Open;
    finish_step(
        &mut steps,
        &runtime,
        &network,
        &model,
        start,
        NetworkTraceDetail::LinkStateChanged {
            direction,
            from: LinkState::Partitioned,
            to: LinkState::Open,
        },
    );

    let write_request = WriteRequest { buffer: vec![0x20] };
    let start = start_step(
        &runtime,
        &network,
        &model,
        "backpressure",
        "write",
        "Hold a write pending on the full buffer, then free one byte with a read.",
    );
    let mut pending_write = left.submit_write(write_request.clone());
    assert!(poll_once(&mut pending_write).is_pending());
    let status_while_pending = network.status();
    assert_eq!(status_while_pending.inflight_operations, 1);
    assert_eq!(
        model.flow(direction).bytes.len(),
        DIRECTIONAL_FLOW_TRACE_CAPACITY
    );

    let read_request = ReadRequest {
        buffer: Vec::new(),
        max_bytes: 1,
    };
    let read_result = runtime
        .block_on(right.submit_read(read_request.clone()))
        .expect("runtime drives capacity-opening read")
        .expect("capacity-opening read succeeds");
    check_read(
        model.flow_mut(direction),
        &read_request,
        &read_result,
        1,
        false,
    );
    let write_result = runtime
        .block_on(pending_write)
        .expect("runtime drives unblocked write")
        .expect("capacity-blocked write succeeds");
    check_write(model.flow_mut(direction), &write_request, &write_result, 1);
    finish_step(
        &mut steps,
        &runtime,
        &network,
        &model,
        start,
        NetworkTraceDetail::CapacityPendingWrite {
            status_while_pending,
            unblocking_read: ReadObservation {
                direction,
                request: read_request,
                result: read_result,
            },
            completed_write: WriteObservation {
                direction,
                request: write_request,
                result: write_result,
            },
        },
    );

    let start = start_step(
        &runtime,
        &network,
        &model,
        "half-close",
        "shutdown-write",
        "Close the sender while preserving its four buffered bytes for the receiver.",
    );
    runtime
        .block_on(left.submit_shutdown_write())
        .expect("runtime drives write shutdown")
        .expect("write shutdown succeeds");
    model.flow_mut(direction).sender_open = false;
    finish_step(
        &mut steps,
        &runtime,
        &network,
        &model,
        start,
        NetworkTraceDetail::WriteHalfClosed { direction },
    );

    for (phase, description, expected_read) in [
        (
            "drain",
            "Read one maximum-sized chunk without reporting EOF.",
            3,
        ),
        (
            "drain",
            "Read the final buffered byte without reporting EOF yet.",
            1,
        ),
        (
            "eof",
            "Observe EOF only after the closed sender's bytes are drained.",
            0,
        ),
    ] {
        let request = ReadRequest {
            buffer: Vec::new(),
            max_bytes: 8,
        };
        let start = start_step(&runtime, &network, &model, phase, "read", description);
        let result = runtime
            .block_on(right.submit_read(request.clone()))
            .expect("runtime drives drain read")
            .expect("drain read succeeds");
        let eof = expected_read == 0;
        check_read(
            model.flow_mut(direction),
            &request,
            &result,
            expected_read,
            eof,
        );
        finish_step(
            &mut steps,
            &runtime,
            &network,
            &model,
            start,
            NetworkTraceDetail::ReadCompleted(ReadObservation {
                direction,
                request,
                result,
            }),
        );
    }

    for step in &steps {
        assert_quiescent_status(step.provider_before);
        assert_quiescent_status(step.provider_after);
    }
    let completed_at = runtime.snapshot().now;
    let provider_completed = network.status();
    assert_quiescent_status(provider_completed);
    assert!(model.flow(direction).bytes.is_empty());
    assert!(!model.flow(direction).sender_open);

    drop(left);
    drop(right);
    drop(network);
    runtime
        .shutdown()
        .expect("focused network runtime shuts down");

    DirectionalFlowTrace {
        scenario: DIRECTIONAL_FLOW_TRACE_SCENARIO,
        seed: DIRECTIONAL_FLOW_TRACE_SEED,
        config,
        started_at,
        completed_at,
        provider_started,
        provider_completed,
        initial_flow,
        steps,
        runtime: runtime.snapshot(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn focused_directional_flow_trace_replays_and_covers_contract_edges() {
        let first = run_directional_flow_trace();
        let repeated = run_directional_flow_trace();
        assert_eq!(first, repeated, "focused network trace did not replay");

        assert_eq!(first.scenario, DIRECTIONAL_FLOW_TRACE_SCENARIO);
        assert_eq!(first.started_at, SimInstant::ZERO);
        assert!(first.completed_at > first.started_at);
        assert_eq!(first.runtime.now, first.completed_at);
        assert_eq!(first.steps.len(), 12);
        assert!(
            first
                .steps
                .iter()
                .enumerate()
                .all(|(index, step)| step.sequence == index as u32)
        );

        let mut partial_write = false;
        let mut partial_read = false;
        let mut partition_rejection = false;
        let mut capacity_pending = false;
        let mut half_close = false;
        let mut drain_reads = 0;
        let mut eof = false;
        for step in &first.steps {
            assert!(step.completed_at >= step.started_at);
            match &step.detail {
                NetworkTraceDetail::WriteCompleted(observation) => {
                    partial_write |=
                        observation.result.bytes_written < observation.request.buffer.len();
                }
                NetworkTraceDetail::ReadCompleted(observation) => {
                    partial_read |= observation.result.bytes_read < observation.request.max_bytes
                        && observation.result.bytes_read > 0;
                    if step.phase == "drain" {
                        drain_reads += 1;
                        assert!(!observation.result.end_of_stream);
                    }
                    if observation.result.end_of_stream {
                        eof = true;
                        assert_eq!(step.phase, "eof");
                        assert_eq!(observation.result.bytes_read, 0);
                    }
                }
                NetworkTraceDetail::WriteRejected(observation) => {
                    partition_rejection =
                        matches!(observation.error, NetworkError::Partitioned { .. })
                            && observation.certainty == CompletionCertainty::NotApplied
                            && observation.bytes_transferred == 0
                            && observation.returned_buffer == observation.request.buffer;
                }
                NetworkTraceDetail::CapacityPendingWrite {
                    status_while_pending,
                    unblocking_read,
                    completed_write,
                } => {
                    capacity_pending = status_while_pending.inflight_operations == 1
                        && step.flow_before.left_to_right.bytes == [0x10, 0x11, 0x12, 0x13]
                        && unblocking_read.result.buffer == [0x10]
                        && unblocking_read.result.bytes_read == 1
                        && completed_write.result.buffer == [0x20]
                        && completed_write.result.bytes_written == 1
                        && step.flow_after.left_to_right.bytes == [0x11, 0x12, 0x13, 0x20];
                }
                NetworkTraceDetail::WriteHalfClosed { direction } => {
                    half_close = *direction == FlowDirection::LeftToRight
                        && step.flow_before.left_to_right.sender_open
                        && !step.flow_after.left_to_right.sender_open
                        && step.flow_before.left_to_right.bytes
                            == step.flow_after.left_to_right.bytes;
                }
                NetworkTraceDetail::LinkStateChanged { .. } => {}
            }
        }

        assert!(partial_write, "scenario missed a partial write");
        assert!(partial_read, "scenario missed a partial read");
        assert!(partition_rejection, "scenario missed partition rejection");
        assert!(capacity_pending, "scenario missed capacity pending write");
        assert!(half_close, "scenario missed half-close preservation");
        assert_eq!(drain_reads, 2);
        assert!(eof, "scenario missed EOF after drain");
        assert!(
            first
                .steps
                .last()
                .is_some_and(|step| step.flow_after.left_to_right.bytes.is_empty()
                    && !step.flow_after.left_to_right.sender_open)
        );
        assert!(first.initial_flow.right_to_left.sender_open);
        assert!(first.initial_flow.right_to_left.bytes.is_empty());
    }
}
