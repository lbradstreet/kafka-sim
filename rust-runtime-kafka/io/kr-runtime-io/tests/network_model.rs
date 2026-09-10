use std::cell::RefCell;
use std::collections::VecDeque;
use std::rc::Rc;

use kr_runtime::rng::RandomStream;
use kr_runtime::{DeterminismCheckpoint, RandomHandle, RuntimeConfig, SimDuration, SimRuntime};
use kr_runtime_io::latency::SimLatencyModel;
use kr_runtime_io::network::{
    ByteStreamSubmit, LinkConfig, LinkKey, LinkState, NetworkConfig, NetworkError, NodeId,
    ReadRequest, SimNetwork, SimStream, WriteRequest,
};

const SEEDS: u64 = 24;
const STEPS: usize = 64;
const CAPACITY: usize = 16;
const MAX_CHUNK: usize = 4;

/// Completion jitter drawn per operation from the schedule stream.
///
/// The links themselves have no base latency, so this is the whole delay: two
/// operations admitted in one order complete in whichever order their draws
/// decide. The exact width does not matter, only that it is wide enough for
/// two independent draws to differ.
const COMPLETION_JITTER: SimDuration = SimDuration::from_nanos(16);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Side {
    Left,
    Right,
}

impl Side {
    const fn nodes(self) -> (NodeId, NodeId) {
        match self {
            Self::Left => (NodeId(1), NodeId(2)),
            Self::Right => (NodeId(2), NodeId(1)),
        }
    }
}

#[derive(Debug, Default)]
struct FlowModel {
    bytes: VecDeque<u8>,
    sender_open: bool,
}

impl FlowModel {
    fn new() -> Self {
        Self {
            bytes: VecDeque::new(),
            sender_open: true,
        }
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
struct Coverage {
    writes: u64,
    capacity_pending_writes: u64,
    reads: u64,
    partitions: u64,
    shutdowns: u64,
    eof_reads: u64,
    /// Concurrent cross-direction writes that completed left-first.
    concurrent_left_first: u64,
    /// Concurrent cross-direction writes that completed right-first.
    concurrent_right_first: u64,
}

#[derive(Debug, Eq, PartialEq)]
struct Artifact {
    trace: Vec<String>,
    checkpoint: DeterminismCheckpoint,
    coverage: Coverage,
}

fn below(random: &RandomHandle, upper: u64) -> u64 {
    random
        .random_below(upper)
        .expect("model random bounds are nonzero")
}

fn write_one(
    runtime: &mut SimRuntime,
    stream: &SimStream,
    receiver: &SimStream,
    model: &mut FlowModel,
    random: &RandomHandle,
    trace: &mut Vec<String>,
    coverage: &mut Coverage,
) {
    let length = below(random, 9) as usize;
    let payload = (0..length)
        .map(|_| random.random_u64().expect("model runtime is active") as u8)
        .collect::<Vec<_>>();
    if !model.sender_open {
        let result = runtime
            .block_on(stream.submit_write(WriteRequest {
                buffer: payload.clone(),
            }))
            .expect("runtime drives closed write");
        let error = result.expect_err("writes after shutdown are closed");
        assert_eq!(error.error().error(), &NetworkError::WriteClosed);
        assert_eq!(error.into_parts().1.into_buffer(), Some(payload));
        trace.push("write:closed".to_owned());
        return;
    }
    if payload.is_empty() {
        let result = runtime
            .block_on(stream.submit_write(WriteRequest {
                buffer: payload.clone(),
            }))
            .expect("runtime drives empty write");
        let success = result.expect("empty write succeeds even at capacity");
        assert_eq!(success.buffer, payload);
        assert_eq!(success.bytes_written, 0);
        coverage.writes += 1;
        trace.push("write:0:0".to_owned());
        return;
    }
    if model.bytes.len() == CAPACITY {
        let pending = stream.submit_write(WriteRequest {
            buffer: payload.clone(),
        });
        let drained = runtime
            .block_on(receiver.submit_read(ReadRequest {
                buffer: Vec::new(),
                max_bytes: 1,
            }))
            .expect("runtime drives capacity-opening read")
            .expect("capacity-opening read succeeds");
        assert_eq!(drained.bytes_read, 1);
        assert_eq!(
            drained.buffer,
            vec![model.bytes.pop_front().expect("full model has a byte")]
        );
        let success = runtime
            .block_on(pending)
            .expect("runtime drives formerly blocked write")
            .expect("capacity-blocked write succeeds");
        assert_eq!(success.buffer, payload);
        assert_eq!(success.bytes_written, 1);
        model.bytes.push_back(payload[0]);
        coverage.writes += 1;
        coverage.capacity_pending_writes += 1;
        coverage.reads += 1;
        trace.push(format!("write:pending:{length}:1"));
        return;
    }

    let result = runtime
        .block_on(stream.submit_write(WriteRequest {
            buffer: payload.clone(),
        }))
        .expect("runtime drives modeled write");
    let written = length.min(MAX_CHUNK).min(CAPACITY - model.bytes.len());
    let success = result.expect("write succeeds within modeled capacity");
    assert_eq!(success.buffer, payload);
    assert_eq!(success.bytes_written, written);
    model.bytes.extend(payload[..written].iter().copied());
    coverage.writes += 1;
    trace.push(format!("write:{length}:{written}"));
}

fn read_one(
    runtime: &mut SimRuntime,
    stream: &SimStream,
    model: &mut FlowModel,
    random: &RandomHandle,
    trace: &mut Vec<String>,
    coverage: &mut Coverage,
) {
    assert!(
        !model.bytes.is_empty() || !model.sender_open,
        "the model never blocks on an open empty direction"
    );
    let max_bytes = 1 + below(random, 8) as usize;
    let prefix = vec![0xa5];
    let expected = model
        .bytes
        .iter()
        .take(max_bytes.min(MAX_CHUNK))
        .copied()
        .collect::<Vec<_>>();
    let result = runtime
        .block_on(stream.submit_read(ReadRequest {
            buffer: prefix.clone(),
            max_bytes,
        }))
        .expect("runtime drives modeled read")
        .expect("modeled read succeeds");
    assert_eq!(result.bytes_read, expected.len());
    assert_eq!(result.buffer, [prefix, expected.clone()].concat());
    for byte in &expected {
        assert_eq!(model.bytes.pop_front(), Some(*byte));
    }
    let eof = expected.is_empty() && !model.sender_open;
    assert_eq!(result.end_of_stream, eof);
    coverage.reads += 1;
    coverage.eof_reads += u64::from(eof);
    trace.push(format!("read:{max_bytes}:{}:{eof}", expected.len()));
}

fn partitioned_write(
    runtime: &mut SimRuntime,
    network: &SimNetwork,
    side: Side,
    stream: &SimStream,
    model: &FlowModel,
    trace: &mut Vec<String>,
    coverage: &mut Coverage,
) {
    if !model.sender_open || model.bytes.len() == CAPACITY {
        return;
    }
    let (from, to) = side.nodes();
    let link = LinkKey { from, to };
    network
        .set_link(
            link,
            LinkConfig {
                state: LinkState::Partitioned,
                max_chunk_bytes: MAX_CHUNK,
                ..LinkConfig::default()
            },
        )
        .expect("partition link");
    let payload = vec![0x7e, side as u8];
    let error = runtime
        .block_on(stream.submit_write(WriteRequest {
            buffer: payload.clone(),
        }))
        .expect("runtime drives partitioned write")
        .expect_err("partitioned write is rejected");
    assert_eq!(error.error().error(), &NetworkError::Partitioned { link });
    assert_eq!(error.into_parts().1.into_buffer(), Some(payload));
    network
        .set_link(
            link,
            LinkConfig {
                max_chunk_bytes: MAX_CHUNK,
                ..LinkConfig::default()
            },
        )
        .expect("heal link");
    coverage.partitions += 1;
    trace.push(format!("partition:{side:?}"));
}

fn shutdown_one(
    runtime: &mut SimRuntime,
    stream: &SimStream,
    model: &mut FlowModel,
    trace: &mut Vec<String>,
    coverage: &mut Coverage,
) {
    if !model.sender_open {
        return;
    }
    runtime
        .block_on(stream.submit_shutdown_write())
        .expect("runtime drives shutdown")
        .expect("first write shutdown succeeds");
    model.sender_open = false;
    coverage.shutdowns += 1;
    trace.push("shutdown".to_owned());
}

/// Drives one write on each direction with both admitted and in flight.
///
/// Every other action in this campaign awaits its operation before issuing the
/// next, so the provider never has two completions outstanding and a seed can
/// only explore one completion order. This phase is the exception: both writes
/// are admitted before either is driven, so the order they complete in is
/// decided by the jitter each one draws.
///
/// The two writes travel opposite directions on purpose. Each direction owns
/// its own `FlowModel`, so the two completions touch disjoint model state and
/// the oracle stays exact and single-valued whichever order they land in —
/// concurrency is added without weakening the checker into accepting a set of
/// outcomes.
fn concurrent_cross_writes(
    runtime: &mut SimRuntime,
    left: &SimStream,
    right: &SimStream,
    left_to_right: &mut FlowModel,
    right_to_left: &mut FlowModel,
    trace: &mut Vec<String>,
    coverage: &mut Coverage,
) {
    assert!(
        left_to_right.sender_open && right_to_left.sender_open,
        "the concurrent phase runs before any shutdown"
    );
    assert!(
        left_to_right.bytes.is_empty() && right_to_left.bytes.is_empty(),
        "the concurrent phase runs before any buffered bytes"
    );

    let left_payload = vec![0xc0, 0xc1];
    let right_payload = vec![0xd0, 0xd1];
    let left_write = left.submit_write(WriteRequest {
        buffer: left_payload.clone(),
    });
    let right_write = right.submit_write(WriteRequest {
        buffer: right_payload.clone(),
    });

    let order: Rc<RefCell<Vec<Side>>> = Rc::new(RefCell::new(Vec::new()));
    let handle = runtime.handle();
    for (side, write, payload) in [
        (Side::Left, left_write, left_payload.clone()),
        (Side::Right, right_write, right_payload.clone()),
    ] {
        let order = Rc::clone(&order);
        handle
            .spawn(async move {
                let success = write
                    .await
                    .expect("a concurrent cross-direction write succeeds");
                assert_eq!(success.buffer, payload);
                assert_eq!(success.bytes_written, payload.len());
                order.borrow_mut().push(side);
            })
            .expect("spawn a concurrent writer");
    }
    runtime
        .run_until_stalled()
        .expect("runtime drives both concurrent writes");

    let observed = order.borrow().clone();
    assert_eq!(observed.len(), 2, "both concurrent writes must complete");
    left_to_right.bytes.extend(left_payload.iter().copied());
    right_to_left.bytes.extend(right_payload.iter().copied());
    coverage.writes += 2;
    match observed[0] {
        Side::Left => coverage.concurrent_left_first += 1,
        Side::Right => coverage.concurrent_right_first += 1,
    }
    trace.push(format!("concurrent:{:?}:{:?}", observed[0], observed[1]));
}

fn run_seed(seed: u64) -> Artifact {
    let mut runtime = SimRuntime::new(RuntimeConfig {
        seed,
        ..RuntimeConfig::default()
    });
    let random = runtime.random_source(RandomStream::Workload);
    let network = SimNetwork::new_with_schedule_random(
        runtime.handle(),
        NetworkConfig {
            directional_buffer_bytes: CAPACITY,
            default_link: LinkConfig {
                max_chunk_bytes: MAX_CHUNK,
                ..LinkConfig::default()
            },
            latency_model: SimLatencyModel::uniform_jitter_v1(COMPLETION_JITTER),
            ..NetworkConfig::default()
        },
        runtime.random_source(RandomStream::Schedule),
    )
    .expect("valid model network");
    let (left, right) = network
        .connected_pair(NodeId(1), NodeId(2))
        .expect("model stream pair");
    let mut left_to_right = FlowModel::new();
    let mut right_to_left = FlowModel::new();
    let mut trace = Vec::with_capacity(STEPS + 32);
    let mut coverage = Coverage::default();

    // Run first, while both directions are provably open and empty, so every
    // seed reaches the concurrent path rather than depending on the random walk
    // to leave the streams in a usable state.
    concurrent_cross_writes(
        &mut runtime,
        &left,
        &right,
        &mut left_to_right,
        &mut right_to_left,
        &mut trace,
        &mut coverage,
    );

    for _ in 0..STEPS {
        let side = if below(&random, 2) == 0 {
            Side::Left
        } else {
            Side::Right
        };
        let (sender, receiver, flow) = match side {
            Side::Left => (&left, &right, &mut left_to_right),
            Side::Right => (&right, &left, &mut right_to_left),
        };
        match below(&random, 100) {
            0..=39 => write_one(
                &mut runtime,
                sender,
                receiver,
                flow,
                &random,
                &mut trace,
                &mut coverage,
            ),
            40..=69 if !flow.bytes.is_empty() || !flow.sender_open => read_one(
                &mut runtime,
                receiver,
                flow,
                &random,
                &mut trace,
                &mut coverage,
            ),
            70..=84 => partitioned_write(
                &mut runtime,
                &network,
                side,
                sender,
                flow,
                &mut trace,
                &mut coverage,
            ),
            85..=89 => shutdown_one(&mut runtime, sender, flow, &mut trace, &mut coverage),
            _ => write_one(
                &mut runtime,
                sender,
                receiver,
                flow,
                &random,
                &mut trace,
                &mut coverage,
            ),
        }
    }

    shutdown_one(
        &mut runtime,
        &left,
        &mut left_to_right,
        &mut trace,
        &mut coverage,
    );
    shutdown_one(
        &mut runtime,
        &right,
        &mut right_to_left,
        &mut trace,
        &mut coverage,
    );
    while !left_to_right.bytes.is_empty() {
        read_one(
            &mut runtime,
            &right,
            &mut left_to_right,
            &random,
            &mut trace,
            &mut coverage,
        );
    }
    read_one(
        &mut runtime,
        &right,
        &mut left_to_right,
        &random,
        &mut trace,
        &mut coverage,
    );
    while !right_to_left.bytes.is_empty() {
        read_one(
            &mut runtime,
            &left,
            &mut right_to_left,
            &random,
            &mut trace,
            &mut coverage,
        );
    }
    read_one(
        &mut runtime,
        &left,
        &mut right_to_left,
        &random,
        &mut trace,
        &mut coverage,
    );

    drop(left);
    drop(right);
    drop(network);
    drop(random);
    runtime.shutdown().expect("model runtime shuts down");
    Artifact {
        trace,
        checkpoint: runtime.snapshot().determinism_checkpoint(),
        coverage,
    }
}

#[test]
fn randomized_network_model_preserves_directional_bytes_and_replays() {
    let mut coverage = Coverage::default();
    for seed in 0..SEEDS {
        let first = run_seed(seed);
        let repeated = run_seed(seed);
        assert_eq!(first, repeated, "network model seed {seed} diverged");
        coverage.writes += first.coverage.writes;
        coverage.capacity_pending_writes += first.coverage.capacity_pending_writes;
        coverage.reads += first.coverage.reads;
        coverage.partitions += first.coverage.partitions;
        coverage.shutdowns += first.coverage.shutdowns;
        coverage.eof_reads += first.coverage.eof_reads;
        coverage.concurrent_left_first += first.coverage.concurrent_left_first;
        coverage.concurrent_right_first += first.coverage.concurrent_right_first;
    }
    assert!(coverage.writes > 0, "no modeled writes: {coverage:#?}");
    assert!(
        coverage.capacity_pending_writes > 0,
        "no capacity-pending writes: {coverage:#?}"
    );
    assert!(coverage.reads > 0, "no modeled reads: {coverage:#?}");
    assert!(coverage.partitions > 0, "no partitions: {coverage:#?}");
    assert_eq!(coverage.shutdowns, SEEDS * 2);
    assert!(
        coverage.eof_reads >= SEEDS * 2,
        "each direction must reach EOF: {coverage:#?}"
    );
    assert_eq!(
        coverage.concurrent_left_first + coverage.concurrent_right_first,
        SEEDS,
        "every seed must run the concurrent phase exactly once: {coverage:#?}"
    );
    // The point of drawing completion latency from the schedule stream: seeds
    // must actually reach both orders. A campaign that only ever observed one
    // would be running the concurrent path without exploring it.
    assert!(
        coverage.concurrent_left_first > 0 && coverage.concurrent_right_first > 0,
        "seeds must explore both concurrent completion orders: {coverage:#?}"
    );
}
