//! Randomized campaign over the simulated network's byte budgets.
//!
//! The oracle predicts each admission decision from the provider's own
//! reported budget rather than from a private replica of it, so the campaign
//! cannot pass by reproducing an implementation bug in its model. What it
//! checks after every step is the pair of properties the budgets exist to
//! provide: an operation is admitted exactly when its charge fits the budget
//! it draws on, and every charge is returned once its output is consumed.

use kr_runtime::rng::RandomStream;
use kr_runtime::{DeterminismCheckpoint, RandomHandle, RuntimeConfig, SimRuntime};
use kr_runtime_io::network::{
    ByteStreamSubmit, LinkConfig, NetworkConfig, NetworkError, NetworkStatus, NodeId, ReadRequest,
    SimNetwork, SimStream, WriteRequest,
};

const SEEDS: u64 = 32;
const STEPS: usize = 48;

/// Small enough that the budgets bind constantly rather than incidentally.
const MAX_OPERATION_BYTES: usize = 16;
const READ_BUDGET: usize = 24;
const WRITE_BUDGET: usize = 24;
/// Generous, so writes complete instead of blocking on directional capacity.
/// Blocked writes are covered by a dedicated regression test; this campaign is
/// about the budgets themselves.
const DIRECTIONAL_CAPACITY: usize = 4_096;

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
struct Coverage {
    reads_admitted: u64,
    reads_refused: u64,
    writes_admitted: u64,
    writes_refused: u64,
    /// Submissions whose charge exactly filled the remaining budget.
    exact_fits: u64,
    /// Zero-charge submissions admitted against a fully committed budget.
    zero_charge_admissions: u64,
    /// Submissions whose buffer capacity exceeded the bytes the request would
    /// move, so the charge was decided by the allocation.
    over_provisioned: u64,
}

#[derive(Debug, Eq, PartialEq)]
struct Artifact {
    trace: Vec<String>,
    checkpoint: DeterminismCheckpoint,
    coverage: Coverage,
}

/// The oracle: an operation is admitted exactly when its charge fits what the
/// provider reports as still available.
const fn fits(committed: usize, charge: usize, limit: usize) -> bool {
    match committed.checked_add(charge) {
        Some(total) => total <= limit,
        None => false,
    }
}

fn below(random: &RandomHandle, upper: u64) -> u64 {
    random
        .random_below(upper)
        .expect("model random bounds are nonzero")
}

fn network_config() -> NetworkConfig {
    NetworkConfig {
        directional_buffer_bytes: DIRECTIONAL_CAPACITY,
        max_operation_bytes: MAX_OPERATION_BYTES,
        max_outstanding_read_bytes: READ_BUDGET,
        max_outstanding_write_bytes: WRITE_BUDGET,
        default_link: LinkConfig {
            max_chunk_bytes: MAX_OPERATION_BYTES,
            ..LinkConfig::default()
        },
        ..NetworkConfig::default()
    }
}

/// A submitted operation whose response has not been consumed yet.
enum Pending {
    Read(<SimStream as ByteStreamSubmit>::ReadResponse),
    Write(<SimStream as ByteStreamSubmit>::WriteResponse),
}

fn submit_read(
    network: &SimNetwork,
    stream: &SimStream,
    random: &RandomHandle,
    trace: &mut Vec<String>,
    coverage: &mut Coverage,
) -> Option<Pending> {
    let prefix = below(random, 4) as usize;
    let max_bytes = below(random, (MAX_OPERATION_BYTES - prefix + 1) as u64) as usize;
    // Sometimes hand the provider a pooled-style buffer with more capacity
    // than the request needs; the charge must then follow the allocation.
    let slack = below(random, (MAX_OPERATION_BYTES - prefix + 1) as u64) as usize;
    let mut buffer = Vec::with_capacity(prefix + slack);
    buffer.resize(prefix, 0xa5);
    // The oracle charges what the provider sees: the real capacity, which the
    // allocator may round above the request, never below it.
    let charge = buffer.capacity().max(prefix + max_bytes);
    if buffer.capacity() > prefix + max_bytes {
        coverage.over_provisioned += 1;
    }

    let before: NetworkStatus = network.status();
    let expected =
        charge <= MAX_OPERATION_BYTES && fits(before.outstanding_read_bytes, charge, READ_BUDGET);
    if expected && before.outstanding_read_bytes + charge == READ_BUDGET {
        coverage.exact_fits += 1;
    }
    if charge == 0 && before.outstanding_read_bytes == READ_BUDGET {
        coverage.zero_charge_admissions += 1;
    }

    let response = stream.submit_read(ReadRequest { buffer, max_bytes });
    let after = network.status();

    if expected {
        assert_eq!(
            after.outstanding_read_bytes,
            before.outstanding_read_bytes + charge,
            "an admitted read must commit exactly its charge"
        );
        coverage.reads_admitted += 1;
        trace.push(format!("read:admit:{charge}"));
        Some(Pending::Read(response))
    } else {
        assert_eq!(
            after.outstanding_read_bytes, before.outstanding_read_bytes,
            "a refused read must leave the budget untouched"
        );
        coverage.reads_refused += 1;
        trace.push(format!("read:refuse:{charge}"));
        // The response is already terminal; the caller consumes it below.
        Some(Pending::Read(response))
    }
}

fn submit_write(
    network: &SimNetwork,
    stream: &SimStream,
    random: &RandomHandle,
    trace: &mut Vec<String>,
    coverage: &mut Coverage,
) -> Option<Pending> {
    let payload = below(random, (MAX_OPERATION_BYTES + 1) as u64) as usize;
    let slack = below(random, (MAX_OPERATION_BYTES - payload + 1) as u64) as usize;
    let mut buffer = Vec::with_capacity(payload + slack);
    buffer.resize(payload, 0x5a);
    // A write charges its allocation, which the allocator may round above the
    // requested capacity, never below it.
    let charge = buffer.capacity();
    if charge > payload {
        coverage.over_provisioned += 1;
    }

    let before = network.status();
    let expected =
        charge <= MAX_OPERATION_BYTES && fits(before.outstanding_write_bytes, charge, WRITE_BUDGET);
    if expected && before.outstanding_write_bytes + charge == WRITE_BUDGET {
        coverage.exact_fits += 1;
    }
    if charge == 0 && before.outstanding_write_bytes == WRITE_BUDGET {
        coverage.zero_charge_admissions += 1;
    }

    let response = stream.submit_write(WriteRequest { buffer });
    let after = network.status();

    if expected {
        assert_eq!(
            after.outstanding_write_bytes,
            before.outstanding_write_bytes + charge,
            "an admitted write must commit exactly its charge"
        );
        coverage.writes_admitted += 1;
        trace.push(format!("write:admit:{charge}"));
    } else {
        assert_eq!(
            after.outstanding_write_bytes, before.outstanding_write_bytes,
            "a refused write must leave the budget untouched"
        );
        coverage.writes_refused += 1;
        trace.push(format!("write:refuse:{charge}"));
    }
    Some(Pending::Write(response))
}

/// Consumes one pending response, asserting a refusal names the byte budget
/// rather than some unrelated limit.
fn resolve(runtime: &mut SimRuntime, pending: Pending, trace: &mut Vec<String>) {
    match pending {
        Pending::Read(response) => {
            let result = runtime.block_on(response).expect("runtime completes read");
            match result {
                Ok(success) => trace.push(format!("read:done:{}", success.bytes_read)),
                Err(error) => {
                    let failure = error.into_parts().1;
                    if let NetworkError::ResourceExhausted { resource, limit } = failure.error() {
                        assert_eq!(*resource, "outstanding read bytes");
                        assert_eq!(*limit, READ_BUDGET);
                    }
                    assert!(
                        failure.into_buffer().is_some(),
                        "a refused read returns its buffer"
                    );
                    trace.push("read:err".to_owned());
                }
            }
        }
        Pending::Write(response) => {
            let result = runtime.block_on(response).expect("runtime completes write");
            match result {
                Ok(success) => trace.push(format!("write:done:{}", success.bytes_written)),
                Err(error) => {
                    let failure = error.into_parts().1;
                    if let NetworkError::ResourceExhausted { resource, limit } = failure.error() {
                        assert_eq!(*resource, "outstanding write bytes");
                        assert_eq!(*limit, WRITE_BUDGET);
                    }
                    assert!(
                        failure.into_buffer().is_some(),
                        "a refused write returns its buffer"
                    );
                    trace.push("write:err".to_owned());
                }
            }
        }
    }
}

fn run_seed(seed: u64) -> Artifact {
    let mut runtime = SimRuntime::new(RuntimeConfig {
        seed,
        start_time: RuntimeConfig::derived_start_time(seed),
        ..RuntimeConfig::default()
    });
    let random = runtime.random_source(RandomStream::Workload);
    let network =
        SimNetwork::new(runtime.handle(), network_config()).expect("valid campaign network");
    let (left, right) = network
        .connected_pair(NodeId(1), NodeId(2))
        .expect("campaign stream pair");

    let mut trace = Vec::with_capacity(STEPS * 2);
    let mut coverage = Coverage::default();
    // Reads and writes are held separately because only writes can be resolved
    // mid-run. A read of an open, empty direction blocks until a peer writes,
    // so resolving one here could park the campaign on its own scheduling
    // rather than on anything the budgets do. Reads are drained after a
    // half-close below, where they terminalize on end-of-stream instead.
    let mut pending_reads: Vec<Pending> = Vec::new();
    let mut pending_writes: Vec<Pending> = Vec::new();

    for _ in 0..STEPS {
        let resolve_write = !pending_writes.is_empty() && below(&random, 100) < 35;
        if resolve_write {
            let index = below(&random, pending_writes.len() as u64) as usize;
            let pending = pending_writes.remove(index);
            resolve(&mut runtime, pending, &mut trace);
        } else if below(&random, 2) == 0 {
            if let Some(pending) = submit_read(&network, &right, &random, &mut trace, &mut coverage)
            {
                pending_reads.push(pending);
            }
        } else if let Some(pending) =
            submit_write(&network, &left, &random, &mut trace, &mut coverage)
        {
            pending_writes.push(pending);
        }

        let status = network.status();
        assert!(
            status.outstanding_read_bytes <= READ_BUDGET,
            "the read budget was exceeded: {status:?}"
        );
        assert!(
            status.outstanding_write_bytes <= WRITE_BUDGET,
            "the write budget was exceeded: {status:?}"
        );
    }

    while let Some(pending) = pending_writes.pop() {
        resolve(&mut runtime, pending, &mut trace);
    }
    // Half-closing the direction terminalizes every outstanding read: each one
    // returns whatever is buffered, or end-of-stream, instead of waiting.
    runtime
        .block_on(left.submit_shutdown_write())
        .expect("runtime completes half-close")
        .expect("half-close succeeds");
    while let Some(pending) = pending_reads.pop() {
        resolve(&mut runtime, pending, &mut trace);
    }

    // Conservation: draining every response returns both budgets to zero. A
    // charge that leaked would strand bytes here.
    let drained = network.status();
    assert_eq!(
        drained.outstanding_read_bytes, 0,
        "read bytes leaked: {drained:?}"
    );
    assert_eq!(
        drained.outstanding_write_bytes, 0,
        "write bytes leaked: {drained:?}"
    );

    drop(left);
    drop(right);
    drop(network);
    drop(random);
    runtime.shutdown().expect("campaign runtime shuts down");
    Artifact {
        trace,
        checkpoint: runtime.snapshot().determinism_checkpoint(),
        coverage,
    }
}

#[test]
fn randomized_byte_budget_model_admits_exactly_what_fits_and_replays() {
    let mut total = Coverage::default();
    for seed in 0..SEEDS {
        let first = run_seed(seed);
        let repeated = run_seed(seed);
        assert_eq!(
            first, repeated,
            "byte budget campaign seed {seed} diverged; repro: KR_RUNTIME_SEED={seed} cargo test -p kr-runtime-io --test byte_budget_model"
        );

        // Per-seed baseline: a seed that never submitted anything would make
        // the aggregate gate meaningless.
        assert!(
            first.coverage.reads_admitted + first.coverage.writes_admitted > 0,
            "seed {seed} admitted nothing: {:#?}",
            first.coverage
        );

        total.reads_admitted += first.coverage.reads_admitted;
        total.reads_refused += first.coverage.reads_refused;
        total.writes_admitted += first.coverage.writes_admitted;
        total.writes_refused += first.coverage.writes_refused;
        total.exact_fits += first.coverage.exact_fits;
        total.zero_charge_admissions += first.coverage.zero_charge_admissions;
        total.over_provisioned += first.coverage.over_provisioned;
    }

    // Aggregate gates: the campaign must actually reach refusal and the exact
    // boundary, or it is only testing the happy path.
    assert!(
        total.reads_refused > 0,
        "no read was ever refused for budget: {total:#?}"
    );
    assert!(
        total.writes_refused > 0,
        "no write was ever refused for budget: {total:#?}"
    );
    assert!(
        total.exact_fits > 0,
        "no charge ever exactly filled a budget: {total:#?}"
    );
    assert!(
        total.over_provisioned > 0,
        "no submission ever carried an over-provisioned buffer: {total:#?}"
    );
}

/// Meta-test: the oracle must reject a mutated budget rather than agree with
/// whatever the provider reports.
#[test]
fn the_admission_oracle_rejects_mutated_limits() {
    // Truthful.
    assert!(fits(0, 16, 16));
    assert!(fits(8, 8, 16));
    assert!(!fits(9, 8, 16));
    assert!(fits(16, 0, 16), "a zero charge always fits");

    // Off-by-one mutants must disagree with the truthful oracle somewhere.
    let off_by_one_high =
        |committed: usize, charge: usize, limit: usize| committed + charge < limit;
    let off_by_one_low =
        |committed: usize, charge: usize, limit: usize| committed + charge <= limit + 1;
    assert_ne!(fits(8, 8, 16), off_by_one_high(8, 8, 16));
    assert_ne!(fits(9, 8, 16), off_by_one_low(9, 8, 16));

    // Overflow must refuse rather than wrap into an apparent fit.
    assert!(!fits(1, usize::MAX, usize::MAX));
}
