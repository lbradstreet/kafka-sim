//! Modeled completion latency is the simulation's source of interleaving
//! diversity.
//!
//! The executor keeps strict FIFO ready ordering and never consumes the
//! schedule stream, so without a perturbing latency model a fixed workload
//! explores one completion order for every seed. These tests pin the property
//! that actually matters for bug finding: with jitter installed, seeds reorder
//! concurrent completions, and every such run stays exactly reproducible.

use std::cell::RefCell;
use std::rc::Rc;

use kr_runtime::rng::RandomStream;
use kr_runtime::{RuntimeConfig, SimDuration, SimRuntime};
use kr_runtime_io::latency::SimLatencyModel;
use kr_runtime_io::network::{NetworkConfig, SimNetwork};
use kr_runtime_io::storage::{
    FileIoSubmit, SimDisk, SimPipelineModel, SimRandomSources, SimStorage, SimStorageConfig,
    WriteAtRequest,
};

/// The completion order of two writes issued to two independent disks.
///
/// Each write goes to its own session, so nothing serializes them behind one
/// worker: their relative completion order is decided purely by the latency
/// each one draws.
fn write_completion_order(seed: u64, model: SimLatencyModel) -> Vec<u8> {
    let config = RuntimeConfig {
        seed,
        start_time: RuntimeConfig::derived_start_time(seed),
        ..RuntimeConfig::default()
    };
    let mut runtime = SimRuntime::new(config);
    let handle = runtime.handle();
    let storage_config = SimStorageConfig {
        default_latency: SimDuration::from_nanos(1_000),
        latency_model: model,
        ..SimStorageConfig::default()
    };

    let sessions: Vec<SimStorage> = (0..2)
        .map(|_| {
            SimStorage::open_with_random_sources(
                handle.clone(),
                SimDisk::default(),
                storage_config,
                SimRandomSources::default()
                    .with_schedule(runtime.random_source(RandomStream::Schedule)),
            )
            .expect("open a simulated disk")
        })
        .collect();

    let order = Rc::new(RefCell::new(Vec::new()));
    for (index, storage) in sessions.iter().enumerate() {
        let write = storage.submit_write_at(WriteAtRequest::new(0, vec![index as u8; 8]));
        let order = Rc::clone(&order);
        handle
            .spawn(async move {
                let _outcome = write.await;
                order.borrow_mut().push(index as u8);
            })
            .expect("spawn a writer");
    }

    runtime.block_on(async {}).expect("root completes");
    runtime.run_until_stalled().expect("writers complete");
    let observed = order.borrow().clone();
    assert_eq!(observed.len(), 2, "both writes must complete");
    observed
}

#[test]
fn a_fixed_latency_model_explores_one_completion_order_for_every_seed() {
    let orders: Vec<Vec<u8>> = (0..16)
        .map(|seed| write_completion_order(seed, SimLatencyModel::Fixed))
        .collect();
    let first = &orders[0];
    assert!(
        orders.iter().all(|order| order == first),
        "without jitter every seed produces the same completion order: {orders:?}",
    );
}

#[test]
fn jitter_makes_seeds_explore_both_completion_orders() {
    let model = SimLatencyModel::uniform_jitter_v1(SimDuration::from_nanos(500));
    let orders: Vec<Vec<u8>> = (0..16)
        .map(|seed| write_completion_order(seed, model))
        .collect();
    let distinct: std::collections::BTreeSet<_> = orders.iter().cloned().collect();
    assert_eq!(
        distinct.len(),
        2,
        "16 seeds must reach both completion orders, saw: {distinct:?}",
    );
}

#[test]
fn a_jittered_run_is_exactly_reproducible() {
    let model = SimLatencyModel::uniform_jitter_v1(SimDuration::from_nanos(500));
    for seed in 0..8 {
        assert_eq!(
            write_completion_order(seed, model),
            write_completion_order(seed, model),
            "seed {seed} must replay identically",
        );
    }
}

#[test]
fn a_perturbing_storage_model_without_a_schedule_source_is_rejected() {
    let mut runtime = SimRuntime::default();
    let config = SimStorageConfig {
        latency_model: SimLatencyModel::uniform_jitter_v1(SimDuration::from_nanos(4)),
        ..SimStorageConfig::default()
    };
    let Err(error) = SimStorage::open(runtime.handle(), SimDisk::default(), config) else {
        panic!("jitter needs a schedule source");
    };
    assert!(
        format!("{error}").contains("Schedule random source"),
        "unexpected error: {error}",
    );
    runtime.block_on(async {}).expect("root completes");
}

#[test]
fn a_storage_schedule_source_must_use_the_schedule_stream() {
    let mut runtime = SimRuntime::default();
    let config = SimStorageConfig {
        latency_model: SimLatencyModel::uniform_jitter_v1(SimDuration::from_nanos(4)),
        ..SimStorageConfig::default()
    };
    let sources =
        SimRandomSources::default().with_schedule(runtime.random_source(RandomStream::Fault));
    let Err(error) =
        SimStorage::open_with_random_sources(runtime.handle(), SimDisk::default(), config, sources)
    else {
        panic!("the Fault stream is not a latency source");
    };
    assert!(
        format!("{error}").contains("Schedule stream"),
        "unexpected error: {error}",
    );
    runtime.block_on(async {}).expect("root completes");
}

#[test]
fn a_perturbing_network_model_without_a_schedule_source_is_rejected() {
    let mut runtime = SimRuntime::default();
    let config = NetworkConfig {
        latency_model: SimLatencyModel::uniform_jitter_v1(SimDuration::from_nanos(4)),
        ..NetworkConfig::default()
    };
    let Err(error) = SimNetwork::new(runtime.handle(), config) else {
        panic!("jitter needs a schedule source");
    };
    assert!(
        format!("{error}").contains("Schedule random source"),
        "unexpected error: {error}",
    );
    runtime.block_on(async {}).expect("root completes");
}

#[test]
fn a_network_schedule_source_must_use_the_schedule_stream() {
    let mut runtime = SimRuntime::default();
    let config = NetworkConfig {
        latency_model: SimLatencyModel::uniform_jitter_v1(SimDuration::from_nanos(4)),
        ..NetworkConfig::default()
    };
    let Err(error) = SimNetwork::new_with_schedule_random(
        runtime.handle(),
        config,
        runtime.random_source(RandomStream::Workload),
    ) else {
        panic!("the Workload stream is not a latency source");
    };
    assert!(
        format!("{error}").contains("Schedule stream"),
        "unexpected error: {error}",
    );
    runtime.block_on(async {}).expect("root completes");
}

#[test]
fn a_scripted_delay_is_never_perturbed_by_the_latency_model() {
    use kr_runtime_io::storage::{SimFault, SimOutcome, StorageOperation};

    let mut runtime = SimRuntime::new(RuntimeConfig::default());
    let handle = runtime.handle();
    let config = SimStorageConfig {
        default_latency: SimDuration::from_nanos(1_000),
        latency_model: SimLatencyModel::uniform_jitter_v1(SimDuration::from_nanos(500)),
        ..SimStorageConfig::default()
    };
    let storage = SimStorage::open_with_random_sources(
        handle.clone(),
        SimDisk::default(),
        config,
        SimRandomSources::default().with_schedule(runtime.random_source(RandomStream::Schedule)),
    )
    .expect("open a simulated disk");
    let scripted_delay = SimDuration::from_nanos(7_000);
    storage
        .inject(SimFault::new(
            StorageOperation::WriteAt,
            scripted_delay,
            SimOutcome::Success,
        ))
        .expect("script an exact delay");

    let start = handle.now();
    let write = storage.submit_write_at(WriteAtRequest::new(0, vec![1; 8]));
    let elapsed = Rc::new(RefCell::new(None));
    let observed = Rc::clone(&elapsed);
    let clock = handle.clone();
    handle
        .spawn(async move {
            let _outcome = write.await;
            *observed.borrow_mut() = Some(clock.now());
        })
        .expect("spawn a writer");

    runtime.block_on(async {}).expect("root completes");
    runtime.run_until_stalled().expect("the write completes");
    let completed = elapsed.borrow().expect("the write completed");
    assert_eq!(
        completed.checked_duration_since(start),
        Some(scripted_delay),
        "a scripted delay must be exact even under a perturbing model",
    );
}

/// The completion order of two non-overlapping writes on one session.
///
/// Unlike [`write_completion_order`], both writes go through one worker, so
/// reordering requires the session itself to overlap its commuting prefix:
/// this is what [`SimPipelineModel::CommutingOverlapV1`] adds.
fn same_session_write_completion_order(seed: u64, model: SimLatencyModel) -> Vec<u8> {
    let config = RuntimeConfig {
        seed,
        start_time: RuntimeConfig::derived_start_time(seed),
        ..RuntimeConfig::default()
    };
    let mut runtime = SimRuntime::new(config);
    let handle = runtime.handle();
    let storage_config = SimStorageConfig {
        default_latency: SimDuration::from_nanos(1_000),
        latency_model: model,
        pipeline_model: SimPipelineModel::CommutingOverlapV1,
        ..SimStorageConfig::default()
    };
    let storage = SimStorage::open_with_random_sources(
        handle.clone(),
        SimDisk::default(),
        storage_config,
        SimRandomSources::default().with_schedule(runtime.random_source(RandomStream::Schedule)),
    )
    .expect("open a simulated disk");

    let order = Rc::new(RefCell::new(Vec::new()));
    for index in 0..2_u64 {
        let write = storage.submit_write_at(WriteAtRequest::new(index * 8, vec![index as u8; 8]));
        let order = Rc::clone(&order);
        handle
            .spawn(async move {
                let _outcome = write.await;
                order.borrow_mut().push(index as u8);
            })
            .expect("spawn a writer");
    }

    runtime.block_on(async {}).expect("root completes");
    runtime.run_until_stalled().expect("writers complete");
    let reordered = storage.status().reordered_completions;
    let observed = order.borrow().clone();
    assert_eq!(observed.len(), 2, "both writes must complete");
    assert_eq!(
        reordered,
        u64::from(observed == [1, 0]),
        "the reordered-completion counter must match the observed order"
    );
    observed
}

#[test]
fn jitter_reorders_commuting_writes_within_one_session() {
    let model = SimLatencyModel::uniform_jitter_v1(SimDuration::from_nanos(500));
    let orders: Vec<Vec<u8>> = (0..16)
        .map(|seed| same_session_write_completion_order(seed, model))
        .collect();
    let distinct: std::collections::BTreeSet<_> = orders.iter().cloned().collect();
    assert_eq!(
        distinct.len(),
        2,
        "16 seeds must reach both same-session completion orders, saw: {distinct:?}",
    );
}

#[test]
fn a_jittered_overlapping_session_replays_exactly() {
    let model = SimLatencyModel::uniform_jitter_v1(SimDuration::from_nanos(500));
    for seed in 0..8 {
        assert_eq!(
            same_session_write_completion_order(seed, model),
            same_session_write_completion_order(seed, model),
            "seed {seed} must replay identically",
        );
    }
}
