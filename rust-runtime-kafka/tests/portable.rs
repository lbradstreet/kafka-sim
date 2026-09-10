//! The portable `RuntimeHandle` runs one application actor on both executors.

use kr_runtime::{
    HostConfig, HostRuntime, RuntimeConfig, RuntimeDuration, RuntimeHandle, SimRuntime, yield_now,
};

/// One runtime-agnostic actor exercising the whole portable surface.
async fn portable_scenario(handle: RuntimeHandle) -> (u64, bool, u64) {
    let started = handle.now();
    let worker = handle
        .clone()
        .spawn(async move {
            yield_now().await;
            let below = handle.random_below(10).expect("runtime is active");
            let ratio = handle.random_bool_ratio(1, 2).expect("runtime is active");
            let raw = handle.random_u64().expect("runtime is active");
            handle
                .sleep(RuntimeDuration::from_millis(5).expect("duration fits"))
                .await
                .expect("runtime stays active across the sleep");
            (below, ratio, raw)
        })
        .expect("worker task is admitted");
    let (below, ratio, raw) = worker.await.expect("worker completes");
    let inner = RuntimeHandle::current().expect("scenario runs inside a runtime task");
    let elapsed = inner
        .now()
        .checked_duration_since(started)
        .expect("time does not run backwards");
    assert!(elapsed >= RuntimeDuration::from_millis(5).expect("duration fits"));
    (below, ratio, raw)
}

#[test]
fn one_actor_produces_identical_seeded_behavior_on_both_executors() {
    let seed = 17;

    let mut sim = SimRuntime::new(RuntimeConfig {
        seed,
        ..RuntimeConfig::default()
    });
    let sim_handle = RuntimeHandle::from(sim.handle());
    let sim_result = sim
        .block_on(portable_scenario(sim_handle))
        .expect("simulation runs the portable scenario");

    let mut host = HostRuntime::new(HostConfig {
        seed,
        ..HostConfig::default()
    })
    .expect("host config is valid");
    let host_handle = RuntimeHandle::from(host.handle());
    let host_result = host
        .block_on(portable_scenario(host_handle))
        .expect("host runtime runs the portable scenario");

    assert_eq!(sim_result, host_result);
}

#[test]
fn current_returns_the_owning_executor_variant() {
    assert!(RuntimeHandle::current().is_none());

    let mut sim = SimRuntime::default();
    sim.block_on(async {
        assert!(matches!(
            RuntimeHandle::current(),
            Some(RuntimeHandle::Sim(_))
        ));
    })
    .expect("simulation root completes");

    let mut host = HostRuntime::default();
    host.block_on(async {
        assert!(matches!(
            RuntimeHandle::current(),
            Some(RuntimeHandle::Host(_))
        ));
    })
    .expect("host root completes");
}
