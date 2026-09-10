//! Simulation-mode behavior of the tokio-shaped facade.
//!
//! These tests compile only under `--cfg kr_runtime_sim`:
//!
//! ```text
//! RUSTFLAGS='--cfg kr_runtime_sim' cargo test -p kr-runtime-tokio
//! ```

#![cfg(kr_runtime_sim)]

mod common;

use kr_runtime::{HostRuntime, RuntimeConfig, SimRuntime};
use kr_runtime_io::network::{LinkConfig, LinkKey, LinkState, NetworkConfig, NodeId, SimNetwork};
use kr_runtime_tokio::net::{SimNetContext, TcpListener, TcpStream};
use kr_runtime_tokio::task;
use kr_runtime_tokio::time::{Duration, Instant, sleep, timeout};
use std::net::Ipv4Addr;

const SERVER_IP: Ipv4Addr = Ipv4Addr::new(10, 0, 0, 1);
const CLIENT_IP: Ipv4Addr = Ipv4Addr::new(10, 0, 0, 2);
const SERVER_ENDPOINT: &str = "10.0.0.1:7000";

fn node(ip: Ipv4Addr) -> NodeId {
    NodeId(u64::from(u32::from(ip)))
}

fn swept_config(seed: u64) -> RuntimeConfig {
    RuntimeConfig {
        seed,
        start_time: RuntimeConfig::derived_start_time(seed),
        ..RuntimeConfig::default()
    }
}

#[test]
fn a_tokio_shaped_component_runs_on_exact_virtual_time_under_every_seed() {
    kr_runtime::seed_sweep!(8, |seed| {
        let mut runtime = SimRuntime::new(swept_config(seed));
        let (ticks, elapsed) = runtime
            .block_on(common::timed_ticks(100, Duration::from_secs(1)))
            .expect("root completes");
        assert_eq!(ticks, 100);
        assert_eq!(elapsed, Duration::from_secs(100));
    });
}

#[test]
fn two_runs_of_the_facade_component_produce_identical_determinism_checkpoints() {
    kr_runtime::seed_sweep!(8, |seed| {
        let checkpoint = |config: &RuntimeConfig| {
            let mut runtime = SimRuntime::new(config.clone());
            runtime
                .block_on(async {
                    let ticker = task::spawn(common::timed_ticks(5, Duration::from_millis(3)));
                    common::timed_ticks(3, Duration::from_millis(5)).await;
                    ticker.await.expect("ticker joins")
                })
                .expect("root completes");
            runtime.snapshot().determinism_checkpoint()
        };
        let config = swept_config(seed);
        assert_eq!(checkpoint(&config), checkpoint(&config));
    });
}

#[test]
fn timeout_cancels_the_losing_timer_and_stops_virtual_time_at_the_winner() {
    let mut runtime = SimRuntime::new(swept_config(0));
    runtime
        .block_on(async {
            let start = Instant::now();

            let lost = timeout(Duration::from_millis(1), sleep(Duration::from_millis(2))).await;
            assert!(lost.is_err(), "the deadline elapses first");
            assert_eq!(start.elapsed(), Duration::from_millis(1));

            let won = timeout(Duration::from_millis(2), sleep(Duration::from_millis(1))).await;
            assert!(won.is_ok(), "the inner future completes first");
            assert_eq!(start.elapsed(), Duration::from_millis(2));

            let tie = timeout(Duration::from_millis(1), sleep(Duration::from_millis(1))).await;
            assert!(tie.is_ok(), "completion wins a same-instant tie");
            assert_eq!(start.elapsed(), Duration::from_millis(3));
        })
        .expect("root completes");
}

#[test]
fn spawned_facade_tasks_join_with_their_outputs() {
    let mut runtime = SimRuntime::new(swept_config(0));
    runtime
        .block_on(async {
            let first = task::spawn(async { 1_u32 });
            let second = task::spawn_local(async {
                task::yield_now().await;
                2_u32
            });
            let blocking = task::spawn_blocking(|| 3_u32);
            assert_eq!(first.await.expect("first joins"), 1);
            assert_eq!(second.await.expect("second joins"), 2);
            assert_eq!(blocking.await.expect("blocking joins"), 3);
        })
        .expect("root completes");
}

#[test]
fn aborting_a_facade_task_reports_a_cancelled_join_error() {
    let mut runtime = SimRuntime::new(swept_config(0));
    runtime
        .block_on(async {
            let task = task::spawn(async {
                sleep(Duration::from_secs(1)).await;
            });
            task.abort();
            let error = task.await.expect_err("abort wins before the first poll");
            assert!(error.is_cancelled());
            assert!(!error.is_panic());
        })
        .expect("root completes");
}

#[test]
fn tokio_io_combinators_echo_bytes_over_the_simulated_network() {
    kr_runtime::seed_sweep!(4, |seed| {
        let run = |config: &RuntimeConfig| {
            let mut runtime = SimRuntime::new(config.clone());
            let network = SimNetwork::new(runtime.handle(), NetworkConfig::default())
                .expect("network builds");
            let server_context = SimNetContext::new(network.clone(), SERVER_IP);
            let client_context = SimNetContext::new(network, CLIENT_IP);
            runtime
                .block_on(async {
                    let listener = server_context
                        .scope(TcpListener::bind(SERVER_ENDPOINT))
                        .await
                        .expect("bind succeeds");
                    let server = task::spawn(async move {
                        let (stream, _) = listener.accept().await.expect("accept succeeds");
                        common::echo_once(stream).await.expect("echo completes")
                    });
                    let client = client_context
                        .scope(TcpStream::connect(SERVER_ENDPOINT))
                        .await
                        .expect("connect succeeds");
                    assert_eq!(
                        client.peer_addr().expect("client knows its peer").port(),
                        7000
                    );
                    let response = common::request_response(client, b"deterministic tokio")
                        .await
                        .expect("exchange completes");
                    assert_eq!(response, b"deterministic tokio");
                    assert_eq!(server.await.expect("server joins"), b"deterministic tokio");
                })
                .expect("root completes");
            runtime.snapshot().determinism_checkpoint()
        };
        let config = swept_config(seed);
        assert_eq!(run(&config), run(&config));
    });
}

#[test]
fn a_partitioned_link_surfaces_as_a_host_unreachable_io_error() {
    let mut runtime = SimRuntime::new(swept_config(0));
    let network =
        SimNetwork::new(runtime.handle(), NetworkConfig::default()).expect("network builds");
    network
        .set_link(
            LinkKey {
                from: node(CLIENT_IP),
                to: node(SERVER_IP),
            },
            LinkConfig {
                state: LinkState::Partitioned,
                ..LinkConfig::default()
            },
        )
        .expect("link override accepted");
    let server_context = SimNetContext::new(network.clone(), SERVER_IP);
    let client_context = SimNetContext::new(network, CLIENT_IP);
    runtime
        .block_on(async {
            let _listener = server_context
                .scope(TcpListener::bind(SERVER_ENDPOINT))
                .await
                .expect("bind succeeds");
            let error = client_context
                .scope(TcpStream::connect(SERVER_ENDPOINT))
                .await
                .expect_err("connect crosses the partitioned direction");
            assert_eq!(error.kind(), std::io::ErrorKind::HostUnreachable);
        })
        .expect("root completes");
}

#[test]
fn the_same_component_completes_identically_on_sim_and_host() {
    let tick = Duration::from_millis(2);

    let mut sim = SimRuntime::new(swept_config(0));
    let (sim_ticks, sim_elapsed) = sim
        .block_on(common::timed_ticks(3, tick))
        .expect("sim root completes");
    assert_eq!(sim_ticks, 3);
    assert_eq!(sim_elapsed, Duration::from_millis(6));

    let mut host = HostRuntime::default();
    let (host_ticks, host_elapsed) = host
        .block_on(common::timed_ticks(3, tick))
        .expect("host root completes");
    assert_eq!(host_ticks, 3);
    assert!(
        host_elapsed >= Duration::from_millis(6),
        "host timers wait real time: {host_elapsed:?}"
    );
}
